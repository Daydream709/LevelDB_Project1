// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/metadata_index.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <memory>
#include <utility>

#include "db/filename.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/iterator.h"
#include "leveldb/table.h"
#include "port/port.h"
#include "table/merger.h"
#include "util/coding.h"

namespace leveldb {

class MetadataTableAccess {
 public:
  struct Result {
    std::string key;
    std::string value;
    bool found = false;
  };

  static void SaveResult(void* arg, const Slice& key, const Slice& value) {
    Result* result = static_cast<Result*>(arg);
    result->key.assign(key.data(), key.size());
    result->value.assign(value.data(), value.size());
    result->found = true;
  }

  static Status Get(Table* table, const ReadOptions& options, const Slice& key,
                    Result* result) {
    return table->InternalGet(options, key, result, &SaveResult);
  }
};

namespace {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

void DeleteTableEntry(const Slice&, void* value) {
  TableAndFile* entry = static_cast<TableAndFile*>(value);
  delete entry->table;
  delete entry->file;
  delete entry;
}

class MapIterator final : public Iterator {
 public:
  explicit MapIterator(const MetadataValueMap* map)
      : map_(map), iter_(map_->end()) {}

  bool Valid() const override { return iter_ != map_->end(); }
  void SeekToFirst() override { iter_ = map_->begin(); }
  void SeekToLast() override {
    iter_ = map_->empty() ? map_->end() : std::prev(map_->end());
  }
  void Seek(const Slice& target) override {
    iter_ = map_->lower_bound(target.ToString());
  }
  void Next() override {
    assert(Valid());
    ++iter_;
  }
  void Prev() override {
    assert(Valid());
    if (iter_ == map_->begin()) {
      iter_ = map_->end();
    } else {
      --iter_;
    }
  }
  Slice key() const override {
    assert(Valid());
    return iter_->first;
  }
  Slice value() const override {
    assert(Valid());
    encoded_value_ = MetadataIndex::EncodeValue(iter_->second);
    return encoded_value_;
  }
  Status status() const override { return Status::OK(); }

 private:
  const MetadataValueMap* const map_;
  MetadataValueMap::const_iterator iter_;
  mutable std::string encoded_value_;
};

struct LookupResult {
  MetadataValue value;
  bool found = false;
  Status status;
};

struct CachedLookup {
  MetadataValue value;
  bool found;
};

void DeleteCachedLookup(const Slice&, void* value) {
  delete static_cast<CachedLookup*>(value);
}

const FilterPolicy* MetadataFilterPolicy() {
  // Process-lifetime singleton. Bloom policy objects are immutable and have no
  // state tied to a DB instance.
  static const FilterPolicy* policy = NewBloomFilterPolicy(10);
  return policy;
}

}  // namespace

bool MetadataKeyLess::operator()(const std::string& a,
                                 const std::string& b) const {
  return BytewiseComparator()->Compare(Slice(a), Slice(b)) < 0;
}

class MetadataIndex::MetadataTableCache {
 public:
  MetadataTableCache(Env* env, const std::string& dbname,
                     const Options& options, size_t capacity)
      : env_(env), dbname_(dbname), options_(options),
        cache_(NewLRUCache(std::max<size_t>(1, capacity))) {}

  ~MetadataTableCache() { delete cache_; }

  size_t TotalCharge() const { return cache_->TotalCharge(); }

  Status Get(const MetadataFileMeta& metadata_file, const Slice& key,
             LookupResult* result) const {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(metadata_file, &handle);
    if (!s.ok()) return s;
    TableAndFile* entry = static_cast<TableAndFile*>(cache_->Value(handle));
    ReadOptions read_options;
    read_options.verify_checksums = true;
    result->found = false;
    result->status = Status::OK();
    MetadataTableAccess::Result point_result;
    result->status = MetadataTableAccess::Get(entry->table, read_options, key,
                                              &point_result);
    if (result->status.ok() && point_result.found &&
        point_result.key == key) {
      result->found = MetadataIndex::DecodeValue(point_result.value,
                                                 &result->value);
      if (!result->found) {
        result->status = Status::Corruption("invalid metadata table value");
      }
    }
    cache_->Release(handle);
    if (!result->status.ok()) return result->status;
    return Status::OK();
  }

  Iterator* NewIterator(const MetadataFileMeta& metadata_file) const {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(metadata_file, &handle);
    if (!s.ok()) return NewErrorIterator(s);
    TableAndFile* entry = static_cast<TableAndFile*>(cache_->Value(handle));
    ReadOptions read_options;
    read_options.verify_checksums = true;
    Iterator* iter = entry->table->NewIterator(read_options);
    iter->RegisterCleanup(&Release, cache_, handle);
    return iter;
  }

  void Evict(uint64_t number) const {
    char key[sizeof(number)];
    EncodeFixed64(key, number);
    cache_->Erase(Slice(key, sizeof(key)));
  }

 private:
  static void Release(void* cache_arg, void* handle_arg) {
    Cache* cache = static_cast<Cache*>(cache_arg);
    cache->Release(static_cast<Cache::Handle*>(handle_arg));
  }

  Status FindTable(const MetadataFileMeta& metadata_file,
                   Cache::Handle** handle) const {
    const uint64_t number = metadata_file.number;
    char encoded_number[sizeof(number)];
    EncodeFixed64(encoded_number, number);
    Slice key(encoded_number, sizeof(encoded_number));
    *handle = cache_->Lookup(key);
    if (*handle != nullptr) return Status::OK();

    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    const std::string filename =
        metadata_file.kind == kMetadataChange
            ? MetadataChangeFileName(dbname_, number)
            : MetadataFileName(dbname_, number);
    Status s = env_->NewRandomAccessFile(filename, &file);
    if (s.ok()) s = Table::Open(options_, file, metadata_file.file_size, &table);
    if (!s.ok()) {
      delete table;
      delete file;
      return s;
    }
    TableAndFile* entry = new TableAndFile{file, table};
    const uint64_t max_charge =
        static_cast<uint64_t>(std::numeric_limits<size_t>::max());
    const size_t charge = static_cast<size_t>(
        std::max<uint64_t>(1, std::min(metadata_file.file_size, max_charge)));
    *handle = cache_->Insert(key, entry, charge, &DeleteTableEntry);
    return Status::OK();
  }

  Env* const env_;
  const std::string dbname_;
  Options options_;
  Cache* const cache_;
};

MetadataIndex::MetadataIndex(Env* env, const std::string& dbname,
                             const Options& options)
    : env_(env),
      dbname_(dbname),
      options_(options),
      active_memory_usage_(0),
      next_generation_id_(1),
      value_cache_generation_(0),
      value_cache_(NewLRUCache(std::max<size_t>(
          1, options.metadata_cache_size / 4))),
      block_cache_(NewLRUCache(std::max<size_t>(
          1, options.metadata_cache_size / 2))),
      table_cache_(new MetadataTableCache(
          env, dbname, TableOptions(), std::max<size_t>(
                                          1, options.metadata_cache_size / 4))) {}

MetadataIndex::~MetadataIndex() {
  delete table_cache_;
  delete value_cache_;
  delete block_cache_;
}

Options MetadataIndex::TableOptions() const {
  Options result = options_;
  result.comparator = BytewiseComparator();
  result.filter_policy = MetadataFilterPolicy();
  result.block_cache = block_cache_;
  result.paranoid_checks = true;
  return result;
}

size_t MetadataIndex::ApproximateMemoryUsage() const {
  return active_memory_usage_ + ImmutableMemoryUsage() +
         value_cache_->TotalCharge() + block_cache_->TotalCharge() +
         table_cache_->TotalCharge();
}

Status MetadataIndex::Lookup(const std::vector<MetadataFileMeta>& files,
                             const Slice& key, MetadataValue* value,
                             bool* found) const {
  std::string cache_key(8, '\0');
  EncodeFixed64(&cache_key[0], value_cache_generation_);
  cache_key.append(key.data(), key.size());
  Cache::Handle* cached = value_cache_->Lookup(cache_key);
  if (cached != nullptr) {
    const CachedLookup* result =
        static_cast<const CachedLookup*>(value_cache_->Value(cached));
    *found = result->found;
    if (*found) *value = result->value;
    value_cache_->Release(cached);
    return Status::OK();
  }

  *found = false;
  auto active = active_.find(key.ToString());
  if (active != active_.end()) {
    *value = active->second;
    *found = true;
  }
  for (const FrozenGeneration& generation : frozen_) {
    auto frozen = generation.values.find(key.ToString());
    if (frozen != generation.values.end() &&
        (!*found || frozen->second.mseq > value->mseq)) {
      *value = frozen->second;
      *found = true;
    }
  }

  for (const MetadataFileMeta& file : files) {
    if (file.kind != kMetadataIndex && file.kind != kMetadataChange) continue;
    if (!file.smallest.empty() && BytewiseComparator()->Compare(key, file.smallest) < 0) {
      continue;
    }
    if (!file.largest.empty() && BytewiseComparator()->Compare(key, file.largest) > 0) {
      continue;
    }
    if (*found && file.max_mseq <= value->mseq) continue;

    LookupResult result;
    Status s = table_cache_->Get(file, key, &result);
    if (!s.ok()) return s;
    if (result.found && (!*found || result.value.mseq > value->mseq)) {
      *value = result.value;
      *found = true;
    }
  }
  const size_t key_size = key.size();
  const size_t charge = key_size > std::numeric_limits<size_t>::max() -
                                      sizeof(CachedLookup)
                            ? std::numeric_limits<size_t>::max()
                            : key_size + sizeof(CachedLookup);
  CachedLookup* result = new CachedLookup;
  result->found = *found;
  if (*found) result->value = *value;
  Cache::Handle* inserted =
      value_cache_->Insert(cache_key, result, std::max<size_t>(1, charge),
                           &DeleteCachedLookup);
  value_cache_->Release(inserted);
  return Status::OK();
}

void MetadataIndex::Apply(
    const std::vector<std::pair<std::string, MetadataValue>>& values) {
  for (const auto& item : values) {
    auto iter = active_.find(item.first);
    if (iter == active_.end()) {
      active_memory_usage_ += item.first.size() + sizeof(MetadataValue) + 48;
      active_.emplace(item.first, item.second);
    } else {
      iter->second = item.second;
    }
    Invalidate(item.first);
  }
}

void MetadataIndex::Invalidate(const Slice& key) {
  std::string cache_key(8, '\0');
  EncodeFixed64(&cache_key[0], value_cache_generation_);
  cache_key.append(key.data(), key.size());
  value_cache_->Erase(cache_key);
}

void MetadataIndex::ClearActiveThrough(uint64_t mseq) {
  for (auto iter = active_.begin(); iter != active_.end();) {
    if (iter->second.mseq <= mseq) {
      active_memory_usage_ -= iter->first.size() + sizeof(MetadataValue) + 48;
      iter = active_.erase(iter);
    } else {
      ++iter;
    }
  }
}

void MetadataIndex::ResetAfterRebuild() {
  active_.clear();
  active_memory_usage_ = 0;
  frozen_.clear();
  ++value_cache_generation_;
}

size_t MetadataIndex::ImmutableMemoryUsage() const {
  size_t result = 0;
  for (const FrozenGeneration& generation : frozen_) {
    result += generation.memory_usage;
  }
  return result;
}

const MetadataIndex::FrozenGeneration* MetadataIndex::OldestImmutable() const {
  return frozen_.empty() ? nullptr : &frozen_.front();
}

bool MetadataIndex::FreezeActive(uint64_t mseq, uint64_t total_entries,
                                 uint64_t distinct_keys, uint64_t wal_number,
                                 uint64_t wal_bytes,
                                 bool allow_empty) {
  if (!frozen_.empty() || (active_.empty() && !allow_empty)) return false;
  FrozenGeneration generation;
  generation.id = next_generation_id_++;
  generation.mseq = mseq;
  generation.total_entries = total_entries;
  generation.distinct_keys = distinct_keys;
  generation.wal_number = wal_number;
  generation.wal_bytes = wal_bytes;
  generation.memory_usage = active_memory_usage_;
  generation.values = std::move(active_);
  frozen_.push_back(std::move(generation));
  active_.clear();
  active_memory_usage_ = 0;
  return true;
}

bool MetadataIndex::CompleteImmutable(uint64_t generation_id) {
  if (frozen_.empty() || frozen_.front().id != generation_id) return false;
  frozen_.pop_front();
  return true;
}

Iterator* MetadataIndex::NewIterator(
    const std::vector<MetadataFileMeta>& files, bool include_active,
    bool include_immutables) const {
  std::vector<Iterator*> children;
  children.reserve(files.size() + frozen_.size() + (include_active ? 1 : 0));
  for (const MetadataFileMeta& file : files) {
    if (file.kind == kMetadataIndex || file.kind == kMetadataChange) {
      children.push_back(table_cache_->NewIterator(file));
    }
  }
  if (include_immutables) {
    for (const FrozenGeneration& generation : frozen_) {
      if (!generation.values.empty()) {
        children.push_back(new MapIterator(&generation.values));
      }
    }
  }
  if (include_active && !active_.empty()) {
    children.push_back(new MapIterator(&active_));
  }
  if (children.empty()) return NewEmptyIterator();
  if (children.size() == 1) return children[0];
  return NewMergingIterator(BytewiseComparator(), children.data(), children.size());
}

void MetadataIndex::Evict(uint64_t file_number) {
  table_cache_->Evict(file_number);
}

std::string MetadataIndex::EncodeValue(const MetadataValue& value) {
  std::string encoded(16, '\0');
  EncodeFixed64(&encoded[0], value.mseq);
  EncodeFixed64(&encoded[8], value.count);
  return encoded;
}

bool MetadataIndex::DecodeValue(const Slice& encoded, MetadataValue* value) {
  if (encoded.size() != 16) return false;
  value->mseq = DecodeFixed64(encoded.data());
  value->count = DecodeFixed64(encoded.data() + 8);
  return true;
}

}  // namespace leveldb
