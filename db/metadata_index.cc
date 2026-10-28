// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/metadata_index.h"

#include <algorithm>
#include <cassert>
#include <cstring>
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

  static Status GetMany(Table* table, const ReadOptions& options,
                        const std::vector<Slice>& keys,
                        std::vector<Result>* results,
                        size_t* data_block_probes) {
    std::vector<Table::GetResult> table_results;
    Status s = table->InternalGetMany(options, keys, &table_results,
                                      data_block_probes);
    if (!s.ok()) return s;
    results->clear();
    results->resize(table_results.size());
    for (size_t i = 0; i < table_results.size(); ++i) {
      if (!table_results[i].found) continue;
      (*results)[i].key = std::move(table_results[i].key);
      (*results)[i].value = std::move(table_results[i].value);
      (*results)[i].found = true;
    }
    return Status::OK();
  }
};

namespace {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

// POSIX mmap-backed RandomAccessFiles return a pointer into the mapping instead
// of the caller's scratch buffer. table::ReadBlock deliberately avoids caching
// such blocks because it cannot own that memory. Metadata files are immutable,
// so copy the read into scratch and let the existing bounded block cache retain
// the checked block across point lookups.
class MetadataScratchFile final : public RandomAccessFile {
 public:
  explicit MetadataScratchFile(RandomAccessFile* file) : file_(file) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    Slice contents;
    Status s = file_->Read(offset, n, &contents, scratch);
    if (!s.ok()) return s;
    if (contents.data() != scratch && !contents.empty()) {
      std::memcpy(scratch, contents.data(), contents.size());
      contents = Slice(scratch, contents.size());
    }
    *result = contents;
    return Status::OK();
  }

 private:
  std::unique_ptr<RandomAccessFile> file_;
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

class CountingCache final : public Cache {
 public:
  CountingCache(Cache* cache, MetadataProfile* profile)
      : cache_(cache), profile_(profile) {}

  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice&, void*)) override {
    return cache_->Insert(key, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    Handle* handle = cache_->Lookup(key);
    profile_->Count(handle == nullptr ? MetadataProfileCounter::kBlockCacheMisses
                                      : MetadataProfileCounter::kBlockCacheHits);
    return handle;
  }
  void Release(Handle* handle) override { cache_->Release(handle); }
  void* Value(Handle* handle) override { return cache_->Value(handle); }
  void Erase(const Slice& key) override { cache_->Erase(key); }
  uint64_t NewId() override { return cache_->NewId(); }
  void Prune() override { cache_->Prune(); }
  size_t TotalCharge() const override { return cache_->TotalCharge(); }

 private:
  Cache* const cache_;
  MetadataProfile* const profile_;
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
                     const Options& options, size_t capacity,
                     MetadataProfile* profile)
      : env_(env), dbname_(dbname), options_(options),
        cache_(NewLRUCache(std::max<size_t>(1, capacity))),
        profile_(profile) {}

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

  Status GetMany(const MetadataFileMeta& metadata_file,
                 const std::vector<Slice>& keys,
                 std::vector<LookupResult>* results) const {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(metadata_file, &handle);
    if (!s.ok()) return s;
    TableAndFile* entry = static_cast<TableAndFile*>(cache_->Value(handle));
    ReadOptions read_options;
    read_options.verify_checksums = true;
    size_t block_probes = 0;
    std::vector<MetadataTableAccess::Result> table_results;
    s = MetadataTableAccess::GetMany(entry->table, read_options, keys,
                                     &table_results, &block_probes);
    cache_->Release(handle);
    if (!s.ok()) return s;
    if (profile_ != nullptr) {
      profile_->Count(MetadataProfileCounter::kBatchTableGets);
      profile_->Count(MetadataProfileCounter::kBatchDataBlockProbes,
                      block_probes);
    }
    results->clear();
    results->resize(table_results.size());
    for (size_t i = 0; i < table_results.size(); ++i) {
      LookupResult& result = (*results)[i];
      result.status = Status::OK();
      if (!table_results[i].found || table_results[i].key != keys[i]) continue;
      if (!MetadataIndex::DecodeValue(table_results[i].value, &result.value)) {
        return Status::Corruption("invalid metadata table value");
      }
      result.found = true;
    }
    return Status::OK();
  }

  Iterator* NewIterator(const MetadataFileMeta& metadata_file) const {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(metadata_file, &handle);
    if (!s.ok()) return NewErrorIterator(s);
    TableAndFile* entry = static_cast<TableAndFile*>(cache_->Value(handle));
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
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
    if (*handle != nullptr) {
      if (profile_ != nullptr)
        profile_->Count(MetadataProfileCounter::kTableCacheHits);
      return Status::OK();
    }
    if (profile_ != nullptr)
      profile_->Count(MetadataProfileCounter::kTableCacheMisses);

    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    const std::string filename =
        metadata_file.kind == kMetadataChange
            ? MetadataChangeFileName(dbname_, number)
            : MetadataFileName(dbname_, number);
    Status s = env_->NewRandomAccessFile(filename, &file);
    if (s.ok()) file = new MetadataScratchFile(file);
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
  MetadataProfile* const profile_;
};

MetadataIndex::MetadataIndex(Env* env, const std::string& dbname,
                             const Options& options,
                             MetadataProfile* profile)
    : env_(env),
      dbname_(dbname),
      options_(options),
      profile_(profile),
      active_memory_usage_(0),
      next_generation_id_(1),
      value_cache_generation_(0),
      value_cache_(NewLRUCache(std::max<size_t>(
          1, options.metadata_cache_size / 4))),
      block_cache_(NewLRUCache(std::max<size_t>(
          1, options.metadata_cache_size / 2))),
      block_cache_observer_(profile == nullptr
                                ? nullptr
                                : new CountingCache(block_cache_, profile)),
      table_cache_(new MetadataTableCache(
          env, dbname, TableOptions(), std::max<size_t>(
                                          1, options.metadata_cache_size / 4),
          profile)) {}

MetadataIndex::~MetadataIndex() {
  delete table_cache_;
  delete block_cache_observer_;
  delete value_cache_;
  delete block_cache_;
}

Options MetadataIndex::TableOptions() const {
  Options result = options_;
  result.comparator = BytewiseComparator();
  result.filter_policy = MetadataFilterPolicy();
  result.block_cache = block_cache_observer_ == nullptr ? block_cache_
                                                        : block_cache_observer_;
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
  MetadataProfile::Timer timer(profile_, MetadataProfileStage::kLookup);
  if (profile_ != nullptr)
    profile_->Count(MetadataProfileCounter::kMetadataLookups);
  std::string cache_key(8, '\0');
  EncodeFixed64(&cache_key[0], value_cache_generation_);
  cache_key.append(key.data(), key.size());
  Cache::Handle* cached = value_cache_->Lookup(cache_key);
  if (cached != nullptr) {
    if (profile_ != nullptr)
      profile_->Count(MetadataProfileCounter::kValueCacheHits);
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
    if (profile_ != nullptr)
      profile_->Count(MetadataProfileCounter::kActiveHits);
    *value = active->second;
    *found = true;
  }
  for (const FrozenGeneration& generation : frozen_) {
    auto frozen = generation.values.find(key.ToString());
    if (frozen != generation.values.end() &&
        (!*found || frozen->second.mseq > value->mseq)) {
      if (profile_ != nullptr)
        profile_->Count(MetadataProfileCounter::kImmutableHits);
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
    // MetadataLookupFiles is ordered newest transaction first. Once a file's
    // maximum transaction cannot beat the current value, no older file can.
    if (*found && file.max_mseq <= value->mseq) break;

    LookupResult result;
    if (profile_ != nullptr)
      profile_->Count(MetadataProfileCounter::kMetadataFilesProbed);
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

Status MetadataIndex::LookupMany(
    const std::vector<MetadataFileMeta>& files,
    const std::vector<Slice>& keys, std::vector<MetadataValue>* values,
    std::vector<bool>* found) const {
  if (keys.size() < 8) {
    values->resize(keys.size());
    found->resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      bool key_found = false;
      Status s = Lookup(files, keys[i], &(*values)[i], &key_found);
      (*found)[i] = key_found;
      if (!s.ok()) return s;
    }
    return Status::OK();
  }

  for (size_t i = 1; i < keys.size(); ++i) {
    if (BytewiseComparator()->Compare(keys[i - 1], keys[i]) >= 0) {
      values->resize(keys.size());
      found->resize(keys.size());
      for (size_t j = 0; j < keys.size(); ++j) {
        bool key_found = false;
        Status s = Lookup(files, keys[j], &(*values)[j], &key_found);
        (*found)[j] = key_found;
        if (!s.ok()) return s;
      }
      return Status::OK();
    }
  }

  values->assign(keys.size(), MetadataValue());
  found->assign(keys.size(), false);
  std::vector<bool> cached(keys.size(), false);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (profile_ != nullptr)
      profile_->Count(MetadataProfileCounter::kMetadataLookups);
    std::string cache_key(8, '\0');
    EncodeFixed64(&cache_key[0], value_cache_generation_);
    cache_key.append(keys[i].data(), keys[i].size());
    Cache::Handle* handle = value_cache_->Lookup(cache_key);
    if (handle != nullptr) {
      if (profile_ != nullptr)
        profile_->Count(MetadataProfileCounter::kValueCacheHits);
      const CachedLookup* result =
          static_cast<const CachedLookup*>(value_cache_->Value(handle));
      (*found)[i] = result->found;
      if (result->found) (*values)[i] = result->value;
      cached[i] = true;
      value_cache_->Release(handle);
      continue;
    }

    const std::string key = keys[i].ToString();
    auto active = active_.find(key);
    if (active != active_.end()) {
      if (profile_ != nullptr)
        profile_->Count(MetadataProfileCounter::kActiveHits);
      (*values)[i] = active->second;
      (*found)[i] = true;
    }
    for (const FrozenGeneration& generation : frozen_) {
      auto frozen = generation.values.find(key);
      if (frozen != generation.values.end() &&
          (!(*found)[i] || frozen->second.mseq > (*values)[i].mseq)) {
        if (profile_ != nullptr)
          profile_->Count(MetadataProfileCounter::kImmutableHits);
        (*values)[i] = frozen->second;
        (*found)[i] = true;
      }
    }
  }

  for (const MetadataFileMeta& file : files) {
    if (file.kind != kMetadataIndex && file.kind != kMetadataChange) continue;
    std::vector<size_t> candidate_indexes;
    std::vector<Slice> candidate_keys;
    candidate_indexes.reserve(keys.size());
    candidate_keys.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      if (cached[i] || ((*found)[i] && file.max_mseq <= (*values)[i].mseq)) {
        continue;
      }
      if (!file.smallest.empty() &&
          BytewiseComparator()->Compare(keys[i], file.smallest) < 0) {
        continue;
      }
      if (!file.largest.empty() &&
          BytewiseComparator()->Compare(keys[i], file.largest) > 0) {
        continue;
      }
      candidate_indexes.push_back(i);
      candidate_keys.push_back(keys[i]);
      if (profile_ != nullptr)
        profile_->Count(MetadataProfileCounter::kMetadataFilesProbed);
    }
    if (candidate_keys.empty()) continue;

    std::vector<LookupResult> results;
    Status s = table_cache_->GetMany(file, candidate_keys, &results);
    if (!s.ok()) return s;
    for (size_t j = 0; j < results.size(); ++j) {
      const size_t i = candidate_indexes[j];
      if (results[j].found &&
          (!(*found)[i] || results[j].value.mseq > (*values)[i].mseq)) {
        (*values)[i] = results[j].value;
        (*found)[i] = true;
      }
    }
  }

  for (size_t i = 0; i < keys.size(); ++i) {
    if (cached[i]) continue;
    std::string cache_key(8, '\0');
    EncodeFixed64(&cache_key[0], value_cache_generation_);
    cache_key.append(keys[i].data(), keys[i].size());
    const size_t charge = keys[i].size() >
                                  std::numeric_limits<size_t>::max() -
                                      sizeof(CachedLookup)
                              ? std::numeric_limits<size_t>::max()
                              : keys[i].size() + sizeof(CachedLookup);
    CachedLookup* result = new CachedLookup;
    result->found = (*found)[i];
    if ((*found)[i]) result->value = (*values)[i];
    Cache::Handle* inserted = value_cache_->Insert(
        cache_key, result, std::max<size_t>(1, charge), &DeleteCachedLookup);
    value_cache_->Release(inserted);
  }
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

void MetadataIndex::InvalidateAll() {
  ++value_cache_generation_;
  if (value_cache_generation_ == 0) ++value_cache_generation_;
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
