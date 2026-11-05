// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include <memory>

#include "leveldb/env.h"
#include "leveldb/table.h"

#include "table/local_count_index.h"
#include "util/coding.h"
#include "util/profiler.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      bytewise_internal_(
          static_cast<const InternalKeyComparator*>(options.comparator)
              ->user_comparator() == BytewiseComparator()),
      cache_(NewLRUCache(entries)),
      local_count_cache_(NewLRUCache(options.metadata_cache_size / 2)) {}

TableCache::~TableCache() {
  delete cache_;
  delete local_count_cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result, bytewise_internal_);
    cache_->Release(handle);
  }
  return s;
}

Status TableCache::CountUserKeyRecords(uint64_t file_number,
                                       uint64_t file_size,
                                       const Slice& user_key,
                                       uint64_t* count) {
  if (count == nullptr) {
    return Status::InvalidArgument("record count output pointer is null");
  }
  *count = 0;
  Cache::Handle* handle = nullptr;
  Status open = FindTable(file_number, file_size, &handle);
  if (!open.ok()) return open;
  Table* table = static_cast<TableAndFile*>(cache_->Value(handle))->table;
  Status indexed =
      LocalCountTableAccess::Lookup(table, local_count_cache_, user_key, count);
  Status primary = indexed.ok() ? Status::OK() : table->VerifyPrimaryIndex();
  cache_->Release(handle);
  if (!primary.ok()) return primary;
  if (indexed.ok()) return indexed;
  *count = 0;
  ProfileScope::Count("metadata.local_count_fallback");
  ReadOptions read;
  read.verify_checksums = true;
  Iterator* iter = NewIterator(read, file_number, file_size);
  InternalKey seek(user_key, kMaxSequenceNumber, kValueTypeForSeek);
  iter->Seek(seek.Encode());
  const Comparator* user_comparator =
      static_cast<const InternalKeyComparator*>(options_.comparator)
          ->user_comparator();
  Status s;
  while (iter->Valid()) {
    ParsedInternalKey parsed;
    if (!ParseInternalKey(iter->key(), &parsed)) {
      s = Status::Corruption("invalid internal key while counting SST records");
      break;
    }
    if (user_comparator->Compare(parsed.user_key, user_key) != 0) break;
    if (parsed.user_key == user_key) {
      if (*count == std::numeric_limits<uint64_t>::max()) {
        s = Status::Corruption("SST user key record count overflow");
        break;
      }
      ++*count;
    }
    iter->Next();
  }
  if (s.ok()) s = iter->status();
  delete iter;
  return s;
}

Status TableCache::HasUserKeyRecords(uint64_t number, uint64_t size,
                                     const Slice& key, bool* exists) {
  *exists = false;
  Cache::Handle* handle = nullptr;
  Status s = FindTable(number, size, &handle);
  if (!s.ok()) return s;
  uint64_t count = 0;
  Table* table = static_cast<TableAndFile*>(cache_->Value(handle))->table;
  s = LocalCountTableAccess::Lookup(table, local_count_cache_, key, &count);
  Status primary = s.ok() ? Status::OK() : table->VerifyPrimaryIndex();
  cache_->Release(handle);
  if (!primary.ok()) return primary;
  if (s.ok()) {
    *exists = count != 0;
    return s;
  }
  ProfileScope::Count("metadata.local_count_fallback");
  ReadOptions read;
  read.verify_checksums = true;
  std::unique_ptr<Iterator> iter(NewIterator(read, number, size));
  InternalKey seek(key, kMaxSequenceNumber, kValueTypeForSeek);
  iter->Seek(seek.Encode());
  const Comparator* cmp =
      static_cast<const InternalKeyComparator*>(options_.comparator)
          ->user_comparator();
  while (iter->Valid()) {
    ParsedInternalKey parsed;
    if (!ParseInternalKey(iter->key(), &parsed))
      return Status::Corruption("invalid internal key in presence lookup");
    if (cmp->Compare(parsed.user_key, key) != 0) break;
    if (parsed.user_key == key) {
      *exists = true;
      ProfileScope::Count("metadata.presence_early_stop");
      return Status::OK();
    }
    iter->Next();
  }
  return iter->status();
}

Status TableCache::GetInternalStats(uint64_t file_number, uint64_t file_size,
                                    TableInternalStats* stats) {
  if (stats == nullptr) {
    return Status::InvalidArgument("table statistics output pointer is null");
  }
  stats->valid = false;
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) return s;
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  s = table->GetInternalStats(stats);
  cache_->Release(handle);
  return s;
}

Status TableCache::GetTTLStats(uint64_t file_number, uint64_t file_size,
                               TableInternalStats* stats) {
  if (stats == nullptr) {
    return Status::InvalidArgument("table statistics output pointer is null");
  }
  stats->valid = false;
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) return s;
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  s = table->GetTTLStats(stats);
  cache_->Release(handle);
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
