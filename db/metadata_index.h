// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_METADATA_INDEX_H_
#define STORAGE_LEVELDB_DB_METADATA_INDEX_H_

#include <map>
#include <deque>
#include <string>
#include <vector>

#include "db/version_edit.h"
#include "db/metadata_profile.h"
#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class Env;
class Iterator;

struct MetadataValue {
  uint64_t mseq = 0;
  uint64_t count = 0;  // Zero is a persistent tombstone.
};

struct MetadataKeyLess {
  bool operator()(const std::string& a, const std::string& b) const;
};

using MetadataValueMap =
    std::map<std::string, MetadataValue, MetadataKeyLess>;

// Mutable, bounded metadata generations plus bounded caches for resolved
// per-key values and immutable SSTs. Mutations run under DBImpl's state mutex.
class MetadataIndex {
 public:
  struct FrozenGeneration {
    uint64_t id = 0;
    uint64_t mseq = 0;
    uint64_t total_entries = 0;
    uint64_t distinct_keys = 0;
    uint64_t wal_number = 0;
    uint64_t wal_bytes = 0;
    size_t memory_usage = 0;
    MetadataValueMap values;
  };

  MetadataIndex(Env* env, const std::string& dbname, const Options& options,
               MetadataProfile* profile = nullptr);
  ~MetadataIndex();

  MetadataIndex(const MetadataIndex&) = delete;
  MetadataIndex& operator=(const MetadataIndex&) = delete;

  Status Lookup(const std::vector<MetadataFileMeta>& files, const Slice& key,
                MetadataValue* value, bool* found) const;
  // Looks up sorted, unique raw user keys together. Callers should keep each
  // chunk to at most 256 keys and min(64KiB, metadata_workspace_size / 4)
  // encoded workspace; a single key larger than that budget uses Lookup().
  Status LookupMany(const std::vector<MetadataFileMeta>& files,
                    const std::vector<Slice>& keys,
                    std::vector<MetadataValue>* values,
                    std::vector<bool>* found) const;
  void Apply(const std::vector<std::pair<std::string, MetadataValue>>& values);
  void Invalidate(const Slice& key);
  void InvalidateAll();

  size_t ActiveMemoryUsage() const { return active_memory_usage_; }
  size_t ImmutableMemoryUsage() const;
  bool HasImmutable() const { return !frozen_.empty(); }
  size_t ImmutableCount() const { return frozen_.size(); }
  const MetadataValueMap& active() const { return active_; }
  const FrozenGeneration* OldestImmutable() const;
  bool FreezeActive(uint64_t mseq, uint64_t total_entries,
                    uint64_t distinct_keys, uint64_t wal_number,
                    uint64_t wal_bytes,
                    bool allow_empty);
  bool CompleteImmutable(uint64_t generation_id);
  void ClearActiveThrough(uint64_t mseq);
  void ResetAfterRebuild();

  Iterator* NewIterator(const std::vector<MetadataFileMeta>& files,
                        bool include_active,
                        bool include_immutables = false) const;
  void Evict(uint64_t file_number);
  size_t ApproximateMemoryUsage() const;

  static std::string EncodeValue(const MetadataValue& value);
  static bool DecodeValue(const Slice& encoded, MetadataValue* value);

  // Comparator/filter configuration for metadata table files. The input
  // Options comparator and filter policy are intentionally replaced: metadata
  // keys use exact byte ordering, independent of the primary DB comparator.
  Options TableOptions() const;

 private:
  class MetadataTableCache;

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  MetadataProfile* const profile_;
  MetadataValueMap active_;
  size_t active_memory_usage_;
  std::deque<FrozenGeneration> frozen_;
  uint64_t next_generation_id_;
  uint64_t value_cache_generation_;
  Cache* const value_cache_;
  Cache* const block_cache_;
  Cache* const block_cache_observer_;
  MetadataTableCache* const table_cache_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_METADATA_INDEX_H_
