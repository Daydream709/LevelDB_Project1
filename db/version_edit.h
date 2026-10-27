// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
};

enum MetadataFileKind { kMetadataIndex = 1, kMetadataChange = 2 };

struct MetadataFileMeta {
  MetadataFileMeta() : kind(kMetadataIndex), number(0), file_size(0),
                      min_mseq(0), max_mseq(0), level(0) {}
  MetadataFileKind kind;
  uint64_t number;
  uint64_t file_size;
  uint64_t min_mseq;
  uint64_t max_mseq;
  int level;
  std::string smallest;
  std::string largest;
};

struct MetadataCommitMeta {
  uint64_t file_number = 0;
  uint64_t mseq = 0;
  uint64_t prev_mseq = 0;
  uint64_t total_entries = 0;
  uint64_t distinct_keys = 0;
  uint64_t wal_number = 0;
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void AddMetadataFile(const MetadataFileMeta& file) {
    metadata_files_.push_back(file);
  }
  void RemoveMetadataFile(uint64_t number) {
    deleted_metadata_files_.insert(number);
  }
  void AddMetadataCommit(const MetadataCommitMeta& commit) {
    metadata_commits_.push_back(commit);
  }
  void SetMetadataCheckpoint(uint64_t mseq, uint64_t total_entries,
                             uint64_t distinct_keys, uint64_t wal_number) {
    has_metadata_checkpoint_ = true;
    metadata_checkpoint_mseq_ = mseq;
    metadata_total_entries_ = total_entries;
    metadata_distinct_keys_ = distinct_keys;
    metadata_wal_number_ = wal_number;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;
  uint64_t metadata_checkpoint_mseq_;
  uint64_t metadata_total_entries_;
  uint64_t metadata_distinct_keys_;
  uint64_t metadata_wal_number_;
  bool has_metadata_checkpoint_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, FileMetaData>> new_files_;
  std::vector<MetadataFileMeta> metadata_files_;
  std::vector<MetadataCommitMeta> metadata_commits_;
  std::set<uint64_t> deleted_metadata_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
