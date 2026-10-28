// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <chrono>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/metadata_profile.h"
#include "db/snapshot.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class MetadataIndex;
struct MetadataCommitMeta;
class TableCache;
class Version;
class VersionSet;


class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;

  Status GetCurrentStats(const ReadOptions& options, Stats* stats) override;

  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Force metadata through a checkpoint and wait for metadata maintenance.
  // Intended for benchmarks and tests after foreground writers have stopped.
  Status TEST_WaitForMetadata(uint64_t target_mseq, bool compact,
                              uint64_t timeout_micros);

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status ApplyMetadataCommit(const MetadataCommitMeta& commit)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status ReplayMetadataCommitsBefore(uint64_t mseq)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status SyncMetadataWALPrefix(uint64_t through_log)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status ReserveMetadataMSeq(uint64_t* mseq)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  uint64_t UncheckpointedMetadataWALBytes()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  uint64_t PendingMetadataChangeBytes()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  size_t PendingMetadataChangeFiles()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  const std::vector<MetadataFileMeta>& MetadataLookupFiles()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status LoadMetadataWALFile(uint64_t number, uint64_t mseq,
                             MetadataFileMeta* file)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void BeginMetadataCommit() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void EndMetadataCommit() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status FlushMetadata() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status CompactMetadata() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void MaybeFreezeMetadata() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status RebuildMetadata() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status LogAndApply(VersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base,
                          uint64_t* output_number = nullptr,
                          bool keep_pending_until_install = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  static void MetadataBGWork(void* db);
  void BackgroundMetadataCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  MetadataProfile metadata_profile_;
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;
  MetadataIndex* const metadata_index_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  // Serializes WAL append/sync operations while mutex_ is released. In
  // particular, prefix sync must not race a foreground append to logfile_.
  port::Mutex metadata_wal_io_mutex_;
  std::atomic<bool> shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  port::CondVar metadata_work_signal_ GUARDED_BY(mutex_);
  port::CondVar metadata_worker_finished_signal_ GUARDED_BY(mutex_);
  bool metadata_worker_exited_ GUARDED_BY(mutex_);
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.
  uint64_t metadata_total_entries_ GUARDED_BY(mutex_);
  uint64_t metadata_distinct_keys_ GUARDED_BY(mutex_);
  uint64_t metadata_mseq_ GUARDED_BY(mutex_);
  uint64_t metadata_mseq_allocator_ GUARDED_BY(mutex_);
  uint64_t metadata_recovery_max_mseq_ GUARDED_BY(mutex_);
  uint64_t metadata_wal_bytes_ GUARDED_BY(mutex_);
  uint64_t metadata_wal_checkpoint_bytes_ GUARDED_BY(mutex_);
  struct MetadataWALPosition {
    uint64_t appended_position = 0;
    uint64_t durable_position = 0;
    bool durable_known = false;
  };
  std::map<uint64_t, MetadataWALPosition> metadata_wal_positions_
      GUARDED_BY(mutex_);
  std::set<uint64_t> metadata_wal_sync_pins_ GUARDED_BY(mutex_);
  bool metadata_wal_prefix_sync_in_progress_ GUARDED_BY(mutex_);
  bool metadata_checkpoint_pending_since_valid_ GUARDED_BY(mutex_);
  std::chrono::steady_clock::time_point metadata_checkpoint_pending_since_
      GUARDED_BY(mutex_);
  bool metadata_active_pending_since_valid_ GUARDED_BY(mutex_);
  std::chrono::steady_clock::time_point metadata_active_pending_since_
      GUARDED_BY(mutex_);
  uint64_t metadata_checkpoint_target_mseq_ GUARDED_BY(mutex_);
  std::vector<MetadataFileMeta> metadata_wal_files_ GUARDED_BY(mutex_);
  uint64_t metadata_wal_files_revision_ GUARDED_BY(mutex_);
  uint64_t metadata_lookup_files_version_revision_ GUARDED_BY(mutex_);
  uint64_t metadata_lookup_files_wal_revision_ GUARDED_BY(mutex_);
  bool metadata_lookup_files_cache_valid_ GUARDED_BY(mutex_);
  std::vector<MetadataFileMeta> metadata_lookup_files_cache_
      GUARDED_BY(mutex_);
  size_t recovery_metadata_event_index_ GUARDED_BY(mutex_);
  bool metadata_needs_rebuild_ GUARDED_BY(mutex_);
  bool metadata_rebuild_only_error_ GUARDED_BY(mutex_);
  bool metadata_rebuild_in_progress_ GUARDED_BY(mutex_);
  bool metadata_commit_in_progress_ GUARDED_BY(mutex_);
  size_t metadata_commit_blockers_ GUARDED_BY(mutex_);
  bool metadata_storage_work_in_progress_ GUARDED_BY(mutex_);
  bool metadata_compaction_prepare_in_progress_ GUARDED_BY(mutex_);
  CompactionState* metadata_compaction_prepare_ GUARDED_BY(mutex_);
  bool manifest_commit_in_progress_ GUARDED_BY(mutex_);
  bool write_log_in_progress_ GUARDED_BY(mutex_);

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
