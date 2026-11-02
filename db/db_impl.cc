// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/metadata_log.h"
#include "db/metadata_index.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

namespace {

const char kMetadataWALMagic[] = "LDBMETA2";
const size_t kMetadataWALMagicSize = sizeof(kMetadataWALMagic) - 1;
constexpr size_t kMaxMetadataWALUpdates = 1U << 24;

struct MetadataWALRecord {
  uint64_t mseq = 0;
  uint64_t prev_mseq = 0;
  bool has_prev_mseq = false;
  bool has_metadata_file = false;
  uint64_t metadata_file_number = 0;
  uint64_t total_entries = 0;
  uint64_t distinct_keys = 0;
  Slice batch;
  std::vector<std::pair<std::string, MetadataValue>> values;
};

bool HasMetadataWALMagic(const Slice& record) {
  return IsMetadataWALRecord(record);
}

std::string EncodeMetadataWALRecord(
    const Slice& batch, uint64_t mseq, uint64_t prev_mseq,
    uint64_t total_entries,
    uint64_t distinct_keys,
    const std::vector<std::pair<std::string, MetadataValue>>& values,
    uint64_t metadata_file_number = 0) {
  static const char kMetadataWALFileMagic[] = "LDBMETA3";
  const bool use_file = metadata_file_number != 0;
  std::string encoded(use_file ? kMetadataWALFileMagic : kMetadataWALMagic,
                      kMetadataWALMagicSize);
  PutFixed64(&encoded, mseq);
  PutFixed64(&encoded, prev_mseq);
  PutFixed64(&encoded, total_entries);
  PutFixed64(&encoded, distinct_keys);
  PutLengthPrefixedSlice(&encoded, batch);
  if (use_file) {
    encoded.push_back(1);  // Metadata values are in a synced .mchg file.
    PutFixed64(&encoded, metadata_file_number);
    return encoded;
  }
  PutVarint32(&encoded, static_cast<uint32_t>(values.size()));
  for (const auto& entry : values) {
    PutLengthPrefixedSlice(&encoded, entry.first);
    PutFixed64(&encoded, entry.second.count);
  }
  return encoded;
}

Status DecodeMetadataWALRecord(const Slice& record, MetadataWALRecord* out) {
  if (!HasMetadataWALMagic(record)) {
    return Status::InvalidArgument("legacy write batch");
  }
  Slice input = record;
  const unsigned version = MetadataWALVersion(record);
  input.remove_prefix(kMetadataWALMagicSize);
  const size_t header_size = version >= 2 ? 32 : 24;
  if (input.size() < header_size) {
    return Status::Corruption("metadata WAL header truncated");
  }
  out->mseq = DecodeFixed64(input.data());
  if (version >= 2) {
    out->prev_mseq = DecodeFixed64(input.data() + 8);
    out->total_entries = DecodeFixed64(input.data() + 16);
    out->distinct_keys = DecodeFixed64(input.data() + 24);
    out->has_prev_mseq = true;
  } else {
    out->total_entries = DecodeFixed64(input.data() + 8);
    out->distinct_keys = DecodeFixed64(input.data() + 16);
    out->has_prev_mseq = false;
  }
  input.remove_prefix(header_size);
  if (!GetLengthPrefixedSlice(&input, &out->batch)) {
    return Status::Corruption("metadata WAL batch truncated");
  }
  if (version == 3) {
    if (input.empty()) return Status::Corruption("metadata WAL mode missing");
    const unsigned char mode = static_cast<unsigned char>(input[0]);
    input.remove_prefix(1);
    if (mode == 1) {
      if (input.size() != 8) {
        return Status::Corruption("metadata WAL file reference truncated");
      }
      out->has_metadata_file = true;
      out->metadata_file_number = DecodeFixed64(input.data());
      if (out->metadata_file_number == 0) {
        return Status::Corruption("metadata WAL file number is zero");
      }
      input.remove_prefix(8);
      return input.empty() ? Status::OK()
                           : Status::Corruption("metadata WAL trailing bytes");
    }
    if (mode != 0) return Status::Corruption("metadata WAL mode invalid");
  }
  uint32_t count;
  if (!GetVarint32(&input, &count) || count > kMaxMetadataWALUpdates) {
    return Status::Corruption("metadata WAL update count");
  }
  out->values.clear();
  out->values.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    Slice key;
    uint64_t value;
    if (!GetLengthPrefixedSlice(&input, &key) || input.size() < 8) {
      return Status::Corruption("metadata WAL update truncated");
    }
    value = DecodeFixed64(input.data());
    input.remove_prefix(8);
    MetadataValue metadata_value;
    metadata_value.mseq = out->mseq;
    metadata_value.count = value;
    out->values.emplace_back(key.ToString(), metadata_value);
  }
  if (!input.empty()) return Status::Corruption("metadata WAL trailing bytes");
  return Status::OK();
}

class BatchMetadataCounter final : public WriteBatch::Handler {
 public:
  using SpillCallback = std::function<Status(const MetadataValueMap&)>;

  BatchMetadataCounter(size_t budget, SpillCallback spill)
      : budget_(std::max<size_t>(1, budget)),
        spill_(std::move(spill)),
        memory_usage_(0),
        total_records_(0),
        count_overflow_(false) {}

  void Put(const Slice& key, const Slice&) override { Increment(key); }
  void Delete(const Slice& key) override { Increment(key); }

  void Increment(const Slice& key) {
    if (!status_.ok()) return;
    if (total_records_ == std::numeric_limits<uint64_t>::max()) {
      count_overflow_ = true;
      return;
    }
    ++total_records_;
    auto iter = counts.find(key.ToString());
    if (iter == counts.end()) {
      const size_t entry_charge = key.size() + sizeof(uint64_t) + 64;
      if (!counts.empty() &&
          (counts.size() >= kMaxMetadataWALUpdates ||
           entry_charge > budget_ - std::min(budget_, memory_usage_))) {
        status_ = SpillNow();
        if (!status_.ok()) return;
      }
      iter = counts.emplace(key.ToString(), MetadataValue()).first;
      memory_usage_ += key.size() + sizeof(uint64_t) + 64;
    }
    if (iter->second.count == std::numeric_limits<uint64_t>::max()) {
      count_overflow_ = true;
      return;
    }
    ++iter->second.count;
    if (memory_usage_ >= budget_) status_ = SpillNow();
  }

  Status SpillNow() {
    if (counts.empty()) return Status::OK();
    Status s = spill_(counts);
    if (s.ok()) {
      counts.clear();
      memory_usage_ = 0;
      spilled_ = true;
    }
    return s;
  }

  bool spilled() const { return spilled_; }
  const Status& status() const { return status_; }
  uint64_t total_records() const { return total_records_; }
  bool count_overflow() const { return count_overflow_; }

  MetadataValueMap counts;

 private:
  const size_t budget_;
  const SpillCallback spill_;
  size_t memory_usage_;
  uint64_t total_records_;
  bool count_overflow_;
  bool spilled_ = false;
  Status status_;
};

struct PrimaryMetadataDelta {
  int64_t net_records = 0;
  uint64_t output_records = 0;
};

std::string EncodeMetadataDelta(int64_t delta, uint64_t output_records) {
  MetadataValue value;
  value.mseq = output_records;
  std::memcpy(&value.count, &delta, sizeof(delta));
  return MetadataIndex::EncodeValue(value);
}

bool DecodeMetadataDelta(const Slice& encoded, int64_t* delta,
                         uint64_t* output_records) {
  MetadataValue value;
  if (!MetadataIndex::DecodeValue(encoded, &value)) {
    return false;
  }
  std::memcpy(delta, &value.count, sizeof(*delta));
  *output_records = value.mseq;
  return true;
}

bool MetadataKeyInRange(const Slice& key, const std::string& smallest,
                       const std::string& largest) {
  return BytewiseComparator()->Compare(key, smallest) >= 0 &&
         BytewiseComparator()->Compare(key, largest) <= 0;
}

bool MetadataRangesOverlap(const std::string& smallest_a,
                           const std::string& largest_a,
                           const std::string& smallest_b,
                           const std::string& largest_b) {
  return BytewiseComparator()->Compare(smallest_a, largest_b) <= 0 &&
         BytewiseComparator()->Compare(smallest_b, largest_a) <= 0;
}

}  // namespace

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        smallest_snapshot_time_micros(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;
  VersionEdit* replacement_edit = nullptr;
  const char* profiling_trigger = "size";

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;
  uint64_t smallest_snapshot_time_micros;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
  std::map<std::string, PrimaryMetadataDelta, MetadataKeyLess> metadata_deltas;
  size_t metadata_delta_bytes = 0;
  std::vector<MetadataFileMeta> metadata_delta_runs;
  std::vector<uint64_t> metadata_delta_output_numbers;
  bool has_metadata_delta_range = false;
  std::string metadata_delta_smallest;
  std::string metadata_delta_largest;
  bool metadata_journal_finishing = false;
  bool metadata_journal_pressure = false;
  size_t metadata_journal_bytes = 0;
  MetadataValueMap metadata_journal_values;
  std::vector<MetadataFileMeta> metadata_journal_files;
  uint64_t metadata_baseline_mseq = 0;
  uint64_t metadata_correction_mseq = 0;
  bool has_prepared_baseline = false;
  MetadataFileMeta prepared_baseline;
  bool has_prepared_correction = false;
  MetadataFileMeta prepared_correction;
  uint64_t delta_positive_records = 0;
  uint64_t delta_negative_records = 0;
  bool has_metadata_changes = false;
  std::string first_metadata_delta_key;
  uint64_t first_metadata_after_count = 0;
  bool has_first_metadata_delta = false;
};

namespace {
class ValidateBatch : public WriteBatch::Handler {
 public:
  void Put(const Slice&, const Slice&) override {}
  void Delete(const Slice&) override {}
};
}  // namespace

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  ClipToRange(&result.metadata_memtable_size, size_t{64 << 10},
              size_t{1} << 30);
  ClipToRange(&result.metadata_cache_size, size_t{64 << 10},
              size_t{1} << 30);
  ClipToRange(&result.metadata_workspace_size, size_t{64 << 10},
              size_t{1} << 30);
  ClipToRange(&result.metadata_wal_size_limit, uint64_t{64 << 10},
              uint64_t{1} << 30);
  ClipToRange(&result.metadata_checkpoint_interval_micros, uint64_t{1000},
              uint64_t{60} * 1000 * 1000);
  ClipToRange(&result.metadata_profile_sample_rate, size_t{1},
              size_t{1} << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

namespace {
Options WithProfilingEnv(const Options& src, Env* env) {
  Options result = src;
  result.env = env;
  return result;
}
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname,
               uint64_t profiling_id)
    : profiler_(raw_options.profiler),
      profiling_id_(profiler_ ? (profiling_id ? profiling_id : NewProfileDatabaseID()) : 0),
      profiling_env_(profiler_ ? NewProfilingEnv(raw_options.env, profiling_id_) : nullptr),
      profiling_mutex_observer_(profiling_id_),
      env_(profiling_env_ ? profiling_env_.get() : raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, WithProfilingEnv(raw_options, env_))),
      metadata_profile_(options_.enable_metadata_profiling,
                        options_.metadata_profile_sample_rate),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      metadata_index_(new MetadataIndex(
          env_, dbname_, options_,
          options_.enable_metadata_profiling ? &metadata_profile_ : nullptr)),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      metadata_work_signal_(&mutex_),
      metadata_worker_finished_signal_(&mutex_),
      metadata_worker_exited_(true),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      metadata_total_entries_(0),
      metadata_distinct_keys_(0),
      published_stats_compact_snapshot_(kCompactStatsFallback),
      published_stats_available_(false),
      published_stats_version_(0),
      published_stats_packed_counts_(0),
      published_stats_sequence_(0),
      metadata_mseq_(0),
      metadata_mseq_allocator_(0),
      metadata_recovery_max_mseq_(0),
      metadata_wal_bytes_(0),
      metadata_wal_checkpoint_bytes_(0),
      metadata_wal_prefix_sync_in_progress_(false),
      metadata_checkpoint_pending_since_valid_(false),
      metadata_active_pending_since_valid_(false),
      metadata_checkpoint_target_mseq_(0),
      metadata_wal_files_revision_(0),
      metadata_lookup_files_version_revision_(0),
      metadata_lookup_files_wal_revision_(0),
      metadata_lookup_files_cache_valid_(false),
      recovery_metadata_event_index_(0),
      metadata_needs_rebuild_(false),
      metadata_rebuild_only_error_(false),
      metadata_rebuild_in_progress_(false),
      metadata_commit_in_progress_(false),
      metadata_commit_blockers_(0),
      metadata_storage_work_in_progress_(false),
      metadata_compaction_prepare_in_progress_(false),
      metadata_compaction_prepare_(nullptr),
      manifest_commit_in_progress_(false),
      write_log_in_progress_(false),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {
  if (profiler_) mutex_.SetObserver(&profiling_mutex_observer_);
}

DBImpl::~DBImpl() {
  ProfileScope profile(profiler_, profiling_id_, "op.close");
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  metadata_work_signal_.SignalAll();
  while (background_compaction_scheduled_ || profiling_callbacks_ != 0) {
    background_work_finished_signal_.Wait();
  }
  while (!metadata_worker_exited_) {
    metadata_worker_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;
  delete metadata_index_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (versions_->metadata_wal_number() != 0 &&
                   number >= versions_->metadata_wal_number()) ||
                  (number == versions_->PrevLogNumber()) ||
                  metadata_wal_sync_pins_.find(number) !=
                      metadata_wal_sync_pins_.end());
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kMetadataFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        } else if (type == kMetadataFile) {
          metadata_index_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  std::vector<uint64_t> removed_logs;
  for (const std::string& filename : files_to_delete) {
    Status remove_status = env_->RemoveFile(dbname_ + "/" + filename);
    if (remove_status.ok()) {
      uint64_t removed_number;
      FileType removed_type;
      if (ParseFileName(filename, &removed_number, &removed_type) &&
          removed_type == kLogFile) {
        removed_logs.push_back(removed_number);
      }
    }
  }
  mutex_.Lock();
  for (uint64_t removed_number : removed_logs) {
    metadata_wal_positions_.erase(removed_number);
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();
  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kRecovery, true);

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  { ProfileScope stage("recovery.manifest"); s = versions_->Recover(save_manifest); stage.SetStatus(s); }
  if (!s.ok()) {
    return s;
  }
  if (versions_->has_metadata_checkpoint() ||
      !versions_->metadata_files().empty() ||
      !versions_->metadata_commits().empty()) {
    return Status::NotSupported(
        "database uses the auxiliary metadata LSM format; this prototype "
        "supports databases created with primary-attached metadata only");
  }
  metadata_needs_rebuild_ = true;
  if (versions_->has_primary_metadata_checkpoint() &&
      versions_->primary_metadata_checkpoint_sequence() <=
          versions_->LastSequence() &&
      versions_->primary_metadata_total_entries() >=
          versions_->primary_metadata_distinct_keys()) {
    metadata_total_entries_ = versions_->primary_metadata_total_entries();
    metadata_distinct_keys_ = versions_->primary_metadata_distinct_keys();
    metadata_needs_rebuild_ = false;

    // A scalar checkpoint is usable only when the attached per-table count
    // metadata it depends on is intact. In paranoid mode, also verify all data
    // blocks before accepting the checkpoint, matching the full-rebuild path's
    // checksum guarantee.
    for (int level = 0; level < config::kNumLevels &&
                        !metadata_needs_rebuild_;
         ++level) {
      for (FileMetaData* file : versions_->current()->files(level)) {
        TableInternalStats table_stats;
        Status table_status = table_cache_->GetInternalStats(
            file->number, file->file_size, &table_stats);
        if (!table_status.ok() || !table_stats.valid) {
          metadata_needs_rebuild_ = true;
          break;
        }
        if (options_.paranoid_checks) {
          ReadOptions verify_options;
          verify_options.verify_checksums = true;
          Iterator* table = table_cache_->NewIterator(
              verify_options, file->number, file->file_size);
          table->SeekToFirst();
          while (table->Valid()) table->Next();
          table_status = table->status();
          delete table;
          if (!table_status.ok()) {
            metadata_needs_rebuild_ = true;
            break;
          }
        }
      }
    }
  }
  recovery_metadata_event_index_ = 0;
  metadata_recovery_max_mseq_ = metadata_mseq_;
  for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
    metadata_recovery_max_mseq_ =
        std::max(metadata_recovery_max_mseq_, commit.mseq);
  }

  // Check manifest references and the table boundaries without scanning every
  // metadata key. Full point reads verify the relevant data blocks on demand.
  const std::vector<MetadataFileMeta>& metadata_files =
      versions_->metadata_files();
  for (const MetadataFileMeta& metadata_file : metadata_files) {
    const std::string metadata_path =
        metadata_file.kind == kMetadataChange
            ? MetadataChangeFileName(dbname_, metadata_file.number)
            : MetadataFileName(dbname_, metadata_file.number);
    if (!env_->FileExists(metadata_path)) {
      metadata_needs_rebuild_ = true;
      continue;
    }
    Iterator* check = metadata_index_->NewIterator({metadata_file}, false);
    check->SeekToFirst();
    if (check->Valid()) {
      MetadataValue value;
      if (!MetadataIndex::DecodeValue(check->value(), &value)) {
        metadata_needs_rebuild_ = true;
      }
      check->SeekToLast();
      if (check->Valid() &&
          !MetadataIndex::DecodeValue(check->value(), &value)) {
        metadata_needs_rebuild_ = true;
      }
    }
    if (!check->status().ok()) metadata_needs_rebuild_ = true;
    delete check;
  }
  for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
    bool referenced = false;
    for (const MetadataFileMeta& file : metadata_files) {
      if (file.number == commit.file_number &&
          file.min_mseq <= commit.mseq && file.max_mseq == commit.mseq) {
        referenced = true;
        break;
      }
    }
    if (!referenced || commit.mseq <= metadata_mseq_) {
      metadata_needs_rebuild_ = true;
    }
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  const uint64_t metadata_min_log = versions_->metadata_wal_number();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveTableFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile) {
        const bool retained =
            (number >= min_log) || (number == prev_log) ||
            (metadata_min_log != 0 && number >= metadata_min_log);
        if (retained) {
          // Recovery cannot infer which bytes reached stable storage. The
          // first dependent checkpoint therefore syncs every recovered log.
          MetadataWALPosition position;
          position.appended_position = 1;
          position.durable_known = false;
          metadata_wal_positions_[number] = position;
          logs.push_back(number);
        }
      }
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  // Manifest compaction events can be newer than every retained WAL. Apply
  // the remaining tail only after all earlier WAL transactions were merged by
  // mseq. A broken chain is repaired from the recovered primary state below.
  s = ReplayMetadataCommitsBefore(std::numeric_limits<uint64_t>::max());
  if (!s.ok()) metadata_needs_rebuild_ = true;

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }
  if (!versions_->has_primary_metadata_checkpoint() ||
      versions_->primary_metadata_checkpoint_sequence() !=
          versions_->LastSequence() ||
      versions_->primary_metadata_total_entries() <
          versions_->primary_metadata_distinct_keys()) {
    metadata_needs_rebuild_ = true;
  }

  return Status::OK();
}

Status DBImpl::ApplyMetadataCommit(const MetadataCommitMeta& commit) {
  mutex_.AssertHeld();
  if (metadata_needs_rebuild_) return Status::OK();
  if (commit.prev_mseq != metadata_mseq_ || commit.mseq <= commit.prev_mseq ||
      commit.total_entries < commit.distinct_keys) {
    metadata_needs_rebuild_ = true;
    return Status::Corruption("metadata Manifest event chain mismatch");
  }
  const MetadataFileMeta* committed_file = nullptr;
  for (const MetadataFileMeta& file : versions_->metadata_files()) {
    if (file.number == commit.file_number &&
        file.min_mseq <= commit.mseq && file.max_mseq == commit.mseq) {
      committed_file = &file;
      break;
    }
  }
  const std::string committed_filename =
      committed_file == nullptr
          ? std::string()
          : committed_file->kind == kMetadataChange
                ? MetadataChangeFileName(dbname_, commit.file_number)
                : MetadataFileName(dbname_, commit.file_number);
  if (committed_file == nullptr || !env_->FileExists(committed_filename)) {
    metadata_needs_rebuild_ = true;
    return Status::Corruption("metadata Manifest event file is missing");
  }
  metadata_mseq_ = commit.mseq;
  metadata_total_entries_ = commit.total_entries;
  metadata_distinct_keys_ = commit.distinct_keys;
  return Status::OK();
}

Status DBImpl::ReplayMetadataCommitsBefore(uint64_t mseq) {
  mutex_.AssertHeld();
  const std::vector<MetadataCommitMeta>& commits = versions_->metadata_commits();
  while (recovery_metadata_event_index_ < commits.size()) {
    const MetadataCommitMeta& commit =
        commits[recovery_metadata_event_index_];
    if (commit.mseq >= mseq && mseq != std::numeric_limits<uint64_t>::max()) {
      break;
    }
    Status s = ApplyMetadataCommit(commit);
    ++recovery_metadata_event_index_;
    if (!s.ok()) return s;
  }
  return Status::OK();
}

Status DBImpl::SyncMetadataWALPrefix(uint64_t through_log) {
  mutex_.AssertHeld();
  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kWalSync);
  while (write_log_in_progress_) {
    background_work_finished_signal_.Wait();
  }
  uint64_t first_log = versions_->metadata_wal_number();
  if (first_log == 0) first_log = versions_->LogNumber();
  if (first_log == 0 || through_log < first_log) return Status::OK();

  while (metadata_wal_prefix_sync_in_progress_) {
    background_work_finished_signal_.Wait();
  }

  std::vector<std::pair<uint64_t, MetadataWALPosition>> dirty_logs;
  for (auto iter = metadata_wal_positions_.lower_bound(first_log);
       iter != metadata_wal_positions_.end() && iter->first <= through_log;
       ++iter) {
    const MetadataWALPosition& position = iter->second;
    if (!position.durable_known ||
        position.appended_position > position.durable_position) {
      dirty_logs.push_back(*iter);
    }
  }
  if (dirty_logs.empty()) {
    metadata_profile_.Count(MetadataProfileCounter::kWalSyncSkipped);
    return Status::OK();
  }

  const uint64_t active_log_number = logfile_number_;
  WritableFile* const active_log = logfile_;
  metadata_wal_prefix_sync_in_progress_ = true;
  for (const auto& log : dirty_logs) {
    metadata_wal_sync_pins_.insert(log.first);
  }
  mutex_.Unlock();
  MetadataProfile::Timer wal_io_wait_timer(
      &metadata_profile_, MetadataProfileStage::kWalIoWait);
  metadata_wal_io_mutex_.Lock();
  wal_io_wait_timer.Stop();
  Status s;
  for (const auto& log : dirty_logs) {
    if (!s.ok()) break;
    const uint64_t number = log.first;
    metadata_profile_.Count(MetadataProfileCounter::kWalSyncs);
    if (number == active_log_number && active_log != nullptr) {
      s = active_log->Sync();
      continue;
    }
    WritableFile* file = nullptr;
    s = env_->NewAppendableFile(LogFileName(dbname_, number), &file);
    if (s.ok()) s = file->Sync();
    if (file != nullptr) {
      Status close_status = file->Close();
      if (s.ok()) s = close_status;
      delete file;
    }
  }
  metadata_wal_io_mutex_.Unlock();
  mutex_.Lock();
  if (s.ok()) {
    for (const auto& log : dirty_logs) {
      MetadataWALPosition& position = metadata_wal_positions_[log.first];
      position.durable_position = log.second.appended_position;
      position.durable_known = true;
    }
  }
  for (const auto& log : dirty_logs) metadata_wal_sync_pins_.erase(log.first);
  metadata_wal_prefix_sync_in_progress_ = false;
  background_work_finished_signal_.SignalAll();
  return s;
}

Status DBImpl::ReserveMetadataMSeq(uint64_t* mseq) {
  mutex_.AssertHeld();
  if (metadata_mseq_allocator_ == std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption("metadata transaction number overflow");
  }
  *mseq = ++metadata_mseq_allocator_;
  return Status::OK();
}

uint64_t DBImpl::UncheckpointedMetadataWALBytes() {
  mutex_.AssertHeld();
  return metadata_wal_bytes_ >= metadata_wal_checkpoint_bytes_
             ? metadata_wal_bytes_ - metadata_wal_checkpoint_bytes_
             : std::numeric_limits<uint64_t>::max();
}

uint64_t DBImpl::PendingMetadataChangeBytes() {
  mutex_.AssertHeld();
  const uint64_t checkpoint = versions_->metadata_checkpoint_mseq();
  std::set<uint64_t> seen;
  uint64_t total = 0;
  auto add_file = [&](const MetadataFileMeta& file) {
    if (file.kind != kMetadataChange || !seen.insert(file.number).second) {
      return;
    }
    if (std::numeric_limits<uint64_t>::max() - total < file.file_size) {
      total = std::numeric_limits<uint64_t>::max();
    } else {
      total += file.file_size;
    }
  };
  for (const MetadataFileMeta& file : metadata_wal_files_) {
    if (file.max_mseq > checkpoint) add_file(file);
  }
  for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
    if (commit.mseq <= checkpoint) continue;
    for (const MetadataFileMeta& file : versions_->metadata_files()) {
      if (file.number == commit.file_number) {
        add_file(file);
        break;
      }
    }
  }
  return total;
}

size_t DBImpl::PendingMetadataChangeFiles() {
  mutex_.AssertHeld();
  const uint64_t checkpoint = versions_->metadata_checkpoint_mseq();
  std::set<uint64_t> seen;
  for (const MetadataFileMeta& file : metadata_wal_files_) {
    if (file.max_mseq > checkpoint) seen.insert(file.number);
  }
  for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
    if (commit.mseq > checkpoint) seen.insert(commit.file_number);
  }
  return seen.size();
}

const std::vector<MetadataFileMeta>& DBImpl::MetadataLookupFiles() {
  mutex_.AssertHeld();
  const uint64_t version_revision = versions_->metadata_file_revision();
  if (metadata_lookup_files_cache_valid_ &&
      metadata_lookup_files_version_revision_ == version_revision &&
      metadata_lookup_files_wal_revision_ == metadata_wal_files_revision_) {
    return metadata_lookup_files_cache_;
  }
  metadata_lookup_files_cache_ = versions_->metadata_files();
  metadata_lookup_files_cache_.insert(metadata_lookup_files_cache_.end(),
                                      metadata_wal_files_.begin(),
                                      metadata_wal_files_.end());
  std::stable_sort(metadata_lookup_files_cache_.begin(),
                   metadata_lookup_files_cache_.end(),
                   [](const MetadataFileMeta& a,
                      const MetadataFileMeta& b) {
                     if (a.max_mseq != b.max_mseq)
                       return a.max_mseq > b.max_mseq;
                     return a.number > b.number;
                   });
  metadata_lookup_files_version_revision_ = version_revision;
  metadata_lookup_files_wal_revision_ = metadata_wal_files_revision_;
  metadata_lookup_files_cache_valid_ = true;
  return metadata_lookup_files_cache_;
}

Status DBImpl::LoadMetadataWALFile(uint64_t number, uint64_t mseq,
                                   MetadataFileMeta* file) {
  mutex_.AssertHeld();
  file->kind = kMetadataChange;
  file->number = number;
  file->min_mseq = mseq;
  file->max_mseq = mseq;
  Status s = env_->GetFileSize(MetadataChangeFileName(dbname_, number),
                               &file->file_size);
  if (!s.ok()) return s;

  Iterator* iter = metadata_index_->NewIterator({*file}, false);
  iter->SeekToFirst();
  if (!iter->Valid()) {
    s = iter->status().ok()
            ? Status::Corruption("empty metadata WAL change file")
            : iter->status();
  } else {
    MetadataValue value;
    if (!MetadataIndex::DecodeValue(iter->value(), &value) ||
        value.mseq != mseq) {
      s = Status::Corruption("invalid metadata WAL change file value");
    } else {
      file->smallest = iter->key().ToString();
      iter->SeekToLast();
      if (!iter->Valid()) {
        s = iter->status().ok()
                ? Status::Corruption("metadata WAL change file is unreadable")
                : iter->status();
      } else if (!MetadataIndex::DecodeValue(iter->value(), &value) ||
                 value.mseq != mseq) {
        s = Status::Corruption("invalid metadata WAL change file boundary");
      } else {
        file->largest = iter->key().ToString();
        s = iter->status();
      }
    }
  }
  delete iter;
  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  ProfileScope profile("recovery.wal", true, "open");
  ProfileScope::Count("recovery.wal_files");
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    bool* metadata_untrusted;
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      *metadata_untrusted = true;
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return profile.Result(status);
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  reporter.metadata_untrusted = &metadata_needs_rebuild_;
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Data and metadata may have different recovery boundaries. Metadata-only
  // log files are replayed for counts without reinserting their user batches.
  const bool replay_data =
      log_number >= versions_->LogNumber() ||
      log_number == versions_->PrevLogNumber();
  const uint64_t metadata_wal_number = versions_->metadata_wal_number();
  const bool replay_metadata =
      metadata_wal_number != 0 && log_number >= metadata_wal_number;

  // Read all the records and add the needed side to its recovery view.
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    MetadataWALRecord metadata_record;
    const bool has_metadata = HasMetadataWALMagic(record);
    if (has_metadata) {
      status = DecodeMetadataWALRecord(record, &metadata_record);
      if (!status.ok()) {
        metadata_needs_rebuild_ = true;
        break;
      }
      metadata_recovery_max_mseq_ =
          std::max(metadata_recovery_max_mseq_, metadata_record.mseq);
      if (replay_metadata &&
          metadata_record.mseq > versions_->metadata_checkpoint_mseq()) {
        if (std::numeric_limits<uint64_t>::max() - metadata_wal_bytes_ <
            record.size()) {
          metadata_needs_rebuild_ = true;
        } else {
          metadata_wal_bytes_ += record.size();
        }
        if (metadata_record.has_metadata_file &&
            !metadata_needs_rebuild_) {
          MetadataFileMeta metadata_file;
          Status file_status = LoadMetadataWALFile(
              metadata_record.metadata_file_number, metadata_record.mseq,
              &metadata_file);
          if (!file_status.ok()) {
            metadata_needs_rebuild_ = true;
          } else {
            metadata_wal_files_.push_back(metadata_file);
            ++metadata_wal_files_revision_;
            pending_outputs_.insert(metadata_file.number);
            versions_->MarkFileNumberUsed(metadata_file.number);
          }
        }
      }
      WriteBatchInternal::SetContents(&batch, metadata_record.batch);
    } else {
      if (replay_metadata ||
          (!versions_->has_metadata_checkpoint() &&
           !versions_->has_primary_metadata_checkpoint())) {
        metadata_needs_rebuild_ = true;
      }
      if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      if (versions_->has_primary_metadata_checkpoint() &&
          WriteBatchInternal::Count(&batch) > 0) {
        const uint64_t batch_last_sequence =
            WriteBatchInternal::Sequence(&batch) +
            WriteBatchInternal::Count(&batch) - 1;
        if (batch_last_sequence >
            versions_->primary_metadata_checkpoint_sequence()) {
          metadata_needs_rebuild_ = true;
        }
      }
    }

    if (has_metadata && replay_metadata && !metadata_needs_rebuild_) {
      status = ReplayMetadataCommitsBefore(metadata_record.mseq);
      if (!status.ok()) {
        metadata_needs_rebuild_ = true;
        status = Status::OK();  // Primary WAL recovery remains authoritative.
      }
      const std::vector<MetadataCommitMeta>& commits =
          versions_->metadata_commits();
      if (recovery_metadata_event_index_ < commits.size() &&
          commits[recovery_metadata_event_index_].mseq ==
              metadata_record.mseq) {
        metadata_needs_rebuild_ = true;
      }
      if (!metadata_needs_rebuild_ &&
          metadata_record.mseq > metadata_mseq_) {
        if ((metadata_record.has_prev_mseq &&
             metadata_record.prev_mseq != metadata_mseq_) ||
            metadata_record.mseq <= metadata_mseq_ ||
            metadata_record.total_entries < metadata_record.distinct_keys) {
          metadata_needs_rebuild_ = true;
        } else {
          if (!metadata_record.has_metadata_file) {
            metadata_index_->Apply(metadata_record.values);
          }
          metadata_mseq_ = metadata_record.mseq;
          metadata_total_entries_ = metadata_record.total_entries;
          metadata_distinct_keys_ = metadata_record.distinct_keys;
        }
      }
    }

    if (replay_data) {
      if (mem == nullptr) {
        mem = new MemTable(internal_comparator_);
        mem->Ref();
      }
      status = WriteBatchInternal::InsertInto(&batch, mem);
      MaybeIgnoreError(&status);
      if (!status.ok()) break;
      if (WriteBatchInternal::Count(&batch) > 0) {
        const SequenceNumber last_seq =
            WriteBatchInternal::Sequence(&batch) +
            WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) *max_sequence = last_seq;
      }

      if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
        compactions++;
        *save_manifest = true;
        status = WriteLevel0Table(mem, edit, nullptr);
        mem->Unref();
        mem = nullptr;
        if (!status.ok()) {
          // Reflect errors immediately so that conditions like full
          // file-systems cause the DB::Open() to fail.
          break;
        }
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  if (status.ok()) {
    metadata_mseq_allocator_ =
        std::max(metadata_mseq_allocator_,
                 std::max(metadata_mseq_, metadata_recovery_max_mseq_));
  }
  return status;
}

Status DBImpl::GetCurrentStats(const ReadOptions& options, Stats* stats) {
  (void)options;  // Statistics describe the current published state.
  if (stats == nullptr) {
    return Status::InvalidArgument("stats must not be null");
  }
  if (profiler_ == nullptr && !metadata_profile_.enabled()) {
    return GetCurrentStatsSnapshot(stats);
  }
  return GetCurrentStatsProfiled(stats);
}

Status DBImpl::GetCurrentStatsProfiled(Stats* stats) {
  ProfileScope profile(profiler_, profiling_id_, "op.stats_query");
  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kStatsQuery);
  metadata_profile_.Count(MetadataProfileCounter::kStatsQueries);
  return GetCurrentStatsSnapshot(stats);
}

Status DBImpl::GetCurrentStatsFallback(Stats* stats) {
  mutex_.Lock();
  while (metadata_rebuild_in_progress_) {
    background_work_finished_signal_.Wait();
  }
  if (metadata_needs_rebuild_) {
    Status s = bg_error_.ok()
                   ? Status::Corruption("metadata statistics need rebuild")
                   : bg_error_;
    mutex_.Unlock();
    return s;
  }
  stats->total_entries = metadata_total_entries_;
  stats->distinct_keys = metadata_distinct_keys_;
  stats->seq = versions_->LastSequence();
  mutex_.Unlock();
  return Status::OK();
}

void DBImpl::UpdatePublishedStatsSnapshot() {
  mutex_.AssertHeld();
  const uint64_t version =
      published_stats_version_.load(std::memory_order_relaxed);
  const uint64_t version_counter = version & kPublishedStatsVersionMask;
  assert((version_counter & 1U) == 0);
  const bool wide_counts =
      metadata_total_entries_ > std::numeric_limits<uint32_t>::max() ||
      metadata_distinct_keys_ > std::numeric_limits<uint32_t>::max();
  const uint64_t wide_flag = wide_counts ? kPublishedStatsWideCounts : 0;
  const uint64_t write_version =
      (version_counter + 1) & kPublishedStatsVersionMask;
  published_stats_version_.store(wide_flag | write_version,
                                 std::memory_order_seq_cst);
  const uint64_t packed_counts =
      wide_counts ? 0
                  : (metadata_distinct_keys_ << 32) | metadata_total_entries_;
  published_stats_packed_counts_.store(packed_counts,
                                       std::memory_order_seq_cst);
  published_stats_sequence_.store(versions_->LastSequence(),
                                  std::memory_order_seq_cst);
  published_stats_version_.store(
      wide_flag | ((write_version + 1) & kPublishedStatsVersionMask),
      std::memory_order_seq_cst);
  UpdatePublishedStatsCompactSnapshot();
}

void DBImpl::UpdatePublishedStatsCompactSnapshot() {
  mutex_.AssertHeld();
  const uint64_t sequence = versions_->LastSequence();
  if (published_stats_available_.load(std::memory_order_relaxed) &&
      metadata_total_entries_ <= kCompactStatsFieldMask &&
      metadata_distinct_keys_ <= kCompactStatsFieldMask &&
      sequence <= kCompactStatsFieldMask) {
    const uint64_t compact_snapshot =
        metadata_total_entries_ |
        (metadata_distinct_keys_ << kCompactStatsFieldBits) |
        (sequence << (2 * kCompactStatsFieldBits));
    published_stats_compact_snapshot_.store(compact_snapshot,
                                            std::memory_order_relaxed);
  } else {
    published_stats_compact_snapshot_.store(kCompactStatsFallback,
                                            std::memory_order_relaxed);
  }
}

void DBImpl::EnablePublishedStatsSnapshot() {
  mutex_.AssertHeld();
  UpdatePublishedStatsSnapshot();
  published_stats_available_.store(true, std::memory_order_release);
  UpdatePublishedStatsCompactSnapshot();
}

Status DBImpl::CountPrimaryRecordsForKey(const Slice& key, uint64_t* count) {
  mutex_.AssertHeld();
  if (count == nullptr) {
    return Status::InvalidArgument("record count output pointer is null");
  }
  uint64_t result = 0;
  if (mem_ != nullptr) result = mem_->CountUserKeyRecords(key);
  if (imm_ != nullptr) {
    const uint64_t immutable_count = imm_->CountUserKeyRecords(key);
    if (std::numeric_limits<uint64_t>::max() - result < immutable_count) {
      return Status::Corruption("user key record count overflow");
    }
    result += immutable_count;
  }
  uint64_t table_count = 0;
  Status s = versions_->current()->CountUserKeyRecords(key, nullptr,
                                                        &table_count);
  if (!s.ok()) return s;
  if (std::numeric_limits<uint64_t>::max() - result < table_count) {
    return Status::Corruption("user key record count overflow");
  }
  *count = result + table_count;
  return Status::OK();
}

Status DBImpl::LogAndApply(VersionEdit* edit) {
  mutex_.AssertHeld();
  while (manifest_commit_in_progress_) {
    background_work_finished_signal_.Wait();
  }
  if (!edit->has_primary_metadata_checkpoint() &&
      !metadata_needs_rebuild_ && imm_ == nullptr &&
      (mem_ == nullptr || mem_->NumEntries() == 0)) {
    edit->SetPrimaryMetadataCheckpoint(
        versions_->LastSequence(), metadata_total_entries_,
        metadata_distinct_keys_);
  }
  if (edit->has_primary_metadata_checkpoint()) {
    Status sync_status = SyncMetadataWALPrefix(logfile_number_);
    if (!sync_status.ok()) return sync_status;
  }
  manifest_commit_in_progress_ = true;
  Status s = versions_->LogAndApply(edit, &mutex_);
  manifest_commit_in_progress_ = false;
  background_work_finished_signal_.SignalAll();
  return s;
}

void DBImpl::BeginMetadataCommit() {
  mutex_.AssertHeld();
  ++metadata_commit_blockers_;
  metadata_commit_in_progress_ = true;
}

void DBImpl::EndMetadataCommit() {
  mutex_.AssertHeld();
  assert(metadata_commit_blockers_ > 0);
  --metadata_commit_blockers_;
  metadata_commit_in_progress_ = metadata_commit_blockers_ != 0;
  background_work_finished_signal_.SignalAll();
  if (!metadata_commit_in_progress_) metadata_work_signal_.Signal();
}

Status DBImpl::FlushMetadata() {
  mutex_.AssertHeld();
  const MetadataIndex::FrozenGeneration* generation =
      metadata_index_->OldestImmutable();
  if (generation == nullptr) return Status::OK();
  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kMetadataFlush, true);
  const uint64_t generation_id = generation->id;
  const uint64_t checkpoint_mseq = generation->mseq;
  const uint64_t checkpoint_total = generation->total_entries;
  const uint64_t checkpoint_distinct = generation->distinct_keys;
  const uint64_t checkpoint_wal = generation->wal_number;
  const uint64_t checkpoint_wal_bytes = generation->wal_bytes;
  std::vector<MetadataFileMeta> checkpoint_files;
  std::set<uint64_t> checkpoint_file_numbers;
  for (const MetadataFileMeta& file : metadata_wal_files_) {
    if (file.max_mseq <= checkpoint_mseq) {
      checkpoint_files.push_back(file);
      checkpoint_file_numbers.insert(file.number);
    }
  }
  // A primary compaction's Manifest transaction may be the only source of
  // metadata changes after the previous checkpoint. Include its committed
  // metadata file when folding the new checkpoint, even if no user write has
  // created a regular WAL generation since then.
  for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
    if (commit.mseq > checkpoint_mseq || commit.mseq > generation->mseq) {
      continue;
    }
    for (const MetadataFileMeta& file : versions_->metadata_files()) {
      if (file.number == commit.file_number &&
          checkpoint_file_numbers.insert(file.number).second) {
        checkpoint_files.push_back(file);
        break;
      }
    }
  }

  // Build and sync the immutable output before taking the foreground write
  // gate. The frozen generation and its source files are immutable; newer
  // writes remain in the active generation and are outside this checkpoint.
  Status s;
  bool metadata_commit_started = false;

  const uint64_t file_number = versions_->NewFileNumber();
  pending_outputs_.insert(file_number);

  // Flush only this immutable generation as a new L0 run. Older runs and
  // checkpointed change files remain queryable until metadata compaction.
  Iterator* input = metadata_index_->NewIterator(checkpoint_files, false, true);
  Options table_options = metadata_index_->TableOptions();
  WritableFile* file = nullptr;
  const std::string filename = MetadataFileName(dbname_, file_number);
  MetadataFileMeta metadata_file;
  metadata_file.kind = kMetadataIndex;
  metadata_file.number = file_number;
  metadata_file.min_mseq = std::numeric_limits<uint64_t>::max();
  metadata_file.max_mseq = 0;
  bool have_key = false;
  uint64_t file_size = 0;

  mutex_.Unlock();
  s = env_->NewWritableFile(filename, &file);
  if (s.ok()) {
    TableBuilder builder(table_options, file);
    input->SeekToFirst();
    while (input->Valid() && s.ok()) {
      const std::string key = input->key().ToString();
      MetadataValue latest;
      if (!MetadataIndex::DecodeValue(input->value(), &latest)) {
        s = Status::Corruption("invalid metadata index value during flush");
        break;
      }
      input->Next();
      while (input->Valid() && input->key() == Slice(key)) {
        MetadataValue candidate;
        if (!MetadataIndex::DecodeValue(input->value(), &candidate)) {
          s = Status::Corruption("invalid metadata index value during merge");
          break;
        }
        if (candidate.mseq == latest.mseq &&
            candidate.count != latest.count) {
          s = Status::Corruption("conflicting metadata values during flush");
          break;
        }
        if (candidate.mseq > latest.mseq) latest = candidate;
        input->Next();
      }
      if (!s.ok()) break;
      if (latest.mseq > checkpoint_mseq) {
        s = Status::Corruption("metadata generation exceeds flush boundary");
        break;
      }
      builder.Add(key, MetadataIndex::EncodeValue(latest));
      if (!have_key) {
        metadata_file.smallest = key;
        have_key = true;
      }
      metadata_file.largest = key;
      metadata_file.min_mseq = std::min(metadata_file.min_mseq, latest.mseq);
      metadata_file.max_mseq = std::max(metadata_file.max_mseq, latest.mseq);
    }
    if (s.ok()) s = input->status();
    if (s.ok()) s = builder.Finish();
    file_size = builder.FileSize();
    if (s.ok()) s = file->Sync();
    if (s.ok()) s = file->Close();
  }
  delete file;
  delete input;
  mutex_.Lock();

  if (s.ok() && have_key) {
    metadata_file.file_size = file_size;
    Iterator* verify = metadata_index_->NewIterator({metadata_file}, false);
    verify->SeekToFirst();
    s = verify->status();
    if (s.ok() && verify->Valid()) {
      MetadataValue value;
      if (!MetadataIndex::DecodeValue(verify->value(), &value)) {
        s = Status::Corruption("metadata table verification failed");
      }
    }
    delete verify;
  }

  if (s.ok()) {
    // Only the final durability and Manifest installation need to exclude
    // commits. This avoids holding up writers while a potentially large SST
    // is merged and written.
    BeginMetadataCommit();
    metadata_commit_started = true;
    s = SyncMetadataWALPrefix(checkpoint_wal);
  }

  if (s.ok()) {
    VersionEdit edit;
    if (have_key) edit.AddMetadataFile(metadata_file);
    edit.SetMetadataCheckpoint(checkpoint_mseq, checkpoint_total,
                               checkpoint_distinct, checkpoint_wal);
    s = LogAndApply(&edit);
    if (s.ok()) {
      metadata_profile_.Count(MetadataProfileCounter::kMetadataFlushes);
      metadata_profile_.Count(MetadataProfileCounter::kMetadataFlushBytes,
                             file_size);
      if (!metadata_index_->CompleteImmutable(generation_id)) {
        s = Status::Corruption("metadata immutable generation changed");
      }
      if (s.ok()) {
        metadata_wal_checkpoint_bytes_ = checkpoint_wal_bytes;
        for (const MetadataFileMeta& checkpoint_file : checkpoint_files) {
          metadata_index_->Evict(checkpoint_file.number);
          pending_outputs_.erase(checkpoint_file.number);
        }
        metadata_wal_files_.erase(
            std::remove_if(metadata_wal_files_.begin(),
                           metadata_wal_files_.end(),
                           [checkpoint_mseq](const MetadataFileMeta& file) {
                             return file.max_mseq <= checkpoint_mseq;
                           }),
            metadata_wal_files_.end());
        ++metadata_wal_files_revision_;
        RemoveObsoleteFiles();
        if (metadata_mseq_ > checkpoint_mseq ||
            UncheckpointedMetadataWALBytes() != 0 ||
            PendingMetadataChangeFiles() != 0) {
          if (metadata_active_pending_since_valid_) {
            metadata_checkpoint_pending_since_ =
                metadata_active_pending_since_;
          }
          metadata_checkpoint_pending_since_valid_ = true;
        } else {
          metadata_checkpoint_pending_since_valid_ = false;
          metadata_active_pending_since_valid_ = false;
        }
        MaybeFreezeMetadata();
      }
    }
  }
  if (metadata_commit_started) EndMetadataCommit();

  pending_outputs_.erase(file_number);
  if (!s.ok()) {
    if (s.IsCorruption()) {
      published_stats_available_.store(false, std::memory_order_release);
      published_stats_compact_snapshot_.store(kCompactStatsFallback,
                                              std::memory_order_relaxed);
      metadata_needs_rebuild_ = true;
      metadata_rebuild_only_error_ = true;
    }
    // Keep the active generation and WAL as the recovery source. The orphaned
    // output is removed by the ordinary obsolete-file pass after a successful
    // retry or on the next open.
    return s;
  }
  return Status::OK();
}

Status DBImpl::CompactMetadata() {
  mutex_.AssertHeld();
  std::set<uint64_t> tail_event_files;
  for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
    tail_event_files.insert(commit.file_number);
  }
  std::vector<MetadataFileMeta> inputs;
  std::vector<MetadataFileMeta> excluded_files;
  const uint64_t checkpoint = versions_->metadata_checkpoint_mseq();
  for (const MetadataFileMeta& file : versions_->metadata_files()) {
    if (file.max_mseq <= checkpoint &&
        tail_event_files.find(file.number) == tail_event_files.end()) {
      inputs.push_back(file);
    } else {
      excluded_files.push_back(file);
    }
  }
  excluded_files.insert(excluded_files.end(), metadata_wal_files_.begin(),
                        metadata_wal_files_.end());
  if (inputs.size() <= 8) return Status::OK();

  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kMetadataCompaction, true);
  uint64_t input_bytes = 0;
  for (const MetadataFileMeta& input : inputs) input_bytes += input.file_size;

  std::vector<uint64_t> output_numbers;
  std::vector<MetadataFileMeta> runs = inputs;
  Options table_options = metadata_index_->TableOptions();
  Status s;

  auto write_merged_run = [&](const std::vector<MetadataFileMeta>& sources,
                              MetadataFileMeta* output_meta,
                              bool prune_tombstones,
                              bool* has_output) -> Status {
    *has_output = false;
    const uint64_t number = versions_->NewFileNumber();
    output_numbers.push_back(number);
    pending_outputs_.insert(number);

    Iterator* input = metadata_index_->NewIterator(sources, false);
    WritableFile* file = nullptr;
    const std::string filename = MetadataFileName(dbname_, number);
    output_meta->kind = kMetadataIndex;
    output_meta->number = number;
    output_meta->level = 1;
    output_meta->min_mseq = std::numeric_limits<uint64_t>::max();
    output_meta->max_mseq = 0;
    bool have_key = false;

    mutex_.Unlock();
    Status run_status = env_->NewWritableFile(filename, &file);
    if (run_status.ok()) {
      TableBuilder builder(table_options, file);
      input->SeekToFirst();
      while (input->Valid() && run_status.ok()) {
        const std::string key = input->key().ToString();
        MetadataValue latest;
        bool have_latest = false;
        while (input->Valid() && input->key() == Slice(key)) {
          MetadataValue candidate;
          if (!MetadataIndex::DecodeValue(input->value(), &candidate)) {
            run_status = Status::Corruption("invalid metadata value in compaction");
            break;
          }
          if (!have_latest || candidate.mseq > latest.mseq) {
            latest = candidate;
            have_latest = true;
          } else if (candidate.mseq == latest.mseq &&
                     candidate.count != latest.count) {
            run_status = Status::Corruption("conflicting metadata versions");
            break;
          }
          input->Next();
        }
        if (!run_status.ok()) break;
        if (!have_latest) continue;
        if (prune_tombstones && latest.count == 0) {
          // A tombstone is removable only if no uncheckpointed file can
          // contain an older value for this key. File ranges make this check
          // conservative without loading the metadata LSM into memory.
          bool may_hide_older_value = false;
          for (const MetadataFileMeta& excluded : excluded_files) {
            if ((!excluded.smallest.empty() &&
                 BytewiseComparator()->Compare(key, excluded.smallest) < 0) ||
                (!excluded.largest.empty() &&
                 BytewiseComparator()->Compare(key, excluded.largest) > 0)) {
              continue;
            }
            may_hide_older_value = true;
            break;
          }
          if (!may_hide_older_value) continue;
        }
        builder.Add(key, MetadataIndex::EncodeValue(latest));
        if (!have_key) {
          output_meta->smallest = key;
          have_key = true;
        }
        output_meta->largest = key;
        output_meta->min_mseq =
            std::min(output_meta->min_mseq, latest.mseq);
        output_meta->max_mseq =
            std::max(output_meta->max_mseq, latest.mseq);
      }
      if (run_status.ok()) run_status = input->status();
      if (run_status.ok()) run_status = builder.Finish();
      output_meta->file_size = builder.FileSize();
      if (run_status.ok()) run_status = file->Sync();
      if (run_status.ok()) run_status = file->Close();
      if (!run_status.ok()) builder.Abandon();
    }
    delete file;
    delete input;
    mutex_.Lock();
    *has_output = have_key && run_status.ok();
    if (run_status.ok() && !have_key && !prune_tombstones) {
      run_status = Status::Corruption("empty metadata compaction output");
    }
    return run_status;
  };

  const size_t kFanIn = 8;
  while (s.ok() && runs.size() > kFanIn) {
    std::vector<MetadataFileMeta> next_runs;
    next_runs.reserve((runs.size() + kFanIn - 1) / kFanIn);
    for (size_t begin = 0; begin < runs.size() && s.ok(); begin += kFanIn) {
      const size_t end = std::min(runs.size(), begin + kFanIn);
      if (end - begin == 1) {
        next_runs.push_back(runs[begin]);
      } else {
        std::vector<MetadataFileMeta> group(runs.begin() + begin,
                                            runs.begin() + end);
        MetadataFileMeta merged;
        bool has_output = false;
        s = write_merged_run(group, &merged, false, &has_output);
        if (s.ok()) next_runs.push_back(merged);
      }
    }
    if (s.ok()) runs.swap(next_runs);
  }

  MetadataFileMeta final_file;
  bool have_final = false;
  bool manifest_attempted = false;
  if (s.ok()) {
    s = write_merged_run(runs, &final_file, true, &have_final);
  }
  if (s.ok()) {
    VersionEdit edit;
    for (const MetadataFileMeta& input : inputs) {
      edit.RemoveMetadataFile(input.number);
    }
    if (have_final) edit.AddMetadataFile(final_file);
    manifest_attempted = true;
    s = LogAndApply(&edit);
  }

  for (uint64_t number : output_numbers) pending_outputs_.erase(number);
  if (s.ok()) {
    for (const MetadataFileMeta& input : inputs) {
      metadata_index_->Evict(input.number);
    }
    metadata_profile_.Count(MetadataProfileCounter::kMetadataCompactions);
    metadata_profile_.Count(
        MetadataProfileCounter::kMetadataCompactionInputBytes, input_bytes);
    metadata_profile_.Count(
        MetadataProfileCounter::kMetadataCompactionOutputBytes,
        have_final ? final_file.file_size : 0);
    RemoveObsoleteFiles();
  } else if (!manifest_attempted) {
    for (uint64_t number : output_numbers) {
      env_->RemoveFile(MetadataFileName(dbname_, number));
    }
  }
  if (!s.ok() && s.IsCorruption()) {
    published_stats_available_.store(false, std::memory_order_release);
    published_stats_compact_snapshot_.store(kCompactStatsFallback,
                                            std::memory_order_relaxed);
    metadata_needs_rebuild_ = true;
    metadata_rebuild_only_error_ = true;
  }
  return s;
}

void DBImpl::MaybeFreezeMetadata() {
  mutex_.AssertHeld();
  const uint64_t checkpoint = versions_->metadata_checkpoint_mseq();
  const bool has_uncheckpointed_manifest_commit = std::any_of(
      versions_->metadata_commits().begin(), versions_->metadata_commits().end(),
      [checkpoint](const MetadataCommitMeta& commit) {
        return commit.mseq > checkpoint;
      });
  const bool has_uncheckpointed_change_file = std::any_of(
      metadata_wal_files_.begin(), metadata_wal_files_.end(),
      [checkpoint](const MetadataFileMeta& file) {
        return file.max_mseq > checkpoint;
      });
  const uint64_t wal_pending_bytes = UncheckpointedMetadataWALBytes();
  const uint64_t change_pending_bytes = PendingMetadataChangeBytes();
  const size_t pending_change_files = PendingMetadataChangeFiles();
  const bool has_pending =
      metadata_mseq_ > checkpoint || wal_pending_bytes != 0 ||
      pending_change_files != 0 || has_uncheckpointed_manifest_commit ||
      has_uncheckpointed_change_file;
  if (!has_pending) {
    metadata_checkpoint_pending_since_valid_ = false;
    metadata_active_pending_since_valid_ = false;
    return;
  }
  if (!metadata_index_->HasImmutable()) {
    if (!metadata_active_pending_since_valid_) {
      metadata_active_pending_since_ = std::chrono::steady_clock::now();
      metadata_active_pending_since_valid_ = true;
    }
  } else {
    const MetadataIndex::FrozenGeneration* frozen =
        metadata_index_->OldestImmutable();
    const bool active_generation_pending =
        metadata_index_->ActiveMemoryUsage() != 0 ||
        metadata_mseq_ > frozen->mseq ||
        metadata_wal_bytes_ > frozen->wal_bytes ||
        std::any_of(metadata_wal_files_.begin(), metadata_wal_files_.end(),
                    [frozen](const MetadataFileMeta& file) {
                      return file.max_mseq > frozen->mseq;
                    });
    if (active_generation_pending && !metadata_active_pending_since_valid_) {
      metadata_active_pending_since_ = std::chrono::steady_clock::now();
      metadata_active_pending_since_valid_ = true;
    }
  }
  if (!metadata_checkpoint_pending_since_valid_) {
    metadata_checkpoint_pending_since_ =
        metadata_active_pending_since_valid_
            ? metadata_active_pending_since_
            : std::chrono::steady_clock::now();
    metadata_checkpoint_pending_since_valid_ = true;
    metadata_work_signal_.Signal();
  }
  if (metadata_index_->HasImmutable()) return;

  const bool memtable_over_budget =
      metadata_index_->ActiveMemoryUsage() != 0 &&
      metadata_index_->ActiveMemoryUsage() >= options_.metadata_memtable_size;
  const bool wal_over_budget =
      wal_pending_bytes >= options_.metadata_wal_size_limit;
  const bool change_bytes_over_budget =
      change_pending_bytes >= options_.metadata_memtable_size;
  const bool too_many_change_files = pending_change_files > 8;
  const bool interval_elapsed =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() -
          metadata_checkpoint_pending_since_)
          .count() >= static_cast<int64_t>(
                         options_.metadata_checkpoint_interval_micros);
  const bool waiter_requested = metadata_checkpoint_target_mseq_ > checkpoint;
  if (!memtable_over_budget && !wal_over_budget &&
      !change_bytes_over_budget && !too_many_change_files &&
      !interval_elapsed && !waiter_requested) {
    // The worker was signaled when this pending generation began. Repeated
    // writes keep the original deadline and do not create wakeup traffic.
    return;
  }

  const bool allow_empty = has_uncheckpointed_manifest_commit ||
                           has_uncheckpointed_change_file ||
                           pending_change_files != 0 || waiter_requested;
  if (metadata_index_->FreezeActive(metadata_mseq_, metadata_total_entries_,
                                    metadata_distinct_keys_, logfile_number_,
                                    metadata_wal_bytes_, allow_empty)) {
    metadata_active_pending_since_valid_ = false;
    if (memtable_over_budget)
      metadata_profile_.Count(
          MetadataProfileCounter::kCheckpointTriggerMemtable);
    if (wal_over_budget)
      metadata_profile_.Count(
          MetadataProfileCounter::kCheckpointTriggerWalBudget);
    if (change_bytes_over_budget)
      metadata_profile_.Count(
          MetadataProfileCounter::kCheckpointTriggerChangeBudget);
    if (too_many_change_files)
      metadata_profile_.Count(
          MetadataProfileCounter::kCheckpointTriggerChangeFiles);
    if (interval_elapsed)
      metadata_profile_.Count(
          MetadataProfileCounter::kCheckpointTriggerInterval);
    if (waiter_requested)
      metadata_profile_.Count(
          MetadataProfileCounter::kCheckpointTriggerWaiter);
    metadata_work_signal_.Signal();
    background_work_finished_signal_.SignalAll();
  }
}

Status DBImpl::RebuildMetadata() {
  mutex_.AssertHeld();
  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kRebuild, true);
  metadata_profile_.Count(MetadataProfileCounter::kRebuilds);

  // Rebuild reads every physical internal record, including shadowed values,
  // tombstones, and expired TTL records. The SST internal-statistics block and
  // MemTable entry counter are attached to the primary data path; this scan is
  // needed only to reconstruct the global raw-byte distinct-key count.
  const Comparator* comparator = user_comparator();
  const bool bytewise = comparator == BytewiseComparator();
  const size_t workspace = std::max<size_t>(1, options_.metadata_workspace_size);
  MetadataValueMap distinct_keys;
  size_t distinct_memory = 0;
  std::vector<MetadataFileMeta> distinct_runs;
  std::vector<uint64_t> scratch_numbers;
  uint64_t total = 0;
  uint64_t distinct = 0;
  std::string previous_key;
  bool have_previous_key = false;
  Status status;

  auto allocate_scratch = [&]() -> uint64_t {
    mutex_.Lock();
    const uint64_t number = versions_->NewFileNumber();
    pending_outputs_.insert(number);
    scratch_numbers.push_back(number);
    mutex_.Unlock();
    return number;
  };
  auto write_distinct_run = [&](MetadataFileMeta* run) -> Status {
    if (distinct_keys.empty()) return Status::OK();
    const uint64_t number = allocate_scratch();
    const std::string filename = MetadataFileName(dbname_, number);
    WritableFile* output = nullptr;
    Status s = env_->NewWritableFile(filename, &output);
    bool have_key = false;
    if (s.ok()) {
      TableBuilder builder(metadata_index_->TableOptions(), output);
      MetadataValue value;
      for (const auto& entry : distinct_keys) {
        builder.Add(entry.first, MetadataIndex::EncodeValue(value));
        if (!have_key) {
          run->smallest = entry.first;
          have_key = true;
        }
        run->largest = entry.first;
      }
      s = builder.Finish();
      run->file_size = builder.FileSize();
      if (s.ok()) s = output->Close();
      if (!s.ok()) builder.Abandon();
    }
    delete output;
    if (s.ok()) {
      run->kind = kMetadataIndex;
      run->number = number;
      distinct_runs.push_back(*run);
      *run = MetadataFileMeta();
      distinct_keys.clear();
      distinct_memory = 0;
    } else {
      env_->RemoveFile(filename);
    }
    return s;
  };
  auto merge_distinct_group = [&](const std::vector<MetadataFileMeta>& sources,
                                  MetadataFileMeta* output_meta,
                                  uint64_t* unique_keys) -> Status {
    const uint64_t number = allocate_scratch();
    const std::string filename = MetadataFileName(dbname_, number);
    Iterator* input = metadata_index_->NewIterator(sources, false);
    WritableFile* output = nullptr;
    Status s = env_->NewWritableFile(filename, &output);
    bool have_key = false;
    if (s.ok()) {
      TableBuilder builder(metadata_index_->TableOptions(), output);
      input->SeekToFirst();
      while (input->Valid() && s.ok()) {
        const std::string key = input->key().ToString();
        MetadataValue value;
        if (unique_keys != nullptr) {
          if (*unique_keys == std::numeric_limits<uint64_t>::max()) {
            s = Status::Corruption("distinct key count overflow during rebuild");
            break;
          }
          ++*unique_keys;
        }
        builder.Add(key, MetadataIndex::EncodeValue(value));
        if (!have_key) {
          output_meta->smallest = key;
          have_key = true;
        }
        output_meta->largest = key;
        while (input->Valid() && input->key() == Slice(key)) input->Next();
      }
      if (s.ok()) s = input->status();
      if (s.ok()) s = builder.Finish();
      output_meta->file_size = builder.FileSize();
      if (s.ok()) s = output->Close();
      if (!s.ok()) builder.Abandon();
    }
    delete output;
    delete input;
    if (s.ok() && have_key) {
      output_meta->kind = kMetadataIndex;
      output_meta->number = number;
      mutex_.Lock();
      for (const MetadataFileMeta& source : sources) {
        metadata_index_->Evict(source.number);
        pending_outputs_.erase(source.number);
      }
      mutex_.Unlock();
      for (const MetadataFileMeta& source : sources) {
        env_->RemoveFile(MetadataFileName(dbname_, source.number));
      }
    } else {
      env_->RemoveFile(filename);
      mutex_.Lock();
      pending_outputs_.erase(number);
      mutex_.Unlock();
    }
    return s;
  };

  mutex_.Unlock();
  ReadOptions rebuild_read_options;
  rebuild_read_options.verify_checksums = true;
  SequenceNumber ignored_sequence;
  uint64_t ignored_read_time;
  uint32_t ignored_seed;
  Iterator* primary = NewInternalIterator(rebuild_read_options,
                                          &ignored_sequence,
                                          &ignored_read_time, &ignored_seed);
  primary->SeekToFirst();
  while (primary->Valid() && status.ok()) {
    if (total == std::numeric_limits<uint64_t>::max()) {
      status = Status::Corruption("total entry count overflow during rebuild");
      break;
    }
    ++total;
    ParsedInternalKey parsed;
    if (!ParseInternalKey(primary->key(), &parsed)) {
      status = Status::Corruption("invalid internal key during metadata rebuild");
      break;
    }
    if (bytewise) {
      if (!have_previous_key ||
          !(Slice(previous_key) == parsed.user_key)) {
        if (distinct == std::numeric_limits<uint64_t>::max()) {
          status = Status::Corruption("distinct key count overflow during rebuild");
          break;
        }
        ++distinct;
        previous_key.assign(parsed.user_key.data(), parsed.user_key.size());
        have_previous_key = true;
      }
    } else {
      const std::string key = parsed.user_key.ToString();
      if (distinct_keys.emplace(key, MetadataValue()).second) {
        const size_t charge = key.size() + sizeof(MetadataValue) + 48;
        distinct_memory = charge > std::numeric_limits<size_t>::max() - distinct_memory
                              ? std::numeric_limits<size_t>::max()
                              : distinct_memory + charge;
        if (distinct_memory >= workspace) {
          MetadataFileMeta run;
          status = write_distinct_run(&run);
        }
      }
    }
    primary->Next();
  }
  if (status.ok()) status = primary->status();
  delete primary;
  if (status.ok() && !bytewise) {
    if (distinct_runs.empty()) {
      distinct = static_cast<uint64_t>(distinct_keys.size());
    } else {
      MetadataFileMeta run;
      status = write_distinct_run(&run);
      while (status.ok() && distinct_runs.size() > 1) {
        std::vector<MetadataFileMeta> next_runs;
        for (size_t begin = 0; begin < distinct_runs.size() && status.ok();
             begin += 8) {
          const size_t end = std::min(distinct_runs.size(), begin + 8);
          if (end - begin == 1) {
            next_runs.push_back(distinct_runs[begin]);
          } else {
            std::vector<MetadataFileMeta> group(distinct_runs.begin() + begin,
                                                distinct_runs.begin() + end);
            MetadataFileMeta merged;
            status = merge_distinct_group(group, &merged, nullptr);
            if (status.ok()) next_runs.push_back(std::move(merged));
          }
        }
        if (status.ok()) distinct_runs.swap(next_runs);
      }
      if (status.ok() && !distinct_runs.empty()) {
        Iterator* unique = metadata_index_->NewIterator(distinct_runs, false);
        unique->SeekToFirst();
        while (unique->Valid() && status.ok()) {
          if (distinct == std::numeric_limits<uint64_t>::max()) {
            status = Status::Corruption("distinct key count overflow during rebuild");
            break;
          }
          ++distinct;
          const std::string key = unique->key().ToString();
          while (unique->Valid() && unique->key() == Slice(key)) unique->Next();
        }
        if (status.ok()) status = unique->status();
        delete unique;
      }
    }
  }

  mutex_.Lock();
  for (uint64_t number : scratch_numbers) {
    metadata_index_->Evict(number);
    pending_outputs_.erase(number);
    env_->RemoveFile(MetadataFileName(dbname_, number));
  }
  if (status.ok()) {
    metadata_total_entries_ = total;
    metadata_distinct_keys_ = distinct;
    metadata_mseq_ = 0;
    metadata_recovery_max_mseq_ = 0;
    metadata_wal_bytes_ = 0;
    metadata_wal_checkpoint_bytes_ = 0;
    metadata_index_->ResetAfterRebuild();
    metadata_needs_rebuild_ = false;
    metadata_rebuild_only_error_ = false;
  }
  return status;
}

Status DBImpl::PersistPrimaryMetadataCheckpoint() {
  mutex_.AssertHeld();
  if (metadata_needs_rebuild_ ||
      metadata_total_entries_ < metadata_distinct_keys_) {
    return Status::Corruption("primary metadata checkpoint is not trusted");
  }
  VersionEdit edit;
  edit.SetPrimaryMetadataCheckpoint(versions_->LastSequence(),
                                    metadata_total_entries_,
                                    metadata_distinct_keys_);
  return LogAndApply(&edit);
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base, uint64_t* output_number,
                                bool keep_pending_until_install) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  if (output_number != nullptr) *output_number = meta.number;
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  if (!keep_pending_until_install || !s.ok() || meta.file_size == 0) {
    pending_outputs_.erase(meta.number);
  }

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  ProfileScope profile("flush", true, "memtable");
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  uint64_t output_number = 0;
  Status s =
      WriteLevel0Table(imm_, &edit, base, &output_number, true);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // A memtable's data WAL is retained until this table is installed. Sync the
  // old log here, on the background thread, before advancing that boundary.
  // Write-buffer rotation itself must not block a caller on fsync.
  if (s.ok()) {
    const uint64_t old_log_number = versions_->LogNumber();
    if (old_log_number != 0 && old_log_number != logfile_number_) {
      s = SyncMetadataWALPrefix(old_log_number);
    }
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    if (!metadata_needs_rebuild_ &&
        (mem_ == nullptr || mem_->NumEntries() == 0)) {
      edit.SetPrimaryMetadataCheckpoint(
          versions_->LastSequence(), metadata_total_entries_,
          metadata_distinct_keys_);
    }
    s = LogAndApply(&edit);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
  // Keep the output table protected while WriteLevel0Table has dropped the
  // mutex and until its VersionEdit is installed. The metadata worker can run
  // RemoveObsoleteFiles concurrently with this primary memtable flush.
  if (output_number != 0) pending_outputs_.erase(output_number);
  profile.SetStatus(s);
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  ProfileScope profile(profiler_, profiling_id_, "op.compact_range");
  // Flush before choosing the levels: this call may create the first SST,
  // and a non-overlapping memtable can be placed directly above level 0.
  Status status = TEST_CompactMemTable();
  if (!status.ok()) {
    MutexLock l(&mutex_);
    RecordBackgroundError(status);
    profile.SetStatus(status);
    return;
  }
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  // Keep the original placement policy for ordinary manual compaction.
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }

  // Lower-level compactions may have moved TTL records into the highest
  // occupied level. Rewrite that level only if expiration is possible, so
  // ordinary files are not unnecessarily promoted. Summaries select work;
  // DoCompactionWork still enforces all snapshot/sequence deletion rules.
  if (max_level_with_files + 1 < config::kNumLevels) {
    bool may_have_expired_ttl = false;
    {
      MutexLock l(&mutex_);
      status = bg_error_;
      const uint64_t now = env_->NowMicros();
      if (status.ok()) {
        for (FileMetaData* file : versions_->current()->files(max_level_with_files)) {
          if ((begin != nullptr &&
               user_comparator()->Compare(file->largest.user_key(), *begin) < 0) ||
              (end != nullptr &&
               user_comparator()->Compare(file->smallest.user_key(), *end) > 0)) {
            continue;
          }
          TableInternalStats stats;
          status = table_cache_->GetTTLStats(file->number, file->file_size, &stats);
          if (!status.ok()) break;
          // A missing/invalid optional summary cannot prove there is no TTL.
          if (!stats.valid ||
              (stats.ttl_records != 0 && stats.min_expire_time_micros <= now)) {
            may_have_expired_ttl = true;
            break;
          }
        }
      }
    }
    if (!status.ok()) {
      MutexLock l(&mutex_);
      RecordBackgroundError(status);
      profile.SetStatus(status);
      return;
    }
    if (may_have_expired_ttl) {
      TEST_CompactRange(max_level_with_files, begin, end);
    }
  }
  status = RewriteBottomLevelRange(begin, end);
  if (!status.ok()) {
    MutexLock l(&mutex_);
    RecordBackgroundError(status);
  }
  profile.SetStatus(status);
}

Status DBImpl::RewriteBottomLevelRange(const Slice* begin, const Slice* end) {
  mutex_.Lock();
  while (background_compaction_scheduled_ && bg_error_.ok() &&
         !shutting_down_.load(std::memory_order_acquire)) {
    background_work_finished_signal_.Wait();
  }
  if (!bg_error_.ok()) {
    Status status = bg_error_;
    mutex_.Unlock();
    return status;
  }
  if (shutting_down_.load(std::memory_order_acquire)) {
    mutex_.Unlock();
    return Status::IOError("DB is shutting down");
  }
  Status status = RewriteBottomLevelRangeLocked(begin, end);
  if (status.ok()) MaybeScheduleCompaction();
  mutex_.Unlock();
  return status;
}

Status DBImpl::RewriteBottomLevelRangeLocked(const Slice* begin,
                                              const Slice* end) {
  ProfileScope profile("compaction.bottom_rewrite", true,
                       background_compaction_scheduled_ ? "ttl" : "manual", config::kNumLevels - 1);
  mutex_.AssertHeld();
  if (!bg_error_.ok()) return bg_error_;
  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::IOError("DB is shutting down");
  }
  bottom_rewrite_in_progress_ = true;
  const int bottom_level = config::kNumLevels - 1;
  Version* base = versions_->current();
  // Bottom-level files have disjoint user-key ranges. Only rewrite files
  // intersecting the requested range; retain boundary records inside those
  // files below. Removing unrelated files would turn a small TTL cleanup into
  // a rewrite of the entire bottom level.
  std::vector<FileMetaData*> files;
  for (FileMetaData* file : base->files(bottom_level)) {
    if (begin != nullptr &&
        user_comparator()->Compare(file->largest.user_key(), *begin) < 0) {
      continue;
    }
    if (end != nullptr &&
        user_comparator()->Compare(file->smallest.user_key(), *end) > 0) {
      continue;
    }
    files.push_back(file);
  }
  if (files.empty()) {
    bottom_rewrite_in_progress_ = false;
    return Status::OK();
  }

  uint64_t profiling_input_bytes = 0;
  for (FileMetaData* file : files) profiling_input_bytes += file->file_size;
  ProfileScope::Count("gc.input_sst_bytes", profiling_input_bytes);
  std::vector<Iterator*> children;
  children.reserve(files.size());
  for (FileMetaData* file : files) {
    children.push_back(table_cache_->NewIterator(ReadOptions(), file->number,
                                                 file->file_size));
  }
  Iterator* input = NewMergingIterator(&internal_comparator_, children.data(),
                                      children.size());
  const bool preserve_snapshots = !snapshots_.empty();
  const uint64_t read_time_micros = env_->NowMicros();

  VersionEdit edit;
  for (FileMetaData* file : files) edit.RemoveFile(bottom_level, file->number);

  FileMetaData output_meta;
  // A completely expired range produces no output file. Keep its sentinel
  // deterministic so verification/cleanup never uses an uninitialized number.
  output_meta.number = 0;
  output_meta.file_size = 0;
  WritableFile* outfile = nullptr;
  TableBuilder* builder = nullptr;
  auto* compact = new CompactionState(nullptr);
  compact->replacement_edit = &edit;
  uint64_t removed_records = 0;
  // Preserve TTL filtering while spilling raw-key deltas within the local
  // metadata workspace budget. Install through the auxiliary LSM transaction.
  auto allocate_delta_file = [&]() {
    const uint64_t number = versions_->NewFileNumber();
    pending_outputs_.insert(number);
    compact->metadata_delta_output_numbers.push_back(number);
    return number;
  };

  auto merge_delta_runs = [&]() -> Status {
    if (compact->metadata_delta_runs.size() < 2) return Status::OK();
    const std::vector<MetadataFileMeta> sources = compact->metadata_delta_runs;
    const uint64_t number = allocate_delta_file();
    Iterator* deltas = metadata_index_->NewIterator(sources, false);
    WritableFile* output = nullptr;
    const std::string filename = MetadataFileName(dbname_, number);
    MetadataFileMeta merged;
    merged.kind = kMetadataIndex;
    merged.number = number;
    bool have_key = false;

    Status merge_status = env_->NewWritableFile(filename, &output);
    if (merge_status.ok()) {
      TableBuilder builder(metadata_index_->TableOptions(), output);
      deltas->SeekToFirst();
      while (deltas->Valid() && merge_status.ok()) {
        const std::string key = deltas->key().ToString();
        int64_t sum = 0;
        uint64_t output_records_sum = 0;
        while (deltas->Valid() && deltas->key() == Slice(key)) {
          int64_t delta;
          uint64_t output_records;
          if (!DecodeMetadataDelta(deltas->value(), &delta, &output_records) ||
              (delta > 0 && sum > std::numeric_limits<int64_t>::max() - delta) ||
              (delta < 0 && sum < std::numeric_limits<int64_t>::min() - delta)) {
            merge_status =
                Status::Corruption("metadata compaction delta merge overflow");
            break;
          }
          sum += delta;
          if (std::numeric_limits<uint64_t>::max() - output_records_sum <
              output_records) {
            merge_status = Status::Corruption(
                "metadata compaction output count overflow");
            break;
          }
          output_records_sum += output_records;
          deltas->Next();
        }
        if (!merge_status.ok()) break;
        if (sum == 0) continue;
        builder.Add(key, EncodeMetadataDelta(sum, output_records_sum));
        if (!have_key) {
          merged.smallest = key;
          have_key = true;
        }
        merged.largest = key;
      }
      if (merge_status.ok()) merge_status = deltas->status();
      if (merge_status.ok()) merge_status = builder.Finish();
      merged.file_size = builder.FileSize();
      if (merge_status.ok()) merge_status = output->Close();
      if (!merge_status.ok()) builder.Abandon();
    }
    delete output;
    delete deltas;

    for (const MetadataFileMeta& source : sources) {
      metadata_index_->Evict(source.number);
      pending_outputs_.erase(source.number);
    }
    compact->metadata_delta_runs.clear();
    if (merge_status.ok() && have_key) {
      compact->metadata_delta_runs.push_back(merged);
    } else {
      pending_outputs_.erase(number);
    }
    for (const MetadataFileMeta& source : sources) {
      env_->RemoveFile(MetadataFileName(dbname_, source.number));
    }
    if (!have_key) env_->RemoveFile(filename);
    return merge_status;
  };

  auto spill_metadata_deltas = [&]() -> Status {
    if (compact->metadata_deltas.empty()) return Status::OK();
    const uint64_t number = allocate_delta_file();
    WritableFile* output = nullptr;
    const std::string filename = MetadataFileName(dbname_, number);
    MetadataFileMeta run;
    run.kind = kMetadataIndex;
    run.number = number;
    bool have_key = false;
    Status spill_status = env_->NewWritableFile(filename, &output);
    if (spill_status.ok()) {
      TableBuilder builder(metadata_index_->TableOptions(), output);
      for (const auto& entry : compact->metadata_deltas) {
        if (entry.second.net_records == 0) continue;
        builder.Add(entry.first,
                    EncodeMetadataDelta(entry.second.net_records,
                                        entry.second.output_records));
        if (!have_key) {
          run.smallest = entry.first;
          have_key = true;
        }
        run.largest = entry.first;
      }
      spill_status = builder.Finish();
      run.file_size = builder.FileSize();
      if (spill_status.ok()) spill_status = output->Close();
      if (!spill_status.ok()) builder.Abandon();
    }
    delete output;
    if (!spill_status.ok()) return spill_status;

    compact->metadata_deltas.clear();
    compact->metadata_delta_bytes = 0;
    if (have_key) compact->metadata_delta_runs.push_back(run);
    else {
        pending_outputs_.erase(number);
        env_->RemoveFile(filename);
    }
    if (compact->metadata_delta_runs.size() >= 8) {
      return merge_delta_runs();
    }
    return Status::OK();
  };

  auto add_metadata_delta = [&](const std::string& key, int64_t adjustment,
                                uint64_t output_adjustment) -> Status {
    auto iter = compact->metadata_deltas.find(key);
    if (iter == compact->metadata_deltas.end()) {
      compact->metadata_delta_bytes += key.size() + sizeof(PrimaryMetadataDelta) + 64;
      iter = compact->metadata_deltas.emplace(key, PrimaryMetadataDelta()).first;
    }
    if ((adjustment > 0 &&
         iter->second.net_records >
             std::numeric_limits<int64_t>::max() - adjustment) ||
        (adjustment < 0 &&
         iter->second.net_records <
             std::numeric_limits<int64_t>::min() - adjustment) ||
        std::numeric_limits<uint64_t>::max() -
                iter->second.output_records <
            output_adjustment) {
      return Status::Corruption("metadata compaction delta overflow");
    }
    iter->second.net_records += adjustment;
    iter->second.output_records += output_adjustment;
    if (compact->metadata_delta_bytes >= options_.metadata_workspace_size) {
      return spill_metadata_deltas();
    }
    return Status::OK();
  };


  std::string current_user_key;
  bool have_user_key = false;
  bool current_key_in_range = false;
  bool keep_current_key = false;
  Status status = Status::OK();

  input->SeekToFirst();
  while (input->Valid()) {
    const Slice internal_key = input->key();
    ParsedInternalKey parsed;
    if (!ParseInternalKey(internal_key, &parsed)) {
      status = Status::Corruption(
          "invalid internal key in bottom-level rewrite");
      break;
    }
    ProfileScope filtering("gc.iterate_filter");
    ProfileScope::Count("gc.records_checked");
    status = add_metadata_delta(parsed.user_key.ToString(), -1, 0);
    if (!status.ok()) break;

    const bool new_user_key =
        !have_user_key ||
        user_comparator()->Compare(parsed.user_key, Slice(current_user_key)) !=
            0;
    if (new_user_key) {
      current_user_key.assign(parsed.user_key.data(), parsed.user_key.size());
      have_user_key = true;
      current_key_in_range =
          (begin == nullptr ||
           user_comparator()->Compare(parsed.user_key, *begin) >= 0) &&
          (end == nullptr ||
           user_comparator()->Compare(parsed.user_key, *end) <= 0);
      keep_current_key = true;
      if (!preserve_snapshots && current_key_in_range) {
        bool has_higher_level_data = false;
        for (int level = 0; level < bottom_level; ++level) {
          if (base->OverlapInLevel(level, &parsed.user_key, &parsed.user_key)) {
            has_higher_level_data = true;
            break;
          }
        }
        if (parsed.type == kTypeDeletion && !has_higher_level_data) {
          keep_current_key = false;
        } else if (parsed.type == kTypeValueWithTTL &&
                   !has_higher_level_data) {
          const Slice raw_value = input->value();
          if (raw_value.size() < 8) {
            status = Status::Corruption(
                "TTL value is shorter than its deadline during rewrite");
            break;
          }
          keep_current_key =
              read_time_micros < DecodeFixed64(raw_value.data());
        }
      }
    }

    const bool keep =
        !current_key_in_range || preserve_snapshots || keep_current_key;
    if (keep) {
      if (preserve_snapshots) ProfileScope::Count("gc.retained_snapshot_records");
      if (builder == nullptr) {
        output_meta.number = versions_->NewFileNumber();
        pending_outputs_.insert(output_meta.number);
        status = env_->NewWritableFile(
            TableFileName(dbname_, output_meta.number), &outfile);
        if (!status.ok()) break;
        builder = new TableBuilder(options_, outfile);
        builder->EnableInternalStatsMetadata();
      }
      if (builder->NumEntries() == 0) {
        output_meta.smallest.DecodeFrom(internal_key);
      }
      output_meta.largest.DecodeFrom(internal_key);
      { ProfileScope emit("gc.output_add"); builder->Add(internal_key, input->value()); }
      ProfileScope::Count("gc.records_output");
      status = add_metadata_delta(parsed.user_key.ToString(), 1, 1);
      if (!status.ok()) break;
    }
    if (!keep) {
      ++removed_records;
      ProfileScope::Count("gc.records_dropped");
      ProfileScope::Count("gc.dropped_encoded_bytes", internal_key.size() + input->value().size());
      if (new_user_key && parsed.type == kTypeValueWithTTL) ProfileScope::Count("gc.dropped_expired_ttl");
      else if (new_user_key && parsed.type == kTypeDeletion) ProfileScope::Count("gc.dropped_tombstone");
      else ProfileScope::Count("gc.dropped_hidden_version");
    }
    input->Next();
  }
  if (status.ok()) status = input->status();
  delete input;

  if (builder != nullptr) {
    if (status.ok()) {
      status = ProfileStatus("sst.finish", [&] { return builder->Finish(); });
    } else {
      builder->Abandon();
    }
    output_meta.file_size = builder->FileSize();
    ProfileScope::Count("gc.generated_output_sst_bytes", output_meta.file_size);
    delete builder;
    builder = nullptr;
  }
  if (outfile != nullptr) {
    if (status.ok()) status = ProfileStatus("sst.sync", [&] { return outfile->Sync(); });
    if (status.ok()) status = outfile->Close();
    delete outfile;
    outfile = nullptr;
  }
  if (status.ok() && output_meta.number != 0) {
    Iterator* verify = table_cache_->NewIterator(
        ReadOptions(), output_meta.number, output_meta.file_size);
    status = verify->status();
    delete verify;
    if (status.ok()) {
      edit.AddFile(bottom_level, output_meta.number, output_meta.file_size,
                   output_meta.smallest, output_meta.largest);
    }
  }

  if (status.ok()) status = spill_metadata_deltas();
  bool installation_attempted = false;
  if (status.ok()) {
    installation_attempted = true;
    { ProfileScope stage("compaction.install_manifest");
      status = InstallCompactionResults(compact); stage.SetStatus(status); }
    if (status.ok()) {
      ProfileScope::Count("gc.installed_input_sst_bytes", profiling_input_bytes);
      ProfileScope::Count("gc.installed_output_sst_bytes", output_meta.file_size);
      ProfileScope::Count("gc.installed_removed_records", removed_records);
    }
  }
  if (output_meta.number != 0) {
    pending_outputs_.erase(output_meta.number);
    if (!status.ok() && !installation_attempted) {
      table_cache_->Evict(output_meta.number);
      env_->RemoveFile(TableFileName(dbname_, output_meta.number));
    }
  }
  CleanupCompaction(compact);
  if (status.ok()) RemoveObsoleteFiles();
  else RecordBackgroundError(status);  // Block dependent writes before unlocking.
  bottom_rewrite_in_progress_ = false;
  return profile.Result(status);
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  // Finish current background compaction in the case where
  // `background_work_finished_signal_` was signalled due to an error.
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

Status DBImpl::TEST_WaitForMetadata(uint64_t target_mseq, bool compact,
                                    uint64_t timeout_micros) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::microseconds(timeout_micros);
  MutexLock l(&mutex_);
  if (target_mseq > metadata_mseq_) {
    return Status::InvalidArgument("metadata wait target is in the future");
  }
  metadata_checkpoint_target_mseq_ =
      std::max(metadata_checkpoint_target_mseq_, target_mseq);
  MaybeFreezeMetadata();
  metadata_work_signal_.SignalAll();

  auto needs_compaction = [this]() {
    std::set<uint64_t> tail_event_files;
    for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
      tail_event_files.insert(commit.file_number);
    }
    size_t eligible = 0;
    const uint64_t checkpoint = versions_->metadata_checkpoint_mseq();
    for (const MetadataFileMeta& file : versions_->metadata_files()) {
      if (file.max_mseq <= checkpoint &&
          tail_event_files.find(file.number) == tail_event_files.end()) {
        ++eligible;
      }
    }
    return eligible > 8;
  };

  while (true) {
    if (!bg_error_.ok()) return bg_error_;
    const bool checkpoint_pending =
        versions_->metadata_checkpoint_mseq() < target_mseq;
    const bool maintenance_pending = metadata_storage_work_in_progress_ ||
                                     metadata_index_->HasImmutable() ||
                                     (compact && needs_compaction());
    if (!checkpoint_pending && !maintenance_pending) {
      if (metadata_checkpoint_target_mseq_ <=
          versions_->metadata_checkpoint_mseq()) {
        metadata_checkpoint_target_mseq_ = 0;
      }
      return Status::OK();
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return Status::IOError("timed out waiting for metadata maintenance");
    }
    const uint64_t remaining = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(deadline - now)
            .count());
    background_work_finished_signal_.TimedWait(std::max<uint64_t>(1, remaining));
  }
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
    metadata_work_signal_.SignalAll();
  }
}

Status DBImpl::FindTTLCompactionCandidate(bool* found) {
  ProfileScope profile("ttl.schedule");
  mutex_.AssertHeld();
  *found = false;
  // Background scheduling is opportunistic and conservative. Explicit
  // compactions still use the sequence/time snapshot bounds, while automatic
  // TTL work waits for snapshots to be released so it cannot spin on a record
  // whose older view is still protected.
  if (!snapshots_.empty()) { ProfileScope::Count("ttl.schedule_snapshot_skips"); return Status::OK(); }
  const uint64_t now = env_->NowMicros();
  const uint64_t safe_time = snapshots_.oldest_read_time_micros(now);
  Version* current = versions_->current();
  double best_score = -1.0;
  int best_level = -1;
  std::string best_begin;
  std::string best_end;

  for (int level = 0; level < config::kNumLevels; ++level) {
    for (FileMetaData* file : current->files(level)) {
      TableInternalStats stats;
      ProfileScope::Count("ttl.schedule_checked_files");
      Status status = ProfileStatus("ttl.load_summary", [&] {
        return table_cache_->GetTTLStats(file->number, file->file_size, &stats);
      });
      if (!status.ok()) return profile.Result(status);

      if (!stats.valid) {
        ProfileScope fallback("ttl.summary_fallback_scan");
        ProfileScope::Count("ttl.summary_fallback_files");
        std::vector<uint64_t> expirations;
        ReadOptions scan_options;
        scan_options.verify_checksums = true;
        Iterator* iter = table_cache_->NewIterator(
            scan_options, file->number, file->file_size);
        uint64_t total = 0;
        Status scan_status = Status::OK();
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
          ParsedInternalKey key;
          if (!ParseInternalKey(iter->key(), &key)) {
            scan_status = Status::Corruption(
                "invalid internal key while scanning TTL summary");
            break;
          }
          ++total;
          if (key.type == kTypeValueWithTTL) {
            Slice value = iter->value();
            if (value.size() < 8) {
              scan_status = Status::Corruption(
                  "TTL value is shorter than its deadline while scanning summary");
              break;
            }
            expirations.push_back(DecodeFixed64(value.data()));
          }
        }
        if (scan_status.ok()) scan_status = iter->status();
        fallback.SetStatus(scan_status);
        delete iter;
        if (!scan_status.ok()) return scan_status;
        stats.total_records = total;
        stats.ttl_records = expirations.size();
        if (!expirations.empty()) {
          const auto bounds = std::minmax_element(expirations.begin(),
                                                   expirations.end());
          stats.min_expire_time_micros = *bounds.first;
          stats.max_expire_time_micros = *bounds.second;
          const long double span = static_cast<long double>(
              stats.max_expire_time_micros - stats.min_expire_time_micros);
          for (uint64_t expiry : expirations) {
            const long double offset = static_cast<long double>(
                expiry - stats.min_expire_time_micros);
            const size_t bucket =
                span == 0 ? 0 : std::min<size_t>(15, offset / span * 16);
            ++stats.expiration_histogram[bucket];
          }
        }
      }

      if (stats.total_records == 0 || stats.ttl_records == 0 ||
          safe_time < stats.min_expire_time_micros) {
        continue;
      }
      uint64_t expired_estimate = 0;
      if (safe_time >= stats.max_expire_time_micros) {
        expired_estimate = stats.ttl_records;
      } else {
        const long double span = static_cast<long double>(
            stats.max_expire_time_micros - stats.min_expire_time_micros);
        for (size_t bucket = 0; bucket < 16; ++bucket) {
          const uint64_t upper = static_cast<uint64_t>(
              stats.min_expire_time_micros +
              span * static_cast<long double>(bucket + 1) / 16.0L);
          if (upper <= safe_time) {
            expired_estimate += stats.expiration_histogram[bucket];
          }
        }
      }
      const bool aged =
          safe_time >= stats.min_expire_time_micros &&
          safe_time - stats.min_expire_time_micros >= 60ULL * 1000000ULL;
      const double expired_ratio =
          static_cast<double>(expired_estimate) / stats.total_records;
      if (expired_estimate == 0 || (!aged && expired_ratio < 0.30)) continue;

      ProfileScope scoring("ttl.score_candidate");
      ProfileScope::Count("ttl.schedule_candidates");
      int target_level = level;
      const Slice smallest = file->smallest.user_key();
      const Slice largest = file->largest.user_key();
      for (int upper_level = 0; upper_level < level; ++upper_level) {
        if (current->OverlapInLevel(upper_level, &smallest, &largest)) {
          target_level = upper_level;
          break;
        }
      }
      if (target_level == config::kNumLevels - 1 && !snapshots_.empty()) {
        continue;
      }

      uint64_t estimated_io = file->file_size;
      if (target_level + 1 < config::kNumLevels) {
        for (FileMetaData* overlap : current->files(target_level + 1)) {
          if (user_comparator()->Compare(overlap->largest.user_key(), smallest) >= 0 &&
              user_comparator()->Compare(overlap->smallest.user_key(), largest) <= 0) {
            estimated_io += overlap->file_size;
          }
        }
      }
      const double score =
          static_cast<double>(file->file_size) * expired_ratio /
          std::max<uint64_t>(estimated_io, 1);
      if (score > best_score) {
        best_score = score;
        best_level = target_level;
        best_begin.assign(smallest.data(), smallest.size());
        best_end.assign(largest.data(), largest.size());
      }
    }
  }

  if (best_level >= 0) {
    ProfileScope::Count("ttl.schedule_selected");
    ttl_compaction_level_ = best_level;
    ttl_compaction_begin_.swap(best_begin);
    ttl_compaction_end_.swap(best_end);
    *found = true;
  }
  return Status::OK();
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (bottom_rewrite_in_progress_) {
    // A same-level bottom rewrite owns the current Version until its
    // replacement is installed.
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    bool ttl_candidate = false;
    Status status = FindTTLCompactionCandidate(&ttl_candidate);
    if (!status.ok()) {
      RecordBackgroundError(status);
    } else if (ttl_candidate) {
      background_compaction_scheduled_ = true;
      profiling_schedule_time_ = ProfileScope::Current() ? ProfileScope::Now() : 0;
      profiling_parent_job_id_ = ProfileScope::CurrentTaskID();
      env_->Schedule(&DBImpl::BGWork, this);
    }
  } else {
    background_compaction_scheduled_ = true;
    profiling_schedule_time_ = ProfileScope::Current() ? ProfileScope::Now() : 0;
      profiling_parent_job_id_ = ProfileScope::CurrentTaskID();
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::MetadataBGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundMetadataCall();
}

void DBImpl::BackgroundMetadataCall() {
  MutexLock l(&mutex_);
  auto has_compaction_work = [this]() {
    std::set<uint64_t> tail_event_files;
    for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
      tail_event_files.insert(commit.file_number);
    }
    size_t eligible = 0;
    const uint64_t checkpoint = versions_->metadata_checkpoint_mseq();
    for (const MetadataFileMeta& file : versions_->metadata_files()) {
      if (file.max_mseq <= checkpoint &&
          tail_event_files.find(file.number) == tail_event_files.end()) {
        ++eligible;
      }
    }
    return eligible > 8;
  };
  while (!shutting_down_.load(std::memory_order_acquire)) {
    while (!shutting_down_.load(std::memory_order_acquire)) {
      const bool repair = metadata_needs_rebuild_ &&
                          metadata_rebuild_only_error_;
      const bool storage_work = metadata_index_->HasImmutable() ||
                                has_compaction_work();
      if (!metadata_commit_in_progress_ &&
          !metadata_compaction_prepare_in_progress_ &&
          (repair || (storage_work && bg_error_.ok()))) {
        break;
      }
      const uint64_t checkpoint = versions_->metadata_checkpoint_mseq();
      const bool checkpoint_pending =
          metadata_mseq_ > checkpoint ||
          UncheckpointedMetadataWALBytes() != 0 ||
          PendingMetadataChangeFiles() != 0;
      if (!metadata_commit_in_progress_ && bg_error_.ok() &&
          checkpoint_pending) {
        MaybeFreezeMetadata();
        if (metadata_index_->HasImmutable() &&
            !metadata_compaction_prepare_in_progress_) {
          continue;
        }
        if (metadata_checkpoint_pending_since_valid_) {
          const auto elapsed = std::chrono::duration_cast<
              std::chrono::microseconds>(
              std::chrono::steady_clock::now() -
              metadata_checkpoint_pending_since_);
          const uint64_t interval =
              options_.metadata_checkpoint_interval_micros;
          const uint64_t used = elapsed.count() > 0
                                    ? static_cast<uint64_t>(elapsed.count())
                                    : 0;
          const uint64_t remaining = used >= interval ? 1 : interval - used;
          metadata_work_signal_.TimedWait(remaining);
          continue;
        }
      }
      metadata_work_signal_.Wait();
    }
    if (shutting_down_.load(std::memory_order_acquire)) break;

    if (metadata_needs_rebuild_ && metadata_rebuild_only_error_) {
      metadata_storage_work_in_progress_ = true;
      metadata_rebuild_in_progress_ = true;
      Status s = RebuildMetadata();
      metadata_rebuild_in_progress_ = false;
      if (s.ok()) {
        bg_error_ = Status::OK();
        EnablePublishedStatsSnapshot();
        RemoveObsoleteFiles();
      } else {
        metadata_rebuild_only_error_ = false;
      }
      metadata_storage_work_in_progress_ = false;
      background_work_finished_signal_.SignalAll();
      continue;
    }

    metadata_storage_work_in_progress_ = true;
    Status s = metadata_index_->HasImmutable() ? FlushMetadata()
                                               : CompactMetadata();
    if (!s.ok()) RecordBackgroundError(s);
    metadata_storage_work_in_progress_ = false;
    background_work_finished_signal_.SignalAll();
  }
  metadata_worker_exited_ = true;
  metadata_worker_finished_signal_.SignalAll();
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  const bool tracked = profiler_ != nullptr;
  {
  ProfileScope profile(profiler_, profiling_id_, "background", true, "scheduled");
  MutexLock l(&mutex_);
  if (tracked) ++profiling_callbacks_;
  profile.SetParentTaskID(profiling_parent_job_id_);
  profiling_parent_job_id_ = 0;
  if (profile.active() && profiling_schedule_time_) {
    ProfileScope::Duration("background.queue_wait", ProfileScope::Now() - profiling_schedule_time_, true);
  }
  profiling_schedule_time_ = 0;
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
    profile.SetStatus(bg_error_);
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
  }  // Publish the profiling event before allowing DB teardown.
  if (tracked) {
    MutexLock l(&mutex_);
    assert(profiling_callbacks_ != 0);
    --profiling_callbacks_;
    background_work_finished_signal_.SignalAll();
  }

}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c = nullptr;
  bool is_manual = (manual_compaction_ != nullptr);
  bool is_ttl_compaction = false;
  bool ttl_bottom_rewrite = false;
  InternalKey manual_end;
  InternalKey ttl_begin;
  InternalKey ttl_end;
  Status status;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else if (ttl_compaction_level_ >= 0) {
    const int level = ttl_compaction_level_;
    const std::string begin = ttl_compaction_begin_;
    const std::string end = ttl_compaction_end_;
    ttl_compaction_level_ = -1;
    ttl_compaction_begin_.clear();
    ttl_compaction_end_.clear();
    if (level == config::kNumLevels - 1) {
      const Slice begin_slice(begin);
      const Slice end_slice(end);
      status = RewriteBottomLevelRangeLocked(&begin_slice, &end_slice);
      ttl_bottom_rewrite = true;
      if (!status.ok()) RecordBackgroundError(status);
    } else {
      ttl_begin = InternalKey(begin, kMaxSequenceNumber, kValueTypeForSeek);
      ttl_end = InternalKey(end, 0, static_cast<ValueType>(0));
      c = versions_->CompactRange(level, &ttl_begin, &ttl_end);
      is_ttl_compaction = true;
    }
  } else {
    c = versions_->PickCompaction();
  }

  if (ttl_bottom_rewrite) {
    // The same-level rewrite already installed its Version edit.
  } else if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && !is_ttl_compaction && c->IsTrivialMove()) {
    ProfileScope move("compaction.move", true, "trivial_move", c->level());
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = LogAndApply(c->edit());
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    compact->profiling_trigger = is_ttl_compaction ? "ttl" : is_manual ? "manual" : "size";
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  for (uint64_t number : compact->metadata_delta_output_numbers) {
    metadata_index_->Evict(number);
    pending_outputs_.erase(number);
    env_->RemoveFile(MetadataFileName(dbname_, number));
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
    compact->builder->EnableInternalStatsMetadata();
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  MetadataProfile::Timer timer(
      &metadata_profile_, MetadataProfileStage::kPrimaryCompactionCommit, true);
  if (compact->compaction != nullptr) {
    Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
        compact->compaction->num_input_files(0),
        compact->compaction->level(),
        compact->compaction->num_input_files(1),
        compact->compaction->level() + 1,
        static_cast<long long>(compact->total_bytes));
  }
  if (!bg_error_.ok()) return bg_error_;

  VersionEdit* edit = compact->compaction != nullptr
                          ? compact->compaction->edit()
                          : compact->replacement_edit;
  assert(edit != nullptr);
  auto add_primary_file_edits = [&]() {
    if (compact->compaction == nullptr) return;
    compact->compaction->AddInputDeletions(edit);
    const int level = compact->compaction->level();
    for (const CompactionState::Output& out : compact->outputs) {
      edit->AddFile(level + 1, out.number, out.file_size, out.smallest,
                    out.largest);
    }
  };

  // Identical input/output record counts do not change any statistic. These
  // jobs can install without blocking foreground writes.
  if (compact->metadata_delta_runs.empty()) {
    add_primary_file_edits();
    Status s = LogAndApply(edit);
    if (s.ok())
      metadata_profile_.Count(MetadataProfileCounter::kPrimaryCompactions);
    return s;
  }

  // The primary Version and the global scalars are one published state. Stop
  // new write commits, drain the in-flight WAL group, calculate the exact
  // zero-crossings against the still-current Version, then install one edit.
  BeginMetadataCommit();
  while (write_log_in_progress_) {
    background_work_finished_signal_.Wait();
  }

  uint64_t positive_records = 0;
  uint64_t negative_records = 0;
  uint64_t removed_distinct = 0;
  Iterator* deltas = metadata_index_->NewIterator(
      compact->metadata_delta_runs, false);
  deltas->SeekToFirst();
  Status status;
  while (deltas->Valid() && status.ok()) {
    const std::string key = deltas->key().ToString();
    int64_t net_records = 0;
    uint64_t output_records = 0;
    while (deltas->Valid() && deltas->key() == Slice(key)) {
      int64_t delta = 0;
      uint64_t output = 0;
      if (!DecodeMetadataDelta(deltas->value(), &delta, &output) ||
          (delta > 0 && net_records >
                            std::numeric_limits<int64_t>::max() - delta) ||
          (delta < 0 && net_records <
                            std::numeric_limits<int64_t>::min() - delta) ||
          std::numeric_limits<uint64_t>::max() - output_records < output) {
        status = Status::Corruption("invalid primary compaction delta");
        break;
      }
      net_records += delta;
      output_records += output;
      deltas->Next();
    }
    if (!status.ok() || net_records == 0) continue;

    if (net_records > 0) {
      const uint64_t amount = static_cast<uint64_t>(net_records);
      if (output_records < amount ||
          std::numeric_limits<uint64_t>::max() - positive_records < amount) {
        status = Status::Corruption("primary compaction count overflow");
        break;
      }
      positive_records += amount;
    } else {
      const uint64_t amount =
          static_cast<uint64_t>(-(net_records + 1)) + 1;
      if (std::numeric_limits<uint64_t>::max() - negative_records < amount ||
          output_records > std::numeric_limits<uint64_t>::max() - amount) {
        status = Status::Corruption("primary compaction count overflow");
        break;
      }
      negative_records += amount;

      // output = input + delta. If compaction still outputs this key, it
      // remains distinct without a lookup. Only a key with no output can cross
      // to zero, so probe the current primary state for those keys only.
      if (output_records == 0) {
        uint64_t before = 0;
        status = CountPrimaryRecordsForKey(key, &before);
        if (!status.ok()) break;
        if (before < amount) {
          status = Status::Corruption(
              "primary compaction count underflow");
          break;
        }
        if (before == amount) {
          if (removed_distinct == std::numeric_limits<uint64_t>::max()) {
            status = Status::Corruption("distinct key count overflow");
            break;
          }
          ++removed_distinct;
        }
      }
    }
  }
  if (status.ok()) status = deltas->status();
  delete deltas;

  uint64_t next_total = metadata_total_entries_;
  uint64_t next_distinct = metadata_distinct_keys_;
  if (status.ok()) {
    if (next_total < negative_records ||
        std::numeric_limits<uint64_t>::max() -
                (next_total - negative_records) < positive_records) {
      status = Status::Corruption("primary compaction total underflow/overflow");
    } else {
      next_total = next_total - negative_records + positive_records;
    }
  }
  if (status.ok()) {
    if (next_distinct < removed_distinct) {
      status = Status::Corruption("primary compaction distinct underflow");
    } else {
      next_distinct -= removed_distinct;
    }
  }
  if (status.ok()) {
    add_primary_file_edits();
    edit->SetPrimaryMetadataCheckpoint(versions_->LastSequence(), next_total,
                                       next_distinct);
    status = LogAndApply(edit);
  }
  if (status.ok()) {
    metadata_total_entries_ = next_total;
    metadata_distinct_keys_ = next_distinct;
    UpdatePublishedStatsSnapshot();
    metadata_profile_.Count(MetadataProfileCounter::kPrimaryCompactions);
    if (negative_records >= positive_records) {
      ProfileScope::Count("gc.installed_removed_records",
                          negative_records - positive_records);
    }
    if (compact->compaction != nullptr) {
      for (int which = 0; which < 2; ++which) {
        for (int i = 0; i < compact->compaction->num_input_files(which); ++i) {
          ProfileScope::Count(
              "gc.installed_input_sst_bytes",
              compact->compaction->input(which, i)->file_size);
        }
      }
      for (const auto& out : compact->outputs) {
        ProfileScope::Count("gc.installed_output_sst_bytes", out.file_size);
      }
    }
  }

  EndMetadataCommit();
  return status;
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  ProfileScope profile("compaction.merge", true, compact->profiling_trigger, compact->compaction->level());
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }
  const uint64_t compaction_time_micros = env_->NowMicros();
  compact->smallest_snapshot_time_micros =
      snapshots_.oldest_read_time_micros(compaction_time_micros);

  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Status status;
  auto allocate_delta_file = [&]() {
    mutex_.Lock();
    const uint64_t number = versions_->NewFileNumber();
    pending_outputs_.insert(number);
    compact->metadata_delta_output_numbers.push_back(number);
    mutex_.Unlock();
    return number;
  };

  auto merge_delta_runs = [&]() -> Status {
    if (compact->metadata_delta_runs.size() < 2) return Status::OK();
    const std::vector<MetadataFileMeta> sources = compact->metadata_delta_runs;
    const uint64_t number = allocate_delta_file();
    Iterator* deltas = metadata_index_->NewIterator(sources, false);
    WritableFile* output = nullptr;
    const std::string filename = MetadataFileName(dbname_, number);
    MetadataFileMeta merged;
    merged.kind = kMetadataIndex;
    merged.number = number;
    bool have_key = false;

    Status merge_status = env_->NewWritableFile(filename, &output);
    if (merge_status.ok()) {
      TableBuilder builder(metadata_index_->TableOptions(), output);
      deltas->SeekToFirst();
      while (deltas->Valid() && merge_status.ok()) {
        const std::string key = deltas->key().ToString();
        int64_t sum = 0;
        uint64_t output_records_sum = 0;
        while (deltas->Valid() && deltas->key() == Slice(key)) {
          int64_t delta;
          uint64_t output_records;
          if (!DecodeMetadataDelta(deltas->value(), &delta, &output_records) ||
              (delta > 0 && sum > std::numeric_limits<int64_t>::max() - delta) ||
              (delta < 0 && sum < std::numeric_limits<int64_t>::min() - delta)) {
            merge_status =
                Status::Corruption("metadata compaction delta merge overflow");
            break;
          }
          sum += delta;
          if (std::numeric_limits<uint64_t>::max() - output_records_sum <
              output_records) {
            merge_status = Status::Corruption(
                "metadata compaction output count overflow");
            break;
          }
          output_records_sum += output_records;
          deltas->Next();
        }
        if (!merge_status.ok()) break;
        if (sum == 0) continue;
        builder.Add(key, EncodeMetadataDelta(sum, output_records_sum));
        if (!have_key) {
          merged.smallest = key;
          have_key = true;
        }
        merged.largest = key;
      }
      if (merge_status.ok()) merge_status = deltas->status();
      if (merge_status.ok()) merge_status = builder.Finish();
      merged.file_size = builder.FileSize();
      if (merge_status.ok()) merge_status = output->Close();
      if (!merge_status.ok()) builder.Abandon();
    }
    delete output;
    delete deltas;

    mutex_.Lock();
    for (const MetadataFileMeta& source : sources) {
      metadata_index_->Evict(source.number);
      pending_outputs_.erase(source.number);
    }
    compact->metadata_delta_runs.clear();
    if (merge_status.ok() && have_key) {
      compact->metadata_delta_runs.push_back(merged);
    } else {
      pending_outputs_.erase(number);
    }
    mutex_.Unlock();
    for (const MetadataFileMeta& source : sources) {
      env_->RemoveFile(MetadataFileName(dbname_, source.number));
    }
    if (!have_key) env_->RemoveFile(filename);
    return merge_status;
  };

  auto spill_metadata_deltas = [&]() -> Status {
    if (compact->metadata_deltas.empty()) return Status::OK();
    const uint64_t number = allocate_delta_file();
    WritableFile* output = nullptr;
    const std::string filename = MetadataFileName(dbname_, number);
    MetadataFileMeta run;
    run.kind = kMetadataIndex;
    run.number = number;
    bool have_key = false;
    Status spill_status = env_->NewWritableFile(filename, &output);
    if (spill_status.ok()) {
      TableBuilder builder(metadata_index_->TableOptions(), output);
      for (const auto& entry : compact->metadata_deltas) {
        if (entry.second.net_records == 0) continue;
        builder.Add(entry.first,
                    EncodeMetadataDelta(entry.second.net_records,
                                        entry.second.output_records));
        if (!have_key) {
          run.smallest = entry.first;
          have_key = true;
        }
        run.largest = entry.first;
      }
      spill_status = builder.Finish();
      run.file_size = builder.FileSize();
      if (spill_status.ok()) spill_status = output->Close();
      if (!spill_status.ok()) builder.Abandon();
    }
    delete output;
    if (!spill_status.ok()) return spill_status;

    compact->metadata_deltas.clear();
    compact->metadata_delta_bytes = 0;
    if (have_key) compact->metadata_delta_runs.push_back(run);
    else {
      mutex_.Lock();
      pending_outputs_.erase(number);
      mutex_.Unlock();
      env_->RemoveFile(filename);
    }
    if (compact->metadata_delta_runs.size() >= 8) {
      return merge_delta_runs();
    }
    return Status::OK();
  };

  auto add_metadata_delta = [&](const std::string& key, int64_t adjustment,
                                uint64_t output_adjustment) -> Status {
    auto iter = compact->metadata_deltas.find(key);
    if (iter == compact->metadata_deltas.end()) {
      compact->metadata_delta_bytes += key.size() + sizeof(PrimaryMetadataDelta) + 64;
      iter = compact->metadata_deltas.emplace(key, PrimaryMetadataDelta()).first;
    }
    if ((adjustment > 0 &&
         iter->second.net_records >
             std::numeric_limits<int64_t>::max() - adjustment) ||
        (adjustment < 0 &&
         iter->second.net_records <
             std::numeric_limits<int64_t>::min() - adjustment) ||
        std::numeric_limits<uint64_t>::max() -
                iter->second.output_records <
            output_adjustment) {
      return Status::Corruption("metadata compaction delta overflow");
    }
    iter->second.net_records += adjustment;
    iter->second.output_records += output_adjustment;
    if (compact->metadata_delta_bytes >= options_.metadata_workspace_size) {
      return spill_metadata_deltas();
    }
    return Status::OK();
  };

  // The common bytewise-comparator case is already grouped and sorted by the
  // primary compaction iterator. Emit one net delta per key directly to sorted
  // runs instead of building a map proportional to the whole compaction.
  const bool stream_sorted_deltas =
      user_comparator() == BytewiseComparator();
  WritableFile* sorted_delta_file = nullptr;
  TableBuilder* sorted_delta_builder = nullptr;
  MetadataFileMeta sorted_delta_run;
  bool sorted_delta_has_key = false;

  auto finish_sorted_delta_run = [&]() -> Status {
    if (sorted_delta_builder == nullptr) return Status::OK();
    Status run_status = sorted_delta_builder->Finish();
    sorted_delta_run.file_size = sorted_delta_builder->FileSize();
    if (run_status.ok()) run_status = sorted_delta_file->Close();
    if (!run_status.ok()) sorted_delta_builder->Abandon();
    delete sorted_delta_builder;
    delete sorted_delta_file;
    sorted_delta_builder = nullptr;
    sorted_delta_file = nullptr;

    if (!run_status.ok()) return run_status;
    if (sorted_delta_has_key) {
      compact->metadata_delta_runs.push_back(sorted_delta_run);
      sorted_delta_run = MetadataFileMeta();
      sorted_delta_has_key = false;
      if (compact->metadata_delta_runs.size() >= 8) {
        return merge_delta_runs();
      }
    }
    return Status::OK();
  };

  auto append_sorted_delta = [&](const std::string& key, int64_t delta,
                                 uint64_t output_records) -> Status {
    if (delta == 0) return Status::OK();
    if (sorted_delta_builder == nullptr) {
      const uint64_t number = allocate_delta_file();
      sorted_delta_run.kind = kMetadataIndex;
      sorted_delta_run.number = number;
      const std::string filename = MetadataFileName(dbname_, number);
      Status run_status = env_->NewWritableFile(filename, &sorted_delta_file);
      if (!run_status.ok()) return run_status;
      sorted_delta_builder =
          new TableBuilder(metadata_index_->TableOptions(), sorted_delta_file);
    }
    sorted_delta_builder->Add(
        key, EncodeMetadataDelta(delta, output_records));
    if (!sorted_delta_has_key) {
      sorted_delta_run.smallest = key;
      sorted_delta_has_key = true;
    }
    sorted_delta_run.largest = key;
    if (sorted_delta_builder->FileSize() >= options_.metadata_workspace_size) {
      return finish_sorted_delta_run();
    }
    return Status::OK();
  };

  input->SeekToFirst();
  ParsedInternalKey ikey;
  std::string current_user_key;
  int64_t current_key_delta = 0;
  uint64_t current_key_output_records = 0;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    ProfileScope filtering("gc.iterate_filter");
    ProfileScope::Count("gc.records_checked");
    Slice key = input->key();
    if (key.size() < 8) {
      status = Status::Corruption("internal key is too short during compaction");
      break;
    }
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      status = Status::Corruption("invalid internal key during compaction");
      break;
    } else {
      std::string raw_key(ikey.user_key.data(), ikey.user_key.size());
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        if (stream_sorted_deltas && has_current_user_key) {
          status = append_sorted_delta(current_user_key, current_key_delta,
                                       current_key_output_records);
          if (!status.ok()) break;
        }
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        current_key_delta = 0;
        current_key_output_records = 0;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (stream_sorted_deltas) {
        if (current_key_delta == std::numeric_limits<int64_t>::min()) {
          status = Status::Corruption("metadata compaction delta overflow");
          break;
        }
        --current_key_delta;
      } else {
        status = add_metadata_delta(raw_key, -1, 0);
        if (!status.ok()) break;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
        ProfileScope::Count("gc.dropped_hidden_version");
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
        ProfileScope::Count("gc.dropped_tombstone");
      } else if (ikey.type == kTypeValueWithTTL) {
        if (ikey.sequence > compact->smallest_snapshot) {
          ProfileScope::Count("gc.ttl_retained_sequence_boundary");
        } else {
        const Slice raw_value = input->value();
        if (raw_value.size() < 8) {
          status = Status::Corruption(
              "TTL value is shorter than its deadline during compaction");
          break;
        }
        if (DecodeFixed64(raw_value.data()) <=
            compact->smallest_snapshot_time_micros) {
          // The expired value acts as a deletion barrier. Older records in
          // this compaction are hidden by the normal sequence rule above.
          // Only expired records need the lower-level safety check. Its
          // cursors can skip live keys because input user keys are ordered.
          if (compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
            drop = true;
            ProfileScope::Count("gc.dropped_expired_ttl");
          }
        } else {
          ProfileScope::Count("gc.ttl_retained_time_boundary");
        }
        }
      }
      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (drop) {
      ProfileScope::Count("gc.records_dropped");
      ProfileScope::Count("gc.dropped_encoded_bytes", key.size() + input->value().size());
    }
    if (!drop) {
      ProfileScope::Count("gc.records_output");
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());
      if (stream_sorted_deltas) {
        if (current_key_delta == std::numeric_limits<int64_t>::max()) {
          status = Status::Corruption("metadata compaction delta overflow");
          break;
        }
        ++current_key_delta;
        if (current_key_output_records ==
            std::numeric_limits<uint64_t>::max()) {
          status = Status::Corruption("metadata output count overflow");
          break;
        }
        ++current_key_output_records;
      } else {
        std::string raw_key(ikey.user_key.data(), ikey.user_key.size());
        status = add_metadata_delta(raw_key, 1, 1);
        if (!status.ok()) break;
      }

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;
  if (status.ok() && stream_sorted_deltas && has_current_user_key) {
    status = append_sorted_delta(current_user_key, current_key_delta,
                                 current_key_output_records);
  }
  if (status.ok()) {
    status = stream_sorted_deltas ? finish_sorted_delta_run()
                                 : spill_metadata_deltas();
  } else if (sorted_delta_builder != nullptr) {
    sorted_delta_builder->Abandon();
    delete sorted_delta_builder;
    delete sorted_delta_file;
    sorted_delta_builder = nullptr;
    sorted_delta_file = nullptr;
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
      ProfileScope::Count("gc.input_sst_bytes", compact->compaction->input(which, i)->file_size);
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
    ProfileScope::Count("gc.generated_output_sst_bytes", compact->outputs[i].file_size);
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return profile.Result(status);
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint64_t* read_time_micros,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();
  *read_time_micros = env_->NowMicros();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint64_t ignored_time;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_time,
                             &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  ProfileScope profile(profiler_, profiling_id_, "op.get");
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  uint64_t read_time_micros;
  if (options.snapshot != nullptr) {
    const auto* snap = static_cast<const SnapshotImpl*>(options.snapshot);
    snapshot = snap->sequence_number();
    read_time_micros = snap->read_time_micros();
  } else {
    snapshot = versions_->LastSequence();
    read_time_micros = env_->NowMicros();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    bool found;
    { ProfileScope lookup("read.memtable"); found = mem->Get(lkey, read_time_micros, value, &s); lookup.SetStatus(s); }
    if (found) {
      // Done
    } else if (imm != nullptr && [&] {
      ProfileScope lookup("read.immutable_memtable");
      bool found_imm = imm->Get(lkey, read_time_micros, value, &s);
      lookup.SetStatus(s); return found_imm;
    }()) {
      // Done
    } else {
      s = ProfileStatus("read.sst", [&] { return current->Get(options, lkey, value, read_time_micros, &stats); });
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return profile.Result(s);
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  ProfileScope profile(profiler_, profiling_id_, "op.new_iterator");
  SequenceNumber latest_snapshot;
  uint64_t latest_read_time_micros;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot,
                                       &latest_read_time_micros, &seed);
  SequenceNumber sequence = latest_snapshot;
  uint64_t read_time_micros = latest_read_time_micros;
  if (options.snapshot != nullptr) {
    const auto* snap = static_cast<const SnapshotImpl*>(options.snapshot);
    sequence = snap->sequence_number();
    read_time_micros = snap->read_time_micros();
  }
  return NewDBIterator(this, user_comparator(), iter,
                       sequence, read_time_micros, seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  ProfileScope profile(profiler_, profiling_id_, "op.snapshot");
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence(), env_->NowMicros());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  ProfileScope profile(profiler_, profiling_id_, "op.release_snapshot");
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
  MaybeScheduleCompaction();
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  ProfileScope profile(profiler_, profiling_id_, "op.put");
  return profile.Result(DB::Put(o, key, val));
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value, uint64_t ttl) {
  ProfileScope profile(profiler_, profiling_id_, "op.ttl_put");
  uint64_t deadline = 0;
  Status deadline_status = ProfileStatus("ttl.deadline", [&] {
    const uint64_t now = env_->NowMicros();
    const uint64_t scale = 1000000;
    if (ttl > std::numeric_limits<uint64_t>::max() / scale)
      return Status::InvalidArgument("TTL seconds overflow microseconds");
    const uint64_t ttl_micros = ttl * scale;
    if (now > std::numeric_limits<uint64_t>::max() - ttl_micros)
      return Status::InvalidArgument("TTL deadline overflows Env time");
    deadline = now + ttl_micros;
    return Status::OK();
  });
  if (!deadline_status.ok()) return profile.Result(deadline_status);
  WriteBatch batch;
  {
    ProfileScope encoding("ttl.encode");
    WriteBatchInternal::PutWithExpiration(&batch, key, value, deadline);
  }
  return profile.Result(Write(options, &batch));
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  ProfileScope profile(profiler_, profiling_id_, "op.delete");
  return profile.Result(DB::Delete(options, key));
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  ProfileScope profile(profiler_, profiling_id_,
                       updates ? "op.write_batch" : "internal.flush_request",
                       false, "", -1, true);
  if (updates != nullptr) {
    ValidateBatch validation;
    Status validation_status = ProfileStatus("write.validate_batch", [&] { return updates->Iterate(&validation); });
    if (!validation_status.ok()) return profile.Result(validation_status);
  }

  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MetadataProfile::Timer queue_wait_timer(
      &metadata_profile_, MetadataProfileStage::kWriteQueueWait);
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  {
  ProfileScope queue("write.queue_wait");
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  }
  queue_wait_timer.Stop();
  if (w.done) {
    return profile.Result(w.status);
  }

  // May temporarily unlock and wait.
  Status status;
  { ProfileScope stage("write.make_room"); status = MakeRoomForWrite(updates == nullptr); stage.SetStatus(status); }
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  WriteBatch* write_batch = nullptr;
  if (status.ok() && updates != nullptr) {
    write_batch = BuildBatchGroup(&last_writer);
    if (static_cast<uint64_t>(WriteBatchInternal::Count(write_batch)) >
        kMaxSequenceNumber - last_sequence) {
      status = Status::InvalidArgument("sequence number space exhausted");
    }
  }
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    metadata_profile_.Count(MetadataProfileCounter::kWriteBatches);
    metadata_profile_.Count(MetadataProfileCounter::kWriteOperations,
                           WriteBatchInternal::Count(write_batch));
    const uint64_t batch_count = WriteBatchInternal::Count(write_batch);
    WriteBatchInternal::SetSequence(write_batch,
                                   batch_count == 0 ? last_sequence : last_sequence + 1);
    last_sequence += batch_count;

    std::vector<MetadataFileMeta> batch_runs;
    std::function<Status(const std::vector<MetadataFileMeta>&,
                         MetadataFileMeta*)>
        merge_batch_runs;
    auto spill_batch_counts = [&](const MetadataValueMap& counts) -> Status {
      const uint64_t number = versions_->NewFileNumber();
      pending_outputs_.insert(number);
      const std::string filename = MetadataFileName(dbname_, number);
      WritableFile* output = nullptr;
      Status spill_status = env_->NewWritableFile(filename, &output);
      MetadataFileMeta run;
      run.kind = kMetadataIndex;
      run.number = number;
      run.min_mseq = 0;
      run.max_mseq = 0;
      bool have_key = false;
      if (spill_status.ok()) {
        TableBuilder builder(metadata_index_->TableOptions(), output);
        for (const auto& entry : counts) {
          MetadataValue value;
          value.count = entry.second.count;
          builder.Add(entry.first, MetadataIndex::EncodeValue(value));
          if (!have_key) {
            run.smallest = entry.first;
            have_key = true;
          }
          run.largest = entry.first;
        }
        spill_status = builder.Finish();
        run.file_size = builder.FileSize();
        if (spill_status.ok()) spill_status = output->Close();
        if (!spill_status.ok()) builder.Abandon();
      }
      delete output;
      if (spill_status.ok() && have_key) {
        metadata_profile_.Count(MetadataProfileCounter::kBatchSpills);
        metadata_profile_.Count(MetadataProfileCounter::kBatchSpillBytes,
                               run.file_size);
        batch_runs.push_back(run);
        if (batch_runs.size() >= 8 && merge_batch_runs) {
          std::vector<MetadataFileMeta> group(batch_runs.begin(),
                                              batch_runs.begin() + 8);
          MetadataFileMeta merged;
          spill_status = merge_batch_runs(group, &merged);
          if (spill_status.ok()) {
            batch_runs.erase(batch_runs.begin(), batch_runs.begin() + 8);
            batch_runs.push_back(std::move(merged));
          }
        }
      } else {
        metadata_index_->Evict(number);
        pending_outputs_.erase(number);
        env_->RemoveFile(filename);
      }
      return spill_status;
    };

    merge_batch_runs = [&](const std::vector<MetadataFileMeta>& sources,
                           MetadataFileMeta* merged) -> Status {
      const uint64_t number = versions_->NewFileNumber();
      pending_outputs_.insert(number);
      const std::string filename = MetadataFileName(dbname_, number);
      WritableFile* output = nullptr;
      Iterator* input = metadata_index_->NewIterator(sources, false);
      Status merge_status = env_->NewWritableFile(filename, &output);
      MetadataFileMeta result;
      result.kind = kMetadataIndex;
      result.number = number;
      result.min_mseq = 0;
      result.max_mseq = 0;
      bool have_key = false;
      if (merge_status.ok()) {
        TableBuilder builder(metadata_index_->TableOptions(), output);
        input->SeekToFirst();
        while (input->Valid() && merge_status.ok()) {
          const std::string key = input->key().ToString();
          uint64_t count = 0;
          while (input->Valid() && input->key() == Slice(key)) {
            MetadataValue value;
            if (!MetadataIndex::DecodeValue(input->value(), &value) ||
                value.mseq != 0 ||
                std::numeric_limits<uint64_t>::max() - count < value.count) {
              merge_status =
                  Status::Corruption("invalid metadata batch spill run");
              break;
            }
            count += value.count;
            input->Next();
          }
          if (!merge_status.ok()) break;
          MetadataValue value;
          value.count = count;
          builder.Add(key, MetadataIndex::EncodeValue(value));
          if (!have_key) {
            result.smallest = key;
            have_key = true;
          }
          result.largest = key;
        }
        if (merge_status.ok()) merge_status = input->status();
        if (merge_status.ok()) merge_status = builder.Finish();
        result.file_size = builder.FileSize();
        if (merge_status.ok()) merge_status = output->Close();
        if (!merge_status.ok()) builder.Abandon();
      }
      delete output;
      delete input;
      if (merge_status.ok() && have_key) {
        metadata_profile_.Count(MetadataProfileCounter::kBatchMergePasses);
        *merged = result;
        for (const MetadataFileMeta& source : sources) {
          metadata_index_->Evict(source.number);
          pending_outputs_.erase(source.number);
          env_->RemoveFile(MetadataFileName(dbname_, source.number));
        }
      } else {
        metadata_index_->Evict(number);
        pending_outputs_.erase(number);
        env_->RemoveFile(filename);
      }
      return merge_status;
    };

    MetadataProfile::Timer prepare_timer(
        &metadata_profile_, MetadataProfileStage::kWritePrepare);
    BatchMetadataCounter batch_counter(options_.metadata_workspace_size,
                                       spill_batch_counts);
    status = write_batch->Iterate(&batch_counter);
    if (status.ok() && !batch_counter.status().ok()) {
      status = batch_counter.status();
    }
    if (status.ok() && batch_counter.count_overflow()) {
      status = Status::Corruption("metadata batch key count overflow");
    }

    auto batch_overlaps_compaction_range = [&](CompactionState* compact) {
      if (compact == nullptr || !compact->has_metadata_delta_range) {
        return false;
      }
      for (const MetadataFileMeta& run : batch_runs) {
        if (!run.smallest.empty() &&
            MetadataRangesOverlap(run.smallest, run.largest,
                                  compact->metadata_delta_smallest,
                                  compact->metadata_delta_largest)) {
          return true;
        }
      }
      for (const auto& entry : batch_counter.counts) {
        if (MetadataKeyInRange(entry.first,
                               compact->metadata_delta_smallest,
                               compact->metadata_delta_largest)) {
          return true;
        }
      }
      return false;
    };
    auto overlapping_journal_bytes = [&](CompactionState* compact) {
      size_t bytes = 0;
      for (const auto& entry : batch_counter.counts) {
        if (!MetadataKeyInRange(entry.first,
                                compact->metadata_delta_smallest,
                                compact->metadata_delta_largest) ||
            compact->metadata_journal_values.find(entry.first) !=
                compact->metadata_journal_values.end()) {
          continue;
        }
        const size_t charge = entry.first.size() + sizeof(MetadataValue) + 48;
        bytes = charge > std::numeric_limits<size_t>::max() - bytes
                    ? std::numeric_limits<size_t>::max()
                    : bytes + charge;
      }
      return bytes;
    };
    while (status.ok() && metadata_compaction_prepare_ != nullptr &&
           batch_overlaps_compaction_range(metadata_compaction_prepare_)) {
      CompactionState* compact = metadata_compaction_prepare_;
      const size_t journal_limit = std::max<size_t>(
          1, options_.metadata_workspace_size / 4);
      const size_t needed = overlapping_journal_bytes(compact);
      const bool journal_over_budget =
          needed > journal_limit ||
          compact->metadata_journal_bytes >
              journal_limit - std::min(journal_limit, needed);
      if (compact->metadata_journal_finishing ||
          (batch_counter.spilled() &&
           compact->metadata_journal_files.size() >= 8) ||
          (journal_over_budget && compact->metadata_journal_files.size() >= 8)) {
        compact->metadata_journal_pressure = true;
        background_work_finished_signal_.Wait();
        continue;
      }
      if (!batch_counter.spilled() && journal_over_budget) {
        status = batch_counter.SpillNow();
        if (!status.ok()) break;
        // The resulting change file is one pinned journal source, so it does
        // not consume the in-memory correction workspace.
        continue;
      }
      break;
    }

    if (status.ok() && batch_counter.spilled()) {
      status = batch_counter.SpillNow();
      while (status.ok() && batch_runs.size() > 8) {
        std::vector<MetadataFileMeta> next_runs;
        for (size_t begin = 0; begin < batch_runs.size() && status.ok();
             begin += 8) {
          const size_t end = std::min(batch_runs.size(), begin + 8);
          if (end - begin == 1) {
            next_runs.push_back(batch_runs[begin]);
          } else {
            std::vector<MetadataFileMeta> group(batch_runs.begin() + begin,
                                                batch_runs.begin() + end);
            MetadataFileMeta merged;
            status = merge_batch_runs(group, &merged);
            if (status.ok()) next_runs.push_back(std::move(merged));
          }
        }
        if (status.ok()) batch_runs.swap(next_runs);
      }
    }

    uint64_t next_total_entries = metadata_total_entries_;
    uint64_t next_distinct_keys = metadata_distinct_keys_;
    bool wal_attempted = false;
    bool wal_record_appended = false;
    bool wal_record_synced = false;
    uint64_t write_log_number = logfile_number_;

    const uint64_t records_added = batch_counter.total_records();
    if (status.ok() &&
        std::numeric_limits<uint64_t>::max() - next_total_entries <
            records_added) {
      status = Status::Corruption("total entry count overflow");
    }
    if (status.ok()) next_total_entries += records_added;

    auto account_key = [&](const Slice& key) -> Status {
      metadata_profile_.Count(MetadataProfileCounter::kWriteDistinctKeys);
      uint64_t old_count = 0;
      Status count_status = CountPrimaryRecordsForKey(key, &old_count);
      if (!count_status.ok()) return count_status;
      if (old_count == 0) {
        if (next_distinct_keys == std::numeric_limits<uint64_t>::max()) {
          return Status::Corruption("distinct key count overflow");
        }
        ++next_distinct_keys;
      }
      return Status::OK();
    };

    if (status.ok() && batch_counter.spilled()) {
      Iterator* keys = metadata_index_->NewIterator(batch_runs, false);
      keys->SeekToFirst();
      std::string previous_key;
      bool have_previous = false;
      while (keys->Valid() && status.ok()) {
        std::string key = keys->key().ToString();
        while (keys->Valid() && keys->key() == Slice(key)) keys->Next();
        if (!have_previous || key != previous_key) {
          status = account_key(key);
          previous_key = std::move(key);
          have_previous = true;
        }
      }
      if (status.ok()) status = keys->status();
      delete keys;
    } else if (status.ok()) {
      for (const auto& key_count : batch_counter.counts) {
        status = account_key(key_count.first);
        if (!status.ok()) break;
      }
    }

    for (const MetadataFileMeta& run : batch_runs) {
      metadata_index_->Evict(run.number);
      pending_outputs_.erase(run.number);
      env_->RemoveFile(MetadataFileName(dbname_, run.number));
    }
    batch_runs.clear();

    prepare_timer.Stop();
    std::string wal_record;
    if (status.ok()) {
      // Primary WriteBatch bytes are the complete recovery record. Statistics
      // are rebuilt from primary internal records after recovery.
      wal_record = WriteBatchInternal::Contents(write_batch).ToString();
    }

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    if (status.ok()) {
      wal_attempted = true;
      write_log_in_progress_ = true;
      write_log_number = logfile_number_;
      mutex_.Unlock();
      MetadataProfile::Timer wal_io_wait_timer(
          &metadata_profile_, MetadataProfileStage::kWalIoWait);
      metadata_wal_io_mutex_.Lock();
      wal_io_wait_timer.Stop();
      {
        MetadataProfile::Timer append_timer(
            &metadata_profile_, MetadataProfileStage::kWalAppend);
        status = log_->AddRecord(wal_record);
      }
      wal_record_appended = status.ok();
      if (status.ok()) {
        const uint64_t payload_bytes =
            WriteBatchInternal::ByteSize(write_batch);
        metadata_profile_.Count(MetadataProfileCounter::kWalRecords);
        metadata_profile_.Count(MetadataProfileCounter::kWalPayloadBytes,
                                payload_bytes);
        if (wal_record.size() > payload_bytes) {
          metadata_profile_.Count(MetadataProfileCounter::kWalMetadataBytes,
                                  wal_record.size() - payload_bytes);
        }
      }
      bool sync_error = false;
      if (status.ok() && options.sync) {
        {
          MetadataProfile::Timer sync_timer(
              &metadata_profile_, MetadataProfileStage::kWalSync);
          metadata_profile_.Count(MetadataProfileCounter::kWalSyncs);
          status = logfile_->Sync();
          wal_record_synced = status.ok();
        }
        if (!status.ok()) {
          sync_error = true;
        }
      }
      metadata_wal_io_mutex_.Unlock();
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      MetadataWALPosition& wal_position =
          metadata_wal_positions_[write_log_number];
      if (wal_record_appended) {
        const uint64_t record_bytes = wal_record.size();
        wal_position.appended_position =
            std::numeric_limits<uint64_t>::max() -
                    wal_position.appended_position <
                    record_bytes
                ? std::numeric_limits<uint64_t>::max()
                : wal_position.appended_position + record_bytes;
      } else if (wal_attempted) {
        wal_position.durable_known = false;
      }
      if (wal_record_synced) {
        wal_position.durable_position = wal_position.appended_position;
        wal_position.durable_known = true;
      }
      write_log_in_progress_ = false;
      background_work_finished_signal_.SignalAll();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (status.ok()) {
      ProfileScope::Count("write.committed_groups");
      ProfileScope::Count("write.committed_records", batch_count);
      metadata_total_entries_ = next_total_entries;
      metadata_distinct_keys_ = next_distinct_keys;
      versions_->SetLastSequence(last_sequence);
      UpdatePublishedStatsSnapshot();
    } else {
      RecordBackgroundError(status);
    }
  }

  if (write_batch == tmp_batch_) tmp_batch_->Clear();

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
  background_work_finished_signal_.SignalAll();

  return profile.Result(status);
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (metadata_commit_in_progress_) {
      background_work_finished_signal_.Wait();
    } else if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force && metadata_index_->HasImmutable() &&
               ((metadata_index_->ActiveMemoryUsage() != 0 &&
                 metadata_index_->ActiveMemoryUsage() >=
                     options_.metadata_memtable_size) ||
                UncheckpointedMetadataWALBytes() >=
                    options_.metadata_wal_size_limit ||
                PendingMetadataChangeBytes() >=
                    options_.metadata_memtable_size ||
                PendingMetadataChangeFiles() > 8)) {
      MetadataProfile::Timer wait_timer(
          &metadata_profile_, MetadataProfileStage::kBackpressure);
      metadata_profile_.Count(MetadataProfileCounter::kBackpressureWaits);
      Log(options_.info_log, "Metadata generations full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (metadata_wal_sync_pins_.find(logfile_number_) !=
               metadata_wal_sync_pins_.end()) {
      // A background WAL-prefix sync has borrowed the active file handle.
      // Keep it alive until SyncMetadataWALPrefix releases its pin.
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      delete log_;

      s = logfile_->Close();
      if (!s.ok()) {
        // We may have lost some data written to the previous log file.
        // Switch to the new log file anyway, but record as a background
        // error so we do not attempt any more writes.
        //
        // We could perhaps attempt to save the memtable corresponding
        // to log file and suppress the error if that works, but that
        // would add more complexity in a critical code path.
        RecordBackgroundError(s);
      }
      delete logfile_;

      logfile_ = lfile;
      logfile_number_ = new_log_number;
      MetadataWALPosition new_position;
      new_position.durable_known = true;
      metadata_wal_positions_[new_log_number] = new_position;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "metadata-stats") {
    char buf[1024];
    const size_t memory = metadata_index_->ApproximateMemoryUsage();
    const size_t journal_bytes = metadata_compaction_prepare_ == nullptr
                                     ? 0
                                     : metadata_compaction_prepare_
                                           ->metadata_journal_bytes;
    const size_t journal_files = metadata_compaction_prepare_ == nullptr
                                     ? 0
                                     : metadata_compaction_prepare_
                                           ->metadata_journal_files.size();
    std::snprintf(
        buf, sizeof(buf),
        "published_mseq=%llu checkpoint_mseq=%llu total_entries=%llu "
        "distinct_keys=%llu metadata_files=%zu immutable_generations=%zu "
        "memory_bytes=%zu memory_budget_bytes=%zu wal_pending_bytes=%llu "
        "wal_budget_bytes=%llu checkpoint_interval_us=%llu "
        "pending_change_bytes=%llu pending_change_files=%zu rebuild_pending=%d "
        "compaction_prepare=%d compaction_finishing=%d "
        "compaction_journal_bytes=%zu compaction_journal_files=%zu",
        static_cast<unsigned long long>(metadata_mseq_),
        static_cast<unsigned long long>(versions_->metadata_checkpoint_mseq()),
        static_cast<unsigned long long>(metadata_total_entries_),
        static_cast<unsigned long long>(metadata_distinct_keys_),
        versions_->metadata_files().size(), metadata_index_->ImmutableCount(),
        memory, options_.metadata_memtable_size + options_.metadata_cache_size +
                    options_.metadata_workspace_size,
        static_cast<unsigned long long>(UncheckpointedMetadataWALBytes()),
        static_cast<unsigned long long>(options_.metadata_wal_size_limit),
        static_cast<unsigned long long>(
            options_.metadata_checkpoint_interval_micros),
        static_cast<unsigned long long>(PendingMetadataChangeBytes()),
        PendingMetadataChangeFiles(),
        metadata_needs_rebuild_ ? 1 : 0,
        metadata_compaction_prepare_ != nullptr ? 1 : 0,
        metadata_compaction_prepare_ != nullptr &&
                metadata_compaction_prepare_->metadata_journal_finishing
            ? 1
            : 0,
        journal_bytes, journal_files);
    *value = buf;
    return true;
  } else if (in == "metadata-profile") {
    std::ostringstream out;
    out << "{\"schema_version\":2,\"enabled\":"
        << (metadata_profile_.enabled() ? "true" : "false")
        << ",\"sample_rate\":" << metadata_profile_.sample_rate()
        << ",\"configuration\":{\"metadata_memtable_size_bytes\":"
        << options_.metadata_memtable_size
        << ",\"metadata_wal_size_limit_bytes\":"
        << options_.metadata_wal_size_limit
        << ",\"metadata_checkpoint_interval_micros\":"
        << options_.metadata_checkpoint_interval_micros << "}"
        << ",\"counters\":{";
    for (size_t i = 0;
         i < static_cast<size_t>(MetadataProfileCounter::kCount); ++i) {
      if (i != 0) out << ',';
      const auto counter = static_cast<MetadataProfileCounter>(i);
      out << '"' << MetadataProfile::CounterName(counter) << "\":"
          << metadata_profile_.CounterValue(counter);
    }
    out << "},\"stages\":{";
    for (size_t i = 0;
         i < static_cast<size_t>(MetadataProfileStage::kCount); ++i) {
      if (i != 0) out << ',';
      const auto stage = static_cast<MetadataProfileStage>(i);
      out << '"' << MetadataProfile::StageName(stage) << "\":{";
      out << "\"events\":" << metadata_profile_.StageEvents(stage)
          << ",\"samples\":" << metadata_profile_.StageSamples(stage)
          << ",\"sampled_total_ns\":"
          << metadata_profile_.StageTotalNanoseconds(stage)
          << ",\"sampled_max_ns\":"
          << metadata_profile_.StageMaximumNanoseconds(stage)
          << ",\"histogram_ns_upper_bound\":[";
      for (size_t bucket = 0; bucket < MetadataProfile::kHistogramBuckets;
           ++bucket) {
        if (bucket != 0) out << ',';
        out << metadata_profile_.StageBucket(stage, bucket);
      }
      out << "]}";
    }
    out << "},\"resources\":{\"metadata_memory_estimate_bytes\":"
        << metadata_index_->ApproximateMemoryUsage()
        << ",\"metadata_active_memory_estimate_bytes\":"
        << metadata_index_->ActiveMemoryUsage()
        << ",\"metadata_immutable_memory_estimate_bytes\":"
        << metadata_index_->ImmutableMemoryUsage()
        << ",\"metadata_file_count\":"
        << versions_->metadata_files().size()
        << ",\"metadata_wal_pending_bytes\":"
        << UncheckpointedMetadataWALBytes()
        << ",\"metadata_wal_size_limit_bytes\":"
        << options_.metadata_wal_size_limit
        << ",\"metadata_checkpoint_interval_micros\":"
        << options_.metadata_checkpoint_interval_micros
        << ",\"metadata_pending_change_bytes\":"
        << PendingMetadataChangeBytes()
        << ",\"metadata_pending_change_files\":"
        << PendingMetadataChangeFiles()
        << "}}";
    *value = out.str();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    total_usage += metadata_index_->ApproximateMemoryUsage();
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::GetCurrentStats(const ReadOptions& options, Stats* stats) {
  (void)options;
  (void)stats;
  return Status::OK();
}

Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Put(const WriteOptions&, const Slice&, const Slice&, uint64_t) {
  // Preserve a safe default for DB implementations that do not support TTL.
  return Status::NotSupported("TTL Put is not implemented");
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  const uint64_t profiling_id = options.profiler ? NewProfileDatabaseID() : 0;
  ProfileScope profile(options.profiler, profiling_id, "recovery.open", true, "open");
  DBImpl* impl = new DBImpl(options, dbname, profiling_id);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = impl->env_->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      DBImpl::MetadataWALPosition new_position;
      new_position.durable_known = true;
      impl->metadata_wal_positions_[new_log_number] = new_position;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->LogAndApply(&edit);
  }
  if (s.ok() && impl->metadata_needs_rebuild_) {
    s = impl->RebuildMetadata();
    if (s.ok()) s = impl->PersistPrimaryMetadataCheckpoint();
  } else if (s.ok()) {
    impl->MaybeFreezeMetadata();
  }
  if (s.ok()) {
    ProfileScope publish("recovery.publish");
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
    impl->metadata_work_signal_.Signal();
    impl->EnablePublishedStatsSnapshot();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return profile.Result(s);
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
