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

std::string EncodeMetadataDelta(int64_t delta) {
  uint64_t bits;
  std::memcpy(&bits, &delta, sizeof(bits));
  MetadataValue value;
  value.mseq = 0;
  value.count = bits;
  return MetadataIndex::EncodeValue(value);
}

bool DecodeMetadataDelta(const Slice& encoded, int64_t* delta) {
  MetadataValue value;
  if (!MetadataIndex::DecodeValue(encoded, &value) || value.mseq != 0) {
    return false;
  }
  std::memcpy(delta, &value.count, sizeof(*delta));
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
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
  std::map<std::string, int64_t, MetadataKeyLess> metadata_deltas;
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

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
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
      metadata_worker_exited_(false),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      metadata_total_entries_(0),
      metadata_distinct_keys_(0),
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
  env_->StartThread(&DBImpl::MetadataBGWork, this);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  metadata_work_signal_.SignalAll();
  while (background_compaction_scheduled_) {
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
          keep = ((number >= std::min(versions_->LogNumber(),
                                      versions_->metadata_wal_number())) ||
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

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  if (versions_->has_metadata_checkpoint()) {
    metadata_mseq_ = versions_->metadata_checkpoint_mseq();
    metadata_total_entries_ = versions_->metadata_total_entries();
    metadata_distinct_keys_ = versions_->metadata_distinct_keys();
  } else {
    metadata_needs_rebuild_ = true;
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
    return status;
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
      if (replay_metadata || !versions_->has_metadata_checkpoint()) {
        metadata_needs_rebuild_ = true;
      }
      if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
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
  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kStatsQuery);
  metadata_profile_.Count(MetadataProfileCounter::kStatsQueries);
  (void)options;  // Statistics describe the current published state.
  if (stats == nullptr) {
    return Status::InvalidArgument("stats must not be null");
  }
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

Status DBImpl::LogAndApply(VersionEdit* edit) {
  mutex_.AssertHeld();
  while (manifest_commit_in_progress_) {
    background_work_finished_signal_.Wait();
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
  if (!metadata_needs_rebuild_) return Status::OK();
  MetadataProfile::Timer timer(&metadata_profile_,
                               MetadataProfileStage::kRebuild, true);
  metadata_profile_.Count(MetadataProfileCounter::kRebuilds);

  // Rebuild using bounded sorted runs. The primary iterator exposes every
  // physical internal record, including overwritten values and tombstones.
  const std::vector<MetadataFileMeta> old_files = versions_->metadata_files();
  const std::vector<MetadataFileMeta> old_wal_files = metadata_wal_files_;
  uint64_t rebuilt_mseq =
      std::max(metadata_mseq_, metadata_recovery_max_mseq_);
  for (const MetadataCommitMeta& commit : versions_->metadata_commits()) {
    rebuilt_mseq = std::max(rebuilt_mseq, commit.mseq);
  }
  const size_t run_budget = options_.metadata_workspace_size;
  Options table_options = metadata_index_->TableOptions();
  std::vector<MetadataFileMeta> runs;
  std::vector<uint64_t> output_numbers;
  std::map<std::string, uint64_t, MetadataKeyLess> chunk;
  size_t chunk_bytes = 0;

  auto allocate_output_number = [&]() {
    mutex_.Lock();
    const uint64_t number = versions_->NewFileNumber();
    pending_outputs_.insert(number);
    mutex_.Unlock();
    return number;
  };

  mutex_.Unlock();
  Iterator* primary = TEST_NewInternalIterator();
  Status s;

  auto write_run = [&]() -> Status {
    if (chunk.empty()) return Status::OK();
    const uint64_t number = allocate_output_number();
    output_numbers.push_back(number);
    WritableFile* output = nullptr;
    Status run_status =
        env_->NewWritableFile(MetadataFileName(dbname_, number), &output);
    MetadataFileMeta run;
    run.kind = kMetadataIndex;
    run.number = number;
    run.min_mseq = rebuilt_mseq;
    run.max_mseq = rebuilt_mseq;
    bool first = true;
    if (run_status.ok()) {
      TableBuilder builder(table_options, output);
      for (const auto& entry : chunk) {
        MetadataValue value;
        value.mseq = rebuilt_mseq;
        value.count = entry.second;
        builder.Add(entry.first, MetadataIndex::EncodeValue(value));
        if (first) {
          run.smallest = entry.first;
          first = false;
        }
        run.largest = entry.first;
      }
      run_status = builder.Finish();
      run.file_size = builder.FileSize();
      if (run_status.ok()) run_status = output->Sync();
      if (run_status.ok()) run_status = output->Close();
      if (!run_status.ok()) builder.Abandon();
    }
    delete output;
    if (run_status.ok()) {
      runs.push_back(run);
      chunk.clear();
      chunk_bytes = 0;
    }
    return run_status;
  };

  // Keep recovery's merge workspace and number of open metadata tables
  // bounded even when a small memory budget produces many sorted runs.
  auto merge_run_group = [&](const std::vector<MetadataFileMeta>& inputs,
                             MetadataFileMeta* merged_file) -> Status {
    const uint64_t number = allocate_output_number();
    output_numbers.push_back(number);
    WritableFile* output = nullptr;
    Status merge_status =
        env_->NewWritableFile(MetadataFileName(dbname_, number), &output);
    Iterator* merged = metadata_index_->NewIterator(inputs, false);
    merged_file->kind = kMetadataIndex;
    merged_file->number = number;
    merged_file->min_mseq = rebuilt_mseq;
    merged_file->max_mseq = rebuilt_mseq;
    bool have_key = false;
    if (merge_status.ok()) {
      TableBuilder builder(table_options, output);
      merged->SeekToFirst();
      while (merged->Valid() && merge_status.ok()) {
        const std::string key = merged->key().ToString();
        uint64_t count = 0;
        while (merged->Valid() && merged->key() == Slice(key)) {
          MetadataValue value;
          if (!MetadataIndex::DecodeValue(merged->value(), &value) ||
              std::numeric_limits<uint64_t>::max() - count < value.count) {
            merge_status =
                Status::Corruption("invalid metadata rebuild run group");
            break;
          }
          count += value.count;
          merged->Next();
        }
        if (!merge_status.ok()) break;
        MetadataValue value;
        value.mseq = rebuilt_mseq;
        value.count = count;
        builder.Add(key, MetadataIndex::EncodeValue(value));
        if (!have_key) {
          merged_file->smallest = key;
          have_key = true;
        }
        merged_file->largest = key;
      }
      if (merge_status.ok()) merge_status = merged->status();
      if (merge_status.ok()) merge_status = builder.Finish();
      merged_file->file_size = builder.FileSize();
      if (merge_status.ok()) merge_status = output->Sync();
      if (merge_status.ok()) merge_status = output->Close();
      if (!merge_status.ok()) builder.Abandon();
    }
    delete output;
    delete merged;
    return merge_status;
  };

  uint64_t rebuilt_records = 0;
  primary->SeekToFirst();
  while (primary->Valid() && s.ok()) {
    ++rebuilt_records;
    ParsedInternalKey parsed;
    if (!ParseInternalKey(primary->key(), &parsed)) {
      s = Status::Corruption("invalid internal key during metadata rebuild");
      break;
    }
    std::string key(parsed.user_key.data(), parsed.user_key.size());
    auto iter = chunk.find(key);
    if (iter == chunk.end()) {
      chunk_bytes += key.size() + sizeof(uint64_t) + 64;
      iter = chunk.emplace(std::move(key), 0).first;
    }
    if (iter->second == std::numeric_limits<uint64_t>::max()) {
      s = Status::Corruption("per-key metadata count overflow during rebuild");
      break;
    }
    ++iter->second;
    primary->Next();
    if (chunk_bytes >= run_budget) s = write_run();
  }
  if (s.ok()) s = primary->status();
  if (s.ok()) s = write_run();
  delete primary;

  const size_t kMetadataMergeFanIn = 8;
  while (s.ok() && runs.size() > kMetadataMergeFanIn) {
    std::vector<MetadataFileMeta> next_runs;
    next_runs.reserve((runs.size() + kMetadataMergeFanIn - 1) /
                      kMetadataMergeFanIn);
    for (size_t begin = 0; begin < runs.size() && s.ok();
         begin += kMetadataMergeFanIn) {
      const size_t end =
          std::min(runs.size(), begin + kMetadataMergeFanIn);
      if (end - begin == 1) {
        next_runs.push_back(runs[begin]);
        continue;
      }
      std::vector<MetadataFileMeta> group(runs.begin() + begin,
                                          runs.begin() + end);
      MetadataFileMeta merged_file;
      s = merge_run_group(group, &merged_file);
      if (!s.ok()) break;
      next_runs.push_back(merged_file);
      for (const MetadataFileMeta& old_file : group) {
        metadata_index_->Evict(old_file.number);
        env_->RemoveFile(MetadataFileName(dbname_, old_file.number));
        mutex_.Lock();
        pending_outputs_.erase(old_file.number);
        mutex_.Unlock();
      }
    }
    if (s.ok()) runs.swap(next_runs);
  }

  MetadataFileMeta rebuilt_file;
  bool have_rebuilt_file = false;
  uint64_t rebuilt_total = 0;
  uint64_t rebuilt_distinct = 0;
  uint64_t final_number = 0;
  if (s.ok() && !runs.empty()) {
    final_number = allocate_output_number();
    output_numbers.push_back(final_number);
    WritableFile* output = nullptr;
    s = env_->NewWritableFile(MetadataFileName(dbname_, final_number), &output);
    Iterator* merged = metadata_index_->NewIterator(runs, false);
    rebuilt_file.kind = kMetadataIndex;
    rebuilt_file.number = final_number;
    rebuilt_file.min_mseq = std::numeric_limits<uint64_t>::max();
    rebuilt_file.max_mseq = 0;
    bool have_key = false;
    if (s.ok()) {
      TableBuilder builder(table_options, output);
      merged->SeekToFirst();
      while (merged->Valid() && s.ok()) {
        const std::string key = merged->key().ToString();
        uint64_t count = 0;
        uint64_t value_mseq = 0;
        while (merged->Valid() && merged->key() == Slice(key)) {
          MetadataValue value;
          if (!MetadataIndex::DecodeValue(merged->value(), &value)) {
            s = Status::Corruption("invalid metadata rebuild run");
            break;
          }
          if (std::numeric_limits<uint64_t>::max() - count < value.count) {
            s = Status::Corruption("per-key metadata count overflow in merge");
            break;
          }
          count += value.count;
          value_mseq = std::max(value_mseq, value.mseq);
          merged->Next();
        }
        if (!s.ok()) break;
        if (std::numeric_limits<uint64_t>::max() - rebuilt_total < count ||
            rebuilt_distinct == std::numeric_limits<uint64_t>::max()) {
          s = Status::Corruption("metadata totals overflow during rebuild");
          break;
        }
        rebuilt_total += count;
        ++rebuilt_distinct;
        MetadataValue latest;
        latest.mseq = value_mseq;
        latest.count = count;
        builder.Add(key, MetadataIndex::EncodeValue(latest));
        if (!have_key) {
          rebuilt_file.smallest = key;
          have_key = true;
        }
        rebuilt_file.largest = key;
        rebuilt_file.min_mseq = std::min(rebuilt_file.min_mseq, value_mseq);
        rebuilt_file.max_mseq = std::max(rebuilt_file.max_mseq, value_mseq);
      }
      if (s.ok()) s = merged->status();
      if (s.ok()) s = builder.Finish();
      rebuilt_file.file_size = builder.FileSize();
      if (s.ok()) s = output->Sync();
      if (s.ok()) s = output->Close();
      if (!s.ok()) builder.Abandon();
      have_rebuilt_file = have_key && s.ok();
    }
    delete output;
    delete merged;
  }

  mutex_.Lock();
  bool manifest_attempted = false;
  if (s.ok()) s = SyncMetadataWALPrefix(logfile_number_);
  if (s.ok()) {
    VersionEdit edit;
    for (const MetadataFileMeta& old_file : old_files) {
      edit.RemoveMetadataFile(old_file.number);
    }
    if (have_rebuilt_file) edit.AddMetadataFile(rebuilt_file);
    edit.SetMetadataCheckpoint(rebuilt_mseq, rebuilt_total,
                               rebuilt_distinct, logfile_number_);
    manifest_attempted = true;
    s = LogAndApply(&edit);
  }

  for (uint64_t number : output_numbers) pending_outputs_.erase(number);
  if (s.ok()) {
    metadata_total_entries_ = rebuilt_total;
    metadata_distinct_keys_ = rebuilt_distinct;
    metadata_mseq_ = rebuilt_mseq;
    metadata_recovery_max_mseq_ = rebuilt_mseq;
    metadata_wal_bytes_ = 0;
    metadata_wal_checkpoint_bytes_ = 0;
    metadata_index_->ResetAfterRebuild();
    metadata_profile_.Count(MetadataProfileCounter::kRebuiltRecords,
                           rebuilt_records);
    for (const MetadataFileMeta& file : old_wal_files) {
      metadata_index_->Evict(file.number);
      pending_outputs_.erase(file.number);
    }
    metadata_wal_files_.clear();
    ++metadata_wal_files_revision_;
    metadata_needs_rebuild_ = false;
    metadata_rebuild_only_error_ = false;
    RemoveObsoleteFiles();
  } else if (!manifest_attempted) {
    for (uint64_t number : output_numbers) {
      env_->RemoveFile(MetadataFileName(dbname_, number));
    }
  }
  return s;
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
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
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
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
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

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
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
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
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
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
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
    pending_outputs_.erase(number);
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
      &metadata_profile_, MetadataProfileStage::kPrimaryCompactionCommit,
      true);
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  if (!bg_error_.ok()) return bg_error_;

  auto add_primary_file_edits = [&]() {
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();
    for (const CompactionState::Output& out : compact->outputs) {
      compact->compaction->edit()->AddFile(level + 1, out.number,
                                           out.file_size, out.smallest,
                                           out.largest);
    }
  };
  auto finish_prepare = [&]() {
    if (metadata_compaction_prepare_ == compact) {
      metadata_compaction_prepare_ = nullptr;
      metadata_compaction_prepare_in_progress_ = false;
      background_work_finished_signal_.SignalAll();
      metadata_work_signal_.SignalAll();
    }
  };

  // A count-preserving compaction has no metadata transaction to coordinate.
  // It can install its primary files without serializing foreground writes.
  if (compact->metadata_delta_runs.empty()) {
    add_primary_file_edits();
    Status s = LogAndApply(compact->compaction->edit());
    if (s.ok())
      metadata_profile_.Count(MetadataProfileCounter::kPrimaryCompactions);
    return s;
  }

  // Let any existing metadata flush/compaction finish before taking the
  // baseline file view. Once registered, the metadata worker waits until this
  // compaction has installed or abandoned its prepared files.
  while (metadata_storage_work_in_progress_ || write_log_in_progress_) {
    background_work_finished_signal_.Wait();
  }

  for (const MetadataFileMeta& run : compact->metadata_delta_runs) {
    if (run.smallest.empty()) continue;
    if (!compact->has_metadata_delta_range) {
      compact->metadata_delta_smallest = run.smallest;
      compact->metadata_delta_largest = run.largest;
      compact->has_metadata_delta_range = true;
    } else {
      if (BytewiseComparator()->Compare(run.smallest,
                                        compact->metadata_delta_smallest) < 0) {
        compact->metadata_delta_smallest = run.smallest;
      }
      if (BytewiseComparator()->Compare(run.largest,
                                        compact->metadata_delta_largest) > 0) {
        compact->metadata_delta_largest = run.largest;
      }
    }
  }
  if (!compact->has_metadata_delta_range) {
    add_primary_file_edits();
    Status s = LogAndApply(compact->compaction->edit());
    if (s.ok())
      metadata_profile_.Count(MetadataProfileCounter::kPrimaryCompactions);
    return s;
  }

  Status status = ReserveMetadataMSeq(&compact->metadata_baseline_mseq);
  if (!status.ok()) return status;
  metadata_compaction_prepare_ = compact;
  metadata_compaction_prepare_in_progress_ = true;
  MetadataProfile::Timer prepare_timer(
      &metadata_profile_, MetadataProfileStage::kPrimaryCompactionPrepare,
      true);
  const std::vector<MetadataFileMeta> baseline_sources = MetadataLookupFiles();

  const size_t workspace = std::min<size_t>(
      64 << 10, std::max<size_t>(1, options_.metadata_workspace_size / 4));
  const uint64_t baseline_number = versions_->NewFileNumber();
  pending_outputs_.insert(baseline_number);
  compact->metadata_delta_output_numbers.push_back(baseline_number);
  compact->prepared_baseline.kind = kMetadataIndex;
  compact->prepared_baseline.number = baseline_number;
  compact->prepared_baseline.min_mseq = compact->metadata_baseline_mseq;
  compact->prepared_baseline.max_mseq = compact->metadata_baseline_mseq;
  const std::string baseline_filename = MetadataFileName(dbname_, baseline_number);

  WritableFile* baseline_file = nullptr;
  std::unique_ptr<TableBuilder> baseline_builder;
  Iterator* deltas = metadata_index_->NewIterator(
      compact->metadata_delta_runs, false);
  mutex_.Unlock();
  status = env_->NewWritableFile(baseline_filename, &baseline_file);
  if (status.ok()) {
    baseline_builder.reset(
        new TableBuilder(metadata_index_->TableOptions(), baseline_file));
  }
  mutex_.Lock();

  auto apply_delta = [](uint64_t old_count, int64_t delta,
                        uint64_t* new_count) -> Status {
    if (delta < 0) {
      const uint64_t magnitude =
          static_cast<uint64_t>(-(delta + 1)) + 1;
      if (old_count < magnitude) {
        return Status::Corruption("metadata compaction count underflow");
      }
      *new_count = old_count - magnitude;
    } else {
      const uint64_t increment = static_cast<uint64_t>(delta);
      if (std::numeric_limits<uint64_t>::max() - old_count < increment) {
        return Status::Corruption("metadata compaction count overflow");
      }
      *new_count = old_count + increment;
    }
    return Status::OK();
  };

  bool baseline_have_key = false;
  deltas->SeekToFirst();
  while (status.ok() && deltas->Valid()) {
    std::vector<std::string> chunk_keys;
    std::vector<int64_t> chunk_deltas;
    size_t chunk_bytes = 0;
    while (status.ok() && deltas->Valid() && chunk_keys.size() < 256) {
      const std::string key = deltas->key().ToString();
      int64_t delta_sum = 0;
      while (deltas->Valid() && deltas->key() == Slice(key)) {
        int64_t delta;
        if (!DecodeMetadataDelta(deltas->value(), &delta) ||
            (delta > 0 &&
             delta_sum > std::numeric_limits<int64_t>::max() - delta) ||
            (delta < 0 &&
             delta_sum < std::numeric_limits<int64_t>::min() - delta)) {
          status = Status::Corruption("metadata compaction delta overflow");
          break;
        }
        delta_sum += delta;
        deltas->Next();
      }
      if (!status.ok()) break;
      if (delta_sum == 0) continue;
      const size_t charge = key.size() + sizeof(int64_t) + 16;
      if (!chunk_keys.empty() &&
          charge > workspace - std::min(workspace, chunk_bytes)) {
        // The current key is already consumed. Keep it for the next chunk by
        // processing this chunk immediately after appending it.
      }
      chunk_bytes = charge > std::numeric_limits<size_t>::max() - chunk_bytes
                        ? std::numeric_limits<size_t>::max()
                        : chunk_bytes + charge;
      chunk_keys.push_back(key);
      chunk_deltas.push_back(delta_sum);
      if (chunk_bytes >= workspace) break;
    }
    if (!status.ok()) break;
    if (chunk_keys.empty()) continue;

    std::vector<Slice> lookup_keys;
    lookup_keys.reserve(chunk_keys.size());
    for (const std::string& key : chunk_keys) lookup_keys.emplace_back(key);
    std::vector<MetadataValue> old_values;
    std::vector<bool> old_found;
    status = metadata_index_->LookupMany(baseline_sources, lookup_keys,
                                         &old_values, &old_found);
    if (!status.ok()) {
      metadata_needs_rebuild_ = true;
      metadata_rebuild_only_error_ = true;
      break;
    }

    std::vector<std::pair<std::string, MetadataValue>> after_values;
    after_values.reserve(chunk_keys.size());
    for (size_t i = 0; i < chunk_keys.size(); ++i) {
      const uint64_t old_count = old_found[i] ? old_values[i].count : 0;
      uint64_t after_count = 0;
      status = apply_delta(old_count, chunk_deltas[i], &after_count);
      if (!status.ok()) break;
      MetadataValue after;
      after.mseq = compact->metadata_baseline_mseq;
      after.count = after_count;
      after_values.emplace_back(chunk_keys[i], after);

      if (chunk_deltas[i] > 0) {
        const uint64_t amount = static_cast<uint64_t>(chunk_deltas[i]);
        if (std::numeric_limits<uint64_t>::max() -
                compact->delta_positive_records < amount) {
          status = Status::Corruption("metadata compaction total overflow");
          break;
        }
        compact->delta_positive_records += amount;
      } else {
        const uint64_t amount =
            static_cast<uint64_t>(-(chunk_deltas[i] + 1)) + 1;
        if (std::numeric_limits<uint64_t>::max() -
                compact->delta_negative_records < amount) {
          status = Status::Corruption("metadata compaction total overflow");
          break;
        }
        compact->delta_negative_records += amount;
      }
      if (!compact->has_first_metadata_delta) {
        compact->first_metadata_delta_key = chunk_keys[i];
        compact->first_metadata_after_count = after_count;
        compact->has_first_metadata_delta = true;
      }
      compact->has_metadata_changes = true;
    }
    if (!status.ok()) break;
    mutex_.Unlock();
    for (const auto& entry : after_values) {
      baseline_builder->Add(entry.first,
                            MetadataIndex::EncodeValue(entry.second));
      if (!baseline_have_key) {
        compact->prepared_baseline.smallest = entry.first;
        baseline_have_key = true;
      }
      compact->prepared_baseline.largest = entry.first;
    }
    mutex_.Lock();
  }
  if (status.ok()) status = deltas->status();
  mutex_.Unlock();
  if (status.ok() && baseline_builder != nullptr) {
    status = baseline_builder->Finish();
    compact->prepared_baseline.file_size = baseline_builder->FileSize();
  }
  if (status.ok() && baseline_file != nullptr) status = baseline_file->Sync();
  if (status.ok() && baseline_file != nullptr) status = baseline_file->Close();
  if (!status.ok() && baseline_builder != nullptr) baseline_builder->Abandon();
  delete baseline_file;
  delete deltas;
  mutex_.Lock();

  if (status.ok() && baseline_have_key) {
    compact->has_prepared_baseline = true;
  } else if (status.ok()) {
    pending_outputs_.erase(baseline_number);
    env_->RemoveFile(baseline_filename);
  }

  if (!status.ok()) {
    finish_prepare();
    return status;
  }

  // If every key's net record-count delta cancelled, the primary files can be
  // installed without a statistics event.
  if (!compact->has_metadata_changes) {
    prepare_timer.Stop();
    finish_prepare();
    add_primary_file_edits();
    status = LogAndApply(compact->compaction->edit());
    if (status.ok())
      metadata_profile_.Count(MetadataProfileCounter::kPrimaryCompactions);
    return status;
  }

  // Close the range to new foreground commits and wait for any group that
  // passed admission before this point. A large tail naturally stops new
  // overlapping groups at their pre-WAL admission check.
  compact->metadata_journal_finishing = true;
  MetadataProfile::Timer tail_timer(
      &metadata_profile_, MetadataProfileStage::kPrimaryCompactionTail, true);
  while (write_log_in_progress_) {
    background_work_finished_signal_.Wait();
  }
  status = ReserveMetadataMSeq(&compact->metadata_correction_mseq);
  if (!status.ok()) {
    finish_prepare();
    return status;
  }

  const uint64_t correction_number = versions_->NewFileNumber();
  pending_outputs_.insert(correction_number);
  compact->metadata_delta_output_numbers.push_back(correction_number);
  const std::string correction_filename =
      MetadataFileName(dbname_, correction_number);
  WritableFile* correction_file = nullptr;
  std::unique_ptr<TableBuilder> correction_builder;
  compact->prepared_correction.kind = kMetadataIndex;
  compact->prepared_correction.number = correction_number;
  compact->prepared_correction.min_mseq = compact->metadata_correction_mseq;
  compact->prepared_correction.max_mseq = compact->metadata_correction_mseq;

  mutex_.Unlock();
  status = env_->NewWritableFile(correction_filename, &correction_file);
  if (status.ok()) {
    correction_builder.reset(
        new TableBuilder(metadata_index_->TableOptions(), correction_file));
  }
  mutex_.Lock();

  Iterator* correction_deltas = metadata_index_->NewIterator(
      compact->metadata_delta_runs, false);
  Iterator* baseline_iter =
      metadata_index_->NewIterator({compact->prepared_baseline}, false);
  correction_deltas->SeekToFirst();
  baseline_iter->SeekToFirst();
  bool correction_have_key = false;
  uint64_t tail_touched_keys = 0;
  uint64_t distinct_added = 0;
  uint64_t distinct_removed = 0;
  auto count_transition = [&](uint64_t before, uint64_t after) -> Status {
    if (before == 0 && after != 0) {
      if (distinct_added == std::numeric_limits<uint64_t>::max()) {
        return Status::Corruption("metadata distinct count overflow");
      }
      ++distinct_added;
    } else if (before != 0 && after == 0) {
      if (distinct_removed == std::numeric_limits<uint64_t>::max()) {
        return Status::Corruption("metadata distinct count overflow");
      }
      ++distinct_removed;
    }
    return Status::OK();
  };

  while (status.ok() && correction_deltas->Valid()) {
    struct CorrectionEntry {
      std::string key;
      int64_t delta;
      uint64_t before;
      uint64_t baseline_after;
      bool may_be_touched;
    };
    std::vector<CorrectionEntry> chunk;
    size_t chunk_bytes = 0;
    while (correction_deltas->Valid() && chunk.size() < 256) {
      const std::string key = correction_deltas->key().ToString();
      int64_t delta_sum = 0;
      while (correction_deltas->Valid() &&
             correction_deltas->key() == Slice(key)) {
        int64_t delta;
        if (!DecodeMetadataDelta(correction_deltas->value(), &delta) ||
            (delta > 0 &&
             delta_sum > std::numeric_limits<int64_t>::max() - delta) ||
            (delta < 0 &&
             delta_sum < std::numeric_limits<int64_t>::min() - delta)) {
          status = Status::Corruption("metadata compaction delta overflow");
          break;
        }
        delta_sum += delta;
        correction_deltas->Next();
      }
      if (!status.ok()) break;
      if (delta_sum == 0) continue;
      if (!baseline_iter->Valid() || baseline_iter->key() != Slice(key)) {
        status = Status::Corruption("metadata prepared baseline is incomplete");
        break;
      }
      MetadataValue baseline_after;
      if (!MetadataIndex::DecodeValue(baseline_iter->value(),
                                      &baseline_after) ||
          baseline_after.mseq != compact->metadata_baseline_mseq) {
        status = Status::Corruption("invalid metadata prepared baseline");
        break;
      }
      baseline_iter->Next();
      uint64_t baseline_before = 0;
      if (delta_sum > 0) {
        const uint64_t amount = static_cast<uint64_t>(delta_sum);
        if (baseline_after.count < amount) {
          status = Status::Corruption("metadata baseline underflow");
          break;
        }
        baseline_before = baseline_after.count - amount;
      } else {
        const uint64_t amount =
            static_cast<uint64_t>(-(delta_sum + 1)) + 1;
        if (std::numeric_limits<uint64_t>::max() - baseline_after.count <
            amount) {
          status = Status::Corruption("metadata baseline overflow");
          break;
        }
        baseline_before = baseline_after.count + amount;
      }
      bool may_be_touched =
          compact->metadata_journal_values.find(key) !=
          compact->metadata_journal_values.end();
      for (const MetadataFileMeta& journal_file :
           compact->metadata_journal_files) {
        if (journal_file.max_mseq > compact->metadata_baseline_mseq &&
            MetadataKeyInRange(key, journal_file.smallest,
                               journal_file.largest)) {
          may_be_touched = true;
          break;
        }
      }
      const size_t charge = key.size() + sizeof(CorrectionEntry) + 16;
      chunk_bytes = charge > std::numeric_limits<size_t>::max() - chunk_bytes
                        ? std::numeric_limits<size_t>::max()
                        : chunk_bytes + charge;
      chunk.push_back({key, delta_sum, baseline_before,
                       baseline_after.count, may_be_touched});
      if (chunk_bytes >= workspace) break;
    }
    if (!status.ok()) break;
    if (chunk.empty()) continue;

    std::vector<Slice> candidate_keys;
    for (size_t i = 0; i < chunk.size(); ++i) {
      if (chunk[i].may_be_touched) {
        candidate_keys.emplace_back(chunk[i].key);
      }
    }
    std::vector<MetadataValue> latest_values;
    std::vector<bool> latest_found;
    if (!candidate_keys.empty()) {
      const std::vector<MetadataFileMeta>& current_files = MetadataLookupFiles();
      status = metadata_index_->LookupMany(current_files, candidate_keys,
                                           &latest_values, &latest_found);
      if (!status.ok()) {
        metadata_needs_rebuild_ = true;
        metadata_rebuild_only_error_ = true;
        break;
      }
    }

    std::vector<std::pair<std::string, MetadataValue>> corrections;
    corrections.reserve(candidate_keys.size());
    size_t candidate_index = 0;
    for (size_t i = 0; i < chunk.size(); ++i) {
      uint64_t before = chunk[i].before;
      uint64_t after = chunk[i].baseline_after;
      bool touched = false;
      if (chunk[i].may_be_touched) {
        const bool found_value = latest_found[candidate_index];
        const MetadataValue latest = latest_values[candidate_index];
        ++candidate_index;
        if (found_value && latest.mseq > compact->metadata_baseline_mseq) {
          before = latest.count;
          status = apply_delta(before, chunk[i].delta, &after);
          if (!status.ok()) break;
          touched = true;
        }
      }
      status = count_transition(before, after);
      if (!status.ok()) break;
      if (!compact->has_first_metadata_delta) {
        compact->first_metadata_delta_key = chunk[i].key;
        compact->has_first_metadata_delta = true;
      }
      if (compact->first_metadata_delta_key == chunk[i].key) {
        compact->first_metadata_after_count = after;
      }
      if (touched) {
        MetadataValue corrected;
        corrected.mseq = compact->metadata_correction_mseq;
        corrected.count = after;
        corrections.emplace_back(chunk[i].key, corrected);
      }
    }
    if (!status.ok()) break;
    tail_touched_keys += corrections.size();
    mutex_.Unlock();
    for (const auto& correction : corrections) {
      correction_builder->Add(correction.first,
                              MetadataIndex::EncodeValue(correction.second));
      if (!correction_have_key) {
        compact->prepared_correction.smallest = correction.first;
        correction_have_key = true;
      }
      compact->prepared_correction.largest = correction.first;
    }
    mutex_.Lock();
  }
  if (status.ok()) status = correction_deltas->status();
  if (status.ok()) status = baseline_iter->status();
  mutex_.Unlock();
  if (status.ok() && correction_builder != nullptr) {
    status = correction_builder->Finish();
    compact->prepared_correction.file_size = correction_builder->FileSize();
  }
  if (status.ok() && correction_file != nullptr) status = correction_file->Sync();
  if (status.ok() && correction_file != nullptr) status = correction_file->Close();
  if (!status.ok() && correction_builder != nullptr) correction_builder->Abandon();
  delete correction_file;
  delete correction_deltas;
  delete baseline_iter;
  mutex_.Lock();

  if (status.ok() && correction_have_key) {
    compact->has_prepared_correction = true;
    metadata_profile_.Count(MetadataProfileCounter::kCompactionTailKeys,
                            tail_touched_keys);
    metadata_profile_.Count(MetadataProfileCounter::kCompactionJournalFiles,
                            compact->metadata_journal_files.size());
    metadata_profile_.Count(MetadataProfileCounter::kCompactionTailBytes,
                            compact->prepared_correction.file_size);
  } else if (status.ok()) {
    pending_outputs_.erase(correction_number);
    env_->RemoveFile(correction_filename);
  }
  if (!status.ok()) {
    prepare_timer.Stop();
    tail_timer.Stop();
    finish_prepare();
    return status;
  }

  // Sync the WAL prefix while foreground writers can still make progress.
  // Writes that append during this sync are caught by the short tail sync in
  // the final commit gate below.
  prepare_timer.Stop();
  tail_timer.Stop();
  status = SyncMetadataWALPrefix(logfile_number_);
  if (!status.ok()) {
    finish_prepare();
    return status;
  }

  // The final gate covers scalar reconciliation, any WAL tail sync, and one
  // Manifest edit. The already prepared baseline is also the durable event
  // witness, so no extra change file needs to be written under this gate.
  MetadataProfile::Timer gate_timer(
      &metadata_profile_, MetadataProfileStage::kPrimaryCompactionGate, true);
  BeginMetadataCommit();
  while (write_log_in_progress_) {
    background_work_finished_signal_.Wait();
  }
  const uint64_t previous_mseq = metadata_mseq_;
  uint64_t commit_mseq = 0;
  status = ReserveMetadataMSeq(&commit_mseq);
  uint64_t next_total = metadata_total_entries_;
  uint64_t next_distinct = metadata_distinct_keys_;
  if (status.ok()) {
    if (next_total < compact->delta_negative_records ||
        std::numeric_limits<uint64_t>::max() -
                (next_total - compact->delta_negative_records) <
            compact->delta_positive_records) {
      status = Status::Corruption("metadata compaction total overflow");
    } else {
      next_total = next_total - compact->delta_negative_records +
                   compact->delta_positive_records;
    }
  }
  if (status.ok()) {
    if (next_distinct < distinct_removed ||
        std::numeric_limits<uint64_t>::max() -
                (next_distinct - distinct_removed) <
            distinct_added) {
      status = Status::Corruption("metadata compaction distinct overflow");
    } else {
      next_distinct = next_distinct - distinct_removed + distinct_added;
    }
  }
  if (status.ok()) status = SyncMetadataWALPrefix(logfile_number_);

  if (status.ok()) {
    compact->prepared_baseline.max_mseq = commit_mseq;
    if (compact->has_prepared_correction) {
      compact->prepared_correction.max_mseq = commit_mseq;
    }
    add_primary_file_edits();
    if (compact->has_prepared_baseline)
      compact->compaction->edit()->AddMetadataFile(compact->prepared_baseline);
    if (compact->has_prepared_correction)
      compact->compaction->edit()->AddMetadataFile(
          compact->prepared_correction);
    MetadataCommitMeta commit;
    commit.file_number = compact->prepared_baseline.number;
    commit.mseq = commit_mseq;
    commit.prev_mseq = previous_mseq;
    commit.total_entries = next_total;
    commit.distinct_keys = next_distinct;
    commit.wal_number = versions_->metadata_wal_number();
    compact->compaction->edit()->AddMetadataCommit(commit);
    status = LogAndApply(compact->compaction->edit());
    if (status.ok()) {
      metadata_profile_.Count(MetadataProfileCounter::kPrimaryCompactions);
      metadata_total_entries_ = next_total;
      metadata_distinct_keys_ = next_distinct;
      metadata_mseq_ = commit_mseq;
      metadata_index_->InvalidateAll();
      MaybeFreezeMetadata();
    }
  }

  EndMetadataCommit();
  gate_timer.Stop();
  finish_prepare();
  return status;
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
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
        while (deltas->Valid() && deltas->key() == Slice(key)) {
          int64_t delta;
          if (!DecodeMetadataDelta(deltas->value(), &delta) ||
              (delta > 0 && sum > std::numeric_limits<int64_t>::max() - delta) ||
              (delta < 0 && sum < std::numeric_limits<int64_t>::min() - delta)) {
            merge_status =
                Status::Corruption("metadata compaction delta merge overflow");
            break;
          }
          sum += delta;
          deltas->Next();
        }
        if (!merge_status.ok()) break;
        if (sum == 0) continue;
        builder.Add(key, EncodeMetadataDelta(sum));
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
        if (entry.second == 0) continue;
        builder.Add(entry.first, EncodeMetadataDelta(entry.second));
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

  auto add_metadata_delta = [&](const std::string& key,
                                int64_t adjustment) -> Status {
    auto iter = compact->metadata_deltas.find(key);
    if (iter == compact->metadata_deltas.end()) {
      compact->metadata_delta_bytes += key.size() + sizeof(int64_t) + 64;
      iter = compact->metadata_deltas.emplace(key, 0).first;
    }
    if ((adjustment > 0 &&
         iter->second > std::numeric_limits<int64_t>::max() - adjustment) ||
        (adjustment < 0 &&
         iter->second < std::numeric_limits<int64_t>::min() - adjustment)) {
      return Status::Corruption("metadata compaction delta overflow");
    }
    iter->second += adjustment;
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

  auto append_sorted_delta = [&](const std::string& key,
                                 int64_t delta) -> Status {
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
    sorted_delta_builder->Add(key, EncodeMetadataDelta(delta));
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

    Slice key = input->key();
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
          status = append_sorted_delta(current_user_key, current_key_delta);
          if (!status.ok()) break;
        }
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        current_key_delta = 0;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (stream_sorted_deltas) {
        if (current_key_delta == std::numeric_limits<int64_t>::min()) {
          status = Status::Corruption("metadata compaction delta overflow");
          break;
        }
        --current_key_delta;
      } else {
        status = add_metadata_delta(raw_key, -1);
        if (!status.ok()) break;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
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

    if (!drop) {
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
      } else {
        std::string raw_key(ikey.user_key.data(), ikey.user_key.size());
        status = add_metadata_delta(raw_key, 1);
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
    status = append_sorted_delta(current_user_key, current_key_delta);
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
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
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
  return status;
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
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

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
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
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
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
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
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MetadataProfile::Timer queue_wait_timer(
      &metadata_profile_, MetadataProfileStage::kWriteQueueWait);
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  queue_wait_timer.Stop();
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    metadata_profile_.Count(MetadataProfileCounter::kWriteBatches);
    metadata_profile_.Count(MetadataProfileCounter::kWriteOperations,
                           WriteBatchInternal::Count(write_batch));
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

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
    uint64_t next_mseq = metadata_mseq_;
    std::vector<std::pair<std::string, MetadataValue>> metadata_updates;
    MetadataFileMeta metadata_change_file;
    uint64_t metadata_change_file_number = 0;
    bool has_metadata_change_file = false;
    bool wal_attempted = false;
    bool wal_record_appended = false;
    bool wal_record_synced = false;
    uint64_t write_log_number = logfile_number_;

    const uint64_t records_added = batch_counter.total_records();
    if (status.ok() && records_added != 0) {
      status = ReserveMetadataMSeq(&next_mseq);
    }
    if (status.ok() &&
        std::numeric_limits<uint64_t>::max() - next_total_entries <
            records_added) {
      status = Status::Corruption("total entry count overflow");
    }
    if (status.ok()) next_total_entries += records_added;

    const std::vector<MetadataFileMeta>& lookup_files = MetadataLookupFiles();
    const size_t lookup_workspace = std::min<size_t>(
        64 << 10, options_.metadata_workspace_size / 4);
    std::vector<std::string> lookup_chunk_keys;
    std::vector<uint64_t> lookup_chunk_increments;
    size_t lookup_chunk_bytes = 0;
    auto flush_lookup_chunk = [&](TableBuilder* change_builder) -> Status {
      if (lookup_chunk_keys.empty()) return Status::OK();
      std::vector<Slice> keys;
      keys.reserve(lookup_chunk_keys.size());
      for (const std::string& key : lookup_chunk_keys) keys.emplace_back(key);
      std::vector<MetadataValue> previous_values;
      std::vector<bool> previous_found;
      metadata_profile_.Count(MetadataProfileCounter::kWriteDistinctKeys,
                              keys.size());
      Status lookup_status = metadata_index_->LookupMany(
          lookup_files, keys, &previous_values, &previous_found);
      if (!lookup_status.ok()) {
        metadata_needs_rebuild_ = true;
        metadata_rebuild_only_error_ = true;
        return lookup_status;
      }
      for (size_t i = 0; i < keys.size(); ++i) {
        const uint64_t old_count = previous_found[i]
                                       ? previous_values[i].count
                                       : 0;
        const uint64_t increment = lookup_chunk_increments[i];
        if (old_count == 0) {
          if (next_distinct_keys == std::numeric_limits<uint64_t>::max()) {
            return Status::Corruption("distinct key count overflow");
          }
          ++next_distinct_keys;
        }
        if (std::numeric_limits<uint64_t>::max() - old_count < increment) {
          return Status::Corruption("per-key record count overflow");
        }
        MetadataValue updated;
        updated.mseq = next_mseq;
        updated.count = old_count + increment;
        if (change_builder != nullptr) {
          change_builder->Add(keys[i], MetadataIndex::EncodeValue(updated));
          metadata_index_->Invalidate(keys[i]);
        } else {
          metadata_updates.emplace_back(lookup_chunk_keys[i], updated);
        }
      }
      lookup_chunk_keys.clear();
      lookup_chunk_increments.clear();
      lookup_chunk_bytes = 0;
      return Status::OK();
    };
    auto append_lookup_item = [&](const std::string& key, uint64_t increment,
                                  TableBuilder* change_builder) -> Status {
      const size_t item_bytes = key.size() + sizeof(uint64_t) + 16;
      if (!lookup_chunk_keys.empty() &&
          (lookup_chunk_keys.size() >= 256 ||
           item_bytes > lookup_workspace -
                            std::min(lookup_workspace, lookup_chunk_bytes))) {
        Status s = flush_lookup_chunk(change_builder);
        if (!s.ok()) return s;
      }
      lookup_chunk_keys.push_back(key);
      lookup_chunk_increments.push_back(increment);
      lookup_chunk_bytes =
          item_bytes > std::numeric_limits<size_t>::max() - lookup_chunk_bytes
              ? std::numeric_limits<size_t>::max()
              : lookup_chunk_bytes + item_bytes;
      if (lookup_chunk_keys.size() >= 256 ||
          lookup_chunk_bytes >= lookup_workspace) {
        return flush_lookup_chunk(change_builder);
      }
      return Status::OK();
    };

    if (status.ok() && batch_counter.spilled()) {
      metadata_change_file_number = versions_->NewFileNumber();
      pending_outputs_.insert(metadata_change_file_number);
      metadata_change_file.kind = kMetadataChange;
      metadata_change_file.number = metadata_change_file_number;
      metadata_change_file.min_mseq = next_mseq;
      metadata_change_file.max_mseq = next_mseq;
      const std::string filename =
          MetadataChangeFileName(dbname_, metadata_change_file_number);
      WritableFile* output = nullptr;
      Iterator* deltas = metadata_index_->NewIterator(batch_runs, false);
      status = env_->NewWritableFile(filename, &output);
      bool have_key = false;
      if (status.ok()) {
        TableBuilder builder(metadata_index_->TableOptions(), output);
        deltas->SeekToFirst();
        while (deltas->Valid() && status.ok()) {
          const std::string key = deltas->key().ToString();
          uint64_t count = 0;
          while (deltas->Valid() && deltas->key() == Slice(key)) {
            MetadataValue delta;
            if (!MetadataIndex::DecodeValue(deltas->value(), &delta) ||
                delta.mseq != 0 ||
                std::numeric_limits<uint64_t>::max() - count < delta.count) {
              status = Status::Corruption("invalid metadata batch delta");
              break;
            }
            count += delta.count;
            deltas->Next();
          }
          if (!status.ok()) break;
          status = append_lookup_item(key, count, &builder);
          if (!status.ok()) break;
          if (!have_key) {
            metadata_change_file.smallest = key;
            have_key = true;
          }
          metadata_change_file.largest = key;
        }
        if (status.ok()) status = flush_lookup_chunk(&builder);
        if (status.ok()) status = deltas->status();
        if (status.ok()) status = builder.Finish();
        metadata_change_file.file_size = builder.FileSize();
        if (status.ok()) status = output->Sync();
        if (status.ok()) status = output->Close();
        if (!status.ok()) builder.Abandon();
      }
      delete output;
      delete deltas;
      if (status.ok() && have_key) {
        has_metadata_change_file = true;
      } else if (status.ok()) {
        status = Status::Corruption("empty metadata batch change file");
      }
    } else if (status.ok()) {
      metadata_updates.reserve(batch_counter.counts.size());
      for (const auto& key_count : batch_counter.counts) {
        status = append_lookup_item(key_count.first, key_count.second.count,
                                    nullptr);
        if (!status.ok()) break;
      }
      if (status.ok()) status = flush_lookup_chunk(nullptr);
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
      wal_record = EncodeMetadataWALRecord(
          WriteBatchInternal::Contents(write_batch), next_mseq, metadata_mseq_,
          next_total_entries, next_distinct_keys, metadata_updates,
          has_metadata_change_file ? metadata_change_file_number : 0);
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
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    if (status.ok()) {
      if (has_metadata_change_file) {
        metadata_wal_files_.push_back(metadata_change_file);
        ++metadata_wal_files_revision_;
        metadata_index_->InvalidateAll();
        metadata_profile_.Count(MetadataProfileCounter::kChangeFileBytes,
                               metadata_change_file.file_size);
        CompactionState* journal = metadata_compaction_prepare_;
        if (journal != nullptr && journal->has_metadata_delta_range &&
            MetadataRangesOverlap(metadata_change_file.smallest,
                                  metadata_change_file.largest,
                                  journal->metadata_delta_smallest,
                                  journal->metadata_delta_largest)) {
          journal->metadata_journal_files.push_back(metadata_change_file);
          metadata_profile_.Count(
              MetadataProfileCounter::kCompactionJournalFiles);
          metadata_profile_.Count(
              MetadataProfileCounter::kCompactionJournalBytes,
              metadata_change_file.file_size);
        }
      } else {
        metadata_index_->Apply(metadata_updates);
        CompactionState* journal = metadata_compaction_prepare_;
        if (journal != nullptr && journal->has_metadata_delta_range) {
          for (const auto& update : metadata_updates) {
            if (!MetadataKeyInRange(update.first,
                                    journal->metadata_delta_smallest,
                                    journal->metadata_delta_largest)) {
              continue;
            }
            auto existing = journal->metadata_journal_values.find(update.first);
            if (existing == journal->metadata_journal_values.end()) {
              const size_t charge = update.first.size() +
                                    sizeof(MetadataValue) + 48;
              journal->metadata_journal_bytes += charge;
              metadata_profile_.Count(
                  MetadataProfileCounter::kCompactionJournalBytes, charge);
              journal->metadata_journal_values.emplace(update.first,
                                                        update.second);
            } else {
              existing->second = update.second;
            }
          }
        }
      }
      metadata_total_entries_ = next_total_entries;
      metadata_distinct_keys_ = next_distinct_keys;
      metadata_mseq_ = next_mseq;
      if (std::numeric_limits<uint64_t>::max() - metadata_wal_bytes_ <
          wal_record.size()) {
        metadata_wal_bytes_ = std::numeric_limits<uint64_t>::max();
      } else {
        metadata_wal_bytes_ += wal_record.size();
      }
      versions_->SetLastSequence(last_sequence);
      MaybeFreezeMetadata();
    } else {
      if (metadata_change_file_number != 0 && !wal_attempted) {
        metadata_index_->Evict(metadata_change_file_number);
        pending_outputs_.erase(metadata_change_file_number);
        env_->RemoveFile(MetadataChangeFileName(dbname_,
                                                metadata_change_file_number));
      }
      RecordBackgroundError(status);
    }
  }

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

  return status;
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
  // TODO: Replace this starter stub with TTL-aware storage and reads.
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

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
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
  } else if (s.ok()) {
    impl->MaybeFreezeMetadata();
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
    impl->metadata_work_signal_.Signal();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
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
