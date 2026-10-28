// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
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
  const char* profiling_trigger = "size";
  std::map<std::string, int64_t> record_deltas;
};

namespace {
class CountBatchKeys : public WriteBatch::Handler {
 public:
  void Put(const Slice& key, const Slice&) override { ++counts_[key.ToString()]; }
  void Delete(const Slice& key) override { ++counts_[key.ToString()]; }

  const std::map<std::string, uint64_t>& counts() const { return counts_; }

 private:
  std::map<std::string, uint64_t> counts_;
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
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
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
  while (background_compaction_scheduled_ || profiling_callbacks_ != 0) {
    background_work_finished_signal_.Wait();
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
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
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
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

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
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
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

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RebuildCurrentRecordCounts() {
  ProfileScope profile("recovery.rebuild_counts");
  mutex_.AssertHeld();
  std::map<std::string, uint64_t> counts;
  uint64_t total = 0;
  auto add_internal_iterator = [&](Iterator* iter) -> Status {
    iter->SeekToFirst();
    while (iter->Valid()) {
      ParsedInternalKey key;
      if (!ParseInternalKey(iter->key(), &key)) {
        Status status =
            Status::Corruption("invalid internal key while rebuilding statistics");
        delete iter;
        return status;
      }
      ++counts[key.user_key.ToString()];
      ++total;
      ProfileScope::Count("recovery.scanned_records");
      ProfileScope::Count("recovery.scanned_encoded_bytes", iter->key().size() + iter->value().size());
      iter->Next();
    }
    Status status = iter->status();
    delete iter;
    return status;
  };

  Status status;
  {
    ProfileScope memory("recovery.merge_memtables");
    status = add_internal_iterator(mem_->NewIterator());
    if (status.ok() && imm_ != nullptr) status = add_internal_iterator(imm_->NewIterator());
    memory.SetStatus(status);
  }
  Version* current = versions_->current();
  for (int level = 0; status.ok() && level < config::kNumLevels; ++level) {
    for (FileMetaData* file : current->files(level)) {
      TableInternalStats stats;
      ProfileScope::Count("recovery.sst_files");
      status = ProfileStatus("recovery.sst_index", [&] {
        return table_cache_->GetInternalStats(file->number, file->file_size, &stats);
      });
      if (!status.ok()) break;
      if (stats.valid) {
        ProfileScope merge_index("recovery.merge_index");
        ProfileScope::Count("recovery.index_files");
        for (const auto& item : stats.key_counts) {
          counts[item.first] += item.second;
          total += item.second;
        }
      } else {
        ProfileScope fallback("recovery.fallback_scan");
        ProfileScope::Count("recovery.fallback_files");
        ReadOptions scan_options;
        scan_options.verify_checksums = true;
        status = add_internal_iterator(table_cache_->NewIterator(
            scan_options, file->number, file->file_size));
        fallback.SetStatus(status);
        if (!status.ok()) break;
      }
    }
  }
  if (!status.ok()) return profile.Result(status);
  record_counts_.swap(counts);
  total_entries_ = total;
  return Status::OK();
}

void DBImpl::ApplyRecordCountDeltas(
    const std::map<std::string, int64_t>& deltas) {
  ProfileScope profile("metadata.apply_deltas");
  ProfileScope::Count("metadata.delta_keys", deltas.size());
  mutex_.AssertHeld();
  for (const auto& item : deltas) {
    const std::string& key = item.first;
    const int64_t delta = item.second;
    if (delta == 0) continue;
    auto it = record_counts_.find(key);
    const uint64_t old_count = it == record_counts_.end() ? 0 : it->second;
    if (delta > 0) {
      if (it == record_counts_.end()) {
        record_counts_.emplace(key, static_cast<uint64_t>(delta));
      } else {
        it->second += static_cast<uint64_t>(delta);
      }
      total_entries_ += static_cast<uint64_t>(delta);
    } else {
      const uint64_t remove_count = static_cast<uint64_t>(-delta);
      assert(old_count >= remove_count);
      assert(total_entries_ >= remove_count);
      if (old_count == remove_count) {
        record_counts_.erase(key);
      } else {
        it->second -= remove_count;
      }
      total_entries_ -= remove_count;
    }
  }
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
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
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
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = ProfileStatus("recovery.wal_replay", [&] { return WriteBatchInternal::InsertInto(&batch, mem); });
    ProfileScope::Count("recovery.wal_batches");
    ProfileScope::Count("recovery.wal_attempted_records", WriteBatchInternal::Count(&batch));
    if (status.ok()) ProfileScope::Count("recovery.wal_records", WriteBatchInternal::Count(&batch));
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
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

  return profile.Result(status);
}

Status DBImpl::GetCurrentStats(const ReadOptions& options, Stats* stats) {
  ProfileScope profile(profiler_, profiling_id_, "op.stats_query");
  (void)options;
  if (stats == nullptr) {
    return profile.Result(Status::InvalidArgument("statistics output pointer is null"));
  }
  MutexLock l(&mutex_);
  stats->total_entries = total_entries_;
  stats->distinct_keys = record_counts_.size();
  stats->seq = versions_->LastSequence();
  return Status::OK();
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  ProfileScope profile("flush.write_table");
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = ProfileStatus("flush.build_table", [&] { return BuildTable(dbname_, env_, options_, table_cache_, iter, &meta); });
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

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
  return profile.Result(s);
}

void DBImpl::CompactMemTable() {
  ProfileScope profile("flush", true, "memtable");
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
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
  WritableFile* outfile = nullptr;
  TableBuilder* builder = nullptr;
  std::map<std::string, int64_t> deltas;
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
    --deltas[parsed.user_key.ToString()];

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
      ++deltas[parsed.user_key.ToString()];
    }
    if (!keep) {
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

  if (status.ok()) {
    { ProfileScope stage("compaction.install_manifest"); status = versions_->LogAndApply(&edit, &mutex_); stage.SetStatus(status); }
    if (status.ok()) {
      ProfileScope::Count("gc.installed_input_sst_bytes", profiling_input_bytes);
      ProfileScope::Count("gc.installed_output_sst_bytes", output_meta.file_size);
      for (const auto& delta : deltas)
        if (delta.second < 0) ProfileScope::Count("gc.installed_removed_records", static_cast<uint64_t>(-delta.second));
      ApplyRecordCountDeltas(deltas);
    }
  }
  if (output_meta.number != 0) {
    pending_outputs_.erase(output_meta.number);
    if (!status.ok()) {
      table_cache_->Evict(output_meta.number);
      env_->RemoveFile(TableFileName(dbname_, output_meta.number));
    }
  }
  if (status.ok()) RemoveObsoleteFiles();
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

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
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
    status = versions_->LogAndApply(c->edit(), &mutex_);
    move.SetStatus(status);
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
    s = ProfileStatus("sst.finish", [&] { return compact->builder->Finish(); });
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
    s = ProfileStatus("sst.sync", [&] { return compact->outfile->Sync(); });
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
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  Status status;
  { ProfileScope stage("compaction.install_manifest");
    status = versions_->LogAndApply(compact->compaction->edit(), &mutex_);
    stage.SetStatus(status);
  }
  if (status.ok()) {
    for (int which = 0; which < 2; ++which)
      for (int i = 0; i < compact->compaction->num_input_files(which); ++i)
        ProfileScope::Count("gc.installed_input_sst_bytes", compact->compaction->input(which, i)->file_size);
    for (const auto& output : compact->outputs)
      ProfileScope::Count("gc.installed_output_sst_bytes", output.file_size);
    for (const auto& delta : compact->record_deltas)
      if (delta.second < 0) ProfileScope::Count("gc.installed_removed_records", static_cast<uint64_t>(-delta.second));
    ApplyRecordCountDeltas(compact->record_deltas);
  }
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

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
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
    --compact->record_deltas[ExtractUserKey(key).ToString()];
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
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
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
      } else if (ikey.type == kTypeValueWithTTL &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
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
          drop = true;
          ProfileScope::Count("gc.dropped_expired_ttl");
        } else {
          ProfileScope::Count("gc.ttl_retained_time_boundary");
        }
      }

      if (!drop && ikey.type == kTypeValueWithTTL) {
        if (ikey.sequence > compact->smallest_snapshot)
          ProfileScope::Count("gc.ttl_retained_sequence_boundary");
        // The preceding TTL branch already checked base level and records its
        // time-bound retention; do not call IsBaseLevelForKey a second time.
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
      { ProfileScope emit("gc.output_add"); compact->builder->Add(key, input->value()); }
      ++compact->record_deltas[ExtractUserKey(key).ToString()];

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
    CountBatchKeys validation;
    Status validation_status = ProfileStatus("write.validate_batch", [&] { return updates->Iterate(&validation); });
    if (!validation_status.ok()) return profile.Result(validation_status);
  }

  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  {
  ProfileScope queue("write.queue_wait");
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  }
  if (w.done) {
    return profile.Result(w.status);
  }

  // May temporarily unlock and wait.
  Status status;
  { ProfileScope stage("write.make_room"); status = MakeRoomForWrite(updates == nullptr); stage.SetStatus(status); }
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch;
    { ProfileScope grouping("write.group_batch"); write_batch = BuildBatchGroup(&last_writer); }
    bool sequence_assigned = false;
    CountBatchKeys batch_counts;
    status = ProfileStatus("metadata.count_batch_keys", [&] { return write_batch->Iterate(&batch_counts); });
    const uint64_t batch_count =
        static_cast<uint64_t>(WriteBatchInternal::Count(write_batch));
    if (status.ok() && batch_count > kMaxSequenceNumber - last_sequence) {
      status = Status::InvalidArgument("sequence number space exhausted");
    }
    if (status.ok()) {
      WriteBatchInternal::SetSequence(
          write_batch, batch_count == 0 ? last_sequence : last_sequence + 1);
      last_sequence += batch_count;
      sequence_assigned = true;

      // Add to log and apply to memtable. We can release the lock during this
      // phase since this writer owns the front of the queue.
      {
        mutex_.Unlock();
        status = ProfileStatus("write.wal_append", [&] { return log_->AddRecord(WriteBatchInternal::Contents(write_batch)); });
        bool sync_error = false;
        if (status.ok() && options.sync) {
          status = ProfileStatus("write.wal_sync", [&] { return logfile_->Sync(); });
          if (!status.ok()) {
            sync_error = true;
          }
        }
        if (status.ok()) {
          status = ProfileStatus("write.memtable_insert", [&] { return WriteBatchInternal::InsertInto(write_batch, mem_); });
        }
        mutex_.Lock();
        if (sync_error) {
          RecordBackgroundError(status);
        }
      }
      if (status.ok()) {
        ProfileScope publishing("write.publish_counts");
        ProfileScope::Count("write.committed_groups");
        ProfileScope::Count("write.committed_records", batch_count);
        std::map<std::string, int64_t> deltas;
        for (const auto& item : batch_counts.counts()) {
          deltas[item.first] = static_cast<int64_t>(item.second);
        }
        ApplyRecordCountDeltas(deltas);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    if (status.ok() && sequence_assigned) {
      versions_->SetLastSequence(last_sequence);
      MaybeScheduleCompaction();
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
    if (!bg_error_.ok()) {
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
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
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
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    { ProfileScope stage("recovery.final_counts"); s = impl->RebuildCurrentRecordCounts(); stage.SetStatus(s); }
  }
  if (s.ok()) {
    ProfileScope publish("recovery.publish");
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
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
