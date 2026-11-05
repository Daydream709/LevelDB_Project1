// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/table_cache.h"
#include "util/mutexlock.h"
#include "db/write_batch_internal.h"
#include "util/profiler.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <condition_variable>
#include <fstream>
#include <map>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "util/testutil.h"

#if defined(LEVELDB_TTL_CRASH_WRITER_PATH)
#include <cerrno>
#include <csignal>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#include <dirent.h>
extern char** environ;
#endif

namespace leveldb {
// Inspect selection without scheduling GC, so threshold tests are deterministic.
// No production state or public API is added by this test peer.
class TTLCompactionTestPeer {
 public:
  static uint64_t ScanDeadline(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    return impl->ttl_scan_not_before_micros_;
  }
  static void WaitForIdle(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    while (impl->background_compaction_scheduled_ || impl->profiling_callbacks_ != 0)
      impl->background_work_finished_signal_.Wait();
  }
  static bool WaitForIdleFor(DB* db, std::chrono::milliseconds timeout) {
    auto* impl = static_cast<DBImpl*>(db);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    MutexLock lock(&impl->mutex_);
    while (impl->background_compaction_scheduled_ || impl->profiling_callbacks_ != 0) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      impl->background_work_finished_signal_.TimedWait(10000);
    }
    return true;
  }
  static void Schedule(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    impl->MaybeScheduleCompaction();
  }
  static void EvictTables(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    for (int level = 0; level < config::kNumLevels; ++level)
      for (const auto* file : impl->versions_->current()->files(level))
        impl->table_cache_->Evict(file->number);
  }
  static Status PublishEmptyVersion(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    VersionEdit edit;
    return impl->LogAndApply(&edit);
  }
  static bool BackgroundPending(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    return impl->background_compaction_scheduled_;
  }
  static Status BackgroundError(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    return impl->bg_error_;
  }

  static Status RewriteRange(DB* db, const Slice* begin, const Slice* end) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    return impl->RewriteBottomLevelRangeLocked(begin, end);
  }
  static Status ProfiledRewriteRange(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    ProfileScope profile(impl->profiler_, impl->profiling_id_, "test.bottom_gc");
    MutexLock lock(&impl->mutex_);
    return impl->RewriteBottomLevelRangeLocked(nullptr, nullptr);
  }
  static std::vector<std::pair<std::string, std::string>> BottomBounds(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    std::vector<std::pair<std::string, std::string>> result;
    for (const auto* f : impl->versions_->current()->files(config::kNumLevels - 1))
      result.emplace_back(f->smallest.user_key().ToString(),
                          f->largest.user_key().ToString());
    return result;
  }
  // Reference the previous linear I/O calculation using actual SST metadata.
  // This fixture uses uniform deadlines, so each file is either all expired
  // TTL records or ordinary records (no histogram approximation is required).
  static void CheckCandidateAgainstLinearScan(DB* db) {
    auto* impl = static_cast<DBImpl*>(db);
    MutexLock lock(&impl->mutex_);
    auto* version = impl->versions_->current();
    const auto* cmp = impl->user_comparator();
    double best = -1;
    int expected_level = -1;
    std::string expected_begin, expected_end;
    for (int level = 0; level < config::kNumLevels; ++level) {
      for (const auto* file : version->files(level)) {
        TableInternalStats stats;
        ASSERT_LEVELDB_OK(impl->table_cache_->GetTTLStats(file->number, file->file_size, &stats));
        ASSERT_TRUE(stats.valid);
        if (stats.ttl_records == 0) continue;
        ASSERT_EQ(stats.min_expire_time_micros, stats.max_expire_time_micros);
        ASSERT_LE(stats.max_expire_time_micros, impl->env_->NowMicros());
        const double ratio = double(stats.ttl_records) / stats.total_records;
        if (ratio < .30 && impl->env_->NowMicros() - stats.min_expire_time_micros < 60000000) continue;
        const Slice low = file->smallest.user_key(), high = file->largest.user_key();
        int target = level;
        for (int upper = 0; upper < level; ++upper)
          if (version->OverlapInLevel(upper, &low, &high)) { target = upper; break; }
        uint64_t io = file->file_size;
        if (target + 1 < config::kNumLevels)
          for (const auto* other : version->files(target + 1))
            if (cmp->Compare(other->largest.user_key(), low) >= 0 &&
                cmp->Compare(other->smallest.user_key(), high) <= 0)
              io += other->file_size;
        const double score = double(file->file_size) * ratio / std::max<uint64_t>(io, 1);
        if (score > best) {
          best = score; expected_level = target;
          expected_begin = low.ToString(); expected_end = high.ToString();
        }
      }
    }
    bool found = false;
    ASSERT_LEVELDB_OK(impl->FindTTLCompactionCandidate(&found));
    EXPECT_EQ(expected_level >= 0, found);
    EXPECT_EQ(expected_level, impl->ttl_compaction_level_);
    EXPECT_EQ(expected_begin, impl->ttl_compaction_begin_);
    EXPECT_EQ(expected_end, impl->ttl_compaction_end_);
    impl->ttl_compaction_level_ = -1;
    impl->ttl_compaction_begin_.clear(); impl->ttl_compaction_end_.clear();
  }

  static Status FindCandidate(DB* db, bool* found) {
    auto* impl = static_cast<DBImpl*>(db);
    ProfileScope profile(impl->profiler_, impl->profiling_id_,
                         "test.ttl_candidate");
    impl->mutex_.Lock();
    Status status = impl->FindTTLCompactionCandidate(found);
    impl->ttl_compaction_level_ = -1;
    impl->ttl_compaction_begin_.clear();
    impl->ttl_compaction_end_.clear();
    impl->mutex_.Unlock();
    return status;
  }
};
namespace {

constexpr uint64_t kTestEpochMicros = 1700000000000000ULL;
constexpr uint64_t kMicrosPerSecond = 1000000ULL;

class FaultClockEnv : public EnvWrapper {
 public:
  enum class Fault { kNone, kLogAppend, kTableSync, kManifestAppend };

  FaultClockEnv() : EnvWrapper(Env::Default()), now_(kTestEpochMicros) {}

  uint64_t NowMicros() override { return now_.load(); }
  void SetTime(uint64_t value) { now_.store(value); }
  void Advance(uint64_t delta) { now_.fetch_add(delta); }
  void SetFault(Fault fault) { fault_.store(static_cast<int>(fault)); }
  unsigned FaultHits() const { return fault_hits_.load(); }
  unsigned MetadataFilesCreated() const { return metadata_files_created_.load(); }

  void BlockNextLogAppend() {
    std::lock_guard<std::mutex> lock(log_mu_);
    block_next_log_append_ = true;
    log_append_blocked_ = false;
    release_log_append_ = false;
  }
  bool WaitForBlockedLogAppend() {
    std::unique_lock<std::mutex> lock(log_mu_);
    return log_cv_.wait_for(lock, std::chrono::seconds(10),
                            [this] { return log_append_blocked_; });
  }
  void ReleaseLogAppend() {
    std::lock_guard<std::mutex> lock(log_mu_);
    release_log_append_ = true;
    log_cv_.notify_all();
  }
  void MaybeBlockLogAppend(const std::string& fname) {
    if (fname.size() < 4 || fname.compare(fname.size() - 4, 4, ".log") != 0)
      return;
    std::unique_lock<std::mutex> lock(log_mu_);
    if (!block_next_log_append_) return;
    block_next_log_append_ = false;
    log_append_blocked_ = true;
    log_cv_.notify_all();
    log_cv_.wait(lock, [this] { return release_log_append_; });
    log_append_blocked_ = false;
  }

  void BlockNextTableRead(bool fail = false) {
    std::lock_guard<std::mutex> lock(read_mu_);
    block_next_table_read_ = true;
    table_read_blocked_ = false;
    release_table_read_ = false;
    fail_table_read_ = fail;
  }
  bool WaitForBlockedTableRead() {
    std::unique_lock<std::mutex> lock(read_mu_);
    return read_cv_.wait_for(lock, std::chrono::seconds(10),
                            [this] { return table_read_blocked_; });
  }
  void ReleaseTableRead() {
    std::lock_guard<std::mutex> lock(read_mu_);
    release_table_read_ = true;
    read_cv_.notify_all();
  }
  Status BeforeTableRead() {
    std::unique_lock<std::mutex> lock(read_mu_);
    if (!block_next_table_read_) return Status::OK();
    block_next_table_read_ = false;
    table_read_blocked_ = true;
    read_cv_.notify_all();
    read_cv_.wait(lock, [this] { return release_table_read_; });
    table_read_blocked_ = false;
    return fail_table_read_ ? Status::IOError("injected candidate read failure")
                            : Status::OK();
  }
  Status NewRandomAccessFile(const std::string& fname,
                             RandomAccessFile** result) override {
    Status status = target()->NewRandomAccessFile(fname, result);
    if (status.ok() && fname.size() > 4 && fname.substr(fname.size() - 4) == ".ldb")
      *result = new GatedRandomFile(this, *result);
    return status;
  }

  void BlockNextTableSync() {
    std::lock_guard<std::mutex> lock(sync_mu_);
    block_next_table_sync_ = true;
    table_sync_blocked_ = false;
    release_table_sync_ = false;
  }

  bool WaitForBlockedTableSync() {
    std::unique_lock<std::mutex> lock(sync_mu_);
    return sync_cv_.wait_for(lock, std::chrono::seconds(10),
                             [this] { return table_sync_blocked_; });
  }

  void ReleaseTableSync() {
    std::lock_guard<std::mutex> lock(sync_mu_);
    release_table_sync_ = true;
    sync_cv_.notify_all();
  }

  Status NewWritableFile(const std::string& fname,
                         WritableFile** result) override {
    Status status = target()->NewWritableFile(fname, result);
    if (status.ok() && fname.size() >= 5 &&
        fname.compare(fname.size() - 5, 5, ".midx") == 0)
      ++metadata_files_created_;
    if (status.ok()) *result = new FaultyWritableFile(this, fname, *result);
    return status;
  }

  Status NewAppendableFile(const std::string& fname,
                           WritableFile** result) override {
    Status status = target()->NewAppendableFile(fname, result);
    if (status.ok()) *result = new FaultyWritableFile(this, fname, *result);
    return status;
  }

  bool ShouldFailAppend(const std::string& fname) const {
    const Fault fault = static_cast<Fault>(fault_.load());
    if (fault == Fault::kLogAppend) {
      return fname.size() >= 4 && fname.compare(fname.size() - 4, 4, ".log") == 0;
    }
    return fault == Fault::kManifestAppend &&
           fname.find("MANIFEST-") != std::string::npos;
  }

  bool ShouldFailSync(const std::string& fname) const {
    return static_cast<Fault>(fault_.load()) == Fault::kTableSync &&
           fname.size() >= 4 && fname.compare(fname.size() - 4, 4, ".ldb") == 0;
  }

  void MaybeBlockTableSync(const std::string& fname) {
    if (fname.size() < 4 || fname.compare(fname.size() - 4, 4, ".ldb") != 0) {
      return;
    }
    std::unique_lock<std::mutex> lock(sync_mu_);
    if (!block_next_table_sync_) return;
    block_next_table_sync_ = false;
    table_sync_blocked_ = true;
    sync_cv_.notify_all();
    sync_cv_.wait(lock, [this] { return release_table_sync_; });
    table_sync_blocked_ = false;
    release_table_sync_ = false;
  }

 private:
  class GatedRandomFile : public RandomAccessFile {
   public:
    GatedRandomFile(FaultClockEnv* env, RandomAccessFile* file)
        : env_(env), file_(file) {}
    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
      Status status = env_->BeforeTableRead();
      return status.ok() ? file_->Read(offset, n, result, scratch) : status;
    }
   private:
    FaultClockEnv* env_;
    std::unique_ptr<RandomAccessFile> file_;
  };

  class FaultyWritableFile : public WritableFile {
   public:
    FaultyWritableFile(FaultClockEnv* env, std::string fname,
                       WritableFile* file)
        : env_(env), fname_(std::move(fname)), file_(file) {}

    Status Append(const Slice& data) override {
      env_->MaybeBlockLogAppend(fname_);
      if (env_->ShouldFailAppend(fname_)) {
        ++env_->fault_hits_;
        return Status::IOError(fname_, "injected append failure");
      }
      return file_->Append(data);
    }
    Status Close() override { return file_->Close(); }
    Status Flush() override { return file_->Flush(); }
    Status Sync() override {
      if (env_->ShouldFailSync(fname_)) {
        ++env_->fault_hits_;
        return Status::IOError(fname_, "injected sync failure");
      }
      env_->MaybeBlockTableSync(fname_);
      return file_->Sync();
    }

   private:
    FaultClockEnv* const env_;
    const std::string fname_;
    std::unique_ptr<WritableFile> file_;
  };

  std::mutex read_mu_;
  std::mutex log_mu_;
  std::condition_variable log_cv_;
  bool block_next_log_append_ = false;
  bool log_append_blocked_ = false;
  bool release_log_append_ = false;
  std::condition_variable read_cv_;
  bool block_next_table_read_ = false;
  bool table_read_blocked_ = false;
  bool release_table_read_ = false;
  bool fail_table_read_ = false;
  std::atomic<uint64_t> now_;
  std::atomic<int> fault_{static_cast<int>(Fault::kNone)};
  std::atomic<unsigned> fault_hits_{0};
  std::atomic<unsigned> metadata_files_created_{0};
  std::mutex sync_mu_;
  std::condition_variable sync_cv_;
  bool block_next_table_sync_ = false;
  bool table_sync_blocked_ = false;
  bool release_table_sync_ = false;
};

class TTLMetadataTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    std::string test_dir;
    ASSERT_LEVELDB_OK(Env::Default()->GetTestDirectory(&test_dir));
    dbname_ = test_dir + "/ttl-metadata-" +
              std::to_string(Env::Default()->NowMicros()) + "-" +
              std::to_string(reinterpret_cast<uintptr_t>(this));
    options_.env = &env_;
    if (GetParam()) {
      options_.profiler = std::make_shared<Profiler>();
      options_.profiler->SetEnabled(true);
    }
    options_.compression = kNoCompression;
    options_.write_buffer_size = 64 << 10;
    options_.max_file_size = 1 << 20;
    DestroyDB(dbname_, options_);
  }

  void TearDown() override {
    db_.reset();
    env_.SetFault(FaultClockEnv::Fault::kNone);
    DestroyDB(dbname_, options_);
  }

  Status Open(bool create_if_missing) {
    options_.create_if_missing = create_if_missing;
    DB* db = nullptr;
    Status status = DB::Open(options_, dbname_, &db);
    if (status.ok()) db_.reset(db);
    return status;
  }

  void Reopen() {
    db_.reset();
    ASSERT_LEVELDB_OK(Open(false));
  }

  void MakeTTLSummariesUnavailable(bool corrupt = false) {
    db_.reset();
    std::vector<std::string> children;
    ASSERT_LEVELDB_OK(env_.GetChildren(dbname_, &children));
    size_t changed = 0;
    for (const std::string& child : children) {
      if (child.size() <= 4 || child.substr(child.size() - 4) != ".ldb") continue;
      const std::string path = dbname_ + "/" + child;
      std::string contents;
      ASSERT_LEVELDB_OK(ReadFileToString(&env_, path, &contents));
      if (corrupt) {
        const size_t pos = contents.find("LDS1");
        ASSERT_NE(pos, std::string::npos);
        contents[pos] ^= 1;  // Optional summary checksum must fail.
      } else {
        Footer footer;
        Slice bytes(contents.data() + contents.size() - Footer::kEncodedLength,
                    Footer::kEncodedLength);
        ASSERT_LEVELDB_OK(footer.DecodeFrom(&bytes));
        const auto offset = footer.metaindex_handle().offset();
        const auto size = footer.metaindex_handle().size();
        const std::string marker = "leveldb.internal-stats.v1";
        const size_t pos = contents.find(marker, offset);
        ASSERT_NE(pos, std::string::npos);
        ASSERT_LE(pos + marker.size(), offset + size);
        contents[pos + marker.size() - 1] = '0';
        EncodeFixed32(&contents[offset + size + 1],
                      crc32c::Mask(crc32c::Value(contents.data() + offset,
                                                size + 1)));
      }
      ASSERT_LEVELDB_OK(WriteStringToFile(&env_, contents, path));
      ++changed;
    }
    ASSERT_GT(changed, 0U);
    ASSERT_LEVELDB_OK(Open(false));
  }

  void ExpectValue(const Slice& key, const std::string& expected,
                   const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.snapshot = snapshot;
    std::string value;
    ASSERT_LEVELDB_OK(db_->Get(options, key, &value));
    EXPECT_EQ(value, expected);
  }

  void ExpectMissing(const Slice& key, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.snapshot = snapshot;
    std::string value = "unchanged";
    EXPECT_TRUE(db_->Get(options, key, &value).IsNotFound());
    EXPECT_EQ(value, "unchanged");
  }

  void ExpectStatsMatchPhysicalScan(uint64_t sequence) {
    auto* impl = static_cast<DBImpl*>(db_.get());
    std::unique_ptr<Iterator> iter(impl->TEST_NewInternalIterator());
    std::set<std::string> keys;
    uint64_t total = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_GE(iter->key().size(), 8U);
      Slice key = ExtractUserKey(iter->key());
      keys.insert(key.ToString());
      ++total;
    }
    ASSERT_LEVELDB_OK(iter->status());

    Stats stats;
    ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
    EXPECT_EQ(stats.total_entries, total);
    EXPECT_EQ(stats.distinct_keys, keys.size());
    EXPECT_EQ(stats.seq, sequence);
  }

  std::set<std::string> PhysicalKeys() {
    std::set<std::string> keys;
    std::unique_ptr<Iterator> iter(
        static_cast<DBImpl*>(db_.get())->TEST_NewInternalIterator());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.insert(ExtractUserKey(iter->key()).ToString());
    }
    EXPECT_TRUE(iter->status().ok()) << iter->status().ToString();
    return keys;
  }

  void PushToBottom() {
    auto* impl = static_cast<DBImpl*>(db_.get());
    ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
    for (int level = 0; level < config::kNumLevels - 1; ++level) {
      impl->TEST_CompactRange(level, nullptr, nullptr);
    }
  }

  void CheckPartialRange(const Comparator* comparator) {
    options_.comparator = comparator;
    ASSERT_LEVELDB_OK(Open(true));
    std::vector<std::string> keys;
    for (char key = 'a'; key <= 'p'; ++key) keys.emplace_back(1, key);
    std::sort(keys.begin(), keys.end(), [comparator](const std::string& a,
                                                  const std::string& b) {
      return comparator->Compare(a, b) < 0;
    });
    for (size_t i = 0; i < keys.size(); ++i) {
      // Fewer than 30% expire: opportunistic TTL scheduling must not clean
      // the out-of-range record before we can inspect the manual rewrite.
      Status status = (i == 4 || i == 6 || i == 10)
          ? db_->Put(WriteOptions(), keys[i], keys[i], 2)
          : db_->Put(WriteOptions(), keys[i], keys[i]);
      ASSERT_LEVELDB_OK(status);
    }
    ASSERT_NO_FATAL_FAILURE(PushToBottom());
    std::string file_count;
    ASSERT_TRUE(db_->GetProperty("leveldb.num-files-at-level6", &file_count));
    ASSERT_EQ("1", file_count);  // All keys share a single boundary SST.
    env_.Advance(2 * kMicrosPerSecond);
    const Slice begin(keys[4]), end(keys[6]);
    db_->CompactRange(&begin, &end);
    std::set<std::string> expected(keys.begin(), keys.end());
    expected.erase(keys[4]);
    expected.erase(keys[6]);
    EXPECT_EQ(expected, PhysicalKeys());
    ExpectMissing(keys[4]);
    ExpectMissing(keys[6]);
    ExpectMissing(keys[10]);  // Logically expired, still physically retained.
    ExpectValue(keys[3], keys[3]);
    ExpectValue(keys[5], keys[5]);
    ExpectValue(keys[7], keys[7]);
    ExpectStatsMatchPhysicalScan(16);
    Reopen();
    EXPECT_EQ(expected, PhysicalKeys());
    ExpectMissing(keys[10]);
  }

  void CheckMultiFileRange(const Comparator* comparator) {
    options_.comparator = comparator;
    options_.write_buffer_size = 16 << 20;
    ASSERT_LEVELDB_OK(Open(true));
    std::vector<std::string> keys;
    for (int i = 0; i < 128; ++i) {
      char key[16]; std::snprintf(key, sizeof(key), "key-%04d", i);
      keys.emplace_back(key);
    }
    std::sort(keys.begin(), keys.end(), [comparator](const std::string& a, const std::string& b) {
      return comparator->Compare(a, b) < 0;
    });
    WriteBatch batch;
    for (const auto& key : keys)
      batch.PutWithExpiration(key, std::string(32768, 'v'), kTestEpochMicros + kMicrosPerSecond);
    ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_NO_FATAL_FAILURE(PushToBottom());
    const auto bounds = TTLCompactionTestPeer::BottomBounds(db_.get());
    ASSERT_GE(bounds.size(), 3U);
    env_.Advance(2 * kMicrosPerSecond);
    std::set<std::string> expected(keys.begin(), keys.end());
    // Empty range between two real keys, and an explicitly inverted range.
    const std::string gap = "key-0063x"; const Slice gap_slice(gap);
    ASSERT_LEVELDB_OK(TTLCompactionTestPeer::RewriteRange(db_.get(), &gap_slice, &gap_slice));
    EXPECT_EQ(expected, PhysicalKeys());
    const Slice inverted_begin(keys.back()), inverted_end(keys.front());
    ASSERT_LEVELDB_OK(TTLCompactionTestPeer::RewriteRange(db_.get(), &inverted_begin, &inverted_end));
    EXPECT_EQ(expected, PhysicalKeys());
    const Slice begin(bounds[1].first), end(bounds[2].second);
    ASSERT_LEVELDB_OK(TTLCompactionTestPeer::RewriteRange(db_.get(), &begin, &end));
    for (auto it = expected.begin(); it != expected.end();) {
      if (comparator->Compare(*it, begin) >= 0 && comparator->Compare(*it, end) <= 0) it = expected.erase(it);
      else ++it;
    }
    EXPECT_EQ(expected, PhysicalKeys());
    ExpectStatsMatchPhysicalScan(128);
    // Open-ended prefix followed by the remaining suffix, including endpoints.
    const Slice prefix_end(keys[2]);
    ASSERT_LEVELDB_OK(TTLCompactionTestPeer::RewriteRange(db_.get(), nullptr, &prefix_end));
    for (const auto& key : keys) if (comparator->Compare(key, prefix_end) <= 0) expected.erase(key);
    EXPECT_EQ(expected, PhysicalKeys());
    const Slice suffix_begin(keys[3]);
    ASSERT_LEVELDB_OK(TTLCompactionTestPeer::RewriteRange(db_.get(), &suffix_begin, nullptr));
    EXPECT_TRUE(PhysicalKeys().empty());
    ExpectStatsMatchPhysicalScan(128);
  }

  void CheckMultiLevelCandidate(const Comparator* comparator) {
    options_.comparator = comparator;
    options_.write_buffer_size = 16 << 20;
    ASSERT_LEVELDB_OK(Open(true));
    WriteBatch bottom;
    for (int i = 0; i < 128; ++i) {
      char key[16]; std::snprintf(key, sizeof(key), "key-%04d", i);
      bottom.PutWithExpiration(key, std::string(32768, 'b'), kTestEpochMicros + kMicrosPerSecond);
    }
    ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &bottom));
    ASSERT_NO_FATAL_FAILURE(PushToBottom());
    ASSERT_GE(TTLCompactionTestPeer::BottomBounds(db_.get()).size(), 3U);
    WriteBatch upper;
    for (int i = 0; i < 128; i += 3) {
      char key[16]; std::snprintf(key, sizeof(key), "key-%04d", i);
      upper.Put(key, std::string(32768, 'u'));
    }
    ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &upper));
    auto* impl = static_cast<DBImpl*>(db_.get());
    ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
    for (int level = 2; level < 5; ++level) impl->TEST_CompactRange(level, nullptr, nullptr);
    std::string count;
    ASSERT_TRUE(db_->GetProperty("leveldb.num-files-at-level5", &count));
    ASSERT_GT(std::stoul(count), 0U);
    env_.Advance(2 * kMicrosPerSecond);
    TTLCompactionTestPeer::CheckCandidateAgainstLinearScan(db_.get());
    ExpectStatsMatchPhysicalScan(171);
  }

  // 0: snapshot; 1: Version replacement; 2: I/O failure; 3: shutdown;
  // 4: manual flush/compaction queued while the scan owns the work slot.
  void CheckUnlockedCandidateScan(int mode) {
    ASSERT_LEVELDB_OK(Open(true));
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "ttl", "value", 1));
    ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "hot", "before"));
    // Memtable flush completion does not imply its background callback has
    // finished candidate scanning/profiling. Isolate the intended scan first.
    TTLCompactionTestPeer::WaitForIdle(db_.get());
    TTLCompactionTestPeer::EvictTables(db_.get());
    env_.Advance(2 * kMicrosPerSecond);
    env_.BlockNextTableRead(mode == 2);
    DB* raw = db_.get();
    std::thread scanner([raw] { TTLCompactionTestPeer::Schedule(raw); });
    const bool entered = env_.WaitForBlockedTableRead();
    if (!entered) {
      env_.ReleaseTableRead(); scanner.join();
      FAIL() << "candidate scan did not enter the controlled SST read";
    }
    std::mutex done_mu;
    std::condition_variable done_cv;
    bool done = false;
    Status foreground_status;
    std::string value;
    const Snapshot* snapshot = nullptr;
    std::thread foreground([&] {
      if (mode == 3) {
        db_.reset();
      } else if (mode == 4) {
        raw->CompactRange(nullptr, nullptr);
      } else {
        foreground_status = raw->Get(ReadOptions(), "hot", &value);
        if (foreground_status.ok())
          foreground_status = raw->Put(WriteOptions(), "hot", "after");
        if (mode == 0) snapshot = raw->GetSnapshot();
        if (mode == 1 && foreground_status.ok())
          foreground_status = TTLCompactionTestPeer::PublishEmptyVersion(raw);
      }
      std::lock_guard<std::mutex> lock(done_mu);
      done = true; done_cv.notify_all();
    });
    bool finished_while_read_blocked;
    {
      std::unique_lock<std::mutex> lock(done_mu);
      finished_while_read_blocked = done_cv.wait_for(
          lock, std::chrono::milliseconds(500), [&] { return done; });
    }
    env_.ReleaseTableRead();
    scanner.join(); foreground.join();
    EXPECT_EQ(mode < 3, finished_while_read_blocked);
    if (mode == 3) { EXPECT_EQ(nullptr, db_.get()); return; }
    if (mode == 4) {
      ExpectValue("hot", "before"); ExpectMissing("ttl");
      ExpectStatsMatchPhysicalScan(2); return;
    }
    EXPECT_TRUE(foreground_status.ok()) << foreground_status.ToString();
    EXPECT_EQ("before", value);
    ExpectValue("hot", "after");
    if (mode == 2) {
      EXPECT_TRUE(TTLCompactionTestPeer::BackgroundError(raw).IsIOError());
      EXPECT_TRUE(raw->Put(WriteOptions(), "later", "value").IsIOError());
      Reopen(); ExpectValue("hot", "after");
    } else {
      EXPECT_FALSE(TTLCompactionTestPeer::BackgroundPending(raw));
      EXPECT_EQ(1U, PhysicalKeys().count("ttl"));
    }
    if (snapshot != nullptr) raw->ReleaseSnapshot(snapshot);
    db_->CompactRange(nullptr, nullptr);
    ExpectMissing("ttl"); ExpectValue("hot", "after");
    ExpectStatsMatchPhysicalScan(3);
  }

  void CheckCompactionFailure(FaultClockEnv::Fault fault, bool bottom) {
    ASSERT_LEVELDB_OK(Open(true));
    for (int i = 0; i < 16; ++i) {
      ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), std::to_string(i), "plain"));
    }
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "ttl", "value", 2));
    auto* impl = static_cast<DBImpl*>(db_.get());
    if (bottom) {
      ASSERT_NO_FATAL_FAILURE(PushToBottom());
    } else {
      ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
    }
    Stats before;
    ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &before));
    const auto keys_before = PhysicalKeys();
    env_.Advance(2 * kMicrosPerSecond);
    const unsigned hits = env_.FaultHits();
    env_.SetFault(fault);
    if (bottom) {
      db_->CompactRange(nullptr, nullptr);
    } else {
      // Fresh non-overlapping flushes are placed at level 2.
      impl->TEST_CompactRange(2, nullptr, nullptr);
    }
    env_.SetFault(FaultClockEnv::Fault::kNone);
    ASSERT_GT(env_.FaultHits(), hits);  // Prove the fault was actually reached.
    Stats after;
    ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &after));
    EXPECT_EQ(before.total_entries, after.total_entries);
    EXPECT_EQ(before.distinct_keys, after.distinct_keys);
    EXPECT_EQ(before.seq, after.seq);
    EXPECT_EQ(keys_before, PhysicalKeys());
    ExpectMissing("ttl");
    ExpectValue("0", "plain");
    Reopen();
    ExpectMissing("ttl");
    ExpectValue("0", "plain");
    ExpectStatsMatchPhysicalScan(17);
    // Retry the same operation that failed, rather than a different level.
    if (bottom) {
      db_->CompactRange(nullptr, nullptr);
    } else {
      static_cast<DBImpl*>(db_.get())->TEST_CompactRange(2, nullptr, nullptr);
    }
    EXPECT_EQ(0U, PhysicalKeys().count("ttl"));
    ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &after));
    EXPECT_EQ(16U, after.total_entries);
    EXPECT_EQ(17U, after.seq);
  }

  void CheckKilledWriter(const char* mode) {
#if defined(LEVELDB_TTL_CRASH_WRITER_PATH)
    int ready[2];
    ASSERT_EQ(0, pipe(ready));
    posix_spawn_file_actions_t actions;
    ASSERT_EQ(0, posix_spawn_file_actions_init(&actions));
    posix_spawn_file_actions_adddup2(&actions, ready[1], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&actions, ready[0]);
    posix_spawn_file_actions_addclose(&actions, ready[1]);
    std::string executable = LEVELDB_TTL_CRASH_WRITER_PATH;
    std::string storage_mode = mode;
    char* argv[] = {&executable[0], &dbname_[0], &storage_mode[0], nullptr};
    pid_t child;
    const int spawn_status = posix_spawn(&child, executable.c_str(), &actions,
                                          nullptr, argv, environ);
    posix_spawn_file_actions_destroy(&actions);
    close(ready[1]);
    if (spawn_status != 0) {
      close(ready[0]);
      FAIL() << "posix_spawn: " << spawn_status;
    }
    pollfd readiness{ready[0], POLLIN, 0};
    int polled;
    do { polled = poll(&readiness, 1, 15000); } while (polled < 0 && errno == EINTR);
    char ack = 0;
    const ssize_t bytes = polled > 0 ? read(ready[0], &ack, 1) : -1;
    close(ready[0]);
    // Always reap the owned helper, even if setup/sync acknowledgement failed.
    const int killed = kill(child, SIGKILL);
    int status = 0;
    pid_t waited;
    do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(1, bytes) << "writer did not acknowledge successful sync";
    ASSERT_EQ('R', ack);
    ASSERT_EQ(0, killed);
    ASSERT_EQ(child, waited);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(SIGKILL, WTERMSIG(status));
    env_.SetTime(kTestEpochMicros + kMicrosPerSecond);
    ASSERT_LEVELDB_OK(Open(false));
    ExpectValue("plain", "permanent");
    ExpectValue("victim", "ttl");
    ExpectMissing("zero");
    ExpectMissing("gone");
    ExpectStatsMatchPhysicalScan(6);
    env_.SetTime(kTestEpochMicros + 2 * kMicrosPerSecond);
    ExpectMissing("victim");
    Reopen();
    ExpectMissing("victim");
    ExpectValue("plain", "permanent");
    ExpectStatsMatchPhysicalScan(6);
#else
    GTEST_SKIP() << "Requires the POSIX crash-writer test helper";
#endif
  }

  FaultClockEnv env_;
  Options options_;
  std::string dbname_;
  std::unique_ptr<DB> db_;
};

TEST_P(TTLMetadataTest, KilledWriterRecoversSyncedWALWithoutRenewal) {
  CheckKilledWriter("wal");
}

TEST_P(TTLMetadataTest, KilledWriterRecoversFlushedSSTWithoutRenewal) {
  CheckKilledWriter("sst");
}

TEST_P(TTLMetadataTest, KilledAtWALAndFileInstallationBoundaries) {
#if defined(LEVELDB_TTL_CRASH_WRITER_PATH)
  for (const char* mode : {"wal_partial", "wal_flushed", "wal_synced",
                           "sst_synced", "manifest_before_sync", "manifest_synced",
                           "compact_sst_synced", "compact_manifest_before_sync",
                           "compact_manifest_synced", "gc_sst_synced",
                           "gc_manifest_before_sync", "gc_manifest_synced"}) {
    SCOPED_TRACE(mode);
    db_.reset();
    ASSERT_LEVELDB_OK(DestroyDB(dbname_, options_));
    int ready[2];
    ASSERT_EQ(0, pipe(ready));
    posix_spawn_file_actions_t actions;
    ASSERT_EQ(0, posix_spawn_file_actions_init(&actions));
    posix_spawn_file_actions_adddup2(&actions, ready[1], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&actions, ready[0]);
    posix_spawn_file_actions_addclose(&actions, ready[1]);
    std::string executable = LEVELDB_TTL_CRASH_WRITER_PATH;
    std::string mode_arg = mode;
    char* argv[] = {&executable[0], &dbname_[0], &mode_arg[0], nullptr};
    pid_t child;
    const int spawn_status = posix_spawn(&child, executable.c_str(), &actions, nullptr, argv, environ);
    posix_spawn_file_actions_destroy(&actions);
    close(ready[1]);
    if (spawn_status != 0) { close(ready[0]); FAIL() << spawn_status; return; }
    pollfd readiness{ready[0], POLLIN, 0};
    int polled;
    do { polled = poll(&readiness, 1, 15000); } while (polled < 0 && errno == EINTR);
    char ack = 0;
    const ssize_t bytes = polled > 0 ? read(ready[0], &ack, 1) : -1;
    close(ready[0]);
    const int killed = kill(child, SIGKILL);
    int status = 0;
    pid_t waited;
    do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(1, bytes) << "requested fault point not reached";
    ASSERT_EQ('R', ack);
    ASSERT_EQ(0, killed);
    ASSERT_EQ(child, waited);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(SIGKILL, WTERMSIG(status));
    std::fprintf(stderr, "crash-hook hit: %s\n", mode);

    const bool gc_mode = std::strncmp(mode, "gc_", 3) == 0;
    const bool compact_mode = std::strncmp(mode, "compact_", 8) == 0;
    env_.SetTime(kTestEpochMicros + (gc_mode ? 3 : 1) * kMicrosPerSecond);
    ASSERT_LEVELDB_OK(Open(false));
    ExpectValue("stable", "before");
    std::string value;
    const Status first = db_->Get(ReadOptions(), "first", &value);
    const bool recovered = first.ok();
    if (recovered) EXPECT_EQ(std::string(1024, 'x'), value);
    else EXPECT_TRUE(first.IsNotFound()) << first.ToString();
    if (std::strcmp(mode, "wal_partial") == 0) EXPECT_FALSE(recovered);
    if (std::strcmp(mode, "wal_synced") == 0 ||
        std::strcmp(mode, "sst_synced") == 0 ||
        std::strcmp(mode, "manifest_before_sync") == 0 ||
        std::strcmp(mode, "manifest_synced") == 0 || gc_mode || compact_mode)
      EXPECT_TRUE(recovered);
    if (recovered && !gc_mode) ExpectValue("second", "ttl");
    else ExpectMissing("second");
    const uint64_t recovered_sequence = (gc_mode || compact_mode) ? 4 : (recovered ? 3 : 1);
    ExpectStatsMatchPhysicalScan(recovered_sequence);
    env_.SetTime(kTestEpochMicros + (gc_mode ? 3 : 2) * kMicrosPerSecond);
    ExpectMissing("second");
    Reopen();
    ExpectValue("stable", "before");
    ExpectMissing("second");
    ExpectStatsMatchPhysicalScan(recovered_sequence);
  }
#else
  GTEST_SKIP() << "Requires POSIX crash-writer test helper";
#endif
}

TEST_P(TTLMetadataTest, CorruptMiddleWALRebuildsStatsFromRecoveredRecords) {
  options_.paranoid_checks = false;
  ASSERT_LEVELDB_OK(Open(true));
  WriteOptions sync;
  sync.sync = true;
  ASSERT_LEVELDB_OK(db_->Put(sync, "first", "one"));
  ASSERT_LEVELDB_OK(db_->Put(sync, "damaged", std::string(32768, 'y')));
  ASSERT_LEVELDB_OK(db_->Put(sync, "last", "three"));
  db_.reset();
  std::vector<std::string> children;
  ASSERT_LEVELDB_OK(env_.GetChildren(dbname_, &children));
  uint64_t latest = 0;
  for (const auto& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kLogFile)
      latest = std::max(latest, number);
  }
  ASSERT_GT(latest, 0U);
  const std::string path = LogFileName(dbname_, latest);
  std::string bytes;
  ASSERT_LEVELDB_OK(ReadFileToString(&env_, path, &bytes));
  size_t offset = 0;
  for (int record = 0; record < 2; ++record) {
    ASSERT_LE(offset + 7, bytes.size());
    const size_t length = static_cast<unsigned char>(bytes[offset + 4]) |
        (static_cast<size_t>(static_cast<unsigned char>(bytes[offset + 5])) << 8);
    ASSERT_EQ(record == 0 ? 1 : 2, static_cast<unsigned char>(bytes[offset + 6]));
    ASSERT_GT(length, 0U);
    ASSERT_LE(offset + 7 + length, bytes.size());
    if (record == 1) bytes[offset + 7 + length / 2] ^= 1;
    offset += 7 + length;
  }
  ASSERT_LEVELDB_OK(WriteStringToFile(&env_, bytes, path));
  ASSERT_LEVELDB_OK(Open(false));
  ExpectValue("first", "one");
  ExpectMissing("damaged");
  ExpectValue("last", "three");
  ExpectStatsMatchPhysicalScan(3);
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(2U, stats.total_entries);
  EXPECT_EQ(2U, stats.distinct_keys);
}

TEST_P(TTLMetadataTest, PartialRangeRetainsOutsideRecordsInSameSST) {
  CheckPartialRange(BytewiseComparator());
}

class TTLReverseComparator : public Comparator {
 public:
  int Compare(const Slice& a, const Slice& b) const override { return b.compare(a); }
  const char* Name() const override { return "test.TTLReverseComparator"; }
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
};

class TTLCaseFoldComparator : public Comparator {
 public:
  int Compare(const Slice& a, const Slice& b) const override {
    const size_t n = std::min(a.size(), b.size());
    for (size_t i = 0; i < n; ++i) {
      auto fold = [](unsigned char c) {
        return c >= 'A' && c <= 'Z' ? static_cast<unsigned char>(c + 32) : c;
      };
      const auto x = fold(static_cast<unsigned char>(a[i]));
      const auto y = fold(static_cast<unsigned char>(b[i]));
      if (x != y) return x < y ? -1 : 1;
    }
    return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
  }
  const char* Name() const override { return "test.TTLCaseFoldComparator"; }
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
};

TEST_P(TTLMetadataTest, RawDistinctKeysWithEquivalentComparatorAndSnapshot) {
  static const TTLCaseFoldComparator comparator;
  options_.comparator = &comparator;
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "A", "old"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "new", 2));
  ReadOptions options;
  options.snapshot = snapshot;
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(options, &stats));
  EXPECT_EQ(2U, stats.total_entries);
  EXPECT_EQ(2U, stats.distinct_keys);
  EXPECT_EQ(2U, stats.seq);
  ExpectValue("A", "old", snapshot);
  ExpectValue("A", "new");
  ExpectStatsMatchPhysicalScan(2);
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  ExpectStatsMatchPhysicalScan(2);
  db_->ReleaseSnapshot(snapshot);
  env_.Advance(2 * kMicrosPerSecond);
  GCResult result;
  ASSERT_LEVELDB_OK(db_->RunFullGC(&result));
  EXPECT_TRUE(result.complete);
  ExpectMissing("A");
  ExpectStatsMatchPhysicalScan(2);
  Reopen();
  ExpectMissing("a");
  ExpectStatsMatchPhysicalScan(2);
}

TEST_P(TTLMetadataTest, PartialRangeUsesCustomComparator) {
  // Static lifetime also covers fixture TearDown after an assertion failure.
  static const TTLReverseComparator comparator;
  CheckPartialRange(&comparator);
}

TEST_P(TTLMetadataTest, MultiFileRangeUsesInclusiveBounds) {
  CheckMultiFileRange(BytewiseComparator());
}
TEST_P(TTLMetadataTest, MultiFileRangeUsesReverseComparator) {
  static const TTLReverseComparator comparator;
  CheckMultiFileRange(&comparator);
}
TEST_P(TTLMetadataTest, MultiLevelCandidateMatchesLinearScoring) {
  CheckMultiLevelCandidate(BytewiseComparator());
}
TEST_P(TTLMetadataTest, MultiLevelCandidateMatchesReverseLinearScoring) {
  static const TTLReverseComparator comparator;
  CheckMultiLevelCandidate(&comparator);
}

TEST_P(TTLMetadataTest, UnlockedCandidateAllowsReadsWritesAndRejectsSnapshot) {
  CheckUnlockedCandidateScan(0);
}
TEST_P(TTLMetadataTest, UnlockedCandidateRejectsChangedVersion) {
  CheckUnlockedCandidateScan(1);
}
TEST_P(TTLMetadataTest, UnlockedCandidatePublishesReadError) {
  CheckUnlockedCandidateScan(2);
}
TEST_P(TTLMetadataTest, ShutdownWaitsForUnlockedCandidateScan) {
  CheckUnlockedCandidateScan(3);
}
TEST_P(TTLMetadataTest, NegativeScanCacheExpiresAndInvalidates) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "future", "value", 3600));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  TTLCompactionTestPeer::WaitForIdle(db_.get());
  env_.Advance(2000);
  TTLCompactionTestPeer::Schedule(db_.get());
  const uint64_t first = TTLCompactionTestPeer::ScanDeadline(db_.get());
  EXPECT_EQ(env_.NowMicros() + 1000, first);
  env_.Advance(500);
  TTLCompactionTestPeer::Schedule(db_.get());
  EXPECT_EQ(first, TTLCompactionTestPeer::ScanDeadline(db_.get()));
  ASSERT_LEVELDB_OK(TTLCompactionTestPeer::PublishEmptyVersion(db_.get()));
  TTLCompactionTestPeer::Schedule(db_.get());
  EXPECT_EQ(env_.NowMicros() + 1000, TTLCompactionTestPeer::ScanDeadline(db_.get()));
  env_.Advance(1000);
  TTLCompactionTestPeer::Schedule(db_.get());
  EXPECT_EQ(env_.NowMicros() + 1000, TTLCompactionTestPeer::ScanDeadline(db_.get()));
  env_.SetTime(kTestEpochMicros - kMicrosPerSecond);
  TTLCompactionTestPeer::Schedule(db_.get());
  EXPECT_EQ(env_.NowMicros() + 1000, TTLCompactionTestPeer::ScanDeadline(db_.get()));
  ExpectValue("future", "value");
}

TEST_P(TTLMetadataTest, NegativeScanCacheDoesNotDelayManualGCOrFlush) {
  ASSERT_LEVELDB_OK(Open(true));
  WriteBatch batch;
  batch.PutWithExpiration("short", "value", env_.NowMicros() + 500);
  ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  TTLCompactionTestPeer::WaitForIdle(db_.get());
  env_.Advance(500);
  ExpectMissing("short");
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "new", "value"));
  db_->CompactRange(nullptr, nullptr);
  EXPECT_EQ(std::set<std::string>({"new"}), PhysicalKeys());
  ExpectStatsMatchPhysicalScan(2);
}

TEST_P(TTLMetadataTest, PrunedScoringMatchesExhaustiveSelection) {
  options_.write_buffer_size = 16 << 20;
  ASSERT_LEVELDB_OK(Open(true));
  WriteBatch batch;
  for (int i = 0; i < 192; ++i) {
    char key[32]; std::snprintf(key, sizeof(key), "mixed-%04d", i);
    if (i < 64 || i % 2 == 0)
      batch.PutWithExpiration(key, std::string(32768, 'v'), kTestEpochMicros + kMicrosPerSecond);
    else batch.Put(key, std::string(32768, 'v'));
  }
  ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_NO_FATAL_FAILURE(PushToBottom());
  ASSERT_GE(TTLCompactionTestPeer::BottomBounds(db_.get()).size(), 3U);
  env_.Advance(2 * kMicrosPerSecond);
  ASSERT_NO_FATAL_FAILURE(TTLCompactionTestPeer::CheckCandidateAgainstLinearScan(db_.get()));
  ExpectStatsMatchPhysicalScan(192);
}

TEST_P(TTLMetadataTest, BottomRewritePreservesNewerMemtableStats) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "old", 1));
  ASSERT_NO_FATAL_FAILURE(PushToBottom());
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "new"));
  env_.Advance(kMicrosPerSecond);
  ASSERT_LEVELDB_OK(TTLCompactionTestPeer::RewriteRange(db_.get(), nullptr, nullptr));
  ExpectValue("key", "new");
  ExpectStatsMatchPhysicalScan(2);
  Reopen();
  ExpectValue("key", "new");
  ExpectStatsMatchPhysicalScan(2);
}

TEST_P(TTLMetadataTest, UnlockedCandidateReschedulesManualWork) {
  CheckUnlockedCandidateScan(4);
}

TEST_P(TTLMetadataTest, TTLCompactionTableSyncFailureIsNotPublished) {
  CheckCompactionFailure(FaultClockEnv::Fault::kTableSync, false);
}

TEST_P(TTLMetadataTest, TTLCompactionManifestFailureIsNotPublished) {
  CheckCompactionFailure(FaultClockEnv::Fault::kManifestAppend, false);
}

TEST_P(TTLMetadataTest, TTLBottomRewriteTableSyncFailureIsNotPublished) {
  CheckCompactionFailure(FaultClockEnv::Fault::kTableSync, true);
}

TEST_P(TTLMetadataTest, TTLBottomRewriteManifestFailureIsNotPublished) {
  CheckCompactionFailure(FaultClockEnv::Fault::kManifestAppend, true);
}

TEST_P(TTLMetadataTest, PublicCompactRangeReclaimsSparseExpiredHighestLevel) {
  ASSERT_LEVELDB_OK(Open(true));
  for (int i = 0; i < 16; ++i) {
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), std::to_string(i), "plain"));
  }
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "ttl", "value", 2));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  std::string files;
  ASSERT_TRUE(db_->GetProperty("leveldb.num-files-at-level2", &files));
  ASSERT_EQ("1", files);
  env_.Advance(2 * kMicrosPerSecond);
  ExpectMissing("ttl");
  db_->CompactRange(nullptr, nullptr);
  // No faults, snapshots, or iterator pins. Low expiry density avoids relying
  // on opportunistic scheduling; explicit reclamation must visit the file.
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(16U, stats.total_entries);
  EXPECT_EQ(0U, PhysicalKeys().count("ttl"));
  // A direct rewrite of the occupied level distinguishes missed scheduling
  // from a visibility, persistence, or safe-deletion failure.
  static_cast<DBImpl*>(db_.get())->TEST_CompactRange(2, nullptr, nullptr);
  EXPECT_EQ(0U, PhysicalKeys().count("ttl"));
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(16U, stats.total_entries);
  EXPECT_EQ(17U, stats.seq);
}

TEST_P(TTLMetadataTest, PublicCompactRangeReclaimsTTLFlushedByTheSameCall) {
  ASSERT_LEVELDB_OK(Open(true));
  for (int i = 0; i < 16; ++i) {
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), std::to_string(i), "plain"));
  }
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "ttl", "value", 2));
  env_.Advance(2 * kMicrosPerSecond);
  // Start entirely in MemTable: CompactRange itself creates the first SST.
  db_->CompactRange(nullptr, nullptr);
  ExpectMissing("ttl");
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(16U, stats.total_entries);
  EXPECT_EQ(0U, PhysicalKeys().count("ttl"));
  EXPECT_EQ(17U, stats.seq);
}

TEST_P(TTLMetadataTest, MultipleSnapshotTimesAndSequencesProtectVersions) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "old"));
  const Snapshot* old = db_->GetSnapshot();
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "long", 10));
  const Snapshot* long_early = db_->GetSnapshot();
  env_.Advance(5 * kMicrosPerSecond);
  const Snapshot* long_late = db_->GetSnapshot();  // Same seq, different time.
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "short", 2));
  const Snapshot* short_live = db_->GetSnapshot();
  env_.Advance(2 * kMicrosPerSecond);
  const Snapshot* short_expired = db_->GetSnapshot();
  db_->CompactRange(nullptr, nullptr);
  ExpectMissing("key");
  ExpectValue("key", "old", old);
  ExpectValue("key", "long", long_early);
  ExpectValue("key", "long", long_late);
  ExpectValue("key", "short", short_live);
  ExpectMissing("key", short_expired);
  db_->ReleaseSnapshot(old);
  db_->ReleaseSnapshot(long_early);
  db_->ReleaseSnapshot(short_live);
  env_.Advance(10 * kMicrosPerSecond);  // Both TTL versions now expired in real time.
  db_->CompactRange(nullptr, nullptr);
  ExpectValue("key", "long", long_late);
  ExpectMissing("key", short_expired);
  db_->ReleaseSnapshot(short_expired);
  db_->CompactRange(nullptr, nullptr);
  ExpectValue("key", "long", long_late);
  db_->ReleaseSnapshot(long_late);
  db_->CompactRange(nullptr, nullptr);
  EXPECT_TRUE(PhysicalKeys().empty());
  ExpectStatsMatchPhysicalScan(3);
  Reopen();
  ExpectMissing("key");
  ExpectStatsMatchPhysicalScan(3);
}

TEST_P(TTLMetadataTest, TruncatedTTLBatchNeverPublishesPartialWrites) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "existing", "old"));
  WriteBatch original;
  original.Put("prefix", "must-not-commit");
  const size_t prefix_size = WriteBatchInternal::Contents(&original).size();
  original.PutWithExpiration("existing", "new", env_.NowMicros() + 1000000);
  const std::string encoded = WriteBatchInternal::Contents(&original).ToString();
  for (size_t length = prefix_size; length < encoded.size(); ++length) {
    SCOPED_TRACE(length);
    WriteBatch truncated;
    WriteBatchInternal::SetContents(&truncated, Slice(encoded.data(), length));
    EXPECT_TRUE(db_->Write(WriteOptions(), &truncated).IsCorruption());
    ExpectMissing("prefix");
    ExpectValue("existing", "old");
    ExpectStatsMatchPhysicalScan(1);
  }
  Reopen();
  ExpectMissing("prefix");
  ExpectValue("existing", "old");
  ExpectStatsMatchPhysicalScan(1);
}

TEST_P(TTLMetadataTest, FixedTimeSnapshotsAndBidirectionalIterator) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "old"));
  const Snapshot* before_ttl = db_->GetSnapshot();
  const std::string binary_value("x\0\xffy", 4);
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", binary_value, 2));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "b", "plain"));

  std::unique_ptr<Iterator> pinned(db_->NewIterator(ReadOptions()));
  env_.Advance(2 * kMicrosPerSecond);
  ExpectMissing("a");  // The expired latest value hides "old".
  ExpectValue("a", "old", before_ttl);
  ExpectValue("b", "plain");

  const Snapshot* after_expiry = db_->GetSnapshot();
  ExpectMissing("a", after_expiry);
  pinned->SeekToFirst();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ(pinned->key().ToString(), "a");
  EXPECT_EQ(pinned->value().ToString(), binary_value);
  pinned->Next();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ(pinned->key().ToString(), "b");
  pinned->Prev();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ(pinned->key().ToString(), "a");
  EXPECT_EQ(pinned->value().ToString(), binary_value);
  pinned->Next();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ(pinned->key().ToString(), "b");
  pinned->SeekToLast();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ(pinned->key().ToString(), "b");
  pinned->Prev();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ(pinned->key().ToString(), "a");
  ASSERT_LEVELDB_OK(pinned->status());

  db_->ReleaseSnapshot(before_ttl);
  db_->ReleaseSnapshot(after_expiry);
}

TEST_P(TTLMetadataTest, RepeatedKeyBatchPreservesViewsThroughRecoveryAndGC) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "old"));
  const Snapshot* before = db_->GetSnapshot();
  const uint64_t deadline = env_.NowMicros() + 4 * kMicrosPerSecond;

  WriteBatch batch;
  batch.PutWithExpiration("key", "temporary", deadline);
  batch.Delete("key");
  batch.Put("key", "permanent");
  batch.PutWithExpiration("expired", "hidden", env_.NowMicros());
  batch.PutWithExpiration("live", "ttl", deadline);
  ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
  ExpectStatsMatchPhysicalScan(6);
  Stats stats;
  ReadOptions old_options;
  old_options.snapshot = before;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(old_options, &stats));
  EXPECT_EQ(6U, stats.total_entries);
  EXPECT_EQ(3U, stats.distinct_keys);
  EXPECT_EQ(6U, stats.seq);
  ExpectValue("key", "old", before);
  ExpectValue("key", "permanent");
  ExpectMissing("expired");
  ExpectValue("live", "ttl");

  std::unique_ptr<Iterator> pinned(db_->NewIterator(ReadOptions()));
  env_.Advance(4 * kMicrosPerSecond);
  ExpectMissing("live");
  pinned->SeekToFirst();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ("key", pinned->key().ToString());
  pinned->Next();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ("live", pinned->key().ToString());
  EXPECT_EQ("ttl", pinned->value().ToString());
  pinned->Prev();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ("key", pinned->key().ToString());
  ASSERT_LEVELDB_OK(pinned->status());

  GCResult blocked;
  ASSERT_LEVELDB_OK(db_->RunFullGC(&blocked));
  EXPECT_FALSE(blocked.complete);
  ExpectValue("key", "old", before);
  db_->ReleaseSnapshot(before);
  pinned.reset();
  GCResult complete;
  ASSERT_LEVELDB_OK(db_->RunFullGC(&complete));
  ASSERT_TRUE(complete.complete);
  ExpectValue("key", "permanent");
  ExpectMissing("expired");
  ExpectMissing("live");
  ExpectStatsMatchPhysicalScan(6);
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(1U, stats.total_entries);
  EXPECT_EQ(1U, stats.distinct_keys);
  Reopen();
  ExpectValue("key", "permanent");
  ExpectMissing("expired");
  ExpectMissing("live");
  ExpectStatsMatchPhysicalScan(6);
}

TEST_P(TTLMetadataTest, BinaryKeysAndEmptyValuesSurviveOverwriteAndGC) {
  ASSERT_LEVELDB_OK(Open(true));
  const std::vector<std::string> keys = {
      "", std::string("\0", 1), std::string("\0\xff", 2),
      std::string("\x7f", 1), std::string("\x80", 1),
      std::string("\xff", 1), std::string(4096, 'x')};
  for (const std::string& key : keys)
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), key, ""));
  auto* impl = static_cast<DBImpl*>(db_.get());
  ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), keys[5], "short", 1));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), keys[0], "short", 1));
  ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), keys[3]));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), keys[1], "updated"));
  ExpectStatsMatchPhysicalScan(11);
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(11U, stats.total_entries);
  EXPECT_EQ(7U, stats.distinct_keys);
  env_.Advance(kMicrosPerSecond);
  Reopen();
  ExpectMissing(keys[0]);
  ExpectMissing(keys[3]);
  ExpectMissing(keys[5]);
  ExpectValue(keys[1], "updated");
  ExpectValue(keys[2], "");
  ExpectValue(keys[4], "");
  ExpectValue(keys[6], "");
  GCResult gc;
  ASSERT_LEVELDB_OK(db_->RunFullGC(&gc));
  ASSERT_TRUE(gc.complete);
  ExpectStatsMatchPhysicalScan(11);
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(4U, stats.total_entries);
  EXPECT_EQ(4U, stats.distinct_keys);
  Reopen();
  ExpectMissing(keys[0]);
  ExpectValue(keys[1], "updated");
  ExpectValue(keys[2], "");
  ExpectMissing(keys[3]);
  ExpectValue(keys[4], "");
  ExpectMissing(keys[5]);
  ExpectValue(keys[6], "");
  ExpectStatsMatchPhysicalScan(11);
}

TEST_P(TTLMetadataTest, SameKeyAcrossSSTImmutableAndActiveMemtable) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "shared", "sst-1"));
  const Snapshot* pin = db_->GetSnapshot();
  auto* impl = static_cast<DBImpl*>(db_.get());
  ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "shared", "sst-2"));
  ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
  ASSERT_TRUE(TTLCompactionTestPeer::WaitForIdleFor(db_.get(), std::chrono::seconds(5)));
  unsigned table_count = 0;
  for (int level = 0; level < config::kNumLevels; ++level) {
    std::string count;
    ASSERT_TRUE(db_->GetProperty("leveldb.num-files-at-level" + std::to_string(level), &count));
    table_count += std::stoul(count);
  }
  ASSERT_GE(table_count, 2U);
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "shared", "immutable"));
  env_.BlockNextTableSync();
  Status flush_status;
  std::thread flush([&] { flush_status = impl->TEST_CompactMemTable(); });
  if (!env_.WaitForBlockedTableSync()) {
    env_.ReleaseTableSync();
    flush.join();
    db_->ReleaseSnapshot(pin);
    FAIL() << "immutable flush did not reach table sync";
    return;
  }
  Status write_status = db_->Put(WriteOptions(), "shared", "active");
  if (write_status.ok()) {
    ExpectValue("shared", "active");
    ExpectValue("shared", "sst-1", pin);
    ExpectStatsMatchPhysicalScan(4);
  }
  env_.ReleaseTableSync();
  flush.join();
  db_->ReleaseSnapshot(pin);
  ASSERT_LEVELDB_OK(write_status);
  ASSERT_LEVELDB_OK(flush_status);
  ASSERT_TRUE(TTLCompactionTestPeer::WaitForIdleFor(db_.get(), std::chrono::seconds(5)));
  ExpectStatsMatchPhysicalScan(4);
  Reopen();
  ExpectValue("shared", "active");
  ExpectStatsMatchPhysicalScan(4);
}

TEST_P(TTLMetadataTest, ConcurrentWriterHandoffPreservesStatsAndRecovery) {
  options_.enable_metadata_profiling = true;
  ASSERT_LEVELDB_OK(Open(true));
  constexpr int kThreads = 8;
  constexpr int kWrites = 200;
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      for (int i = 0; i < kWrites; ++i) {
        WriteOptions options;
        options.sync = (i % 31 == 0);
        const std::string key = "writer/" + std::to_string(t) + "/" + std::to_string(i);
        Status s;
        if (i % 3 == 0) {
          s = db_->Delete(options, key);
        } else if (i % 3 == 1) {
          s = db_->Put(options, key, "ttl", 100);
        } else {
          s = db_->Put(options, key, "value");
        }
        if (!s.ok()) failed.store(true, std::memory_order_relaxed);
        if (i % 8 == 0 && !db_->Put(options, "shared", "value").ok())
          failed.store(true, std::memory_order_relaxed);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& worker : workers) worker.join();
  ASSERT_FALSE(failed.load());
  constexpr uint64_t kSequence = kThreads * (kWrites + kWrites / 8);
  ExpectStatsMatchPhysicalScan(kSequence);
  std::string profile;
  ASSERT_TRUE(db_->GetProperty("leveldb.metadata-profile", &profile));
  for (const char* marker : {"\"writer_spins\":", "\"writer_spin_ready\":",
                             "\"writer_parks\":", "\"writer_wakeups\":",
                             "\"write_group_size_8_plus\":",
                             "\"memtable_insert\":"}) {
    EXPECT_NE(profile.find(marker), std::string::npos) << marker;
  }
  auto counter = [&](const std::string& name) {
    const std::string marker = "\"" + name + "\":";
    const size_t position = profile.find(marker);
    EXPECT_NE(position, std::string::npos) << name;
    return position == std::string::npos ? uint64_t{0}
        : static_cast<uint64_t>(std::stoull(profile.substr(position + marker.size())));
  };
  uint64_t groups = counter("write_group_size_8_plus");
  for (int i = 1; i <= 7; ++i)
    groups += counter("write_group_size_" + std::to_string(i));
  EXPECT_EQ(groups, counter("write_batches"));
  EXPECT_LE(counter("writer_spin_ready"), counter("writer_spins"));
  EXPECT_EQ(counter("writer_parks"), counter("writer_wakeups"));
  Reopen();
  ExpectStatsMatchPhysicalScan(kSequence);
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kWrites; ++i) {
      const std::string key = "writer/" + std::to_string(t) + "/" + std::to_string(i);
      if (i % 3 == 0) ExpectMissing(key);
      else ExpectValue(key, i % 3 == 1 ? "ttl" : "value");
    }
  }
}

TEST_P(TTLMetadataTest, WaitingWritersWakeAfterBlockedLogAppend) {
  options_.enable_metadata_profiling = true;
  ASSERT_LEVELDB_OK(Open(true));
  env_.BlockNextLogAppend();
  std::atomic<bool> failed{false};
  std::thread leader([&] {
    if (!db_->Put(WriteOptions(), "leader", "value").ok()) failed.store(true);
  });
  const bool blocked = env_.WaitForBlockedLogAppend();
  std::vector<std::thread> followers;
  if (blocked) {
    for (int i = 0; i < 7; ++i) {
      followers.emplace_back([&, i] {
        if (!db_->Put(WriteOptions(), "follower/" + std::to_string(i), "value").ok())
          failed.store(true);
      });
    }
  }
  bool observed_park = false;
  const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (blocked && !observed_park && std::chrono::steady_clock::now() < limit) {
    std::string profile;
    if (db_->GetProperty("leveldb.metadata-profile", &profile)) {
      const std::string marker = "\"writer_parks\":";
      const auto pos = profile.find(marker);
      if (pos != std::string::npos)
        observed_park = std::stoull(profile.substr(pos + marker.size())) > 0;
    }
    std::this_thread::yield();
  }
  env_.ReleaseLogAppend();
  leader.join();
  for (auto& follower : followers) follower.join();
  ASSERT_TRUE(blocked);
  ASSERT_TRUE(observed_park);
  ASSERT_FALSE(failed.load());
  ExpectStatsMatchPhysicalScan(8);
  Reopen();
  ExpectStatsMatchPhysicalScan(8);
}

TEST_P(TTLMetadataTest, ConcurrentWriterLogFailureDoesNotPublishStats) {
  ASSERT_LEVELDB_OK(Open(true));
  env_.SetFault(FaultClockEnv::Fault::kLogAppend);
  constexpr int kThreads = 8;
  std::atomic<bool> start{false};
  std::atomic<int> failed{0};
  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      if (!db_->Put(WriteOptions(), "fault/" + std::to_string(t), "v").ok())
        failed.fetch_add(1, std::memory_order_relaxed);
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& worker : workers) worker.join();
  EXPECT_EQ(kThreads, failed.load());
  ExpectStatsMatchPhysicalScan(0);
  env_.SetFault(FaultClockEnv::Fault::kNone);
  Reopen();
  ExpectStatsMatchPhysicalScan(0);
}

TEST_P(TTLMetadataTest, MixedBatchWritersPublishAtomicStatsAcrossThreadCounts) {
  for (const int writer_count : {1, 2, 4, 8, 16, 32}) {
    SCOPED_TRACE(::testing::Message() << "writers=" << writer_count);
    db_.reset();
    ASSERT_LEVELDB_OK(DestroyDB(dbname_, options_));
    env_.SetTime(kTestEpochMicros);
    ASSERT_LEVELDB_OK(Open(true));
    const Snapshot* empty = db_->GetSnapshot();
    std::atomic<bool> start{false}, done{false}, failed{false};
    std::atomic<unsigned> samples{0};
    std::vector<std::thread> workers;
    std::thread reader([&] {
      ReadOptions read;
      read.snapshot = empty;
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      do {
        Stats stats;
        Status s = db_->GetCurrentStats(read, &stats);
        if (!s.ok() || stats.seq % 3 != 0 || stats.total_entries != stats.seq ||
            stats.distinct_keys != (stats.seq == 0 ? 0 : 1 + 2 * (stats.seq / 3)) ||
            stats.seq > static_cast<uint64_t>(writer_count * 100 * 3))
          failed.store(true, std::memory_order_relaxed);
        samples.fetch_add(1, std::memory_order_relaxed);
      } while (!done.load(std::memory_order_acquire));
    });
    for (int t = 0; t < writer_count; ++t) {
      workers.emplace_back([&, t] {
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        for (int i = 0; i < 100; ++i) {
          const std::string suffix = std::to_string(t) + "/" + std::to_string(i);
          WriteBatch batch;
          batch.Put("unique/" + suffix, "value");
          batch.PutWithExpiration("shared", "ttl", env_.NowMicros() + 60 * kMicrosPerSecond);
          batch.Delete("deleted/" + suffix);
          WriteOptions options;
          options.sync = i % 31 == 0;
          if (!db_->Write(options, &batch).ok()) failed.store(true, std::memory_order_relaxed);
        }
      });
    }
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    done.store(true, std::memory_order_release);
    reader.join();
    EXPECT_FALSE(failed.load());
    EXPECT_GT(samples.load(), 0U);
    ExpectStatsMatchPhysicalScan(static_cast<uint64_t>(writer_count * 100 * 3));
    db_->ReleaseSnapshot(empty);
    Reopen();
    ExpectStatsMatchPhysicalScan(static_cast<uint64_t>(writer_count * 100 * 3));
    ASSERT_FALSE(HasFailure());
  }
}

TEST_P(TTLMetadataTest, ConcurrentMixedBatchStatsIgnoreSnapshotAtomically) {
  ASSERT_LEVELDB_OK(Open(true));
  const Snapshot* empty_snapshot = db_->GetSnapshot();
  constexpr int kBatches = 200;
  std::atomic<bool> start{false};
  std::atomic<bool> done{false};
  std::atomic<bool> inconsistent{false};
  std::atomic<unsigned> samples{0};
  std::atomic<unsigned> observed_intermediate{0};
  auto reader = [&](unsigned bit) {
    ReadOptions options;
    options.snapshot = empty_snapshot;
    while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
    do {
      Stats stats;
      const Status status = db_->GetCurrentStats(options, &stats);
      const uint64_t expected_distinct = stats.seq == 0 ? 0 : 1 + 2 * (stats.seq / 3);
      if (!status.ok() || stats.seq % 3 != 0 ||
          stats.total_entries != stats.seq || stats.distinct_keys != expected_distinct ||
          stats.seq > 3 * kBatches) {
        inconsistent.store(true, std::memory_order_relaxed);
      }
      if (status.ok() && stats.seq > 0 && stats.seq < 3 * kBatches)
        observed_intermediate.fetch_or(bit, std::memory_order_relaxed);
      samples.fetch_add(1, std::memory_order_relaxed);
    } while (!done.load(std::memory_order_acquire));
  };
  std::thread first(reader, 1U), second(reader, 2U);
  start.store(true, std::memory_order_release);
  bool writes_ok = true;
  for (int i = 0; i < kBatches; ++i) {
    WriteBatch batch;
    batch.PutWithExpiration("shared", "ttl", env_.NowMicros() + 60 * kMicrosPerSecond);
    batch.Put("left/" + std::to_string(i), "value");
    batch.Delete("right/" + std::to_string(i));
    if (!db_->Write(WriteOptions(), &batch).ok()) {
      writes_ok = false;
      break;
    }
    if (i == 0) {
      const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (observed_intermediate.load(std::memory_order_relaxed) != 3 &&
             std::chrono::steady_clock::now() < limit)
        std::this_thread::yield();
    }
    std::this_thread::yield();
  }
  done.store(true, std::memory_order_release);
  first.join();
  second.join();
  ASSERT_TRUE(writes_ok);
  EXPECT_FALSE(inconsistent.load());
  EXPECT_GT(samples.load(), 0U);
  EXPECT_EQ(3U, observed_intermediate.load());
  Stats stats;
  ReadOptions old_options;
  old_options.snapshot = empty_snapshot;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(old_options, &stats));
  EXPECT_EQ(3U * kBatches, stats.total_entries);
  EXPECT_EQ(1U + 2U * kBatches, stats.distinct_keys);
  EXPECT_EQ(3U * kBatches, stats.seq);
  ExpectStatsMatchPhysicalScan(3U * kBatches);
  db_->ReleaseSnapshot(empty_snapshot);
}

TEST_P(TTLMetadataTest, OverwriteDeadlinesRemainBarriersAfterRecovery) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "old"));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "short", 2));
  env_.Advance(kMicrosPerSecond);
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "long", 4));
  env_.Advance(3 * kMicrosPerSecond);
  ExpectValue("key", "long");
  env_.Advance(kMicrosPerSecond);
  ExpectMissing("key");
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  db_->CompactRange(nullptr, nullptr);
  ExpectMissing("key");
  Reopen();
  ExpectMissing("key");
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "permanent"));
  env_.Advance(10 * kMicrosPerSecond);
  ExpectValue("key", "permanent");
  Reopen();
  ExpectValue("key", "permanent");
}

TEST_P(TTLMetadataTest, FullGCReportsPinnedViewsAndThenReclaims) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_TRUE(db_->RunFullGC(nullptr).IsInvalidArgument());
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "expired", "old"));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "expired", "new", 1));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "deleted", "value"));
  ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), "deleted"));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "live", "value"));
  const Snapshot* snapshot = db_->GetSnapshot();
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  env_.Advance(kMicrosPerSecond);
  GCResult result;
  ASSERT_LEVELDB_OK(db_->RunFullGC(&result));
  EXPECT_FALSE(result.complete);
  EXPECT_TRUE(result.snapshots_blocked);
  EXPECT_TRUE(result.iterators_blocked);
  ExpectValue("expired", "new", snapshot);
  ExpectMissing("expired");
  db_->ReleaseSnapshot(snapshot);
  iter.reset();
  ASSERT_LEVELDB_OK(db_->RunFullGC(&result));
  EXPECT_TRUE(result.complete);
  EXPECT_EQ(5U, result.entry_sequence);
  ExpectMissing("expired");
  ExpectMissing("deleted");
  ExpectValue("live", "value");
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(1U, stats.total_entries);
  EXPECT_EQ(1U, stats.distinct_keys);
  EXPECT_EQ(5U, stats.seq);
  ExpectStatsMatchPhysicalScan(5);
  Reopen();
  ExpectMissing("expired");
  ExpectMissing("deleted");
  ExpectValue("live", "value");
  ExpectStatsMatchPhysicalScan(5);
}

TEST_P(TTLMetadataTest, FullGCUsesEntryTimeDespiteClockAdvance) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "value", 2));
  env_.BlockNextTableSync();
  GCResult result;
  Status gc_status;
  std::thread gc([&] { gc_status = db_->RunFullGC(&result); });
  if (!env_.WaitForBlockedTableSync()) {
    env_.ReleaseTableSync();
    gc.join();
    FAIL() << "full GC did not reach table sync";
    return;
  }
  env_.Advance(2 * kMicrosPerSecond);
  env_.ReleaseTableSync();
  gc.join();
  ASSERT_LEVELDB_OK(gc_status);
  EXPECT_TRUE(result.complete);
  ExpectMissing("key");
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(1U, stats.total_entries);
  ASSERT_LEVELDB_OK(db_->RunFullGC(&result));
  EXPECT_TRUE(result.complete);
  ExpectStatsMatchPhysicalScan(1);
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(0U, stats.total_entries);
}

TEST_P(TTLMetadataTest, FullGCReportsConcurrentWrite) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "old"));
  env_.BlockNextTableSync();
  GCResult result;
  Status gc_status;
  std::thread gc([&] { gc_status = db_->RunFullGC(&result); });
  if (!env_.WaitForBlockedTableSync()) {
    env_.ReleaseTableSync();
    gc.join();
    FAIL() << "full GC did not reach table sync";
    return;
  }
  const Status write_status = db_->Put(WriteOptions(), "key", "new");
  env_.ReleaseTableSync();
  gc.join();
  ASSERT_LEVELDB_OK(write_status);
  ASSERT_LEVELDB_OK(gc_status);
  EXPECT_FALSE(result.complete);
  EXPECT_TRUE(result.concurrent_state_changed);
  ExpectValue("key", "new");
  ExpectStatsMatchPhysicalScan(2);
}

TEST_P(TTLMetadataTest, FullGCReportsFlushFailure) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "value"));
  env_.SetFault(FaultClockEnv::Fault::kTableSync);
  GCResult result;
  const Status status = db_->RunFullGC(&result);
  env_.SetFault(FaultClockEnv::Fault::kNone);
  EXPECT_TRUE(status.IsIOError()) << status.ToString();
  EXPECT_FALSE(result.complete);
  Reopen();
  ExpectValue("key", "value");
  ExpectStatsMatchPhysicalScan(1);
}

TEST_P(TTLMetadataTest, ConcurrentFullGCReportsItsEntryBoundary) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "value"));
  env_.BlockNextTableSync();
  GCResult first;
  Status first_status;
  std::thread gc([&] { first_status = db_->RunFullGC(&first); });
  if (!env_.WaitForBlockedTableSync()) {
    env_.ReleaseTableSync();
    gc.join();
    FAIL() << "full GC did not reach table sync";
    return;
  }
  GCResult second;
  const Status second_status = db_->RunFullGC(&second);
  env_.ReleaseTableSync();
  gc.join();
  ASSERT_LEVELDB_OK(first_status);
  ASSERT_LEVELDB_OK(second_status);
  EXPECT_FALSE(second.complete);
  EXPECT_TRUE(second.concurrent_state_changed);
  EXPECT_EQ(second.entry_sequence, first.entry_sequence);
  EXPECT_GE(second.entry_time_micros, first.entry_time_micros);
}

TEST_P(TTLMetadataTest, FullGCReportsManifestFailureAndRecovers) {
  ASSERT_LEVELDB_OK(Open(true));
  WriteOptions durable;
  durable.sync = true;
  ASSERT_LEVELDB_OK(db_->Put(durable, "key", "old"));
  ASSERT_LEVELDB_OK(db_->Put(durable, "key", "expiring", 1));
  env_.Advance(kMicrosPerSecond);
  env_.SetFault(FaultClockEnv::Fault::kManifestAppend);
  GCResult result;
  const Status status = db_->RunFullGC(&result);
  env_.SetFault(FaultClockEnv::Fault::kNone);
  EXPECT_TRUE(status.IsIOError()) << status.ToString();
  EXPECT_FALSE(result.complete);
  EXPECT_GT(env_.FaultHits(), 0U);
  Reopen();
  ExpectMissing("key");
  ExpectStatsMatchPhysicalScan(2);
  ASSERT_LEVELDB_OK(db_->RunFullGC(&result));
  EXPECT_TRUE(result.complete);
  ExpectMissing("key");
  ExpectStatsMatchPhysicalScan(2);
}

TEST_P(TTLMetadataTest, FullGCReportsTableReadFailure) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  TTLCompactionTestPeer::WaitForIdle(db_.get());
  TTLCompactionTestPeer::EvictTables(db_.get());
  env_.BlockNextTableRead(true);
  GCResult result;
  Status status;
  std::thread gc([&] { status = db_->RunFullGC(&result); });
  if (!env_.WaitForBlockedTableRead()) {
    env_.ReleaseTableRead();
    gc.join();
    FAIL() << "full GC did not read a table";
    return;
  }
  env_.ReleaseTableRead();
  gc.join();
  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(result.complete);
}

TEST_P(TTLMetadataTest, ZeroTTLAndOverflowDoNotResurrectOrConsumeSequence) {
  env_.SetTime(0);
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "old"));
  const Snapshot* old = db_->GetSnapshot();
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "expired", 0));
  ExpectMissing("key");
  ExpectValue("key", "old", old);
  ExpectStatsMatchPhysicalScan(2);
  EXPECT_TRUE(db_->Put(WriteOptions(), "key", "overflow",
                      std::numeric_limits<uint64_t>::max()).IsInvalidArgument());
  env_.SetTime(std::numeric_limits<uint64_t>::max() - 999999);
  EXPECT_TRUE(db_->Put(WriteOptions(), "key", "overflow", 1).IsInvalidArgument());
  ExpectStatsMatchPhysicalScan(2);
  ExpectMissing("key");
  db_->ReleaseSnapshot(old);
}

TEST_P(TTLMetadataTest, MixedTTLBatchSurvivesWALFlushAndReopen) {
  ASSERT_LEVELDB_OK(Open(true));
  const uint64_t deadline = env_.NowMicros() + 2 * kMicrosPerSecond;
  const std::string binary("a\0b", 3);
  WriteBatch batch, tail;
  batch.Put("plain", "permanent");
  batch.Put("ttl", "old");
  tail.PutWithExpiration("ttl", binary, deadline);
  tail.PutWithExpiration("zero", "expired", 0);
  tail.Put("deleted", "value");
  tail.Delete("deleted");
  batch.Append(tail);
  WriteOptions sync;
  sync.sync = true;
  ASSERT_LEVELDB_OK(db_->Write(sync, &batch));
  ExpectStatsMatchPhysicalScan(6);
  Reopen();  // WAL replay must preserve both the type and the absolute deadline.
  env_.SetTime(deadline - 1);
  ExpectValue("ttl", binary);
  ExpectValue("plain", "permanent");
  ExpectMissing("zero");
  ExpectMissing("deleted");
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  Reopen();  // The same deadline must also survive SST loading.
  ExpectValue("ttl", binary);
  env_.SetTime(deadline);
  ExpectMissing("ttl");
  Reopen();
  ExpectMissing("ttl");
  ExpectValue("plain", "permanent");
}

TEST_P(TTLMetadataTest, SnapshotAndIteratorSurviveExpiredBottomLevelGC) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "old"));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "ttl", 2));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "b", "permanent"));
  const Snapshot* snapshot = db_->GetSnapshot();
  auto* impl = static_cast<DBImpl*>(db_.get());
  ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
  for (int level = 0; level < config::kNumLevels - 1; ++level) {
    impl->TEST_CompactRange(level, nullptr, nullptr);
  }
  std::unique_ptr<Iterator> pinned(db_->NewIterator(ReadOptions()));
  env_.Advance(2 * kMicrosPerSecond);
  db_->CompactRange(nullptr, nullptr);
  ExpectMissing("a");
  ExpectValue("a", "ttl", snapshot);
  db_->ReleaseSnapshot(snapshot);
  db_->CompactRange(nullptr, nullptr);
  // An iterator pins the old Version even after its files leave current state.
  pinned->Seek("a");
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ("a", pinned->key().ToString());
  EXPECT_EQ("ttl", pinned->value().ToString());
  pinned->SeekToLast();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ("b", pinned->key().ToString());
  pinned->Prev();
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ("ttl", pinned->value().ToString());
  ASSERT_LEVELDB_OK(pinned->status());
  pinned.reset();
  db_->CompactRange(nullptr, nullptr);
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(1U, stats.total_entries);
  EXPECT_EQ(1U, stats.distinct_keys);
  EXPECT_EQ(3U, stats.seq);
  ExpectStatsMatchPhysicalScan(3);
  Reopen();
  ExpectMissing("a");
  ExpectValue("b", "permanent");
}

TEST_P(TTLMetadataTest, BottomRangeCleanupLeavesDisjointFilesUntouched) {
  ASSERT_LEVELDB_OK(Open(true));
  auto* impl = static_cast<DBImpl*>(db_.get());
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "z", "permanent"));
  ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
  for (int level = 0; level < config::kNumLevels - 1; ++level) {
    impl->TEST_CompactRange(level, nullptr, nullptr);
  }
  std::vector<std::string> children;
  ASSERT_LEVELDB_OK(env_.GetChildren(dbname_, &children));
  std::set<std::string> permanent_files;
  for (const std::string& name : children) {
    if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".ldb") == 0) {
      permanent_files.insert(name);
    }
  }
  ASSERT_EQ(1U, permanent_files.size());
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "ttl", 2));
  ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
  for (int level = 0; level < config::kNumLevels - 1; ++level) {
    impl->TEST_CompactRange(level, nullptr, nullptr);
  }
  env_.Advance(2 * kMicrosPerSecond);
  const Slice key("a");
  db_->CompactRange(&key, &key);
  ExpectMissing("a");
  ExpectValue("z", "permanent");
  // Checking file identity detects an unnecessary whole-bottom-level rewrite.
  for (const std::string& name : permanent_files) {
    EXPECT_TRUE(env_.FileExists(dbname_ + "/" + name));
  }
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(1U, stats.total_entries);
  ExpectStatsMatchPhysicalScan(2);
}

TEST_P(TTLMetadataTest, CorruptTTLSummaryPreservesAuxiliaryCounts) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "one"));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "two", 60));
  ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), "b"));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  ExpectStatsMatchPhysicalScan(3);

  std::vector<std::string> children;
  ASSERT_LEVELDB_OK(env_.GetChildren(dbname_, &children));
  std::string table_file;
  for (const std::string& child : children) {
    if (child.size() >= 4 && child.compare(child.size() - 4, 4, ".ldb") == 0) {
      table_file = dbname_ + "/" + child;
      break;
    }
  }
  ASSERT_FALSE(table_file.empty());
  std::string contents;
  ASSERT_LEVELDB_OK(ReadFileToString(&env_, table_file, &contents));
  const size_t metadata = contents.find("LDS1");
  ASSERT_NE(metadata, std::string::npos);
  contents[metadata] ^= 1;  // Make the optional metadata block checksum fail.
  ASSERT_LEVELDB_OK(WriteStringToFile(&env_, contents, table_file));

  Reopen();
  ExpectStatsMatchPhysicalScan(3);
  ExpectValue("a", "two");
  ExpectMissing("b");
  if (GetParam()) {
    EXPECT_GT(ProfilerTestPeer::MetricField(*options_.profiler, "metadata.load_index", "errors"), 0U);
  }
}


TEST_P(TTLMetadataTest, MissingTTLSummaryPreservesAuxiliaryCounts) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "legacy", "value"));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  db_.reset();
  std::vector<std::string> children;
  ASSERT_LEVELDB_OK(env_.GetChildren(dbname_, &children));
  std::string file;
  for (const auto& child : children)
    if (child.size() > 4 && child.substr(child.size() - 4) == ".ldb") file = dbname_ + "/" + child;
  ASSERT_FALSE(file.empty());
  std::string contents;
  ASSERT_LEVELDB_OK(ReadFileToString(&env_, file, &contents));
  Footer footer;
  Slice footer_bytes(contents.data() + contents.size() - Footer::kEncodedLength,
                     Footer::kEncodedLength);
  ASSERT_LEVELDB_OK(footer.DecodeFrom(&footer_bytes));
  const auto offset = footer.metaindex_handle().offset();
  const auto size = footer.metaindex_handle().size();
  const std::string marker = "leveldb.internal-stats.v1";
  const auto position = contents.find(marker, offset);
  ASSERT_NE(position, std::string::npos);
  ASSERT_LT(position + marker.size(), offset + size + 1);
  contents[position + marker.size() - 1] = '0'; // valid legacy/unknown optional metadata name
  EncodeFixed32(&contents[offset + size + 1],
                crc32c::Mask(crc32c::Value(contents.data() + offset, size + 1)));
  ASSERT_LEVELDB_OK(WriteStringToFile(&env_, contents, file));
  Reopen();
  ExpectValue("legacy", "value");
  ExpectStatsMatchPhysicalScan(1);
  if (GetParam()) {
    EXPECT_GT(ProfilerTestPeer::Counter(*options_.profiler, "metadata.index_missing"), 0U);
  }
}

TEST_P(TTLMetadataTest, FallbackCountsExpiredRecordsAtThreshold) {
  for (int expired : {0, 29, 30, 80}) {
    SCOPED_TRACE(expired);
    db_.reset();
    ASSERT_LEVELDB_OK(DestroyDB(dbname_, options_));
    env_.SetTime(kTestEpochMicros);
    ASSERT_LEVELDB_OK(Open(true));
    WriteBatch batch;
    for (int i = 0; i < 100; ++i) {
      const std::string key = "key-" + std::to_string(i);
      if (i < 80) {
        batch.PutWithExpiration(key, "value", kTestEpochMicros +
            (i < expired ? 20 : 3600) * kMicrosPerSecond);
      } else {
        batch.Put(key, "value");  // Ordinary records belong in the denominator.
      }
    }
    ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
    MakeTTLSummariesUnavailable();
    ASSERT_FALSE(HasFatalFailure());
    env_.Advance(20 * kMicrosPerSecond);  // Equality means expired, age is zero.
    bool found = false;
    ASSERT_LEVELDB_OK(TTLCompactionTestPeer::FindCandidate(db_.get(), &found));
    EXPECT_EQ(expired >= 30, found);
    EXPECT_EQ(100U, PhysicalKeys().size());  // Selection itself removes nothing.
    for (int i = 0; i < 100; ++i) {
      const std::string key = "key-" + std::to_string(i);
      if (i < expired) ExpectMissing(key);
      else ExpectValue(key, "value");
    }
    ExpectStatsMatchPhysicalScan(100);
  }
}

TEST_P(TTLMetadataTest, FallbackAgeThresholdAndSnapshotProtection) {
  ASSERT_LEVELDB_OK(Open(true));
  WriteBatch batch;
  batch.PutWithExpiration("ttl", "value",
                          kTestEpochMicros + 20 * kMicrosPerSecond);
  for (int i = 0; i < 99; ++i) batch.Put("plain-" + std::to_string(i), "value");
  ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  MakeTTLSummariesUnavailable();
  ASSERT_FALSE(HasFatalFailure());
  env_.Advance(79 * kMicrosPerSecond);
  bool found = false;
  ASSERT_LEVELDB_OK(TTLCompactionTestPeer::FindCandidate(db_.get(), &found));
  EXPECT_FALSE(found);  // 1% expired, only 59 seconds old.
  env_.Advance(kMicrosPerSecond);
  ASSERT_LEVELDB_OK(TTLCompactionTestPeer::FindCandidate(db_.get(), &found));
  EXPECT_TRUE(found);  // Exactly 60 seconds old overrides the ratio threshold.
  const Snapshot* snapshot = db_->GetSnapshot();
  const Status status = TTLCompactionTestPeer::FindCandidate(db_.get(), &found);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_FALSE(found);  // Automatic selection remains conservative with snapshots.
  db_->ReleaseSnapshot(snapshot);
  db_->CompactRange(nullptr, nullptr);
  ExpectMissing("ttl");
  EXPECT_EQ(99U, PhysicalKeys().size());
  ExpectStatsMatchPhysicalScan(100);
}

TEST_P(TTLMetadataTest, CorruptSummaryFallbackHandlesMaximumDeadline) {
  ASSERT_LEVELDB_OK(Open(true));
  WriteBatch batch;
  for (int i = 0; i < 10; ++i)
    batch.PutWithExpiration("key-" + std::to_string(i), "value",
                            std::numeric_limits<uint64_t>::max());
  ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  MakeTTLSummariesUnavailable(true);
  ASSERT_FALSE(HasFatalFailure());
  bool found = false;
  env_.SetTime(std::numeric_limits<uint64_t>::max() - 1);
  ASSERT_LEVELDB_OK(TTLCompactionTestPeer::FindCandidate(db_.get(), &found));
  EXPECT_FALSE(found);
  env_.Advance(1);
  ASSERT_LEVELDB_OK(TTLCompactionTestPeer::FindCandidate(db_.get(), &found));
  EXPECT_TRUE(found);  // min == max, all expired exactly at the deadline.
  EXPECT_EQ(10U, PhysicalKeys().size());
  ExpectStatsMatchPhysicalScan(10);
}

TEST_P(TTLMetadataTest, SuccessfulGCCountsOnlyInstalledRecordRemoval) {
  options_.profiler = std::make_shared<Profiler>();
  options_.profiler->SetEnabled(true);
  ASSERT_LEVELDB_OK(Open(true));
  for (int i = 0; i < 8; ++i)
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), std::to_string(i), "value", i < 4 ? 1 : 3600));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  env_.Advance(2 * kMicrosPerSecond);
  for (int level = 0; level < config::kNumLevels - 1; ++level)
    static_cast<DBImpl*>(db_.get())->TEST_CompactRange(level, nullptr, nullptr);
  db_->CompactRange(nullptr, nullptr);
  ExpectStatsMatchPhysicalScan(8);
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(stats.total_entries, 4U);
  EXPECT_EQ(ProfilerTestPeer::Counter(*options_.profiler, "gc.installed_removed_records"), 4U);
  EXPECT_GT(ProfilerTestPeer::Counter(*options_.profiler, "gc.installed_input_sst_bytes"), 0U);
}

TEST_P(TTLMetadataTest, UnreadableSSTDataFailsPrimaryStatsRebuild) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "a", "one"));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());

  std::vector<std::string> children;
  ASSERT_LEVELDB_OK(env_.GetChildren(dbname_, &children));
  std::string table_file;
  for (const std::string& child : children) {
    if (child.size() >= 4 && child.compare(child.size() - 4, 4, ".ldb") == 0) {
      table_file = dbname_ + "/" + child;
      break;
    }
  }
  ASSERT_FALSE(table_file.empty());
  std::string contents;
  ASSERT_LEVELDB_OK(ReadFileToString(&env_, table_file, &contents));
  const size_t metadata = contents.find("LDS1");
  ASSERT_NE(metadata, std::string::npos);
  contents[metadata] ^= 1;
  contents[0] ^= 1;  // Damage the first data block checksum as well.
  ASSERT_LEVELDB_OK(WriteStringToFile(&env_, contents, table_file));

  // Reopening rebuilds the in-memory scalars from primary internal records,
  // so the damaged data block must prevent publishing a partial count.
  db_.reset();
  options_.create_if_missing = false;
  options_.paranoid_checks = true;
  DB* reopened = nullptr;
  const Status status = DB::Open(options_, dbname_, &reopened);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(reopened, nullptr);
  delete reopened;
  EXPECT_TRUE(status.IsCorruption() || status.IsIOError()) << status.ToString();
  if (GetParam()) {
    EXPECT_EQ(ProfilerTestPeer::MetricField(*options_.profiler, "recovery.open", "errors"), 1U);
  }
}

TEST_P(TTLMetadataTest, LargeTTLBatchUsesAuxiliarySpillAndReopens) {
  options_.metadata_workspace_size = 64 << 10;
  options_.enable_metadata_profiling = true;
  ASSERT_LEVELDB_OK(Open(true));
  constexpr int keys = 3000;
  const uint64_t deadline = env_.NowMicros() + 60 * kMicrosPerSecond;
  WriteBatch batch;
  for (int round = 0; round < 2; ++round)
    for (int i = 0; i < keys; ++i)
      batch.PutWithExpiration("ttl-" + std::to_string(i), "value", deadline);
  WriteOptions write;
  write.sync = true;
  ASSERT_LEVELDB_OK(db_->Write(write, &batch));
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(stats.total_entries, 2U * keys);
  EXPECT_EQ(stats.distinct_keys, keys);
  EXPECT_EQ(stats.seq, 2U * keys);
  std::string profile;
  ASSERT_TRUE(db_->GetProperty("leveldb.metadata-profile", &profile));
  const std::string spill_marker = "\"batch_spills\":";
  const auto spill = profile.find(spill_marker);
  ASSERT_NE(spill, std::string::npos);
  EXPECT_GT(std::stoull(profile.substr(spill + spill_marker.size())), 0U);
  Reopen();
  db_->CompactRange(nullptr, nullptr);
  ExpectValue("ttl-0", "value");
  ExpectStatsMatchPhysicalScan(2U * keys);
  env_.Advance(60 * kMicrosPerSecond);
  ExpectMissing("ttl-0");
  db_->CompactRange(nullptr, nullptr);
  ExpectStatsMatchPhysicalScan(2U * keys);
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(stats.total_entries, 0U);
  EXPECT_EQ(stats.distinct_keys, 0U);
  Reopen();
  ExpectStatsMatchPhysicalScan(2U * keys);
}

TEST_P(TTLMetadataTest, BottomGCSpillsBufferedDeltasAndPreservesCounts) {
  options_.metadata_workspace_size = 64 << 10;
  options_.profiler = std::make_shared<Profiler>();
  options_.profiler->SetEnabled(true);
  ASSERT_LEVELDB_OK(Open(true));
  constexpr int kKeys = 4096;
  const uint64_t deadline = env_.NowMicros() + 60 * kMicrosPerSecond;
  WriteBatch batch;
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "expired-" + std::string(48, 'x') + std::to_string(i);
    batch.PutWithExpiration(key, "value", deadline);
  }
  batch.Put("permanent", "retained");
  ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
  auto* impl = static_cast<DBImpl*>(db_.get());
  ASSERT_LEVELDB_OK(impl->TEST_CompactMemTable());
  for (int level = 0; level < config::kNumLevels - 1; ++level)
    impl->TEST_CompactRange(level, nullptr, nullptr);
  ASSERT_FALSE(TTLCompactionTestPeer::BottomBounds(db_.get()).empty());
  TTLCompactionTestPeer::WaitForIdle(db_.get());
  env_.Advance(60 * kMicrosPerSecond);
  const unsigned files_before = env_.MetadataFilesCreated();
  const uint64_t removed_before = ProfilerTestPeer::Counter(
      *options_.profiler, "gc.installed_removed_records");
  ASSERT_LEVELDB_OK(TTLCompactionTestPeer::ProfiledRewriteRange(db_.get()));
  EXPECT_GT(env_.MetadataFilesCreated(), files_before);
  EXPECT_EQ(ProfilerTestPeer::Counter(*options_.profiler,
                                      "gc.installed_removed_records") - removed_before,
            static_cast<uint64_t>(kKeys));
  ExpectValue("permanent", "retained");
  ExpectMissing("expired-" + std::string(48, 'x') + "0");
  ExpectStatsMatchPhysicalScan(kKeys + 1);
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(stats.total_entries, 1U);
  EXPECT_EQ(stats.distinct_keys, 1U);
  Reopen();
  ExpectValue("permanent", "retained");
  ExpectStatsMatchPhysicalScan(kKeys + 1);
}

// Explicit opt-in: this is a long-running stability test, not a unit test.
// Run one parameter at a time with --gtest_also_run_disabled_tests.
TEST_P(TTLMetadataTest, DISABLED_MixedWorkloadSoak) {
  const char* seconds_env = std::getenv("LEVELDB_SOAK_SECONDS");
  const int seconds = seconds_env ? std::atoi(seconds_env) : 1200;
  ASSERT_GT(seconds, 0);
  ASSERT_LEVELDB_OK(Open(true));
  constexpr int kWriters = 4, kKeys = 64;
  struct Expected { bool present = false; uint64_t deadline = 0; std::string value; };
  std::vector<Expected> expected(kWriters * kKeys);
  uint64_t sequence = 0;
  auto start = std::chrono::steady_clock::now();
  unsigned rounds = 0;
  while (std::chrono::steady_clock::now() - start < std::chrono::seconds(seconds)) {
    std::atomic<bool> stop{false}, failed{false};
    std::atomic<uint64_t> writes{0}, reads{0};
    std::vector<std::thread> workers;
    for (int w = 0; w < kWriters; ++w) {
      workers.emplace_back([&, w] {
        std::mt19937 random(0xabc000 + w * 101 + rounds);
        while (!stop.load()) {
          const int slot = w * kKeys + random() % kKeys;
          const std::string key = "soak-" + std::to_string(slot);
          const unsigned kind = random() % 4;
          const std::string value = std::to_string(rounds) + ":" + std::to_string(random());
          const uint64_t deadline = kind == 1 ? env_.NowMicros() + 3 * kMicrosPerSecond : 0;
          WriteBatch batch;
          if (kind == 0) batch.Delete(key);
          else if (kind == 1) batch.PutWithExpiration(key, value, deadline);
          else batch.Put(key, value);
          Status status = db_->Write(WriteOptions(), &batch);
          if (!status.ok()) { failed.store(true); break; }
          expected[slot] = {kind != 0, deadline, value};
          ++writes;
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
      });
    }
    workers.emplace_back([&] {
      while (!stop.load()) {
        Stats stats;
        if (!db_->GetCurrentStats(ReadOptions(), &stats).ok() ||
            stats.distinct_keys > stats.total_entries || stats.total_entries > stats.seq) {
          failed.store(true); break;
        }
        const Snapshot* snapshot = db_->GetSnapshot();
        ReadOptions read;
        read.snapshot = snapshot;
        std::unique_ptr<Iterator> it(db_->NewIterator(read));
        std::string previous;
        bool first = true;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          if (!first && previous >= it->key().ToString()) failed.store(true);
          previous = it->key().ToString(); first = false;
        }
        if (!it->status().ok()) failed.store(true);
        it.reset();
        db_->ReleaseSnapshot(snapshot);
        ++reads;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
    auto round_end = std::min(start + std::chrono::seconds(seconds),
                              std::chrono::steady_clock::now() + std::chrono::seconds(10));
    while (!failed.load() && std::chrono::steady_clock::now() < round_end)
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop.store(true);
    for (auto& worker : workers) worker.join();
    ASSERT_FALSE(failed.load());
    ASSERT_GT(writes.load(), 0U);
    ASSERT_GT(reads.load(), 0U);
    sequence += writes.load();
    env_.Advance(kMicrosPerSecond);
    GCResult gc;
    ASSERT_LEVELDB_OK(db_->RunFullGC(&gc));
    ASSERT_TRUE(gc.complete);
    ASSERT_TRUE(TTLCompactionTestPeer::WaitForIdleFor(db_.get(), std::chrono::seconds(10)));
    ExpectStatsMatchPhysicalScan(sequence);
    Reopen();
    for (int slot = 0; slot < kWriters * kKeys; ++slot) {
      const std::string key = "soak-" + std::to_string(slot);
      if (expected[slot].present &&
          (expected[slot].deadline == 0 || expected[slot].deadline > env_.NowMicros()))
        ExpectValue(key, expected[slot].value);
      else
        ExpectMissing(key);
    }
    ASSERT_FALSE(HasFailure());
    std::vector<std::string> files;
    ASSERT_LEVELDB_OK(env_.GetChildren(dbname_, &files));
    size_t logs = 0, tables = 0;
    for (const auto& f : files) {
      if (f.size() >= 4 && f.substr(f.size() - 4) == ".log") ++logs;
      if (f.size() >= 4 && f.substr(f.size() - 4) == ".ldb") ++tables;
    }
    std::ifstream statm("/proc/self/statm");
    uint64_t pages = 0, resident = 0;
    statm >> pages >> resident;
    size_t descriptors = 0;
#if defined(LEVELDB_TTL_CRASH_WRITER_PATH)
    DIR* dir = opendir("/proc/self/fd");
    if (dir) { while (readdir(dir)) ++descriptors; closedir(dir); }
#endif
    std::fprintf(stderr, "SOAK seconds=%lld round=%u seq=%llu writes=%llu reads=%llu rss_pages=%llu fds=%zu wal=%zu sst=%zu gc_complete=%d\n",
                 static_cast<long long>(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count()),
                 ++rounds, static_cast<unsigned long long>(sequence),
                 static_cast<unsigned long long>(writes.load()), static_cast<unsigned long long>(reads.load()),
                 static_cast<unsigned long long>(resident), descriptors, logs, tables, gc.complete);
    std::fflush(stderr);
  }
}

TEST_P(TTLMetadataTest, SeededMixedOperationsMatchReferenceModel) {
  struct Entry { bool present; uint64_t expiry; std::string value; };
  using Model = std::map<std::string, Entry>;
  struct View { const Snapshot* snapshot; Model model; uint64_t time; };
  const std::vector<std::string> keys = {
      "", std::string("binary\0key", 10), std::string(512, 'x'),
      "a", "b", "c", "d", "e", "f", "g", "h", "i"};
  const char* selected_seed = std::getenv("LEVELDB_ROBUST_SEED");
  const char* operation_log = std::getenv("LEVELDB_ROBUST_LOG");
  const uint64_t seed_begin = selected_seed ? std::strtoull(selected_seed, nullptr, 10) : 0;
  const uint64_t seed_end = selected_seed ? seed_begin + 1 : 20;
  for (uint64_t seed = seed_begin; seed < seed_end; ++seed) {
    std::fprintf(stderr, "robust-model seed=%llu\n", static_cast<unsigned long long>(seed));
    SCOPED_TRACE(::testing::Message() << "seed=" << seed);
    db_.reset();
    ASSERT_LEVELDB_OK(DestroyDB(dbname_, options_));
    env_.SetTime(kTestEpochMicros);
    ASSERT_LEVELDB_OK(Open(true));
    std::mt19937_64 random(0x71a5c0deULL + seed);
    Model model;
    std::vector<View> views;
    uint64_t sequence = 0;
    auto check = [&](const Model& expected, uint64_t time, const Snapshot* snap) {
      ReadOptions ro;
      ro.snapshot = snap;
      for (const auto& key : keys) {
        auto it = expected.find(key);
        const bool visible = it != expected.end() && it->second.present &&
            (it->second.expiry == 0 || time < it->second.expiry);
        std::string got;
        Status status = db_->Get(ro, key, &got);
        if (visible) {
          EXPECT_TRUE(status.ok()) << status.ToString();
          if (status.ok()) EXPECT_EQ(got, it->second.value);
        } else {
          EXPECT_TRUE(status.IsNotFound()) << status.ToString();
        }
      }
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      auto expected_it = expected.begin();
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        while (expected_it != expected.end() &&
               (!expected_it->second.present ||
                (expected_it->second.expiry && time >= expected_it->second.expiry)))
          ++expected_it;
        ASSERT_NE(expected_it, expected.end());
        EXPECT_EQ(iter->key().ToString(), expected_it->first);
        EXPECT_EQ(iter->value().ToString(), expected_it->second.value);
        ++expected_it;
      }
      while (expected_it != expected.end() &&
             (!expected_it->second.present ||
              (expected_it->second.expiry && time >= expected_it->second.expiry)))
        ++expected_it;
      EXPECT_EQ(expected_it, expected.end());
      EXPECT_TRUE(iter->status().ok()) << iter->status().ToString();
    };
    for (int i = 0; i < 10000; ++i) {
      SCOPED_TRACE(::testing::Message() << "operation=" << i);
      const unsigned action = random() % 100;
      if (operation_log) std::fprintf(stderr, "op=%d action=%u time=%llu\n", i, action,
          static_cast<unsigned long long>(env_.NowMicros()));
      if (action < 78) {
        WriteBatch batch;
        const int size = 1 + random() % 4;
        for (int j = 0; j < size; ++j) {
          const size_t key_index = random() % keys.size();
          const std::string& key = keys[key_index];
          const std::string value = std::to_string(i) + "/" + std::to_string(j);
          const unsigned type = random() % 3;
          if (operation_log) std::fprintf(stderr, "  record=%d key_index=%zu type=%u\n", j, key_index, type);
          if (type == 0) { batch.Delete(key); model[key] = {false, 0, ""}; }
          else if (type == 1) { batch.Put(key, value); model[key] = {true, 0, value}; }
          else {
            const uint64_t expiry = env_.NowMicros() + (random() % 5) * kMicrosPerSecond;
            if (operation_log) std::fprintf(stderr, "  expiry=%llu\n", static_cast<unsigned long long>(expiry));
            batch.PutWithExpiration(key, value, expiry);
            model[key] = {true, expiry, value};
          }
        }
        ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &batch));
        sequence += size;
      } else if (action < 86) {
        env_.Advance(kMicrosPerSecond);
      } else if (action < 90 && views.size() < 3) {
        views.push_back({db_->GetSnapshot(), model, env_.NowMicros()});
      } else if (action < 93 && !views.empty()) {
        if (operation_log) std::fprintf(stderr, "  release_snapshot begin\n");
        db_->ReleaseSnapshot(views.front().snapshot);
        if (operation_log) std::fprintf(stderr, "  release_snapshot end\n");
        views.erase(views.begin());
      } else if (action == 93) {
        ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
      } else if (action == 94) {
        db_->CompactRange(nullptr, nullptr);
      } else if (action == 95 && views.empty()) {
        Reopen();
      } else if (action == 96 && i % 16 == 0) {
        GCResult result;
        ASSERT_LEVELDB_OK(db_->RunFullGC(&result));
      }
      if (i % 100 == 0 || i == 9999) {
        if (operation_log) std::fprintf(stderr, "  wait_idle begin\n");
        TTLCompactionTestPeer::WaitForIdle(db_.get());
        if (operation_log) std::fprintf(stderr, "  wait_idle end\n");
        check(model, env_.NowMicros(), nullptr);
        if (operation_log) std::fprintf(stderr, "  current_check end\n");
        for (const auto& view : views) check(view.model, view.time, view.snapshot);
        if (operation_log) std::fprintf(stderr, "  snapshot_checks end\n");
        ExpectStatsMatchPhysicalScan(sequence);
        if (operation_log) std::fprintf(stderr, "  physical_check end\n");
        ASSERT_FALSE(HasFailure());
      }
    }
    for (const auto& view : views) db_->ReleaseSnapshot(view.snapshot);
  }
}

TEST_P(TTLMetadataTest, BottomRewriteReclaimsExpiredVersionHiddenByLiveValue) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "old", 1));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "key", "new"));
  ASSERT_NO_FATAL_FAILURE(PushToBottom());
  env_.Advance(2 * kMicrosPerSecond);
  ExpectValue("key", "new");
  db_->ReleaseSnapshot(snapshot);
  ASSERT_TRUE(TTLCompactionTestPeer::WaitForIdleFor(db_.get(), std::chrono::seconds(5)));
  ExpectValue("key", "new");
  ExpectStatsMatchPhysicalScan(2);
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(1U, stats.total_entries);
  EXPECT_EQ(1U, stats.distinct_keys);
  Reopen();
  ExpectValue("key", "new");
  ExpectStatsMatchPhysicalScan(2);
}

TEST_P(TTLMetadataTest, RandomOperationsMatchVisibilityAndPhysicalOracle) {
  ASSERT_LEVELDB_OK(Open(true));
  struct ModelValue {
    bool present = false;
    bool has_expiry = false;
    uint64_t expiry = 0;
    std::string value;
  };
  std::map<std::string, ModelValue> model;
  std::mt19937_64 random(0x71a5c0deULL);
  uint64_t operations = 0;
  for (int i = 0; i < 240; ++i) {
    const std::string key = "model/" + std::to_string(random() % 12);
    switch (random() % 3) {
      case 0: {
        const std::string value = "v/" + std::to_string(i);
        ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), key, value));
        model[key] = ModelValue{true, false, 0, value};
        break;
      }
      case 1: {
        const std::string value = "ttl/" + std::to_string(i);
        const uint64_t ttl = random() % 4;
        ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), key, value, ttl));
        model[key] = ModelValue{true, true,
                                env_.NowMicros() + ttl * kMicrosPerSecond,
                                value};
        break;
      }
      default:
        ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), key));
        model[key] = ModelValue{};
        break;
    }
    ++operations;
    if ((random() & 3) == 0) env_.Advance(kMicrosPerSecond);
  }

  for (const auto& item : model) {
    const ModelValue& expected = item.second;
    if (!expected.present ||
        (expected.has_expiry && env_.NowMicros() >= expected.expiry)) {
      ExpectMissing(item.first);
    } else {
      ExpectValue(item.first, expected.value);
    }
  }
  ExpectStatsMatchPhysicalScan(operations);

  db_->CompactRange(nullptr, nullptr);
  for (const auto& item : model) {
    const ModelValue& expected = item.second;
    if (!expected.present ||
        (expected.has_expiry && env_.NowMicros() >= expected.expiry)) {
      ExpectMissing(item.first);
    } else {
      ExpectValue(item.first, expected.value);
    }
  }
  ExpectStatsMatchPhysicalScan(operations);
}

TEST_P(TTLMetadataTest, CompactionDeltaIncludesConcurrentSameKeyWrite) {
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "same", "older"));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "same", "ttl", 60));
  ASSERT_LEVELDB_OK(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable());
  ExpectStatsMatchPhysicalScan(2);

  env_.BlockNextTableSync();
  std::thread compaction([this] { db_->CompactRange(nullptr, nullptr); });
  if (!env_.WaitForBlockedTableSync()) {
    env_.ReleaseTableSync();
    compaction.join();
    FAIL() << "compaction did not reach its output sync point";
    return;
  }

  const Status write_status =
      db_->Put(WriteOptions(), "same", "concurrent");
  env_.ReleaseTableSync();
  compaction.join();
  ASSERT_LEVELDB_OK(write_status);

  ExpectValue("same", "concurrent");
  ExpectStatsMatchPhysicalScan(3);
}

TEST_P(TTLMetadataTest, WALTableAndManifestFailuresDoNotCorruptPublishedCounts) {
  ASSERT_LEVELDB_OK(Open(true));
  env_.SetFault(FaultClockEnv::Fault::kLogAppend);
  EXPECT_TRUE(db_->Put(WriteOptions(), "wal-failure", "v").IsIOError());
  env_.SetFault(FaultClockEnv::Fault::kNone);
  Stats stats;
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(stats.total_entries, 0U);
  EXPECT_EQ(stats.seq, 0U);
  if (GetParam()) {
    EXPECT_EQ(ProfilerTestPeer::Counter(*options_.profiler, "io.wal.append_errors"), 1U);
    EXPECT_EQ(ProfilerTestPeer::Counter(*options_.profiler, "write.committed_records"), 0U);
  }

  // A new database isolates the flush and manifest fault cases.
  db_.reset();
  DestroyDB(dbname_, options_);
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "table-failure", "v"));
  env_.SetFault(FaultClockEnv::Fault::kTableSync);
  EXPECT_FALSE(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable().ok());
  env_.SetFault(FaultClockEnv::Fault::kNone);
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(stats.total_entries, 1U);
  ExpectValue("table-failure", "v");
  Reopen();
  ExpectStatsMatchPhysicalScan(1);

  db_.reset();
  DestroyDB(dbname_, options_);
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "manifest-failure", "v"));
  env_.SetFault(FaultClockEnv::Fault::kManifestAppend);
  EXPECT_FALSE(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable().ok());
  env_.SetFault(FaultClockEnv::Fault::kNone);
  ASSERT_LEVELDB_OK(db_->GetCurrentStats(ReadOptions(), &stats));
  EXPECT_EQ(stats.total_entries, 1U);
  ExpectValue("manifest-failure", "v");
  Reopen();
  ExpectStatsMatchPhysicalScan(1);
  if (GetParam()) {
    EXPECT_GT(ProfilerTestPeer::Counter(*options_.profiler, "io.sst.sync_errors"), 0U);
    EXPECT_GT(ProfilerTestPeer::Counter(*options_.profiler, "io.manifest.append_errors"), 0U);
    EXPECT_GE(ProfilerTestPeer::MetricField(*options_.profiler, "flush", "errors"), 2U);
    EXPECT_EQ(ProfilerTestPeer::Counter(*options_.profiler, "gc.installed_removed_records"), 0U);
  }
}


TEST_P(TTLMetadataTest, RuntimeToggleDuringFlushCompletesCapturedTask) {
  options_.profiler = std::make_shared<Profiler>();
  options_.profiler->SetEnabled(true);
  ASSERT_LEVELDB_OK(Open(true));
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "toggle", "value", 60));
  env_.BlockNextTableSync();
  Status flush_status;
  std::thread flush([&] {
    flush_status = static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable();
  });
  const bool blocked = env_.WaitForBlockedTableSync();
  options_.profiler->SetEnabled(false);
  env_.ReleaseTableSync();
  flush.join();
  ASSERT_TRUE(blocked);
  ASSERT_LEVELDB_OK(flush_status);
  EXPECT_EQ(ProfilerTestPeer::MetricField(*options_.profiler, "flush", "completed"), 1U);
  const auto queries = ProfilerTestPeer::MetricField(*options_.profiler, "op.get", "calls");
  ExpectValue("toggle", "value");
  EXPECT_EQ(ProfilerTestPeer::MetricField(*options_.profiler, "op.get", "calls"), queries);
  options_.profiler->SetEnabled(true);
  ExpectValue("toggle", "value");
  EXPECT_EQ(ProfilerTestPeer::MetricField(*options_.profiler, "op.get", "calls"), queries + 1);
}

INSTANTIATE_TEST_SUITE_P(PlainAndProfiled, TTLMetadataTest, testing::Bool());

}  // namespace
}  // namespace leveldb
