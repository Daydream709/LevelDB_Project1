// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
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
#include <condition_variable>
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
extern char** environ;
#endif

namespace leveldb {
// Inspect selection without scheduling GC, so threshold tests are deterministic.
// No production state or public API is added by this test peer.
class TTLCompactionTestPeer {
 public:
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
  std::condition_variable read_cv_;
  bool block_next_table_read_ = false;
  bool table_read_blocked_ = false;
  bool release_table_read_ = false;
  bool fail_table_read_ = false;
  std::atomic<uint64_t> now_;
  std::atomic<int> fault_{static_cast<int>(Fault::kNone)};
  std::atomic<unsigned> fault_hits_{0};
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
