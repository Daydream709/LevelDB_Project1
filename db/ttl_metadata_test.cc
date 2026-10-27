// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include "util/profiler.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/testutil.h"

namespace leveldb {
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
  class FaultyWritableFile : public WritableFile {
   public:
    FaultyWritableFile(FaultClockEnv* env, std::string fname,
                       WritableFile* file)
        : env_(env), fname_(std::move(fname)), file_(file) {}

    Status Append(const Slice& data) override {
      if (env_->ShouldFailAppend(fname_)) {
        return Status::IOError(fname_, "injected append failure");
      }
      return file_->Append(data);
    }
    Status Close() override { return file_->Close(); }
    Status Flush() override { return file_->Flush(); }
    Status Sync() override {
      if (env_->ShouldFailSync(fname_)) {
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

  std::atomic<uint64_t> now_;
  std::atomic<int> fault_{static_cast<int>(Fault::kNone)};
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

  FaultClockEnv env_;
  Options options_;
  std::string dbname_;
  std::unique_ptr<DB> db_;
};

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

TEST_P(TTLMetadataTest, CorruptSSTIndexFallsBackToScanningData) {
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
    EXPECT_GT(ProfilerTestPeer::Counter(*options_.profiler, "recovery.fallback_files"), 0U);
    EXPECT_GT(ProfilerTestPeer::MetricField(*options_.profiler, "metadata.load_index", "errors"), 0U);
  }
}


TEST_P(TTLMetadataTest, MissingSSTIndexFallsBackAndRebuildsExactCounts) {
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
    EXPECT_GT(ProfilerTestPeer::Counter(*options_.profiler, "recovery.fallback_files"), 0U);
  }
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

TEST_P(TTLMetadataTest, UnreadableSSTDataFailsIndexRebuild) {
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

  db_.reset();
  options_.create_if_missing = false;
  DB* reopened = nullptr;
  const Status status = DB::Open(options_, dbname_, &reopened);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(reopened, nullptr);
  EXPECT_TRUE(status.IsCorruption() || status.IsIOError()) << status.ToString();
  if (GetParam()) {
    EXPECT_EQ(ProfilerTestPeer::MetricField(*options_.profiler, "recovery.open", "errors"), 1U);
    EXPECT_GT(ProfilerTestPeer::Counter(*options_.profiler, "recovery.fallback_files"), 0U);
  }
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
