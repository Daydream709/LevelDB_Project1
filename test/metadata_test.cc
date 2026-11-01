#include "gtest/gtest.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <unistd.h>   // getpid
#include <cstdio>     // snprintf
#include <cctype>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace leveldb;

namespace {

class CaseInsensitiveComparator final : public Comparator {
 public:
  int Compare(const Slice& a, const Slice& b) const override {
    const size_t common = std::min(a.size(), b.size());
    for (size_t i = 0; i < common; ++i) {
      const unsigned char ac = static_cast<unsigned char>(a[i]);
      const unsigned char bc = static_cast<unsigned char>(b[i]);
      const int al = std::tolower(ac);
      const int bl = std::tolower(bc);
      if (al != bl) return al < bl ? -1 : 1;
    }
    if (a.size() == b.size()) return 0;
    return a.size() < b.size() ? -1 : 1;
  }

  const char* Name() const override { return "test.CaseInsensitiveComparator"; }
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
};

struct PhysicalStats {
  uint64_t total_entries = 0;
  uint64_t distinct_keys = 0;
};

PhysicalStats CountPhysicalRecords(DB* db) {
  PhysicalStats stats;
  std::set<std::string> keys;
  Iterator* iter = static_cast<DBImpl*>(db)->TEST_NewInternalIterator();
  ParsedInternalKey parsed;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    EXPECT_TRUE(ParseInternalKey(iter->key(), &parsed));
    if (!ParseInternalKey(iter->key(), &parsed)) continue;
    ++stats.total_entries;
    keys.insert(parsed.user_key.ToString());
  }
  EXPECT_TRUE(iter->status().ok());
  delete iter;
  stats.distinct_keys = keys.size();
  return stats;
}

bool PropertyHas(DB* db, const std::string& property,
                 const std::string& token) {
  std::string value;
  return db->GetProperty(property, &value) &&
         value.find(token) != std::string::npos;
}

}  // namespace

class StatsDBTest : public ::testing::Test {
 protected:
  std::string dbname_;
  DB* db_ = nullptr;

  void SetUp() override {
    const testing::TestInfo* info =
        testing::UnitTest::GetInstance()->current_test_info();
    // 生成唯一的 DB 目录名，降低并发/重复运行时冲突
    char buf[512];
    std::snprintf(buf, sizeof(buf), "testdb-%s-%s-%d-%lld",
                  info->test_suite_name(), info->name(), (int)getpid(),
                  static_cast<long long>(Env::Default()->NowMicros()));
    dbname_ = buf;

    // 确保干净环境
    Options opt;
    DestroyDB(dbname_, opt);

    // 打开数据库
    opt.create_if_missing = true;
    ASSERT_TRUE(DB::Open(opt, dbname_, &db_).ok());
  }

  void TearDown() override {
    // 先关闭，再销毁目录，彻底隔离
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
    Options opt;
    DestroyDB(dbname_, opt);
  }
};

TEST_F(StatsDBTest, BasicCount) {
  ASSERT_NE(db_, nullptr);
  db_->Put(WriteOptions(), "k1", "v1");
  db_->Put(WriteOptions(), "k2", "v2");
  db_->Put(WriteOptions(), "k1", "v3");  // 重复 key

  Stats stats;
  db_->GetCurrentStats(ReadOptions(), &stats);

  ASSERT_EQ(stats.total_entries, 3);
  ASSERT_EQ(stats.distinct_keys, 2);
}

TEST_F(StatsDBTest, StatsStayInPrimaryStateWithoutAuxiliaryCheckpoint) {
  delete db_;
  db_ = nullptr;
  Options options;
  ASSERT_EQ(options.metadata_checkpoint_interval_micros, 1000000U);
  options.create_if_missing = false;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "checkpoint-key", "value").ok());

  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 1);
  ASSERT_EQ(stats.distinct_keys, 1);
  ASSERT_EQ(stats.seq, 1);
  ASSERT_TRUE(PropertyHas(db_, "leveldb.metadata-stats", "metadata_files=0"));
  Env::Default()->SleepForMicroseconds(200000);
  ASSERT_TRUE(PropertyHas(db_, "leveldb.metadata-stats", "metadata_files=0"));
}

TEST_F(StatsDBTest, BatchUpdatesProbePrimarySSTBlocks) {
  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.metadata_workspace_size = 64 * 1024;
  options.metadata_memtable_size = 1 * 1024 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  WriteBatch initial;
  const int kInitialKeys = 700;
  for (int i = 0; i < kInitialKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%04d", i);
    initial.Put(key, "old");
  }
  const std::string binary_a("\0a", 2);
  const std::string binary_ff("\xff", 1);
  initial.Put(binary_a, "binary-old");
  initial.Put(binary_ff, "ff-old");
  ASSERT_TRUE(db_->Write(WriteOptions(), &initial).ok());
  db_->CompactRange(nullptr, nullptr);

  // This write group has hundreds of sorted keys and resolves existing
  // physical keys through primary SST blocks while also inserting new keys.
  WriteBatch updates;
  const int kUpdates = 320;
  const int kNewKeys = 120;
  for (int i = 0; i < kUpdates; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%04d", i);
    updates.Put(key, "updated");
  }
  for (int i = 0; i < kNewKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "new-%04d", i);
    updates.Put(key, "new");
  }
  updates.Put(binary_a, "binary-new");
  updates.Put(binary_ff, "ff-new");
  ASSERT_TRUE(db_->Write(WriteOptions(), &updates).ok());

  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries,
            kInitialKeys + 2 + kUpdates + kNewKeys + 2);
  ASSERT_EQ(stats.distinct_keys, kInitialKeys + 2 + kNewKeys);
  ASSERT_EQ(stats.seq, stats.total_entries);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), binary_a, &value).ok());
  ASSERT_EQ(value, "binary-new");
  ASSERT_TRUE(db_->Get(ReadOptions(), binary_ff, &value).ok());
  ASSERT_EQ(value, "ff-new");
}

TEST_F(StatsDBTest, PrimaryCompactionReconcilesConcurrentMetadataTail) {
  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.write_buffer_size = 2 * 1024 * 1024;
  options.metadata_workspace_size = 64 * 1024;
  options.metadata_memtable_size = 1 * 1024 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  const int kKeys = 12000;
  WriteBatch first;
  for (int i = 0; i < kKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%05d", i);
    first.Put(key, "first");
  }
  ASSERT_TRUE(db_->Write(WriteOptions(), &first).ok());
  db_->CompactRange(nullptr, nullptr);

  WriteBatch second;
  for (int i = 0; i < kKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%05d", i);
    second.Put(key, "second");
  }
  ASSERT_TRUE(db_->Write(WriteOptions(), &second).ok());
  Stats before_tail;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &before_tail).ok());
  ASSERT_EQ(before_tail.total_entries, 2 * kKeys);
  ASSERT_EQ(before_tail.distinct_keys, kKeys);

  WriteBatch third;
  for (int i = 0; i < kKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%05d", i);
    third.Put(key, "third");
  }
  ASSERT_TRUE(db_->Write(WriteOptions(), &third).ok());

  std::thread compaction([&] {
    db_->CompactRange(nullptr, nullptr);
  });
  // Overlap a foreground tail with primary compaction and compare published
  // statistics with the physical internal-record oracle after both finish.
  WriteBatch tail;
  const int kTailKeys = 400;
  for (int i = 0; i < kTailKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%05d", i);
    tail.Put(key, "tail");
  }
  const Status tail_status = db_->Write(WriteOptions(), &tail);
  compaction.join();
  ASSERT_TRUE(tail_status.ok()) << tail_status.ToString();

  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  const PhysicalStats physical = CountPhysicalRecords(db_);
  ASSERT_EQ(stats.total_entries, physical.total_entries);
  ASSERT_EQ(stats.distinct_keys, physical.distinct_keys);
  ASSERT_EQ(stats.total_entries, kKeys + kTailKeys);
  ASSERT_EQ(stats.distinct_keys, kKeys);
  ASSERT_EQ(stats.seq, 3 * kKeys + kTailKeys);
}

TEST_F(StatsDBTest, PersistAndRecover) {
  ASSERT_NE(db_, nullptr);
  db_->Put(WriteOptions(), "a", "1");
  db_->Put(WriteOptions(), "b", "2");

  Stats before;
  db_->GetCurrentStats(ReadOptions(), &before);

  // 模拟重启：关闭并重新打开同一 DB 目录
  delete db_;
  db_ = nullptr;

  Options opt;
  opt.create_if_missing = false;  // 期望已有目录
  ASSERT_TRUE(DB::Open(opt, dbname_, &db_).ok());

  Stats after;
  db_->GetCurrentStats(ReadOptions(), &after);

  ASSERT_EQ(before.total_entries, after.total_entries);
  ASSERT_EQ(before.distinct_keys, after.distinct_keys);
}

TEST_F(StatsDBTest, CompactionUpdate) {
  ASSERT_NE(db_, nullptr);

  for (int i = 0; i < 10000; ++i) {
    db_->Put(WriteOptions(), "k" + std::to_string(i), "v");
  }

  db_->CompactRange(nullptr, nullptr);

  Stats st;
  db_->GetCurrentStats(ReadOptions(), &st);

  ASSERT_GT(st.total_entries, 0);
  ASSERT_GT(st.distinct_keys, 0);
}

TEST_F(StatsDBTest, CompactionDeltaSpillsWithinWorkspace) {
  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.metadata_workspace_size = 64 * 1024;
  options.metadata_memtable_size = 1 * 1024 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  const int kKeys = 3000;
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "key-" + std::to_string(i);
    ASSERT_TRUE(db_->Put(WriteOptions(), key, "old").ok());
  }
  // Place the first generation on disk so the next flush overlaps it and
  // forces a primary compaction over a delta set larger than the workspace.
  db_->CompactRange(nullptr, nullptr);
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_TRUE(
        db_->Put(WriteOptions(), "key-" + std::to_string(i), "new").ok());
  }

  Stats before;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &before).ok());
  ASSERT_EQ(before.total_entries, 2 * kKeys);
  ASSERT_EQ(before.distinct_keys, kKeys);

  db_->CompactRange(nullptr, nullptr);

  Stats after;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &after).ok());
  ASSERT_EQ(after.total_entries, kKeys);
  ASSERT_EQ(after.distinct_keys, kKeys);
}

TEST_F(StatsDBTest, ManifestTailCompactionRecovery) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "hot", "first").ok());
  db_->CompactRange(nullptr, nullptr);
  ASSERT_TRUE(db_->Put(WriteOptions(), "hot", "second").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "stable", "v").ok());

  Stats before;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &before).ok());
  ASSERT_EQ(before.total_entries, 3);
  ASSERT_EQ(before.distinct_keys, 2);

  db_->CompactRange(nullptr, nullptr);
  Stats compacted;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &compacted).ok());
  ASSERT_EQ(compacted.total_entries, 2);
  ASSERT_EQ(compacted.distinct_keys, 2);

  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  Stats recovered;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &recovered).ok());
  ASSERT_EQ(recovered.total_entries, compacted.total_entries);
  ASSERT_EQ(recovered.distinct_keys, compacted.distinct_keys);
  ASSERT_EQ(recovered.seq, compacted.seq);
}

TEST_F(StatsDBTest, ReopenUsesPrimaryManifestCheckpoint) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "hot", "first").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "hot", "second").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "stable", "value").ok());
  db_->CompactRange(nullptr, nullptr);

  Stats before;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &before).ok());
  ASSERT_EQ(before.total_entries, 3);
  ASSERT_EQ(before.distinct_keys, 2);
  ASSERT_EQ(before.seq, 3);

  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.enable_metadata_profiling = true;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  Stats after;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &after).ok());
  ASSERT_EQ(after.total_entries, before.total_entries);
  ASSERT_EQ(after.distinct_keys, before.distinct_keys);
  ASSERT_EQ(after.seq, before.seq);
  ASSERT_EQ(static_cast<DBImpl*>(db_)->TEST_MetadataRebuildCount(), 0);
}

TEST_F(StatsDBTest, ReopenRebuildsStatsFromPrimaryRecords) {
  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.metadata_memtable_size = 64 * 1024;
  options.enable_metadata_profiling = true;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  const int kEntries = 1600;
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db_->Put(WriteOptions(), "key-" + std::to_string(i), "v").ok());
  }

  delete db_;
  db_ = nullptr;

  std::vector<std::string> children;
  ASSERT_TRUE(Env::Default()->GetChildren(dbname_, &children).ok());
  for (const std::string& child : children) {
    EXPECT_FALSE(child.size() >= 5 &&
                 (child.compare(child.size() - 5, 5, ".midx") == 0 ||
                  child.compare(child.size() - 5, 5, ".mchg") == 0))
        << "temporary metadata file survived close: " << child;
  }

  options.metadata_memtable_size = 1 * 1024 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, kEntries);
  ASSERT_EQ(stats.distinct_keys, kEntries);
  ASSERT_GT(static_cast<DBImpl*>(db_)->TEST_MetadataRebuildCount(), 0);
}

TEST_F(StatsDBTest, LargeWriteBatchSpillsAndRecoversPrimaryStats) {
  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.metadata_workspace_size = 8 * 1024;
  options.metadata_memtable_size = 1 * 1024 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  const int kKeys = 5000;
  WriteBatch batch;
  for (int i = 0; i < kKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%05d", i);
    batch.Put(key, "first");
  }
  // A second pass makes the same keys recur in later spilled runs.
  for (int i = 0; i < kKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-%05d", i);
    batch.Put(key, "second");
  }
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).ok());

  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 2 * kKeys);
  ASSERT_EQ(stats.distinct_keys, kKeys);
  ASSERT_EQ(stats.seq, 2 * kKeys);

  ASSERT_TRUE(db_->Put(WriteOptions(), "key-00001", "third").ok());
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 2 * kKeys + 1);
  ASSERT_EQ(stats.distinct_keys, kKeys);
  ASSERT_EQ(stats.seq, 2 * kKeys + 1);

  delete db_;
  db_ = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 2 * kKeys + 1);
  ASSERT_EQ(stats.distinct_keys, kKeys);
  ASSERT_EQ(stats.seq, 2 * kKeys + 1);

  // A large second batch also exercises bounded temporary workspaces. Its
  // scratch files must be removed before commit and never become DB files.
  delete db_;
  db_ = nullptr;
  options.metadata_memtable_size = 32 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  WriteBatch checkpoint_batch;
  const int kAdditionalKeys = 5000;
  for (int i = 0; i < kAdditionalKeys; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "extra-%05d", i);
    checkpoint_batch.Put(key, "value");
  }
  ASSERT_TRUE(db_->Write(WriteOptions(), &checkpoint_batch).ok());
  delete db_;
  db_ = nullptr;
  std::vector<std::string> children;
  ASSERT_TRUE(Env::Default()->GetChildren(dbname_, &children).ok());
  for (const std::string& child : children) {
    EXPECT_FALSE(child.size() >= 5 &&
                 (child.compare(child.size() - 5, 5, ".midx") == 0 ||
                  child.compare(child.size() - 5, 5, ".mchg") == 0))
        << "temporary metadata file survived close: " << child;
  }
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 2 * kKeys + 1 + kAdditionalKeys);
  ASSERT_EQ(stats.distinct_keys, kKeys + kAdditionalKeys);
  ASSERT_EQ(stats.seq, 2 * kKeys + 1 + kAdditionalKeys);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "key-00001", &value).ok());
  ASSERT_EQ(value, "third");
}

TEST_F(StatsDBTest, LargeBatchRecoversFromPrimaryWAL) {
  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.metadata_workspace_size = 64 * 1024;
  options.metadata_memtable_size = 1 * 1024 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  const int kKeys = 1200;
  WriteBatch batch;
  for (int pass = 0; pass < 2; ++pass) {
    for (int i = 0; i < kKeys; ++i) {
      char key[32];
      std::snprintf(key, sizeof(key), "key-%05d", i);
      batch.Put(key, pass == 0 ? "first" : "second");
    }
  }
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).ok());

  delete db_;
  db_ = nullptr;
  std::vector<std::string> children;
  ASSERT_TRUE(Env::Default()->GetChildren(dbname_, &children).ok());
  for (const std::string& child : children) {
    EXPECT_FALSE(child.size() >= 5 &&
                 (child.compare(child.size() - 5, 5, ".midx") == 0 ||
                  child.compare(child.size() - 5, 5, ".mchg") == 0))
        << "temporary metadata file survived close: " << child;
  }

  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 2 * kKeys);
  ASSERT_EQ(stats.distinct_keys, kKeys);
  ASSERT_EQ(stats.seq, 2 * kKeys);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "key-00001", &value).ok());
  ASSERT_EQ(value, "second");
}

TEST_F(StatsDBTest, RawByteDistinctKeysSurviveComparatorEquivalence) {
  delete db_;
  db_ = nullptr;
  Options defaults;
  ASSERT_TRUE(DestroyDB(dbname_, defaults).ok());

  static const CaseInsensitiveComparator comparator;
  Options options;
  options.create_if_missing = true;
  options.comparator = &comparator;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  WriteBatch batch;
  batch.Put("A", "upper-1");
  batch.Put("a", "lower-1");
  batch.Put("A", "upper-2");
  batch.Put("a", "lower-2");
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).ok());
  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 4);
  ASSERT_EQ(stats.distinct_keys, 2);

  delete db_;
  db_ = nullptr;
  options.create_if_missing = false;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 4);
  ASSERT_EQ(stats.distinct_keys, 2);

  db_->CompactRange(nullptr, nullptr);
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 1);
  ASSERT_EQ(stats.distinct_keys, 1);

  delete db_;
  db_ = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 1);
  ASSERT_EQ(stats.distinct_keys, 1);
  ASSERT_EQ(stats.seq, 4);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
