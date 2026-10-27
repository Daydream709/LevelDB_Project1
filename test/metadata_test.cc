#include "gtest/gtest.h"
#include "leveldb/env.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include <cstdlib>
#include <unistd.h>   // getpid
#include <cstdio>     // snprintf
#include <string>
#include <vector>

using namespace leveldb;

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

TEST_F(StatsDBTest, MissingMetadataRunRebuildsFromPrimary) {
  delete db_;
  db_ = nullptr;
  Options options;
  options.create_if_missing = false;
  options.metadata_memtable_size = 64 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());

  const int kEntries = 1600;
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db_->Put(WriteOptions(), "key-" + std::to_string(i), "v").ok());
  }

  auto metadata_field = [this](const std::string& field) -> unsigned long long {
    std::string property;
    EXPECT_TRUE(db_->GetProperty("leveldb.metadata-stats", &property));
    const std::string key = field + "=";
    const size_t offset = property.find(key);
    EXPECT_NE(offset, std::string::npos) << property;
    if (offset == std::string::npos) return 0;
    return std::strtoull(property.c_str() + offset + key.size(), nullptr, 10);
  };
  bool checkpointed = false;
  for (int i = 0; i < 500; ++i) {
    if (metadata_field("metadata_files") > 0 &&
        metadata_field("checkpoint_mseq") > 0) {
      checkpointed = true;
      break;
    }
    Env::Default()->SleepForMicroseconds(10000);
  }
  ASSERT_TRUE(checkpointed);
  delete db_;
  db_ = nullptr;

  std::vector<std::string> children;
  ASSERT_TRUE(Env::Default()->GetChildren(dbname_, &children).ok());
  std::string metadata_file;
  for (const std::string& child : children) {
    if (child.size() >= 5 && child.compare(child.size() - 5, 5, ".midx") == 0) {
      metadata_file = child;
      break;
    }
  }
  ASSERT_FALSE(metadata_file.empty());
  ASSERT_TRUE(Env::Default()->RemoveFile(dbname_ + "/" + metadata_file).ok());

  options.metadata_memtable_size = 1 * 1024 * 1024;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  Stats stats;
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, kEntries);
  ASSERT_EQ(stats.distinct_keys, kEntries);
}

TEST_F(StatsDBTest, LargeWriteBatchSpillsAndRecoversMetadata) {
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

  // A smaller budget makes the next commit freeze and checkpoint the WAL
  // referenced .mchg file together with the active generation.
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
  auto checkpoint_mseq = [this]() -> unsigned long long {
    std::string property;
    EXPECT_TRUE(db_->GetProperty("leveldb.metadata-stats", &property));
    const std::string key = "checkpoint_mseq=";
    const size_t offset = property.find(key);
    EXPECT_NE(offset, std::string::npos) << property;
    if (offset == std::string::npos) return 0;
    return std::strtoull(property.c_str() + offset + key.size(), nullptr, 10);
  };
  bool checkpointed = false;
  for (int i = 0; i < 500; ++i) {
    if (checkpoint_mseq() >= 3) {
      checkpointed = true;
      break;
    }
    Env::Default()->SleepForMicroseconds(10000);
  }
  if (!checkpointed) {
    std::string property;
    db_->GetProperty("leveldb.metadata-stats", &property);
    ADD_FAILURE() << "metadata checkpoint did not advance: " << property;
  }
  ASSERT_TRUE(checkpointed);

  delete db_;
  db_ = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname_, &db_).ok());
  ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  ASSERT_EQ(stats.total_entries, 2 * kKeys + 1 + kAdditionalKeys);
  ASSERT_EQ(stats.distinct_keys, kKeys + kAdditionalKeys);
  ASSERT_EQ(stats.seq, 2 * kKeys + 1 + kAdditionalKeys);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "key-00001", &value).ok());
  ASSERT_EQ(value, "third");
}

TEST_F(StatsDBTest, MissingLargeBatchChangeFileRebuildsFromPrimary) {
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
  std::string change_file;
  for (const std::string& child : children) {
    if (child.size() >= 5 && child.compare(child.size() - 5, 5, ".mchg") == 0) {
      change_file = child;
      break;
    }
  }
  ASSERT_FALSE(change_file.empty());
  ASSERT_TRUE(Env::Default()->RemoveFile(dbname_ + "/" + change_file).ok());

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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
