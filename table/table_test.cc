// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/write_batch_internal.h"
#include <algorithm>
#include <cstdio>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/table_builder.h"

#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "table/local_count_index.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {

TEST(LocalCountIndexTest, MultiLevelLookupAndCorruption) {
  std::map<uint64_t, std::string> blocks;
  uint64_t offset = 0;
  LocalCountIndexBuilder builder(1024, [&](const Slice& bytes, BlockHandle* h) {
    h->set_offset(offset);
    h->set_size(bytes.size());
    blocks.emplace(offset, bytes.ToString());
    offset += bytes.size() + kBlockTrailerSize;
    return Status::OK();
  });
  for (uint64_t i = 0; i < 10000; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key/%012llu",
                  static_cast<unsigned long long>(i));
    ASSERT_TRUE(builder.Add(key, i + 1).ok());
  }
  std::string descriptor;
  ASSERT_TRUE(builder.Finish(&descriptor).ok());
  ASSERT_GT(blocks.size(), 100);
  const uint64_t file_size = offset + Footer::kEncodedLength;
  auto read = [&](const BlockHandle& h,
                  std::shared_ptr<LocalCountBlock>* data) {
    auto it = blocks.find(h.offset());
    if (it == blocks.end() || it->second.size() != h.size())
      return Status::Corruption("test block");
    data->reset(new LocalCountBlock);
    (*data)->bytes = it->second;
    return Status::OK();
  };
  for (uint64_t i : {uint64_t{0}, uint64_t{511}, uint64_t{9999}}) {
    char key[32];
    std::snprintf(key, sizeof(key), "key/%012llu",
                  static_cast<unsigned long long>(i));
    uint64_t count = 0;
    ASSERT_TRUE(
        LookupLocalCount(descriptor, key, file_size, read, &count).ok());
    ASSERT_EQ(i + 1, count);
  }
  uint64_t count = 99;
  ASSERT_TRUE(
      LookupLocalCount(descriptor, "missing", file_size, read, &count).ok());
  ASSERT_EQ(0, count);
  std::string unknown = descriptor;
  unknown[3] = '9';
  ASSERT_FALSE(
      LookupLocalCount(unknown, "key/000000000000", file_size, read, &count)
          .ok());
  ASSERT_FALSE(LookupLocalCount(Slice(descriptor.data(), descriptor.size() - 1),
                                "x", file_size, read, &count)
                   .ok());
  std::string leaf = blocks.begin()->second;
  blocks.begin()->second.assign(1, '\0');
  ASSERT_FALSE(
      LookupLocalCount(descriptor, "key/000000000000", file_size, read, &count)
          .ok());
  ASSERT_EQ(0, count);
  blocks.begin()->second = leaf;
  blocks.rbegin()->second.assign(1, '\0');
  ASSERT_FALSE(
      LookupLocalCount(descriptor, "missing", file_size, read, &count).ok());
  ASSERT_EQ(0, count);
}

TEST(LocalCountIndexTest, RejectsInvalidCountsAndOrder) {
  LocalCountIndexBuilder builder(1024, [](const Slice&, BlockHandle*) {
    return Status::IOError("test output failure");
  });
  ASSERT_FALSE(builder.Add("x", 0).ok());
  ASSERT_TRUE(builder.Add("", 1).ok());
  ASSERT_FALSE(builder.Add("", 1).ok());
  ASSERT_FALSE(builder.Add("z", std::numeric_limits<uint64_t>::max()).ok());
  std::string descriptor;
  ASSERT_TRUE(builder.Finish(&descriptor).IsIOError());
  ASSERT_TRUE(descriptor.empty());
}

// Return reverse of "key".
// Used to test non-lexicographic comparators.
static std::string Reverse(const Slice& key) {
  std::string str(key.ToString());
  std::string rev("");
  for (std::string::reverse_iterator rit = str.rbegin(); rit != str.rend();
       ++rit) {
    rev.push_back(*rit);
  }
  return rev;
}

namespace {
class ReverseKeyComparator : public Comparator {
 public:
  const char* Name() const override {
    return "leveldb.ReverseBytewiseComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    return BytewiseComparator()->Compare(Reverse(a), Reverse(b));
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    std::string s = Reverse(*start);
    std::string l = Reverse(limit);
    BytewiseComparator()->FindShortestSeparator(&s, l);
    *start = Reverse(s);
  }

  void FindShortSuccessor(std::string* key) const override {
    std::string s = Reverse(*key);
    BytewiseComparator()->FindShortSuccessor(&s);
    *key = Reverse(s);
  }
};
}  // namespace
static ReverseKeyComparator reverse_key_comparator;

static void Increment(const Comparator* cmp, std::string* key) {
  if (cmp == BytewiseComparator()) {
    key->push_back('\0');
  } else {
    assert(cmp == &reverse_key_comparator);
    std::string rev = Reverse(*key);
    rev.push_back('\0');
    *key = Reverse(rev);
  }
}

// An STL comparator that uses a Comparator
namespace {
struct STLLessThan {
  const Comparator* cmp;

  STLLessThan() : cmp(BytewiseComparator()) {}
  STLLessThan(const Comparator* c) : cmp(c) {}
  bool operator()(const std::string& a, const std::string& b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }
};
}  // namespace

class StringSink : public WritableFile {
 public:
  ~StringSink() override = default;

  const std::string& contents() const { return contents_; }

  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }

  Status Append(const Slice& data) override {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};

class StringSource : public RandomAccessFile {
 public:
  StringSource(const Slice& contents)
      : contents_(contents.data(), contents.size()) {}

  ~StringSource() override = default;

  uint64_t Size() const { return contents_.size(); }
  uint64_t Reads() const { return reads_; }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    ++reads_;
    if (offset >= contents_.size()) {
      return Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - offset;
    }
    std::memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  std::string contents_;
  mutable uint64_t reads_ = 0;
};

class RawEquivalentComparator : public Comparator {
 public:
  const char* Name() const override { return "local-count-test-equivalent"; }
  int Compare(const Slice&, const Slice&) const override { return 0; }
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
};

TEST(LocalCountIndexTest, PrimarySSTBytewiseAndCustomSpill) {
  RawEquivalentComparator equivalent;
  for (bool custom : {false, true}) {
    InternalKeyComparator internal(
        custom ? static_cast<const Comparator*>(&equivalent)
               : BytewiseComparator());
    Options options;
    options.comparator = &internal;
    options.compression = kNoCompression;
    options.metadata_workspace_size = 256;  // Exercise multiple merge passes.
    options.metadata_cache_size = 8192;
    options.block_size = 128;
    StringSink sink;
    TableBuilder builder(options, &sink);
    builder.EnableInternalStatsMetadata();
    std::string prefix =
        testing::TempDir() + "local-count-" + std::to_string(custom) + ".ldb";
    LocalCountTableAccess::Enable(&builder, prefix);
    std::map<std::string, uint64_t> expected;
    std::vector<std::pair<std::string, std::string>> records;
    uint64_t seq = 100000;
    for (int i = 0; i < 1200; ++i) {
      std::string key =
          i == 0 ? ""
                 : std::string("raw\0", 4) + std::to_string((i * 43) % 317);
      if (i == 1) key = std::string(10000, 'z');
      ValueType type = static_cast<ValueType>(i % 3);
      InternalKey ikey(key, seq--, type);
      std::string value = "value";
      if (type == kTypeValueWithTTL) {
        value.clear();
        PutFixed64(&value, 1234);
        value += "ttl";
      }
      records.emplace_back(ikey.Encode().ToString(), value);
      ++expected[key];
    }
    std::sort(records.begin(), records.end(),
              [&](const auto& a, const auto& b) {
                return internal.Compare(a.first, b.first) < 0;
              });
    for (const auto& record : records) builder.Add(record.first, record.second);
    ASSERT_TRUE(builder.Finish().ok()) << builder.status().ToString();
    StringSource source(sink.contents());
    Table* raw = nullptr;
    ASSERT_TRUE(Table::Open(options, &source, source.Size(), &raw).ok());
    std::unique_ptr<Table> table(raw);
    std::unique_ptr<Cache> cache(NewLRUCache(options.metadata_cache_size / 2));
    for (const auto& entry : expected) {
      uint64_t count = 99;
      ASSERT_TRUE(LocalCountTableAccess::Lookup(table.get(), cache.get(),
                                                entry.first, &count)
                      .ok());
      ASSERT_EQ(entry.second, count) << entry.first;
    }
    uint64_t absent = 99;
    ASSERT_TRUE(LocalCountTableAccess::Lookup(table.get(), cache.get(),
                                              "absent", &absent)
                    .ok());
    ASSERT_EQ(0, absent);
    TableInternalStats stats;
    ASSERT_TRUE(table->GetInternalStats(&stats).ok());
    ASSERT_TRUE(stats.valid);
    ASSERT_EQ(records.size(), stats.total_records);
    std::unique_ptr<Iterator> iter(table->NewIterator(ReadOptions()));
    size_t n = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++n) {
      ASSERT_EQ(records[n].first, iter->key().ToString());
      ASSERT_EQ(records[n].second, iter->value().ToString());
    }
    ASSERT_TRUE(iter->status().ok());
    ASSERT_EQ(records.size(), n);
    ASSERT_LE(cache->TotalCharge(), options.metadata_cache_size / 2);
    std::vector<std::string> files;
    ASSERT_TRUE(options.env->GetChildren(testing::TempDir(), &files).ok());
    for (const auto& file : files)
      ASSERT_EQ(
          std::string::npos,
          file.find("local-count-" + std::to_string(custom) + ".ldb.counts."));
  }
}

TEST(LocalCountIndexTest, OptionalCorruptionKeepsFileSummary) {
  InternalKeyComparator internal(BytewiseComparator());
  Options options;
  options.comparator = &internal;
  options.compression = kNoCompression;
  StringSink sink;
  {
    TableBuilder builder(options, &sink);
    builder.EnableInternalStatsMetadata();
    LocalCountTableAccess::Enable(&builder, "unused-bytewise");
    builder.Add(InternalKey("a", 1, kTypeValue).Encode(), "v");
    ASSERT_TRUE(builder.Finish().ok());
  }
  Footer footer;
  Slice encoded(
      sink.contents().data() + sink.contents().size() - Footer::kEncodedLength,
      Footer::kEncodedLength);
  ASSERT_TRUE(footer.DecodeFrom(&encoded).ok());
  // Decode the descriptor from the independently checksummed metaindex.
  StringSource original(sink.contents());
  BlockContents contents;
  ReadOptions read;
  read.verify_checksums = true;
  ASSERT_TRUE(
      ReadBlock(&original, read, footer.metaindex_handle(), &contents).ok());
  Block block(contents);
  std::unique_ptr<Iterator> meta(block.NewIterator(BytewiseComparator()));
  meta->Seek("leveldb.local-counts.v1");
  ASSERT_TRUE(meta->Valid());
  BlockHandle descriptor_handle;
  Slice dh = meta->value();
  ASSERT_TRUE(descriptor_handle.DecodeFrom(&dh).ok());
  BlockContents descriptor;
  ASSERT_TRUE(ReadBlock(&original, read, descriptor_handle, &descriptor).ok());
  std::unique_ptr<char[]> descriptor_owner(
      descriptor.heap_allocated ? const_cast<char*>(descriptor.data.data())
                                : nullptr);
  Slice d = descriptor.data;
  d.remove_prefix(4);
  uint32_t height;
  uint64_t entries, records;
  ASSERT_TRUE(GetVarint32(&d, &height));
  ASSERT_TRUE(GetVarint64(&d, &entries));
  ASSERT_TRUE(GetVarint64(&d, &records));
  BlockHandle root;
  ASSERT_TRUE(root.DecodeFrom(&d).ok());
  std::unique_ptr<Cache> cache(NewLRUCache(1 << 20));
  // Populate a cache with a valid table; later instances must never reuse it.
  {
    Table* valid = nullptr;
    ASSERT_TRUE(Table::Open(options, &original, original.Size(), &valid).ok());
    std::unique_ptr<Table> table(valid);
    uint64_t count = 0;
    ASSERT_TRUE(
        LocalCountTableAccess::Lookup(table.get(), cache.get(), "a", &count)
            .ok());
    ASSERT_EQ(1, count);
  }
  for (int corruption = 0; corruption < 3; ++corruption) {
    std::string damaged = sink.contents();
    if (corruption == 0)
      damaged[root.offset()] ^= 0x40;
    else {
      damaged[descriptor_handle.offset() + 3] = '9';
      if (corruption == 2) {
        // Unknown but checksummed descriptor version.
        const auto off = descriptor_handle.offset();
        const auto size = descriptor_handle.size();
        uint32_t crc = crc32c::Value(damaged.data() + off, size + 1);
        EncodeFixed32(&damaged[off + size + 1], crc32c::Mask(crc));
      }
    }
    StringSource source(damaged);
    Table* raw = nullptr;
    ASSERT_TRUE(Table::Open(options, &source, source.Size(), &raw).ok());
    std::unique_ptr<Table> table(raw);
    uint64_t count = 99;
    ASSERT_FALSE(
        LocalCountTableAccess::Lookup(table.get(), cache.get(), "a", &count)
            .ok());
    ASSERT_EQ(0, count);
    TableInternalStats stats;
    ASSERT_TRUE(table->GetInternalStats(&stats).ok());
    ASSERT_TRUE(stats.valid);
    ASSERT_EQ(1, stats.total_records);
    std::unique_ptr<Iterator> iter(table->NewIterator(read));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("v", iter->value().ToString());
  }
}

TEST(LocalCountIndexTest, TableCacheFallbackAndErrorPropagation) {
  const std::string dir = testing::TempDir() + "local-count-fallback";
  Env* env = Env::Default();
  env->CreateDir(dir);
  InternalKeyComparator internal(BytewiseComparator());
  Options options;
  options.comparator = &internal;
  options.compression = kNoCompression;
  for (bool indexed : {false, true}) {
    const uint64_t number = indexed ? 2 : 1;
    const std::string path = TableFileName(dir, number);
    WritableFile* out = nullptr;
    ASSERT_TRUE(env->NewWritableFile(path, &out).ok());
    {
      TableBuilder builder(options, out);
      builder.EnableInternalStatsMetadata();
      if (indexed) LocalCountTableAccess::Enable(&builder, path);
      for (uint64_t seq = 20; seq > 0; --seq)
        builder.Add(InternalKey("a", seq, kTypeValue).Encode(), "value");
      ASSERT_TRUE(builder.Finish().ok());
    }
    ASSERT_TRUE(out->Close().ok());
    delete out;
    uint64_t size = 0;
    ASSERT_TRUE(env->GetFileSize(path, &size).ok());
    // Both old files and files with a damaged optional index must use exact
    // primary records, without hiding real data checksum errors.
    if (indexed) {
      std::string bytes;
      ASSERT_TRUE(ReadFileToString(env, path, &bytes).ok());
      Footer footer;
      Slice encoded(bytes.data() + bytes.size() - Footer::kEncodedLength,
                    Footer::kEncodedLength);
      ASSERT_TRUE(footer.DecodeFrom(&encoded).ok());
      StringSource source(bytes);
      BlockContents contents;
      ASSERT_TRUE(ReadBlock(&source, ReadOptions(), footer.metaindex_handle(),
                            &contents)
                      .ok());
      Block block(contents);
      std::unique_ptr<Iterator> meta(block.NewIterator(BytewiseComparator()));
      meta->Seek("leveldb.local-counts.v1");
      ASSERT_TRUE(meta->Valid());
      BlockHandle descriptor_handle;
      Slice dh = meta->value();
      ASSERT_TRUE(descriptor_handle.DecodeFrom(&dh).ok());
      BlockContents descriptor;
      ASSERT_TRUE(
          ReadBlock(&source, ReadOptions(), descriptor_handle, &descriptor)
              .ok());
      std::unique_ptr<char[]> descriptor_owner(
          descriptor.heap_allocated ? const_cast<char*>(descriptor.data.data())
                                    : nullptr);
      Slice d = descriptor.data;
      d.remove_prefix(4);
      uint32_t h;
      uint64_t n, records;
      ASSERT_TRUE(GetVarint32(&d, &h));
      ASSERT_TRUE(GetVarint64(&d, &n));
      ASSERT_TRUE(GetVarint64(&d, &records));
      BlockHandle root;
      ASSERT_TRUE(root.DecodeFrom(&d).ok());
      bytes[root.offset()] ^= 0x40;
      ASSERT_TRUE(WriteStringToFile(env, bytes, path).ok());
    }
    {
      TableCache cache(dir, options, 4);
      uint64_t count = 0;
      ASSERT_TRUE(cache.CountUserKeyRecords(number, size, "a", &count).ok());
      ASSERT_EQ(20, count);
      bool exists = false;
      ASSERT_TRUE(cache.HasUserKeyRecords(number, size, "a", &exists).ok());
      ASSERT_TRUE(exists);
      ASSERT_TRUE(
          cache.HasUserKeyRecords(number, size, "absent", &exists).ok());
      ASSERT_FALSE(exists);
    }
    std::string good;
    ASSERT_TRUE(ReadFileToString(env, path, &good).ok());
    Footer footer;
    Slice tail(good.data() + good.size() - Footer::kEncodedLength,
               Footer::kEncodedLength);
    ASSERT_TRUE(footer.DecodeFrom(&tail).ok());
    std::string bad_directory = good;
    // Encoding is still readable; only the primary directory CRC is bad.
    bad_directory[footer.index_handle().offset() +
                  footer.index_handle().size() + 1] ^= 0x40;
    ASSERT_TRUE(WriteStringToFile(env, bad_directory, path).ok());
    {
      TableCache cache(dir, options, 4);
      uint64_t count = 99;
      ASSERT_FALSE(cache.CountUserKeyRecords(number, size, "a", &count).ok());
      bool exists = false;
      ASSERT_FALSE(cache.HasUserKeyRecords(number, size, "a", &exists).ok());
    }
    ASSERT_TRUE(WriteStringToFile(env, good, path).ok());
    std::string broken = good;
    broken[0] ^= 0x40;
    ASSERT_TRUE(WriteStringToFile(env, broken, path).ok());
    {
      TableCache cache(dir, options, 4);
      uint64_t count = 99;
      ASSERT_FALSE(cache.CountUserKeyRecords(number, size, "a", &count).ok());
      bool exists = false;
      ASSERT_FALSE(cache.HasUserKeyRecords(number, size, "a", &exists).ok());
    }
    ASSERT_TRUE(env->RemoveFile(path).ok());
  }
  env->RemoveDir(dir);
}

class CountRunFailureEnv : public EnvWrapper {
 public:
  CountRunFailureEnv() : EnvWrapper(Env::Default()) {}
  Status NewWritableFile(const std::string& name,
                         WritableFile** file) override {
    if (name.find(".counts.") != std::string::npos) {
      *file = nullptr;
      return Status::IOError("injected count run failure");
    }
    return target()->NewWritableFile(name, file);
  }
};
TEST(LocalCountIndexTest, SpillFailureAndRecoveryCleanup) {
  CountRunFailureEnv fail;
  Options options;
  options.env = &fail;
  options.metadata_workspace_size = 1;
  LocalCountIndexBuilder builder(
      4096, [](const Slice&, BlockHandle*) { return Status::OK(); });
  LocalCountCollector collector(options, testing::TempDir() + "fail.ldb", false,
                                &builder);
  ASSERT_TRUE(collector.Add("key").IsIOError());
  const std::string dir = testing::TempDir() + "local-count-restart";
  options.env = Env::Default();
  options.create_if_missing = true;
  DestroyDB(dir, options);
  options.env->CreateDir(dir);
  const std::string orphan = dir + "/000123.ldb.counts.45.dbtmp";
  ASSERT_TRUE(WriteStringToFile(options.env, "incomplete run", orphan).ok());
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dir, &db).ok());
  ASSERT_FALSE(options.env->FileExists(orphan));
  delete db;
  ASSERT_TRUE(DestroyDB(dir, options).ok());
}

TEST(LocalCountIndexTest, CacheWarmHitsAndSmallCacheEviction) {
  InternalKeyComparator internal(BytewiseComparator());
  Options options;
  options.comparator = &internal;
  options.metadata_cache_size = 2 << 20;
  StringSink sink;
  {
    TableBuilder builder(options, &sink);
    builder.EnableInternalStatsMetadata();
    LocalCountTableAccess::Enable(&builder, "unused");
    for (uint64_t i = 0; i < 10000; ++i) {
      char key[32];
      std::snprintf(key, sizeof(key), "%012llu", (unsigned long long)i);
      builder.Add(InternalKey(key, 1, kTypeValue).Encode(), "v");
    }
    ASSERT_TRUE(builder.Finish().ok());
  }
  StringSource source(sink.contents());
  Table* raw = nullptr;
  ASSERT_TRUE(Table::Open(options, &source, source.Size(), &raw).ok());
  std::unique_ptr<Table> table(raw);
  std::unique_ptr<Cache> cache(NewLRUCache(options.metadata_cache_size / 2));
  uint64_t count;
  const auto before = source.Reads();
  ASSERT_TRUE(LocalCountTableAccess::Lookup(table.get(), cache.get(),
                                            "000000000017", &count)
                  .ok());
  const auto cold_reads = source.Reads() - before;
  ASSERT_GE(cold_reads, 2);
  ASSERT_LT(cold_reads, 6);
  ASSERT_TRUE(LocalCountTableAccess::Lookup(table.get(), cache.get(),
                                            "000000000017", &count)
                  .ok());
  ASSERT_EQ(before + cold_reads, source.Reads());
  ASSERT_EQ(1, count);
  // Admitted blocks also face real eviction, not just oversized bypass.
  options.metadata_cache_size = 512 << 10;
  Table* pressure_raw = nullptr;
  ASSERT_TRUE(Table::Open(options, &source, source.Size(), &pressure_raw).ok());
  std::unique_ptr<Table> pressure_table(pressure_raw);
  std::unique_ptr<Cache> pressure(NewLRUCache(options.metadata_cache_size / 2));
  for (int pass = 0; pass < 2; ++pass) {
    const auto reads_before = source.Reads();
    for (int i = 0; i < 200; ++i) {
      char key[32];
      std::snprintf(key, sizeof(key), "%012llu", (unsigned long long)(i * 47));
      ASSERT_TRUE(LocalCountTableAccess::Lookup(pressure_table.get(),
                                                pressure.get(), key, &count)
                      .ok());
      ASSERT_EQ(1, count);
      ASSERT_LE(pressure->TotalCharge(), options.metadata_cache_size / 2);
    }
    ASSERT_GT(pressure->TotalCharge(), 0);
    ASSERT_GT(source.Reads(), reads_before);
  }
  options.metadata_cache_size = 128;
  Table* tiny_raw = nullptr;
  ASSERT_TRUE(Table::Open(options, &source, source.Size(), &tiny_raw).ok());
  std::unique_ptr<Table> tiny_table(tiny_raw);
  std::unique_ptr<Cache> tiny(NewLRUCache(64));
  for (int i = 0; i < 20; ++i) {
    ASSERT_TRUE(LocalCountTableAccess::Lookup(tiny_table.get(), tiny.get(),
                                              "000000000017", &count)
                    .ok());
    ASSERT_EQ(1, count);
    ASSERT_LE(tiny->TotalCharge(), 64);
  }
}

TEST(LocalCountIndexTest, MemTableExactRawEquivalenceAndPresence) {
  RawEquivalentComparator cmp;
  InternalKeyComparator internal(&cmp);
  MemTable* mem = new MemTable(internal);
  mem->Ref();
  mem->Add(5, kTypeValue, "a", "v");
  mem->Add(4, kTypeDeletion, "b", "");
  mem->Add(3, kTypeValue, "a", "old");
  mem->Add(2, kTypeDeletion, "", "");
  uint64_t count;
  bool exists;
  ASSERT_TRUE(mem->CountUserKeyRecords("a", &count).ok());
  ASSERT_EQ(2, count);
  ASSERT_TRUE(mem->CountUserKeyRecords("b", &count).ok());
  ASSERT_EQ(1, count);
  ASSERT_TRUE(mem->HasUserKeyRecords("", &exists).ok());
  ASSERT_TRUE(exists);
  ASSERT_TRUE(mem->HasUserKeyRecords("missing", &exists).ok());
  ASSERT_FALSE(exists);
  mem->Unref();
}

typedef std::map<std::string, std::string, STLLessThan> KVMap;

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator* cmp) : data_(STLLessThan(cmp)) {}
  virtual ~Constructor() = default;

  void Add(const std::string& key, const Slice& value) {
    data_[key] = value.ToString();
  }

  // Finish constructing the data structure with all the keys that have
  // been added so far.  Returns the keys in sorted order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options& options, std::vector<std::string>* keys,
              KVMap* kvmap) {
    *kvmap = data_;
    keys->clear();
    for (const auto& kvp : data_) {
      keys->push_back(kvp.first);
    }
    data_.clear();
    Status s = FinishImpl(options, *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options& options, const KVMap& data) = 0;

  virtual Iterator* NewIterator() const = 0;

  const KVMap& data() const { return data_; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor

 private:
  KVMap data_;
};

class BlockConstructor : public Constructor {
 public:
  explicit BlockConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp), block_(nullptr) {}
  ~BlockConstructor() override { delete block_; }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    delete block_;
    block_ = nullptr;
    BlockBuilder builder(&options);

    for (const auto& kvp : data) {
      builder.Add(kvp.first, kvp.second);
    }
    // Open the block
    data_ = builder.Finish().ToString();
    BlockContents contents;
    contents.data = data_;
    contents.cachable = false;
    contents.heap_allocated = false;
    block_ = new Block(contents);
    return Status::OK();
  }
  Iterator* NewIterator() const override {
    return block_->NewIterator(comparator_);
  }

 private:
  const Comparator* const comparator_;
  std::string data_;
  Block* block_;

  BlockConstructor();
};

class TableConstructor : public Constructor {
 public:
  TableConstructor(const Comparator* cmp)
      : Constructor(cmp), source_(nullptr), table_(nullptr) {}
  ~TableConstructor() override { Reset(); }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    Reset();
    StringSink sink;
    TableBuilder builder(options, &sink);

    for (const auto& kvp : data) {
      builder.Add(kvp.first, kvp.second);
      EXPECT_LEVELDB_OK(builder.status());
    }
    Status s = builder.Finish();
    EXPECT_LEVELDB_OK(s);

    EXPECT_EQ(sink.contents().size(), builder.FileSize());

    // Open the table
    source_ = new StringSource(sink.contents());
    Options table_options;
    table_options.comparator = options.comparator;
    return Table::Open(table_options, source_, sink.contents().size(), &table_);
  }

  Iterator* NewIterator() const override {
    return table_->NewIterator(ReadOptions());
  }

  uint64_t ApproximateOffsetOf(const Slice& key) const {
    return table_->ApproximateOffsetOf(key);
  }

 private:
  void Reset() {
    delete table_;
    delete source_;
    table_ = nullptr;
    source_ = nullptr;
  }

  StringSource* source_;
  Table* table_;

  TableConstructor();
};

// A helper class that converts internal format keys into user keys
class KeyConvertingIterator : public Iterator {
 public:
  explicit KeyConvertingIterator(Iterator* iter) : iter_(iter) {}

  KeyConvertingIterator(const KeyConvertingIterator&) = delete;
  KeyConvertingIterator& operator=(const KeyConvertingIterator&) = delete;

  ~KeyConvertingIterator() override { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }
  void Seek(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->Seek(encoded);
  }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }

  Slice key() const override {
    assert(Valid());
    ParsedInternalKey key;
    if (!ParseInternalKey(iter_->key(), &key)) {
      status_ = Status::Corruption("malformed internal key");
      return Slice("corrupted key");
    }
    return key.user_key;
  }

  Slice value() const override { return iter_->value(); }
  Status status() const override {
    return status_.ok() ? iter_->status() : status_;
  }

 private:
  mutable Status status_;
  Iterator* iter_;
};

class MemTableConstructor : public Constructor {
 public:
  explicit MemTableConstructor(const Comparator* cmp)
      : Constructor(cmp), internal_comparator_(cmp) {
    memtable_ = new MemTable(internal_comparator_);
    memtable_->Ref();
  }
  ~MemTableConstructor() override { memtable_->Unref(); }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    memtable_->Unref();
    memtable_ = new MemTable(internal_comparator_);
    memtable_->Ref();
    int seq = 1;
    for (const auto& kvp : data) {
      memtable_->Add(seq, kTypeValue, kvp.first, kvp.second);
      seq++;
    }
    return Status::OK();
  }
  Iterator* NewIterator() const override {
    return new KeyConvertingIterator(memtable_->NewIterator());
  }

 private:
  const InternalKeyComparator internal_comparator_;
  MemTable* memtable_;
};

class DBConstructor : public Constructor {
 public:
  explicit DBConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp) {
    db_ = nullptr;
    NewDB();
  }
  ~DBConstructor() override { delete db_; }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    delete db_;
    db_ = nullptr;
    NewDB();
    for (const auto& kvp : data) {
      WriteBatch batch;
      batch.Put(kvp.first, kvp.second);
      EXPECT_TRUE(db_->Write(WriteOptions(), &batch).ok());
    }
    return Status::OK();
  }
  Iterator* NewIterator() const override {
    return db_->NewIterator(ReadOptions());
  }

  DB* db() const override { return db_; }

 private:
  void NewDB() {
    std::string name = testing::TempDir() + "table_testdb";

    Options options;
    options.comparator = comparator_;
    Status status = DestroyDB(name, options);
    ASSERT_TRUE(status.ok()) << status.ToString();

    options.create_if_missing = true;
    options.error_if_exists = true;
    options.write_buffer_size = 10000;  // Something small to force merging
    status = DB::Open(options, name, &db_);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  const Comparator* const comparator_;
  DB* db_;
};

enum TestType { TABLE_TEST, BLOCK_TEST, MEMTABLE_TEST, DB_TEST };

struct TestArgs {
  TestType type;
  bool reverse_compare;
  int restart_interval;
};

static const TestArgs kTestArgList[] = {
    {TABLE_TEST, false, 16},
    {TABLE_TEST, false, 1},
    {TABLE_TEST, false, 1024},
    {TABLE_TEST, true, 16},
    {TABLE_TEST, true, 1},
    {TABLE_TEST, true, 1024},

    {BLOCK_TEST, false, 16},
    {BLOCK_TEST, false, 1},
    {BLOCK_TEST, false, 1024},
    {BLOCK_TEST, true, 16},
    {BLOCK_TEST, true, 1},
    {BLOCK_TEST, true, 1024},

    // Restart interval does not matter for memtables
    {MEMTABLE_TEST, false, 16},
    {MEMTABLE_TEST, true, 16},

    // Do not bother with restart interval variations for DB
    {DB_TEST, false, 16},
    {DB_TEST, true, 16},
};
static const int kNumTestArgs = sizeof(kTestArgList) / sizeof(kTestArgList[0]);

class Harness : public testing::Test {
 public:
  Harness() : constructor_(nullptr) {}

  void Init(const TestArgs& args) {
    delete constructor_;
    constructor_ = nullptr;
    options_ = Options();

    options_.block_restart_interval = args.restart_interval;
    // Use shorter block size for tests to exercise block boundary
    // conditions more.
    options_.block_size = 256;
    if (args.reverse_compare) {
      options_.comparator = &reverse_key_comparator;
    }
    switch (args.type) {
      case TABLE_TEST:
        constructor_ = new TableConstructor(options_.comparator);
        break;
      case BLOCK_TEST:
        constructor_ = new BlockConstructor(options_.comparator);
        break;
      case MEMTABLE_TEST:
        constructor_ = new MemTableConstructor(options_.comparator);
        break;
      case DB_TEST:
        constructor_ = new DBConstructor(options_.comparator);
        break;
    }
  }

  ~Harness() { delete constructor_; }

  void Add(const std::string& key, const std::string& value) {
    constructor_->Add(key, value);
  }

  void Test(Random* rnd) {
    std::vector<std::string> keys;
    KVMap data;
    constructor_->Finish(options_, &keys, &data);

    TestForwardScan(keys, data);
    TestBackwardScan(keys, data);
    TestRandomAccess(rnd, keys, data);
  }

  void TestForwardScan(const std::vector<std::string>& keys,
                       const KVMap& data) {
    Iterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToFirst();
    for (KVMap::const_iterator model_iter = data.begin();
         model_iter != data.end(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Next();
    }
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  }

  void TestBackwardScan(const std::vector<std::string>& keys,
                        const KVMap& data) {
    Iterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToLast();
    for (KVMap::const_reverse_iterator model_iter = data.rbegin();
         model_iter != data.rend(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Prev();
    }
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  }

  void TestRandomAccess(Random* rnd, const std::vector<std::string>& keys,
                        const KVMap& data) {
    static const bool kVerbose = false;
    Iterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    KVMap::const_iterator model_iter = data.begin();
    if (kVerbose) std::fprintf(stderr, "---\n");
    for (int i = 0; i < 200; i++) {
      const int toss = rnd->Uniform(5);
      switch (toss) {
        case 0: {
          if (iter->Valid()) {
            if (kVerbose) std::fprintf(stderr, "Next\n");
            iter->Next();
            ++model_iter;
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 1: {
          if (kVerbose) std::fprintf(stderr, "SeekToFirst\n");
          iter->SeekToFirst();
          model_iter = data.begin();
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 2: {
          std::string key = PickRandomKey(rnd, keys);
          model_iter = data.lower_bound(key);
          if (kVerbose)
            std::fprintf(stderr, "Seek '%s'\n", EscapeString(key).c_str());
          iter->Seek(Slice(key));
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 3: {
          if (iter->Valid()) {
            if (kVerbose) std::fprintf(stderr, "Prev\n");
            iter->Prev();
            if (model_iter == data.begin()) {
              model_iter = data.end();  // Wrap around to invalid value
            } else {
              --model_iter;
            }
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 4: {
          if (kVerbose) std::fprintf(stderr, "SeekToLast\n");
          iter->SeekToLast();
          if (keys.empty()) {
            model_iter = data.end();
          } else {
            std::string last = data.rbegin()->first;
            model_iter = data.lower_bound(last);
          }
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
      }
    }
    delete iter;
  }

  std::string ToString(const KVMap& data, const KVMap::const_iterator& it) {
    if (it == data.end()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const KVMap& data,
                       const KVMap::const_reverse_iterator& it) {
    if (it == data.rend()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const Iterator* it) {
    if (!it->Valid()) {
      return "END";
    } else {
      return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
    }
  }

  std::string PickRandomKey(Random* rnd, const std::vector<std::string>& keys) {
    if (keys.empty()) {
      return "foo";
    } else {
      const int index = rnd->Uniform(keys.size());
      std::string result = keys[index];
      switch (rnd->Uniform(3)) {
        case 0:
          // Return an existing key
          break;
        case 1: {
          // Attempt to return something smaller than an existing key
          if (!result.empty() && result[result.size() - 1] > '\0') {
            result[result.size() - 1]--;
          }
          break;
        }
        case 2: {
          // Return something larger than an existing key
          Increment(options_.comparator, &result);
          break;
        }
      }
      return result;
    }
  }

  // Returns nullptr if not running against a DB
  DB* db() const { return constructor_->db(); }

 private:
  Options options_;
  Constructor* constructor_;
};

// Test empty table/block.
TEST_F(Harness, Empty) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 1);
    Test(&rnd);
  }
}

// Special test for a block with no restart entries.  The C++ leveldb
// code never generates such blocks, but the Java version of leveldb
// seems to.
TEST_F(Harness, ZeroRestartPointsInBlock) {
  char data[sizeof(uint32_t)];
  memset(data, 0, sizeof(data));
  BlockContents contents;
  contents.data = Slice(data, sizeof(data));
  contents.cachable = false;
  contents.heap_allocated = false;
  Block block(contents);
  Iterator* iter = block.NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());
  iter->SeekToLast();
  ASSERT_TRUE(!iter->Valid());
  iter->Seek("foo");
  ASSERT_TRUE(!iter->Valid());
  delete iter;
}

// Test the empty key
TEST_F(Harness, SimpleEmptyKey) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 1);
    Add("", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleSingle) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 2);
    Add("abc", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleMulti) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 3);
    Add("abc", "v");
    Add("abcd", "v");
    Add("ac", "v2");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleSpecialKey) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 4);
    Add("\xff\xff", "v3");
    Test(&rnd);
  }
}

TEST_F(Harness, Randomized) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 5);
    for (int num_entries = 0; num_entries < 2000;
         num_entries += (num_entries < 50 ? 1 : 200)) {
      if ((num_entries % 10) == 0) {
        std::fprintf(stderr, "case %d of %d: num_entries = %d\n", (i + 1),
                     int(kNumTestArgs), num_entries);
      }
      for (int e = 0; e < num_entries; e++) {
        std::string v;
        Add(test::RandomKey(&rnd, rnd.Skewed(4)),
            test::RandomString(&rnd, rnd.Skewed(5), &v).ToString());
      }
      Test(&rnd);
    }
  }
}

TEST_F(Harness, RandomizedLongDB) {
  Random rnd(test::RandomSeed());
  TestArgs args = {DB_TEST, false, 16};
  Init(args);
  int num_entries = 100000;
  for (int e = 0; e < num_entries; e++) {
    std::string v;
    Add(test::RandomKey(&rnd, rnd.Skewed(4)),
        test::RandomString(&rnd, rnd.Skewed(5), &v).ToString());
  }
  Test(&rnd);

  // We must have created enough data to force merging
  int files = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    std::string value;
    char name[100];
    std::snprintf(name, sizeof(name), "leveldb.num-files-at-level%d", level);
    ASSERT_TRUE(db()->GetProperty(name, &value));
    files += atoi(value.c_str());
  }
  ASSERT_GT(files, 0);
}

TEST(BlockTest, RejectsOverflowingEntryLengths) {
  std::string contents;
  PutVarint32(&contents, 0);
  PutVarint32(&contents, std::numeric_limits<uint32_t>::max());
  PutVarint32(&contents, 2);
  contents.push_back('x');
  PutFixed32(&contents, 0);
  PutFixed32(&contents, 1);
  BlockContents block_contents;
  block_contents.data = contents;
  block_contents.cachable = false;
  block_contents.heap_allocated = false;
  Block block(block_contents);
  std::unique_ptr<Iterator> it(block.NewIterator(BytewiseComparator()));
  it->SeekToFirst();
  EXPECT_FALSE(it->Valid());
  EXPECT_TRUE(it->status().IsCorruption());
}

TEST(BlockTest, IteratorReconstructsKeysAcrossRestartAndLongKeyTransitions) {
  Options options;
  options.block_restart_interval = 2;
  BlockBuilder builder(&options);
  const std::vector<std::string> keys = {
      "a", "aa", std::string(48, 'b') + "0", std::string(48, 'b') + "1",
      "c", "ca", "cb"};
  for (const auto& key : keys) builder.Add(key, key);
  const std::string contents = builder.Finish().ToString();
  BlockContents block_contents;
  block_contents.data = contents;
  block_contents.cachable = false;
  block_contents.heap_allocated = false;
  Block block(block_contents);
  std::unique_ptr<Iterator> it(block.NewIterator(BytewiseComparator()));
  it->SeekToFirst();
  for (const auto& key : keys) {
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(key, it->key().ToString());
    EXPECT_EQ(key, it->value().ToString());
    it->Next();
  }
  EXPECT_FALSE(it->Valid());
  it->SeekToLast();
  for (auto key = keys.rbegin(); key != keys.rend(); ++key) {
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(*key, it->key().ToString());
    it->Prev();
  }
  EXPECT_FALSE(it->Valid());
  for (const auto& key : keys) {
    it->Seek(key);
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(key, it->key().ToString());
  }
  EXPECT_TRUE(it->status().ok());
}

TEST(MemTableTest, Simple) {
  InternalKeyComparator cmp(BytewiseComparator());
  MemTable* memtable = new MemTable(cmp);
  memtable->Ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  batch.Put(std::string("k1"), std::string("v1"));
  batch.Put(std::string("k2"), std::string("v2"));
  batch.Put(std::string("k3"), std::string("v3"));
  batch.Put(std::string("largekey"), std::string("vlarge"));
  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  Iterator* iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }

  delete iter;
  memtable->Unref();
}

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    std::fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
                 (unsigned long long)(val), (unsigned long long)(low),
                 (unsigned long long)(high));
  }
  return result;
}

TEST(TableTest, ApproximateOffsetOfPlain) {
  TableConstructor c(BytewiseComparator());
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  KVMap kvmap;
  Options options;
  options.block_size = 1024;
  options.compression = kNoCompression;
  c.Finish(options, &keys, &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01a"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 10000, 11000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04a"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k05"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k06"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k07"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 610000, 612000));
}

static bool CompressionSupported(CompressionType type) {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  if (type == kSnappyCompression) {
    return port::Snappy_Compress(in.data(), in.size(), &out);
  } else if (type == kZstdCompression) {
    return port::Zstd_Compress(/*level=*/1, in.data(), in.size(), &out);
  }
  return false;
}

class CompressionTableTest
    : public ::testing::TestWithParam<std::tuple<CompressionType>> {};

INSTANTIATE_TEST_SUITE_P(CompressionTests, CompressionTableTest,
                         ::testing::Values(kSnappyCompression,
                                           kZstdCompression));

TEST_P(CompressionTableTest, ApproximateOffsetOfCompressed) {
  CompressionType type = ::testing::get<0>(GetParam());
  if (!CompressionSupported(type)) {
    GTEST_SKIP() << "skipping compression test: " << type;
  }

  Random rnd(301);
  TableConstructor c(BytewiseComparator());
  std::string tmp;
  c.Add("k01", "hello");
  c.Add("k02", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  c.Add("k03", "hello3");
  c.Add("k04", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  std::vector<std::string> keys;
  KVMap kvmap;
  Options options;
  options.block_size = 1024;
  options.compression = type;
  c.Finish(options, &keys, &kvmap);

  // Expected upper and lower bounds of space used by compressible strings.
  static const int kSlop = 1000;  // Compressor effectiveness varies.
  const int expected = 2500;      // 10000 * compression ratio (0.25)
  const int min_z = expected - kSlop;
  const int max_z = expected + kSlop;

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, kSlop));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, kSlop));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, kSlop));
  // Have now emitted a large compressible string, so adjust expected offset.
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), min_z, max_z));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), min_z, max_z));
  // Have now emitted two large compressible strings, so adjust expected offset.
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 2 * min_z, 2 * max_z));
}

}  // namespace leveldb
