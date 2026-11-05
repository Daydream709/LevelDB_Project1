// Diagnostic microbenchmark, separate from the unmodified official suite.
// Build against this checkout's Release libleveldb.a; see SST_LOCAL_COUNTS.md.
#include "db/dbformat.h"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

#include "table/local_count_index.h"
#include "util/profiler.h"
namespace leveldb {
class Sink : public WritableFile {
 public:
  std::string bytes;
  Status Append(const Slice& s) override {
    bytes.append(s.data(), s.size());
    return Status::OK();
  }
  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }
};
class Source : public RandomAccessFile {
 public:
  explicit Source(const std::string& s) : bytes(s) {}
  Status Read(uint64_t offset, size_t size, Slice* out,
              char* scratch) const override {
    ++reads;
    if (offset > bytes.size() || size > bytes.size() - offset)
      return Status::Corruption("probe bounds");
    std::memcpy(scratch, bytes.data() + offset, size);
    *out = Slice(scratch, size);
    return Status::OK();
  }
  const std::string& bytes;
  mutable uint64_t reads = 0;
};
void Check(const Status& s) {
  if (!s.ok()) {
    std::cerr << s.ToString() << '\n';
    std::exit(1);
  }
}
std::string Key(uint64_t n) {
  char key[32];
  std::snprintf(key, sizeof(key), "k%012llu", (unsigned long long)n);
  return key;
}
uint64_t Micros(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}
void Measure(uint64_t keys, uint64_t versions, bool indexed, size_t budget) {
  InternalKeyComparator internal(BytewiseComparator());
  Options options;
  options.comparator = &internal;
  options.metadata_cache_size = budget * 2;
  std::unique_ptr<Cache> primary_cache(NewLRUCache(budget));
  options.block_cache = primary_cache.get();
  options.compression = kNoCompression;
  Sink sink;
  auto start = std::chrono::steady_clock::now();
  {
    TableBuilder builder(options, &sink);
    builder.EnableInternalStatsMetadata();
    if (indexed) LocalCountTableAccess::Enable(&builder, "unused-bytewise");
    for (uint64_t i = 0; i < keys; ++i)
      for (uint64_t v = versions; v > 0; --v)
        builder.Add(InternalKey(Key(i), v, kTypeValue).Encode(),
                    std::string(64, 'v'));
    Check(builder.Finish());
  }
  auto build_us = Micros(start);
  Source source(sink.bytes);
  Table* raw = nullptr;
  Check(Table::Open(options, &source, sink.bytes.size(), &raw));
  std::unique_ptr<Table> table(raw);
  std::unique_ptr<Cache> cache(NewLRUCache(budget));
  auto profiler = std::make_shared<Profiler>();
  for (int pass = 0; pass < 3; ++pass) {
    profiler->SetEnabled(pass == 2);
    uint64_t before = source.reads;
    start = std::chrono::steady_clock::now();
    {
      ProfileScope root(profiler, 1, "probe.lookup");
      for (uint64_t q = 0; q < 4000; ++q) {
        std::string key = Key((q * 7919) % keys);
        uint64_t count = 0;
        if (indexed)
          Check(LocalCountTableAccess::Lookup(table.get(), cache.get(), key,
                                              &count));
        else {
          ReadOptions read;
          read.verify_checksums = true;
          std::unique_ptr<Iterator> iter(table->NewIterator(read));
          iter->Seek(
              InternalKey(key, kMaxSequenceNumber, kValueTypeForSeek).Encode());
          while (iter->Valid()) {
            ParsedInternalKey parsed;
            if (!ParseInternalKey(iter->key(), &parsed)) std::exit(2);
            if (parsed.user_key != Slice(key)) break;
            ++count;
            iter->Next();
          }
          Check(iter->status());
        }
        if (count != versions) std::exit(3);
      }
    }
    std::cout << "{\"keys\":" << keys << ",\"versions\":" << versions
              << ",\"indexed\":" << indexed << ",\"pass\":" << pass
              << ",\"sst_bytes\":" << sink.bytes.size()
              << ",\"build_us\":" << build_us
              << ",\"queries\":4000,\"lookup_us\":" << Micros(start)
              << ",\"block_reads\":" << source.reads - before
              << ",\"cache_charge\":" << cache->TotalCharge()
              << ",\"cache_budget\":" << budget << "}\n";
  }
  if (indexed) std::cout << profiler->SnapshotJSON() << '\n';
}
}  // namespace leveldb
int main() {
  for (int repeat = 0; repeat < 5; ++repeat)
    for (const auto& shape :
         {std::pair<uint64_t, uint64_t>{10000, 8}, {100000, 1}})
      for (bool indexed : {false, true})
        leveldb::Measure(shape.first, shape.second, indexed, 4 << 20);
}
