#pragma once
#include "common/support.h"
#include "metadata/cases.h"

namespace hw {
inline void CombinedCheckPinnedIterator(
    Iterator* it, const std::map<std::string, std::string>& expected) {
  auto forward = expected.begin();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    Check(forward != expected.end(), "unexpected pinned iterator entry");
    Check(it->key().ToString() == forward->first &&
              it->value().ToString() == forward->second,
          "pinned iterator forward view changed");
    ++forward;
  }
  Ok(it->status());
  Check(forward == expected.end(), "missing pinned iterator entry");
  auto reverse = expected.rbegin();
  for (it->SeekToLast(); it->Valid(); it->Prev()) {
    Check(reverse != expected.rend(), "unexpected reverse pinned entry");
    Check(it->key().ToString() == reverse->first &&
              it->value().ToString() == reverse->second,
          "pinned iterator reverse view changed");
    ++reverse;
  }
  Ok(it->status());
  Check(reverse == expected.rend(), "missing reverse pinned entry");
  for (const auto& item : expected) {
    it->Seek(item.first);
    Check(it->Valid() && it->key().ToString() == item.first &&
              it->value().ToString() == item.second,
          "pinned iterator Seek changed");
  }
  Ok(it->status());
}

inline void AddCombined(Cases& cases) {
  cases["combined_contract"] = [](const Config& c) {
    DBState d(c);
    StatsEqual(ReadStats(d.db.get()), 0, 0, 0);
    Ok(d.db->Put(WriteOptions(), "permanent", "plain"));
    Ok(d.db->Put(WriteOptions(), "short", "ttl", 1));
    Ok(d.db->Put(WriteOptions(), "zero", "expired", 0));
    StatsEqual(ReadStats(d.db.get()), 3, 3, 3);
    Expect(d.db.get(), "zero", nullptr);
    d.env.now = kEpoch + kSecond - 1;
    ExpectValue(d.db.get(), "short", "ttl");
    d.env.now = kEpoch + kSecond;
    Expect(d.db.get(), "short", nullptr);
    ExpectValue(d.db.get(), "permanent", "plain");
    StatsEqual(ReadStats(d.db.get()), 3, 3, 3);
    PhysicalEqual(d, 3);
    Check(d.db->Put(WriteOptions(), "short", "overflow",
                   std::numeric_limits<uint64_t>::max()).IsInvalidArgument(),
          "TTL overflow was accepted");
    StatsEqual(ReadStats(d.db.get()), 3, 3, 3);
    Expect(d.db.get(), "short", nullptr);
  };

  cases["combined_overwrites"] = [](const Config& c) {
    DBState d(c);
    Ok(d.db->Put(WriteOptions(), "a", "old"));
    Ok(d.db->Put(WriteOptions(), "a", "temporary", 1));
    Ok(d.db->Put(WriteOptions(), "b", "temporary", 1));
    WriteBatch batch;
    batch.Put("b", "permanent");
    batch.Put("c", "deleted");
    batch.Delete("c");
    batch.Delete("d");
    Ok(d.db->Write(WriteOptions(), &batch));
    StatsEqual(ReadStats(d.db.get()), 7, 4, 7);
    d.env.now += kSecond;
    Expect(d.db.get(), "a", nullptr);
    ExpectValue(d.db.get(), "b", "permanent");
    Expect(d.db.get(), "c", nullptr);
    Expect(d.db.get(), "d", nullptr);
    StatsEqual(ReadStats(d.db.get()), 7, 4, 7);
    Ok(d.db->Put(WriteOptions(), "a", "revived"));
    Ok(d.db->Put(WriteOptions(), "a", "immediately-expired", 0));
    Expect(d.db.get(), "a", nullptr);
    StatsEqual(ReadStats(d.db.get()), 9, 4, 9);
    d.FullGC();
    StatsEqual(ReadStats(d.db.get()), 1, 1, 9);
    PhysicalEqual(d, 9);
    CheckView(d.db.get(), {{"b", "permanent"}});
  };

  cases["combined_views"] = [](const Config& c) {
    DBState d(c);
    Ok(d.db->Put(WriteOptions(), "a", "ttl", 10));
    Ok(d.db->Put(WriteOptions(), "b", "plain"));
    d.Flush();
    SnapshotGuard snapshot(d.db.get());
    std::unique_ptr<Iterator> pinned(d.db->NewIterator(ReadOptions()));
    const std::map<std::string, std::string> original{{"a", "ttl"}, {"b", "plain"}};
    d.env.now += 10 * kSecond;
    CheckView(d.db.get(), {{"b", "plain"}});
    CheckView(d.db.get(), original, snapshot.snapshot);
    StatsEqual(ReadStats(d.db.get(), snapshot.snapshot), 2, 2, 2);
    d.FullGC();
    // A snapshot may protect records in the current Version or pin an older
    // Version. Its reads must survive either strategy; stats count current data.
    PhysicalEqual(d, 2);
    ExpectValue(d.db.get(), "a", "ttl", snapshot.snapshot);
    CombinedCheckPinnedIterator(pinned.get(), original);
    snapshot.Release();
    d.FullGC();
    // An iterator may retain an obsolete Version. Only the installed Version
    // belongs to the current statistics, while its old read view remains valid.
    PhysicalEqual(d, 2);
    Expect(d.db.get(), "a", nullptr);
    CombinedCheckPinnedIterator(pinned.get(), original);
    pinned.reset();
    d.FullGC();
    StatsEqual(ReadStats(d.db.get()), 1, 1, 2);
    PhysicalEqual(d, 2);
  };

  cases["combined_reopen"] = [](const Config& c) {
    for (bool reuse : {false, true}) {
      DBState d(c);
      d.options.reuse_logs = reuse;
      d.Reopen();
      WriteOptions synced;
      synced.sync = true;
      Ok(d.db->Put(synced, "short", "ttl", 2));
      Ok(d.db->Put(synced, "keep", "plain"));
      WriteBatch batch;
      batch.Put("batch", "plain");
      batch.Delete("gone");
      Ok(d.db->Write(synced, &batch));
      StatsEqual(ReadStats(d.db.get()), 4, 4, 4);
      d.env.now += kSecond;
      d.Reopen();  // Recover the WAL, with both log-reuse settings.
      ExpectValue(d.db.get(), "short", "ttl");
      PhysicalEqual(d, 4);
      d.env.now += kSecond;
      Expect(d.db.get(), "short", nullptr);
      PhysicalEqual(d, 4);
      Ok(d.db->Put(synced, "later", "sst-ttl", 2));
      d.Flush();
      d.Reopen();  // Recover flushed SSTs and retain their absolute deadlines.
      ExpectValue(d.db.get(), "later", "sst-ttl");
      d.env.now += 2 * kSecond;
      Expect(d.db.get(), "short", nullptr);
      Expect(d.db.get(), "later", nullptr);
      ExpectValue(d.db.get(), "keep", "plain");
      ExpectValue(d.db.get(), "batch", "plain");
      PhysicalEqual(d, 5);
      d.FullGC();
      StatsEqual(ReadStats(d.db.get()), 2, 2, 5);
      d.Reopen();
      StatsEqual(ReadStats(d.db.get()), 2, 2, 5);
      PhysicalEqual(d, 5);
    }
  };

  cases["combined_gc"] = [](const Config& c) {
    DBState d(c);
    const size_t count = std::max<size_t>(8, std::min<size_t>(c.keys, 256));
    const size_t expired = count / 2;
    for (size_t i = 0; i < count; ++i)
      Ok(d.db->Put(WriteOptions(), Key(i), Value(i, c.value_bytes),
                   i < expired ? 1 : 3600));
    Ok(d.db->Put(WriteOptions(), "permanent", "plain"));
    d.Compact();  // Place positive-TTL records in deep SSTs before expiry.
    StatsEqual(ReadStats(d.db.get()), count + 1, count + 1, count + 1);
    d.env.now += kSecond;
    StatsEqual(ReadStats(d.db.get()), count + 1, count + 1, count + 1);
    d.FullGC();
    StatsEqual(ReadStats(d.db.get()), count - expired + 1,
               count - expired + 1, count + 1);
    PhysicalEqual(d, count + 1);
    for (size_t i = 0; i < count; ++i) {
      if (i < expired) Expect(d.db.get(), Key(i), nullptr);
      else ExpectValue(d.db.get(), Key(i), Value(i, c.value_bytes));
    }
    d.Reopen();
    StatsEqual(ReadStats(d.db.get()), count - expired + 1,
               count - expired + 1, count + 1);
    PhysicalEqual(d, count + 1);
  };

  cases["combined_concurrent"] = [](const Config& c) {
    DBState d(c);
    // Distinct positive-TTL records cannot be collected at this frozen time.
    // The second phase publishes pairs of fresh ordinary keys atomically.
    for (bool batch_phase : {false, true}) {
      std::atomic<size_t> done{0};
      Threads writers;
      for (size_t thread = 0; thread < c.threads; ++thread) {
        writers.Add([&, thread] {
          try {
            for (size_t i = thread; i < c.keys; i += c.threads) {
              if (!batch_phase) {
                Ok(d.db->Put(WriteOptions(), Key(3 * i), Value(i, c.value_bytes), 1));
              } else {
                WriteBatch batch;
                batch.Put(Key(3 * i + 1), Value(i, c.value_bytes));
                batch.Put(Key(3 * i + 2), Value(c.keys + i, c.value_bytes));
                Ok(d.db->Write(WriteOptions(), &batch));
              }
            }
          } catch (...) { ++done; throw; }
          ++done;
        });
      }
      const uint64_t initial = batch_phase ? c.keys : 0;
      const uint64_t expected = batch_phase ? 3 * c.keys : c.keys;
      uint64_t last = initial;
      do {
        Stats stats = ReadStats(d.db.get());
        Check(stats.total_entries == stats.seq && stats.distinct_keys == stats.seq &&
                  stats.seq >= last && stats.seq <= expected,
              "TTL/batch writes published inconsistent statistics");
        if (batch_phase)
          Check((stats.seq - initial) % 2 == 0,
                "statistics exposed only part of a two-key batch");
        last = stats.seq;
        std::this_thread::yield();
      } while (done.load() != c.threads);
      writers.Join();
      d.env.Wait();
      StatsEqual(ReadStats(d.db.get()), expected, expected, expected);
    }
    PhysicalEqual(d, 3 * c.keys);
    d.env.now += kSecond;
    d.FullGC();
    StatsEqual(ReadStats(d.db.get()), 2 * c.keys, 2 * c.keys, 3 * c.keys);
    PhysicalEqual(d, 3 * c.keys);
    for (size_t i = 0; i < c.keys; ++i) {
      Expect(d.db.get(), Key(3 * i), nullptr);
      ExpectValue(d.db.get(), Key(3 * i + 1), Value(i, c.value_bytes));
      ExpectValue(d.db.get(), Key(3 * i + 2), Value(c.keys + i, c.value_bytes));
    }
  };

  cases["perf_combined_stats_under_write"] = [](const Config& c) {
    const size_t expired = c.keys * c.expired_percent / 100;
    for (bool query : {bool(c.seed % 2), !bool(c.seed % 2)}) {
      DBState d(c);
      for (size_t i = 0; i < c.keys; ++i)
        Ok(d.db->Put(WriteOptions(), Key(i), Value(i, c.value_bytes),
                     i < expired ? 1 : 3600));
      d.Flush();
      std::atomic<size_t> done{0};
      Threads writers;
      Measure writes(d, query ? "ttl_put_with_stats_queries" : "ttl_put_without_stats_queries");
      for (size_t thread = 0; thread < c.threads; ++thread) {
        writers.Add([&, thread] {
          try {
            for (size_t i = thread; i < c.keys; i += c.threads)
              Ok(d.db->Put(WriteOptions(), Key(c.keys + i), Value(i, c.value_bytes),
                           i < expired ? 1 : 3600));
          } catch (...) { ++done; throw; }
          ++done;
        });
      }
      Measure queries(d, "stats_under_ttl_write");
      size_t queried = 0;
      uint64_t last = c.keys;
      if (query) {
        do {
          auto start = Clock::now();
          Stats stats = ReadStats(d.db.get());
          queries.Sample(queried++, start);
          Check(stats.total_entries == stats.distinct_keys && stats.seq == stats.total_entries &&
                    stats.seq >= last, "statistics under TTL writes are incoherent");
          last = stats.seq;
        } while (done.load() != c.threads);
      }
      writers.Join();
      const double write_seconds = writes.Elapsed(), query_seconds = queries.Elapsed();
      writes.Finish(c.keys, write_seconds);
      if (query) queries.Finish(queried, query_seconds);
      StatsEqual(ReadStats(d.db.get()), 2 * c.keys, 2 * c.keys, 2 * c.keys);
      d.env.now += kSecond;
      StatsEqual(ReadStats(d.db.get()), 2 * c.keys, 2 * c.keys, 2 * c.keys);
      d.FullGC();
      StatsEqual(ReadStats(d.db.get()), 2 * (c.keys - expired),
                 2 * (c.keys - expired), 2 * c.keys);
      PhysicalEqual(d, 2 * c.keys);
    }
  };

  cases["perf_combined_gc"] = [](const Config& c) {
    DBState d(c);
    const size_t expired = c.keys * c.expired_percent / 100;
    for (size_t i = 0; i < c.keys; ++i)
      Ok(d.db->Put(WriteOptions(), Key(i), Value(i, c.value_bytes),
                   i < expired ? 1 : 3600));
    d.Compact();
    StatsEqual(ReadStats(d.db.get()), c.keys, c.keys, c.keys);
    d.env.now += kSecond;
    StatsEqual(ReadStats(d.db.get()), c.keys, c.keys, c.keys);
    Measure gc(d, "ttl_gc_with_statistics");
    auto start = Clock::now();
    d.FullGC();
    gc.Sample(0, start);
    gc.Finish(c.keys);
    StatsEqual(ReadStats(d.db.get()), c.keys - expired, c.keys - expired, c.keys);
    PhysicalEqual(d, c.keys);
    for (size_t i = 0; i < c.keys; ++i) {
      if (i < expired) Expect(d.db.get(), Key(i), nullptr);
      else ExpectValue(d.db.get(), Key(i), Value(i, c.value_bytes));
    }
  };
}
}  // namespace hw
