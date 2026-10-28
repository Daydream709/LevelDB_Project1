#pragma once

#include "common/support.h"
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace hw {

inline std::string ResearchKey(size_t index, size_t bytes) {
  const std::string digits = std::to_string(index);
  const size_t width = std::max<size_t>(bytes, digits.size() + 2);
  return "k/" + std::string(width - digits.size() - 2, '0') + digits;
}

inline size_t PickResearchIndex(size_t op, size_t keyspace,
                                const std::string& distribution,
                                std::mt19937_64* rng) {
  keyspace = std::max<size_t>(1, keyspace);
  if (distribution == "sequential") return op % keyspace;
  if (distribution == "hotspot") {
    std::uniform_int_distribution<size_t> any(0, keyspace - 1);
    const size_t hot = std::max<size_t>(1, keyspace / 100);
    if ((*rng)() % 100 < 95) return (*rng)() % hot;
    return any(*rng);
  }
  if (distribution == "zipf") {
    const double x = static_cast<double>((*rng)() % 1000000) / 1000000.0;
    return std::min(keyspace - 1,
                    static_cast<size_t>(x * x * keyspace));
  }
  std::uniform_int_distribution<size_t> any(0, keyspace - 1);
  return any(*rng);
}

inline uint64_t ProfileCounterValue(const std::string& json,
                                   const std::string& name) {
  const std::string marker = "\"" + name + "\":";
  const size_t pos = json.find(marker);
  if (pos == std::string::npos) return 0;
  return std::strtoull(json.c_str() + pos + marker.size(), nullptr, 10);
}

inline void ResearchStats(DBState& d, uint64_t total, uint64_t distinct,
                          uint64_t seq) {
  Stats stats;
  Ok(d.db->GetCurrentStats(ReadOptions(), &stats));
  Check(stats.total_entries == total && stats.distinct_keys == distinct &&
            stats.seq == seq,
        "metadata research result disagrees with expected counters");
}

inline Stats ResearchReadStats(DB* db) {
  Stats stats;
  Ok(db->GetCurrentStats(ReadOptions(), &stats));
  return stats;
}

inline void ResearchPhysicalStats(DBState& d, uint64_t expected_seq) {
  d.WaitMetadata(true);
  const Physical physical = ScanPhysical(d);
  const Stats stats = ResearchReadStats(d.db.get());
  Check(stats.total_entries == physical.total &&
            stats.distinct_keys == physical.distinct &&
            stats.seq == expected_seq,
        "metadata statistics disagree with the quiescent primary-record scan");
}

inline WriteOptions ResearchWriteOptions(const Config& c) {
  WriteOptions options;
  options.sync = c.sync_writes;
  return options;
}

inline void CrashWriteFixture(const Config& c, const std::string& path) {
  const pid_t child = fork();
  Check(child >= 0, "fork failed for metadata recovery fixture");
  if (child == 0) {
    Options options;
    options.create_if_missing = true;
    options.metadata_workspace_size = 64 << 10;
    options.enable_metadata_profiling = true;
    options.metadata_profile_sample_rate = c.profile_sample_rate;
    DB* db = nullptr;
    if (!DB::Open(options, path, &db).ok()) _exit(2);
    WriteBatch batch;
    for (size_t i = 0; i < c.keys; ++i) {
      const std::string key = ResearchKey(i, c.key_bytes);
      batch.Put(key, Value(i, c.value_bytes));
    }
    WriteOptions write;
    write.sync = true;
    if (!db->Write(write, &batch).ok()) _exit(3);
    // Leave a complete WAL transaction and its external metadata change file
    // without running orderly DB shutdown or metadata checkpointing.
    _exit(0);
  }
  int status = 0;
  Check(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 0,
        "metadata recovery writer subprocess failed");
}

inline void AddMetadataResearch(Cases& cases) {
  cases["perf_metadata_lookup"] = [](const Config& c) {
    DBState d(c);
    for (size_t i = 0; i < c.keys; ++i)
      Ok(d.db->Put(WriteOptions(), ResearchKey(i, c.key_bytes),
                   Value(i, c.value_bytes)));
    d.WaitMetadata(false);
    const size_t keyspace = std::max<size_t>(
        1, c.keys * std::max<size_t>(1, c.distinct_percent) / 100);
    std::mt19937_64 rng(c.seed);
    Measure m(d, "metadata_write_lookup");
    for (size_t i = 0; i < c.keys; ++i) {
      const size_t key = PickResearchIndex(i, keyspace, c.distribution, &rng);
      const auto start = m.ShouldSample(i) ? Clock::now() : Clock::time_point{};
      Ok(d.db->Put(ResearchWriteOptions(c), ResearchKey(key, c.key_bytes),
                   Value(c.keys + i, c.value_bytes)));
      m.Sample(start);
    }
    m.Finish(c.keys);
    ResearchPhysicalStats(d, 2 * c.keys);
    if (c.enable_metadata_profiling) {
      Check(ProfileCounterValue(m.LastProfile(), "metadata_lookups") >= c.keys,
            "metadata lookup path was not profiled");
    }
  };

  cases["perf_metadata_batch"] = [](const Config& c) {
    DBState d(c);
    for (size_t i = 0; i < c.keys; ++i)
      Ok(d.db->Put(WriteOptions(), ResearchKey(i, c.key_bytes),
                   Value(i, c.value_bytes)));
    d.WaitMetadata(false);
    const size_t keyspace = std::max<size_t>(
        1, c.keys * std::max<size_t>(1, c.distinct_percent) / 100);
    std::mt19937_64 rng(c.seed);
    Measure m(d, "write_batch");
    uint64_t batches = 0;
    for (size_t start_op = 0; start_op < c.keys; start_op += c.batch_size) {
      WriteBatch batch;
      const size_t count = std::min(c.batch_size, c.keys - start_op);
      for (size_t j = 0; j < count; ++j) {
        const size_t key = PickResearchIndex(start_op + j, keyspace,
                                             c.distribution, &rng);
        batch.Put(ResearchKey(key, c.key_bytes),
                  Value(c.keys + start_op + j, c.value_bytes));
      }
      Ok(d.db->Write(ResearchWriteOptions(c), &batch));
      ++batches;
    }
    m.Finish(c.keys);
    ResearchPhysicalStats(d, 2 * c.keys);
    if (c.enable_metadata_profiling) {
      const uint64_t spills = ProfileCounterValue(m.LastProfile(), "batch_spills");
      if (c.keys >= 8192 && c.metadata_workspace_bytes <= (64 << 10))
        Check(spills >= 9, "large batch did not exercise spill and fan-in merges");
    }
    Check(batches > 0, "metadata batch workload executed no batches");
  };

  cases["perf_metadata_contention"] = [](const Config& c) {
    DBState d(c);
    for (size_t i = 0; i < c.keys; ++i)
      Ok(d.db->Put(WriteOptions(), ResearchKey(i, c.key_bytes),
                   Value(i, c.value_bytes)));
    d.WaitMetadata(false);
    std::atomic<bool> writing{true};
    Threads writers, readers;
    Measure write_measure(d, "writers_under_stats_queries");
    Measure query_measure(d, "stats_queries_under_writes");
    for (size_t t = 0; t < c.threads; ++t) {
      writers.Add([&, t] {
        std::vector<double> local;
        size_t sample = 0;
        for (size_t i = t; i < c.keys; i += c.threads) {
          const auto start = sample++ % 64 == 0 ? Clock::now() : Clock::time_point{};
          Ok(d.db->Put(ResearchWriteOptions(c), ResearchKey(i, c.key_bytes),
                       Value(c.keys + i, c.value_bytes)));
          if (start != Clock::time_point{}) local.push_back(Micros(start));
        }
        for (double value : local) write_measure.RecordLatency(value);
      });
    }
    std::atomic<uint64_t> queries{0};
    for (size_t t = 0; t < c.query_threads; ++t) {
      readers.Add([&] {
        std::vector<double> local;
        while (writing.load(std::memory_order_acquire)) {
          const auto start = queries.fetch_add(1) % 64 == 0
                                 ? Clock::now() : Clock::time_point{};
          Stats stats;
          Ok(d.db->GetCurrentStats(ReadOptions(), &stats));
          if (start != Clock::time_point{}) local.push_back(Micros(start));
          Check(stats.seq >= c.keys && stats.distinct_keys == c.keys &&
                    stats.total_entries >= stats.distinct_keys &&
                    stats.total_entries <= stats.seq,
                "concurrent metadata statistics are incoherent");
        }
        for (double value : local) query_measure.RecordLatency(value);
      });
    }
    writers.Join();
    writing.store(false, std::memory_order_release);
    readers.Join();
    const double elapsed = write_measure.Elapsed();
    write_measure.CaptureEnd(c.keys, elapsed);
    if (c.query_threads != 0) query_measure.CaptureEnd(queries.load(), elapsed);
    write_measure.Finish(c.keys, elapsed);
    if (c.query_threads != 0) query_measure.FinishAfterSharedDrain();
    ResearchPhysicalStats(d, 2 * c.keys);
  };

  cases["perf_metadata_steady"] = [](const Config& c) {
    DBState d(c);
    const size_t preload = std::min<size_t>(c.keys, 10000);
    for (size_t i = 0; i < preload; ++i)
      Ok(d.db->Put(WriteOptions(), ResearchKey(i, c.key_bytes),
                   Value(i, c.value_bytes)));
    d.WaitMetadata(false);
    std::mt19937_64 rng(c.seed);
    const auto warmup_end = Clock::now() +
        std::chrono::seconds(c.warmup_seconds);
    size_t warmup_op = 0;
    while (Clock::now() < warmup_end) {
      const size_t key = PickResearchIndex(warmup_op++, preload,
                                           c.distribution, &rng);
      Ok(d.db->Put(WriteOptions(), ResearchKey(key, c.key_bytes),
                   Value(warmup_op, c.value_bytes)));
    }
    std::atomic<bool> running{true};
    std::atomic<uint64_t> writes{0}, queries{0};
    Measure write_measure(d, "steady_metadata_writes");
    Measure query_measure(d, "steady_stats_queries");
    const auto began = Clock::now();
    Threads writers, readers;
    for (size_t t = 0; t < c.threads; ++t) {
      writers.Add([&, t] {
        std::mt19937_64 local_rng(c.seed + t + 1);
        std::vector<double> latency;
        while (running.load(std::memory_order_acquire)) {
          const uint64_t op = writes.fetch_add(1);
          const auto start = op % 64 == 0 ? Clock::now() : Clock::time_point{};
          const size_t key = PickResearchIndex(op, preload, c.distribution,
                                               &local_rng);
          Ok(d.db->Put(ResearchWriteOptions(c), ResearchKey(key, c.key_bytes),
                       Value(preload + op, c.value_bytes)));
          if (start != Clock::time_point{}) latency.push_back(Micros(start));
        }
        for (double value : latency) write_measure.RecordLatency(value);
      });
    }
    for (size_t t = 0; t < c.query_threads; ++t) {
      readers.Add([&] {
        std::vector<double> latency;
        while (running.load(std::memory_order_acquire)) {
          const uint64_t op = queries.fetch_add(1);
          const auto start = op % 64 == 0 ? Clock::now() : Clock::time_point{};
          Stats stats;
          Ok(d.db->GetCurrentStats(ReadOptions(), &stats));
          if (start != Clock::time_point{}) latency.push_back(Micros(start));
        }
        for (double value : latency) query_measure.RecordLatency(value);
      });
    }
    std::this_thread::sleep_until(began + std::chrono::seconds(c.duration_seconds));
    running.store(false, std::memory_order_release);
    writers.Join(); readers.Join();
    const double elapsed = std::chrono::duration<double>(Clock::now() - began).count();
    const uint64_t query_count = queries.load();
    write_measure.CaptureEnd(writes.load(), elapsed);
    if (c.query_threads) query_measure.CaptureEnd(query_count, elapsed);
    write_measure.Finish(writes.load(), elapsed);
    if (c.query_threads) query_measure.FinishAfterSharedDrain();
    const uint64_t expected_seq = preload + warmup_op + writes.load();
    Check(ResearchReadStats(d.db.get()).seq == expected_seq,
          "steady workload sequence number mismatch");
    ResearchPhysicalStats(d, expected_seq);
  };

  cases["perf_metadata_maintenance"] = [](const Config& c) {
    DBState d(c);
    Measure generation_measure(d, "create_metadata_generations_and_drain");
    for (size_t i = 0; i < c.keys; ++i) {
      const std::string key = ResearchKey(i, c.key_bytes);
      Ok(d.db->Put(WriteOptions(), key, Value(i, c.value_bytes)));
    }
    d.Flush();
    for (size_t i = 0; i < c.keys; ++i)
      Ok(d.db->Put(WriteOptions(), ResearchKey(i, c.key_bytes),
                   Value(c.keys + i, c.value_bytes)));
    d.WaitMetadata(true);
    generation_measure.CaptureEnd(2 * c.keys);
    generation_measure.Finish(2 * c.keys);
    const std::string profile = generation_measure.LastProfile();
    if (c.enable_metadata_profiling)
      Check(ProfileCounterValue(profile, "metadata_flushes") > 0,
            "metadata flush did not execute");

    Measure primary_measure(d, "primary_compaction_commit");
    d.Compact();
    d.WaitMetadata(true);
    primary_measure.CaptureEnd(c.keys);
    primary_measure.Finish(c.keys);
    if (c.enable_metadata_profiling)
      Check(ProfileCounterValue(primary_measure.LastProfile(), "primary_compactions") > 0,
            "primary compaction did not execute");
    ResearchPhysicalStats(d, 2 * c.keys);
  };

  cases["perf_metadata_recovery"] = [](const Config& c) {
    CrashWriteFixture(c, c.path);
    DBState recovered(c, false, kEpoch, 0, false);
    Measure recovery(recovered, "mchg_wal_recovery_open");
    const auto start = Clock::now();
    recovered.Open();
    recovery.Sample(start);
    recovery.Finish(1);
    ResearchStats(recovered, c.keys, c.keys, c.keys);

    const std::string rebuild_path = c.path + "/rebuild-case";
    CrashWriteFixture(c, rebuild_path);
    std::vector<std::string> children;
    Ok(recovered.env.GetChildren(rebuild_path, &children));
    size_t removed = 0;
    for (const std::string& file : children) {
      if (file.size() >= 5 && file.compare(file.size() - 5, 5, ".mchg") == 0) {
        Ok(recovered.env.RemoveFile(rebuild_path + "/" + file));
        ++removed;
      }
    }
    Check(removed > 0, "recovery fixture did not contain a .mchg file");
    Config rebuild_config = c; rebuild_config.path = rebuild_path;
    DBState rebuilt(rebuild_config, false, kEpoch, 0, false);
    Measure rebuild(rebuilt, "missing_mchg_rebuild_open");
    const auto rebuild_start = Clock::now();
    rebuilt.Open();
    rebuild.Sample(rebuild_start);
    rebuild.Finish(1);
    ResearchStats(rebuilt, c.keys, c.keys, c.keys);
  };
}

}  // namespace hw
