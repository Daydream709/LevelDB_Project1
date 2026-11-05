// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_METADATA_PROFILE_H_
#define STORAGE_LEVELDB_DB_METADATA_PROFILE_H_

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>

namespace leveldb {

enum class MetadataProfileCounter : size_t {
  kStatsQueries,
  kMetadataLookups,
  kActiveHits,
  kImmutableHits,
  kValueCacheHits,
  kMetadataFilesProbed,
  kBatchTableGets,
  kBatchDataBlockProbes,
  kTableCacheHits,
  kTableCacheMisses,
  kBlockCacheHits,
  kBlockCacheMisses,
  kWriteBatches,
  kWriteOperations,
  kWriteDistinctKeys,
  kBatchSpills,
  kBatchSpillBytes,
  kBatchMergePasses,
  kChangeFileBytes,
  kWalRecords,
  kWalPayloadBytes,
  kWalMetadataBytes,
  kWalSyncs,
  kWalSyncSkipped,
  kCheckpointTriggerMemtable,
  kCheckpointTriggerWalBudget,
  kCheckpointTriggerChangeBudget,
  kCheckpointTriggerChangeFiles,
  kCheckpointTriggerInterval,
  kCheckpointTriggerWaiter,
  kBackpressureWaits,
  kMetadataFlushes,
  kMetadataFlushBytes,
  kMetadataCompactions,
  kMetadataCompactionInputBytes,
  kMetadataCompactionOutputBytes,
  kPrimaryCompactions,
  kCompactionJournalBytes,
  kCompactionJournalFiles,
  kCompactionTailKeys,
  kCompactionTailBytes,
  kRebuilds,
  kRebuiltRecords,
  kWriterSpins,
  kWriterSpinReady,
  kWriterParks,
  kWriterWakeups,
  kWriteGroupSize1,
  kWriteGroupSize2,
  kWriteGroupSize3,
  kWriteGroupSize4,
  kWriteGroupSize5,
  kWriteGroupSize6,
  kWriteGroupSize7,
  kWriteGroupSize8Plus,
  kCount
};

enum class MetadataProfileStage : size_t {
  kStatsQuery,
  kLookup,
  kWritePrepare,
  kWalAppend,
  kWalSync,
  kMetadataFlush,
  kMetadataCompaction,
  kPrimaryCompactionCommit,
  kPrimaryCompactionPrepare,
  kPrimaryCompactionTail,
  kPrimaryCompactionGate,
  kWriteQueueWait,
  kWalIoWait,
  kRecovery,
  kRebuild,
  kBackpressure,
  kMemtableInsert,
  kCount
};

// Optional, bounded-memory profiling for the metadata statistics path.
// Exact counters and sampled durations use atomics so the observer can run
// concurrently with foreground and background database activity.
class MetadataProfile {
 public:
  static constexpr size_t kHistogramBuckets = 32;

  class Timer {
   public:
    Timer(MetadataProfile* profile, MetadataProfileStage stage,
          bool sample_every_event = false)
        : profile_(profile), stage_(stage),
          start_ns_(profile == nullptr
                        ? 0
                        : profile->Start(stage, sample_every_event)) {}
    Timer(Timer&& other) noexcept
        : profile_(other.profile_), stage_(other.stage_),
          start_ns_(other.start_ns_) {
      other.profile_ = nullptr;
    }
    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;
    ~Timer() { Stop(); }
    void Stop() {
      if (profile_ != nullptr) {
        profile_->Finish(stage_, start_ns_);
        profile_ = nullptr;
      }
    }

   private:
    MetadataProfile* profile_;
    MetadataProfileStage stage_;
    uint64_t start_ns_;
  };

  MetadataProfile(bool enabled, size_t sample_rate)
      : enabled_(enabled), sample_rate_(std::max<size_t>(1, sample_rate)) {
    for (auto& counter : counters_) counter.store(0);
    for (auto& stage : stages_) {
      stage.events.store(0);
      stage.samples.store(0);
      stage.total_ns.store(0);
      stage.max_ns.store(0);
      for (auto& bucket : stage.histogram) bucket.store(0);
    }
  }

  bool enabled() const { return enabled_; }
  size_t sample_rate() const { return sample_rate_; }
  Timer Measure(MetadataProfileStage stage, bool sample_every_event = false) {
    return Timer(this, stage, sample_every_event);
  }

  void Count(MetadataProfileCounter counter, uint64_t amount = 1) {
    if (enabled_) counters_[static_cast<size_t>(counter)].fetch_add(
        amount, std::memory_order_relaxed);
  }

  uint64_t CounterValue(MetadataProfileCounter counter) const {
    return counters_[static_cast<size_t>(counter)].load(
        std::memory_order_relaxed);
  }

  uint64_t StageEvents(MetadataProfileStage stage) const {
    return stages_[static_cast<size_t>(stage)].events.load(
        std::memory_order_relaxed);
  }
  uint64_t StageSamples(MetadataProfileStage stage) const {
    return stages_[static_cast<size_t>(stage)].samples.load(
        std::memory_order_relaxed);
  }
  uint64_t StageTotalNanoseconds(MetadataProfileStage stage) const {
    return stages_[static_cast<size_t>(stage)].total_ns.load(
        std::memory_order_relaxed);
  }
  uint64_t StageMaximumNanoseconds(MetadataProfileStage stage) const {
    return stages_[static_cast<size_t>(stage)].max_ns.load(
        std::memory_order_relaxed);
  }
  uint64_t StageBucket(MetadataProfileStage stage, size_t bucket) const {
    return stages_[static_cast<size_t>(stage)].histogram[bucket].load(
        std::memory_order_relaxed);
  }

  static const char* CounterName(MetadataProfileCounter counter) {
    static const char* const names[] = {
        "stats_queries", "metadata_lookups", "active_hits",
        "immutable_hits", "value_cache_hits", "metadata_files_probed",
        "batch_table_gets", "batch_data_block_probes",
        "table_cache_hits", "table_cache_misses", "block_cache_hits",
        "block_cache_misses", "write_batches", "write_operations",
        "write_distinct_keys", "batch_spills", "batch_spill_bytes",
        "batch_merge_passes", "change_file_bytes", "wal_records",
        "wal_payload_bytes", "wal_metadata_bytes", "wal_syncs",
        "wal_sync_skipped", "checkpoint_trigger_memtable",
        "checkpoint_trigger_wal_budget", "checkpoint_trigger_change_budget",
        "checkpoint_trigger_change_files", "checkpoint_trigger_interval",
        "checkpoint_trigger_waiter",
        "backpressure_waits", "metadata_flushes", "metadata_flush_bytes",
        "metadata_compactions", "metadata_compaction_input_bytes",
        "metadata_compaction_output_bytes", "primary_compactions",
        "compaction_journal_bytes", "compaction_journal_files",
        "compaction_tail_keys", "compaction_tail_bytes",
        "rebuilds", "rebuilt_records", "writer_spins", "writer_spin_ready",
        "writer_parks", "writer_wakeups", "write_group_size_1",
        "write_group_size_2", "write_group_size_3", "write_group_size_4",
        "write_group_size_5", "write_group_size_6", "write_group_size_7",
        "write_group_size_8_plus"};
    return names[static_cast<size_t>(counter)];
  }

  static const char* StageName(MetadataProfileStage stage) {
    static const char* const names[] = {
        "stats_query", "lookup", "write_prepare", "wal_append", "wal_sync",
        "metadata_flush", "metadata_compaction", "primary_compaction_commit",
        "primary_compaction_prepare", "primary_compaction_tail",
        "primary_compaction_gate", "write_queue_wait", "wal_io_wait",
        "recovery", "rebuild", "backpressure", "memtable_insert"};
    return names[static_cast<size_t>(stage)];
  }

 private:
  struct Stage {
    std::atomic<uint64_t> events{0};
    std::atomic<uint64_t> samples{0};
    std::atomic<uint64_t> total_ns{0};
    std::atomic<uint64_t> max_ns{0};
    std::array<std::atomic<uint64_t>, kHistogramBuckets> histogram{};
  };

  static uint64_t NowNanoseconds() {
    return static_cast<uint64_t>(std::chrono::duration_cast<
        std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
  }

  uint64_t Start(MetadataProfileStage stage, bool sample_every_event) {
    if (!enabled_) return 0;
    stages_[static_cast<size_t>(stage)].events.fetch_add(
        1, std::memory_order_relaxed);
    const uint64_t sample = sample_counter_.fetch_add(
        1, std::memory_order_relaxed);
    return sample_every_event || sample % sample_rate_ == 0
               ? NowNanoseconds()
               : 0;
  }

  void Finish(MetadataProfileStage stage, uint64_t start_ns) {
    if (start_ns == 0) return;
    const uint64_t elapsed = NowNanoseconds() - start_ns;
    Stage& record = stages_[static_cast<size_t>(stage)];
    record.samples.fetch_add(1, std::memory_order_relaxed);
    record.total_ns.fetch_add(elapsed, std::memory_order_relaxed);
    uint64_t old_max = record.max_ns.load(std::memory_order_relaxed);
    while (old_max < elapsed && !record.max_ns.compare_exchange_weak(
               old_max, elapsed, std::memory_order_relaxed)) {}
    size_t bucket = 0;
    for (uint64_t n = elapsed; n > 1 && bucket + 1 < kHistogramBuckets;
         n >>= 1) {
      ++bucket;
    }
    record.histogram[bucket].fetch_add(1, std::memory_order_relaxed);
  }

  void StartEvent(MetadataProfileStage stage) {
    if (enabled_) stages_[static_cast<size_t>(stage)].events.fetch_add(
        1, std::memory_order_relaxed);
  }

  const bool enabled_;
  const size_t sample_rate_;
  std::atomic<uint64_t> sample_counter_{0};
  std::array<std::atomic<uint64_t>,
             static_cast<size_t>(MetadataProfileCounter::kCount)> counters_{};
  std::array<Stage, static_cast<size_t>(MetadataProfileStage::kCount)> stages_{};
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_METADATA_PROFILE_H_
