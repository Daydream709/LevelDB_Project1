#ifndef STORAGE_LEVELDB_INCLUDE_PROFILER_H_
#define STORAGE_LEVELDB_INCLUDE_PROFILER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {
struct ProfilerOptions {
  // Only foreground timing is sampled. Counts and background tasks are exact.
  uint32_t foreground_sample_interval = 1;
  size_t max_task_events = 4096;
};

class ProfileScope;
class ProfilerTestPeer;

// A controller can be shared by multiple databases and retained after Open
// fails. Disabled by default. Toggle affects new operations, not in-flight
// work.
class LEVELDB_EXPORT Profiler {
 public:
  explicit Profiler(const ProfilerOptions& options = ProfilerOptions());
  ~Profiler();
  Profiler(const Profiler&) = delete;
  Profiler& operator=(const Profiler&) = delete;

  void SetEnabled(bool enabled);
  bool IsEnabled() const;
  // Cumulative, inclusive timings in nanoseconds. Running snapshots can contain
  // in-flight scopes; compare exact totals only after work becomes quiescent.
  std::string SnapshotJSON() const;
  // Non-destructive export. Does not acquire a DB lock. Caller chooses an
  // output directory outside the DB directory. Errors do not change database
  // status.
  Status Export(const std::string& directory) const;

 private:
  friend class ProfileScope;
  friend class ProfilerTestPeer;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace leveldb
#endif
