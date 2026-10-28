#ifndef STORAGE_LEVELDB_UTIL_PROFILER_H_
#define STORAGE_LEVELDB_UTIL_PROFILER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/profiler.h"

#include "port/port.h"

namespace leveldb {
// Each scope lives on the calling thread's stack. Restoring the previous scope
// also supports callbacks that access another DB on the same thread.
class ProfileScope {
 public:
  explicit ProfileScope(const char* name, bool task = false,
                        const char* trigger = "", int level = -1);
  ProfileScope(const std::shared_ptr<Profiler>& profiler, uint64_t db_id,
               const char* name, bool task = false, const char* trigger = "",
               int level = -1, bool suppress_nested = false);
  ~ProfileScope();
  ProfileScope(const ProfileScope&) = delete;
  ProfileScope& operator=(const ProfileScope&) = delete;
  void SetStatus(const Status& status);
  Status Result(const Status& status) {
    SetStatus(status);
    return status;
  }
  bool active() const { return profiler_ != nullptr; }
  static ProfileScope* Current(uint64_t db_id = 0);
  static void Count(const std::string& name, uint64_t value = 1);
  static void Count(const char* name, uint64_t value = 1) {
    if (Current()) Count(std::string(name), value);
  }
  static void Duration(const char* name, uint64_t elapsed, bool sampled);
  static uint64_t Now();
  static uint64_t CurrentTaskID();
  void SetParentTaskID(uint64_t id) { parent_job_id_ = id; }
  bool sampled() const { return sampled_; }

 private:
  void Begin(const char* name, bool task, const char* trigger, int level);
  std::shared_ptr<Profiler> profiler_;
  uint64_t db_id_ = 0;
  ProfileScope* previous_ = nullptr;
  const char* name_ = "";
  const char* trigger_ = "";
  const char* status_ = "ok";
  std::string failure_stage_;
  bool sampled_ = false;
  bool task_ = false;
  bool restore_inactive_ = false;
  int level_ = -1;
  uint64_t start_ = 0;
  uint64_t job_id_ = 0;
  uint64_t parent_job_id_ = 0;
  std::map<std::string, uint64_t> counters_;
  std::map<std::string, std::pair<uint64_t, uint64_t>> stages_;
};

template <class F>
Status ProfileStatus(const char* name, F&& f) {
  ProfileScope scope(name);
  return scope.Result(f());
}

uint64_t NewProfileDatabaseID();
std::unique_ptr<Env> NewProfilingEnv(Env* target, uint64_t db_id);

class DBProfileMutexObserver : public port::MutexObserver {
 public:
  explicit DBProfileMutexObserver(uint64_t db_id) : db_id_(db_id) {}
  uint64_t BeforeLock() override;
  void AfterLock(uint64_t ticket) override;
  void BeforeUnlock() override;
  uint64_t BeforeWait() override;
  void AfterWait(uint64_t ticket) override;

 private:
  uint64_t db_id_;
};

// Internal-only test clock; never replaces the Env clock used by TTL.
class ProfilerTestPeer {
 public:
  static void SetClock(Profiler* profiler, uint64_t (*clock)());
  static uint64_t ClockReads(const Profiler& profiler);
  static uint64_t Counter(const Profiler& profiler, const std::string& name);
  static uint64_t MetricField(const Profiler& profiler, const std::string& name,
                              const std::string& field);
  static uint64_t Inflight(const Profiler& profiler);
};
}  // namespace leveldb
#endif
