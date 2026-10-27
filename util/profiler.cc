#include "util/profiler.h"

#include "db/filename.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <locale>
#include <mutex>
#include <sstream>
#include <utility>

namespace leveldb {
namespace {
thread_local ProfileScope* current_scope = nullptr;
std::atomic<uint64_t> next_database{1}, next_session{1};
struct Metric {
  uint64_t calls = 0, completed = 0, ok = 0, not_found = 0, errors = 0;
  uint64_t samples = 0, total = 0, min = UINT64_MAX, max = 0;
  std::array<uint64_t, 65> buckets{};
  void Finish(const char* status, bool sampled, uint64_t ns) {
    ++completed;
    if (std::string(status) == "ok")
      ++ok;
    else if (std::string(status) == "not_found")
      ++not_found;
    else
      ++errors;
    if (!sampled) return;
    ++samples;
    total += ns;
    min = std::min(min, ns);
    max = std::max(max, ns);
    size_t bucket = 0;
    for (uint64_t n = ns; n != 0; n >>= 1) ++bucket;
    ++buckets[bucket];
  }
  uint64_t Percentile(uint64_t percent) const {
    if (!samples) return 0;
    const uint64_t target = (samples * percent + 99) / 100;
    uint64_t seen = 0;
    for (size_t i = 0; i < buckets.size(); ++i) {
      seen += buckets[i];
      if (seen >= target) return i == 64 ? UINT64_MAX : (uint64_t{1} << i) - 1;
    }
    return max;
  }
};
struct Data {
  std::map<std::string, Metric> metrics;
  std::map<std::string, uint64_t> counters;
};
struct Event {
  uint64_t id, db, parent, start, elapsed;
  std::string kind, trigger, status, failure_stage;
  int level;
  std::map<std::string, uint64_t> counters;
  std::map<std::string, std::pair<uint64_t, uint64_t>> stages;
};
struct Snapshot {
  uint64_t session, dropped, clock_reads;
  bool enabled;
  ProfilerOptions options;
  Data all;
  std::map<uint64_t, Data> databases;
  std::deque<Event> events;
};
std::string Quote(const std::string& text) {
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << '"';
  const char* hex = "0123456789abcdef";
  for (unsigned char c : text) {
    if (c == '"' || c == '\\')
      out << '\\' << char(c);
    else if (c < 32)
      out << "\\u00" << hex[c >> 4] << hex[c & 15];
    else
      out << char(c);
  }
  out << '"';
  return out.str();
}
void CountersJSON(std::ostream& out, const std::map<std::string, uint64_t>& c) {
  out << '{';
  bool first = true;
  for (const auto& item : c) {
    if (!first) out << ',';
    first = false;
    out << Quote(item.first) << ':' << item.second;
  }
  out << '}';
}
void DataJSON(std::ostream& out, const Data& data) {
  out << "\"metrics\":{";
  bool first = true;
  for (const auto& item : data.metrics) {
    if (!first) out << ',';
    first = false;
    const Metric& m = item.second;
    out << Quote(item.first) << ":{\"calls\":" << m.calls
        << ",\"completed\":" << m.completed
        << ",\"in_flight\":" << m.calls - m.completed << ",\"ok\":" << m.ok
        << ",\"not_found\":" << m.not_found << ",\"errors\":" << m.errors
        << ",\"timing_samples\":" << m.samples << ",\"total_ns\":" << m.total
        << ",\"min_ns\":" << (m.samples ? m.min : 0) << ",\"max_ns\":" << m.max
        << ",\"p50_upper_ns\":" << m.Percentile(50)
        << ",\"p95_upper_ns\":" << m.Percentile(95)
        << ",\"p99_upper_ns\":" << m.Percentile(99)
        << ",\"histogram_log2_ns\":[";
    for (size_t i = 0; i < m.buckets.size(); ++i) {
      if (i) out << ',';
      out << m.buckets[i];
    }
    out << "]}";
  }
  out << "},\"counters\":";
  CountersJSON(out, data.counters);
}
std::string SummaryJSON(const Snapshot& s, uint64_t export_id) {
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << "{\"schema_version\":1,\"session_id\":" << s.session
      << ",\"export_id\":" << export_id
      << ",\"enabled\":" << (s.enabled ? "true" : "false")
      << ",\"foreground_sample_interval\":"
      << s.options.foreground_sample_interval
      << ",\"max_task_events\":" << s.options.max_task_events
      << ",\"retained_task_events\":" << s.events.size()
      << ",\"dropped_task_events\":" << s.dropped
      << ",\"clock\":\"steady_clock\",\"time_unit\":\"ns\",\"timings\":"
         "\"inclusive\","
         "\"io_semantics\":\"Env bytes, not device "
         "IO\",\"profiling_clock_reads\":"
      << s.clock_reads << ',';
  DataJSON(out, s.all);
  out << ",\"databases\":{";
  bool first = true;
  for (const auto& db : s.databases) {
    if (!first) out << ',';
    first = false;
    out << Quote(std::to_string(db.first)) << ":{";
    DataJSON(out, db.second);
    out << '}';
  }
  out << "}}";
  return out.str();
}
std::string EventsJSON(const Snapshot& s, uint64_t export_id) {
  std::ostringstream out;
  out.imbue(std::locale::classic());
  for (const Event& e : s.events) {
    out << "{\"schema_version\":1,\"session_id\":" << s.session
        << ",\"export_id\":" << export_id << ",\"event_id\":" << e.id
        << ",\"db_instance_id\":" << e.db << ",\"parent_task_id\":" << e.parent
        << ",\"kind\":" << Quote(e.kind) << ",\"trigger\":" << Quote(e.trigger)
        << ",\"level\":" << e.level << ",\"status\":" << Quote(e.status)
        << ",\"failure_stage\":" << Quote(e.failure_stage)
        << ",\"start_ns\":" << e.start << ",\"elapsed_ns\":" << e.elapsed
        << ",\"counters\":";
    CountersJSON(out, e.counters);
    out << ",\"stages\":{";
    bool first = true;
    for (const auto& stage : e.stages) {
      if (!first) out << ',';
      first = false;
      out << Quote(stage.first) << ":{\"calls\":" << stage.second.first
          << ",\"total_ns\":" << stage.second.second << '}';
    }
    out << "}}\n";
  }
  return out.str();
}
}  // namespace

struct Profiler::Impl {
  explicit Impl(const ProfilerOptions& opt)
      : options(opt), session(next_session++) {
    options.foreground_sample_interval =
        std::max<uint32_t>(1, options.foreground_sample_interval);
  }
  Snapshot Copy() const {
    std::lock_guard<std::mutex> lock(mu);
    return Snapshot{session, dropped, clock_reads.load(), enabled.load(),
                    options, all,     databases,          events};
  }
  mutable std::mutex mu, export_mu;
  std::atomic<bool> enabled{false};
  std::atomic<uint64_t> sequence{0}, job_sequence{1}, clock_reads{0};
  std::atomic<uint64_t (*)()> clock{nullptr};
  mutable uint64_t export_sequence = 0;
  ProfilerOptions options;
  uint64_t session, dropped = 0;
  Data all;
  std::map<uint64_t, Data> databases;
  std::deque<Event> events;
};

Profiler::Profiler(const ProfilerOptions& options) : impl_(new Impl(options)) {}
Profiler::~Profiler() = default;
void Profiler::SetEnabled(bool enabled) {
  impl_->enabled.store(enabled, std::memory_order_release);
}
bool Profiler::IsEnabled() const {
  return impl_->enabled.load(std::memory_order_acquire);
}
std::string Profiler::SnapshotJSON() const {
  return SummaryJSON(impl_->Copy(), 0);
}
Status Profiler::Export(const std::string& dir) const {
  std::lock_guard<std::mutex> export_lock(impl_->export_mu);
  const Snapshot snapshot = impl_->Copy();
  const uint64_t id = ++impl_->export_sequence;
  Env* env = Env::Default();
  if (!env->FileExists(dir)) {
    Status s = env->CreateDir(dir);
    if (!s.ok()) return s;
  }
  const std::string suffix =
      ".tmp." + std::to_string(snapshot.session) + "." + std::to_string(id);
  const std::string summary = dir + "/summary.json",
                    events = dir + "/tasks.jsonl";
  Status s = WriteStringToFile(env, SummaryJSON(snapshot, id) + "\n",
                               summary + suffix);
  if (s.ok())
    s = WriteStringToFile(env, EventsJSON(snapshot, id), events + suffix);
  if (s.ok()) s = env->RenameFile(events + suffix, events);
  if (s.ok()) s = env->RenameFile(summary + suffix, summary);
  if (!s.ok()) {
    env->RemoveFile(summary + suffix);
    env->RemoveFile(events + suffix);
  }
  return s;
}
uint64_t NewProfileDatabaseID() { return next_database.fetch_add(1); }

ProfileScope::ProfileScope(const char* name, bool task, const char* trigger,
                           int level) {
  if (!current_scope) return;
  profiler_ = current_scope->profiler_;
  db_id_ = current_scope->db_id_;
  sampled_ = current_scope->sampled_ || task;
  Begin(name, task, trigger, level);
}
ProfileScope::ProfileScope(const std::shared_ptr<Profiler>& profiler,
                           uint64_t db_id, const char* name, bool task,
                           const char* trigger, int level,
                           bool suppress_nested) {
  ProfileScope* parent = Current(db_id);
  if (parent && parent->profiler_ == profiler) {
    if (suppress_nested) return;
    profiler_ = profiler;
    db_id_ = db_id;
    sampled_ = parent->sampled_ || task;
  } else {
    if (!profiler || !profiler->IsEnabled()) {
      // A disabled DB called from another DB must block inherited attribution.
      previous_ = current_scope;
      current_scope = nullptr;
      restore_inactive_ = true;
      return;
    }
    profiler_ = profiler;
    db_id_ = db_id;
    sampled_ =
        task || profiler_->impl_->sequence.fetch_add(1) %
                        profiler_->impl_->options.foreground_sample_interval ==
                    0;
  }
  Begin(name, task, trigger, level);
}
void ProfileScope::Begin(const char* name, bool task, const char* trigger,
                         int level) {
  name_ = name;
  task_ = task;
  trigger_ = trigger;
  level_ = level;
  previous_ = current_scope;
  if (task) {
    job_id_ = profiler_->impl_->job_sequence.fetch_add(1);
    for (ProfileScope* p = previous_; p; p = p->previous_) {
      if (p->profiler_ == profiler_ && p->db_id_ == db_id_ && p->task_) {
        parent_job_id_ = p->job_id_;
        break;
      }
    }
  }
  current_scope = this;
  if (sampled_) start_ = Now();
  auto& impl = *profiler_->impl_;
  std::lock_guard<std::mutex> lock(impl.mu);
  ++impl.all.metrics[name_].calls;
  ++impl.databases[db_id_].metrics[name_].calls;
}
ProfileScope::~ProfileScope() {
  if (!active()) {
    if (restore_inactive_) current_scope = previous_;
    return;
  }
  const uint64_t elapsed = sampled_ ? Now() - start_ : 0;
  const bool failed =
      std::string(status_) != "ok" && std::string(status_) != "not_found";
  if (failed) {
    if (failure_stage_.empty()) failure_stage_ = name_;
    if (previous_ && previous_->profiler_ == profiler_ &&
        previous_->db_id_ == db_id_)
      previous_->failure_stage_ = failure_stage_;
  }
  for (auto* p = this; p; p = p->previous_) {
    if (p->task_ && p->profiler_ == profiler_ && p->db_id_ == db_id_) {
      auto& stage = p->stages_[name_];
      ++stage.first;
      stage.second += elapsed;
    }
  }
  auto& impl = *profiler_->impl_;
  {
    std::lock_guard<std::mutex> lock(impl.mu);
    impl.all.metrics[name_].Finish(status_, sampled_, elapsed);
    impl.databases[db_id_].metrics[name_].Finish(status_, sampled_, elapsed);
    if (task_) {
      if (impl.options.max_task_events == 0)
        ++impl.dropped;
      else {
        if (impl.events.size() == impl.options.max_task_events) {
          impl.events.pop_front();
          ++impl.dropped;
        }
        impl.events.push_back(Event{job_id_, db_id_, parent_job_id_, start_,
                                    elapsed, name_, trigger_, status_,
                                    failed ? failure_stage_ : "", level_,
                                    std::move(counters_), std::move(stages_)});
      }
    }
  }
  current_scope = previous_;
}
void ProfileScope::SetStatus(const Status& status) {
  status_ = status.ok()                    ? "ok"
            : status.IsNotFound()          ? "not_found"
            : status.IsCorruption()        ? "corruption"
            : status.IsInvalidArgument()   ? "invalid_argument"
            : status.IsNotSupportedError() ? "not_supported"
                                           : "io_error";
}
ProfileScope* ProfileScope::Current(uint64_t db_id) {
  return current_scope && (!db_id || current_scope->db_id_ == db_id)
             ? current_scope
             : nullptr;
}
uint64_t ProfileScope::CurrentTaskID() {
  for (auto* p = current_scope; p; p = p->previous_)
    if (p->task_ && p->db_id_ == current_scope->db_id_ &&
        p->profiler_ == current_scope->profiler_)
      return p->job_id_;
  return 0;
}
uint64_t ProfileScope::Now() {
  if (!current_scope) return 0;
  auto& impl = *current_scope->profiler_->impl_;
  impl.clock_reads.fetch_add(1, std::memory_order_relaxed);
  auto clock = impl.clock.load();
  return clock ? clock()
               : static_cast<uint64_t>(
                     std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
                         .count());
}
void ProfileScope::Count(const std::string& name, uint64_t value) {
  if (!current_scope) return;
  auto& impl = *current_scope->profiler_->impl_;
  {
    std::lock_guard<std::mutex> lock(impl.mu);
    impl.all.counters[name] += value;
    impl.databases[current_scope->db_id_].counters[name] += value;
  }
  for (auto* p = current_scope; p; p = p->previous_) {
    if (p->task_ && p->profiler_ == current_scope->profiler_ &&
        p->db_id_ == current_scope->db_id_)
      p->counters_[name] += value;
  }
}
void ProfileScope::Duration(const char* name, uint64_t elapsed, bool sampled) {
  if (!current_scope) return;
  for (auto* p = current_scope; p; p = p->previous_) {
    if (p->task_ && p->profiler_ == current_scope->profiler_ &&
        p->db_id_ == current_scope->db_id_) {
      auto& stage = p->stages_[name];
      ++stage.first;
      stage.second += elapsed;
    }
  }
  auto& impl = *current_scope->profiler_->impl_;
  std::lock_guard<std::mutex> lock(impl.mu);
  auto add = [&](Data& data) {
    auto& m = data.metrics[name];
    ++m.calls;
    m.Finish("ok", sampled, elapsed);
  };
  add(impl.all);
  add(impl.databases[current_scope->db_id_]);
}
void ProfilerTestPeer::SetClock(Profiler* profiler, uint64_t (*clock)()) {
  profiler->impl_->clock.store(clock);
}
uint64_t ProfilerTestPeer::ClockReads(const Profiler& profiler) {
  return profiler.impl_->clock_reads.load();
}
uint64_t ProfilerTestPeer::Counter(const Profiler& profiler,
                                   const std::string& name) {
  auto snapshot = profiler.impl_->Copy();
  auto it = snapshot.all.counters.find(name);
  return it == snapshot.all.counters.end() ? 0 : it->second;
}
uint64_t ProfilerTestPeer::MetricField(const Profiler& profiler,
                                       const std::string& name,
                                       const std::string& field) {
  auto snapshot = profiler.impl_->Copy();
  auto it = snapshot.all.metrics.find(name);
  if (it == snapshot.all.metrics.end()) return 0;
  const auto& m = it->second;
  if (field == "calls") return m.calls;
  if (field == "completed") return m.completed;
  if (field == "errors") return m.errors;
  if (field == "samples") return m.samples;
  if (field == "total_ns") return m.total;
  if (field == "max_ns") return m.max;
  return 0;
}

uint64_t ProfilerTestPeer::Inflight(const Profiler& profiler) {
  const auto snapshot = profiler.impl_->Copy();
  uint64_t result = 0;
  for (const auto& metric : snapshot.all.metrics)
    result += metric.second.calls - metric.second.completed;
  return result;
}

namespace {
struct HeldLock {
  bool sampled;
  uint64_t start;
};
thread_local std::map<const DBProfileMutexObserver*, HeldLock> held_locks;
}  // namespace
uint64_t DBProfileMutexObserver::BeforeLock() {
  auto* scope = ProfileScope::Current(db_id_);
  if (!scope) return 0;
  return scope->sampled() ? ProfileScope::Now() + 2 : 1;
}
void DBProfileMutexObserver::AfterLock(uint64_t ticket) {
  if (!ticket) return;
  const bool sampled = ticket > 1;
  const uint64_t now = sampled ? ProfileScope::Now() : 0;
  ProfileScope::Duration("lock.db.acquire_wait",
                         sampled ? now - (ticket - 2) : 0, sampled);
  held_locks[this] = HeldLock{sampled, now};
}
void DBProfileMutexObserver::BeforeUnlock() {
  auto it = held_locks.find(this);
  if (it == held_locks.end()) return;
  if (ProfileScope::Current(db_id_)) {
    const auto held = it->second;
    ProfileScope::Duration("lock.db.hold",
                           held.sampled ? ProfileScope::Now() - held.start : 0,
                           held.sampled);
  }
  held_locks.erase(it);
}
uint64_t DBProfileMutexObserver::BeforeWait() {
  BeforeUnlock();
  return BeforeLock();
}
void DBProfileMutexObserver::AfterWait(uint64_t ticket) {
  if (!ticket) return;
  const bool sampled = ticket > 1;
  const uint64_t now = sampled ? ProfileScope::Now() : 0;
  ProfileScope::Duration("lock.db.condition_wait_reacquire",
                         sampled ? now - (ticket - 2) : 0, sampled);
  held_locks[this] = HeldLock{sampled, now};
}

namespace {
std::string FileCategory(const std::string& path) {
  uint64_t number;
  FileType type;
  if (!ParseFileName(path.substr(path.find_last_of('/') + 1), &number, &type))
    return "other";
  switch (type) {
    case kTableFile:
      return "sst";
    case kLogFile:
      return "wal";
    case kDescriptorFile:
      return "manifest";
    default:
      return "other";
  }
}
struct IOInfo {
  IOInfo(uint64_t id, const std::string& path)
      : db(id),
        prefix("io." + FileCategory(path)),
        read_metric(prefix + ".read"),
        append_metric(prefix + ".append"),
        sync_metric(prefix + ".sync") {}
  uint64_t db;
  std::string prefix, read_metric, append_metric, sync_metric;
  void Count(const char* suffix, uint64_t n = 1) const {
    if (ProfileScope::Current(db)) ProfileScope::Count(prefix + suffix, n);
  }
};
class ProfileSequentialFile : public SequentialFile {
 public:
  ProfileSequentialFile(SequentialFile* file, uint64_t db,
                        const std::string& path)
      : file_(file), info_(db, path) {}
  Status Read(size_t n, Slice* result, char* scratch) override {
    if (!ProfileScope::Current(info_.db))
      return file_->Read(n, result, scratch);
    ProfileScope scope(info_.read_metric.c_str());
    info_.Count(".read_calls");
    Status s = file_->Read(n, result, scratch);
    if (s.ok())
      info_.Count(".read_bytes", result->size());
    else
      info_.Count(".read_errors");
    return scope.Result(s);
  }
  Status Skip(uint64_t n) override { return file_->Skip(n); }

 private:
  std::unique_ptr<SequentialFile> file_;
  IOInfo info_;
};
class ProfileRandomFile : public RandomAccessFile {
 public:
  ProfileRandomFile(RandomAccessFile* file, uint64_t db,
                    const std::string& path)
      : file_(file), info_(db, path) {}
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (!ProfileScope::Current(info_.db))
      return file_->Read(offset, n, result, scratch);
    ProfileScope scope(info_.read_metric.c_str());
    info_.Count(".read_calls");
    Status s = file_->Read(offset, n, result, scratch);
    if (s.ok())
      info_.Count(".read_bytes", result->size());
    else
      info_.Count(".read_errors");
    return scope.Result(s);
  }

 private:
  std::unique_ptr<RandomAccessFile> file_;
  IOInfo info_;
};
class ProfileWritableFile : public WritableFile {
 public:
  ProfileWritableFile(WritableFile* file, uint64_t db, const std::string& path)
      : file_(file), info_(db, path) {}
  Status Append(const Slice& data) override {
    if (!ProfileScope::Current(info_.db)) return file_->Append(data);
    ProfileScope scope(info_.append_metric.c_str());
    info_.Count(".append_calls");
    info_.Count(".append_attempt_bytes", data.size());
    Status s = file_->Append(data);
    if (s.ok())
      info_.Count(".append_bytes", data.size());
    else
      info_.Count(".append_errors");
    return scope.Result(s);
  }
  Status Sync() override {
    if (!ProfileScope::Current(info_.db)) return file_->Sync();
    ProfileScope scope(info_.sync_metric.c_str());
    info_.Count(".sync_calls");
    Status s = file_->Sync();
    if (!s.ok()) info_.Count(".sync_errors");
    return scope.Result(s);
  }
  Status Flush() override { return file_->Flush(); }
  Status Close() override { return file_->Close(); }

 private:
  std::unique_ptr<WritableFile> file_;
  IOInfo info_;
};
class ProfilingEnv : public EnvWrapper {
 public:
  ProfilingEnv(Env* target, uint64_t db) : EnvWrapper(target), db_(db) {}
  Status NewSequentialFile(const std::string& path,
                           SequentialFile** out) override {
    Status s = target()->NewSequentialFile(path, out);
    if (s.ok()) *out = new ProfileSequentialFile(*out, db_, path);
    return s;
  }
  Status NewRandomAccessFile(const std::string& path,
                             RandomAccessFile** out) override {
    Status s = target()->NewRandomAccessFile(path, out);
    if (s.ok()) *out = new ProfileRandomFile(*out, db_, path);
    return s;
  }
  Status NewWritableFile(const std::string& path, WritableFile** out) override {
    Status s = target()->NewWritableFile(path, out);
    if (s.ok()) *out = new ProfileWritableFile(*out, db_, path);
    return s;
  }
  Status NewAppendableFile(const std::string& path,
                           WritableFile** out) override {
    Status s = target()->NewAppendableFile(path, out);
    if (s.ok()) *out = new ProfileWritableFile(*out, db_, path);
    return s;
  }

 private:
  uint64_t db_;
};
}  // namespace
std::unique_ptr<Env> NewProfilingEnv(Env* target, uint64_t db_id) {
  return std::unique_ptr<Env>(new ProfilingEnv(target, db_id));
}
}  // namespace leveldb
