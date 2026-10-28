#include "util/profiler.h"

#include "db/db_impl.h"
#include "helpers/memenv/memenv.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "leveldb/write_batch.h"

#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {
namespace {
std::atomic<uint64_t> ticks{0};
uint64_t TestClock() { return ticks.fetch_add(10); }
uint64_t Field(const Profiler& p, const std::string& name,
               const std::string& field = "calls") {
  return ProfilerTestPeer::MetricField(p, name, field);
}
uint64_t Count(const Profiler& p, const std::string& name) {
  return ProfilerTestPeer::Counter(p, name);
}

TEST(ProfilerTest, DisabledDoesNotReadClockAndToggleKeepsInflightWork) {
  auto p = std::make_shared<Profiler>();
  ProfilerTestPeer::SetClock(p.get(), TestClock);
  {
    ProfileScope off(p, 1, "off", true);
    ProfileScope child("off.child");
    ProfileScope::Count("off.count");
  }
  EXPECT_EQ(ProfilerTestPeer::ClockReads(*p), 0U);
  EXPECT_EQ(Field(*p, "off"), 0U);
  p->SetEnabled(true);
  ticks = 100;
  {
    ProfileScope root(p, 1, "root");
    p->SetEnabled(false);
    {
      ProfileScope child("child");
      ProfileScope::Count("rows", 2);
    }
  }
  EXPECT_EQ(Field(*p, "root", "total_ns"), 30U);
  EXPECT_EQ(Field(*p, "child", "total_ns"), 10U);
  EXPECT_EQ(Count(*p, "rows"), 2U);
  const auto reads = ProfilerTestPeer::ClockReads(*p);
  {
    ProfileScope off(p, 1, "off");
  }
  EXPECT_EQ(ProfilerTestPeer::ClockReads(*p), reads);
  p->SetEnabled(true);
  {
    ProfileScope root(p, 1, "root");
  }
  EXPECT_EQ(Field(*p, "root"), 2U);
}

TEST(ProfilerTest, SamplingNeverSamplesCountsOrBackgroundTasks) {
  ProfilerOptions opt;
  opt.foreground_sample_interval = 4;
  auto p = std::make_shared<Profiler>(opt);
  p->SetEnabled(true);
  for (int i = 0; i < 10; ++i) {
    ProfileScope root(p, 1, "foreground");
    ProfileScope::Count("rows");
    ProfileScope task("background_test", true);
  }
  EXPECT_EQ(Field(*p, "foreground"), 10U);
  EXPECT_EQ(Field(*p, "foreground", "samples"), 3U);
  EXPECT_EQ(Field(*p, "background_test", "samples"), 10U);
  EXPECT_EQ(Count(*p, "rows"), 10U);
}

TEST(ProfilerTest, ConditionWaitIsNotCountedAsHoldOrPureAcquireWait) {
  auto p = std::make_shared<Profiler>();
  p->SetEnabled(true);
  ProfilerTestPeer::SetClock(p.get(), TestClock);
  ticks = 10;
  port::Mutex mu;
  DBProfileMutexObserver observer(1);
  mu.SetObserver(&observer);
  port::CondVar cv(&mu);
  std::thread notifier;
  {
    ProfileScope root(p, 1, "locked");
    mu.Lock();  // root=10, BeforeLock=20, AfterLock=30
    notifier = std::thread([&] {
      mu.Lock();  // blocks until Wait releases the mutex; no profiler on this
                  // thread
      ticks = 210;
      cv.Signal();
      mu.Unlock();
    });
    ticks = 100;
    cv.Wait();  // hold 70; condition wait starts 110 and finishes 210
    ticks = 240;
    mu.Unlock();  // hold 30
  }
  notifier.join();
  EXPECT_EQ(Field(*p, "lock.db.acquire_wait", "total_ns"), 10U);
  EXPECT_EQ(Field(*p, "lock.db.hold", "total_ns"), 100U);
  EXPECT_EQ(Field(*p, "lock.db.condition_wait_reacquire", "total_ns"), 100U);
}

class SignallingObserver : public DBProfileMutexObserver {
 public:
  SignallingObserver() : DBProfileMutexObserver(1) {}
  uint64_t BeforeLock() override {
    const uint64_t ticket = DBProfileMutexObserver::BeforeLock();
    if (ticket) {
      std::lock_guard<std::mutex> lock(gate);
      waiting = true;
      ready.notify_all();
    }
    return ticket;
  }
  std::mutex gate;
  std::condition_variable ready;
  bool waiting = false;
};
TEST(ProfilerTest, CompetingThreadAcquisitionWaitIsMeasured) {
  auto p = std::make_shared<Profiler>();
  p->SetEnabled(true);
  ProfilerTestPeer::SetClock(p.get(), TestClock);
  ticks = 10;
  port::Mutex mu;
  SignallingObserver observer;
  mu.SetObserver(&observer);
  mu.Lock();
  std::thread waiter([&] {
    ProfileScope root(p, 1, "contended");
    mu.Lock();
    ticks = 200;
    mu.Unlock();
  });
  bool signalled;
  {
    std::unique_lock<std::mutex> gate(observer.gate);
    signalled = observer.ready.wait_for(gate, std::chrono::seconds(10),
                                        [&] { return observer.waiting; });
  }
  ticks = 100;
  mu.Unlock();
  waiter.join();
  ASSERT_TRUE(signalled);
  EXPECT_EQ(Field(*p, "lock.db.acquire_wait", "total_ns"), 80U);
  EXPECT_EQ(Field(*p, "lock.db.hold", "total_ns"), 100U);
}

TEST(ProfilerTest, NestedDatabasesAndDisabledDatabaseStayIsolated) {
  auto first = std::make_shared<Profiler>();
  first->SetEnabled(true);
  auto second = std::make_shared<Profiler>();
  second->SetEnabled(true);
  {
    ProfileScope outer(first, 1, "first");
    ProfileScope::Count("first.rows");
    {
      ProfileScope inner(second, 2, "second");
      ProfileScope::Count("second.rows", 2);
    }
    {
      ProfileScope disabled(std::shared_ptr<Profiler>{}, 3, "disabled");
      ProfileScope stage("disabled.stage");
      ProfileScope::Count("leak", 99);
    }
    ProfileScope::Count("first.rows");
  }
  EXPECT_EQ(Count(*first, "first.rows"), 2U);
  EXPECT_EQ(Count(*first, "second.rows"), 0U);
  EXPECT_EQ(Count(*first, "leak"), 0U);
  EXPECT_EQ(Count(*second, "second.rows"), 2U);
}

class AppendFile : public WritableFile {
 public:
  Status Append(const Slice& x) override {
    return x == Slice("fail") ? Status::IOError("injected") : Status::OK();
  }
  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::IOError("injected"); }
};
class ShortFile : public RandomAccessFile {
 public:
  Status Read(uint64_t, size_t, Slice* out, char*) const override {
    *out = Slice("hi");
    return Status::OK();
  }
};
class IOEnv : public EnvWrapper {
 public:
  IOEnv() : EnvWrapper(Env::Default()) {}
  Status NewWritableFile(const std::string&, WritableFile** out) override {
    *out = new AppendFile;
    return Status::OK();
  }
  Status NewRandomAccessFile(const std::string&,
                             RandomAccessFile** out) override {
    *out = new ShortFile;
    return Status::OK();
  }
};

TEST(ProfilerTest, IOUsesReturnedBytesAndSeparatesFailedAppendAndSync) {
  auto p = std::make_shared<Profiler>();
  p->SetEnabled(true);
  IOEnv target;
  auto env = NewProfilingEnv(&target, 7);
  WritableFile* w;
  ASSERT_LEVELDB_OK(env->NewWritableFile("000001.log", &w));
  RandomAccessFile* r;
  ASSERT_LEVELDB_OK(env->NewRandomAccessFile("000002.ldb", &r));
  {
    ProfileScope root(p, 7, "io_task", true);
    ASSERT_LEVELDB_OK(w->Append("abc"));
    EXPECT_FALSE(w->Append("fail").ok());
    EXPECT_FALSE(w->Sync().ok());
    char scratch[100];
    Slice result;
    ASSERT_LEVELDB_OK(r->Read(0, sizeof(scratch), &result, scratch));
  }
  delete w;
  delete r;
  EXPECT_EQ(Count(*p, "io.wal.append_attempt_bytes"), 7U);
  EXPECT_EQ(Count(*p, "io.wal.append_bytes"), 3U);
  EXPECT_EQ(Count(*p, "io.wal.append_errors"), 1U);
  EXPECT_EQ(Count(*p, "io.wal.sync_errors"), 1U);
  EXPECT_EQ(Count(*p, "io.sst.read_bytes"), 2U);
  EXPECT_EQ(Field(*p, "io.sst.read"), 1U);
  EXPECT_EQ(Field(*p, "io.wal.sync", "errors"), 1U);
}

TEST(ProfilerTest, BoundedEventsExportFailuresAndConcurrentSnapshots) {
  ProfilerOptions options;
  options.max_task_events = 2;
  auto p = std::make_shared<Profiler>(options);
  p->SetEnabled(true);
  for (int i = 0; i < 4; ++i) {
    ProfileScope task(p, 1, "job", true);
  }
  EXPECT_NE(p->SnapshotJSON().find("\"dropped_task_events\":2"),
            std::string::npos);
  std::string temp;
  ASSERT_LEVELDB_OK(Env::Default()->GetTestDirectory(&temp));
  const std::string dir =
      temp + "/profile-export-" + std::to_string(NewProfileDatabaseID());
  ASSERT_LEVELDB_OK(p->Export(dir));
  std::string json, jsonl;
  ASSERT_LEVELDB_OK(
      ReadFileToString(Env::Default(), dir + "/summary.json", &json));
  ASSERT_LEVELDB_OK(
      ReadFileToString(Env::Default(), dir + "/tasks.jsonl", &jsonl));
  EXPECT_NE(json.find("\"retained_task_events\":2"), std::string::npos);
  EXPECT_EQ(std::count(jsonl.begin(), jsonl.end(), '\n'), 2);
  auto before = p->SnapshotJSON();
  EXPECT_FALSE(p->Export(dir + "/summary.json").ok());  // directory is a file
  EXPECT_EQ(before, p->SnapshotJSON());
  std::thread writer([&] {
    for (int i = 0; i < 50; ++i) {
      ProfileScope op(p, 2, "concurrent");
      ProfileScope::Count("work");
    }
  });
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(p->SnapshotJSON().empty());
    ASSERT_LEVELDB_OK(p->Export(dir));
  }
  writer.join();
  EXPECT_EQ(Field(*p, "concurrent"), 50U);
  EXPECT_EQ(Field(*p, "concurrent", "completed"), 50U);
  Env::Default()->RemoveFile(dir + "/summary.json");
  Env::Default()->RemoveFile(dir + "/tasks.jsonl");
  Env::Default()->RemoveDir(dir);
}

TEST(ProfilerTest,
     SharedControllerSurvivesFailedOpenAndWrapperDoesNotDoubleCount) {
  std::unique_ptr<Env> mem(NewMemEnv(Env::Default()));
  Options opt;
  opt.env = mem.get();
  opt.create_if_missing = true;
  opt.profiler = std::make_shared<Profiler>();
  opt.profiler->SetEnabled(true);
  DB* raw = nullptr;
  ASSERT_LEVELDB_OK(DB::Open(opt, "/profile-db", &raw));
  std::unique_ptr<DB> db(raw);
  ASSERT_LEVELDB_OK(db->Put(WriteOptions(), "secret-key", "secret-value"));
  ASSERT_LEVELDB_OK(db->Put(WriteOptions(), "ttl", "x", 100));
  WriteBatch batch;
  batch.Put("b", "v");
  batch.Delete("absent");
  ASSERT_LEVELDB_OK(db->Write(WriteOptions(), &batch));
  EXPECT_EQ(Field(*opt.profiler, "op.put"), 1U);
  EXPECT_EQ(Field(*opt.profiler, "op.ttl_put"), 1U);
  EXPECT_EQ(Field(*opt.profiler, "op.write_batch"), 1U);
  EXPECT_EQ(Count(*opt.profiler, "write.committed_records"), 4U);
  std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  it->Next();
  it->SeekToLast();
  it->Prev();
  EXPECT_GE(Field(*opt.profiler, "op.iterator.seektofirst"), 1U);
  it.reset();
  db.reset();
  opt.error_if_exists = true;
  EXPECT_FALSE(DB::Open(opt, "/profile-db", &raw).ok());
  EXPECT_EQ(raw, nullptr);
  EXPECT_EQ(Field(*opt.profiler, "recovery.open"), 2U);
  EXPECT_EQ(Field(*opt.profiler, "recovery.open", "errors"), 1U);
  auto output = opt.profiler->SnapshotJSON();
  EXPECT_EQ(output.find("secret-key"), std::string::npos);
  EXPECT_EQ(output.find("secret-value"), std::string::npos);
}

TEST(ProfilerTest, FailedTaskIdentifiesStageButOptionalFailureDoesNotFailTask) {
  auto p = std::make_shared<Profiler>();
  p->SetEnabled(true);
  const auto error = Status::IOError("injected");
  {
    ProfileScope task(p, 1, "failed_task", true);
    {
      ProfileScope child("manifest.read");
      child.SetStatus(error);
    }
    task.SetStatus(error);
  }
  {
    ProfileScope task(p, 1, "successful_fallback", true);
    {
      ProfileScope child("optional.index");
      child.SetStatus(error);
    }
  }
  std::string temp;
  ASSERT_LEVELDB_OK(Env::Default()->GetTestDirectory(&temp));
  const std::string dir =
      temp + "/profile-failure-" + std::to_string(NewProfileDatabaseID());
  ASSERT_LEVELDB_OK(p->Export(dir));
  std::string events;
  ASSERT_LEVELDB_OK(
      ReadFileToString(Env::Default(), dir + "/tasks.jsonl", &events));
  EXPECT_NE(events.find("\"failure_stage\":\"manifest.read\""),
            std::string::npos);
  EXPECT_NE(events.find("\"status\":\"ok\",\"failure_stage\":\"\""),
            std::string::npos);
  Env::Default()->RemoveFile(dir + "/summary.json");
  Env::Default()->RemoveFile(dir + "/tasks.jsonl");
  Env::Default()->RemoveDir(dir);
}

TEST(ProfilerTest, CloseDrainsBackgroundPublicationWithoutExternalEnvWait) {
  std::unique_ptr<Env> memory(NewMemEnv(Env::Default()));
  auto profiler = std::make_shared<Profiler>();
  profiler->SetEnabled(true);
  Options options;
  options.env = memory.get();
  options.profiler = profiler;
  options.create_if_missing = true;
  options.write_buffer_size = 64 << 10;
  options.compression = kNoCompression;
  DB* raw = nullptr;
  ASSERT_LEVELDB_OK(DB::Open(options, "/close-profile-db", &raw));
  std::unique_ptr<DB> db(raw);
  const std::string value(4096, 'x');
  for (int i = 0; i < 64; ++i)
    ASSERT_LEVELDB_OK(db->Put(WriteOptions(), std::to_string(i), value, 60));
  db.reset();  // no TestEnv::Wait: the DB itself must finish publishing
               // callbacks
  EXPECT_EQ(ProfilerTestPeer::Inflight(*profiler), 0U);
  EXPECT_GT(Field(*profiler, "background", "completed"), 0U);
}

}  // namespace
}  // namespace leveldb
