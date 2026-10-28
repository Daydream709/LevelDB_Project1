#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <sys/resource.h>
#include <unistd.h>
#include "db/db_impl.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/profiler.h"
#include "leveldb/write_batch.h"

namespace hw {
using namespace leveldb;
constexpr uint64_t kSecond = 1000000;
constexpr uint64_t kEpoch = 1700000000000000ULL;
using Clock = std::chrono::steady_clock;
inline double Micros(Clock::time_point start) {
  return std::chrono::duration<double, std::micro>(Clock::now()-start).count();
}
inline void Check(bool ok, const std::string& why) {
  if (!ok) throw std::runtime_error(why);
}
inline void Ok(const Status& s) { Check(s.ok(), s.ToString()); }
inline std::string Json(const std::string& s) {
  std::ostringstream o; o << '"';
  for (unsigned char c : s) {
    if (c=='"'||c=='\\') o << '\\' << c;
    else if (c < 32) o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << int(c) << std::dec;
    else o << c;
  }
  return o.str()+'"';
}
struct Config {
  std::string name, path;
  size_t keys=10000, threads=4, rounds=4, value_bytes=256;
  size_t key_bytes=16, batch_size=64, query_threads=1;
  size_t metadata_memtable_bytes=1<<20, metadata_cache_bytes=8<<20;
  size_t metadata_workspace_bytes=4<<20, write_buffer_bytes=256<<10;
  size_t metadata_wal_bytes=16<<20;
  uint64_t metadata_checkpoint_interval_micros=1000000;
  size_t warmup_seconds=1, duration_seconds=3, distinct_percent=100;
  uint64_t seed=7774596;
  unsigned expired_percent=50;
  bool real_clock=false, enable_metadata_profiling=false, sync_writes=false;
  size_t profile_sample_rate=64;
  std::string distribution="sequential";
  bool profiling=false;
  uint32_t profiling_sample_interval=1;
  std::string profiling_output;
  std::shared_ptr<Profiler> profiler;
};
// Track scheduled work so a quiescent oracle cannot race Version installation.
class TestEnv : public EnvWrapper {
 public:
  enum class IoKind : size_t { kPrimarySst, kMetadataSst, kMetadataChange, kWal, kManifest, kOther, kCount };
  struct IoSnapshot {
    uint64_t read_bytes = 0, write_bytes = 0, syncs = 0, sync_ns = 0;
  };
  explicit TestEnv(bool real=false):EnvWrapper(Env::Default()), real_(real) {}
  uint64_t NowMicros() override { return real_ ? target()->NowMicros() : now.load(); }
  std::atomic<uint64_t> now{kEpoch}, bytes_written{0};
  static IoKind Classify(const std::string& name) {
    if (name.find("MANIFEST-") != std::string::npos) return IoKind::kManifest;
    auto ends = [&name](const char* suffix) {
      const size_t n = std::strlen(suffix);
      return name.size() >= n && name.compare(name.size() - n, n, suffix) == 0;
    };
    if (ends(".midx")) return IoKind::kMetadataSst;
    if (ends(".mchg")) return IoKind::kMetadataChange;
    if (ends(".log")) return IoKind::kWal;
    if (ends(".ldb") || ends(".sst")) return IoKind::kPrimarySst;
    return IoKind::kOther;
  }
  IoSnapshot Io(size_t kind) const {
    const auto& c = io_[kind];
    return {c.read_bytes.load(), c.write_bytes.load(), c.syncs.load(),
            c.sync_ns.load()};
  }
  static const char* IoName(IoKind kind) {
    static const char* names[] = {"primary_sst", "metadata_sst",
                                  "metadata_change", "wal", "manifest", "other"};
    return names[static_cast<size_t>(kind)];
  }
  Status NewSequentialFile(const std::string& n, SequentialFile** out) override {
    Status s = target()->NewSequentialFile(n, out);
    if (s.ok()) *out = new CountSequentialFile(this, *out, Classify(n));
    return s;
  }
  Status NewRandomAccessFile(const std::string& n, RandomAccessFile** out) override {
    Status s = target()->NewRandomAccessFile(n, out);
    if (s.ok()) *out = new CountRandomAccessFile(this, *out, Classify(n));
    return s;
  }
  void Schedule(void (*fn)(void*), void* arg) override {
    auto* task=new Task{this,fn,arg};
    { std::lock_guard<std::mutex> l(mu_); ++pending_; }
    target()->Schedule([](void* p) {
      std::unique_ptr<Task> t(static_cast<Task*>(p));
      t->fn(t->arg);
      std::lock_guard<std::mutex> l(t->env->mu_);
      --t->env->pending_; t->env->cv_.notify_all();
    }, task);
  }
  void Wait() {
    std::unique_lock<std::mutex> l(mu_);
    Check(cv_.wait_for(l,std::chrono::seconds(60),[this]{return pending_==0;}),"background work did not become idle");
  }
  Status NewWritableFile(const std::string& n, WritableFile** out) override {
    Status s=target()->NewWritableFile(n,out);
    if(s.ok()) *out=new CountFile(this,*out,Classify(n));
    return s;
  }
  Status NewAppendableFile(const std::string& n, WritableFile** out) override {
    Status s=target()->NewAppendableFile(n,out);
    if(s.ok()) *out=new CountFile(this,*out,Classify(n));
    return s;
  }
 private:
  struct AtomicIo {
    std::atomic<uint64_t> read_bytes{0}, write_bytes{0}, syncs{0}, sync_ns{0};
  };
  void RecordSync(IoKind kind, uint64_t ns) {
    auto& c = io_[static_cast<size_t>(kind)];
    c.syncs.fetch_add(1, std::memory_order_relaxed);
    c.sync_ns.fetch_add(ns, std::memory_order_relaxed);
  }
  struct Task { TestEnv* env; void (*fn)(void*); void* arg; };
  class CountSequentialFile : public SequentialFile {
   public:
    CountSequentialFile(TestEnv* e, SequentialFile* f, IoKind k)
        : env_(e), file_(f), kind_(k) {}
    Status Read(size_t n, Slice* result, char* scratch) override {
      Status s = file_->Read(n, result, scratch);
      if (s.ok()) env_->io_[static_cast<size_t>(kind_)].read_bytes.fetch_add(
          result->size(), std::memory_order_relaxed);
      return s;
    }
    Status Skip(uint64_t n) override { return file_->Skip(n); }
   private:
    TestEnv* env_; std::unique_ptr<SequentialFile> file_; IoKind kind_;
  };
  class CountRandomAccessFile : public RandomAccessFile {
   public:
    CountRandomAccessFile(TestEnv* e, RandomAccessFile* f, IoKind k)
        : env_(e), file_(f), kind_(k) {}
    Status Read(uint64_t offset, size_t n, Slice* result,
                char* scratch) const override {
      Status s = file_->Read(offset, n, result, scratch);
      if (s.ok()) env_->io_[static_cast<size_t>(kind_)].read_bytes.fetch_add(
          result->size(), std::memory_order_relaxed);
      return s;
    }
   private:
    TestEnv* env_; std::unique_ptr<RandomAccessFile> file_; IoKind kind_;
  };
  class CountFile : public WritableFile {
   public:
    CountFile(TestEnv* e,WritableFile* f,IoKind k):env_(e),file_(f),kind_(k){}
    Status Append(const Slice& x) override {
      Status s=file_->Append(x);
      if(s.ok()) {
        env_->bytes_written.fetch_add(x.size(), std::memory_order_relaxed);
        env_->io_[static_cast<size_t>(kind_)].write_bytes.fetch_add(
            x.size(), std::memory_order_relaxed);
      }
      return s;
    }
    Status Close() override {return file_->Close();}
    Status Flush() override {return file_->Flush();}
    Status Sync() override {
      auto start = Clock::now(); Status s = file_->Sync();
      env_->RecordSync(kind_, std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now() - start).count());
      return s;
    }
   private: TestEnv* env_; std::unique_ptr<WritableFile> file_; IoKind kind_;
  };
  std::array<AtomicIo, static_cast<size_t>(IoKind::kCount)> io_{};
  bool real_; std::mutex mu_; std::condition_variable cv_; size_t pending_=0;
};
class Threads {
 public:
  ~Threads(){for(auto& t:threads_) if(t.joinable()) t.join();}
  template<class F> void Add(F f) {
    threads_.emplace_back([this,f]{try{f();}catch(...){std::lock_guard<std::mutex> l(mu_);error_=std::current_exception();}});
  }
  void Join(){for(auto& t:threads_) t.join(); if(error_) std::rethrow_exception(error_);}
 private: std::vector<std::thread> threads_; std::mutex mu_; std::exception_ptr error_;
};
struct DBState {
  TestEnv env; Options options; std::unique_ptr<Cache> cache;
  std::unique_ptr<DB> db; std::string path;
  explicit DBState(const Config& c, bool fresh=true, uint64_t time=kEpoch,
                   size_t metadata_budget=0, bool open_now=true):env(c.real_clock),cache(NewLRUCache(8<<20)),path(c.path) {
    env.now=time; options.env=&env; options.block_cache=cache.get();
    options.profiler=c.profiler;
    options.create_if_missing=fresh; options.compression=kNoCompression;
    options.metadata_memtable_size=metadata_budget==0?c.metadata_memtable_bytes:metadata_budget;
    options.metadata_wal_size_limit=c.metadata_wal_bytes;
    options.metadata_checkpoint_interval_micros=c.metadata_checkpoint_interval_micros;
    options.metadata_cache_size=c.metadata_cache_bytes;
    options.metadata_workspace_size=c.metadata_workspace_bytes;
    options.enable_metadata_profiling=c.enable_metadata_profiling;
    options.metadata_profile_sample_rate=c.profile_sample_rate;
    options.write_buffer_size=c.write_buffer_bytes; options.max_file_size=256<<10;
    if(fresh) { Ok(DestroyDB(path,options)); }
    if(open_now)Open();
  }
  ~DBState(){db.reset(); try{env.Wait();}catch(...) {}}
  void Open(){DB* p=nullptr; Ok(DB::Open(options,path,&p)); db.reset(p);}
  void Reopen(){db.reset();env.Wait();options.create_if_missing=false;Open();}
  DBImpl* Impl(){return static_cast<DBImpl*>(db.get());}
  void Flush();
  void Compact(int last=5);
  void FullGC();
  void WaitMetadata(bool compact=true) {
    env.Wait();
#if defined(HW_METADATA) || defined(HW_TTL) || defined(HW_COMBINED) || defined(HW_METADATA_RESEARCH)
    std::string property;
    Check(db->GetProperty("leveldb.metadata-stats", &property),
          "metadata stats property unavailable");
    const std::string marker = "published_mseq=";
    const size_t pos = property.find(marker);
    Check(pos != std::string::npos, "metadata transaction boundary unavailable");
    const uint64_t target = std::stoull(property.substr(pos + marker.size()));
    Ok(Impl()->TEST_WaitForMetadata(target, compact, 120ULL * 1000000));
#else
    (void)compact;
#endif
    env.Wait();
  }
};
struct SnapshotGuard {
  DB* db; const Snapshot* snapshot;
  explicit SnapshotGuard(DB* d):db(d),snapshot(d->GetSnapshot()){}
  ~SnapshotGuard(){if(snapshot) db->ReleaseSnapshot(snapshot);}
  void Release(){db->ReleaseSnapshot(snapshot);snapshot=nullptr;}
};
inline std::string Key(size_t n){char b[40];std::snprintf(b,sizeof(b),"key/%012zu",n);return b;}
inline std::string Value(size_t n,size_t bytes){std::string s=std::to_string(n)+":"; s.resize(bytes,char('a'+n%23));return s;}
inline void Expect(DB* d,const std::string& key,const std::string* value,const Snapshot* snap=nullptr) {
  ReadOptions ro;ro.snapshot=snap;std::string got;Status s=d->Get(ro,key,&got);
  if(value){Ok(s);Check(got==*value,"value mismatch for "+key);}
  else Check(s.IsNotFound(),"expected NotFound for "+key+", got "+s.ToString());
}
inline void ExpectValue(DB* d,const std::string& key,const std::string& v,const Snapshot* s=nullptr){Expect(d,key,&v,s);}
inline void CheckView(DB* db,const std::map<std::string,std::string>& expected,const Snapshot* snap=nullptr) {
  ReadOptions ro;ro.snapshot=snap;std::unique_ptr<Iterator> it(db->NewIterator(ro));
  auto e=expected.begin();
  for(it->SeekToFirst();it->Valid();it->Next()){
    Check(e!=expected.end(),"unexpected forward entry");Check(it->key().ToString()==e->first&&it->value().ToString()==e->second,"forward view mismatch");++e;
  }
  Ok(it->status());Check(e==expected.end(),"missing forward entry");
  auto rev=expected.rbegin();
  for(it->SeekToLast();it->Valid();it->Prev()){
    Check(rev!=expected.rend(),"unexpected reverse entry");Check(it->key().ToString()==rev->first&&it->value().ToString()==rev->second,"reverse view mismatch");++rev;
  }
  Ok(it->status());Check(rev==expected.rend(),"missing reverse entry");
  for(const auto& kv:expected){it->Seek(kv.first);Check(it->Valid()&&it->key().ToString()==kv.first,"Seek mismatch");}
}
// Uses the original internal-key envelope (8-byte trailer), not a TTL value encoding.
struct Physical {uint64_t total=0, distinct=0;};
inline Physical ScanPhysical(DBState& s);

inline uint64_t DiskBytes(DBState& s,
    std::array<uint64_t, static_cast<size_t>(TestEnv::IoKind::kCount)>* by_kind=nullptr){
  s.env.Wait();std::vector<std::string> names;Ok(s.env.GetChildren(s.path,&names));uint64_t sum=0;
  for(const auto& n:names){uint64_t z=0;if(s.env.GetFileSize(s.path+"/"+n,&z).ok()){
    sum+=z;if(by_kind)(*by_kind)[static_cast<size_t>(TestEnv::Classify(n))]+=z;
  }}
  return sum;
}
struct Metric {
  std::string name;uint64_t ops=0;double seconds=0,drain_seconds=0;
  double p50=0,p95=0,p99=0,max_us=0;uint64_t bytes=0,drain_bytes=0,disk=0,samples=0;
  bool latency_available=false;
  std::array<std::array<uint64_t,4>,static_cast<size_t>(TestEnv::IoKind::kCount)> io{};
  std::array<std::array<uint64_t,4>,static_cast<size_t>(TestEnv::IoKind::kCount)> drain_io{};
  std::array<uint64_t,static_cast<size_t>(TestEnv::IoKind::kCount)> disk_by_kind{};
  std::string metadata_profile_before,metadata_profile_at_end,metadata_profile_after_drain;
};
inline std::vector<Metric>& Metrics(){static std::vector<Metric> m;return m;}
class Measure {
 public:
  Measure(DBState& d,std::string n):s_(d),name_(std::move(n)),start_(Clock::now()),bytes_(d.env.bytes_written) {
    for(size_t i=0;i<before_io_.size();++i) before_io_[i]=s_.env.Io(i);
    Profile(&profile_before_);
  }
  bool ShouldSample(size_t i) const { return i % 64 == 0; }
  void Sample(Clock::time_point start){
    if(start==Clock::time_point{})return;
    std::lock_guard<std::mutex> lock(sample_mu_);latencies_.push_back(Micros(start));
  }
  void Sample(size_t i,Clock::time_point start){
    if(ShouldSample(i))Sample(start);
  }
  void RecordLatency(double micros){
    std::lock_guard<std::mutex> lock(sample_mu_);latencies_.push_back(micros);
  }
  const std::string& LastProfile() const { return last_profile_; }
  double Elapsed() const { return Micros(start_)/1e6; }
  void CaptureEnd(uint64_t ops, double elapsed=-1){
    if(captured_)return;
    captured_metric_.name=name_;
    captured_metric_.ops=ops;
    captured_metric_.seconds=elapsed<0?Elapsed():elapsed;
    phase_end_bytes_=s_.env.bytes_written.load();
    captured_metric_.bytes=phase_end_bytes_-bytes_;
    Profile(&captured_metric_.metadata_profile_at_end);
    captured_metric_.metadata_profile_before=profile_before_;
    for(size_t i=0;i<phase_end_io_.size();++i)phase_end_io_[i]=s_.env.Io(i);
    std::sort(latencies_.begin(),latencies_.end());
    auto q=[this](double p){return latencies_.empty()?0.0:latencies_[std::min(latencies_.size()-1,size_t(p*(latencies_.size()-1)))];};
    captured_metric_.samples=latencies_.size();
    captured_metric_.latency_available=!latencies_.empty();
    captured_metric_.p50=q(.5);captured_metric_.p95=q(.95);captured_metric_.p99=q(.99);
    if(captured_metric_.latency_available)captured_metric_.max_us=latencies_.back();
    for(size_t i=0;i<captured_metric_.io.size();++i){
      const auto& end=phase_end_io_[i];
      captured_metric_.io[i][0]=end.read_bytes-before_io_[i].read_bytes;
      captured_metric_.io[i][1]=end.write_bytes-before_io_[i].write_bytes;
      captured_metric_.io[i][2]=end.syncs-before_io_[i].syncs;
      captured_metric_.io[i][3]=end.sync_ns-before_io_[i].sync_ns;
    }
    captured_=true;
  }
  void Finish(uint64_t ops, double elapsed=-1){
    CaptureEnd(ops,elapsed);
    Metric m=captured_metric_;
    const auto drain_start=Clock::now();
    s_.WaitMetadata(false);
    m.drain_seconds=Micros(drain_start)/1e6;
    Profile(&m.metadata_profile_after_drain);last_profile_=m.metadata_profile_after_drain;
    m.drain_bytes=s_.env.bytes_written.load()-phase_end_bytes_;
    for(size_t i=0;i<m.io.size();++i){
      const auto end=s_.env.Io(i);
      m.drain_io[i][0]=end.read_bytes-phase_end_io_[i].read_bytes;
      m.drain_io[i][1]=end.write_bytes-phase_end_io_[i].write_bytes;
      m.drain_io[i][2]=end.syncs-phase_end_io_[i].syncs;
      m.drain_io[i][3]=end.sync_ns-phase_end_io_[i].sync_ns;
    }
    m.disk=DiskBytes(s_,&m.disk_by_kind);Metrics().push_back(m);
  }
  // Use when multiple concurrent phase measurements share one maintenance
  // drain. This records a zero per-measure drain instead of charging another
  // measurement's already completed drain to this phase.
  void FinishAfterSharedDrain(){
    CaptureEnd(0);
    Metric m=captured_metric_;
    Profile(&m.metadata_profile_after_drain);last_profile_=m.metadata_profile_after_drain;
    m.disk=DiskBytes(s_,&m.disk_by_kind);Metrics().push_back(m);
  }
 private:
  void Profile(std::string* value) const {
    if(s_.db && s_.db->GetProperty("leveldb.metadata-profile",value))return;
    value->clear();
  }
  DBState& s_;std::string name_;Clock::time_point start_;uint64_t bytes_;
  std::array<TestEnv::IoSnapshot,static_cast<size_t>(TestEnv::IoKind::kCount)> before_io_{};
  std::array<TestEnv::IoSnapshot,static_cast<size_t>(TestEnv::IoKind::kCount)> phase_end_io_{};
  uint64_t phase_end_bytes_=0;
  bool captured_=false;
  Metric captured_metric_;
  std::string profile_before_;
  std::string last_profile_;
  std::mutex sample_mu_;std::vector<double>latencies_;
};
inline void Load(DBState& d,const Config& c){for(size_t i=0;i<c.keys;++i)Ok(d.db->Put(WriteOptions(),Key(i),Value(i,c.value_bytes)));}
using Case=std::function<void(const Config&)>;
using Cases=std::map<std::string,Case>;
}

#ifdef HW_STORAGE_ADAPTER
#include HW_STORAGE_ADAPTER
#else
#include "common/storage_adapter.h"
#endif
