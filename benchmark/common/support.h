#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
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
  uint64_t seed=7774596;
  unsigned expired_percent=50;
  bool real_clock=false;
};
// Track scheduled work so a quiescent oracle cannot race Version installation.
class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv(bool real=false):EnvWrapper(Env::Default()), real_(real) {}
  uint64_t NowMicros() override { return real_ ? target()->NowMicros() : now.load(); }
  std::atomic<uint64_t> now{kEpoch}, bytes_written{0};
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
    if(s.ok()) *out=new CountFile(this,*out);
    return s;
  }
  Status NewAppendableFile(const std::string& n, WritableFile** out) override {
    Status s=target()->NewAppendableFile(n,out);
    if(s.ok()) *out=new CountFile(this,*out);
    return s;
  }
 private:
  struct Task { TestEnv* env; void (*fn)(void*); void* arg; };
  class CountFile : public WritableFile {
   public:
    CountFile(TestEnv* e,WritableFile* f):env_(e),file_(f){}
    Status Append(const Slice& x) override {
      Status s=file_->Append(x); if(s.ok()) env_->bytes_written+=x.size(); return s;
    }
    Status Close() override {return file_->Close();}
    Status Flush() override {return file_->Flush();}
    Status Sync() override {return file_->Sync();}
   private: TestEnv* env_; std::unique_ptr<WritableFile> file_;
  };
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
  explicit DBState(const Config& c, bool fresh=true, uint64_t time=kEpoch):env(c.real_clock),cache(NewLRUCache(8<<20)),path(c.path) {
    env.now=time; options.env=&env; options.block_cache=cache.get();
    options.create_if_missing=fresh; options.compression=kNoCompression;
    options.write_buffer_size=256<<10; options.max_file_size=256<<10;
    if(fresh) { Ok(DestroyDB(path,options)); }
    Open();
  }
  ~DBState(){db.reset(); try{env.Wait();}catch(...) {}}
  void Open(){DB* p=nullptr; Ok(DB::Open(options,path,&p)); db.reset(p);}
  void Reopen(){db.reset();env.Wait();options.create_if_missing=false;Open();}
  DBImpl* Impl(){return static_cast<DBImpl*>(db.get());}
  void Flush();
  void Compact(int last=5);
  void FullGC();
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

inline uint64_t DiskBytes(DBState& s){
  s.env.Wait();std::vector<std::string> names;Ok(s.env.GetChildren(s.path,&names));uint64_t sum=0;
  for(const auto& n:names){uint64_t z=0;if(s.env.GetFileSize(s.path+"/"+n,&z).ok())sum+=z;}
  return sum;
}
struct Metric {std::string name;uint64_t ops=0;double seconds=0,p50=0,p95=0,p99=0;uint64_t bytes=0,disk=0,samples=0;};
inline std::vector<Metric>& Metrics(){static std::vector<Metric> m;return m;}
class Measure {
 public:
  Measure(DBState& d,std::string n):s_(d),name_(std::move(n)),start_(Clock::now()),bytes_(d.env.bytes_written){}
  void Sample(size_t i,Clock::time_point start){if(i%64==0)latencies_.push_back(Micros(start));}
  double Elapsed() const { return Micros(start_)/1e6; }
  void Finish(uint64_t ops, double elapsed=-1){
    Metric m;m.name=name_;m.ops=ops;m.seconds=elapsed<0?Elapsed():elapsed;m.bytes=s_.env.bytes_written.load()-bytes_;
    std::sort(latencies_.begin(),latencies_.end());
    auto q=[this](double p){return latencies_.empty()?0.0:latencies_[std::min(latencies_.size()-1,size_t(p*(latencies_.size()-1)))];};
    m.samples=latencies_.size();m.p50=q(.5);m.p95=q(.95);m.p99=q(.99);m.disk=DiskBytes(s_);Metrics().push_back(m);
  }
 private: DBState& s_;std::string name_;Clock::time_point start_;uint64_t bytes_;std::vector<double>latencies_;
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
