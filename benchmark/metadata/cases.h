#pragma once
#include "common/support.h"
namespace hw {
inline Stats ReadStats(DB* d,const Snapshot* snap=nullptr){Stats s;ReadOptions ro;ro.snapshot=snap;Ok(d->GetCurrentStats(ro,&s));return s;}
inline void StatsEqual(const Stats& s,uint64_t n,uint64_t distinct,uint64_t seq){
  Check(s.total_entries==n && s.distinct_keys==distinct && s.seq==seq,
        "stats expected ("+std::to_string(n)+","+std::to_string(distinct)+","+std::to_string(seq)+") got ("+std::to_string(s.total_entries)+","+std::to_string(s.distinct_keys)+","+std::to_string(s.seq)+")");
}
inline void PhysicalEqual(DBState& d,uint64_t seq){Physical p=ScanPhysical(d);StatsEqual(ReadStats(d.db.get()),p.total,p.distinct,seq);}
inline void AddMetadata(Cases& cases){
  cases["metadata_contract"]=[](const Config& c){
    DBState d(c);StatsEqual(ReadStats(d.db.get()),0,0,0);
    Check(d.db->GetCurrentStats(ReadOptions(),nullptr).IsInvalidArgument(),"null stats argument");
    SnapshotGuard snap(d.db.get());Ok(d.db->Put(WriteOptions(),"a","1"));Ok(d.db->Put(WriteOptions(),"b","1"));Ok(d.db->Put(WriteOptions(),"a","2"));
    StatsEqual(ReadStats(d.db.get()),3,2,3);StatsEqual(ReadStats(d.db.get(),snap.snapshot),3,2,3);
  };
  cases["metadata_reopen"]=[](const Config& c){
    DBState d(c);Load(d,c);StatsEqual(ReadStats(d.db.get()),c.keys,c.keys,c.keys);
    for(int i=0;i<3;++i){d.Reopen();StatsEqual(ReadStats(d.db.get()),c.keys,c.keys,c.keys);PhysicalEqual(d,c.keys);}
  };
  cases["perf_stats"]=[](const Config& c){
    DBState d(c);Load(d,c);d.Flush();Measure m(d,"stats_query");Stats s;
    for(size_t i=0;i<c.keys;++i){auto t=Clock::now();Ok(d.db->GetCurrentStats(ReadOptions(),&s));m.Sample(i,t);StatsEqual(s,c.keys,c.keys,c.keys);}
    m.Finish(c.keys);
  };
  cases["perf_stats_under_write"]=[](const Config& c){
    // Two identical preloaded databases, with and without a querying thread.
    for(bool query : std::vector<bool>{bool(c.seed%2),!bool(c.seed%2)}) {
      DBState d(c);Load(d,c);SnapshotGuard snap(d.db.get());
      std::atomic<size_t> done{0};Threads writers;
      Measure write(d,query?"put_with_stats_queries":"put_without_stats_queries");
      for(size_t t=0;t<c.threads;++t)writers.Add([&,t]{try{for(size_t i=t;i<c.keys;i+=c.threads)Ok(d.db->Put(WriteOptions(),Key(c.keys+i),Value(i,c.value_bytes)));}catch(...){++done;throw;}++done;});
      Measure queries(d,"stats_under_write");size_t n=0;uint64_t last=0;
      if(query) do {auto t=Clock::now();Stats s=ReadStats(d.db.get());queries.Sample(n++,t);
        Check(s.total_entries==s.distinct_keys&&s.seq==s.total_entries&&s.seq>=last,"concurrent stats state");last=s.seq;
      }while(done.load()!=c.threads);
      writers.Join();const double elapsed_write=write.Elapsed(),elapsed_queries=queries.Elapsed();
      write.Finish(c.keys,elapsed_write);if(query)queries.Finish(n,elapsed_queries);
      StatsEqual(ReadStats(d.db.get()),2*c.keys,2*c.keys,2*c.keys);
    }
  };
}
}
