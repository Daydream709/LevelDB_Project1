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
  cases["metadata_rebuild"]=[](const Config& c){
    // Removing referenced auxiliary files forces a full rebuild. The small
    // budget creates enough sorted runs to exercise the bounded merge passes.
    DBState d(c,true,kEpoch,64<<10);Load(d,c);d.Flush();
    StatsEqual(ReadStats(d.db.get()),c.keys,c.keys,c.keys);
    d.db.reset();d.env.Wait();
    std::vector<std::string> files;Ok(d.env.GetChildren(d.path,&files));
    size_t removed=0;
    for(const std::string& file:files){
      if(file.size()>=5&&file.substr(file.size()-5)==".midx"){
        Ok(d.env.RemoveFile(d.path+"/"+file));++removed;
      }
    }
    Check(removed>0,"metadata rebuild fixture had no metadata files");
    d.Reopen();
    StatsEqual(ReadStats(d.db.get()),c.keys,c.keys,c.keys);
    PhysicalEqual(d,c.keys);
  };
  cases["metadata_lsm_compaction"]=[](const Config& c){
    // A small metadata budget forces several immutable metadata flushes. The
    // second pass revisits keys already on disk; primary compaction then
    // removes covered versions and base-level deletion records.
    const size_t n=std::min<size_t>(c.keys,2048), deleted=n/4;
    DBState d(c,true,kEpoch,64<<10);
    for(size_t i=0;i<n;++i)Ok(d.db->Put(WriteOptions(),Key(i),Value(i,c.value_bytes)));
    for(size_t i=0;i<n;++i)Ok(d.db->Put(WriteOptions(),Key(i),Value(i+17,c.value_bytes)));
    for(size_t i=0;i<deleted;++i)Ok(d.db->Delete(WriteOptions(),Key(i)));
    // Exercise keys whose high-bit bytes sort differently under signed-char
    // ordering. Metadata SSTs must use LevelDB's unsigned bytewise order.
    const std::vector<std::string> binary_keys={
      std::string("\0b",2), std::string(1,static_cast<char>(0x7f))+"z",
      std::string(1,static_cast<char>(0x80))+"a",
      std::string(1,static_cast<char>(0xff))+"a"};
    for(size_t i=0;i<binary_keys.size();++i){
      Ok(d.db->Put(WriteOptions(),binary_keys[i],Value(i,c.value_bytes)));
      Ok(d.db->Put(WriteOptions(),binary_keys[i],Value(i+1,c.value_bytes)));
    }
    Ok(d.db->Delete(WriteOptions(),binary_keys[0]));
    const uint64_t before=2*n+deleted+binary_keys.size()*2+1;
    const uint64_t before_distinct=n+binary_keys.size();
    Physical physical_before=ScanPhysical(d);
    StatsEqual(ReadStats(d.db.get()),physical_before.total,physical_before.distinct,before);
    Check(physical_before.distinct==before_distinct,"binary metadata key count");
    Check(physical_before.total<=before,"compaction increased physical record count");
    std::vector<std::string> files;Ok(d.env.GetChildren(d.path,&files));
    Check(std::any_of(files.begin(),files.end(),[](const std::string& f){return f.size()>=5&&f.substr(f.size()-5)==".midx";}),"metadata SST flush did not create an index file");
    d.Compact();
    const uint64_t after=n-deleted+binary_keys.size()-1;
    StatsEqual(ReadStats(d.db.get()),after,after,before);
    PhysicalEqual(d,before);
    // The compaction change file remains a lookup layer until a metadata
    // flush folds it into an index SST. Update one of its keys before reopen.
    Ok(d.db->Put(WriteOptions(),binary_keys.back(),"after-compaction"));
    StatsEqual(ReadStats(d.db.get()),after+1,after,before+1);
    PhysicalEqual(d,before+1);
    d.Reopen();StatsEqual(ReadStats(d.db.get()),after+1,after,before+1);PhysicalEqual(d,before+1);
  };
  cases["perf_stats"]=[](const Config& c){
    DBState d(c);Load(d,c);d.Flush();Measure m(d,"stats_query");Stats s;
    for(size_t i=0;i<c.keys;++i){auto t=m.ShouldSample(i)?Clock::now():Clock::time_point{};Ok(d.db->GetCurrentStats(ReadOptions(),&s));m.Sample(t);}
    m.Finish(c.keys);
    StatsEqual(s,c.keys,c.keys,c.keys);
  };
  cases["perf_stats_under_write"]=[](const Config& c){
    // Two identical preloaded databases, with and without a querying thread.
    for(bool query : std::vector<bool>{bool(c.seed%2),!bool(c.seed%2)}) {
      DBState d(c);Load(d,c);SnapshotGuard snap(d.db.get());
      std::atomic<size_t> done{0};Threads writers;
      Measure write(d,query?"put_with_stats_queries":"put_without_stats_queries");
      for(size_t t=0;t<c.threads;++t)writers.Add([&,t]{try{for(size_t i=t;i<c.keys;i+=c.threads)Ok(d.db->Put(WriteOptions(),Key(c.keys+i),Value(i,c.value_bytes)));}catch(...){++done;throw;}++done;});
      Measure queries(d,"stats_under_write");size_t n=0;uint64_t last=0;
      if(query) do {auto t=queries.ShouldSample(n)?Clock::now():Clock::time_point{};Stats s=ReadStats(d.db.get());queries.Sample(t);++n;last=s.seq;
      }while(done.load()!=c.threads);
      writers.Join();const double elapsed_write=write.Elapsed(),elapsed_queries=queries.Elapsed();
      write.Finish(c.keys,elapsed_write);if(query)queries.Finish(n,elapsed_queries);
      StatsEqual(ReadStats(d.db.get()),2*c.keys,2*c.keys,2*c.keys);
      if(query)Check(last==2*c.keys,"concurrent stats final sequence");
    }
  };
}
}
