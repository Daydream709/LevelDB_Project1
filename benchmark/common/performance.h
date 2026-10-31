#pragma once
#include "common/support.h"
namespace hw {
inline void AddPerformance(Cases& cases){
  cases["perf_write"] = [](const Config& c){
    DBState d(c);Measure m(d,"put");
    for(size_t i=0;i<c.keys;++i){auto t=Clock::now();Ok(d.db->Put(WriteOptions(),Key(i),Value(i,c.value_bytes)));m.Sample(i,t);}
    m.Finish(c.keys);
    for(size_t i=0;i<c.keys;++i)ExpectValue(d.db.get(),Key(i),Value(i,c.value_bytes));
  };
  cases["perf_read"] = [](const Config& c){
    DBState d(c);Load(d,c);d.Flush();
    std::mt19937_64 rng(c.seed);std::vector<size_t>order(c.keys);for(auto& x:order)x=rng()%c.keys;
    for(const std::string phase:{"hit","miss"}){
      Measure m(d,"get_"+phase);
      for(size_t i=0;i<c.keys;++i){std::string value;auto t=Clock::now();auto s=d.db->Get(ReadOptions(),phase=="hit"?Key(order[i]):"missing/"+Key(order[i]),&value);m.Sample(i,t);
        if(phase=="hit"){Ok(s);Check(value==Value(order[i],c.value_bytes),"perf get value");}else Check(s.IsNotFound(),"perf get miss");}
      m.Finish(c.keys);
    }
  };
  cases["perf_scan"] = [](const Config& c){
    DBState d(c);Load(d,c);d.Flush();Measure m(d,"scan");size_t i=0;
    std::unique_ptr<Iterator> it(d.db->NewIterator(ReadOptions()));
    for(it->SeekToFirst();it->Valid();it->Next(),++i)Check(i<c.keys&&it->key().ToString()==Key(i)&&it->value().ToString()==Value(i,c.value_bytes),"perf scan mismatch");
    Ok(it->status());Check(i==c.keys,"perf scan count");m.Finish(i);
  };
  cases["perf_reopen"] = [](const Config& c){
    DBState d(c);Load(d,c);d.FullGC();d.db.reset();d.env.Wait();d.options.create_if_missing=false;
    Measure m(d,"open");auto t=Clock::now();d.Open();m.Sample(0,t);m.Finish(1);
    for(size_t i=0;i<c.keys;++i)ExpectValue(d.db.get(),Key(i),Value(i,c.value_bytes));
  };
  cases["perf_concurrent"] = [](const Config& c){
    DBState d(c);Measure m(d,"parallel_put");Threads group;
    for(size_t t=0;t<c.threads;++t)group.Add([&,t]{for(size_t i=t;i<c.keys;i+=c.threads)Ok(d.db->Put(WriteOptions(),Key(i),Value(i,c.value_bytes)));});
    group.Join();m.Finish(c.keys);
    for(size_t i=0;i<c.keys;++i)ExpectValue(d.db.get(),Key(i),Value(i,c.value_bytes));
  };
}
}
