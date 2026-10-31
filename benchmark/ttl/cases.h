#pragma once
#include "common/support.h"
namespace hw {
inline void AddTTL(Cases& cases){
  cases["ttl_contract"]=[](const Config& c){
    DBState d(c);Ok(d.db->Put(WriteOptions(),"permanent","v"));
    Ok(d.db->Put(WriteOptions(),"short","v",1));
    Ok(d.db->Put(WriteOptions(),"zero","v",0));Expect(d.db.get(),"zero",nullptr);
    d.env.now=kEpoch+kSecond-1;ExpectValue(d.db.get(),"short","v");
    d.env.now=kEpoch+kSecond;Expect(d.db.get(),"short",nullptr);ExpectValue(d.db.get(),"permanent","v");
    Check(d.db->Put(WriteOptions(),"overflow","x",std::numeric_limits<uint64_t>::max()).IsInvalidArgument(),"TTL conversion overflow");
    d.env.now=std::numeric_limits<uint64_t>::max()-10;
    Check(d.db->Put(WriteOptions(),"overflow","x",1).IsInvalidArgument(),"TTL addition overflow");
    Expect(d.db.get(),"overflow",nullptr);
  };
  cases["ttl_views"]=[](const Config& c){
    DBState d(c);Ok(d.db->Put(WriteOptions(),"a","A",10));Ok(d.db->Put(WriteOptions(),"b","B"));
    SnapshotGuard snap(d.db.get());std::unique_ptr<Iterator> before(d.db->NewIterator(ReadOptions()));
    d.env.now+=10*kSecond;
    CheckView(d.db.get(),{{"b","B"}});CheckView(d.db.get(),{{"a","A"},{"b","B"}},snap.snapshot);
    d.FullGC();ExpectValue(d.db.get(),"a","A",snap.snapshot);
    before->SeekToFirst();Check(before->Valid()&&before->key().ToString()=="a"&&before->value().ToString()=="A","iterator creation time");Ok(before->status());
  };
  cases["perf_ttl_write"]=[](const Config& c){
    DBState d(c);Measure m(d,"ttl_put");
    for(size_t i=0;i<c.keys;++i){auto t=Clock::now();Ok(d.db->Put(WriteOptions(),Key(i),Value(i,c.value_bytes),3600));m.Sample(i,t);}
    m.Finish(c.keys);for(size_t i=0;i<c.keys;++i)ExpectValue(d.db.get(),Key(i),Value(i,c.value_bytes));
  };
  cases["perf_ttl_mixed"]=[](const Config& c){
    DBState d(c);
    for(size_t i=0;i<c.keys;++i)Ok(d.db->Put(WriteOptions(),Key(i),Value(i,c.value_bytes),i%100<c.expired_percent?1:3600));
    d.Flush();d.env.now+=2*kSecond;
    std::mt19937_64 rng(c.seed);Measure m(d,"ttl_mixed_get");
    for(size_t i=0;i<c.keys;++i){size_t key=rng()%c.keys;std::string v;auto t=Clock::now();auto s=d.db->Get(ReadOptions(),Key(key),&v);m.Sample(i,t);
      if(key%100<c.expired_percent)Check(s.IsNotFound(),"expired read");else{Ok(s);Check(v==Value(key,c.value_bytes),"live TTL read");}}
    m.Finish(c.keys);
  };
  cases["perf_ttl_gc"]=[](const Config& c){
    DBState d(c);for(size_t i=0;i<c.keys;++i)Ok(d.db->Put(WriteOptions(),Key(i),Value(i,c.value_bytes),1));
    d.Compact();Check(ScanPhysical(d).total>0,"GC fixture has no stored records");d.env.now+=2*kSecond;
    Measure m(d,"ttl_full_gc");auto t=Clock::now();d.FullGC();m.Sample(0,t);m.Finish(c.keys);
    Check(ScanPhysical(d).total==0,"expired physical records remain after full GC");CheckView(d.db.get(),{});
  };
}
}
