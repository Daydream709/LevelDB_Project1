#include "common/support.h"
#include <iomanip>
#include "common/performance.h"
#ifdef HW_TTL
#include "ttl/cases.h"
#endif
#ifdef HW_METADATA
#include "metadata/cases.h"
#endif
#ifdef HW_METADATA_RESEARCH
#include "metadata/research_cases.h"
#endif
#ifdef HW_COMBINED
#include "combined/cases.h"
#endif
#ifdef HW_TEACHER
#include "hidden.h"
#endif
int main(int argc,char** argv){
  using namespace hw;
  Config c;Cases cases;AddPerformance(cases);
#ifdef HW_TTL
  AddTTL(cases);
#endif
#ifdef HW_METADATA
  AddMetadata(cases);
#endif
#ifdef HW_METADATA_RESEARCH
  AddMetadataResearch(cases);
#endif
#ifdef HW_COMBINED
  AddCombined(cases);
#endif
#ifdef HW_TEACHER
  AddHidden(cases);
#endif
  try{
    bool profile_override=false;
    for(int i=1;i<argc;++i){std::string arg=argv[i];
      if(arg=="--list"){for(const auto& kv:cases)std::cout<<kv.first<<'\n';return 0;}
      if(arg=="--profile-metadata"){c.enable_metadata_profiling=true;profile_override=true;continue;}
      if(arg=="--no-profile-metadata"){c.enable_metadata_profiling=false;profile_override=true;continue;}
      if(arg=="--sync-writes"){c.sync_writes=true;continue;}
      if(arg=="--async-writes"){c.sync_writes=false;continue;}
      Check(i+1<argc,"missing argument");std::string v=argv[++i];
      if(arg=="--case")c.name=v;else if(arg=="--db")c.path=v;
      else if(arg=="--keys")c.keys=std::stoull(v);else if(arg=="--threads")c.threads=std::stoull(v);
      else if(arg=="--rounds")c.rounds=std::stoull(v);else if(arg=="--value-bytes")c.value_bytes=std::stoull(v);
      else if(arg=="--seed")c.seed=std::stoull(v);else if(arg=="--expired-percent")c.expired_percent=std::stoul(v);
      else if(arg=="--key-bytes")c.key_bytes=std::stoull(v);
      else if(arg=="--batch-size")c.batch_size=std::stoull(v);
      else if(arg=="--query-threads")c.query_threads=std::stoull(v);
      else if(arg=="--metadata-memtable-bytes")c.metadata_memtable_bytes=std::stoull(v);
      else if(arg=="--metadata-wal-bytes")c.metadata_wal_bytes=std::stoull(v);
      else if(arg=="--metadata-checkpoint-interval-micros")c.metadata_checkpoint_interval_micros=std::stoull(v);
      else if(arg=="--metadata-cache-bytes")c.metadata_cache_bytes=std::stoull(v);
      else if(arg=="--metadata-workspace-bytes")c.metadata_workspace_bytes=std::stoull(v);
      else if(arg=="--write-buffer-bytes")c.write_buffer_bytes=std::stoull(v);
      else if(arg=="--warmup-seconds")c.warmup_seconds=std::stoull(v);
      else if(arg=="--duration-seconds")c.duration_seconds=std::stoull(v);
      else if(arg=="--distinct-percent")c.distinct_percent=std::stoull(v);
      else if(arg=="--distribution")c.distribution=v;
      else if(arg=="--profile-sample-rate")c.profile_sample_rate=std::stoull(v);
      else Check(false,"unknown argument "+arg);
    }
    if(!profile_override){
#ifdef HW_METADATA_RESEARCH
      c.enable_metadata_profiling=true;
#endif
    }
    Check(c.keys>0&&c.threads>0&&c.rounds>0&&c.value_bytes>=16&&
          c.key_bytes>0&&c.batch_size>0&&c.metadata_memtable_bytes>0&&
          c.metadata_wal_bytes>0&&c.metadata_checkpoint_interval_micros>0&&
          c.metadata_cache_bytes>0&&c.metadata_workspace_bytes>0&&
          c.write_buffer_bytes>0&&c.profile_sample_rate>0&&
          c.distinct_percent>0&&c.distinct_percent<=100&&
          c.expired_percent<=100&&!c.path.empty(),"invalid config");
    Check(c.distribution=="sequential"||c.distribution=="uniform"||
          c.distribution=="hotspot"||c.distribution=="zipf",
          "distribution must be sequential, uniform, hotspot, or zipf");
    auto it=cases.find(c.name);Check(it!=cases.end(),"unknown case "+c.name);it->second(c);
    rusage usage{};getrusage(RUSAGE_SELF,&usage);
    std::cout<<"HW_RESULT {\"case\":"<<Json(c.name)<<",\"status\":\"pass\",\"peak_rss_kib\":"<<usage.ru_maxrss<<",\"metrics\":[";
    bool first=true;for(const auto& m:Metrics()){if(!first)std::cout<<',';first=false;
      std::cout<<"{\"phase\":"<<Json(m.name)<<",\"ops\":"<<m.ops<<",\"seconds\":"<<m.seconds
        <<",\"ops_per_second\":"<<(m.seconds>0?m.ops/m.seconds:0)<<",\"p50_us\":";
      if(m.latency_available)std::cout<<m.p50;else std::cout<<"null";
      std::cout<<",\"p95_us\":";if(m.latency_available)std::cout<<m.p95;else std::cout<<"null";
      std::cout<<",\"p99_us\":";if(m.latency_available)std::cout<<m.p99;else std::cout<<"null";
      std::cout<<",\"max_us\":";if(m.latency_available)std::cout<<m.max_us;else std::cout<<"null";
      std::cout<<",\"latency_samples\":"<<m.samples<<",\"append_bytes_during_phase\":"<<m.bytes
        <<",\"drain_seconds\":"<<m.drain_seconds<<",\"append_bytes_during_drain\":"<<m.drain_bytes
        <<",\"db_directory_bytes_after_drain\":"<<m.disk<<",\"io\":{";
      for(size_t k=0;k<m.io.size();++k){if(k)std::cout<<',';auto kind=static_cast<TestEnv::IoKind>(k);
        std::cout<<Json(TestEnv::IoName(kind))<<":{\"read_bytes\":"<<m.io[k][0]
          <<",\"write_bytes\":"<<m.io[k][1]<<",\"sync_calls\":"<<m.io[k][2]
          <<",\"sync_ns\":"<<m.io[k][3]<<",\"live_disk_bytes\":"<<m.disk_by_kind[k]<<'}';}
      std::cout<<"},\"drain_io\":{";
      for(size_t k=0;k<m.drain_io.size();++k){if(k)std::cout<<',';auto kind=static_cast<TestEnv::IoKind>(k);
        std::cout<<Json(TestEnv::IoName(kind))<<":{\"read_bytes\":"<<m.drain_io[k][0]
          <<",\"write_bytes\":"<<m.drain_io[k][1]<<",\"sync_calls\":"<<m.drain_io[k][2]
          <<",\"sync_ns\":"<<m.drain_io[k][3]<<'}';}
      std::cout<<"},\"metadata_profile_before\":";
      if(m.metadata_profile_before.empty())std::cout<<"null";else std::cout<<m.metadata_profile_before;
      std::cout<<",\"metadata_profile_at_end\":";
      if(m.metadata_profile_at_end.empty())std::cout<<"null";else std::cout<<m.metadata_profile_at_end;
      std::cout<<",\"metadata_profile_after_drain\":";
      if(m.metadata_profile_after_drain.empty())std::cout<<"null";else std::cout<<m.metadata_profile_after_drain;
      std::cout<<'}';}
    std::cout<<"]}"<<std::endl;return 0;
  }catch(const std::exception& e){std::cout<<"HW_RESULT {\"case\":"<<Json(c.name)<<",\"status\":\"fail\",\"error\":"<<Json(e.what())<<"}"<<std::endl;return 1;}
}
