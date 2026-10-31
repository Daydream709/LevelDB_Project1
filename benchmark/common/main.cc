#include "common/support.h"
#include "common/performance.h"
#ifdef HW_TTL
#include "ttl/cases.h"
#endif
#ifdef HW_METADATA
#include "metadata/cases.h"
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
#ifdef HW_COMBINED
  AddCombined(cases);
#endif
#ifdef HW_TEACHER
  AddHidden(cases);
#endif
  try{
    for(int i=1;i<argc;++i){std::string arg=argv[i];
      if(arg=="--list"){for(const auto& kv:cases)std::cout<<kv.first<<'\n';return 0;}
      Check(i+1<argc,"missing argument");std::string v=argv[++i];
      if(arg=="--case")c.name=v;else if(arg=="--db")c.path=v;
      else if(arg=="--keys")c.keys=std::stoull(v);else if(arg=="--threads")c.threads=std::stoull(v);
      else if(arg=="--rounds")c.rounds=std::stoull(v);else if(arg=="--value-bytes")c.value_bytes=std::stoull(v);
      else if(arg=="--seed")c.seed=std::stoull(v);else if(arg=="--expired-percent")c.expired_percent=std::stoul(v);
      else Check(false,"unknown argument "+arg);
    }
    Check(c.keys>0&&c.threads>0&&c.rounds>0&&c.value_bytes>=16&&c.expired_percent<=100&&!c.path.empty(),"invalid config");
    auto it=cases.find(c.name);Check(it!=cases.end(),"unknown case "+c.name);it->second(c);
    rusage usage{};getrusage(RUSAGE_SELF,&usage);
    std::cout<<"HW_RESULT {\"case\":"<<Json(c.name)<<",\"status\":\"pass\",\"peak_rss_kib\":"<<usage.ru_maxrss<<",\"metrics\":[";
    bool first=true;for(const auto& m:Metrics()){if(!first)std::cout<<',';first=false;
      std::cout<<"{\"phase\":"<<Json(m.name)<<",\"ops\":"<<m.ops<<",\"seconds\":"<<m.seconds
        <<",\"ops_per_second\":"<<(m.seconds>0?m.ops/m.seconds:0)<<",\"p50_us\":"<<m.p50<<",\"p95_us\":"<<m.p95<<",\"p99_us\":"<<m.p99
        <<",\"latency_samples\":"<<m.samples<<",\"append_bytes_during_phase\":"<<m.bytes<<",\"db_directory_bytes_after_drain\":"<<m.disk<<'}';}
    std::cout<<"]}"<<std::endl;return 0;
  }catch(const std::exception& e){std::cout<<"HW_RESULT {\"case\":"<<Json(c.name)<<",\"status\":\"fail\",\"error\":"<<Json(e.what())<<"}"<<std::endl;return 1;}
}
