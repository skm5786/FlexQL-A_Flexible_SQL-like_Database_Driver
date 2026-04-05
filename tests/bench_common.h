/**
 * bench_common.h  —  Shared helpers for all FlexQL benchmark programs.
 *
 * CHANGES: Added DEFAULT_SEED_ROWS cap and timed seeding so benchmarks
 * never hang waiting for millions of rows to insert with WAL overhead.
 */
#pragma once
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <ctime>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <pthread.h>
#include "flexql.h"

#ifdef __GNUC__
#  define BENCH_INLINE __attribute__((always_inline)) inline
#else
#  define BENCH_INLINE inline
#endif

/* Default seed row count for benchmarks — large enough to be meaningful
   but small enough to complete seeding in < 30 seconds with WAL enabled */
#define DEFAULT_SEED_ROWS  100000LL

struct Timer {
    using Clock = std::chrono::steady_clock;
    using TP    = std::chrono::time_point<Clock>;
    TP start_tp, lap_tp;
    void start(){ start_tp=lap_tp=Clock::now(); }
    double elapsed_ms() const {
        using namespace std::chrono;
        return duration<double,std::milli>(Clock::now()-start_tp).count();
    }
    double elapsed_us() const {
        using namespace std::chrono;
        return duration<double,std::micro>(Clock::now()-start_tp).count();
    }
    double lap_ms(){
        using namespace std::chrono;
        TP now=Clock::now();
        double ms=duration<double,std::milli>(now-lap_tp).count();
        lap_tp=now; return ms;
    }
};

struct LatencyStats {
    std::vector<double> samples_us;
    long long total_ops=0;
    double wall_ms=0.0;
    void reserve(size_t n){ samples_us.reserve(n); }
    void record(double us){ samples_us.push_back(us); total_ops++; }
    void set_wall(double ms){ wall_ms=ms; }
    struct Result {
        double mean_us,min_us,p50_us,p95_us,p99_us,max_us;
        double throughput_ops_per_sec;
        long long total_ops; double wall_ms;
    };
    Result compute(){
        Result r{}; r.total_ops=total_ops; r.wall_ms=wall_ms;
        if(samples_us.empty()) return r;
        std::vector<double> s=samples_us; std::sort(s.begin(),s.end());
        double sum=0; for(double v:s)sum+=v;
        auto pct=[&](double p)->double{size_t i=(size_t)(p*s.size()/100.0);if(i>=s.size())i=s.size()-1;return s[i];};
        r.mean_us=sum/s.size(); r.min_us=s.front();
        r.p50_us=pct(50); r.p95_us=pct(95); r.p99_us=pct(99); r.max_us=s.back();
        r.throughput_ops_per_sec=(wall_ms>0)?(total_ops*1000.0/wall_ms):0;
        return r;
    }
};

static void print_header(const char*t){
    printf("\n╔══════════════════════════════════════════════════════════════╗\n");
    printf("║  %-60s║\n",t);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
}
static void print_section(const char*name){
    printf("\n── %s ",name);
    int n=60-(int)strlen(name)-4; if(n<0)n=0;
    for(int i=0;i<n;i++)putchar('-'); putchar('\n');
}
static void print_latency(const char*label,LatencyStats&stats){
    auto r=stats.compute();
    printf("  %-28s  ops=%-8lld  wall=%-8.1fms  tput=%-10.0f ops/s\n"
           "  %-28s  lat(us): mean=%-7.1f p50=%-7.1f p95=%-7.1f p99=%-7.1f max=%-7.1f\n",
           label,r.total_ops,r.wall_ms,r.throughput_ops_per_sec,
           "",r.mean_us,r.p50_us,r.p95_us,r.p99_us,r.max_us);
}
static void print_throughput(const char*label,long long rows,double ms){
    printf("  %-28s  rows=%-10lld  time=%-8.1fms  tput=%.0f rows/s\n",
           label,rows,ms,(ms>0)?rows*1000.0/ms:0.0);
}

/* ── FlexQL helpers ──────────────────────────────────────────────────────── */
static bool fql_exec(FlexQL*db,const char*sql,const char*ctx=nullptr){
    char*err=nullptr;int rc=flexql_exec(db,sql,nullptr,nullptr,&err);
    if(rc!=FLEXQL_OK){fprintf(stderr,"  SQL error [%s]: %s\n  SQL: %.200s\n",ctx?ctx:"?",err?err:"?",sql);if(err)flexql_free(err);return false;}
    return true;
}
static void fql_exec_ignore(FlexQL*db,const char*sql){
    char*err=nullptr;flexql_exec(db,sql,nullptr,nullptr,&err);if(err)flexql_free(err);
}
static int cb_count(void*data,int,char**,char**){(*reinterpret_cast<long long*>(data))++;return 0;}

/* ── Seed: stops early if elapsed > max_ms ───────────────────────────────── */
static long long seed_standard_table(FlexQL*db,const char*table,
                                      long long row_count,int batch=500,
                                      bool progress=true,double max_ms=60000.0){
    long long inserted=0;
    long long next_print=row_count/5; if(next_print<=0)next_print=row_count;
    Timer t; t.start();
    while(inserted<row_count){
        if(t.elapsed_ms()>max_ms){
            printf("  [seed] Time limit %.0fms reached, stopping at %lld/%lld rows\n",
                   max_ms,inserted,row_count);
            break;
        }
        std::ostringstream ss;
        ss<<"INSERT INTO "<<table<<" VALUES ";
        int nb=0;
        while(nb<batch&&inserted<row_count){
            long long id=inserted+1;
            if(nb>0)ss<<",";
            ss<<"("<<id<<","<<(id%10)<<","<<(1000.0+(id%10000))
              <<",'label"<<id<<"',"<<(1700000000LL+id)<<")";
            inserted++;nb++;
        }
        ss<<";";
        if(!fql_exec(db,ss.str().c_str(),"seed"))break;
        if(progress&&inserted>=next_print){
            double el=t.elapsed_ms();
            printf("  seeded %lld/%lld  (%.0f rows/s)\n",
                   inserted,row_count,inserted*1000.0/(el>0?el:1));
            next_print+=row_count/5;
        }
    }
    return inserted;
}

static bool create_standard_table(FlexQL*db,const char*name,bool with_pk=true){
    fql_exec_ignore(db,(std::string("DROP TABLE ")+name+";").c_str());
    std::string sql=std::string("CREATE TABLE ")+name+"(";
    if(with_pk) sql+="ID DECIMAL PRIMARY KEY NOT NULL,";
    else        sql+="ID DECIMAL NOT NULL,";
    sql+="CATEGORY INT NOT NULL,VALUE DECIMAL NOT NULL,"
         "LABEL VARCHAR(64) NOT NULL,CREATED_AT DECIMAL NOT NULL);";
    return fql_exec(db,sql.c_str(),"create_standard_table");
}

static FlexQL*connect_or_die(const char*host="127.0.0.1",int port=9000){
    FlexQL*db=nullptr;
    if(flexql_open(host,port,&db)!=FLEXQL_OK){fprintf(stderr,"Cannot connect to %s:%d\n",host,port);exit(1);}
    return db;
}

struct TestCounter{ int pass=0,fail=0;
    void check(bool c,const char*l){if(c){printf("  [PASS] %s\n",l);pass++;}else{printf("  [FAIL] %s\n",l);fail++;}}
    void summary()const{printf("\n  Results: %d/%d passed",pass,pass+fail);if(fail>0)printf("  (%d FAILED)",fail);printf("\n");}
};
struct ThreadResult{ long long ops_completed=0,errors=0; double elapsed_ms=0.0; };