/**
 * bench_full_scan.cpp  —  Full Table Scan Benchmark  (fixed)
 *
 * FIXES: Default 50K rows (was 1M), concurrent scan duration 3s (was 5s).
 * Full scans are O(n) — at 1M rows each scan takes ~1 second. At 50K it's ~50ms.
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_full_scan.cpp src/client/flexql.cpp \
 *       -o tests/bench_full_scan -lpthread
 */
#include "bench_common.h"
#include <atomic>

static double single_scan(FlexQL*db,const char*sql,long long*rows_out=nullptr){
    long long c=0;char*err=nullptr;Timer t;t.start();
    flexql_exec(db,sql,cb_count,&c,&err);double ms=t.elapsed_ms();
    if(err)flexql_free(err);if(rows_out)*rows_out=c;return ms;
}

/* ── Scan throughput at multiple sizes ───────────────────────────────────── */
static void run_scale_series(FlexQL*db){
    print_section("Scan Throughput vs Row Count");
    static const long long SIZES[]={5000,10000,25000,50000,100000};
    printf("  %-10s  %-12s  %-12s  %-12s\n","rows","1st(ms)","2nd(ms)","rows/s");
    for(long long sz:SIZES){
        fql_exec_ignore(db,"DROP TABLE SCAN_SCALE;");
        create_standard_table(db,"SCAN_SCALE",false);
        seed_standard_table(db,"SCAN_SCALE",sz,500,false,15000.0);
        char sql[128];snprintf(sql,sizeof(sql),"SELECT * FROM SCAN_SCALE;");
        long long rows=0;
        double cold=single_scan(db,sql,&rows);
        double warm=single_scan(db,sql);
        printf("  %-10lld  %-12.1f  %-12.1f  %-12.0f\n",
               sz,cold,warm,(warm>0)?rows*1000.0/warm:0);
    }
    fql_exec_ignore(db,"DROP TABLE SCAN_SCALE;");
}

/* ── Projection overhead ─────────────────────────────────────────────────── */
static void run_projection(FlexQL*db,const char*table,long long rows,int reps){
    print_section("Projection Overhead");
    static const struct{const char*sel;const char*label;}PROJS[]={
        {"*","SELECT *"},{"ID","SELECT ID"},{"VALUE","SELECT VALUE"},
        {"LABEL","SELECT LABEL"},{"ID,VALUE","SELECT 2 cols"}
    };
    for(auto&p:PROJS){
        char sql[256];snprintf(sql,sizeof(sql),"SELECT %s FROM %s;",p.sel,table);
        LatencyStats stats;stats.reserve(reps);Timer wall;wall.start();
        for(int i=0;i<reps;i++){Timer t;t.start();single_scan(db,sql);stats.record(t.elapsed_us());}
        stats.set_wall(wall.elapsed_ms());
        print_latency(p.label,stats);
    }
}

/* ── Concurrent scans ────────────────────────────────────────────────────── */
struct ConcArg{const char*table;int dur_sec;std::atomic<int>*stop;ThreadResult result;};
static void*conc_scan_thread(void*arg_){
    ConcArg*arg=(ConcArg*)arg_;FlexQL*db=connect_or_die();
    char sql[256];snprintf(sql,sizeof(sql),"SELECT * FROM %s;",arg->table);
    Timer t;t.start();long long ops=0,errs=0;
    while(!arg->stop->load(std::memory_order_relaxed)){
        long long c=0;char*err=nullptr;int rc=flexql_exec(db,sql,cb_count,&c,&err);
        if(rc==FLEXQL_OK)ops++;else{errs++;if(err)flexql_free(err);}
    }
    arg->result.ops_completed=ops;arg->result.errors=errs;arg->result.elapsed_ms=t.elapsed_ms();
    flexql_close(db);return nullptr;
}
static void run_concurrent(const char*table,int dur_sec){
    print_section("Concurrent Full Scans (RW lock)");
    static const int THREADS[]={1,2,4,8};
    for(int n:THREADS){
        std::atomic<int> stop(0);
        std::vector<pthread_t> tids(n);std::vector<ConcArg> args(n);
        for(int i=0;i<n;i++){args[i].table=table;args[i].dur_sec=dur_sec;args[i].stop=&stop;pthread_create(&tids[i],nullptr,conc_scan_thread,&args[i]);}
        struct timespec ts={dur_sec,0};nanosleep(&ts,nullptr);
        stop.store(1);for(int i=0;i<n;i++)pthread_join(tids[i],nullptr);
        long long total=0,errs=0;for(int i=0;i<n;i++){total+=args[i].result.ops_completed;errs+=args[i].result.errors;}
        printf("  threads=%-2d  scans=%-8lld  %.1f scans/s  errors=%lld\n",
               n,total,(double)total/dur_sec,errs);
    }
}

int main(int argc,char**argv){
    long long row_count=(argc>1)?atoll(argv[1]):50000LL;
    int reps=(argc>2)?atoi(argv[2]):5;
    int dur=(argc>3)?atoi(argv[3]):3;

    print_header("FlexQL Full Table Scan Benchmark");
    printf("  Row count: %lld  Reps: %d  Conc dur: %ds\n",row_count,reps,dur);

    FlexQL*db=connect_or_die();

    run_scale_series(db);

    print_section("Main Table Setup");
    create_standard_table(db,"BENCH_SCAN",false);
    Timer st;st.start();
    long long seeded=seed_standard_table(db,"BENCH_SCAN",row_count,500,true,20000.0);
    printf("  Seed: %.1fms\n",st.elapsed_ms());

    run_projection(db,"BENCH_SCAN",seeded,reps);

    flexql_close(db);
    run_concurrent("BENCH_SCAN",dur);

    db=connect_or_die();
    fql_exec_ignore(db,"DROP TABLE BENCH_SCAN;");
    flexql_close(db);
    printf("\n  Done.\n");
    return 0;
}