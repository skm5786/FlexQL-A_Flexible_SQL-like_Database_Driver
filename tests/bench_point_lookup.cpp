/**
 * bench_point_lookup.cpp  —  Point Lookup Benchmark  (fixed)
 *
 * FIXES: Default 100K rows (was 1M), full scan capped at 5 reps (was 20).
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_point_lookup.cpp src/client/flexql.cpp \
 *       -o tests/bench_point_lookup -lpthread
 */
#include "bench_common.h"

static unsigned int g_rng=42;
static unsigned int rng_next(){g_rng^=g_rng<<13;g_rng^=g_rng>>17;g_rng^=g_rng<<5;return g_rng;}

int main(int argc,char**argv){
    long long row_count=(argc>1)?atoll(argv[1]):100000LL;
    int reps=(argc>2)?atoi(argv[2]):200;

    print_header("FlexQL Point Lookup Benchmark");
    printf("  Row count  : %lld\n  Repetitions: %d\n",row_count,reps);

    FlexQL*db=connect_or_die();

    print_section("Table Setup");
    create_standard_table(db,"BENCH_POINT",true);
    Timer st;st.start();
    long long seeded=seed_standard_table(db,"BENCH_POINT",row_count,500,true,30000.0);
    printf("  Seed: %.1fms\n",st.elapsed_ms());

    /* ── PK hash lookups ──────────────────────────────────────────────── */
    print_section("PK Hash Index: WHERE ID = val");
    {
        LatencyStats s;s.reserve(reps);Timer w;w.start();
        for(int i=0;i<reps;i++){
            long long key=(rng_next()%seeded)+1;
            char sql[256];snprintf(sql,sizeof(sql),"SELECT * FROM BENCH_POINT WHERE ID = %lld;",key);
            long long c=0;char*err=nullptr;Timer t;t.start();
            flexql_exec(db,sql,cb_count,&c,&err);s.record(t.elapsed_us());if(err)flexql_free(err);
        }
        s.set_wall(w.elapsed_ms());print_latency("Hash-HIT random",s);
    }
    {
        LatencyStats s;s.reserve(reps);Timer w;w.start();
        for(int i=0;i<reps;i++){
            long long key=seeded+1+(i%1000); /* guaranteed miss */
            char sql[256];snprintf(sql,sizeof(sql),"SELECT * FROM BENCH_POINT WHERE ID = %lld;",key);
            long long c=0;char*err=nullptr;Timer t;t.start();
            flexql_exec(db,sql,cb_count,&c,&err);s.record(t.elapsed_us());if(err)flexql_free(err);
        }
        s.set_wall(w.elapsed_ms());print_latency("Hash-MISS missing",s);
    }

    /* ── B+ tree range ────────────────────────────────────────────────── */
    print_section("B+ Tree Range: VALUE > threshold");
    {
        LatencyStats s;s.reserve(reps);Timer w;w.start();
        for(int i=0;i<reps;i++){
            char sql[256];snprintf(sql,sizeof(sql),"SELECT * FROM BENCH_POINT WHERE VALUE > 10900.0;");
            long long c=0;char*err=nullptr;Timer t;t.start();
            flexql_exec(db,sql,cb_count,&c,&err);s.record(t.elapsed_us());if(err)flexql_free(err);
        }
        s.set_wall(w.elapsed_ms());print_latency("VALUE>10900 (~1%)",s);
    }

    /* ── Full scan VARCHAR — strictly limited ─────────────────────────── */
    print_section("Full Scan VARCHAR (no index) — 5 reps max");
    {
        int fr=std::min(reps,5);
        LatencyStats s;s.reserve(fr);Timer w;w.start();
        for(int i=0;i<fr;i++){
            long long key=(rng_next()%seeded)+1;
            char sql[256];snprintf(sql,sizeof(sql),
                "SELECT * FROM BENCH_POINT WHERE LABEL = 'label%lld';",key);
            long long c=0;char*err=nullptr;Timer t;t.start();
            flexql_exec(db,sql,cb_count,&c,&err);s.record(t.elapsed_us());if(err)flexql_free(err);
        }
        s.set_wall(w.elapsed_ms());print_latency("FullScan VARCHAR",s);
    }

    /* ── O(1) scaling test ────────────────────────────────────────────── */
    print_section("Scaling: PK lookup should be flat across row counts");
    static const long long SIZES[]={10000,50000,100000,250000};
    for(long long sz:SIZES){
        create_standard_table(db,"BENCH_SCALE",true);
        seed_standard_table(db,"BENCH_SCALE",sz,500,false,20000.0);
        LatencyStats s;s.reserve(100);Timer w;w.start();
        for(int i=0;i<100;i++){
            long long key=(rng_next()%sz)+1;
            char sql[256];snprintf(sql,sizeof(sql),"SELECT * FROM BENCH_SCALE WHERE ID = %lld;",key);
            long long c=0;char*err=nullptr;Timer t;t.start();
            flexql_exec(db,sql,cb_count,&c,&err);s.record(t.elapsed_us());if(err)flexql_free(err);
        }
        s.set_wall(w.elapsed_ms());
        char label[64];snprintf(label,sizeof(label),"PK rows=%-8lld",sz);
        print_latency(label,s);
        fql_exec_ignore(db,"DROP TABLE BENCH_SCALE;");
    }

    fql_exec_ignore(db,"DROP TABLE BENCH_POINT;");
    flexql_close(db);
    printf("\n  Done.\n");
    return 0;
}