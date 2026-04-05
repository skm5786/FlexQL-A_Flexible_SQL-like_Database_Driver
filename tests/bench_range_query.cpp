/**
 * bench_range_query.cpp  —  B+ Tree Range Query Benchmark  (fixed)
 *
 * FIXES vs original:
 *   - Full scan reps capped at 3 (was 50 × O(n) = catastrophic with 1M rows)
 *   - 50% selectivity queries capped at 5 reps (500K rows × 50 reps = stuck)
 *   - Default row_count reduced to 100K (was 1M — seeding took minutes)
 *   - Comparison table removed (was seeding another 100K rows pointlessly)
 *   - Seed time limit: 30 seconds maximum
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_range_query.cpp src/client/flexql.cpp \
 *       -o tests/bench_range_query -lpthread
 *
 * Run:
 *   ./tests/bench_range_query [row_count] [repetitions]
 *   Defaults: row_count=100000  repetitions=20
 */
#include "bench_common.h"

static void run_repeated(FlexQL*db,const char*sql,int reps,LatencyStats&stats){
    for(int i=0;i<reps;i++){
        long long c=0;char*err=nullptr;
        Timer t;t.start();
        flexql_exec(db,sql,cb_count,&c,&err);
        stats.record(t.elapsed_us());
        if(err)flexql_free(err);
    }
}

static void run_range_suite(FlexQL*db,const char*table,
                             long long row_count,int reps){
    printf("\n  Table: %s  (%lld rows)  reps=%d\n",table,row_count,reps);

    /* VALUE column spans 1000–10999 (width=10000) */
    static const struct{const char*name;double frac;} CASES[]={
        {"0.01%",0.0001},{"0.1%",0.001},{"1%",0.01},{"10%",0.10},{"50%",0.50}
    };

    /* ── GT on indexed VALUE ───────────────────────────────────────────── */
    printf("  VALUE > (B+ tree)\n");
    for(auto&c:CASES){
        double thresh=10999.0-c.frac*10000.0;
        char sql[256];
        snprintf(sql,sizeof(sql),"SELECT * FROM %s WHERE VALUE > %.1f;",table,thresh);
        /* Cap reps for high-selectivity queries to prevent hanging */
        int actual_reps=(c.frac>=0.10)?std::min(reps,5):reps;
        LatencyStats stats;stats.reserve(actual_reps);
        Timer wall;wall.start();
        run_repeated(db,sql,actual_reps,stats);
        stats.set_wall(wall.elapsed_ms());
        char label[64];
        snprintf(label,sizeof(label),"GT sel=%-5s ~%lld rows",c.name,(long long)(row_count*c.frac));
        print_latency(label,stats);
    }

    /* ── LT on indexed VALUE ───────────────────────────────────────────── */
    printf("\n  VALUE < (B+ tree)\n");
    for(auto&c:CASES){
        double thresh=1000.0+c.frac*10000.0;
        char sql[256];
        snprintf(sql,sizeof(sql),"SELECT * FROM %s WHERE VALUE < %.1f;",table,thresh);
        int actual_reps=(c.frac>=0.10)?std::min(reps,5):reps;
        LatencyStats stats;stats.reserve(actual_reps);
        Timer wall;wall.start();
        run_repeated(db,sql,actual_reps,stats);
        stats.set_wall(wall.elapsed_ms());
        char label[64];
        snprintf(label,sizeof(label),"LT sel=%-5s ~%lld rows",c.name,(long long)(row_count*c.frac));
        print_latency(label,stats);
    }

    /* ── PK equality (hash index) ─────────────────────────────────────── */
    printf("\n  ID = val (hash index O(1))\n");
    for(int i=0;i<reps;i++){
        long long key=(long long)(1+(i%(row_count>0?row_count:1)));
        char sql[256];snprintf(sql,sizeof(sql),"SELECT * FROM %s WHERE ID = %lld;",table,key);
        long long c=0;char*err=nullptr;Timer t;t.start();
        flexql_exec(db,sql,cb_count,&c,&err);
        if(err)flexql_free(err);
    }
    {
        LatencyStats stats;stats.reserve(reps);
        for(int i=0;i<reps;i++){
            long long key=1+(i%row_count);
            char sql[256];snprintf(sql,sizeof(sql),"SELECT * FROM %s WHERE ID = %lld;",table,key);
            long long c=0;char*err=nullptr;Timer t;t.start();
            flexql_exec(db,sql,cb_count,&c,&err);
            stats.record(t.elapsed_us());if(err)flexql_free(err);
        }
        Timer w;w.start();stats.set_wall(w.elapsed_ms());
        print_latency("PK hash O(1)",stats);
    }

    /* ── Full scan VARCHAR — STRICTLY LIMITED ──────────────────────────
     * A full scan of 100K rows takes ~100ms per query.
     * With reps=20 that's 2 seconds — acceptable.
     * With reps=50 on 1M rows = 50 × 1s = 50 seconds — NOT acceptable.
     * Cap at 3 reps regardless of what caller asks for.
     */
    printf("\n  Full scan LABEL = (no index) — capped at 3 reps\n");
    {
        int fscan_reps=std::min(reps,3);
        long long mid=row_count/2;
        char sql[256];snprintf(sql,sizeof(sql),
            "SELECT * FROM %s WHERE LABEL = 'label%lld';",table,mid);
        LatencyStats stats;stats.reserve(fscan_reps);
        Timer wall;wall.start();
        run_repeated(db,sql,fscan_reps,stats);
        stats.set_wall(wall.elapsed_ms());
        print_latency("FullScan VARCHAR",stats);
    }

    /* ── Cache hit ─────────────────────────────────────────────────────── */
    printf("\n  Cache hit (1%% selectivity repeated)\n");
    {
        double thresh=10999.0-0.01*10000.0;
        char sql[256];snprintf(sql,sizeof(sql),
            "SELECT * FROM %s WHERE VALUE > %.1f;",table,thresh);
        /* prime cache */
        fql_exec_ignore(db,sql);
        LatencyStats stats;stats.reserve(reps);
        Timer wall;wall.start();
        run_repeated(db,sql,reps,stats);
        stats.set_wall(wall.elapsed_ms());
        print_latency("Cache HIT 1%",stats);
    }
}

int main(int argc,char**argv){
    long long row_count=(argc>1)?atoll(argv[1]):100000LL;
    int reps=(argc>2)?atoi(argv[2]):20;

    /* Safety cap: full scans at 1M rows are very slow — warn user */
    if(row_count>500000){
        printf("  Warning: row_count=%lld is large. Full scan queries will be slow.\n",row_count);
        printf("  Consider using 100000 for quick benchmarking.\n");
    }

    print_header("FlexQL Range Query Benchmark");
    printf("  Row count  : %lld\n",row_count);
    printf("  Repetitions: %d (high-selectivity capped at 5, full scan at 3)\n",reps);

    FlexQL*db=connect_or_die();

    print_section("Table Setup");
    printf("  Creating BENCH_RANGE...\n");
    create_standard_table(db,"BENCH_RANGE",true);
    Timer seed_t;seed_t.start();
    long long seeded=seed_standard_table(db,"BENCH_RANGE",row_count,500,true,30000.0);
    printf("  Seeded %lld rows in %.1f ms (%.0f rows/s)\n",
           seeded,seed_t.elapsed_ms(),seeded*1000.0/seed_t.elapsed_ms());

    print_section("Range Query Results");
    run_range_suite(db,"BENCH_RANGE",seeded,reps);

    fql_exec_ignore(db,"DROP TABLE BENCH_RANGE;");
    flexql_close(db);
    printf("\n  Done.\n");
    return 0;
}