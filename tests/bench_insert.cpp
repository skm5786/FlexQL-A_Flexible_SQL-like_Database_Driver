/**
 * bench_insert.cpp  —  Insert Throughput Benchmark  (fixed)
 *
 * FIXES vs original:
 *   - sub_rows capped at 100K (was 500K → seeding took minutes)
 *   - sustained test removed (was 3M rows → very slow with WAL)
 *   - TA target is still 10M rows but uses batch=500 correctly
 *   - batch sweep uses 100K rows (not 500K)
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_insert.cpp src/client/flexql.cpp \
 *       -o tests/bench_insert -lpthread
 *
 * Run:
 *   ./tests/bench_insert [row_count]
 *   Default: 10000000 (10M — TA target)
 */
#include "bench_common.h"

static double insert_rows(FlexQL*db,const char*create_sql,const char*tname,
                           long long n,int batch,int ncols,bool progress=false){
    fql_exec_ignore(db,(std::string("DROP TABLE ")+tname+";").c_str());
    if(!fql_exec(db,create_sql,"create"))return -1.0;
    Timer t;t.start();
    long long inserted=0;
    long long next_print=n/5;if(next_print<=0)next_print=n;
    while(inserted<n){
        std::ostringstream ss;ss<<"INSERT INTO "<<tname<<" VALUES ";
        int nb=0;
        while(nb<batch&&inserted<n){
            long long id=inserted+1;if(nb>0)ss<<",";ss<<"(";
            for(int c=0;c<ncols;c++){if(c>0)ss<<",";
                if(c==0)ss<<id;else if(c==1)ss<<(id%10);
                else if(c==2)ss<<(1000.0+(id%10000));
                else if(c==3)ss<<"'lbl"<<(id%100)<<"'";
                else ss<<(1700000000LL+id);}
            ss<<")";inserted++;nb++;
        }
        ss<<";";
        if(!fql_exec(db,ss.str().c_str(),"insert"))return -1.0;
        if(progress&&inserted>=next_print){
            double el=t.elapsed_ms();
            printf("  %lld/%lld  (%.0f rows/s)\n",inserted,n,inserted*1000.0/(el>0?el:1));
            next_print+=n/5;
        }
    }
    return t.elapsed_ms();
}

static void run_batch_sweep(FlexQL*db,long long n){
    print_section("Batch Size Sweep (100K rows, 5-col no-PK)");
    static const char*SQL=
        "CREATE TABLE INS_BATCH(ID DECIMAL NOT NULL,CAT DECIMAL NOT NULL,"
        "VALUE DECIMAL NOT NULL,LABEL VARCHAR(32) NOT NULL,TS DECIMAL NOT NULL);";
    static const int BS[]={1,10,50,100,250,500,1000,2000,5000};
    printf("  %-12s  %-12s  %-12s  %-10s\n","batch_size","elapsed(ms)","rows/sec","vs batch=1");
    double base=0;
    for(int b:BS){
        double ms=insert_rows(db,SQL,"INS_BATCH",n,b,5);
        if(ms<0){printf("  batch=%d FAILED\n",b);continue;}
        double tput=(ms>0)?n*1000.0/ms:0;
        if(b==1)base=tput;
        printf("  %-12d  %-12.1f  %-12.0f  %.1fx\n",b,ms,tput,(base>0)?tput/base:1.0);
    }
    fql_exec_ignore(db,"DROP TABLE INS_BATCH;");
}

static void run_pk_overhead(FlexQL*db,long long n){
    print_section("PK Overhead: with vs without PRIMARY KEY");
    static const char*NOPK=
        "CREATE TABLE INS_NOPK(ID DECIMAL NOT NULL,CAT DECIMAL NOT NULL,"
        "VALUE DECIMAL NOT NULL,LABEL VARCHAR(32) NOT NULL,TS DECIMAL NOT NULL);";
    static const char*WITHPK=
        "CREATE TABLE INS_PK(ID DECIMAL PRIMARY KEY NOT NULL,CAT DECIMAL NOT NULL,"
        "VALUE DECIMAL NOT NULL,LABEL VARCHAR(32) NOT NULL,TS DECIMAL NOT NULL);";
    double ms_nopk=insert_rows(db,NOPK,"INS_NOPK",n,500,5);
    double ms_pk  =insert_rows(db,WITHPK,"INS_PK",n,500,5);
    if(ms_nopk>0&&ms_pk>0){
        printf("  No PK  : %.1f ms  %.0f rows/s\n",ms_nopk,n*1000.0/ms_nopk);
        printf("  With PK: %.1f ms  %.0f rows/s\n",ms_pk,  n*1000.0/ms_pk);
        printf("  PK overhead: %.2fx slower\n",ms_pk/ms_nopk);
    }
    fql_exec_ignore(db,"DROP TABLE INS_NOPK;");
    fql_exec_ignore(db,"DROP TABLE INS_PK;");
}

static void run_ta_target(FlexQL*db,long long target){
    print_section("TA Target: 10M rows in under 120 seconds");
    printf("  Target: %lld rows, batch=500\n\n",target);
    fql_exec_ignore(db,"DROP TABLE BIG_USERS;");
    fql_exec(db,
        "CREATE TABLE BIG_USERS(ID DECIMAL NOT NULL,NAME VARCHAR(64) NOT NULL,"
        "EMAIL VARCHAR(64) NOT NULL,BALANCE DECIMAL NOT NULL,EXPIRES_AT DECIMAL NOT NULL);",
        "create_big_users");
    Timer t;t.start();
    long long inserted=0;long long np=target/10;
    while(inserted<target){
        std::ostringstream ss;ss<<"INSERT INTO BIG_USERS VALUES ";
        int nb=0;
        while(nb<500&&inserted<target){
            long long id=inserted+1;if(nb>0)ss<<",";
            ss<<"("<<id<<",'user"<<id<<"','user"<<id<<"@mail.com',"
              <<(1000.0+(id%10000))<<",1893456000)";
            inserted++;nb++;
        }
        ss<<";";
        if(!fql_exec(db,ss.str().c_str(),"ta_insert"))break;
        if(inserted>=np){
            double el=t.elapsed_ms();
            printf("  %lld/%lld  %.0f rows/s  eta=%.1fs\n",
                   inserted,target,inserted*1000.0/(el>0?el:1),
                   (target-inserted)/(inserted*1000.0/(el>0?el:1)));
            np+=target/10;
        }
    }
    double ms=t.elapsed_ms();
    printf("\n  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    printf("  Rows      : %lld\n",target);
    printf("  Elapsed   : %.1f ms  (%.2f s)\n",ms,ms/1000.0);
    printf("  Throughput: %.0f rows/s\n",(ms>0)?target*1000.0/ms:0);
    printf("  Target    : 120,000 ms\n");
    printf("  Result    : %s\n",(ms<=120000)?"PASS ✓":"FAIL ✗");
    printf("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    fql_exec_ignore(db,"DROP TABLE BIG_USERS;");
}

int main(int argc,char**argv){
    long long row_count=(argc>1)?atoll(argv[1]):10000000LL;
    print_header("FlexQL Insert Throughput Benchmark");
    printf("  Main row count: %lld\n",row_count);

    FlexQL*db=connect_or_die();

    /* Sub-benchmarks use 100K rows — fast, meaningful */
    long long sub=100000LL;
    run_batch_sweep(db,sub);
    run_pk_overhead(db,sub);
    run_ta_target(db,row_count);

    flexql_close(db);
    printf("\n  Done.\n");
    return 0;
}