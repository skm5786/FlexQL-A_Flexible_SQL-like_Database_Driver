/**
 * test_storage.cpp  —  Unit Tests for the Lesson 3 Storage Engine
 *
 * Tests every function in storage.h directly, without a server or network.
 *
 * Build:
 *   g++ -std=c++17 -I./include \
 *       tests/test_storage.cpp \
 *       src/storage/storage.cpp \
 *       src/storage/dbmanager.cpp \
 *       -lpthread -o bin/test_storage
 *
 * Run:
 *   ./bin/test_storage
 *
 * TEST GROUPS:
 *   A. string_to_cell  — INT, DECIMAL, VARCHAR, DATETIME, NULL
 *   B. cell_to_string  — all types → string rendering
 *   C. cell_matches_where — WHERE evaluation per type
 *   D. table_create    — schema, duplicates, case folding
 *   E. table_find      — case-insensitive lookup
 *   F. row_insert      — type enforcement, NOT NULL, PK duplicate, count mismatch
 *   G. table_scan      — full scan, WHERE, expiration, early stop
 *   H. row_free_contents — memory management
 *   I. dbmanager       — create, find, drop, list
 *   J. Integration     — multi-table scenario, expired PK reuse
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include "common/types.h"
#include "storage/storage.h"
#include "storage/dbmanager.h"

/* ── Test harness ─────────────────────────────────────────────────────────── */
static int tests_run = 0, tests_passed = 0;
#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS  %s\n", msg); } \
    else      {                 printf("  FAIL  %s\n", msg); } \
} while(0)

/* ── Test fixtures ────────────────────────────────────────────────────────── */
static Database *make_db(const char *name) {
    Database *db = (Database*)calloc(1, sizeof(Database));
    strncpy(db->name, name, FLEXQL_MAX_NAME_LEN-1);
    pthread_mutex_init(&db->schema_lock, nullptr);
    return db;
}
static void free_db(Database *db) {
    for (int i = 0; i < db->table_count; i++) table_free(db->tables[i]);
    pthread_mutex_destroy(&db->schema_lock);
    free(db);
}
static ColumnDef make_col(const char *name, ColumnType type,
                           uint8_t constraints = COL_CONSTRAINT_NONE) {
    ColumnDef c{}; strncpy(c.name, name, FLEXQL_MAX_NAME_LEN-1);
    c.type = type; c.constraints = constraints; return c;
}
static int insert_row(Table *t, time_t expiry, char **errmsg,
                      const char *v0, const char *v1="", const char *v2="",
                      const char *v3="", const char *v4="") {
    char vals[5][FLEXQL_MAX_VARCHAR]={};
    const char *s[5]={v0,v1,v2,v3,v4};
    for(int i=0;i<t->col_count&&i<5;i++) strncpy(vals[i],s[i],FLEXQL_MAX_VARCHAR-1);
    return row_insert(t, vals, t->col_count, expiry, errmsg);
}
static int count_cb(const Row*, void *arg){ (*(int*)arg)++; return 0; }
static int count_rows(Table *t){ int n=0; table_scan(t,nullptr,count_cb,&n); return n; }
static WhereClause make_where(const char *col, CompareOp op, const char *val) {
    WhereClause w{}; w.has_condition=1; w.op=op;
    strncpy(w.col_name,col,FLEXQL_MAX_NAME_LEN-1);
    strncpy(w.value,val,FLEXQL_MAX_VARCHAR-1); return w;
}
struct FirstVal { int col; char buf[256]; int found; };
static int first_cb(const Row *row, void *arg){
    FirstVal *fv=(FirstVal*)arg; char tmp[256];
    cell_to_string(&row->cells[fv->col],tmp,sizeof(tmp));
    strncpy(fv->buf,tmp,sizeof(fv->buf)-1); fv->found=1; return 1;
}
/* Shared 4-row student table used by many scan tests */
static Table *make_student_table(Database *db){
    char *err=nullptr;
    ColumnDef cols[4];
    cols[0]=make_col("ID",COL_TYPE_INT,COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL);
    cols[1]=make_col("FIRST_NAME",COL_TYPE_VARCHAR,COL_CONSTRAINT_NOT_NULL);
    cols[2]=make_col("GPA",COL_TYPE_DECIMAL,COL_CONSTRAINT_NONE);
    cols[3]=make_col("JOINED",COL_TYPE_DATETIME,COL_CONSTRAINT_NONE);
    Table *t=table_create(db,"students",cols,4,&err);
    insert_row(t,0,&err,"1","Alice","3.9","2024-01-10");
    insert_row(t,0,&err,"2","Bob","3.2","2024-02-15");
    insert_row(t,0,&err,"3","Carol","3.7","2024-03-01");
    insert_row(t,0,&err,"4","Dave","2.8","2024-04-20");
    return t;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP A — string_to_cell
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_string_to_cell_int(){
    printf("\n[A] string_to_cell — INT\n");
    CellValue cv; char *err=nullptr;
    CHECK(string_to_cell("42",  COL_TYPE_INT,&cv,&err)==0, "parse '42'");
    CHECK(cv.data.int_val==42,                              "'42' = 42");
    CHECK(string_to_cell("-7",  COL_TYPE_INT,&cv,&err)==0, "parse '-7'");
    CHECK(cv.data.int_val==-7,                              "'-7' = -7");
    CHECK(string_to_cell("0",   COL_TYPE_INT,&cv,&err)==0, "parse '0'");
    CHECK(cv.data.int_val==0,                               "'0' = 0");
    CHECK(string_to_cell("",    COL_TYPE_INT,&cv,&err)==0, "empty = NULL");
    CHECK(cv.is_null==1,                                    "empty is_null=1");
    CHECK(string_to_cell("abc", COL_TYPE_INT,&cv,&err)==-1,"'abc' fails INT");
    free(err); err=nullptr;
    CHECK(string_to_cell("1.5", COL_TYPE_INT,&cv,&err)==-1,"'1.5' fails INT");
    free(err); err=nullptr;
    /* KEY LESSON: 9 < 10 numerically — must NOT compare as strings */
    string_to_cell("9",COL_TYPE_INT,&cv,&err);
    WhereClause w=make_where("id",OP_LT,"10");
    CHECK(cell_matches_where(&cv,&w),"9 < 10 numeric (not lexicographic '9'>'10')");
}
static void test_string_to_cell_decimal(){
    printf("\n[A] string_to_cell — DECIMAL\n");
    CellValue cv; char *err=nullptr;
    CHECK(string_to_cell("3.14",COL_TYPE_DECIMAL,&cv,&err)==0,"parse '3.14'");
    CHECK(cv.data.decimal_val>3.13&&cv.data.decimal_val<3.15,"3.14 in range");
    CHECK(string_to_cell("-0.5",COL_TYPE_DECIMAL,&cv,&err)==0,"parse '-0.5'");
    CHECK(cv.data.decimal_val<0,                              "-0.5 is negative");
    CHECK(string_to_cell("100", COL_TYPE_DECIMAL,&cv,&err)==0,"integer '100' valid for DECIMAL");
    CHECK(cv.data.decimal_val==100.0,                         "'100' = 100.0");
    CHECK(string_to_cell("xyz", COL_TYPE_DECIMAL,&cv,&err)==-1,"'xyz' fails DECIMAL");
    free(err); err=nullptr;
}
static void test_string_to_cell_varchar(){
    printf("\n[A] string_to_cell — VARCHAR\n");
    CellValue cv; char *err=nullptr;
    CHECK(string_to_cell("Hello World",COL_TYPE_VARCHAR,&cv,&err)==0,"parse string");
    CHECK(cv.data.varchar_val!=nullptr,                              "varchar_val not null");
    CHECK(strcmp(cv.data.varchar_val,"Hello World")==0,              "value matches");
    free(cv.data.varchar_val);
    CHECK(string_to_cell("",COL_TYPE_VARCHAR,&cv,&err)==0, "empty = NULL VARCHAR");
    CHECK(cv.is_null==1,                                   "is_null=1 for empty VARCHAR");
}
static void test_string_to_cell_datetime(){
    printf("\n[A] string_to_cell — DATETIME\n");
    CellValue cv; char *err=nullptr;
    CHECK(string_to_cell("1700000000",COL_TYPE_DATETIME,&cv,&err)==0,"Unix timestamp");
    CHECK(cv.data.datetime_val==1700000000,                          "timestamp value correct");
    CHECK(string_to_cell("2024-01-15",COL_TYPE_DATETIME,&cv,&err)==0,"ISO date YYYY-MM-DD");
    CHECK(cv.data.datetime_val>0,                                    "ISO date produces positive ts");
    CHECK(string_to_cell("2024-06-01 12:00:00",COL_TYPE_DATETIME,&cv,&err)==0,"ISO datetime");
    CHECK(cv.data.datetime_val>0,                                    "ISO datetime > 0");
    CHECK(string_to_cell("not-a-date",COL_TYPE_DATETIME,&cv,&err)==-1,"bad format fails");
    free(err); err=nullptr;
}
static void test_string_to_cell_null_all_types(){
    printf("\n[A] string_to_cell — empty string = NULL for all types\n");
    CellValue cv; char *err=nullptr;
    ColumnType types[]={COL_TYPE_INT,COL_TYPE_DECIMAL,COL_TYPE_VARCHAR,COL_TYPE_DATETIME};
    const char *labels[]={"INT","DECIMAL","VARCHAR","DATETIME"};
    for(int i=0;i<4;i++){
        string_to_cell("",types[i],&cv,&err);
        char msg[64]; snprintf(msg,sizeof(msg),"empty='NULL' for %s",labels[i]);
        CHECK(cv.is_null==1,msg);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP B — cell_to_string
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_cell_to_string(){
    printf("\n[B] cell_to_string — all types\n");
    CellValue cv; char buf[128]; char *err=nullptr;

    string_to_cell("99",  COL_TYPE_INT,     &cv,&err);
    cell_to_string(&cv,buf,sizeof(buf));
    CHECK(strcmp(buf,"99")==0,   "INT 99 → '99'");

    string_to_cell("2.5", COL_TYPE_DECIMAL, &cv,&err);
    cell_to_string(&cv,buf,sizeof(buf));
    CHECK(strcmp(buf,"2.5")==0,  "DECIMAL 2.5 → '2.5'");

    string_to_cell("Alice",COL_TYPE_VARCHAR,&cv,&err);
    cell_to_string(&cv,buf,sizeof(buf));
    CHECK(strcmp(buf,"Alice")==0,"VARCHAR 'Alice' → 'Alice'");
    free(cv.data.varchar_val);

    memset(&cv,0,sizeof(cv)); cv.is_null=1;
    cell_to_string(&cv,buf,sizeof(buf));
    CHECK(strcmp(buf,"NULL")==0, "NULL cell → 'NULL'");

    string_to_cell("1700000000",COL_TYPE_DATETIME,&cv,&err);
    cell_to_string(&cv,buf,sizeof(buf));
    CHECK(strlen(buf)==19,"DATETIME → YYYY-MM-DD HH:MM:SS (19 chars)");
    printf("    DATETIME rendered: %s\n",buf);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP C — cell_matches_where
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_cell_matches_where_int(){
    printf("\n[C] cell_matches_where — INT\n");
    CellValue cv; char *err=nullptr;
    string_to_cell("10",COL_TYPE_INT,&cv,&err);
    WhereClause w;
    w=make_where("id",OP_EQ, "10"); CHECK( cell_matches_where(&cv,&w),"10=10");
    w=make_where("id",OP_EQ, "5");  CHECK(!cell_matches_where(&cv,&w),"10≠5");
    w=make_where("id",OP_NEQ,"5");  CHECK( cell_matches_where(&cv,&w),"10!=5");
    w=make_where("id",OP_LT, "20"); CHECK( cell_matches_where(&cv,&w),"10<20");
    w=make_where("id",OP_GT, "5");  CHECK( cell_matches_where(&cv,&w),"10>5");
    w=make_where("id",OP_LTE,"10"); CHECK( cell_matches_where(&cv,&w),"10<=10");
    w=make_where("id",OP_GTE,"10"); CHECK( cell_matches_where(&cv,&w),"10>=10");
    w=make_where("id",OP_GT, "10"); CHECK(!cell_matches_where(&cv,&w),"10>10 false");
}
static void test_cell_matches_where_varchar(){
    printf("\n[C] cell_matches_where — VARCHAR\n");
    CellValue cv; char *err=nullptr;
    string_to_cell("Bob",COL_TYPE_VARCHAR,&cv,&err);
    WhereClause w;
    w=make_where("name",OP_EQ, "Bob");   CHECK( cell_matches_where(&cv,&w),"'Bob'='Bob'");
    w=make_where("name",OP_EQ, "Alice"); CHECK(!cell_matches_where(&cv,&w),"'Bob'≠'Alice'");
    w=make_where("name",OP_NEQ,"Alice"); CHECK( cell_matches_where(&cv,&w),"'Bob'!='Alice'");
    w=make_where("name",OP_GT, "Alice"); CHECK( cell_matches_where(&cv,&w),"'Bob'>'Alice'");
    w=make_where("name",OP_LT, "Carol"); CHECK( cell_matches_where(&cv,&w),"'Bob'<'Carol'");
    free(cv.data.varchar_val);
}
static void test_cell_matches_where_null(){
    printf("\n[C] cell_matches_where — NULL\n");
    CellValue cv{}; cv.is_null=1; cv.type=COL_TYPE_INT;
    WhereClause w=make_where("id",OP_EQ,"0");
    CHECK(!cell_matches_where(&cv,&w), "NULL cell never matches WHERE condition");
    WhereClause nw{}; nw.has_condition=0;
    CHECK( cell_matches_where(&cv,&nw),"no condition matches everything including NULL");
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP D — table_create
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_table_create(){
    printf("\n[D] table_create\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef cols[3];
    cols[0]=make_col("id",   COL_TYPE_INT,    COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL);
    cols[1]=make_col("name", COL_TYPE_VARCHAR,COL_CONSTRAINT_NOT_NULL);
    cols[2]=make_col("score",COL_TYPE_DECIMAL,COL_CONSTRAINT_NONE);
    Table *t=table_create(db,"students",cols,3,&err);
    CHECK(t!=nullptr,                    "table_create returns non-null");
    CHECK(err==nullptr,                  "no error on first create");
    CHECK(db->table_count==1,            "db->table_count=1");
    CHECK(strcmp(t->name,"STUDENTS")==0, "name uppercased to STUDENTS");
    CHECK(t->col_count==3,               "3 columns");
    CHECK(t->pk_col==0,                  "pk_col=0 (ID)");
    CHECK(strcmp(t->schema[0].name,"ID")==0,"col[0]=ID");
    CHECK(t->schema[0].type==COL_TYPE_INT,"col[0] type=INT");
    CHECK(t->schema[1].type==COL_TYPE_VARCHAR,"col[1] type=VARCHAR");
    CHECK(t->schema[2].type==COL_TYPE_DECIMAL,"col[2] type=DECIMAL");
    /* Duplicate */
    Table *t2=table_create(db,"STUDENTS",cols,3,&err);
    CHECK(t2==nullptr,  "duplicate table returns NULL");
    CHECK(err!=nullptr, "error message set for duplicate");
    printf("    Duplicate error: %s\n",err); free(err); err=nullptr;
    /* Case-insensitive duplicate */
    Table *t3=table_create(db,"students",cols,3,&err);
    CHECK(t3==nullptr,"lowercase duplicate also rejected");
    free(err); err=nullptr;
    free_db(db);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP E — table_find
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_table_find(){
    printf("\n[E] table_find\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef col=make_col("id",COL_TYPE_INT,COL_CONSTRAINT_PRIMARY_KEY);
    table_create(db,"orders",&col,1,&err);
    CHECK(table_find(db,"ORDERS")!=nullptr, "find 'ORDERS' uppercase");
    CHECK(table_find(db,"orders")!=nullptr, "find 'orders' lowercase");
    CHECK(table_find(db,"Orders")!=nullptr, "find 'Orders' mixed case");
    CHECK(table_find(db,"nonexistent")==nullptr,"find nonexistent returns NULL");
    CHECK(table_find(db,nullptr)==nullptr,      "find NULL name returns NULL");
    free_db(db);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP F — row_insert
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_row_insert_basic(){
    printf("\n[F] row_insert — basic insertion\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef cols[3];
    cols[0]=make_col("ID",  COL_TYPE_INT,    COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL);
    cols[1]=make_col("NAME",COL_TYPE_VARCHAR,COL_CONSTRAINT_NOT_NULL);
    cols[2]=make_col("GPA", COL_TYPE_DECIMAL,COL_CONSTRAINT_NONE);
    Table *t=table_create(db,"students",cols,3,&err);
    CHECK(insert_row(t,0,&err,"1","Alice","3.9")==0,"insert row 1 OK");
    CHECK(insert_row(t,0,&err,"2","Bob",  "3.2")==0,"insert row 2 OK");
    CHECK(insert_row(t,0,&err,"3","Carol","3.7")==0,"insert row 3 OK");
    CHECK(t->row_count==3,  "row_count=3");
    CHECK(count_rows(t)==3, "scan returns 3 rows");
    /* Prepend order: last inserted = head */
    char buf[64]; cell_to_string(&t->head->cells[0],buf,sizeof(buf));
    CHECK(strcmp(buf,"3")==0,"head row has ID=3 (last inserted, prepend order)");
    free_db(db);
}
static void test_row_insert_type_enforcement(){
    printf("\n[F] row_insert — type enforcement\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef cols[2];
    cols[0]=make_col("ID",   COL_TYPE_INT,    COL_CONSTRAINT_NOT_NULL);
    cols[1]=make_col("PRICE",COL_TYPE_DECIMAL,COL_CONSTRAINT_NOT_NULL);
    Table *t=table_create(db,"products",cols,2,&err);
    CHECK(insert_row(t,0,&err,"abc","9.99")==-1,"INT 'abc' rejected");
    CHECK(err!=nullptr,"error msg set for bad INT");
    printf("    Error: %s\n",err); free(err); err=nullptr;
    CHECK(insert_row(t,0,&err,"1","notanumber")==-1,"DECIMAL 'notanumber' rejected");
    free(err); err=nullptr;
    CHECK(t->row_count==0,"row_count still 0 after type failures");
    CHECK(insert_row(t,0,&err,"1","9.99")==0,"valid insert works after rejections");
    CHECK(t->row_count==1,"row_count=1 after valid insert");
    free_db(db);
}
static void test_row_insert_not_null(){
    printf("\n[F] row_insert — NOT NULL constraint\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef cols[2];
    cols[0]=make_col("ID",  COL_TYPE_INT,    COL_CONSTRAINT_NOT_NULL);
    cols[1]=make_col("NAME",COL_TYPE_VARCHAR,COL_CONSTRAINT_NOT_NULL|COL_CONSTRAINT_PRIMARY_KEY);
    Table *t=table_create(db,"users",cols,2,&err);
    CHECK(insert_row(t,0,&err,"1","")==-1,"NULL in NOT NULL VARCHAR rejected");
    CHECK(err!=nullptr,"error msg set"); printf("    Error: %s\n",err); free(err); err=nullptr;
    CHECK(insert_row(t,0,&err,"","Alice")==-1,"NULL in NOT NULL INT rejected");
    free(err); err=nullptr;
    CHECK(t->row_count==0,"no rows inserted on NOT NULL violation");
    CHECK(insert_row(t,0,&err,"1","Alice")==0,"valid row inserted");
    CHECK(t->row_count==1,"row_count=1");
    free_db(db);
}
static void test_row_insert_pk_duplicate(){
    printf("\n[F] row_insert — PRIMARY KEY duplicate\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef cols[2];
    cols[0]=make_col("ID",  COL_TYPE_INT,    COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL);
    cols[1]=make_col("NAME",COL_TYPE_VARCHAR,COL_CONSTRAINT_NONE);
    Table *t=table_create(db,"emp",cols,2,&err);
    CHECK(insert_row(t,0,&err,"10","Alice")==0,"PK=10 first insert OK");
    CHECK(insert_row(t,0,&err,"20","Bob")  ==0,"PK=20 OK");
    CHECK(insert_row(t,0,&err,"10","Carol")==-1,"PK=10 duplicate rejected");
    CHECK(err!=nullptr,"error msg for duplicate PK");
    printf("    Error: %s\n",err); free(err); err=nullptr;
    CHECK(t->row_count==2,"row_count stays 2");
    CHECK(insert_row(t,0,&err,"30","Dave")==0,"new PK=30 accepted");
    CHECK(t->row_count==3,"row_count=3");
    free_db(db);
}
static void test_row_insert_count_mismatch(){
    printf("\n[F] row_insert — column count mismatch\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef cols[3];
    cols[0]=make_col("A",COL_TYPE_INT,    COL_CONSTRAINT_NONE);
    cols[1]=make_col("B",COL_TYPE_VARCHAR,COL_CONSTRAINT_NONE);
    cols[2]=make_col("C",COL_TYPE_DECIMAL,COL_CONSTRAINT_NONE);
    Table *t=table_create(db,"t",cols,3,&err);
    /* Only 2 values for 3-column table */
    char vals[2][FLEXQL_MAX_VARCHAR]={"1","hello"};
    CHECK(row_insert(t,vals,2,0,&err)==-1,"too few values rejected");
    CHECK(err!=nullptr,"error msg set"); printf("    Error: %s\n",err); free(err); err=nullptr;
    free_db(db);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP G — table_scan
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_table_scan_no_where(){
    printf("\n[G] table_scan — full scan no WHERE\n");
    Database *db=make_db("testdb");
    Table *t=make_student_table(db);
    CHECK(count_rows(t)==4,  "scan returns all 4 rows");
    CHECK(t->row_count==4,   "t->row_count=4");
    free_db(db);
}
static void test_table_scan_where_int(){
    printf("\n[G] table_scan — WHERE on INT\n");
    Database *db=make_db("testdb");
    Table *t=make_student_table(db);
    WhereClause w; int count;
    w=make_where("ID",OP_EQ, "2"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==1,"WHERE ID=2 → 1 row");
    w=make_where("ID",OP_GT, "2"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==2,"WHERE ID>2 → 2 rows");
    w=make_where("ID",OP_GTE,"2"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==3,"WHERE ID>=2 → 3 rows");
    w=make_where("ID",OP_LT, "3"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==2,"WHERE ID<3 → 2 rows");
    w=make_where("ID",OP_NEQ,"1"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==3,"WHERE ID!=1 → 3 rows");
    w=make_where("ID",OP_EQ,"99"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==0,"WHERE ID=99 → 0 rows");
    free_db(db);
}
static void test_table_scan_where_varchar(){
    printf("\n[G] table_scan — WHERE on VARCHAR\n");
    Database *db=make_db("testdb");
    Table *t=make_student_table(db);
    WhereClause w; int count;
    w=make_where("FIRST_NAME",OP_EQ,"Bob"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==1,"WHERE FIRST_NAME='Bob' → 1 row");
    FirstVal fv; fv.col=0; fv.found=0;
    table_scan(t,&w,first_cb,&fv);
    CHECK(fv.found==1,          "found Bob");
    CHECK(strcmp(fv.buf,"2")==0,"Bob ID=2");
    w=make_where("FIRST_NAME",OP_EQ,"Zara"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==0,"WHERE FIRST_NAME='Zara' → 0 rows");
    free_db(db);
}
static void test_table_scan_where_decimal(){
    printf("\n[G] table_scan — WHERE on DECIMAL\n");
    Database *db=make_db("testdb");
    Table *t=make_student_table(db);
    WhereClause w; int count;
    /* Alice(3.9) and Carol(3.7) qualify */
    w=make_where("GPA",OP_GTE,"3.7"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==2,"WHERE GPA>=3.7 → 2 rows");
    /* Dave(2.8) qualifies */
    w=make_where("GPA",OP_LT,"3.0"); count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==1,"WHERE GPA<3.0 → 1 row (Dave)");
    free_db(db);
}
static void test_table_scan_expiration(){
    printf("\n[G] table_scan — expiration / soft delete\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef col=make_col("ID",COL_TYPE_INT,COL_CONSTRAINT_NONE);
    Table *t=table_create(db,"sessions",&col,1,&err);
    time_t now=time(nullptr);
    char v[1][FLEXQL_MAX_VARCHAR];
    strncpy(v[0],"1",FLEXQL_MAX_VARCHAR-1); row_insert(t,v,1,now-100,&err); /* expired   */
    strncpy(v[0],"2",FLEXQL_MAX_VARCHAR-1); row_insert(t,v,1,now+100,&err); /* not yet   */
    strncpy(v[0],"3",FLEXQL_MAX_VARCHAR-1); row_insert(t,v,1,0,      &err); /* permanent */
    strncpy(v[0],"4",FLEXQL_MAX_VARCHAR-1); row_insert(t,v,1,now-100,&err); /* expired   */
    CHECK(count_rows(t)==2, "only 2 non-expired rows visible out of 4 inserted");
    CHECK(t->row_count==4,  "t->row_count=4 (physical, unchanged)");
    free_db(db);
}
static void test_table_scan_unknown_col(){
    printf("\n[G] table_scan — unknown WHERE column\n");
    Database *db=make_db("testdb");
    Table *t=make_student_table(db);
    WhereClause w=make_where("NONEXISTENT",OP_EQ,"1");
    int count=0; table_scan(t,&w,count_cb,&count);
    CHECK(count==0,"WHERE on unknown column returns 0 rows (no crash)");
    free_db(db);
}
static void test_table_scan_early_stop(){
    printf("\n[G] table_scan — early stop\n");
    Database *db=make_db("testdb");
    Table *t=make_student_table(db); /* 4 rows */
    struct StopCtx{ int visited; int stop_after; };
    auto stop_cb=[](const Row*,void *arg)->int{
        StopCtx *ctx=(StopCtx*)arg; ctx->visited++;
        return (ctx->visited>=ctx->stop_after)?1:0;
    };
    StopCtx ctx; ctx.visited=0; ctx.stop_after=2;
    table_scan(t,nullptr,stop_cb,&ctx);
    CHECK(ctx.visited==2,"scan stopped after 2 rows when callback returned 1");
    free_db(db);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP H — row_free_contents
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_row_free_contents(){
    printf("\n[H] row_free_contents\n");
    CellValue *cells=(CellValue*)calloc(2,sizeof(CellValue));
    cells[0].type=COL_TYPE_INT; cells[0].data.int_val=42;
    cells[1].type=COL_TYPE_VARCHAR; cells[1].data.varchar_val=strdup("test");
    Row row{}; row.cells=cells; row.col_count=2;
    row_free_contents(&row);
    CHECK(row.cells==nullptr,"cells nulled after free");
    row_free_contents(&row); /* double-free must not crash */
    CHECK(row.cells==nullptr,"double-free safe");
    row_free_contents(nullptr); /* NULL must not crash */
    CHECK(true,"row_free_contents(nullptr) does not crash");
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP I — DatabaseManager
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_dbmanager_create_find_list(){
    printf("\n[I] DatabaseManager — create, find, list\n");
    DatabaseManager mgr; char *err=nullptr;
    dbmgr_init(&mgr);
    CHECK(dbmgr_create(&mgr,"university",&err)==0,"create 'university'");
    CHECK(dbmgr_create(&mgr,"hospital",  &err)==0,"create 'hospital'");
    CHECK(mgr.db_count==2,"db_count=2");
    Database *db=dbmgr_find(&mgr,"UNIVERSITY");
    CHECK(db!=nullptr,                        "find 'UNIVERSITY' uppercase");
    CHECK(strcmp(db->name,"UNIVERSITY")==0,   "found db name=UNIVERSITY");
    CHECK(dbmgr_find(&mgr,"hospital")!=nullptr,"find 'hospital' lowercase");
    CHECK(dbmgr_find(&mgr,"doesnotexist")==nullptr,"find nonexistent=NULL");
    char names[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
    CHECK(dbmgr_list(&mgr,names)==2,"dbmgr_list returns 2");
    dbmgr_destroy(&mgr);
}
static void test_dbmanager_duplicate(){
    printf("\n[I] DatabaseManager — duplicate prevention\n");
    DatabaseManager mgr; char *err=nullptr;
    dbmgr_init(&mgr);
    dbmgr_create(&mgr,"mydb",&err);
    CHECK(dbmgr_create(&mgr,"mydb",&err)==-1,"duplicate rejected");
    CHECK(err!=nullptr,"error msg set"); printf("    Error: %s\n",err); free(err); err=nullptr;
    CHECK(dbmgr_create(&mgr,"MYDB",&err)==-1,"uppercase duplicate rejected");
    free(err); err=nullptr;
    CHECK(mgr.db_count==1,"db_count stays 1");
    dbmgr_destroy(&mgr);
}
static void test_dbmanager_drop(){
    printf("\n[I] DatabaseManager — drop\n");
    DatabaseManager mgr; char *err=nullptr;
    dbmgr_init(&mgr);
    dbmgr_create(&mgr,"alpha",&err);
    dbmgr_create(&mgr,"beta", &err);
    dbmgr_create(&mgr,"gamma",&err);
    CHECK(dbmgr_drop(&mgr,"beta",&err)==0,           "drop 'beta' OK");
    CHECK(mgr.db_count==2,                           "db_count=2 after drop");
    CHECK(dbmgr_find(&mgr,"beta")==nullptr,          "'beta' gone");
    CHECK(dbmgr_find(&mgr,"alpha")!=nullptr,         "'alpha' still exists");
    CHECK(dbmgr_find(&mgr,"gamma")!=nullptr,         "'gamma' still exists");
    CHECK(dbmgr_drop(&mgr,"nonexistent",&err)==-1,   "drop nonexistent=-1");
    free(err); err=nullptr;
    dbmgr_drop(&mgr,"alpha",&err); dbmgr_drop(&mgr,"gamma",&err);
    CHECK(mgr.db_count==0,"db_count=0 after all dropped");
    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GROUP J — Integration
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_integration_multi_table(){
    printf("\n[J] Integration — multi-table scenario\n");
    DatabaseManager mgr; char *err=nullptr;
    dbmgr_init(&mgr);
    dbmgr_create(&mgr,"shop",&err);
    Database *db=dbmgr_find(&mgr,"shop");
    CHECK(db!=nullptr,"shop database found");

    /* PRODUCTS */
    ColumnDef pcols[2];
    pcols[0]=make_col("PRODUCT_ID",COL_TYPE_INT,COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL);
    pcols[1]=make_col("NAME",      COL_TYPE_VARCHAR,COL_CONSTRAINT_NOT_NULL);
    Table *products=table_create(db,"products",pcols,2,&err);
    insert_row(products,0,&err,"101","Laptop");
    insert_row(products,0,&err,"102","Mouse");
    insert_row(products,0,&err,"103","Keyboard");
    CHECK(products->row_count==3,"3 products inserted");

    /* ORDERS */
    ColumnDef ocols[3];
    ocols[0]=make_col("ORDER_ID",  COL_TYPE_INT,COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL);
    ocols[1]=make_col("PRODUCT_ID",COL_TYPE_INT,COL_CONSTRAINT_NOT_NULL);
    ocols[2]=make_col("QUANTITY",  COL_TYPE_INT,COL_CONSTRAINT_NOT_NULL);
    Table *orders=table_create(db,"orders",ocols,3,&err);
    insert_row(orders,0,&err,"1","101","2");
    insert_row(orders,0,&err,"2","102","5");
    insert_row(orders,0,&err,"3","101","1");
    CHECK(orders->row_count==3,"3 orders inserted");

    CHECK(db->table_count==2,"db has 2 tables");
    CHECK(table_find(db,"products")==products,"find 'products' correct");
    CHECK(table_find(db,"orders")  ==orders,  "find 'orders' correct");

    /* 2 orders for PRODUCT_ID=101 */
    WhereClause w=make_where("PRODUCT_ID",OP_EQ,"101"); int count=0;
    table_scan(orders,&w,count_cb,&count);
    CHECK(count==2,"2 orders for PRODUCT_ID=101");

    /* Mouse product */
    WhereClause w2=make_where("NAME",OP_EQ,"Mouse");
    FirstVal fv; fv.col=0; fv.found=0;
    table_scan(products,&w2,first_cb,&fv);
    CHECK(fv.found==1,            "found Mouse");
    CHECK(strcmp(fv.buf,"102")==0,"Mouse PRODUCT_ID=102");

    dbmgr_destroy(&mgr);
    CHECK(true,"dbmgr_destroy completes without crash");
}
static void test_integration_expiry_pk_reuse(){
    printf("\n[J] Integration — expired row allows PK reuse\n");
    Database *db=make_db("testdb"); char *err=nullptr;
    ColumnDef col=make_col("ID",COL_TYPE_INT,COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL);
    Table *t=table_create(db,"sessions",&col,1,&err);
    time_t past=time(nullptr)-100;
    char v[1][FLEXQL_MAX_VARCHAR];
    strncpy(v[0],"1",FLEXQL_MAX_VARCHAR-1);
    row_insert(t,v,1,past,&err);        /* insert PK=1 as expired          */
    CHECK(t->row_count==1,"expired row physically present (row_count=1)");
    strncpy(v[0],"1",FLEXQL_MAX_VARCHAR-1);
    int rc=row_insert(t,v,1,0,&err);    /* insert PK=1 again (non-expiring) */
    CHECK(rc==0,          "PK=1 reused after original row expired");
    CHECK(t->row_count==2,"row_count=2 (expired+new)");
    CHECK(count_rows(t)==1,"only 1 non-expired row visible");
    free_db(db);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  MAIN
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int main(){
    printf("═══════════════════════════════════════════════════\n");
    printf(" FlexQL Storage Engine Test Suite  (Lesson 3)\n");
    printf("═══════════════════════════════════════════════════\n");

    test_string_to_cell_int();
    test_string_to_cell_decimal();
    test_string_to_cell_varchar();
    test_string_to_cell_datetime();
    test_string_to_cell_null_all_types();
    test_cell_to_string();
    test_cell_matches_where_int();
    test_cell_matches_where_varchar();
    test_cell_matches_where_null();
    test_table_create();
    test_table_find();
    test_row_insert_basic();
    test_row_insert_type_enforcement();
    test_row_insert_not_null();
    test_row_insert_pk_duplicate();
    test_row_insert_count_mismatch();
    test_table_scan_no_where();
    test_table_scan_where_int();
    test_table_scan_where_varchar();
    test_table_scan_where_decimal();
    test_table_scan_expiration();
    test_table_scan_unknown_col();
    test_table_scan_early_stop();
    test_row_free_contents();
    test_dbmanager_create_find_list();
    test_dbmanager_duplicate();
    test_dbmanager_drop();
    test_integration_multi_table();
    test_integration_expiry_pk_reuse();

    printf("\n═══════════════════════════════════════════════════\n");
    printf(" Results: %d / %d tests passed\n",tests_passed,tests_run);
    printf("═══════════════════════════════════════════════════\n");
    return (tests_passed==tests_run)?0:1;
}