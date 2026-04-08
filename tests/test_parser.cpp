/**
 * test_parser.cpp  —  Unit Tests for the Lexer and Parser
 *
 * LESSON: Testing the parser in isolation (without the server) is the
 * correct approach. Each test:
 *   1. Calls parser_parse() with a known SQL string.
 *   2. Checks the returned QueryNode fields match expectations.
 *   3. Prints PASS or FAIL.
 *
 * Run:
 *   g++ -std=c++17 -I../include test_parser.cpp \
 *       ../src/parser/parser.cpp ../src/parser/lexer.cpp \
 *       -o test_parser && ./test_parser
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include "parser/parser.h"
#include "parser/lexer.h"

static int tests_run = 0, tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS  %s\n", msg); } \
    else      { printf("  FAIL  %s\n", msg); } \
} while(0)

/* ─────────────────────────────────────────────────────────── */
static void test_lexer_basic() {
    printf("\n[Lexer basic tokens]\n");
    Lexer lx; lexer_init(&lx, "SELECT * FROM student;");
    Token t;
    t = lexer_next(&lx); CHECK(t.type==KW_SELECT,  "SELECT keyword");
    t = lexer_next(&lx); CHECK(t.type==PUNCT_STAR,  "* punctuation");
    t = lexer_next(&lx); CHECK(t.type==KW_FROM,     "FROM keyword");
    t = lexer_next(&lx); CHECK(t.type==TOK_IDENT && strcmp(t.text,"STUDENT")==0, "STUDENT ident uppercased");
    t = lexer_next(&lx); CHECK(t.type==PUNCT_SEMI,  "; semicolon");
    t = lexer_next(&lx); CHECK(t.type==TOK_EOF,     "EOF");
}

static void test_lexer_string() {
    printf("\n[Lexer string literals]\n");
    Lexer lx; lexer_init(&lx, "INSERT INTO t VALUES (1,'Alice',3.14);");
    lexer_next(&lx); lexer_next(&lx); lexer_next(&lx); /* INSERT INTO t */
    lexer_next(&lx); /* VALUES */
    lexer_next(&lx); /* ( */
    Token t;
    t = lexer_next(&lx); CHECK(t.type==TOK_INTEGER && strcmp(t.text,"1")==0, "integer 1");
    lexer_next(&lx); /* comma */
    t = lexer_next(&lx); CHECK(t.type==TOK_STRING && strcmp(t.text,"Alice")==0, "string 'Alice' without quotes");
    lexer_next(&lx); /* comma */
    t = lexer_next(&lx); CHECK(t.type==TOK_DECIMAL && strcmp(t.text,"3.14")==0, "decimal 3.14");
}

static void test_lexer_operators() {
    printf("\n[Lexer operators]\n");
    Lexer lx; lexer_init(&lx, "= != < > <= >=");
    Token t;
    t = lexer_next(&lx); CHECK(t.type==TOK_EQ,  "=");
    t = lexer_next(&lx); CHECK(t.type==TOK_NEQ, "!=");
    t = lexer_next(&lx); CHECK(t.type==TOK_LT,  "<");
    t = lexer_next(&lx); CHECK(t.type==TOK_GT,  ">");
    t = lexer_next(&lx); CHECK(t.type==TOK_LTE, "<=");
    t = lexer_next(&lx); CHECK(t.type==TOK_GTE, ">=");
}

static void test_parse_create() {
    printf("\n[Parser: CREATE TABLE]\n");
    QueryNode q; char *err = nullptr;
    int rc = parser_parse(
        "CREATE TABLE STUDENT("
        "  ID INT PRIMARY KEY NOT NULL,"
        "  FIRST_NAME TEXT NOT NULL,"
        "  LAST_NAME  TEXT NOT NULL,"
        "  EMAIL      TEXT"
        ");", &q, &err);
    CHECK(rc==0, "parse returns 0");
    CHECK(q.type==QUERY_CREATE_TABLE, "type is QUERY_CREATE_TABLE");
    CHECK(strcmp(q.params.create.table_name,"STUDENT")==0, "table name STUDENT");
    CHECK(q.params.create.col_count==4, "4 columns");
    CHECK(strcmp(q.params.create.columns[0].name,"ID")==0, "col[0] = ID");
    CHECK(q.params.create.columns[0].type==COL_TYPE_INT, "col[0] type INT");
    CHECK(q.params.create.columns[0].constraints & COL_CONSTRAINT_PRIMARY_KEY,
          "col[0] PRIMARY KEY");
    CHECK(q.params.create.columns[0].constraints & COL_CONSTRAINT_NOT_NULL,
          "col[0] NOT NULL");
    CHECK(q.params.create.columns[1].type==COL_TYPE_VARCHAR, "col[1] type VARCHAR(TEXT)");
    free(err);
}

static void test_parse_insert() {
    printf("\n[Parser: INSERT INTO]\n");
    QueryNode q; char *err = nullptr;
    int rc = parser_parse(
        "INSERT INTO STUDENT VALUES (1, 'John', 'Doe', 'john@example.com');",
        &q, &err);
    CHECK(rc==0, "parse returns 0");
    CHECK(q.type==QUERY_INSERT, "type is QUERY_INSERT");
    CHECK(strcmp(q.params.insert.table_name,"STUDENT")==0, "table name STUDENT");
    CHECK(q.params.insert.value_count==4, "4 values");
    CHECK(strcmp(q.params.insert.values[0],"1")==0, "values[0]=1");
    CHECK(strcmp(q.params.insert.values[1],"John")==0, "values[1]=John");
    CHECK(strcmp(q.params.insert.values[3],"john@example.com")==0, "values[3] email");
    free(err);
}

static void test_parse_select_star() {
    printf("\n[Parser: SELECT *]\n");
    QueryNode q; char *err = nullptr;
    int rc = parser_parse("SELECT * FROM STUDENT;", &q, &err);
    CHECK(rc==0, "parse returns 0");
    CHECK(q.type==QUERY_SELECT, "type is QUERY_SELECT");
    CHECK(q.params.select.select.select_all==1, "select_all=1");
    CHECK(strcmp(q.params.select.table_name,"STUDENT")==0, "table STUDENT");
    CHECK(q.params.select.where.has_condition==0, "no WHERE clause");
    free(err);
}

static void test_parse_select_cols() {
    printf("\n[Parser: SELECT specific columns]\n");
    QueryNode q; char *err = nullptr;
    int rc = parser_parse("SELECT ID, FIRST_NAME FROM STUDENT;", &q, &err);
    CHECK(rc==0, "parse returns 0");
    CHECK(q.params.select.select.select_all==0, "select_all=0");
    CHECK(q.params.select.select.col_count==2, "2 columns");
    CHECK(strcmp(q.params.select.select.col_names[0],"ID")==0, "col[0]=ID");
    CHECK(strcmp(q.params.select.select.col_names[1],"FIRST_NAME")==0, "col[1]=FIRST_NAME");
    free(err);
}

static void test_parse_select_where() {
    printf("\n[Parser: SELECT with WHERE]\n");
    QueryNode q; char *err = nullptr;
    int rc = parser_parse("SELECT * FROM STUDENT WHERE ID = 5;", &q, &err);
    CHECK(rc==0, "parse returns 0");
    CHECK(q.params.select.where.has_condition==1, "has WHERE");
    CHECK(strcmp(q.params.select.where.col_name,"ID")==0, "WHERE col = ID");
    CHECK(q.params.select.where.op==OP_EQ, "WHERE op = EQ");
    CHECK(strcmp(q.params.select.where.value,"5")==0, "WHERE value = 5");
    free(err);
}

static void test_parse_inner_join() {
    printf("\n[Parser: INNER JOIN]\n");
    QueryNode q; char *err = nullptr;
    int rc = parser_parse(
        "SELECT * FROM ORDERS INNER JOIN CUSTOMERS "
        "ON ORDERS.CUSTOMER_ID = CUSTOMERS.ID;",
        &q, &err);
    CHECK(rc==0, "parse returns 0");
    CHECK(q.type==QUERY_INNER_JOIN, "type is QUERY_INNER_JOIN");
    CHECK(q.params.select.is_join==1, "is_join=1");
    CHECK(strcmp(q.params.select.table_name,"ORDERS")==0, "table=ORDERS");
    CHECK(strcmp(q.params.select.join_table,"CUSTOMERS")==0, "join_table=CUSTOMERS");
    CHECK(strcmp(q.params.select.join_col_a,"CUSTOMER_ID")==0, "join_col_a=CUSTOMER_ID");
    CHECK(strcmp(q.params.select.join_col_b,"ID")==0, "join_col_b=ID");
    free(err);
}

static void test_parse_error() {
    printf("\n[Parser: error handling]\n");
    QueryNode q; char *err = nullptr;
    int rc = parser_parse("FOOBAR TABLE xyz;", &q, &err);
    CHECK(rc==-1, "unknown statement returns -1");
    CHECK(err != nullptr, "error message allocated");
    printf("    Error msg: %s\n", err ? err : "(null)");
    free(err);

    err = nullptr;
    rc = parser_parse("SELECT * FROM;", &q, &err);
    CHECK(rc==-1, "missing table name returns -1");
    printf("    Error msg: %s\n", err ? err : "(null)");
    free(err);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * DATABASE-LEVEL COMMAND TESTS
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_parse_create_database() {
    printf("\n[Parser: CREATE DATABASE]\n");
    QueryNode q; char *err = nullptr;

    /* Basic: CREATE DATABASE mydb; */
    int rc = parser_parse("CREATE DATABASE mydb;", &q, &err);
    CHECK(rc == 0,                          "CREATE DATABASE parses OK");
    CHECK(q.type == QUERY_CREATE_DB,        "type is QUERY_CREATE_DB");
    CHECK(strcmp(q.params.db.db_name, "MYDB") == 0,
                                            "db_name uppercased to MYDB");
    free(err); err = nullptr;

    /* Without semicolon — should still work */
    rc = parser_parse("CREATE DATABASE university", &q, &err);
    CHECK(rc == 0,                          "CREATE DATABASE without ';' parses OK");
    CHECK(q.type == QUERY_CREATE_DB,        "type is QUERY_CREATE_DB (no semicolon)");
    CHECK(strcmp(q.params.db.db_name, "UNIVERSITY") == 0,
                                            "db_name = UNIVERSITY");
    free(err); err = nullptr;

    /* Mixed case — name must be uppercased */
    rc = parser_parse("CREATE DATABASE MyTestDB;", &q, &err);
    CHECK(rc == 0,                          "mixed case name parses OK");
    CHECK(strcmp(q.params.db.db_name, "MYTESTDB") == 0,
                                            "mixed case name uppercased to MYTESTDB");
    free(err); err = nullptr;

    /* CREATE TABLE must NOT be confused with CREATE DATABASE */
    rc = parser_parse("CREATE TABLE t (id INT);", &q, &err);
    CHECK(rc == 0,                          "CREATE TABLE still parses after CREATE DATABASE support");
    CHECK(q.type == QUERY_CREATE_TABLE,     "type is still QUERY_CREATE_TABLE");
    free(err); err = nullptr;

    /* Error: CREATE DATABASE with no name */
    rc = parser_parse("CREATE DATABASE;", &q, &err);
    CHECK(rc == -1,                         "CREATE DATABASE with no name returns -1");
    CHECK(err != nullptr,                   "error message set for missing db name");
    printf("    Error msg: %s\n", err ? err : "(null)");
    free(err); err = nullptr;
}

static void test_parse_use_database() {
    printf("\n[Parser: USE]\n");
    QueryNode q; char *err = nullptr;

    /* Basic: USE mydb; */
    int rc = parser_parse("USE mydb;", &q, &err);
    CHECK(rc == 0,                          "USE parses OK");
    CHECK(q.type == QUERY_USE_DB,           "type is QUERY_USE_DB");
    CHECK(strcmp(q.params.db.db_name, "MYDB") == 0,
                                            "db_name uppercased to MYDB");
    free(err); err = nullptr;

    /* Without semicolon */
    rc = parser_parse("USE hospital", &q, &err);
    CHECK(rc == 0,                          "USE without ';' parses OK");
    CHECK(q.type == QUERY_USE_DB,           "type is QUERY_USE_DB (no semicolon)");
    CHECK(strcmp(q.params.db.db_name, "HOSPITAL") == 0,
                                            "db_name = HOSPITAL");
    free(err); err = nullptr;

    /* Mixed case */
    rc = parser_parse("USE UniversityDB;", &q, &err);
    CHECK(rc == 0,                          "USE with mixed case parses OK");
    CHECK(strcmp(q.params.db.db_name, "UNIVERSITYDB") == 0,
                                            "mixed case uppercased correctly");
    free(err); err = nullptr;

    /* Error: USE with no name */
    rc = parser_parse("USE;", &q, &err);
    CHECK(rc == -1,                         "USE with no name returns -1");
    CHECK(err != nullptr,                   "error message set for missing db name in USE");
    printf("    Error msg: %s\n", err ? err : "(null)");
    free(err); err = nullptr;
}

static void test_parse_show_databases() {
    printf("\n[Parser: SHOW DATABASES]\n");
    QueryNode q; char *err = nullptr;

    /* Basic */
    int rc = parser_parse("SHOW DATABASES;", &q, &err);
    CHECK(rc == 0,                          "SHOW DATABASES parses OK");
    CHECK(q.type == QUERY_SHOW_DBS,         "type is QUERY_SHOW_DBS");
    free(err); err = nullptr;

    /* Without semicolon */
    rc = parser_parse("SHOW DATABASES", &q, &err);
    CHECK(rc == 0,                          "SHOW DATABASES without ';' parses OK");
    CHECK(q.type == QUERY_SHOW_DBS,         "type is QUERY_SHOW_DBS (no semicolon)");
    free(err); err = nullptr;

    /* Lowercase */
    rc = parser_parse("show databases;", &q, &err);
    CHECK(rc == 0,                          "lowercase 'show databases' parses OK");
    CHECK(q.type == QUERY_SHOW_DBS,         "type is QUERY_SHOW_DBS (lowercase)");
    free(err); err = nullptr;
}

static void test_parse_show_tables() {
    printf("\n[Parser: SHOW TABLES]\n");
    QueryNode q; char *err = nullptr;

    /* Basic */
    int rc = parser_parse("SHOW TABLES;", &q, &err);
    CHECK(rc == 0,                          "SHOW TABLES parses OK");
    CHECK(q.type == QUERY_SHOW_TABLES,      "type is QUERY_SHOW_TABLES");
    free(err); err = nullptr;

    /* Without semicolon */
    rc = parser_parse("SHOW TABLES", &q, &err);
    CHECK(rc == 0,                          "SHOW TABLES without ';' parses OK");
    CHECK(q.type == QUERY_SHOW_TABLES,      "type is QUERY_SHOW_TABLES (no semicolon)");
    free(err); err = nullptr;

    /* Error: SHOW with unknown keyword */
    rc = parser_parse("SHOW INDEXES;", &q, &err);
    CHECK(rc == -1,                         "SHOW INDEXES returns -1 (unsupported)");
    CHECK(err != nullptr,                   "error message set for unsupported SHOW target");
    printf("    Error msg: %s\n", err ? err : "(null)");
    free(err); err = nullptr;
}

static void test_parse_drop_database() {
    printf("\n[Parser: DROP DATABASE]\n");
    QueryNode q; char *err = nullptr;

    /* Basic */
    int rc = parser_parse("DROP DATABASE mydb;", &q, &err);
    CHECK(rc == 0,                          "DROP DATABASE parses OK");
    CHECK(q.type == QUERY_DROP_DB,          "type is QUERY_DROP_DB");
    CHECK(strcmp(q.params.db.db_name, "MYDB") == 0,
                                            "db_name = MYDB");
    free(err); err = nullptr;

    /* Without semicolon */
    rc = parser_parse("DROP DATABASE university", &q, &err);
    CHECK(rc == 0,                          "DROP DATABASE without ';' parses OK");
    CHECK(q.type == QUERY_DROP_DB,          "type is QUERY_DROP_DB (no semicolon)");
    free(err); err = nullptr;

    /* Mixed case */
    rc = parser_parse("drop database TestDB;", &q, &err);
    CHECK(rc == 0,                          "lowercase drop database parses OK");
    CHECK(q.type == QUERY_DROP_DB,          "type is QUERY_DROP_DB (lowercase)");
    CHECK(strcmp(q.params.db.db_name, "TESTDB") == 0,
                                            "name uppercased to TESTDB");
    free(err); err = nullptr;

    /* Error: DROP DATABASE with no name */
    rc = parser_parse("DROP DATABASE;", &q, &err);
    CHECK(rc == -1,                         "DROP DATABASE with no name returns -1");
    CHECK(err != nullptr,                   "error message set for missing db name in DROP");
    printf("    Error msg: %s\n", err ? err : "(null)");
    free(err); err = nullptr;

    /* Error: DROP with no DATABASE keyword */
    rc = parser_parse("DROP mydb;", &q, &err);
    CHECK(rc == -1,                         "DROP without DATABASE keyword returns -1");
    printf("    Error msg: %s\n", err ? err : "(null)");
    free(err); err = nullptr;
}

static void test_db_commands_dont_break_table_commands() {
    printf("\n[Parser: DB commands coexist with table commands]\n");
    QueryNode q; char *err = nullptr;

    /* After all the new keywords, make sure existing table commands
       still parse correctly — regression test */
    int rc = parser_parse(
        "CREATE TABLE STUDENT(ID INT PRIMARY KEY NOT NULL, NAME TEXT);",
        &q, &err);
    CHECK(rc == 0,                          "CREATE TABLE still works");
    CHECK(q.type == QUERY_CREATE_TABLE,     "type is QUERY_CREATE_TABLE");
    CHECK(q.params.create.col_count == 2,   "2 columns parsed correctly");
    free(err); err = nullptr;

    rc = parser_parse("INSERT INTO STUDENT VALUES (1, 'Alice');", &q, &err);
    CHECK(rc == 0,                          "INSERT still works");
    CHECK(q.type == QUERY_INSERT,           "type is QUERY_INSERT");
    free(err); err = nullptr;

    rc = parser_parse("SELECT * FROM STUDENT WHERE ID = 1;", &q, &err);
    CHECK(rc == 0,                          "SELECT still works");
    CHECK(q.type == QUERY_SELECT,           "type is QUERY_SELECT");
    CHECK(q.params.select.where.has_condition == 1, "WHERE clause parsed");
    free(err); err = nullptr;

    rc = parser_parse("SHOW DATABASES;", &q, &err);
    CHECK(rc == 0,                          "SHOW DATABASES works alongside table commands");
    CHECK(q.type == QUERY_SHOW_DBS,         "type is QUERY_SHOW_DBS");
    free(err); err = nullptr;
}

int main() {
    printf("═══════════════════════════════════════════\n");
    printf(" FlexQL Parser Test Suite\n");
    printf("═══════════════════════════════════════════\n");

    /* ── Original tests ── */
    test_lexer_basic();
    test_lexer_string();
    test_lexer_operators();
    test_parse_create();
    test_parse_insert();
    test_parse_select_star();
    test_parse_select_cols();
    test_parse_select_where();
    test_parse_inner_join();
    test_parse_error();

    /* ── New database-level command tests ── */
    test_parse_create_database();
    test_parse_use_database();
    test_parse_show_databases();
    test_parse_show_tables();
    test_parse_drop_database();
    test_db_commands_dont_break_table_commands();

    printf("\n═══════════════════════════════════════════\n");
    printf(" Results: %d / %d tests passed\n", tests_passed, tests_run);
    printf("═══════════════════════════════════════════\n");
    return (tests_passed == tests_run) ? 0 : 1;
}