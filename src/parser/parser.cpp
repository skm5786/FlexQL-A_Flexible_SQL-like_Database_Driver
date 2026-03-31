/**
 * parser.cpp  —  SQL Recursive Descent Parser  (Lesson 2B — Full Implementation)
 */
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include <cstdarg>
#include "parser/parser.h"
#include "parser/lexer.h"

/* ── Helpers ─────────────────────────────────────────────────────────────── */
static void set_err(char **errmsg, const char *msg) {
    if (!errmsg) return;
    free(*errmsg); *errmsg = strdup(msg);
}
static void set_err_fmt(char **errmsg, const char *fmt, ...) {
    if (!errmsg) return;
    char buf[512]; va_list ap;
    va_start(ap, fmt); vsnprintf(buf, sizeof(buf), fmt, ap); va_end(ap);
    free(*errmsg); *errmsg = strdup(buf);
}
static int is_type_kw(TokenType t) {
    return t==KW_INT||t==KW_DECIMAL_T||t==KW_VARCHAR||t==KW_TEXT||t==KW_DATETIME;
}
static ColumnType coltype_from_tok(TokenType t) {
    switch(t){
    case KW_INT:       return COL_TYPE_INT;
    case KW_DECIMAL_T: return COL_TYPE_DECIMAL;
    case KW_VARCHAR: case KW_TEXT: return COL_TYPE_VARCHAR;
    case KW_DATETIME:  return COL_TYPE_DATETIME;
    default:           return COL_TYPE_UNKNOWN;
    }
}
static int tok_to_op(TokenType t, CompareOp *op){
    switch(t){
    case TOK_EQ:  *op=OP_EQ;  return 0;
    case TOK_NEQ: *op=OP_NEQ; return 0;
    case TOK_LT:  *op=OP_LT;  return 0;
    case TOK_GT:  *op=OP_GT;  return 0;
    case TOK_LTE: *op=OP_LTE; return 0;
    case TOK_GTE: *op=OP_GTE; return 0;
    default: return -1;
    }
}

/* ── Forward declarations ─────────────────────────────────────────────────── */
static int parse_create(Lexer*,QueryNode*,char**);
static int parse_insert(Lexer*,QueryNode*,char**);
static int parse_select(Lexer*,QueryNode*,char**);
static int parse_where(Lexer*,WhereClause*,char**);
static int parse_col_def(Lexer*,ColumnDef*,int,char**);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * TOP-LEVEL ENTRY POINT
 * LESSON: Peek at first keyword, dispatch to the right parse function.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int parser_parse(const char *sql, QueryNode *out, char **errmsg) {
    if (!sql||!out){ set_err(errmsg,"NULL argument"); return -1; }
    if (errmsg) *errmsg=nullptr;
    memset(out,0,sizeof(QueryNode));
    out->type=QUERY_UNKNOWN;

    Lexer lx; lexer_init(&lx,sql);
    Token first=lexer_peek(&lx);
    switch(first.type){
    case KW_CREATE: {
        /* Could be CREATE TABLE or CREATE DATABASE — peek second token */
        lexer_next(&lx);  /* consume CREATE */
        Token second = lexer_peek(&lx);
        if (second.type == KW_DATABASE) {
            /* CREATE DATABASE name; */
            lexer_next(&lx);  /* consume DATABASE */
            out->type = QUERY_CREATE_DB;
            Token nm; if(lexer_expect(&lx,TOK_IDENT,&nm,errmsg)!=0) return -1;
            strncpy(out->params.db.db_name, nm.text, FLEXQL_MAX_NAME_LEN-1);
            if(lexer_peek(&lx).type==PUNCT_SEMI) lexer_next(&lx);
            return 0;
        }
        /* Put CREATE back by re-initialising is messy — instead we
           re-parse from scratch with a fresh lexer but set a flag.
           Simplest fix: re-init the lexer and call parse_create.         */
        lexer_init(&lx, sql);
        return parse_create(&lx, out, errmsg);
    }
    case KW_USE: {
        /* USE db_name; */
        lexer_next(&lx);  /* consume USE */
        out->type = QUERY_USE_DB;
        Token nm; if(lexer_expect(&lx,TOK_IDENT,&nm,errmsg)!=0) return -1;
        strncpy(out->params.db.db_name, nm.text, FLEXQL_MAX_NAME_LEN-1);
        if(lexer_peek(&lx).type==PUNCT_SEMI) lexer_next(&lx);
        return 0;
    }
    case KW_SHOW: {
        lexer_next(&lx);  /* consume SHOW */
        Token what = lexer_next(&lx);
        if (what.type == KW_DATABASES) {
            out->type = QUERY_SHOW_DBS;
        } else if (what.type == KW_TABLES) {
            out->type = QUERY_SHOW_TABLES;
        } else {
            set_err_fmt(errmsg,"Expected DATABASES or TABLES after SHOW, got '%s'",what.text);
            return -1;
        }
        if(lexer_peek(&lx).type==PUNCT_SEMI) lexer_next(&lx);
        return 0;
    }
    case KW_DROP: {
        /* DROP DATABASE name; */
        lexer_next(&lx);  /* consume DROP */
        if(lexer_expect(&lx,KW_DATABASE,nullptr,errmsg)!=0) return -1;
        out->type = QUERY_DROP_DB;
        Token nm; if(lexer_expect(&lx,TOK_IDENT,&nm,errmsg)!=0) return -1;
        strncpy(out->params.db.db_name, nm.text, FLEXQL_MAX_NAME_LEN-1);
        if(lexer_peek(&lx).type==PUNCT_SEMI) lexer_next(&lx);
        return 0;
    }
    case KW_INSERT: return parse_insert(&lx,out,errmsg);
    case KW_SELECT: return parse_select(&lx,out,errmsg);
    case TOK_EOF:   set_err(errmsg,"Empty SQL statement"); return -1;
    default:
        set_err_fmt(errmsg,"Unknown statement starting with '%s' at line %d",
                    first.text,first.line);
        return -1;
    }
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * parse_col_def
 * Grammar: IDENT type_kw (PRIMARY KEY)? (NOT NULL)? (VARCHAR(n))?
 *
 * LESSON: After the type keyword we loop peeking for optional modifiers.
 * Peeking never consumes, so missing modifiers don't cause errors.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static int parse_col_def(Lexer *lx, ColumnDef *col, int idx, char **errmsg){
    memset(col,0,sizeof(ColumnDef));
    col->col_index=idx;
    col->constraints=COL_CONSTRAINT_NONE;

    Token nm; if(lexer_expect(lx,TOK_IDENT,&nm,errmsg)!=0) return -1;
    strncpy(col->name,nm.text,FLEXQL_MAX_NAME_LEN-1);

    Token ty=lexer_next(lx);
    if(!is_type_kw(ty.type)){
        set_err_fmt(errmsg,"Expected type keyword after '%s', got '%s'",
                    col->name,ty.text);
        return -1;
    }
    col->type=coltype_from_tok(ty.type);

    /* VARCHAR(n) optional size */
    if((ty.type==KW_VARCHAR||ty.type==KW_TEXT)&&
        lexer_peek(lx).type==PUNCT_LPAREN){
        lexer_next(lx);
        Token sz=lexer_next(lx);
        col->varchar_max_len=(sz.type==TOK_INTEGER)?atoi(sz.text):0;
        if(lexer_peek(lx).type==PUNCT_RPAREN) lexer_next(lx);
    }

    /* Optional constraints in any order */
    for(;;){
        Token nx=lexer_peek(lx);
        if(nx.type==KW_PRIMARY){
            lexer_next(lx);
            if(lexer_peek(lx).type==KW_KEY) lexer_next(lx);
            col->constraints|=COL_CONSTRAINT_PRIMARY_KEY;
        } else if(nx.type==KW_NOT){
            lexer_next(lx);
            if(lexer_peek(lx).type==KW_NULL) lexer_next(lx);
            col->constraints|=COL_CONSTRAINT_NOT_NULL;
        } else break;
    }
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * parse_create
 * Grammar: CREATE TABLE ident '(' col_def (',' col_def)* ')' ';'
 *
 * LESSON: Comma-separated list pattern: parse first item, then loop on
 * COMMA until no more commas.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static int parse_create(Lexer *lx, QueryNode *out, char **errmsg){
    out->type=QUERY_CREATE_TABLE;
    CreateTableParams *p=&out->params.create;

    lexer_next(lx); /* consume CREATE */
    if(lexer_expect(lx,KW_TABLE,nullptr,errmsg)!=0) return -1;
    Token tn; if(lexer_expect(lx,TOK_IDENT,&tn,errmsg)!=0) return -1;
    strncpy(p->table_name,tn.text,FLEXQL_MAX_NAME_LEN-1);
    if(lexer_expect(lx,PUNCT_LPAREN,nullptr,errmsg)!=0) return -1;

    p->col_count=0;
    if(parse_col_def(lx,&p->columns[p->col_count++],0,errmsg)!=0) return -1;
    while(lexer_peek(lx).type==PUNCT_COMMA){
        lexer_next(lx);
        if(p->col_count>=FLEXQL_MAX_COLUMNS){
            set_err(errmsg,"Too many columns"); return -1;
        }
        if(parse_col_def(lx,&p->columns[p->col_count],p->col_count,errmsg)!=0)
            return -1;
        p->col_count++;
    }
    if(lexer_expect(lx,PUNCT_RPAREN,nullptr,errmsg)!=0) return -1;
    if(lexer_peek(lx).type==PUNCT_SEMI) lexer_next(lx);
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * parse_insert
 * Grammar (Lesson 7 — Batch INSERT):
 *   INSERT INTO ident VALUES tuple (',' tuple)* ';'
 *   tuple ::= '(' literal (',' literal)* ')'
 *
 * LESSON 7 — HOW BATCH INSERT WORKS IN THE PARSER:
 *
 *   Old grammar (single tuple):
 *     INSERT INTO users VALUES (1, 'Alice', 95.0);
 *
 *   New grammar (multiple tuples, separated by commas):
 *     INSERT INTO users VALUES (1,'Alice',95.0), (2,'Bob',88.0), (3,'Carol',72.0);
 *
 *   The extension is minimal — after parsing the FIRST closing ')' we
 *   peek at the next token:
 *     COMMA  → another tuple follows → parse it → peek again
 *     SEMI   → done
 *
 *   Row 0 is stored in p->values[][] (the existing inline array).
 *   Rows 1..N-1 are stored in p->extra_rows (heap-allocated on demand).
 *
 *   The executor receives batch_row_count and iterates over all rows.
 *   It frees extra_rows after all inserts succeed or any fails.
 *
 * WHY THIS IS THE RIGHT PLACE FOR THE CHANGE:
 *   The parser is the ONLY place the SQL text is read.  The executor and
 *   storage engine don't touch SQL strings — they work on QueryNode fields.
 *   Adding multi-tuple parsing here means zero changes to the storage layer.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static int parse_one_tuple(Lexer *lx, InsertParams *p, int row_idx, char **errmsg) {
    /* row_idx==0: store into p->values[]
       row_idx> 0: store into p->extra_rows[row_idx-1][]               */
    if (lexer_expect(lx, PUNCT_LPAREN, nullptr, errmsg) != 0) return -1;

    int col = 0;
    for (;;) {
        if (col >= FLEXQL_MAX_COLUMNS) {
            set_err(errmsg, "Too many values in INSERT tuple"); return -1;
        }
        Token v = lexer_next(lx);
        if (v.type == TOK_INTEGER || v.type == TOK_DECIMAL ||
            v.type == TOK_STRING  || v.type == TOK_IDENT  || v.type == KW_NULL) {
            char *dst;
            if (row_idx == 0) {
                dst = p->values[col];
            } else {
                dst = p->extra_rows[row_idx - 1][col];
            }
            if (v.type == KW_NULL) dst[0] = '\0';
            else strncpy(dst, v.text, FLEXQL_MAX_VARCHAR - 1);
        } else {
            set_err_fmt(errmsg, "Expected value in INSERT VALUES, got '%s'", v.text);
            return -1;
        }
        col++;
        Token nx = lexer_peek(lx);
        if      (nx.type == PUNCT_COMMA)  lexer_next(lx);
        else if (nx.type == PUNCT_RPAREN) { lexer_next(lx); break; }
        else {
            set_err_fmt(errmsg, "Expected ',' or ')' in VALUES tuple, got '%s'", nx.text);
            return -1;
        }
    }
    /* All rows must have the same column count */
    if (row_idx == 0) {
        p->value_count = col;
    } else if (col != p->value_count) {
        set_err_fmt(errmsg,
            "Batch INSERT tuple %d has %d values, expected %d",
            row_idx + 1, col, p->value_count);
        return -1;
    }
    return 0;
}

static int parse_insert(Lexer *lx, QueryNode *out, char **errmsg) {
    out->type = QUERY_INSERT;
    InsertParams *p = &out->params.insert;
    p->value_count     = 0;
    p->batch_row_count = 0;
    p->extra_capacity  = 0;
    p->expiry          = 0;
    p->extra_rows      = nullptr;

    lexer_next(lx); /* consume INSERT */
    if (lexer_expect(lx, KW_INTO,   nullptr, errmsg) != 0) return -1;
    Token tn;
    if (lexer_expect(lx, TOK_IDENT, &tn,     errmsg) != 0) return -1;
    strncpy(p->table_name, tn.text, FLEXQL_MAX_NAME_LEN - 1);
    if (lexer_expect(lx, KW_VALUES, nullptr, errmsg) != 0) return -1;

    /* Parse row 0 (always inline — zero extra allocation) */
    if (parse_one_tuple(lx, p, 0, errmsg) != 0) return -1;
    p->batch_row_count = 1;

    /* Peek for more tuples: ',' tuple (',' tuple)*
     * This is the ONLY grammar change for batch INSERT.                   */
    while (lexer_peek(lx).type == PUNCT_COMMA) {
        /* Check next-next token: if '(' it's another tuple, else stop     */
        lexer_next(lx); /* consume ',' */
        if (lexer_peek(lx).type != PUNCT_LPAREN) break; /* trailing comma before ';' */

        if (p->batch_row_count >= FLEXQL_MAX_BATCH_ROWS) {
            set_err_fmt(errmsg, "Batch INSERT exceeds maximum of %d rows",
                        FLEXQL_MAX_BATCH_ROWS);
            return -1;
        }

        /* ── Geometric growth (like std::vector) ──────────────────────────
         * OLD: realloc by 1 slot each time → O(n²) total memcpy
         *   batch=1000 → ~7.8 GB of memcpy (the measured bottleneck!)
         *
         * NEW: double capacity when full → O(n) total amortised memcpy
         *   Start at 16 slots, double to 32, 64, 128, 256, 512, 1024...
         *   batch=1000 → allocates 1024 slots, ~17 MB copied total
         *
         * extra_capacity tracks how many slots are allocated.
         * extra_used = batch_row_count - 1 (rows 1..N-1).              */
        int extra_used = p->batch_row_count - 1;
        if (p->extra_rows == nullptr || extra_used >= p->extra_capacity) {
            int new_cap = (p->extra_capacity == 0) ? 16
                        : (p->extra_capacity * 2);
            if (new_cap > FLEXQL_MAX_BATCH_ROWS) new_cap = FLEXQL_MAX_BATCH_ROWS;
            char (*new_extra)[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR] =
                (char(*)[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR])
                realloc(p->extra_rows,
                        (size_t)new_cap * sizeof(*p->extra_rows));
            if (!new_extra) {
                set_err(errmsg, "Out of memory for batch INSERT rows");
                return -1;
            }
            p->extra_rows    = new_extra;
            p->extra_capacity = new_cap;
        }
        /* Zero-init the new slot */
        memset(p->extra_rows[extra_used], 0, sizeof(*p->extra_rows));

        if (parse_one_tuple(lx, p, p->batch_row_count, errmsg) != 0) return -1;
        p->batch_row_count++;
    }

    if (lexer_peek(lx).type == PUNCT_SEMI) lexer_next(lx);
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * parse_where
 * Grammar: WHERE ident op literal    (single condition only)
 *
 * LESSON: Optional production — if next token isn't WHERE, return OK
 * with has_condition=0. The caller doesn't need to check separately.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static int parse_where(Lexer *lx, WhereClause *wh, char **errmsg){
    memset(wh,0,sizeof(WhereClause));
    if(lexer_peek(lx).type!=KW_WHERE){ wh->has_condition=0; return 0; }
    lexer_next(lx); /* consume WHERE */
    wh->has_condition=1;

    Token ct; if(lexer_expect(lx,TOK_IDENT,&ct,errmsg)!=0) return -1;
    /* Handle optional table.col qualifier */
    if(lexer_peek(lx).type==PUNCT_DOT){
        lexer_next(lx);
        Token c2; if(lexer_expect(lx,TOK_IDENT,&c2,errmsg)!=0) return -1;
        strncpy(wh->col_name,c2.text,FLEXQL_MAX_NAME_LEN-1);
    } else {
        strncpy(wh->col_name,ct.text,FLEXQL_MAX_NAME_LEN-1);
    }

    Token op=lexer_next(lx);
    if(tok_to_op(op.type,&wh->op)!=0){
        set_err_fmt(errmsg,"Expected operator in WHERE, got '%s'",op.text);
        return -1;
    }
    Token vt=lexer_next(lx);
    if(vt.type==KW_NULL) wh->value[0]='\0';
    else if(vt.type==TOK_INTEGER||vt.type==TOK_DECIMAL||
            vt.type==TOK_STRING ||vt.type==TOK_IDENT)
        strncpy(wh->value,vt.text,FLEXQL_MAX_VARCHAR-1);
    else{
        set_err_fmt(errmsg,"Expected value after WHERE operator, got '%s'",vt.text);
        return -1;
    }
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * parse_select
 * Grammar: SELECT list FROM ident (INNER JOIN ident ON q=q)? (WHERE cond)? ;
 *
 * LESSON: After FROM we peek to disambiguate: INNER=join, WHERE=filter,
 * SEMI/EOF=done. This is LL(1) parsing — one token of lookahead suffices.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static int parse_select(Lexer *lx, QueryNode *out, char **errmsg){
    out->type=QUERY_SELECT;
    SelectParams *p=&out->params.select;
    memset(p,0,sizeof(SelectParams));

    lexer_next(lx); /* consume SELECT */

    /* SELECT list */
    if(lexer_peek(lx).type==PUNCT_STAR){
        lexer_next(lx);
        p->select.select_all=1;
    } else {
        p->select.select_all=0;
        for(;;){
            Token c; if(lexer_expect(lx,TOK_IDENT,&c,errmsg)!=0) return -1;
            char cn[FLEXQL_MAX_NAME_LEN]={};
            if(lexer_peek(lx).type==PUNCT_DOT){
                lexer_next(lx);
                Token c2; if(lexer_expect(lx,TOK_IDENT,&c2,errmsg)!=0) return -1;
                snprintf(cn,FLEXQL_MAX_NAME_LEN,"%s.%s",c.text,c2.text);
            } else strncpy(cn,c.text,FLEXQL_MAX_NAME_LEN-1);
            if(p->select.col_count>=FLEXQL_MAX_COLUMNS){
                set_err(errmsg,"Too many columns in SELECT"); return -1;
            }
            strncpy(p->select.col_names[p->select.col_count++],cn,FLEXQL_MAX_NAME_LEN-1);
            if(lexer_peek(lx).type==PUNCT_COMMA) lexer_next(lx);
            else break;
        }
    }

    /* FROM */
    if(lexer_expect(lx,KW_FROM,nullptr,errmsg)!=0) return -1;
    Token tn; if(lexer_expect(lx,TOK_IDENT,&tn,errmsg)!=0) return -1;
    strncpy(p->table_name,tn.text,FLEXQL_MAX_NAME_LEN-1);

    /* Optional INNER JOIN */
    if(lexer_peek(lx).type==KW_INNER){
        out->type=QUERY_INNER_JOIN;
        p->is_join=1;
        lexer_next(lx); /* INNER */
        if(lexer_expect(lx,KW_JOIN,nullptr,errmsg)!=0) return -1;
        Token jt; if(lexer_expect(lx,TOK_IDENT,&jt,errmsg)!=0) return -1;
        strncpy(p->join_table,jt.text,FLEXQL_MAX_NAME_LEN-1);
        if(lexer_expect(lx,KW_ON,nullptr,errmsg)!=0) return -1;
        /* Parse tableA.col = tableB.col */
        Token ta,ca,tb,cb;
        if(lexer_expect(lx,TOK_IDENT,&ta,errmsg)!=0) return -1;
        if(lexer_expect(lx,PUNCT_DOT,nullptr,errmsg)!=0) return -1;
        if(lexer_expect(lx,TOK_IDENT,&ca,errmsg)!=0) return -1;
        if(lexer_expect(lx,TOK_EQ,nullptr,errmsg)!=0) return -1;
        if(lexer_expect(lx,TOK_IDENT,&tb,errmsg)!=0) return -1;
        if(lexer_expect(lx,PUNCT_DOT,nullptr,errmsg)!=0) return -1;
        if(lexer_expect(lx,TOK_IDENT,&cb,errmsg)!=0) return -1;
        strncpy(p->join_col_a,ca.text,FLEXQL_MAX_NAME_LEN-1);
        strncpy(p->join_col_b,cb.text,FLEXQL_MAX_NAME_LEN-1);
        (void)ta;(void)tb;
    }

    /* Optional WHERE */
    if(parse_where(lx,&p->where,errmsg)!=0) return -1;

    /* Trailing semicolon */
    if(lexer_peek(lx).type==PUNCT_SEMI) lexer_next(lx);
    return 0;
}

/* ── Public utility functions ───────────────────────────────────────────── */
ColumnType column_type_from_string(const char *s){
    if(!s) return COL_TYPE_UNKNOWN;
    if(strcmp(s,"INT")==0||strcmp(s,"INTEGER")==0) return COL_TYPE_INT;
    if(strcmp(s,"DECIMAL")==0||strcmp(s,"FLOAT")==0||
       strcmp(s,"DOUBLE")==0) return COL_TYPE_DECIMAL;
    if(strcmp(s,"VARCHAR")==0||strcmp(s,"TEXT")==0) return COL_TYPE_VARCHAR;
    if(strcmp(s,"DATETIME")==0) return COL_TYPE_DATETIME;
    return COL_TYPE_UNKNOWN;
}
int compare_op_from_string(const char *s, CompareOp *op){
    if(!s||!op) return -1;
    if(strcmp(s,"=")==0){*op=OP_EQ; return 0;}
    if(strcmp(s,"!=")==0){*op=OP_NEQ; return 0;}
    if(strcmp(s,"<")==0){*op=OP_LT; return 0;}
    if(strcmp(s,">")==0){*op=OP_GT; return 0;}
    if(strcmp(s,"<=")==0){*op=OP_LTE; return 0;}
    if(strcmp(s,">=")==0){*op=OP_GTE; return 0;}
    return -1;
}