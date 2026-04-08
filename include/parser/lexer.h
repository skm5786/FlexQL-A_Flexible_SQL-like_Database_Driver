/**
 * lexer.h  —  SQL Lexer / Tokeniser
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * LESSON 2A — WHAT IS A LEXER?
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * The LEXER (also called a tokeniser or scanner) is the FIRST phase of any
 * language processor.  Its job is to turn a raw character stream into a
 * sequence of meaningful TOKENS.
 *
 * Example:
 *   Input string:
 *     "SELECT id, name FROM student WHERE id = 5;"
 *
 *   Token stream produced by the lexer:
 *     KW_SELECT  │ "SELECT"
 *     IDENT      │ "id"
 *     COMMA      │ ","
 *     IDENT      │ "name"
 *     KW_FROM    │ "FROM"
 *     IDENT      │ "student"
 *     KW_WHERE   │ "WHERE"
 *     IDENT      │ "id"
 *     OP_EQ      │ "="
 *     INTEGER    │ "5"
 *     SEMICOLON  │ ";"
 *     TOK_EOF    │ ""
 *
 * WHY separate lexer from parser?
 *   The parser would be enormously complex if it had to deal with
 *   whitespace, case folding, quoted strings, and number formats.
 *   The lexer handles all of that so the parser only sees clean tokens.
 *
 * DESIGN DECISIONS:
 *   - SQL keywords are case-insensitive: SELECT = select = Select.
 *     The lexer uppercases everything so the parser only checks uppercase.
 *   - Quoted strings use single quotes: 'Alice'.  The lexer strips the
 *     quotes and returns the raw string content.
 *   - Numbers are returned as strings; the storage layer converts them
 *     to int64_t or double when inserting into a typed column.
 *   - The lexer is a single-pass, O(n) algorithm — it never goes back.
 */

#ifndef FLEXQL_LEXER_H
#define FLEXQL_LEXER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  TOKEN TYPES
 *
 *  LESSON: An enum gives a symbolic name to each token kind.
 *          We group them logically:
 *            TOK_*    — generic tokens (EOF, unknown)
 *            KW_*     — SQL keywords
 *            LIT_*    — literal values (numbers, strings)
 *            PUNCT_*  — punctuation  (, ; ( ) . *)
 *            OP_*     — comparison operators
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef enum {
    /* ── Special ────────────────────────────────────────────────────────── */
    TOK_EOF      = 0,   /* End of input — no more tokens                    */
    TOK_UNKNOWN  = 1,   /* Character the lexer doesn't recognise            */
    TOK_IDENT    = 2,   /* Identifier: table name, column name              */

    /* ── Literal values ──────────────────────────────────────────────────── */
    TOK_INTEGER  = 10,  /* e.g. 42, -7                                      */
    TOK_DECIMAL  = 11,  /* e.g. 3.14                                        */
    TOK_STRING   = 12,  /* Single-quoted string: 'Alice'  →  Alice          */

    /* ── SQL Keywords (all uppercased before comparison) ────────────────── */
    KW_SELECT    = 20,
    KW_FROM      = 21,
    KW_WHERE     = 22,
    KW_INSERT    = 23,
    KW_INTO      = 24,
    KW_VALUES    = 25,
    KW_CREATE    = 26,
    KW_TABLE     = 27,
    KW_INNER     = 28,
    KW_JOIN      = 29,
    KW_ON        = 30,
    KW_NOT       = 31,
    KW_NULL      = 32,
    KW_PRIMARY   = 33,
    KW_KEY       = 34,
    KW_INT       = 35,  /* Also serves as type keyword                      */
    KW_DECIMAL_T = 36,  /* DECIMAL as a type (not same as TOK_DECIMAL)      */
    KW_VARCHAR   = 37,
    KW_DATETIME  = 38,
    KW_TEXT      = 39,  /* Alias for VARCHAR                                */

    /* ── Database-level keywords ─────────────────────────────────────────── */
    KW_DATABASE  = 40,
    KW_DATABASES = 41,
    KW_USE       = 42,
    KW_SHOW      = 43,
    KW_DROP      = 44,
    KW_TABLES    = 45,

    /* ── Punctuation ─────────────────────────────────────────────────────── */
    PUNCT_COMMA     = 50,  /* ,  */
    PUNCT_SEMI      = 51,  /* ;  */
    PUNCT_LPAREN    = 52,  /* (  */
    PUNCT_RPAREN    = 53,  /* )  */
    PUNCT_STAR      = 54,  /* *  */
    PUNCT_DOT       = 55,  /* .  (table.column notation)                   */

    /* ── Comparison operators ─────────────────────────────────────────────── */
    TOK_EQ       = 60,  /* =   */
    TOK_NEQ      = 61,  /* !=  */
    TOK_LT       = 62,  /* <   */
    TOK_GT       = 63,  /* >   */
    TOK_LTE      = 64,  /* <=  */
    TOK_GTE      = 65   /* >=  */
} TokenType;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  TOKEN struct
 *
 *  LESSON: A token has TWO parts:
 *    type  — what kind of token it is (from the enum above)
 *    text  — the raw characters from the source that form this token
 *              (so the parser can read the identifier name / literal value)
 *
 *  We store the text as a fixed-size array (not a pointer) to avoid heap
 *  allocation per token — the lexer is called millions of times during bulk
 *  insert benchmarks.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
#define MAX_TOKEN_TEXT 256

typedef struct {
    TokenType type;
    char      text[MAX_TOKEN_TEXT];  /* Null-terminated token text           */
    int       line;                  /* Source line (for error messages)     */
    int       col;                   /* Source column                        */
} Token;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  LEXER struct  (the lexer's state)
 *
 *  LESSON: The lexer is a STATE MACHINE.  It keeps track of:
 *    src      — the original SQL string (we never modify it)
 *    pos      — current read position (index into src)
 *    line/col — for error messages ("error at line 3, col 12")
 *    peek     — one-token lookahead buffer (used by the parser)
 *
 *  The "peek" technique: the parser often needs to look at the NEXT token
 *  before deciding what rule to apply, without consuming it.  We buffer
 *  one token so  lexer_peek()  can be called multiple times returning the
 *  same token, and  lexer_next()  advances past it.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct {
    const char *src;       /* Source SQL string (not owned)                 */
    size_t      pos;       /* Current read position in src                  */
    size_t      len;       /* Length of src                                 */
    int         line;      /* Current line number (1-based)                 */
    int         col;       /* Current column number (1-based)               */
    Token       peek_buf;  /* Lookahead buffer                              */
    int         has_peek;  /* 1 if peek_buf is valid                        */
} Lexer;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  LEXER API
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/** Initialise a lexer over the given SQL string (not copied). */
void lexer_init(Lexer *lx, const char *sql);

/**
 * lexer_next — consume and return the next token.
 * Skips whitespace and single-line comments (-- ...).
 */
Token lexer_next(Lexer *lx);

/**
 * lexer_peek — return the next token WITHOUT consuming it.
 * Multiple calls return the same token until lexer_next() is called.
 */
Token lexer_peek(Lexer *lx);

/**
 * lexer_expect — consume the next token; error if its type != expected.
 * Returns 0 on success, -1 if the token type doesn't match.
 * Writes an error message to errmsg on failure.
 */
int lexer_expect(Lexer *lx, TokenType expected, Token *out, char **errmsg);

/** Human-readable name for a TokenType (for error messages). */
const char *token_type_name(TokenType t);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_LEXER_H */