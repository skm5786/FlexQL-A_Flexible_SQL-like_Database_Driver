/**
 * lexer.cpp  —  SQL Lexer Implementation
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * LESSON 2A — THE LEXER (TOKENISER) IN DEPTH
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * HOW THE LEXER WORKS — state machine overview:
 *
 *   1. Skip whitespace and comments.
 *   2. Look at the CURRENT character (src[pos]) to decide what kind of
 *      token to scan:
 *        letter / '_'  → identifier or keyword
 *        digit / '-'   → number literal
 *        '\''          → string literal
 *        '('           → PUNCT_LPAREN  (single character token)
 *        '='           → TOK_EQ
 *        '<'           → could be TOK_LT or TOK_LTE — peek one ahead
 *        etc.
 *   3. Advance pos past all consumed characters.
 *   4. Fill in the Token struct and return it.
 *
 * KEYWORD RECOGNITION:
 *   After scanning an identifier (letters/digits/_), we uppercase it and
 *   compare against the known keyword table.  If it matches, we change the
 *   token type from TOK_IDENT to the matching KW_*.
 *   This linear scan over ~20 keywords is O(1) in practice.
 *
 * CASE INSENSITIVITY:
 *   SQL is case-insensitive for keywords.  We uppercase the text copy but
 *   leave user-data strings (quoted values like 'Alice') as-is.
 */

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include "parser/lexer.h"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  KEYWORD TABLE
 *
 *  LESSON: A keyword table is the standard way to separate the keyword check
 *  from the identifier scan.  We scan the identifier first, then look it up
 *  here.  This avoids writing special cases for each keyword in the scanner.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
struct Keyword { const char *word; TokenType type; };

static const Keyword KEYWORDS[] = {
    { "SELECT",   KW_SELECT   },
    { "FROM",     KW_FROM     },
    { "WHERE",    KW_WHERE    },
    { "INSERT",   KW_INSERT   },
    { "INTO",     KW_INTO     },
    { "VALUES",   KW_VALUES   },
    { "CREATE",   KW_CREATE   },
    { "TABLE",    KW_TABLE    },
    { "INNER",    KW_INNER    },
    { "JOIN",     KW_JOIN     },
    { "ON",       KW_ON       },
    { "NOT",      KW_NOT      },
    { "NULL",     KW_NULL     },
    { "PRIMARY",  KW_PRIMARY  },
    { "KEY",      KW_KEY      },
    { "INT",      KW_INT      },
    { "INTEGER",  KW_INT      },  /* alias */
    { "DECIMAL",  KW_DECIMAL_T},
    { "FLOAT",    KW_DECIMAL_T},  /* alias */
    { "DOUBLE",   KW_DECIMAL_T},  /* alias */
    { "VARCHAR",  KW_VARCHAR  },
    { "TEXT",     KW_TEXT     },
    { "DATETIME", KW_DATETIME },
    { "DATABASE",  KW_DATABASE  },
    { "DATABASES", KW_DATABASES },
    { "USE",       KW_USE       },
    { "SHOW",      KW_SHOW      },
    { "DROP",      KW_DROP      },
    { "TABLES",    KW_TABLES    },
    { nullptr, TOK_UNKNOWN }      /* Sentinel — marks end of table           */
};

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  INTERNAL HELPERS
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/** Current character under the read cursor. */
static inline char cur(const Lexer *lx) {
    return (lx->pos < lx->len) ? lx->src[lx->pos] : '\0';
}

/** Peek one character ahead without consuming. */
static inline char peek_char(const Lexer *lx) {
    return (lx->pos + 1 < lx->len) ? lx->src[lx->pos + 1] : '\0';
}

/** Advance the cursor by one character, tracking line/col. */
static inline void advance(Lexer *lx) {
    if (lx->pos < lx->len) {
        if (lx->src[lx->pos] == '\n') { lx->line++; lx->col = 1; }
        else                           { lx->col++;                }
        lx->pos++;
    }
}

/** Skip whitespace (space, tab, newline, carriage-return). */
static void skip_whitespace(Lexer *lx) {
    while (lx->pos < lx->len && isspace((unsigned char)cur(lx)))
        advance(lx);
}

/** Skip a single-line SQL comment: -- until end of line. */
static void skip_comment(Lexer *lx) {
    /* We're called after seeing "--" */
    while (lx->pos < lx->len && cur(lx) != '\n')
        advance(lx);
}

/**
 * uppercase_inplace — convert ASCII letters in buf to uppercase.
 * LESSON: toupper() is locale-dependent; for SQL keywords (ASCII only)
 * it's fine, but for user data we don't touch it.
 */
static void uppercase_inplace(char *buf) {
    for (; *buf; buf++) *buf = (char)toupper((unsigned char)*buf);
}

/** Look up an uppercase identifier in the keyword table. */
static TokenType keyword_lookup(const char *upper_word) {
    for (int i = 0; KEYWORDS[i].word != nullptr; ++i) {
        if (strcmp(KEYWORDS[i].word, upper_word) == 0)
            return KEYWORDS[i].type;
    }
    return TOK_IDENT;  /* Not a keyword — plain identifier */
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  SCAN FUNCTIONS  (one per token category)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * scan_identifier_or_keyword
 *
 * LESSON: SQL identifiers start with a letter or underscore, followed by
 * any combination of letters, digits, and underscores.
 *   Valid:   student, first_name, TABLE_123
 *   Invalid: 123abc (starts with digit)
 *
 * After scanning, we uppercase the text and check the keyword table.
 */
static Token scan_identifier_or_keyword(Lexer *lx) {
    Token tok;
    tok.line = lx->line;
    tok.col  = lx->col;

    int i = 0;
    while (lx->pos < lx->len &&
           (isalnum((unsigned char)cur(lx)) || cur(lx) == '_')) {
        if (i < MAX_TOKEN_TEXT - 1) tok.text[i++] = cur(lx);
        advance(lx);
    }
    tok.text[i] = '\0';

    /* Uppercase copy for keyword matching */
    char upper[MAX_TOKEN_TEXT];
    strncpy(upper, tok.text, MAX_TOKEN_TEXT);
    uppercase_inplace(upper);

    /* Store uppercased version in the token text so comparisons are easy   */
    strncpy(tok.text, upper, MAX_TOKEN_TEXT);

    tok.type = keyword_lookup(upper);
    return tok;
}

/**
 * scan_number
 *
 * LESSON: We scan digits; if we encounter a '.' we switch to DECIMAL.
 * We also handle a leading '-' for negative numbers but only when not
 * preceded by an identifier or ')' (to avoid confusing  a-b  with  a(-b)).
 * For simplicity in FlexQL we always treat '-' as part of a number only when
 * the parser explicitly expects a number literal.
 */
static Token scan_number(Lexer *lx) {
    Token tok;
    tok.line = lx->line;
    tok.col  = lx->col;
    tok.type = TOK_INTEGER;

    int i = 0;
    /* Capture optional leading minus */
    if (cur(lx) == '-') { tok.text[i++] = '-'; advance(lx); }

    while (lx->pos < lx->len && isdigit((unsigned char)cur(lx))) {
        if (i < MAX_TOKEN_TEXT - 1) tok.text[i++] = cur(lx);
        advance(lx);
    }
    /* Decimal point → switch to DECIMAL */
    if (cur(lx) == '.' && isdigit((unsigned char)peek_char(lx))) {
        tok.type = TOK_DECIMAL;
        if (i < MAX_TOKEN_TEXT - 1) tok.text[i++] = '.';
        advance(lx);
        while (lx->pos < lx->len && isdigit((unsigned char)cur(lx))) {
            if (i < MAX_TOKEN_TEXT - 1) tok.text[i++] = cur(lx);
            advance(lx);
        }
    }
    tok.text[i] = '\0';
    return tok;
}

/**
 * scan_string
 *
 * LESSON: SQL string literals are delimited by SINGLE quotes: 'hello world'
 * We consume the opening quote, copy content, then consume the closing quote.
 * Escaped single quotes within a string use the SQL standard doubling: ''
 *   e.g.  'it''s'  →  it's
 *
 * We return the CONTENTS without the surrounding quotes.
 */
static Token scan_string(Lexer *lx) {
    Token tok;
    tok.line = lx->line;
    tok.col  = lx->col;
    tok.type = TOK_STRING;

    advance(lx);  /* Consume opening '\'' */

    int i = 0;
    while (lx->pos < lx->len) {
        char c = cur(lx);
        if (c == '\'') {
            advance(lx);
            /* SQL escaped quote: '' inside a string means a literal ' */
            if (cur(lx) == '\'') {
                if (i < MAX_TOKEN_TEXT - 1) tok.text[i++] = '\'';
                advance(lx);
            } else {
                break;  /* End of string */
            }
        } else {
            if (i < MAX_TOKEN_TEXT - 1) tok.text[i++] = c;
            advance(lx);
        }
    }
    tok.text[i] = '\0';
    return tok;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  PUBLIC LEXER API IMPLEMENTATION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

void lexer_init(Lexer *lx, const char *sql) {
    lx->src      = sql;
    lx->pos      = 0;
    lx->len      = strlen(sql);
    lx->line     = 1;
    lx->col      = 1;
    lx->has_peek = 0;
    memset(&lx->peek_buf, 0, sizeof(Token));
}

/**
 * lexer_next — the core scan function.
 *
 * LESSON — Dispatch on current character:
 *   This is a classic "switch on the first character" approach.  Most
 *   tokens are uniquely identified by their first character, so O(1)
 *   dispatch is possible.  Multi-character tokens like '<=' need one
 *   extra peek at the following character.
 */
Token lexer_next(Lexer *lx) {
    /* If we have a buffered peek token, return and clear it */
    if (lx->has_peek) {
        lx->has_peek = 0;
        return lx->peek_buf;
    }

    /* Skip whitespace and comments */
    while (true) {
        skip_whitespace(lx);
        if (lx->pos < lx->len && cur(lx) == '-' && peek_char(lx) == '-') {
            advance(lx); advance(lx);  /* Consume "--" */
            skip_comment(lx);
        } else {
            break;
        }
    }

    Token tok;
    tok.line = lx->line;
    tok.col  = lx->col;
    tok.text[0] = '\0';

    if (lx->pos >= lx->len) {
        tok.type = TOK_EOF;
        return tok;
    }

    char c = cur(lx);

    /* ── Letters / underscore → identifier or keyword ──────────────────── */
    if (isalpha((unsigned char)c) || c == '_') {
        return scan_identifier_or_keyword(lx);
    }

    /* ── Digits → number literal ────────────────────────────────────────── */
    if (isdigit((unsigned char)c)) {
        return scan_number(lx);
    }

    /* ── Negative number: only when '-' followed immediately by a digit ─── */
    if (c == '-' && isdigit((unsigned char)peek_char(lx))) {
        return scan_number(lx);
    }

    /* ── Single-quoted string ───────────────────────────────────────────── */
    if (c == '\'') {
        return scan_string(lx);
    }

    /* ── Single-character punctuation ──────────────────────────────────── */
    advance(lx);
    tok.text[0] = c;
    tok.text[1] = '\0';

    switch (c) {
    case ',': tok.type = PUNCT_COMMA;   return tok;
    case ';': tok.type = PUNCT_SEMI;    return tok;
    case '(': tok.type = PUNCT_LPAREN;  return tok;
    case ')': tok.type = PUNCT_RPAREN;  return tok;
    case '*': tok.type = PUNCT_STAR;    return tok;
    case '.': tok.type = PUNCT_DOT;     return tok;

    /* ── Comparison operators (may be 1 or 2 chars) ─────────────────────── */
    case '=':
        tok.type = TOK_EQ;
        return tok;

    case '!':
        /* Must be followed by '=' to be a valid token */
        if (cur(lx) == '=') {
            tok.text[1] = '='; tok.text[2] = '\0';
            advance(lx);
            tok.type = TOK_NEQ;
        } else {
            tok.type = TOK_UNKNOWN;
        }
        return tok;

    case '<':
        if (cur(lx) == '=') {
            tok.text[1] = '='; tok.text[2] = '\0';
            advance(lx);
            tok.type = TOK_LTE;
        } else {
            tok.type = TOK_LT;
        }
        return tok;

    case '>':
        if (cur(lx) == '=') {
            tok.text[1] = '='; tok.text[2] = '\0';
            advance(lx);
            tok.type = TOK_GTE;
        } else {
            tok.type = TOK_GT;
        }
        return tok;

    default:
        tok.type = TOK_UNKNOWN;
        return tok;
    }
}

Token lexer_peek(Lexer *lx) {
    if (!lx->has_peek) {
        lx->peek_buf = lexer_next(lx);
        lx->has_peek = 1;
    }
    return lx->peek_buf;
}

int lexer_expect(Lexer *lx, TokenType expected, Token *out, char **errmsg) {
    Token t = lexer_next(lx);
    if (out) *out = t;
    if (t.type != expected) {
        char msg[256];
        snprintf(msg, sizeof(msg),
                 "Expected %s but got '%s' at line %d col %d",
                 token_type_name(expected), t.text, t.line, t.col);
        if (errmsg) *errmsg = strdup(msg);
        return -1;
    }
    return 0;
}

const char *token_type_name(TokenType t) {
    switch (t) {
    case TOK_EOF:      return "EOF";
    case TOK_IDENT:    return "identifier";
    case TOK_INTEGER:  return "integer";
    case TOK_DECIMAL:  return "decimal";
    case TOK_STRING:   return "string";
    case KW_SELECT:    return "SELECT";
    case KW_FROM:      return "FROM";
    case KW_WHERE:     return "WHERE";
    case KW_INSERT:    return "INSERT";
    case KW_INTO:      return "INTO";
    case KW_VALUES:    return "VALUES";
    case KW_CREATE:    return "CREATE";
    case KW_TABLE:     return "TABLE";
    case KW_INNER:     return "INNER";
    case KW_JOIN:      return "JOIN";
    case KW_ON:        return "ON";
    case KW_NOT:       return "NOT";
    case KW_NULL:      return "NULL";
    case KW_PRIMARY:   return "PRIMARY";
    case KW_KEY:       return "KEY";
    case KW_INT:       return "INT";
    case KW_DECIMAL_T: return "DECIMAL";
    case KW_VARCHAR:   return "VARCHAR";
    case KW_TEXT:      return "TEXT";
    case KW_DATETIME:  return "DATETIME";
    case KW_DATABASE:  return "DATABASE";
    case KW_DATABASES: return "DATABASES";
    case KW_USE:       return "USE";
    case KW_SHOW:      return "SHOW";
    case KW_DROP:      return "DROP";
    case KW_TABLES:    return "TABLES";
    case PUNCT_COMMA:  return "','";
    case PUNCT_SEMI:   return "';'";
    case PUNCT_LPAREN: return "'('";
    case PUNCT_RPAREN: return "')'";
    case PUNCT_STAR:   return "'*'";
    case PUNCT_DOT:    return "'.'";
    case TOK_EQ:       return "'='";
    case TOK_NEQ:      return "'!='";
    case TOK_LT:       return "'<'";
    case TOK_GT:       return "'>'";
    case TOK_LTE:      return "'<='";
    case TOK_GTE:      return "'>='";
    default:           return "unknown";
    }
}