/**
 * parser.h  —  SQL Parser Interface
 *
 * LESSON: The parser takes a raw SQL string and converts it to a QueryNode
 * (AST).  This separates "what the user typed" from "what to execute".
 *
 * The parsing pipeline is:
 *   Lexer (tokeniser) → Parser → QueryNode (AST)
 *
 * We will implement this step-by-step in the next lesson.
 */

#ifndef FLEXQL_PARSER_H
#define FLEXQL_PARSER_H

#include "common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * parser_parse — parse one SQL statement into a QueryNode.
 *
 * @param sql     NULL-terminated SQL string.
 * @param out     Output query node (must be pre-allocated by caller).
 * @param errmsg  OUT: heap-allocated error string on failure. Free with free().
 * @return  0 on success, -1 on parse error.
 */
int parser_parse(const char *sql, QueryNode *out, char **errmsg);

/**
 * column_type_from_string — convert "INT", "DECIMAL" etc. to ColumnType.
 * Returns COL_TYPE_UNKNOWN for unrecognised strings.
 */
ColumnType column_type_from_string(const char *type_str);

/**
 * compare_op_from_string — convert "=", "!=", "<" etc. to CompareOp.
 */
int compare_op_from_string(const char *op_str, CompareOp *out_op);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_PARSER_H */