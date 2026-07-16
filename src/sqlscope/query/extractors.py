'''Extracts components from list of tokens using `sqlparse`.'''

from sqlglot import expressions as E

import sqlparse
from sqlparse.sql import Function, Parenthesis, TokenList
from sqlparse.tokens import DML, Keyword

import copy

from .. import util

def extract_functions(tokens, current_clause: str = 'NONE') -> list[tuple[sqlparse.sql.Function, str]]:
    result: list[tuple[sqlparse.sql.Function, str]] = []

    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword or token.ttype is sqlparse.tokens.DML or token.ttype is sqlparse.tokens.CTE:
            if token.value.upper() in ('WITH', 'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT'):
                current_clause = token.value.upper()
            continue


        if isinstance(token, Function):
            # Include this function
            result.append((token, current_clause))
            # Also search inside for nested function calls
            result.extend(extract_functions(token.tokens, current_clause))
        elif token.is_group:
            result.extend(extract_functions(token.tokens, current_clause))
    return result

def extract_comparisons(tokens, current_clause: str = 'NONE') -> list[tuple[sqlparse.sql.Comparison, str]]:
    result: list[tuple[sqlparse.sql.Comparison, str]] = []
    
    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword or token.ttype is sqlparse.tokens.DML or token.ttype is sqlparse.tokens.CTE:
            if token.value.upper() in ('WITH', 'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT'):
                current_clause = token.value.upper()
            continue

        if isinstance(token, sqlparse.sql.Comparison):
            result.append((token, current_clause))
        elif token.is_group:
            result.extend(extract_comparisons(token.tokens, current_clause))
    return result

def remove_ctes(ast: E.Expression | None) -> str:
    '''Removes CTEs from the SQL query and returns the main query as a string.'''

    if ast is None:
        return ''

    ast_copy = copy.deepcopy(ast)

    ast_copy.set('with', None)
    return ast_copy.sql()

def extract_subqueries_ast(ast: E.Expression | None) -> list[E.Subquery]:
    '''
        Extracts subqueries from the SQL query and returns them as a list of sqlglot Expression objects.

        This function relies on AST parsing.
    '''

    if ast is None:
        return []

    return list(ast.find_all(E.Subquery))

def extract_subqueries_tokens(sql: str) -> list[tuple[str, str, int]]:
    """
    Recursively extract all subqueries (including nested ones) from a SQL string.

    Returns a list of (subquery_sql, clause, depth), where:
    - clause is the nearest keyword context (e.g., SELECT, FROM, WHERE, HAVING, JOIN, ON, etc.)
    - depth is the subquery nesting depth (1 = directly inside outer query, 2 = nested within a subquery, ...)
    """
    parsed = sqlparse.parse(sql)
    results: list[tuple[str, str, int]] = []

    def _has_select_inside(group: TokenList) -> bool:
        flattened = [
            (t.ttype, t.value) for t in group.flatten()
            if t.ttype not in (
                sqlparse.tokens.Whitespace,
                sqlparse.tokens.Newline,
                sqlparse.tokens.Punctuation,
                sqlparse.tokens.Comment
            )
        ]

        if not flattened:
            return False
        
        first_token = flattened[0]

        return first_token[0] is DML and first_token[1].upper() == 'SELECT'

    def _walk(tokenlist: TokenList, current_clause: str | None = None, depth: int = 0) -> None:
        tokens = getattr(tokenlist, 'tokens', [])
        for tok in tokens:
            if tok.is_whitespace:
                continue

            # Update clause context when we see a keyword
            if tok.ttype is Keyword or tok.ttype is DML:
                current_clause = tok.normalized.upper()
            if isinstance(tok, sqlparse.sql.Identifier) and tok.value.upper() in ('ALL', 'ANY'):
                current_clause = 'ALL/ANY'
            elif tok.ttype is sqlparse.tokens.Comparison:
                current_clause = 'COMPARISON'
            elif current_clause and ('FROM' in current_clause or 'JOIN' in current_clause):
                if isinstance(tok, sqlparse.sql.Parenthesis):
                    current_clause = f'{current_clause} - TABLE_EXPRESSION'

            if tok.is_group:
                # Detect subquery in parentheses
                if isinstance(tok, Parenthesis) and _has_select_inside(tok):
                    inner = TokenList(tok.tokens[1:-1])
                    subq_depth = depth + 1
                    results.append((inner.value, current_clause or 'UNKNOWN', subq_depth))

                    # Re-parse inner and continue walking deeper with incremented depth
                    for inner_stmt in inner.tokens:
                        _walk(inner_stmt, current_clause=None, depth=subq_depth)
                else:
                    # Normal group: keep same depth and clause context
                    _walk(tok, current_clause=current_clause, depth=depth)

    for stmt in parsed:
        _walk(stmt)

    return results

def strip_filters(sql: str) -> str:
    """
    Removes FILTER (...) clauses from aggregate functions in the SQL query.

    This function uses sqlparse to tokenize the SQL and remove FILTER clauses.
    """
    def _strip_tokenlist(tokenlist: sqlparse.sql.TokenList) -> str:
        parts: list[str] = []
        tokens = tokenlist.tokens
        idx = 0

        while idx < len(tokens):
            token = tokens[idx]

            if token.normalized.upper() == 'FILTER':
                idx += 1
                while idx < len(tokens) and tokens[idx].is_whitespace:
                    idx += 1
                if idx < len(tokens) and isinstance(tokens[idx], Parenthesis):
                    idx += 1
                continue

            if token.is_group:
                parts.append(_strip_tokenlist(token))
            else:
                parts.append(token.value)

            idx += 1

        return ''.join(parts)

    return _strip_tokenlist(sqlparse.parse(sql)[0])

def sanitize_query_str(sql: str) -> str:
    '''Sanitize a SQL query string so that sqlglot can parse it correctly in particular edge cases.'''

    def remove_crlf(sql: str) -> str:
        '''Remove CR from CRLF line endings, since they are not handled well.'''
        return sql.replace('\r\n', '\n')

    def remove_parentheses_from_table_expressions(sql: str) -> str:
        """
        Removes parentheses around table expressions in FROM and JOIN clauses.

        This is necessary because sqlglot will parse "FROM (table1 JOIN table2)" as a subquery instead of a join.
        """

        results: list[str] = []
        parsed = sqlparse.parse(sql)

        def _walk(tokenlist: sqlparse.sql.TokenList, current_clause: str | None) -> None:
            tokens = tokenlist.tokens

            for tok in tokens:
                if util.tokens.is_ws(tok):
                    results.append(tok.value)
                    continue

                # Update clause context when we see a keyword
                if tok.ttype is Keyword or tok.ttype is DML:
                    current_clause = tok.normalized.upper()

                if tok.is_group:
                    if current_clause in ('FROM', 'JOIN'):
                        # a parenthesis with no direct SELECT inside is likely a table expression, so we can remove the parentheses
                        if isinstance(tok, Parenthesis) and not any(t.ttype is DML and t.normalized.upper() == 'SELECT' for t in tok.tokens):
                            inner = tok.tokens[1:-1]  # Remove the outer parentheses
                            _walk(TokenList(inner), current_clause=current_clause)
                            continue
                    # Otherwise, just walk the group normally
                    _walk(tok, current_clause=current_clause)
                else:
                    results.append(tok.value)

        for stmt in parsed:
            _walk(stmt, current_clause=None)

        return ''.join(results)
    
    def add_spaces_before_parentheses(sql: str) -> str:
        """
        Adds a space before opening parentheses in function calls.

        This is necessary because sqlglot will parse "exists(select ...)" as a function call instead of a subquery, which can lead to incorrect parsing.
        """

        result = ''

        parsed = sqlparse.parse(sql)[0]
        for stmt in parsed.flatten():
            if stmt.ttype is sqlparse.tokens.Name and stmt.value.upper() in ('EXISTS', 'ANY', 'ALL'):
                result += stmt.value + ' '
            else:
                result += stmt.value

        return result

    sql = remove_crlf(sql)
    sql = remove_parentheses_from_table_expressions(sql)
    sql = add_spaces_before_parentheses(sql)

    return sql

def strip_comments(sql: str) -> str:
    """
    Removes comments from the SQL query.

    This function uses sqlparse to tokenize the SQL and remove comments.
    """
    parsed = sqlparse.parse(sql)
    result_parts: list[str] = []

    for stmt in parsed:
        for token in stmt.flatten():
            if token.ttype not in (sqlparse.tokens.Comment, sqlparse.tokens.Comment.Multiline, sqlparse.tokens.Comment.Single):
                result_parts.append(token.value)

    return ''.join(result_parts)