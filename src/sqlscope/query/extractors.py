'''Extracts components from list of tokens using `sqlparse`.'''

from sqlglot import expressions as E

import sqlparse
from sqlparse.sql import Function, Parenthesis, TokenList
from sqlparse.tokens import DML, Keyword

import copy

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


def extract_subqueries_tokens(sql: str) -> list[tuple[str, str]]:
    """
    Recursively extract all subqueries (including nested ones) from a SQL string.
    Each result is a tuple (subquery_sql, clause), where clause is the nearest
    keyword context (e.g., SELECT, FROM, WHERE, HAVING, JOIN, ON, etc.).
    """
    parsed = sqlparse.parse(sql)
    results: list[tuple[str, str]] = []

    def _has_select_inside(group: TokenList) -> bool:
        for t in group.flatten():
            if t.ttype is DML and t.normalized == 'SELECT':
                return True
        return False

    def _inner_text_once(p: Parenthesis) -> str:
        v = p.value.strip()
        if v.startswith('(') and v.endswith(')'):
            v = v[1:-1].strip()
        return v

    def _walk(tokenlist: TokenList, current_clause: str | None = None):
        tokens = getattr(tokenlist, 'tokens', [])
        for i, tok in enumerate(tokens):
            if tok.is_whitespace:
                continue

            # Update clause context when we see a keyword
            if tok.ttype is Keyword or tok.ttype is DML:
                current_clause = tok.normalized.upper()
            if isinstance(tok, sqlparse.sql.Identifier) and tok.value.upper() in ('ALL', 'ANY'):
                current_clause = 'ALL/ANY'
            elif tok.ttype is sqlparse.tokens.Comparison:
                current_clause = 'COMPARISON'

            if tok.is_group:
                if isinstance(tok, Parenthesis) and _has_select_inside(tok):
                    inner = _inner_text_once(tok)
                    results.append((inner, current_clause or 'UNKNOWN'))

                    # Re-parse the inner subquery to search deeper
                    for inner_stmt in sqlparse.parse(inner):
                        _walk(inner_stmt)
                else:
                    _walk(tok, current_clause)

    for stmt in parsed:
        _walk(stmt)

    return results
