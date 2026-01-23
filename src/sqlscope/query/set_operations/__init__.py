'''Query representation as set operation trees.'''

from .set_operation import SetOperation
from .binary_set_operation import BinarySetOperation, Union, Intersect, Except
from .select import Select
from ...catalog import Catalog
from ... import util

import sqlparse
from sqlparse.sql import Parenthesis
from sqlparse.tokens import Keyword

def create_set_operation_tree(sql: str, catalog: Catalog = Catalog(), search_path: str = 'public', is_top_level: bool = True) -> SetOperation:
    '''
    Parses a SQL string and constructs a tree of SetOperation objects representing the query structure using sqlparse.

    Args:
        sql (str): The SQL query string to parse.
        catalog (Catalog): The database catalog for resolving table and column names.
        search_path (str): The search path for schema resolution.

    Returns:
        SetOperation: The root of the SetOperation tree representing the query.
    '''

    # strip trailing semicolon
    if sql.strip().endswith(';'):
        sql = sql.strip()[:-1].strip()

    # remove outer parentheses
    stripped_sql = util.sql.remove_parentheses(sql)
    if stripped_sql != sql:
        # if outer parentheses were removed, we are again at top level
        return create_set_operation_tree(stripped_sql, catalog, search_path, True)
    sql = stripped_sql

    parsed = sqlparse.parse(sql)
    if not parsed:
        return Select(sql, catalog=catalog, search_path=search_path)

    statement = parsed[0]
    all_tokens = statement.tokens

    # Strip trailing ORDER BY / LIMIT / OFFSET
    if is_top_level:
        main_tokens, trailing_tokens = strip_trailing_clauses(all_tokens)
    else:
        main_tokens, trailing_tokens = all_tokens, []

    top_ops = find_top_level_ops(main_tokens)
    if not top_ops:
        return Select(sql, catalog=catalog, search_path=search_path)

    # Precedence: split lowest-precedence first (UNION/EXCEPT) so INTERSECT stays grouped
    # start from the last occurrence to get left-associative grouping
    union_except = [(i, op, a) for (i, op, a) in top_ops if op in ('UNION', 'EXCEPT')]
    if union_except:
        split_idx, op, all_in_token = union_except[-1]
    else:
        split_idx, op, all_in_token = top_ops[-1]  # only INTERSECTs remain


    left_tokens, right_tokens, all_kw = split_on(main_tokens, split_idx, all_in_token)

    left_node  = create_set_operation_tree(util.tokens.tokens_to_sql(left_tokens),  catalog, search_path, False)
    right_node = create_set_operation_tree(util.tokens.tokens_to_sql(right_tokens), catalog, search_path, False)

    trailing_sql = util.tokens.tokens_to_sql(trailing_tokens) if trailing_tokens else None

    if op == 'UNION':
        node = Union(sql, left_node, right_node, distinct=not all_kw, trailing_sql=trailing_sql)
    elif op == 'EXCEPT':
        node = Except(sql, left_node, right_node, distinct=not all_kw, trailing_sql=trailing_sql)
    else:  # INTERSECT
        node = Intersect(sql, left_node, right_node, distinct=not all_kw, trailing_sql=trailing_sql)
    return node

def parse_op_token(tok: sqlparse.sql.Token) -> tuple[str, bool | None] | None:
    '''
    Parse "UNION", "INTERSECT", "EXCEPT" with optional inline ALL/DISTINCT.

    Returns:
        tuple: `(op, all_flag)` where all_flag is: True  if ALL inline (e.g., "UNION ALL"); False if DISTINCT inline (e.g., "EXCEPT DISTINCT"); None  if no modifier inline (so caller may look right).
    '''
    if tok.ttype is not Keyword:
        return None
    parts = tok.normalized.upper().split()
    if not parts:
        return None

    op = parts[0]
    if op not in ('UNION', 'INTERSECT', 'EXCEPT'):
        return None

    if len(parts) > 1:
        if parts[1] == 'ALL':
            return (op, True)
        if parts[1] == 'DISTINCT':
            return (op, False)

    return (op, None)

def split_on(tokens: list[sqlparse.sql.Token], idx: int, all_in_token: bool | None) -> tuple[list[sqlparse.sql.Token], list[sqlparse.sql.Token], bool | None]:
    '''
    Splits around the operator at idx. If the modifier wasn't inline,
    consume a single immediate ALL/DISTINCT to the right.
    
    Returns:
        tuple: A tuple containing: left_tokens (list[sqlparse.sql.Token]): Tokens to the left of the operator; right_tokens (list[sqlparse.sql.Token]): Tokens to the right of the operator; all_flag (bool | None): True if ALL, False if DISTINCT, None if unspecified.
    '''
    left_tokens = tokens[:idx]
    right_tokens = tokens[idx + 1:]

    # trim ws
    while right_tokens and util.tokens.is_ws(right_tokens[0]):
        right_tokens = right_tokens[1:]

    all_flag = all_in_token  # True=ALL, False=DISTINCT, None=unspecified
    if all_flag is None and right_tokens and right_tokens[0].ttype is Keyword:
        kw = right_tokens[0].normalized.upper()
        if kw in ('ALL', 'DISTINCT'):
            all_flag = (kw == 'ALL')  # DISTINCT => False
            right_tokens = right_tokens[1:]
            while right_tokens and util.tokens.is_ws(right_tokens[0]):
                right_tokens = right_tokens[1:]

    return left_tokens, right_tokens, all_flag

def find_top_level_ops(tokens: list[sqlparse.sql.Token]) -> list[tuple[int, str, bool]]:
    '''
    Finds top-level set operation tokens (UNION, INTERSECT, EXCEPT) in the token list.
    
    Returns:
        list: a list of tuples (index, operation, all_flag).
    '''
    
    ops = []
    for i, tok in enumerate(tokens):
        if isinstance(tok, Parenthesis):
            continue
        parsed = parse_op_token(tok)
        if parsed:
            op, all_flag = parsed
            ops.append((i, op, all_flag))
    return ops

def strip_trailing_clauses(tokens: list[sqlparse.sql.Token]) -> tuple[list[sqlparse.sql.Token], list[sqlparse.sql.Token]]:
    '''
    Strips trailing ORDER BY, LIMIT, OFFSET clauses from the token list.
    Returns a tuple of (main_tokens, trailing_tokens).

    Returns:
        tuple: A tuple containing two lists of tokens:
            - main_tokens: The tokens excluding trailing clauses.
            - trailing_tokens: The tokens that were stripped as trailing clauses.
    '''

    cut_idx: int | None = None
    depth: int = 0

    # Scan backwards for trailing clauses, stopping at first clause that isn't one of them
    for i, tok in reversed(list(enumerate(tokens))):
        if tok.ttype is sqlparse.tokens.Punctuation and tok.value == ')':
            depth += 1
            continue
        if tok.ttype is sqlparse.tokens.Punctuation and tok.value == '(':
            depth -= 1
            continue
        if tok.ttype is not Keyword:
            continue
        if tok.value.upper() in {'ORDER BY', 'LIMIT', 'OFFSET'} and depth == 0:
            cut_idx = i
        else:
            break
            
    if cut_idx is not None:
        return tokens[:cut_idx], tokens[cut_idx:]

    return tokens, []

    
