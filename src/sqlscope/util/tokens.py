'''Utility functions for processing sqlparse tokens.'''

import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Whitespace, Newline

def tokens_to_sql(tokens: list[sqlparse.sql.Token]) -> str:
    '''Convert a list of sqlparse tokens back to a SQL string.'''
    return TokenList(tokens).value.strip()

def is_ws(token: sqlparse.sql.Token) -> bool:
    '''Check if a token is whitespace or newline.'''
    return token.ttype in (Whitespace, Newline)

def strip_ws(tokens: list[sqlparse.sql.Token]) -> list[sqlparse.sql.Token]:
    '''Remove whitespace and newline tokens from a list of tokens.'''
    return [t for t in tokens if not is_ws(t)]