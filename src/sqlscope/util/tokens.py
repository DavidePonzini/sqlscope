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

def is_select_all(tokens: list[tuple[sqlparse.tokens._TokenType, str]]) -> bool:
    """
    Check if the given list of tokens represents a SELECT * statement.
    This function checks for the presence of a SELECT keyword followed by a wildcard (*).
    """

    in_select = False
    select_all_found = False

    # Remove whitespace and newline tokens
    for ttype, value in tokens:
        if ttype in (Whitespace, Newline):
            continue

        if ttype is sqlparse.tokens.DML and value.upper() == 'SELECT':
            in_select = True
            continue
        if ttype is sqlparse.tokens.Keyword and value.upper() != 'SELECT':
            # If we encounter any other keyword after SELECT, it's not a SELECT *
            return select_all_found

        if in_select:
            if ttype is sqlparse.tokens.Wildcard and value == '*':
                select_all_found = True
            else:
                # If we encounter any other token in the SELECT clause, it's not a SELECT *
                return False

    return select_all_found