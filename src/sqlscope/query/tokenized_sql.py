'''Query representation as a list of tokens. Works even for invalid SQL.'''

import sqlparse
import sqlparse.tokens
from sqlparse.tokens import Whitespace, Newline

from . import extractors

class TokenizedSQL:
    '''Base class for tokenizing SQL queries.'''

    def __init__(self, sql: str) -> None:
        self.sql = sql
        '''The full SQL query string.'''

        # Lazy properties
        self._tokens = None
        self._functions = None
        self._comparisons = None
        # End of lazy properties

        parsed_statements = sqlparse.parse(self.sql)
        if not parsed_statements:
            self.all_statements: list[sqlparse.sql.Statement] = []
            self.parsed = sqlparse.sql.Statement()
        else:
            self.all_statements = list(parsed_statements)
            self.parsed = parsed_statements[0]

    # region Properties
    @property
    def tokens(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Returns a flattened list of tokens as (ttype, value) tuples, excluding whitespace and newlines.'''
        if not self._tokens:
            self._tokens = self._flatten()
        return self._tokens

    @property
    def functions(self) -> list[tuple[sqlparse.sql.Function, str]]:
        '''Returns a list of (function, clause) tuples found in the SQL query.'''

        if self._functions is None:
            self._functions = extractors.extract_functions(self.parsed.tokens)
        return self._functions
    
    @property
    def comparisons(self) -> list[tuple[sqlparse.sql.Comparison, str]]:
        '''Returns a list of (comparison, clause) tuples found in the SQL query.'''
        if self._comparisons is None:
            self._comparisons = extractors.extract_comparisons(self.parsed.tokens)
        return self._comparisons    

    # endregion

    def _flatten(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Flattens the parsed SQL statement into a list of (ttype, value) tuples. Ignores whitespace and newlines.'''

        if not self.parsed:
            return []

        # Flatten tokens into (ttype, value)
        return [
            (tok.ttype, tok.value) for tok in self.parsed.flatten()
            if tok.ttype not in (Whitespace, Newline)
        ]

    def print_tree(self) -> None:
        for stmt in self.all_statements:
            stmt._pprint_tree()

