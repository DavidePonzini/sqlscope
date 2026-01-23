'''Utility functions for SQL query processing.'''

from . import ast, tokens, sql

from dataclasses import dataclass

@dataclass(frozen=True)
class OrderByColumn:
    '''Represents a single column in an ORDER BY clause, with its sorting direction.'''
    column: str
    '''The name of the column to order by.'''
    table: str
    '''The table the column belongs to. Its name matches `referenced_tables` in the query it belongs to.'''
    ascending: bool = True
    '''The sorting direction, either True for ascending or False for descending. Defaults to True.'''



