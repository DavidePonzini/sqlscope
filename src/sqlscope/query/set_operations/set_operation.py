import sqlglot
from sqlglot import exp
from ...catalog import Table, Column

from abc import ABC, abstractmethod

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .select import Select


class SetOperation(ABC):
    '''
    Abstract base class for SQL set operations (i.e., SELECT, UNION, INTERSECT, EXCEPT).
    '''

    def __init__(self, sql: str, parent_query: 'Select | None' = None) -> None:
        self.sql = sql
        '''The SQL string representing the operation.'''
        
        self.parent_query = parent_query
        '''The parent Select if this is a subquery.'''

    @property
    @abstractmethod
    def output(self) -> Table:
        '''Returns the output table schema of the set operation.'''
        pass

    @property
    @abstractmethod
    def referenced_tables(self) -> list[Table]:
        '''Returns a list of tables that are referenced in the SQL query.'''
        pass
    
    def __repr__(self, pre: str = '') -> str:
        return f'{pre}{self.__class__.__name__}'

    
    @abstractmethod
    def print_tree(self, pre: str = '') -> None:
        pass
    
    @property
    @abstractmethod
    def main_selects(self) -> list['Select']:
        '''Returns a list of selects that are part of a set operation.'''
        return []

    @property
    @abstractmethod
    def selects(self) -> list['Select']:
        '''Returns a list of all Select nodes in the tree.'''
        return []
    
    

    

