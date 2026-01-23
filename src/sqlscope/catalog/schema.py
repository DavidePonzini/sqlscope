from .table import Table

from dataclasses import dataclass, field
import json
from typing import Self
from copy import deepcopy

@dataclass
class Schema:
    '''A database schema, with tables and functions.'''

    name: str
    _tables: dict[str, Table] = field(default_factory=dict)
    functions: set[str] = field(default_factory=set)

    def __getitem__(self, table_name: str) -> Table:
        '''Gets a table from the schema, creating it if it does not exist.'''
        if table_name not in self._tables:
            self._tables[table_name] = Table(table_name, self.name)
        return self._tables[table_name]

    def __setitem__(self, table_name: str, table: Table) -> None:
        '''Sets a table in the schema, replacing any existing table with the same name.'''
        self._tables[table_name] = table
    
    def has_table(self, table_name: str) -> bool:
        '''Checks if a table exists in the schema.'''
        return table_name in self._tables
    
    def has_column(self, table_name: str, column_name: str) -> bool:
        '''Checks if a column exists in the schema.'''
        if not self.has_table(table_name):
            return False
        return self.__getitem__(table_name).has_column(column_name)

    @property
    def table_names(self) -> set[str]:
        '''Returns all table names in the schema.'''
        return set(self._tables.keys())

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        tables = '\n'.join([table.__repr__(level + 1) for table in self._tables.values()])
        return f'{indent}Schema(name=\'{self.name}\', tables=[\n{tables}\n{indent}])'

    def to_dict(self) -> dict:
        '''Converts the Schema to a dictionary.'''
        return {
            'name': self.name,
            'tables': {name: tbl.to_dict() for name, tbl in self._tables.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Schema':
        '''Creates a Schema from a dictionary.'''
        schema = cls(name=data['name'])
        for _, tbl_data in (data.get('tables') or {}).items():
            tbl = Table.from_dict(tbl_data, schema_name=schema.name)
            schema._tables[tbl.name] = tbl
        return schema