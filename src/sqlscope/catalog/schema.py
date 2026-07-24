from .table import Table
from .function import Function

from dataclasses import dataclass, field
import json
from typing import Self
from copy import deepcopy

@dataclass
class Schema:
    '''A database schema, with tables and functions.'''

    name: str
    _tables: dict[str, Table] = field(default_factory=dict)
    _functions: list[Function] = field(default_factory=list)

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

    def add_function(self, name: str, arguments: list[str], return_type: str, kind: str) -> Function:
        '''Adds a function to the schema and returns it.'''
        func = Function(name=name, arguments=arguments, return_type=return_type, kind=kind)
        self._functions.append(func)
        return func

    def has_function(self, name: str, arguments: list[str] | None = None) -> bool:
        '''Checks if a function exists in the schema.'''
        if arguments is None:
            return any(func.name == name for func in self._functions)

        return any(func.name == name and func.arguments == arguments for func in self._functions)

    def get_functions(self, name: str) -> list[Function]:
        '''Gets all functions with the given name in the schema.'''
        return [func for func in self._functions if func.name == name]

    def get_function(self, name: str, arguments: list[str]) -> Function | None:
        '''Gets a function with the given name and arguments in the schema.'''
        for func in self._functions:
            if func.name == name and func.arguments == arguments:
                return func
        return None

    @property
    def table_names(self) -> set[str]:
        '''Returns all table names in the schema.'''
        return set(self._tables.keys())

    @property
    def function_names(self) -> set[str]:
        '''Returns all function names in the schema.'''
        return set(func.name for func in self._functions)

    def merge(self, other: 'Schema') -> 'Schema':
        '''Merges another schema into this one, overwriting any existing tables with the same names.'''

        result = deepcopy(self)

        for table_name, table in other._tables.items():
            if table_name not in result._tables:
                result._tables[table_name] = deepcopy(table)
            else:
                result._tables[table_name].merge(table)

        return result

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        tables = '\n'.join([table.__repr__(level + 1) for table in self._tables.values()])
        functions = '\n'.join([func.__repr__(level + 1) for func in self._functions])
        return f'{indent}Schema(name=\'{self.name}\', tables=[\n{tables}\n{indent}], functions=[\n{functions}\n{indent}])'

    # region Serialization
    def to_dict(self) -> dict:
        '''Converts the Schema to a dictionary.'''
        return {
            'name': self.name,
            'tables': {name: tbl.to_dict() for name, tbl in self._tables.items()},
            'functions': [func.to_dict() for func in self._functions]
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Schema':
        '''Creates a Schema from a dictionary.'''
        schema = cls(name=data['name'])
        for _, tbl_data in (data.get('tables') or {}).items():
            tbl = Table.from_dict(tbl_data, schema_name=schema.name)
            schema._tables[tbl.name] = tbl
        for func_data in data.get('functions', []):
            func = Function.from_dict(func_data)
            schema._functions.append(func)
        return schema
    # endregion