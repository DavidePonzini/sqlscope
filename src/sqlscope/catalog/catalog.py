from .table import Table
from .schema import Schema
from .util import split_search_path

from dataclasses import dataclass, field
import json
from typing import Self
from copy import deepcopy


@dataclass
class Catalog:
    '''A database catalog, with schemas, tables, and columns.'''

    _schemas: dict[str, Schema] = field(default_factory=dict)
    
    def lookup_schema(self, search_path: str) -> Schema | None:
        '''Gets the first existing schema from a possibly comma-separated search path.'''

        for schema_name in split_search_path(search_path):
            if schema_name in self._schemas:
                return self._schemas[schema_name]
        return None

    def get_schema(self, search_path: str) -> Schema:
        '''Gets a schema from the catalog, creating the first schema in the search path if needed.'''

        schema = self.lookup_schema(search_path)
        if schema:
            return schema

        # schema not found, create the first schema in the search path
        schema_name = split_search_path(search_path)[0]
        self._schemas[schema_name] = Schema(schema_name)
        return self._schemas[schema_name]
    
    def get_table(self, search_path: str, table_name: str) -> Table:
        '''Gets a table from the catalog, creating the schema and table if needed.'''


        for schema in split_search_path(search_path):
            if schema in self._schemas and self._schemas[schema].has_table(table_name):
                return self._schemas[schema][table_name]
            
        # table not found, create the first schema in the search path and the table
        schema_name = split_search_path(search_path)[0]
        schema = self.get_schema(schema_name)
        schema[table_name] = Table(table_name, schema_name)

        return schema[table_name]

    def __setitem__(self, search_path: str, schema: Schema) -> Schema:
        '''Sets a schema in the catalog, replacing any existing schema with the same name.'''
        
        # in case multiple schemas are specified, only use the first one
        schema_name = split_search_path(search_path)[0]
        
        self._schemas[schema_name] = schema

        return schema
    
    def copy_table(self, search_path: str, table_name: str, table: Table) -> Table:
        '''Copies a table into the first schema in the search path, creating it if needed.'''
        
        new_table = deepcopy(table)
        schema_name = split_search_path(search_path)[0]
        
        self.get_schema(schema_name)[table_name] = new_table
        
        return new_table

    def lookup_table(self, search_path: str, table_name: str) -> Table | None:
        '''Gets the first matching table from a possibly comma-separated schema search path.'''

        for schema_name in split_search_path(search_path):
            schema = self._schemas.get(schema_name)
            if schema and schema.has_table(table_name):
                return schema[table_name]
        return None

    def has_table(self, search_path: str, table_name: str) -> bool:
        '''
            Checks if a table exists in any schema from the specified search path.

            Returns False if none of the schemas contain the table.
        '''

        return self.lookup_table(search_path, table_name) is not None

    def add_column(self, search_path: str, table_name: str, column_name: str,
                   column_type: str, numeric_precision: int | None = None, numeric_scale: int | None = None,
                   is_nullable: bool = True,
                   fk_schema: str | None = None, fk_table: str | None = None, fk_column: str | None = None) -> None:
        '''Adds a column to the catalog, creating the schema and table if they do not exist.'''

        target_table = self.lookup_table(search_path, table_name)
        if target_table is None:
            schema_name = split_search_path(search_path)[0]
            target_table = self.get_table(schema_name, table_name)

        target_table.add_column(name=column_name,
                                column_type=column_type, numeric_precision=numeric_precision, numeric_scale=numeric_scale,
                                is_nullable=is_nullable,
                                fk_schema=fk_schema, fk_table=fk_table, fk_column=fk_column)
        
    @property
    def schema_names(self) -> set[str]:
        '''Returns all schema names in the catalog.'''
        return set(self._schemas.keys())

    @property
    def table_names(self) -> set[str]:
        '''Returns all table names in the catalog, regardless of schema.'''

        result = set()
        for schema in self._schemas.values():
            result.update(schema.table_names)
        return result
    
    def merge(self, other: 'Catalog') -> 'Catalog':
        '''Merges another catalog into this one, overwriting any existing schemas, tables, or columns with the same names.'''

        result = self.copy()

        for schema_name, schema in other._schemas.items():
            if schema_name not in result._schemas:
                result._schemas[schema_name] = deepcopy(schema)
            else:
                result._schemas[schema_name].merge(schema)

        return result

    def copy(self) -> Self:
        '''Creates a deep copy of the catalog.'''
        return deepcopy(self)
    
    def __repr__(self) -> str:
        schemas = [schema.__repr__(1) for schema in self._schemas.values()]

        result = 'Catalog('
        for schema in schemas:
            result += '\n' + schema
        result += '\n)'

        return result

    # region Serialization
    def to_dict(self) -> dict:
        '''Converts the Catalog to a dictionary.'''
        return {
            'version': 1,
            'schemas': {name: sch.to_dict() for name, sch in self._schemas.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Catalog':
        '''Creates a Catalog from a dictionary.'''
        cat = cls()
        for _, sch_data in (data.get('schemas') or {}).items():
            sch = Schema.from_dict(sch_data)
            cat._schemas[sch.name] = sch
        return cat

    #  String-based JSON (handy for DB/blob storage)
    def to_json(self, *, indent: int | None = 2) -> str:
        '''Converts the Catalog to a JSON string.'''
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, s: str) -> 'Catalog':
        '''Creates a Catalog from a JSON string.'''
        return cls.from_dict(json.loads(s))

    def to_sqlglot_schema(self) -> dict[str, dict[str, dict[str, str]]]:
        '''Converts to a sqlglot-compatible catalog format.'''

        result: dict[str, dict[str, dict[str, str]]] = {}

        for sch_name, sch in self._schemas.items():
            result[sch_name] = {}
            for tbl_name, tbl in sch._tables.items():
                if not tbl.columns:
                    continue
                result[sch_name][tbl_name] = {}
                for col in tbl.columns:
                    result[sch_name][tbl_name][col.name] = col.column_type
            if not result[sch_name]:
                del result[sch_name]

        return result
    # endregion

    # region File Helpers
    def save_json(self, path: str, *, indent: int | None = 2) -> None:
        '''Saves the Catalog to a JSON file.'''
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=indent)

    @classmethod
    def load_json(cls, path: str) -> 'Catalog':
        '''Loads a Catalog from a JSON file.'''
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return cls.from_dict(data)
    # endregion