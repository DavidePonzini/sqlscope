from .table import Table
from .schema import Schema

from dataclasses import dataclass, field
import json
from typing import Self
from copy import deepcopy

@dataclass
class Catalog:
    '''A database catalog, with schemas, tables, and columns.'''

    _schemas: dict[str, Schema] = field(default_factory=dict)

    def __getitem__(self, schema_name: str) -> Schema:
        '''Gets a schema from the catalog, creating it if it does not exist.'''

        if schema_name not in self._schemas:
            self._schemas[schema_name] = Schema(schema_name)
        return self._schemas[schema_name]
    
    def __setitem__(self, schema_name: str, schema: Schema) -> Schema:
        '''Sets a schema in the catalog, replacing any existing schema with the same name.'''
        
        self._schemas[schema_name] = schema
        return schema
    
    def has_schema(self, schema_name: str) -> bool:
        '''Checks if a schema exists in the catalog.'''
        
        return schema_name in self._schemas
    
    def copy_table(self, schema_name: str, table_name: str, table: Table) -> Table:
        '''Copies a table into the catalog, creating the schema if it does not exist.'''
        
        new_table = deepcopy(table)
        self[schema_name][table_name] = new_table
        
        return new_table

    def has_table(self, schema_name: str, table_name: str) -> bool:
        '''
            Checks if a table exists in the specified schema in the catalog.

            Returns False if the schema or table do not exist.
        '''

        if not self.has_schema(schema_name):
            return False
        return self.__getitem__(schema_name).has_table(table_name)

    def add_column(self, schema_name: str, table_name: str, column_name: str,
                   column_type: str, numeric_precision: int | None = None, numeric_scale: int | None = None,
                   is_nullable: bool = True,
                   fk_schema: str | None = None, fk_table: str | None = None, fk_column: str | None = None) -> None:
        '''Adds a column to the catalog, creating the schema and table if they do not exist.'''

        self[schema_name][table_name].add_column(name=column_name,
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

    # String-based JSON (handy for DB/blob storage)
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

    # Convenience file helpers
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