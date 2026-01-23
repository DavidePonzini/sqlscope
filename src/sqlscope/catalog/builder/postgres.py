from ..catalog import Catalog
from ..constraint import ConstraintType

import psycopg2
import time
from dataclasses import dataclass

# region Data Classes
@dataclass(frozen=True)
class CatalogColumnInfo:
    '''Holds information about a database column.'''
    schema_name: str
    table_name: str
    column_name: str
    column_type: str
    numeric_precision: int | None
    numeric_scale: int | None
    is_nullable: bool
    foreign_key_schema: str | None
    foreign_key_table: str | None
    foreign_key_column: str | None

    def to_dict(self) -> dict:
        return {
            'schema_name': self.schema_name,
            'table_name': self.table_name,
            'column_name': self.column_name,
            'column_type': self.column_type,
            'numeric_precision': self.numeric_precision,
            'numeric_scale': self.numeric_scale,
            'is_nullable': self.is_nullable,
            'foreign_key_schema': self.foreign_key_schema,
            'foreign_key_table': self.foreign_key_table,
            'foreign_key_column': self.foreign_key_column,
        }

@dataclass(frozen=True)
class CatalogUniqueConstraintInfo:
    '''Holds information about a unique constraint or primary key.'''
    schema_name: str
    table_name: str
    constraint_type: str
    columns: str  # Postgres returns this as a string like '{col1,col2,...}'

    def to_dict(self) -> dict:
        return {
            'schema_name': self.schema_name,
            'table_name': self.table_name,
            'constraint_type': self.constraint_type,
            'columns': self.columns,
        }
# endregion
    
# region Catalog Builder
def build_catalog(columns_info: list[CatalogColumnInfo], unique_constraints_info: list[CatalogUniqueConstraintInfo]) -> Catalog:
    '''Builds a catalog from the provided column and unique constraint information.'''
    result = Catalog()
    
    for column in columns_info:
        result.add_column(
            schema_name=column.schema_name,
            table_name=column.table_name,
            column_name=column.column_name,
            column_type=column.column_type,
            numeric_precision=column.numeric_precision,
            numeric_scale=column.numeric_scale,
            is_nullable=column.is_nullable,
            fk_schema=column.foreign_key_schema,
            fk_table=column.foreign_key_table,
            fk_column=column.foreign_key_column,
        )
        
    for constraint in unique_constraints_info:
        columns = set(constraint.columns.strip('{}').split(','))  # Postgres returns {col1,col2,...}
        constraint_type = ConstraintType.PRIMARY_KEY if constraint.constraint_type == 'PRIMARY KEY' else ConstraintType.UNIQUE

        result[constraint.schema_name][constraint.table_name].add_unique_constraint(columns, constraint_type=constraint_type)

    return result


def build_catalog_from_postgres(sql_string: str, *, hostname: str, port: int, user: str, password: str, schema: str | None = None, create_temp_schema: bool = False) -> Catalog:
    '''Builds a catalog by executing the provided SQL string in a temporary PostgreSQL database.'''
    if sql_string.strip() == '':
        return Catalog()

    conn = psycopg2.connect(host=hostname, port=port, user=user, password=password)
    cur = conn.cursor()
    
    # Use a temporary schema with a fixed name
    if create_temp_schema:
        if schema is None:
            schema_name = f'sql_error_categorizer_{time.time_ns()}'
        else:
            schema_name = schema
        cur.execute(f'CREATE schema {schema_name};')
        cur.execute(f'SET search_path TO {schema_name};')
    else:
        schema_name = '%' if schema is None else schema
    
    # Create the tables
    cur.execute(sql_string)

    # Fetch the catalog information
    cur.execute(COLUMNS(schema_name))
    columns_info = cur.fetchall()

    columns_data = [
        CatalogColumnInfo(
            schema_name=row[0],
            table_name=row[1],
            column_name=row[2],
            column_type=row[3],
            numeric_precision=row[4],
            numeric_scale=row[5],
            is_nullable=row[6],
            foreign_key_schema=row[7],
            foreign_key_table=row[8],
            foreign_key_column=row[9],
        )
        for row in columns_info
    ]

    # Fetch unique constraints (including primary keys)
    cur.execute(UNIQUE_COLUMNS(schema_name))
    unique_constraints_info = cur.fetchall()

    unique_constraints_data = [
        CatalogUniqueConstraintInfo(
            schema_name=row[0],
            table_name=row[1],
            constraint_type=row[2],
            columns=row[3],
        )
        for row in unique_constraints_info
    ]

    # Clean up
    if create_temp_schema:
        cur.execute(f'DROP schema {schema_name} CASCADE;')
    conn.rollback()     # no need to save anything

    return build_catalog(columns_data, unique_constraints_data)
# endregion

# region SQL Queries
def UNIQUE_COLUMNS(schema_name: str = '%') -> str:
    return f'''
    SELECT
        kcu.table_schema AS schema_name,
        kcu.table_name,
        tc.constraint_type,
        array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.constraint_schema = kcu.constraint_schema
    WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
    AND kcu.table_schema LIKE '{schema_name}'
    GROUP BY
        kcu.table_schema,
        kcu.table_name,
        kcu.constraint_name,
        tc.constraint_type;
'''

def COLUMNS(schema_name: str = '%') -> str:
    return f'''
    SELECT
        cols.table_schema AS schema_name,
        cols.table_name,
        cols.column_name,
        cols.data_type AS column_type,
        cols.numeric_precision,
        cols.numeric_scale,
        (cols.is_nullable = 'YES') AS is_nullable,
        fk.foreign_table_schema AS foreign_key_schema,
        fk.foreign_table_name AS foreign_key_table,
        fk.foreign_column_name AS foreign_key_column
    FROM information_schema.columns AS cols

    -- Foreign Key
    LEFT JOIN (
        SELECT
            kcu.table_schema,
            kcu.table_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name   AS foreign_table_name,
            ccu.column_name  AS foreign_column_name
        FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.constraint_schema = kcu.constraint_schema
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
            JOIN information_schema.constraint_column_usage AS ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
    ) fk ON fk.table_schema = cols.table_schema
        AND fk.table_name = cols.table_name
        AND fk.column_name = cols.column_name

    WHERE cols.table_schema LIKE '{schema_name}'
'''
# endregion