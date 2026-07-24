from ..catalog import Catalog
from .postgres import build_catalog, build_catalog_from_postgres, build_catalog_from_postgres_schema, CatalogColumnInfo, CatalogUniqueConstraintInfo, CatalogFunctionInfo
from .sql import build_catalog_from_sql

def load_catalog(path: str) -> Catalog:
    '''Loads a catalog from a JSON file.'''
    return Catalog.load_json(path)

