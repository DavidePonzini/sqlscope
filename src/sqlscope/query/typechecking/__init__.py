from .base import get_type
from . import primitives, functions, queries, unary_ops, binary_ops, predicates
from ...catalog import Catalog
from sqlglot import exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

__all__ = ["get_type"]

def rewrite_expression(expression: exp.Expression, catalog: Catalog, search_path: str = 'public') -> exp.Expression:
    '''
    Rewrites the expression by annotating types to its nodes based on the catalog.
    '''

    schema = catalog.to_sqlglot_schema()

    return annotate_types(qualify(expression, schema=schema, db=search_path, validate_qualify_columns=False), schema)

# This function needs to be called on a typed expression
def collect_errors(expression: exp.Expression, catalog: Catalog, search_path: str = 'public') -> list[tuple[str, str, str | None]]:
    return get_type(expression, catalog, search_path).messages