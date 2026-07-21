from .base import get_type
from . import primitives, functions, queries, unary_ops, binary_ops, predicates
from ...catalog import Catalog
from ...dialects import Dialect
from sqlglot import exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

__all__ = ["get_type"]

def rewrite_expression(
        expression: exp.Expression,
        catalog: Catalog,
        search_path: str = 'public',
        dialect: Dialect | None = None,
    ) -> exp.Expression:
    '''
    Rewrites the expression by annotating types to its nodes based on the catalog.
    '''

    schema_sqlglot = catalog.to_sqlglot_schema()
    dialect_sqlglot = dialect.get_sqlglot_dialect() if dialect else None

    qualified_expression = qualify(
        expression=expression,
        schema=schema_sqlglot,
        db=search_path,
        validate_qualify_columns=False,
        dialect=dialect_sqlglot
    )

    return annotate_types(
        expression=qualified_expression,
        schema=schema_sqlglot,
        dialect=dialect_sqlglot
    )

# This function needs to be called on a typed expression
def collect_errors(
        expression: exp.Expression,
        catalog: Catalog,
        search_path: str = 'public',
        dialect: Dialect | None = None,
    ) -> list[tuple[str, str, str | None]]:
    '''This function needs to be called on a typed expression. It collects all typechecking errors from the expression tree.'''
    
    return get_type(expression, catalog, search_path, dialect).messages