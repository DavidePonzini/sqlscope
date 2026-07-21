import sqlglot.expressions as exp
from .types import AtomicType, ResultType
from ...catalog import Catalog
from ...dialects import Dialect
from functools import singledispatch

@singledispatch
def get_type(expression: exp.Expression, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    '''Returns the type of the given SQL expression.'''
    return AtomicType() # Default to unhandled expression