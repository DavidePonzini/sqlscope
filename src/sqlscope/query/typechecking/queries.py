from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType, TupleType
from .util import error_message

@get_type.register
def _(expression: exp.Select, catalog: Catalog, search_path: str) -> ResultType:
    types = []
    old_messages = []

    for col in expression.expressions:
        col_type = get_type(col, catalog, search_path)
        if col_type.messages:
            old_messages.extend(col_type.messages)
        types.append(col_type)

    if not types:
        old_messages.append(error_message(expression, "No columns selected"))

    where = expression.args.get("where")
    if where:
        old_messages.extend(get_type(where.this, catalog, search_path).messages)

    having = expression.args.get("having")
    if having:
        old_messages.extend(get_type(having.this, catalog, search_path).messages)
    if len(types) == 1:
        return AtomicType(data_type=types[0].data_type, messages=old_messages, nullable=types[0].nullable)

    return TupleType(types=types, messages=old_messages, nullable=any(t.nullable for t in types))

@get_type.register
def _(expression: exp.Subquery, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)