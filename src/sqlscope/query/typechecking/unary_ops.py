from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType
from sqlglot.expressions import DataType
from .util import is_number, error_message
from ...dialects import Dialect

@get_type.register
def _(expression: exp.Neg, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path, dialect)

    old_messages = inner_type.messages
    
    data_type = expression.type.this if expression.type else DataType.Type.UNKNOWN

    if inner_type.data_type == DataType.Type.UNKNOWN:
        return AtomicType(data_type=data_type, messages=old_messages)

    if not is_number(data_type):
        old_messages.append(error_message(expression, 'numeric', inner_type))
    
    return AtomicType(data_type=data_type, nullable=inner_type.nullable, constant=inner_type.constant, messages=old_messages, value=inner_type.value)

@get_type.register
def _(expression: exp.Not, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path, dialect)

    old_messages = inner_type.messages
    data_type = expression.type.this if expression.type else DataType.Type.UNKNOWN

    if inner_type.data_type == DataType.Type.UNKNOWN:
        return AtomicType(data_type=data_type, messages=old_messages)

    if inner_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append(error_message(expression, 'boolean', inner_type))

    return AtomicType(data_type=data_type, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Paren, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    return get_type(expression.this, catalog, search_path, dialect)

@get_type.register
def _(expression: exp.Alias, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    return get_type(expression.this, catalog, search_path, dialect)

# To handle COUNT(DISTINCT ...) or similar constructs
@get_type.register
def _(expression: exp.Distinct, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    
    if len(expression.expressions) != 1:
        return AtomicType(messages=[error_message(expression, 'To many arguments')])

    return get_type(expression.expressions[0], catalog, search_path, dialect)
