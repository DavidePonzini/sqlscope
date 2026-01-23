from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType
from sqlglot.expressions import DataType
from .util import is_number, error_message

@get_type.register
def _(expression: exp.Neg, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type == DataType.Type.UNKNOWN:
        return AtomicType(data_type=expression.type.this, messages=old_messages)

    if not is_number(expression.type.this):
        old_messages.append(error_message(expression, 'numeric', inner_type))
    
    return AtomicType(data_type=expression.type.this, nullable=inner_type.nullable, constant=inner_type.constant, messages=old_messages, value=inner_type.value)

@get_type.register
def _(expression: exp.Not, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type == DataType.Type.UNKNOWN:
        return AtomicType(data_type=expression.type.this, messages=old_messages)

    if inner_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append(error_message(expression, 'boolean', inner_type))

    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Paren, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)

@get_type.register
def _(expression: exp.Alias, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)

# To handle COUNT(DISTINCT ...) or similar constructs
@get_type.register
def _(expression: exp.Distinct, catalog: Catalog, search_path: str) -> ResultType:
    
    if len(expression.expressions) != 1:
        return AtomicType(messages=[error_message(expression, 'To many arguments')])

    return get_type(expression.expressions[0], catalog, search_path)
