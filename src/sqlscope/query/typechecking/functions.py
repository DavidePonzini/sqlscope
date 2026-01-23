from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType
from sqlglot.expressions import DataType
from .util import is_number, error_message

@get_type.register
def _(expression: exp.Count, catalog: Catalog, search_path: str) -> ResultType:
    old_messages = get_type(expression.this, catalog, search_path).messages

    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Avg, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type != DataType.Type.UNKNOWN and not is_number(inner_type.data_type):
        old_messages.append(error_message(expression, inner_type, "NUMERIC"))

    return AtomicType(data_type=expression.type.this, nullable=True, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Sum, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type != DataType.Type.UNKNOWN and not is_number(inner_type.data_type):
        old_messages.append(error_message(expression, inner_type, "NUMERIC"))

    return AtomicType(data_type=expression.type.this, nullable=True, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Min, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type != DataType.Type.UNKNOWN and inner_type.data_type == DataType.Type.BOOLEAN:
        old_messages.append(error_message(expression, inner_type))

    return AtomicType(data_type=inner_type.data_type, nullable=inner_type.nullable, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Max, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type != DataType.Type.UNKNOWN and inner_type.data_type == DataType.Type.BOOLEAN:
        old_messages.append(error_message(expression, inner_type))

    return AtomicType(data_type=inner_type.data_type, nullable=inner_type.nullable, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Concat, catalog: Catalog, search_path: str) -> ResultType:
    old_messages = []
    args_type = []

    for arg in expression.expressions:
        arg_type = get_type(arg, catalog, search_path)
        if arg_type.messages:
            old_messages.extend(arg_type.messages)
        args_type.append(arg_type)
        

    if not args_type:
        old_messages.append(error_message(expression, "Empty arguments"))
    
    # if all args are NULL, result is NULL
    if all(target_type.data_type == DataType.Type.NULL for target_type in args_type):
        return AtomicType(data_type=DataType.Type.NULL, constant=True, messages=old_messages)

    constant = all(target_type.constant for target_type in args_type)
    nullable = any(target_type.nullable for target_type in args_type)

    # Always returns VARCHAR
    return AtomicType(data_type=expression.type.this, nullable=nullable, constant=constant, messages=old_messages)
