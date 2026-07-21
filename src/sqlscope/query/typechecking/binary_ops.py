from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType
from sqlglot.expressions import DataType
from .util import is_number, to_number, to_date, error_message, is_string
from ...dialects import Dialect

@get_type.register
def _(expression: exp.Binary, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    left_type = get_type(expression.this, catalog, search_path, dialect)
    right_type = get_type(expression.expression, catalog, search_path, dialect)

    old_messages = left_type.messages + right_type.messages
    
    # handle comparison operators
    if isinstance(expression, exp.Predicate):
        return typecheck_comparisons(left_type, right_type, expression, old_messages)
    

    if left_type != right_type:

        if left_type.data_type != DataType.Type.UNKNOWN and not to_number(left_type) and left_type.data_type != DataType.Type.NULL:
            old_messages.append(error_message(expression, left_type, "numeric"))

        if right_type.data_type != DataType.Type.UNKNOWN and not to_number(right_type) and right_type.data_type != DataType.Type.NULL:
            old_messages.append(error_message(expression, right_type, "numeric"))

    elif DataType.Type.UNKNOWN != left_type.data_type and not is_number(left_type.data_type) and not is_number(right_type.data_type):
        if left_type.data_type != DataType.Type.NULL or right_type.data_type != DataType.Type.NULL:
            old_messages.append(error_message(expression, left_type, "numeric"))

    data_type = expression.type.this if expression.type else DataType.Type.UNKNOWN

    return AtomicType(data_type=data_type, nullable=left_type.nullable or right_type.nullable, constant=left_type.constant and right_type.constant, messages=old_messages)

# handle comparison typechecking (e.g =, <, >, etc.)
def typecheck_comparisons(left_type: ResultType, right_type: ResultType, expression: exp.Binary, old_messages: list) -> ResultType:

    data_type = expression.type.this if expression.type else DataType.Type.UNKNOWN

    if DataType.Type.UNKNOWN in (left_type.data_type, right_type.data_type):
        return AtomicType(data_type=data_type, messages=old_messages)

    # for boolean comparisons we can have only equality/inequality
    if DataType.Type.BOOLEAN == left_type.data_type == right_type.data_type:
        if not isinstance(expression, (exp.EQ, exp.NEQ)):
            old_messages.append(error_message(expression, left_type, "boolean"))

    if left_type != right_type and left_type.data_type != DataType.Type.NULL and right_type.data_type != DataType.Type.NULL:
        
        # handle implicit casts
        if to_number(left_type) and to_number(right_type):
            return AtomicType(data_type=data_type, nullable=False, constant=True, messages=old_messages)

        if to_date(left_type) and to_date(right_type):
            return AtomicType(data_type=data_type, nullable=False, constant=True, messages=old_messages)

        old_messages.append(error_message(expression, left_type.data_type_str + " & " + right_type.data_type_str))

    # Always returns boolean
    return AtomicType(data_type=data_type, nullable=False, constant=True, messages=old_messages)


# TODO: add test cases for this function
# Handle string concatenation typechecking ('str1' || 'str2')
@get_type.register
def _(expression: exp.DPipe, catalog: Catalog, search_path: str, dialect: Dialect | None = None) -> ResultType:
    if dialect is not Dialect.POSTGRES:
        return AtomicType() # Default to unhandled expression for non-Postgres dialects

    data_type = expression.type.this if expression.type else DataType.Type.UNKNOWN

    left_type = get_type(expression.this, catalog, search_path, dialect)
    right_type = get_type(expression.expression, catalog, search_path, dialect)

    old_messages = left_type.messages + right_type.messages

    if DataType.Type.UNKNOWN in (left_type.data_type, right_type.data_type):
        return AtomicType(data_type=data_type, messages=old_messages)

    if not is_string(left_type.data_type) or not is_string(right_type.data_type):
        old_messages.append(error_message(expression, left_type.data_type_str + " || " + right_type.data_type_str, "string"))

    return AtomicType(data_type=data_type, nullable=left_type.nullable or right_type.nullable, constant=left_type.constant and right_type.constant, messages=old_messages)