from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType
from sqlglot.expressions import DataType
from .util import is_string, to_number, to_date, error_message

@get_type.register
def _(expression: exp.Like, catalog: Catalog, search_path: str) -> ResultType:
    left_type = get_type(expression.this, catalog, search_path)
    right_type = get_type(expression.expression, catalog, search_path)

    old_messages = left_type.messages + right_type.messages
    
    if left_type.data_type != DataType.Type.UNKNOWN and not is_string(left_type.data_type) and left_type.data_type != DataType.Type.NULL:
        old_messages.append(error_message(expression, left_type, 'string'))

    if right_type.data_type != DataType.Type.UNKNOWN and not is_string(right_type.data_type) and right_type.data_type != DataType.Type.NULL:
        old_messages.append(error_message(expression, right_type, 'string'))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Is, catalog: Catalog, search_path: str) -> ResultType:  
    left_type = get_type(expression.this, catalog, search_path)
    right_type = get_type(expression.expression, catalog, search_path)

    old_messages = left_type.messages + right_type.messages

    # IS right operand must be BOOLEAN or NULL constant
    if right_type.data_type not in (DataType.Type.BOOLEAN, DataType.Type.NULL) or not right_type.constant:
        old_messages.append(error_message(expression, right_type, 'boolean|null'))

    # if right is BOOLEAN and left is not NULL, left must be BOOLEAN
    if left_type.data_type != DataType.Type.UNKNOWN and right_type.data_type == DataType.Type.BOOLEAN and left_type.data_type != DataType.Type.NULL:
        if left_type.data_type != DataType.Type.BOOLEAN:
            old_messages.append(error_message(expression, left_type, 'boolean'))
    
    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Between, catalog: Catalog, search_path: str) -> ResultType:
    target_type = get_type(expression.this, catalog, search_path)
    low_type = get_type(expression.args.get("low"), catalog, search_path)
    high_type = get_type(expression.args.get("high"), catalog, search_path)

    old_messages = target_type.messages + low_type.messages + high_type.messages

    # if the target is NULL, the result will always be NULL (no matter the bounds)
    if target_type.data_type == DataType.Type.UNKNOWN or target_type.data_type == DataType.Type.NULL:
        return AtomicType(data_type=expression.type.this, constant=True, messages=old_messages)

    if low_type.data_type != DataType.Type.UNKNOWN and low_type.data_type != target_type.data_type and low_type.data_type != DataType.Type.NULL:

        # check for implicit casts
        if (to_number(target_type) and not to_number(low_type)) or (to_date(target_type) and not to_date(low_type)):
            old_messages.append(error_message(expression, low_type, target_type))

    if high_type.data_type != DataType.Type.UNKNOWN and high_type.data_type != target_type.data_type and high_type.data_type != DataType.Type.NULL:
        
        # check for implicit casts
        if (to_number(target_type) and not to_number(high_type)) or (to_date(target_type) and not to_date(high_type)):
            old_messages.append(error_message(expression, high_type, target_type))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)


@get_type.register
def _(expression: exp.In, catalog: Catalog, search_path: str) -> ResultType:
    target_type = get_type(expression.this, catalog, search_path)

    old_messages = target_type.messages

    if target_type.data_type == DataType.Type.UNKNOWN:
        return AtomicType(data_type=expression.type.this, messages=old_messages)

    # Case IN (<list>)
    for item in expression.expressions:
        item_type = get_type(item, catalog, search_path)
        old_messages.extend(item_type.messages)
        if target_type != item_type:
            old_messages.append(error_message(expression, item_type, target_type))

    # Case IN (subquery)
    if expression.args.get("query"):
        subquery_type = get_type(expression.args.get("query"), catalog, search_path)
        old_messages.extend(subquery_type.messages)
        if target_type != subquery_type:
            old_messages.append(error_message(expression, subquery_type, target_type))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

# AND, OR
@get_type.register
def _(expression: exp.Connector, catalog: Catalog, search_path: str) -> ResultType:
    left_type = get_type(expression.this, catalog, search_path)
    right_type = get_type(expression.expression, catalog, search_path)

    old_messages = left_type.messages + right_type.messages

    if left_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append(error_message(expression, left_type, 'boolean'))

    if right_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append(error_message(expression, right_type, 'boolean'))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

# ANY, ALL
@get_type.register
def _(expression: exp.SubqueryPredicate, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)

# EXISTS
@get_type.register
def _(expression: exp.Exists, catalog: Catalog, search_path: str) -> ResultType:
    old_messages = get_type(expression.this, catalog, search_path).messages
    return AtomicType(data_type=DataType.Type.BOOLEAN, nullable=False, constant=True, messages=old_messages)
