from .types import ResultType
from sqlglot.expressions import DataType
from dateutil.parser import parse
from sqlglot import exp

def to_date(target: ResultType) -> bool:
    if target.data_type in DataType.TEMPORAL_TYPES:
        return True
    if target.data_type in DataType.TEXT_TYPES and target.value is not None:
        try:
            parse(target.value)
            return True
        except ValueError:
            return False
    return False
    
def to_number(target: ResultType) -> bool:
    if target.data_type in DataType.NUMERIC_TYPES:
        return True
    if target.data_type in DataType.TEXT_TYPES and target.value is not None:
        try:
            float(target.value)
            return True
        except ValueError:
            return False
    return False

def is_number(target: DataType.Type):
    return target in DataType.NUMERIC_TYPES

def is_string(target: DataType.Type):
    return target in DataType.TEXT_TYPES

def is_date(target: DataType.Type):
    return target in DataType.TEMPORAL_TYPES

def error_message(expression: exp.Expression | str, found: ResultType | str, expected: ResultType | str | None = None) -> tuple[str, str, str | None]:
    '''Return an error message tuple containing (sql_snippet, found_type, expected_type).'''
    if expected is not None:
        if isinstance(expected, ResultType):
            expected = expected.data_type_str
        expected = expected.lower()

    if isinstance(found, ResultType):
        found = found.data_type_str
    found = found.lower()

    if isinstance(expression, exp.Expression):
        expression = expression.sql()

    return (expression, found, expected)