'''Utility functions related to SQL functions in ASTs made with sqlglot.'''

from sqlglot import exp

def get_name(func_expr: exp.Func) -> str:
    '''Extract the function name from a function expression.'''
    if isinstance(func_expr, exp.Anonymous):
        return func_expr.name.upper()
    return func_expr.__class__.__name__.lower()

