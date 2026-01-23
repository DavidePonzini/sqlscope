'''Utility functions related to SQL subqueries in ASTs made with sqlglot.'''

import sqlglot.optimizer.normalize
from sqlglot import exp
from copy import deepcopy

def get_name(subquery: exp.Subquery) -> str:
    '''Returns the subquery name or alias, in lowercase if unquoted.'''
    
    if subquery.args.get('alias'):
        quoted = subquery.args['alias'].this.quoted
        name = subquery.alias_or_name

        return name if quoted else name.lower()

    return subquery.alias_or_name

def extract_function_name(func_expr: exp.Func) -> str:
    '''Extract the function name from a function expression.'''
    if isinstance(func_expr, exp.Anonymous):
        return func_expr.name.upper()
    return func_expr.__class__.__name__.lower()

