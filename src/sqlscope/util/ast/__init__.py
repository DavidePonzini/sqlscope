'''Utility functions for processing SQL ASTs made with sqlglot.'''

from . import column, function, subquery, table

import sqlglot.optimizer.normalize
from sqlglot import exp
from copy import deepcopy

def extract_DNF(expr) -> list[exp.Expression]:
    '''Given a boolean expression, extract its Disjunctive Normal Form (DNF)'''
    expr = deepcopy(expr)       # Avoid modifying the original expression

    # Remove outer parentheses
    while isinstance(expr, exp.Paren):
        expr = expr.this

    dnf_expr = sqlglot.optimizer.normalize.normalize(expr, dnf=True)

    if not isinstance(dnf_expr, exp.Or):
        return [dnf_expr]
    
    disjuncts = dnf_expr.flatten()  # list Di (A1 OR A2 OR ... OR Dn)
    
    result: list[exp.Expression] = []
    for disj in disjuncts:
        # Remove outer parentheses from each disjunct
        while isinstance(disj, exp.Paren):
            disj = disj.this
        result.append(disj)

    return result

def extract_CNF(expr) -> list[exp.Expression]:
    '''Given a boolean expression, extract its Conjunctive Normal Form (CNF)'''
    expr = deepcopy(expr)       # Avoid modifying the original expression

    # Remove outer parentheses
    while isinstance(expr, exp.Paren):
        expr = expr.this

    cnf_expr = sqlglot.optimizer.normalize.normalize(expr, dnf=False)

    if not isinstance(cnf_expr, exp.And):
        return [cnf_expr]
    
    conjuncts = cnf_expr.flatten()  # list Ci (A1 AND A2 AND ... AND Cn)

    result: list[exp.Expression] = []
    for conj in conjuncts:
        # Remove outer parentheses from each conjunct
        while isinstance(conj, exp.Paren):
            conj = conj.this
        result.append(conj)

    return result