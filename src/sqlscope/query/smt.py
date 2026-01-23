'''Convert SQL expressions to Z3 expressions for logical reasoning.'''

from typing import Any, Callable
from sqlglot import exp
from z3 import (
    Int, IntVal,
    Real, RealVal,
    Bool, BoolVal,
    String, StringVal,
    And, Or, Not,
    Solver,
    unsat,
    is_expr,
    BoolSort,
    ExprRef,
    Re,
    AllChar,
    Concat,
    InRe,
    PrefixOf,
    SuffixOf,
    Contains,
)

from ..catalog import Table


# ----------------------------------------------------------------------
#  Z3 variable creation
# ----------------------------------------------------------------------

def create_z3_var(variables: dict[str, Any], table_name: str | None,
                  col_name: str, col_type: Callable[[str], ExprRef] | None = None) -> None:
    '''
    Create a Z3 variable for the given column name and type, and add it to the
    variables dictionary. If col_type is None, default to Int.
    '''
    if col_type is None:
        col_type = Int  # default type

    # unqualified
    variables[col_name] = col_type(col_name)
    variables[f'{col_name}_isnull'] = Bool(f'{col_name}_isnull')

    # qualified
    if table_name:
        variables[f'{table_name}.{col_name}'] = col_type(f'{table_name}.{col_name}')
        variables[f'{table_name}.{col_name}_isnull'] = Bool(f'{table_name}.{col_name}_isnull')


def fresh_symbol(prefix: str, sort: str):
    '''Generate a fresh Z3 symbol with the given prefix and sort.'''
    if sort == 'int':
        return Int(f'{prefix}_{id(prefix)}')
    if sort == 'real':
        return Real(f'{prefix}_{id(prefix)}')
    if sort == 'bool':
        return Bool(f'{prefix}_{id(prefix)}')
    return String(f'{prefix}_{id(prefix)}')


# ----------------------------------------------------------------------
#  Infer expected type of a subquery based on parent expression
# ----------------------------------------------------------------------

def infer_subquery_sort_from_parent(expr) -> str:
    '''
    Infer the expected Z3 sort of a subquery based on its parent expression.
    '''
    parent = expr.parent

    # Arithmetic context → numeric
    if isinstance(parent, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.Pow)):
        return 'real'

    # Comparison context → numeric
    if isinstance(parent, (exp.GT, exp.GTE, exp.LT, exp.LTE)):
        return 'real'

    # BETWEEN → numeric
    if isinstance(parent, exp.Between):
        return 'real'

    # LIKE → string
    if isinstance(parent, exp.Like):
        return 'string'

    # String concatenation (|| operator)
    if isinstance(parent, exp.Concat):
        return 'string'

    # Default: boolean (EXISTS, WHERE (...))
    return 'bool'


# ----------------------------------------------------------------------
#  Catalog → Z3 vars
# ----------------------------------------------------------------------

def catalog_table_to_z3_vars(table: Table) -> dict[str, ExprRef]:
    '''Convert catalog table columns to Z3 variables.'''
    variables = {}
    for column in table.columns:
        col_name = column.name
        col_type = column.column_type.upper()

        if col_type in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT'):
            create_z3_var(variables, table.name, col_name, Int)
        elif col_type in ('FLOAT', 'REAL', 'DOUBLE'):
            create_z3_var(variables, table.name, col_name, Real)
        elif col_type in ('BOOLEAN', 'BOOL'):
            create_z3_var(variables, table.name, col_name, Bool)
        elif col_type in ('VARCHAR', 'CHAR', 'TEXT', 'CHARACTER VARYING'):
            create_z3_var(variables, table.name, col_name, String)
        else:
            create_z3_var(variables, table.name, col_name)
    return variables


# ----------------------------------------------------------------------
#  SQL → Z3 conversion
# ----------------------------------------------------------------------

def sql_to_z3(expr, variables: dict[str, ExprRef] = {}) -> Any:
    '''Convert a SQLGlot expression to a Z3 expression.'''

    # --- Columns ---
    if isinstance(expr, exp.Column):
        name = expr.name.lower()
        if name not in variables:
            create_z3_var(variables, None, name)
        return variables[name]

    # --- Literals ---
    elif isinstance(expr, exp.Literal):
        val = expr.this
        if expr.is_int:
            return IntVal(int(val))
        elif expr.is_number:
            return RealVal(float(val))
        elif expr.is_string:
            return StringVal(val.strip("'"))
        elif val.upper() in ('TRUE', 'FALSE'):
            return BoolVal(val.upper() == 'TRUE')
        elif val.upper() == 'NULL':
            return None
        else:
            raise NotImplementedError(f"Unsupported literal: {val}")

    elif isinstance(expr, exp.Null):
        return None

    # --- Boolean comparisons ---
    elif isinstance(expr, exp.EQ):
        return sql_to_z3(expr.left, variables) == sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.NEQ):
        return sql_to_z3(expr.left, variables) != sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.GT):
        return sql_to_z3(expr.left, variables) > sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.GTE):
        return sql_to_z3(expr.left, variables) >= sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.LT):
        return sql_to_z3(expr.left, variables) < sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.LTE):
        return sql_to_z3(expr.left, variables) <= sql_to_z3(expr.right, variables)

    # --- Logical connectives ---
    elif isinstance(expr, exp.And):
        return And(sql_to_z3(expr.left, variables), sql_to_z3(expr.right, variables))
    elif isinstance(expr, exp.Or):
        return Or(sql_to_z3(expr.left, variables), sql_to_z3(expr.right, variables))
    elif isinstance(expr, exp.Not):
        return Not(sql_to_z3(expr.this, variables))
    elif isinstance(expr, exp.Paren):
        return sql_to_z3(expr.this, variables)

    # --- Arithmetic ---
    elif isinstance(expr, exp.Add):
        return sql_to_z3(expr.left, variables) + sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Sub):
        return sql_to_z3(expr.left, variables) - sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Mul):
        return sql_to_z3(expr.left, variables) * sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Div):
        return sql_to_z3(expr.left, variables) / sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Mod):
        return sql_to_z3(expr.left, variables) % sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Pow):
        return sql_to_z3(expr.left, variables) ** sql_to_z3(expr.right, variables)

    # --- BETWEEN ---
    elif isinstance(expr, exp.Between):
        target = sql_to_z3(expr.this, variables)
        low = sql_to_z3(expr.args['low'], variables)
        high = sql_to_z3(expr.args['high'], variables)
        return And(target >= low, target <= high)

    # --- IN ---
    elif isinstance(expr, exp.In):
        target = sql_to_z3(expr.this, variables)

        if isinstance(expr.args.get('query'), exp.Subquery):
            # subquery → symbolic value
            sym = fresh_symbol('subq_in', 'string')
            return target == sym

        options = [sql_to_z3(e, variables) for e in expr.expressions]

        return Or(*[target == o for o in options])

    # --- IS / IS NOT ---
    elif isinstance(expr, exp.Is):
        target_expr = expr.this
        right_expr = expr.args.get('expression')

        if isinstance(right_expr, exp.Null):
            if isinstance(target_expr, exp.Column):
                name = target_expr.name.lower()
                flag = variables.setdefault(f'{name}_isnull', Bool(f'{name}_isnull'))
                return flag
            return BoolVal(False)

        if isinstance(right_expr, exp.Not) and isinstance(right_expr.this, exp.Null):
            if isinstance(target_expr, exp.Column):
                name = target_expr.name.lower()
                flag = variables.setdefault(f'{name}_isnull', Bool(f'{name}_isnull'))
                return Not(flag)
            return BoolVal(True)

        return sql_to_z3(target_expr, variables) == sql_to_z3(right_expr, variables)

    # --- LIKE ---
    elif isinstance(expr, exp.Like):
        target = sql_to_z3(expr.this, variables)
        pattern_expr = sql_to_z3(expr.expression, variables)

        # If pattern is a variable → fallback
        if not isinstance(expr.expression, exp.Literal):
            return target == pattern_expr

        pattern = expr.expression.this.strip("'")
        wildcard_count = pattern.count('%') + pattern.count('_')

        if wildcard_count > 2:
            return target == StringVal(pattern)

        if '%' in pattern and '_' not in pattern:
            # PREFIX pattern: abc%
            if pattern.endswith('%') and pattern.count('%') == 1:
                prefix = pattern[:-1]
                return PrefixOf(StringVal(prefix), target)

            # CONTAINS: %abc%
            if pattern.startswith('%') and pattern.endswith('%') and pattern.count('%') == 2:
                mid = pattern[1:-1]
                return Contains(target, StringVal(mid))

            # SUFFIX: %abc
            if pattern.startswith('%') and pattern.count('%') == 1:
                suffix = pattern[1:]
                return SuffixOf(StringVal(suffix), target)

        # EXACTLY ONE '_' wildcard
        if '_' in pattern and '%' not in pattern and wildcard_count == 1:
            parts = pattern.split('_')
            regex = None
            for i, p in enumerate(parts):
                r = Re(StringVal(p))
                regex = r if regex is None else Concat(regex, r)
                if i < len(parts) - 1:
                    regex = Concat(regex, AllChar(r.sort()))
            return InRe(target, regex)

        return target == StringVal(pattern)

    # --- EXISTS ---
    elif isinstance(expr, exp.Exists):
        return fresh_symbol('subq_exists', 'bool')

    # --- SUBQUERY ---
    elif isinstance(expr, exp.Subquery):
        sort = infer_subquery_sort_from_parent(expr)
        if sort == 'int':
            return fresh_symbol('subq_val', 'int')
        elif sort == 'real':
            return fresh_symbol('subq_val', 'real')
        elif sort == 'string':
            return fresh_symbol('subq_val', 'string')
        else:
            return fresh_symbol('subq_bool', 'bool')

    # --- Fallback ---
    return BoolVal(True)


# ----------------------------------------------------------------------
#  Formula checking
# ----------------------------------------------------------------------

def check_formula(expr) -> str:
    '''Check if the given SQLGlot expression is a tautology, contradiction, or contingent.'''
    
    formula = sql_to_z3(expr, {})
    
    if formula is None:
        return 'unknown'

    solver = Solver()

    solver.push()
    solver.add(formula)

    if solver.check() == unsat:
        return 'contradiction'

    solver.pop()
    solver.push()
    solver.add(Not(formula))

    if solver.check() == unsat:
        return 'tautology'

    return 'contingent'

def is_satisfiable(expr_z3) -> bool:

    solver = Solver()
    solver.add(expr_z3)
    result = solver.check() != unsat

    return result

def is_bool_expr(e) -> bool:
    return is_expr(e) and e.sort().kind() == BoolSort().kind()
