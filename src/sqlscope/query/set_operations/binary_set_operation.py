from .set_operation import SetOperation
from ...catalog import Table, Constraint, ConstraintType, ConstraintColumn

from abc import ABC
from copy import deepcopy
import sqlglot
from sqlglot import exp

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .select import Select


class BinarySetOperation(SetOperation, ABC):
    '''Represents a binary set operation (e.g., UNION, INTERSECT, EXCEPT).'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, distinct: bool = True, trailing_sql: str | None = None):
        super().__init__(sql)
        self.left = left
        self.right = right
        self.distinct = distinct
        '''Indicates whether the operation is ALL (duplicates allowed) or DISTINCT (duplicates removed).'''

        self.trailing_sql: str | None = trailing_sql
        '''Trailing SQL clauses (e.g., ORDER BY, LIMIT) applied at this level.'''

        # Cached properties
        self._trailing_ast: exp.Expression | None = None

    def __repr__(self, pre: str = '') -> str:
        modifiers = []

        if not self.distinct:
            modifiers.append('ALL=True')

        if self.order_by:
            modifiers.append(f'ORDER_BY={[col.name for col in self.order_by]}')
        if self.limit is not None:
            modifiers.append(f'LIMIT={self.limit}')
        if self.offset is not None:
            modifiers.append(f'OFFSET={self.offset}')

        result = f'{pre}{self.__class__.__name__}{"(" + ", ".join(modifiers) + ")" if modifiers else ""}\n'
        result +=  self.left.__repr__(pre + '|- ') + '\n'
        result += self.right.__repr__(pre + '`- ')

        return result

    @property
    def trailing_ast(self) -> exp.Expression | None:
        '''Parses and returns the AST of the trailing SQL clauses (e.g., ORDER BY, LIMIT) if present, with a fake `SELECT 1` prefix.'''
        if self.trailing_sql is None:
            return None
        if self._trailing_ast is None:
            # Parse trailing SQL with a fake SELECT to get valid AST
            fake_sql = f'SELECT 1 {self.trailing_sql}'
            parsed = sqlglot.parse_one(fake_sql)
            self._trailing_ast = parsed
        return self._trailing_ast

    @property
    def output(self) -> Table:
        # Assume the output schema is the same as the left input
        result = deepcopy(self.left.output)

        # Remove ALL constraints
        result.unique_constraints = [
            constraint for constraint in result.unique_constraints
            if constraint.constraint_type != ConstraintType.SET_OP
        ]

        # If DISTINCT, add a new constraint covering all columns
        if self.distinct:
            all_columns = { ConstraintColumn(col.name, col.table_idx) for col in result.columns }
            result.unique_constraints.append(Constraint(all_columns, ConstraintType.SET_OP))

        return result
    
    @property
    def referenced_tables(self) -> list[Table]:
        return self.left.referenced_tables

    
    def print_tree(self, pre: str = '') -> None:
        print(f'{pre}{self.__class__.__name__} (ALL={not self.distinct})')
        print(                      f'{pre}|- Left:')
        self.left.print_tree(pre=   f'{pre}|  ')
        print(                      f'{pre}`- Right:')
        self.right.print_tree(pre=  f'{pre}   ')

    @property
    def main_selects(self) -> list['Select']:
        return self.left.main_selects + self.right.main_selects

    @property
    def selects(self) -> list['Select']:
        return self.left.selects + self.right.selects
    
    @property
    def order_by(self) -> list[exp.Expression]:
        if not self.trailing_ast:
            return []
        order = self.trailing_ast.args.get('order')
        if not order:
            return []

        return order.expressions

    @property
    def limit(self) -> int | None:
        if not self.trailing_ast:
            return None
        limit_exp = self.trailing_ast.args.get('limit')
        if not limit_exp:
            return None
        try:
            return int(limit_exp.expression.this)
        except Exception:
            return None
        
    @property
    def offset(self) -> int | None:
        if not self.trailing_ast:
            return None
        offset_exp = self.trailing_ast.args.get('offset')
        if not offset_exp:
            return None
        try:
            return int(offset_exp.expression.this)
        except Exception:
            return None
    

class Union(BinarySetOperation):
    '''Represents a SQL UNION operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, distinct: bool = True, trailing_sql: str | None = None):
        super().__init__(sql, left, right, distinct=distinct, trailing_sql=trailing_sql)

    @property
    def output(self) -> Table:
        # Get output table with ALL constraints
        result = super().output

        # Remove all other constraints, since UNION only guarantees uniqueness based on ALL
        result.unique_constraints = [
            constraint for constraint in result.unique_constraints
            if constraint.constraint_type == ConstraintType.SET_OP
        ]

        return result

class Intersect(BinarySetOperation):
    '''Represents a SQL INTERSECT operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, distinct: bool = True, trailing_sql: str | None = None):
        super().__init__(sql, left, right, distinct=distinct, trailing_sql=trailing_sql)

class Except(BinarySetOperation):
    '''Represents a SQL EXCEPT operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, distinct: bool = True, trailing_sql: str | None = None):
        super().__init__(sql, left, right, distinct=distinct, trailing_sql=trailing_sql)

    

