from dataclasses import dataclass, field
from enum import Enum

class ConstraintType(Enum):
    UNIQUE = 'UNIQUE'
    PRIMARY_KEY = 'PRIMARY_KEY'
    GROUP_BY = 'GROUP_BY'
    DISTINCT = 'DISTINCT'
    SET_OP = 'SET_OP'

@dataclass(frozen=True)
class ConstraintColumn:
    '''Represents a column that is part of a unique constraint.'''
    
    name: str
    '''Name of the column.'''

    table_idx: int | None = None
    '''Index of the table in `referenced_tables`. If None, the column is not associated with a specific table in `referenced_tables`.'''

    def __repr__(self) -> str:
        if self.table_idx is not None:
            return f'{self.table_idx}.{self.name}'
        return self.name

    def to_dict(self) -> dict:
        '''Converts the UniqueConstraintColumn to a dictionary.'''
        return {
            'name': self.name,
            'table_idx': self.table_idx,
        }
    
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ConstraintColumn):
            return False

        return self.name == value.name and self.table_idx == value.table_idx
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ConstraintColumn':
        '''Creates a UniqueConstraintColumn from a dictionary.'''
        return cls(
            name=data['name'],
            table_idx=data.get('table_idx')
        )

@dataclass
class Constraint:
    '''A unique constraint on a set of columns in a table.'''

    columns: set[ConstraintColumn] = field(default_factory=set)
    constraint_type: ConstraintType = ConstraintType.UNIQUE
    '''The type of the unique constraint.'''

    @property
    def is_pk(self) -> bool:
        '''Returns True if the constraint is a primary key.'''
        return self.constraint_type ==  ConstraintType.PRIMARY_KEY

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        return f'{indent}{self.constraint_type.value}({self.columns})'

    def to_dict(self) -> dict:
        '''Converts the UniqueConstraint to a dictionary.'''
        return {
            'columns': [col.to_dict() for col in self.columns],
            'is_pk': self.is_pk,
        }
    
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Constraint):
            return False

        return self.is_pk == value.is_pk and len(self.columns) == len(value.columns) and all(col in value.columns for col in self.columns)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Constraint':
        '''Creates a UniqueConstraint from a dictionary.'''
        return cls(
            columns={ConstraintColumn.from_dict(col) for col in data['columns']},
            constraint_type=ConstraintType.PRIMARY_KEY if data['is_pk'] else ConstraintType.UNIQUE
        )
