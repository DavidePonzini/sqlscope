from .constraint import Constraint, ConstraintColumn, ConstraintType
from .column import Column

from dataclasses import dataclass, field

@dataclass
class Table:
    '''A database table, with columns and unique constraints. Supports multiple columns with the same name (e.g. from joins).'''

    name: str
    real_name: str = field(init=False)
    schema_name: str
    cte_idx: int | None = None
    unique_constraints: list[Constraint] = field(default_factory=list)
    columns: list[Column] = field(default_factory=list)

    def __post_init__(self):
        self.real_name = self.name

    def add_unique_constraint(self, columns: set[str], constraint_type: ConstraintType = ConstraintType.UNIQUE) -> None:
        '''Adds a unique constraint to the table.'''
        self.unique_constraints.append(Constraint({ConstraintColumn(name=col) for col in columns}, constraint_type=constraint_type))

    def add_column(self,
                   name: str,
                   column_type: str,
                   real_name: str | None = None,
                   table_idx: int | None = None,
                   numeric_precision: int | None = None,
                   numeric_scale: int | None = None,
                   is_nullable: bool = True,
                   is_constant: bool = False,
                   fk_schema: str | None = None,
                   fk_table: str | None = None,
                   fk_column: str | None = None) -> Column:
        '''Adds a column to the table and returns it.'''
        column = Column(name=name,
                        column_type=column_type,
                        table_idx=table_idx,
                        numeric_precision=numeric_precision,
                        numeric_scale=numeric_scale,
                        is_nullable=is_nullable,
                        is_constant=is_constant,
                        fk_schema=fk_schema,
                        fk_table=fk_table,
                        fk_column=fk_column)
        
        if real_name is not None:
            column.real_name = real_name
            
        self.columns.append(column)
        return column
    
    def has_column(self, column_name: str) -> bool:
        '''Checks if a column exists in the table.'''
        return any(col.name == column_name for col in self.columns)

    def __getitem__(self, column_name: str) -> Column:
        '''Gets a column from the table, creating it if it does not exist.'''
        for col in self.columns:
            if col.name == column_name:
                return col

        new_col = Column(name=column_name)
        self.columns.append(new_col)
        return new_col

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level

        columns = '\n'.join([col.__repr__(level + 1) for col in self.columns])
        if len(repr(self.unique_constraints)) < 80:
            unique_constraints_str = ', '.join([uc.__repr__(0) for uc in self.unique_constraints])
        else:
            unique_constraints_str = '\n' + '\n'.join([uc.__repr__(level + 1) for uc in self.unique_constraints]) + '\n' + indent
        
        if len(self.columns) > 0:
            columns = '\n' + columns + '\n' + indent

        cte_idx = f'cte_idx={self.cte_idx}, ' if self.cte_idx is not None else ''
        schema_name = f'schema_name=\'{self.schema_name}\', ' if self.schema_name is not None else ''
        
        return f'{indent}Table(' \
                f'name=\'{self.name}\', ' \
                f'real_name=\'{self.real_name}\', ' \
                f'{schema_name}' \
                f'{cte_idx}' \
                f'columns=[{columns}], ' \
                f'unique_constraints=[{unique_constraints_str}])'

    def to_dict(self) -> dict:
        '''Converts the Table to a dictionary.'''
        return {
            'name': self.name,
            'unique_constraints': [uc.to_dict() for uc in self.unique_constraints],
            'columns': [col.to_dict() for col in self.columns],
        }

    @classmethod
    def from_dict(cls, data: dict, schema_name: str) -> 'Table':
        '''Creates a Table from a dictionary.'''
        table = cls(name=data['name'], schema_name=schema_name)
        # Unique constraints first (so Column.is_pk works immediately on repr, etc.)
        for uc_data in data.get('unique_constraints', []):
            uc = Constraint.from_dict(uc_data)
            table.unique_constraints.append(uc)
        # Columns
        for col_data in (data.get('columns') or []):
            col = Column.from_dict(col_data)
            table.columns.append(col)
        return table
    