from dataclasses import dataclass, field

@dataclass
class Column:
    '''A database table column, with type and constraints.'''

    name: str
    real_name: str = field(init=False)
    table_idx: int | None = None
    '''Index of the table in `referenced_tables`. If None, the column is not associated with a specific table in `referenced_tables`.'''

    column_type: str = 'UNKNOWN'
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    is_nullable: bool = True
    is_constant: bool = False
    fk_schema: str | None = None
    fk_table: str | None = None
    fk_column: str | None = None

    def __post_init__(self):
        self.real_name = self.name

    @property
    def is_fk(self) -> bool:
        '''Returns True if the column is a foreign key.'''
        return all([self.fk_schema, self.fk_table, self.fk_column])

    def __repr__(self, level: int = 0, max_col_len: int = 20) -> str:
        indent = '  ' * level

        idx_str = f'table_idx={self.table_idx}, ' if self.table_idx is not None else ''
        return f'{indent}Column(' \
                    f'name=\'{self.name}\',{" " * (max_col_len - len(self.name))} ' \
                    f'real_name=\'{self.real_name}\',{" " * (max_col_len - len(self.real_name))} ' \
                    f'{idx_str}' \
                    f'is_fk={self.is_fk}, ' \
                    f'is_nullable={self.is_nullable}, ' \
                    f'is_constant={self.is_constant}, ' \
                    f'type=\'{self.column_type}\'' \
                f')'

    def to_dict(self) -> dict:
        '''Converts the Column to a dictionary.'''
        return {
            'name': self.name,
            'column_type': self.column_type,
            'numeric_precision': self.numeric_precision,
            'numeric_scale': self.numeric_scale,
            'is_nullable': self.is_nullable,
            'fk_schema': self.fk_schema,
            'fk_table': self.fk_table,
            'fk_column': self.fk_column,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Column':
        '''Creates a Column from a dictionary.'''
        return cls(
            name=data['name'],
            column_type=data['column_type'],
            numeric_precision=data.get('numeric_precision'),
            numeric_scale=data.get('numeric_scale'),
            is_nullable=data.get('is_nullable', True),
            fk_schema=(data.get('fk_schema') or None),
            fk_table=(data.get('fk_table') or None),
            fk_column=(data.get('fk_column') or None),
        )
