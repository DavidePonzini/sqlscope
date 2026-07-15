'''Utility functions related to SQL columns in ASTs made with sqlglot.'''

from sqlglot import exp

def get_real_name(column: exp.Column | exp.Alias) -> str:
    '''Returns the column real name, in lowercase if unquoted.'''

    col = column.find(exp.Column)
    if col is None:
        # return column.alias_or_name
        return ''

    quoted = col.this.quoted
    name = col.this.name

    return name if quoted else name.lower()

def get_name(column: exp.Column | exp.Alias) -> str:
    '''Returns the column name or alias, in lowercase if unquoted.'''

    while isinstance(column.this, exp.Alias):
        column = column.this
    
    if column.args.get('alias'):
        quoted = column.args['alias'].args.get('quoted', False)
        name = column.alias_or_name

        return name if quoted else name.lower()

    return get_real_name(column)

def get_table(column: exp.Column | exp.Alias) -> str | None:
    '''Returns the table name or alias for the column, in lowercase if unquoted.'''
    
    col = column.find(exp.Column)
    if col is None:
        return None

    if col.args.get('table'):
        quoted = col.args['table'].quoted
        name = col.table

        return name if quoted else name.lower()
    
    return None

def get_schema(column: exp.Column | exp.Alias) -> str | None:
    '''Returns the schema name for the column, in lowercase if unquoted.'''
    
    col = column.find(exp.Column)
    if col is None:
        return None

    if col.args.get('db'):
        quoted = col.args['db'].quoted
        name = col.db

        return name if quoted else name.lower()
    
    return None


