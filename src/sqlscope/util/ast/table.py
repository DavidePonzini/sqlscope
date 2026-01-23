'''Utility functions related to SQL tables in ASTs made with sqlglot.'''

from sqlglot import exp

def get_real_name(table: exp.Table) -> str:
    '''Returns the table real name, in lowercase if unquoted.'''

    quoted = table.this.quoted
    name = table.this.name

    return name if quoted else name.lower()


def get_name(table: exp.Table) -> str:
    '''Returns the table name or alias, in lowercase if unquoted.'''
    
    if table.args.get('alias'):
        quoted = table.args['alias'].args.get('quoted', False)
        name = table.alias_or_name

        return name if quoted else name.lower()

    return get_real_name(table)

def get_schema(table: exp.Table) -> str | None:
    '''Returns the schema name, in lowercase if unquoted.'''
    
    if table.args.get('db'):
        quoted = table.args['db'].quoted
        name = table.db

        return name if quoted else name.lower()
    
    return None


