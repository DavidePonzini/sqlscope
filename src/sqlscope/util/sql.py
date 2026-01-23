'''Utility functions for processing SQL strings.'''

def remove_parentheses(sql: str) -> str:
    '''Remove outer parentheses from a SQL string.'''
    sql = sql.strip()

    # check if the entire string is wrapped in parentheses or if there are inner parentheses
    # i.e., (SELECT 1) UNION (SELECT 2) should not have parentheses removed
    depth = 0
    for idx, char in enumerate(sql):
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
        if depth == 0 and idx < len(sql) - 1:
            return sql  # parentheses are not outermost

    while sql.startswith('(') and sql.endswith(')'):
        sql = sql[1:-1].strip()
    return sql

def normalize_identifier_name(identifier: str) -> str:
    '''Normalize an SQL identifier by stripping quotes and converting to lowercase if unquoted.'''
    if identifier.startswith('"') and identifier.endswith('"') and len(identifier) > 1:
        return identifier[1:-1]
    
    return identifier.lower()