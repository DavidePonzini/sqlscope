def split_search_path(schema_name: str) -> list[str]:
    '''Splits a comma-separated schema search path into schema names.'''

    return [name.strip() for name in schema_name.split(',')]