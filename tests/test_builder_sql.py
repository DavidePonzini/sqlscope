import pytest
from sqlscope import build_catalog_from_sql

@pytest.mark.parametrize('file_path', [
    'tests/datasets/unicorsi.sql',
])
def test_parse_sql_file(file_path):
    with open(file_path, 'r') as f:
        sql_string = f.read()
    
    # Should not raise any exceptions
    build_catalog_from_sql(sql_string)


@pytest.mark.parametrize('sql_string', [
    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100)",
])
def test_parse_invalid_sql(sql_string):
    with pytest.raises(Exception):
        build_catalog_from_sql(sql_string)