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
    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
    "CREATE TABLE orders (order_id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id), amount DECIMAL CHECK (amount > 0))",
    "CREATE TABLE products (product_id SERIAL PRIMARY KEY, product_name VARCHAR(255) UNIQUE, price DECIMAL CHECK (price >= 0))",
    "CREATE TABLE inventory (inventory_id SERIAL PRIMARY KEY); CREATE TABLE stock (stock_id SERIAL PRIMARY KEY, inventory_id SERIAL REFERENCES inventory(inventory_id), product_id SERIAL REFERENCES products(product_id), quantity INT CHECK (quantity >= 0))",
])
def test_parse_valid_sql(sql_string):
    catalog = build_catalog_from_sql(sql_string)

    # TODO: Add assertions to verify the structure of the catalog

@pytest.mark.parametrize('sql_string', [
    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100)",
])
def test_parse_invalid_sql(sql_string):
    with pytest.raises(Exception):
        build_catalog_from_sql(sql_string)

