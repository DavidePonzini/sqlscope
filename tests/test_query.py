import pytest
from sqlscope import load_catalog
from sqlscope.query import *
from sqlscope.catalog import Constraint, ConstraintColumn

# region CTEs
def test_main_query_no_cte():
    sql = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    query = Query(sql)

    assert query.main_query.sql == sql

def test_ctes_no_cte():
    sql = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    query = Query(sql)

    assert len(query.ctes) == 0

def test_main_query_with_cte():
    sql_cte = 'WITH cte AS (SELECT id, name FROM users)'

    sql_main = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'
    query = Query(f'{sql_cte} {sql_main}')

    assert query.main_query.sql == sql_main

def test_ctes_with_cte():
    sql_cte = 'SELECT id, name FROM users'

    sql = f'WITH cte AS ({sql_cte}) SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    query = Query(sql)

    assert len(query.ctes) == 1
    assert query.ctes[0].sql == sql_cte

@pytest.mark.parametrize('sql, expected_ctes, expected_main_query', [
    ("WITH cte1 AS (SELECT * FROM table1), cte2 AS (SELECT * FROM table2) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id",
     ['SELECT * FROM table1', 'SELECT * FROM table2'], 'SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id'),
    ("SELECT * FROM table;", [], 'SELECT * FROM table'),
    ("WITH cte AS (SELECT a FROM b) SELECT * FROM cte", ['SELECT a FROM b'], 'SELECT * FROM cte')
])
def test_query_cte_extraction(sql, expected_ctes, expected_main_query):
    query = Query(sql)
    assert [cte.sql.strip() for cte in query.ctes] == expected_ctes
    assert query.main_query.sql.strip() == expected_main_query


# endregion

# region Properties
@pytest.mark.parametrize('sql', [
    'SELECT DISTINCT id, name FROM users',
    'SELECT DISTINCT t1.id FROM t1 JOIN t2 ON t1.id = t2.id',
    'SELECT DISTINCT name FROM t1 JOIN t2 ON t1.id = t2.id'
])
def test_distinct_true(sql):
    query = Query(sql)

    assert isinstance(query.main_query, Select)
    assert query.main_query.distinct is True

def test_distinct_false():
    sql = 'SELECT id, name FROM users'

    query = Query(sql)

    assert isinstance(query.main_query, Select)
    assert query.main_query.distinct is False

def test_select_star():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'

    sql = f'SELECT * FROM {table}'

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == len(query.catalog[db][table].columns)

def test_select_multiple_stars():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'

    sql = f'SELECT *,* FROM {table}'

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == len(query.catalog[db][table].columns) * 2

def test_select_star_on_a_cte():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'
    cte_name = 'cte_store'

    sql = f'WITH {cte_name} AS (SELECT sid, sname FROM {table}) SELECT sid,* FROM {cte_name}'

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == 3  # sid + all columns from cte_store (sid, sname)

def test_select_star_on_a_table():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'
    join = 'transaction'

    sql = f"SELECT {table}.*, {join}.date FROM {table} JOIN {join} ON {table}.sid = {join}.sid;"

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == len(catalog_db[db][table].columns) + 1  # sid + all columns from store

# region set_operations

@pytest.mark.xfail(reason="Not implemented yet")
def test_set_operation_order_by_limit_offset_left():
    db = 'miedema'
    catalog_db = load_catalog(f"datasets/catalogs/{db}.json")
    sql = "(SELECT sid,sname FROM store WHERE city = 'Breda' ORDER BY sname LIMIT 3 OFFSET 1) EXCEPT SELECT sid, sname FROM store WHERE city = 'Amsterdam';"

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert isinstance(query.main_query, BinarySetOperation)
    assert isinstance(query.main_query.left, Select)
    assert query.main_query.left.limit == 3
    assert query.main_query.left.offset == 1
    assert query.main_query.left.order_by == [('sname', 'ASC')]

@pytest.mark.xfail(reason="Not implemented yet")
def test_set_operation_order_by_limit_offset_right():
    db = 'miedema'
    catalog_db = load_catalog(f"datasets/catalogs/{db}.json")
    sql = "SELECT sid,sname FROM store WHERE city = 'Breda' EXCEPT SELECT sid, sname FROM store WHERE city = 'Amsterdam' ORDER BY sname LIMIT 3 OFFSET 1;"

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert isinstance(query.main_query, BinarySetOperation)
    assert query.main_query.limit == 3
    assert query.main_query.offset == 1
    assert query.main_query.order_by == [('sname', 'ASC')]

def test_set_operation_intersect_precedence1():
    sql = "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"

    query = Query(sql)

    assert isinstance(query.main_query, Union)
    assert query.main_query.left.sql == "SELECT 1"
    assert query.main_query.right.sql == "SELECT 2 INTERSECT SELECT 3"

    assert isinstance(query.main_query.right, Intersect)
    assert query.main_query.right.left.sql == "SELECT 2"
    assert query.main_query.right.right.sql == "SELECT 3"

def test_set_operation_intersect_precedence2():
    sql = "SELECT 1 INTERSECT SELECT 2 UNION SELECT 3"

    query = Query(sql)

    assert isinstance(query.main_query, Union)
    assert query.main_query.left.sql == "SELECT 1 INTERSECT SELECT 2"
    assert query.main_query.right.sql == "SELECT 3"

    assert isinstance(query.main_query.left, Intersect)
    assert query.main_query.left.left.sql == "SELECT 1"
    assert query.main_query.left.right.sql == "SELECT 2"

def test_selects_single():
    sql = "SELECT 1 FROM t1"

    query = Query(sql)

    assert len(query.selects) == 1
    assert query.selects[0].sql == "SELECT 1 FROM t1"

def test_selects_multiple():
    sql = "SELECT 1 FROM t1 UNION ALL SELECT id FROM t2 INTERSECT SELECT name, last_name FROM users"

    query = Query(sql)

    assert len(query.selects) == 3
    assert query.selects[0].sql == "SELECT 1 FROM t1"
    assert query.selects[1].sql == "SELECT id FROM t2"
    assert query.selects[2].sql == "SELECT name, last_name FROM users"


@pytest.mark.parametrize('sql, expected_selects', [
    ("SELECT a,b FROM table1 WHERE a > (SELECT MAX(a) FROM table2);", 2),
    ("WITH cte AS (SELECT a FROM b) SELECT * FROM cte WHERE a > (SELECT AVG(a) FROM b);", 3),
    ("SELECT * FROM table;", 1),
    ("WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM (SELECT * FROM t2)) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id;", 4)
])
def test_query_selects(sql, expected_selects):
    query = Query(sql)
    assert len(query.selects) == expected_selects, f"Expected {expected_selects} selects, got {len(query.selects)}"

def test_from_subquery_scope_is_left_to_right():
    catalog_db = load_catalog('datasets/catalogs/miedema.json')
    sql = '''
        SELECT *
        FROM customer c,
             (SELECT c.cid AS cid1) sq1,
             (SELECT c.cid AS cid2, sq1.cid1 AS prev_id FROM store s) sq2,
             transaction t
    '''

    query = Query(sql, catalog=catalog_db, search_path='miedema')
    sq1, sq2 = [subquery for subquery, _, _ in query.main_query.subqueries]

    assert {table.name for table in sq1.referenced_tables} == {'c'}
    assert {table.name for table in sq2.referenced_tables} == {'s', 'c', 'sq1'}

def test_select_strip_filters_removes_filter_clauses():
    query = Select(
        'SELECT COUNT(*) FILTER (WHERE amount > 1), '
        'SUM(total) filter (WHERE total IS NOT NULL) '
        'FROM orders'
    )

    stripped = query.strip_filters()

    assert isinstance(stripped, Select)
    assert 'FILTER' not in stripped.sql.upper()
    assert ' '.join(stripped.sql.split()) == 'SELECT COUNT(*) , SUM(total) FROM orders'

def test_select_strip_subqueries_replaces_subqueries_by_clause():
    query = Select(
        'SELECT * FROM (SELECT id FROM users) u '
        'JOIN orders o ON o.user_id IN (SELECT user_id FROM items)'
    )

    stripped = query.strip_subqueries()

    assert isinstance(stripped, Select)
    assert ' '.join(stripped.sql.split()) == (
        'SELECT * FROM __subq1 u JOIN orders o ON o.user_id IN (NULL)'
    )

def test_select_strip_subqueries_respects_min_depth():
    query = Select(
        'SELECT * FROM users '
        'WHERE id IN ('
        'SELECT user_id FROM orders '
        'WHERE amount > (SELECT AVG(amount) FROM orders)'
        ')'
    )

    stripped = query.strip_subqueries(min_depth=2)

    assert ' '.join(stripped.sql.split()) == (
        'SELECT * FROM users WHERE id IN '
        '(SELECT user_id FROM orders WHERE amount > NULL )'
    )

@pytest.mark.parametrize('sql, expected', [
    ("SELECT a,b,c FROM table1 WHERE a > (SELECT MAX(a) FROM table2);", ['SELECT MAX(a) FROM table2']),
    ("SELECT * FROM (SELECT id, name FROM users) AS sub WHERE id IN (SELECT user_id FROM orders);", ['SELECT id, name FROM users', 'SELECT user_id FROM orders']),
    ("SELECT * FROM table;", []),
    ("WITH cte AS (SELECT a FROM b) SELECT * FROM cte WHERE a > (SELECT AVG(a) FROM b);", ['SELECT AVG(a) FROM b']),
    ("select nome, cognome from studenti where not (nome in (select nome from professori) and cognome in (select cognome from professori))", ['select nome from professori', 'select cognome from professori'])
])
def test_extract_subqueries(sql, expected):
    query = Query(sql)
    subqueries = [subquery.sql for subquery, _, _ in query.main_query.selects[0].subqueries]
    assert [subquery.strip() for subquery in subqueries] == expected

def test_natural_join_equalities_simple():
    catalog_db = load_catalog('datasets/catalogs/miedema.json')
    query = Query(
        'SELECT * FROM customer NATURAL JOIN store',
        catalog=catalog_db,
        search_path='miedema',
    )

    assert query.main_query.selects[0].get_natural_join_equalities() == {
        'street': {0, 1},
        'city': {0, 1},
    }

def test_natural_join_equalities_chain_accumulates_previous_natural_joins():
    catalog_db = load_catalog('datasets/catalogs/miedema.json')
    query = Query(
        'SELECT * FROM customer NATURAL JOIN transaction NATURAL JOIN shoppinglist',
        catalog=catalog_db,
        search_path='miedema',
    )

    assert query.main_query.selects[0].get_natural_join_equalities() == {
        'cid': {0, 1, 2},
        'pid': {1, 2},
        'date': {1, 2},
        'quantity': {1, 2},
    }

def test_natural_join_equalities_after_regular_join():
    catalog_db = load_catalog('datasets/catalogs/miedema.json')
    query = Query(
        'SELECT * FROM customer NATURAL JOIN transaction '
        'JOIN product ON transaction.pid = product.pid '
        'NATURAL JOIN shoppinglist',
        catalog=catalog_db,
        search_path='miedema',
    )

    assert query.main_query.selects[0].get_natural_join_equalities() == {
        'cid': {0, 1, 3},
        'pid': {1, 2, 3},
        'date': {1, 3},
        'quantity': {1, 3},
    }

def test_natural_join_equalities_before_regular_join():
    catalog_db = load_catalog('datasets/catalogs/miedema.json')
    query = Query(
        'SELECT * FROM customer NATURAL JOIN store JOIN transaction ON store.sid = transaction.sid',
        catalog=catalog_db,
        search_path='miedema',
    )

    assert query.main_query.selects[0].get_natural_join_equalities() == {
        'street': {0, 1},
        'city': {0, 1},
    }

# TODO: Implement tests for set operations properties
@pytest.mark.xfail(reason="Not yet implemented")
def test_set_operation_properties():
    assert False, 'Not yet implemented'
# endregion

### Helper function ###
def constraint(columns: list[tuple[str, int | None]]) -> Constraint:
    return Constraint(columns={ ConstraintColumn(name=name, table_idx=idx) for name, idx in columns })
#######################

# region Constraints
@pytest.mark.parametrize('sql, catalog, expected_constraints', [
    ("SELECT * FROM store WHERE sid > 10;", 'miedema', [
        constraint([('sid', 0)])
    ]),
    ("SELECT street FROM transaction", 'miedema', [
        # Empy list, no unique constraints on street
    ]),
    ("SELECT * FROM customer, store;", 'miedema', [
        constraint([('cid', 0), ('sid', 1)])
    ]),
    ("SELECT * FROM customer c JOIN store s ON c.cid = s.sid;", 'miedema', [
        constraint([('cid', 0)]), constraint([('sid', 1)])
    ]),
    ("SELECT DISTINCT cid, cname FROM customer;", 'miedema', [
        constraint([('cid', 0)]), constraint([('cid', 0), ('cname', 0)])
    ]),
    ("SELECT * FROM departments d JOIN courses c ON d.id = c.dept_id;", 'constraints', [
        constraint([('id', 1), ('dept_id', 1)]),
        constraint([('id', 1), ('id', 0)]),
        constraint([('title', 1), ('dept_id', 1)]),
        constraint([('title', 1), ('id', 0)]),
        constraint([('name', 0), ('id', 1), ('dept_id', 1)]),
        constraint([('name', 0), ('id', 1), ('id', 0)]),
        constraint([('name', 0), ('title', 1), ('dept_id', 1)]),
        constraint([('name', 0), ('title', 1), ('id', 0)]),
    ]),
    ("SELECT name, dept_id, title FROM departments d JOIN courses c ON d.id = c.dept_id;", 'constraints', [
        constraint([('title', 1), ('dept_id', 1)]),
        constraint([('name', 0), ('title', 1), ('dept_id', 1)]),
    ]),
    ("SELECT cid, cname, count(distinct cname) FROM customer GROUP BY cid, cname;", 'miedema', [
        constraint([('cid', 0), ('cname', 0)]),
        constraint([('cid', 0)]),
    ]),
    ("SELECT DISTINCT cid, cname, count(distinct cname) c FROM customer GROUP BY cid, cname;", 'miedema', [
        constraint([('cid', 0), ('cname', 0), ('c', None)]),
        constraint([('cid', 0), ('cname', 0)]),
        constraint([('cid', 0)]),
    ]),
    ('SELECT DISTINCT name FROM t1 JOIN t2 ON t1.id = t2.id;', 'constraints', [
        constraint([('name', None)]),
    ]),
    ('SELECT DISTINCT c.city FROM customer c JOIN store s ON c.cid = s.sid;', 'miedema', [
        constraint([('city', 0)]),
    ]),
    ('SELECT c.city FROM customer c JOIN store s ON c.cid = s.sid GROUP BY c.city;', 'miedema', [
        constraint([('city', 0)]),
    ]),
    ('SELECT DISTINCT c.city FROM customer c JOIN store s ON c.cid = s.sid GROUP BY c.city;', 'miedema', [
        constraint([('city', 0)]),
        constraint([('city', 0)]),
    ]),
    
])
def test_query_constraints(sql, catalog, expected_constraints):
    catalog_db = load_catalog(f'datasets/catalogs/{catalog}.json')
    query = Query(sql, catalog=catalog_db, search_path=catalog)
    
    output_constraints = query.main_query.output.unique_constraints
    
    # NOTE: we cannot rely on order of constraints, so we check length and presence
    assert len(output_constraints) == len(expected_constraints)
    for expected in expected_constraints:
        assert expected in output_constraints


@pytest.mark.parametrize('sql, schema, expected', [
    (
        'SELECT cid from customer WHERE street = "Main St";',
        'miedema',
        [ ('miedema', 'customer', 'cid') ]
    ),
    (
        'SELECT * from customer JOIN store ON customer.cid = store.sid;',
        'miedema',
        [
            ('miedema', 'customer', 'cid'),
            ('miedema', 'customer', 'cname'),
            ('miedema', 'customer', 'city'),
            ('miedema', 'customer', 'street'),
            ('miedema', 'store', 'sid'),
            ('miedema', 'store', 'sname'),
            ('miedema', 'store', 'city'),
            ('miedema', 'store', 'street'),
        ]
    ),
    (
        # select `t.*`
        'SELECT t.* FROM customer t JOIN store s ON t.cid = s.sid;',
        'miedema',
        [
            ('miedema', 'customer', 'cid'),
            ('miedema', 'customer', 'cname'),
            ('miedema', 'customer', 'city'),
            ('miedema', 'customer', 'street'),
        ]
    ),
    (
        '''
            WITH temp AS (
                SELECT cid c, street + cname AS col2 FROM customer
                INTERSECT
                SELECT sid, 'nn' FROM store
            )

            SELECT DISTINCT c, temp.col2 d, store.street as e FROM temp, store
            INTERSECT
            SELECT tid, (SELECT sname FROM store) FROM transaction
            UNION
            SELECT pid, pname FROM product;
        ''',
        'miedema',
        [ ('miedema', 'customer', 'cid'),
          ('miedema', 'customer', 'street'),
          ('miedema', 'customer', 'cname'),
          ('miedema', 'store', 'sid'),
          ('miedema', 'store', 'street'),
          ('miedema', 'transaction', 'tid'),
          ('miedema', 'store', 'sname'),
          ('miedema', 'product', 'pid'),
          ('miedema', 'product', 'pname'),
        ]
    ),
])
def test_output_columns_source(sql, schema, expected):
    catalog_db = load_catalog(f'datasets/catalogs/{schema}.json')
    query = Query(sql, catalog=catalog_db, search_path=schema)

    output_source = query.output_columns_source

    assert len(output_source) == len(expected)
    for col in expected:
        assert col in output_source
# endregion
