import pytest
from sqlglot import parse_one
from sqlscope.query.extractors import *

@pytest.mark.parametrize('sql, expected', [
    ("SELECT COUNT(*), AVG(price) FROM store JOIN transaction ON store.sid = transaction.sid;",
     [('COUNT', 'SELECT'), ('AVG', 'SELECT')]),
    ("SELECT sname FROM store JOIN transaction ON store.sid = transaction.sid GROUP BY sname HAVING COUNT(store.sid) > 1;",
     [('COUNT', 'HAVING')]),
    ("SELECT a FROM b WHERE COUNT(a) > 5;",
     [('COUNT', 'WHERE')])
])
def test_extract_functions(sql, expected):
    parsed = sqlparse.parse(sql)[0]
    functions = extract_functions(parsed.tokens)
    assert [(func.get_name(), clause) for func, clause in functions] == expected


@pytest.mark.parametrize('sql, expected', [
    ("SELECT * FROM store WHERE price > 100 AND sid = 5;", [('price > 100', 'WHERE'), ('sid = 5', 'WHERE')]),
    ("SELECT * FROM store JOIN transaction ON store.sid = transaction.sid WHERE price < 50;",
     [('store.sid = transaction.sid', 'FROM'), ('price < 50', 'WHERE')]),
    ("SELECT a > 10 FROM b;", [('a > 10', 'SELECT')])
])
def test_extract_comparisons(sql, expected):
    parsed = sqlparse.parse(sql)[0]
    comparisons = extract_comparisons(parsed.tokens)
    assert [(str(comp).strip(), clause) for comp, clause in comparisons] == expected


@pytest.mark.parametrize('sql, expected', [
    ("SELECT a,b,c FROM table1 WHERE a > (SELECT MAX(a) FROM table2);", [('(SELECT MAX(a) FROM table2)')]),
    ("SELECT * FROM (SELECT id, name FROM users) AS sub WHERE id IN (SELECT user_id FROM orders);",
     ['(SELECT id, name FROM users) AS sub', '(SELECT user_id FROM orders)']),
    ("SELECT * FROM table;", []),
    ("WITH cte AS (SELECT a FROM b) SELECT * FROM cte WHERE a > (SELECT AVG(a) FROM b);",
     ['(SELECT a FROM b)', '(SELECT AVG(a) FROM b)'])
])
def test_extract_subqueries(sql, expected):
    ast = parse_one(sql)
    subqueries = extract_subqueries_ast(ast)
    assert all(subquery.sql() in expected for subquery in subqueries)