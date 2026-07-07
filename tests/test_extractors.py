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
