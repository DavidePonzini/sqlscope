.. dav-tools documentation master file, created by
   sphinx-quickstart on Sun Jul 16 15:00:51 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to sqlscope's documentation!
=================================================
This project extracts catalog and query metadata from a SQL query.
Each SELECT statement is parsed as a list of tokens as well as an AST.
The former works even for incomplete or invalid SQL queries, while the latter
requires a valid SQL syntax.
Additionally, catalog metadata is extracted from the database schema.

For each SELECT statement, the package extracts:
- the list of SELECT queries (in case of set operations, nested queries, or ctes)
- the main SELECT query
- the list of schemas/tables/columns available in the catalog
- the list of tables referenced in each query
- the resulting table from each query execution, including its columns and their types
- each clause of each query


Contents
========

.. toctree::
   :maxdepth: 4


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Installation
============
``$ pip install sql_error_categorizer``

