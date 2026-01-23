# Introduction
This project extracts catalog and query metadata from a SQL query.
Each SELECT statement is parsed as a list of tokens as well as an AST.
The former works even for incomplete or invalid SQL queries, while the latter
requires a valid SQL syntax.
Additionally, catalog metadata is extracted from the database schema.

# Details
For each SELECT statement, the package extracts:
- the list of SELECT queries (in case of set operations, nested queries, or ctes)
- the main SELECT query
- the list of schemas/tables/columns available in the catalog
- the list of tables referenced in each query
- the resulting table from each query execution, including its columns and their types
- each clause of each query

The catalog extract schema/table/column metadata.
For each column, the following information are extracted:
- name
- data type
- nullability
- foreign key

Additionally, for each table, PRIMARY KEY/UNIQUE constraints are extracted. This is also computed for the result of each SELECT query. 


# Credits
Special thanks to Flavio Venturini for his valuable contributions to the development of this project.

# Limitations
- Fully identified schema names are not supported when specifying column names (e.g. `SELECT schema.table.column [...]`)
