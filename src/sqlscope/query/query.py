import sqlparse
import sqlparse.tokens
from sqlglot import exp

from .set_operations import SetOperation, BinarySetOperation, Select, create_set_operation_tree
from .tokenized_sql import TokenizedSQL
from ..catalog import Catalog
from .. import util

class Query(TokenizedSQL):
    def __init__(self,
                sql: str,
                *,
                catalog: Catalog = Catalog(),
                search_path: str = 'public'
        ) -> None:
        '''
        Represents a full SQL query, potentially with multiple statements (i.e., CTEs and set operations).
        '''

        super().__init__(sql)

        self.catalog = catalog.copy()
        '''Catalog representing tables that can be referenced in this query.'''
        
        self.search_path = search_path
        '''The search path for resolving unqualified table names.'''

        self._output_columns_source_cache: set[tuple[str, str | None, str]] | None = None
        '''Cache for output_columns_source property.'''

        # Extract CTEs and main query
        self.ctes: list[SetOperation] = []
        cte_tokens = []
        main_query_tokens = []

        is_cte_section = False
        for token in self.parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword.CTE:
                is_cte_section = True
                continue

            if token.ttype is sqlparse.tokens.DML:
                is_cte_section = False

            if not is_cte_section:
                # Collect all tokens for the main query
                main_query_tokens.append(token)
                continue

            # Collect CTE names and their corresponding SQL
            if isinstance(token, sqlparse.sql.IdentifierList):
                # Multiple CTEs
                cte_tokens.extend(token.get_identifiers())

            elif isinstance(token, sqlparse.sql.Identifier):
                # Single CTE
                cte_tokens.append(token)

            elif isinstance(token, sqlparse.sql.Parenthesis) or token.ttype is sqlparse.tokens.Keyword:
                # CTE using a keyword as name (e.g., "WITH temp AS (...)")
                cte_tokens.append(token)

        current_token_index = 0
        while current_token_index < len(cte_tokens):
            cte_token = cte_tokens[current_token_index]
            current_token_index += 1

            if isinstance(cte_token, sqlparse.sql.Identifier):
                # Standard CTE name
                cte_name = str(cte_token.get_name())
                
                cte_parenthesis = next(cte_token.get_sublists())
                if not cte_parenthesis:
                    continue
            else:
                # Fallback for non-standard CTE definitions (e.g., using keywords)

                # In this case we have:
                #  - Keyword (CTE name)             -> use as CTE name    
                #  - Keyword (AS)                   -> skip
                #  - Parenthesis (CTE query)        -> parse as CTE query
                cte_name = str(cte_token)

                # Skip AS token
                if current_token_index < len(cte_tokens) and str(cte_tokens[current_token_index]).upper() == 'AS':
                    current_token_index += 1

                if current_token_index >= len(cte_tokens):
                    continue

                cte_parenthesis = cte_tokens[current_token_index]
                current_token_index += 1

                if not isinstance(cte_parenthesis, sqlparse.sql.Parenthesis):
                    continue

            cte_parenthesis_str = str(cte_parenthesis)[1:-1]  # Remove surrounding parentheses
            cte = create_set_operation_tree(cte_parenthesis_str, search_path=self.search_path, catalog=self.catalog)

            self.ctes.append(cte)

            # Add CTE output columns to catalog
            output = cte.output
            output.name = cte_name
            output.real_name = cte_name
            output.cte_idx = len(self.ctes) - 1

            self.catalog[output.schema_name][cte_name] = output

        main_query_sql = ''.join(str(token) for token in main_query_tokens).strip()
        self.main_query = create_set_operation_tree(main_query_sql, catalog=self.catalog, search_path=self.search_path)

    # region Properties
    @property
    def selects(self) -> list[Select]:
        result: list[Select] = []

        for cte in self.ctes:
            result.extend(cte.selects)

        result.extend(self.main_query.selects)

        return result        
    
    @property
    def output_columns_source(self) -> set[tuple[str, str | None, str]]:
        '''Returns a set of (schema, table, column) tuples representing the source columns for the output of the main query.'''

        if self._output_columns_source_cache is not None:
            return self._output_columns_source_cache

        def _output_columns_source_recursive(so: SetOperation) -> set[tuple[str, str | None, str]]:
            result: set[tuple[str, str | None, str]] = set()

            if isinstance(so, BinarySetOperation):
                result.update(_output_columns_source_recursive(so.left))
                result.update(_output_columns_source_recursive(so.right))
            
            elif isinstance(so, Select):
                if so.ast is None:
                    return result
                
                # First, process subqueries to gather their output columns
                for expression in so.ast.expressions:
                    for subquery in expression.find_all(exp.Subquery):
                        sub_so = create_set_operation_tree(str(subquery.this), catalog=so.catalog, search_path=so.search_path)
                        result.update(_output_columns_source_recursive(sub_so))

                # Then, process regular columns
                no_subqueries = so.strip_subqueries()

                if no_subqueries.ast is None:
                    return result

                for expression in no_subqueries.ast.expressions:
                    for column in expression.find_all(exp.Column):
                        column_name = util.ast.column.get_real_name(column)
                        table_name = util.ast.column.get_table(column)

                        if table_name is None:
                            table_idx = next((idx for idx, table in enumerate(so.referenced_tables)
                                            if so.referenced_tables[idx].has_column(column_name)), None)
                        else:
                            table_idx = next((idx for idx, table in enumerate(so.referenced_tables)
                                            if table.name == table_name), None)
                            
                        if table_idx is not None:
                            table = so.referenced_tables[table_idx]

                            if table.cte_idx is not None:
                                # Column comes from a CTE
                                cte = self.ctes[table.cte_idx]
                                result.update(_output_columns_source_recursive(cte))
                            else:
                                result.add((table.schema_name, table.real_name, column_name))
                        else:
                            # Table not found; assume search path
                            result.add((so.search_path, table_name if table_name is not None else None, column_name))
            
            self._output_columns_source_cache = result
            return result

        return _output_columns_source_recursive(self.main_query)

    # endregion   

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.sql})'
    
    