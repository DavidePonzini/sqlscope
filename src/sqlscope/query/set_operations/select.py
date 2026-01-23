from .set_operation import SetOperation
from .. import extractors
from ..tokenized_sql import TokenizedSQL
from ..typechecking import get_type, rewrite_expression
from ... import util
from ...catalog import Catalog, Table, ConstraintColumn, Constraint, ConstraintType

import sqlglot
import sqlglot.errors
from sqlglot import exp
import re
from copy import deepcopy


class Select(SetOperation, TokenizedSQL):
    '''Represents a single SQL SELECT statement.'''

    def __init__(self,
                 query: str,
                 *,
                 catalog: Catalog = Catalog(),
                 search_path: str = 'public',
                 parent_query: 'Select | None' = None,
        ) -> None:
        '''
        Initializes a SelectStatement object.

        Args:
            query (str): The SQL query string to tokenize.
            catalog_db (Catalog): The database catalog for resolving table and column names.
            catalog_query (Catalog): The query-specific catalog for resolving table and column names.
            search_path (str): The search path for schema resolution.
            parent (TokenizedSQL | None): The parent TokenizedSQL object if this is a subquery.
        '''

        SetOperation.__init__(self, query, parent_query=parent_query)
        TokenizedSQL.__init__(self, query)

        self.catalog = catalog
        '''Catalog representing tables that can be referenced in this query.'''
        
        self.search_path = search_path

        # Initialize cached properties
        self._subqueries: list[tuple[Select, str]] | None = None
        self._referenced_tables: list[Table] | None = None
        self._all_constraints: list[Constraint] | None = None
        self._output_table: Table | None = None # half-computed output table, missing constraints
        self._output: Table | None = None       # fully computed output table with constraints

        try:
            self.ast = sqlglot.parse_one(self.sql)
        except sqlglot.errors.ParseError:
            self.ast = None  # Empty expression on parse error

        if self.ast is not None:
            try:
                self.typed_ast = rewrite_expression(deepcopy(self.ast), self.catalog, search_path=self.search_path)
            except sqlglot.errors.OptimizeError:
                self.typed_ast = None
        else:
            self.typed_ast = None        
        
    # region Auxiliary
    def _get_referenced_tables(self) -> list[Table]:
        '''Extracts referenced tables from the SQL query and returns them as a Catalog object.'''

        result: list[Table] = []

        if not self.ast:
            return result

        def add_tables_from_expression(expr: exp.Expression) -> None:
            '''Recursively adds tables from an expression to the result catalog.'''
            # Subquery: get its output columns
            if isinstance(expr, exp.Subquery):
                table_name_out = util.ast.subquery.get_name(expr)

                subquery_sql = expr.this.sql()
                subquery = Select(subquery_sql, catalog=self.catalog, search_path=self.search_path)

                result.append(subquery.output)
            
            # Table: look it up in the IN catalog
            elif isinstance(expr, exp.Table):
                # schema name
                schema_name = util.ast.table.get_schema(expr) or self.search_path
                table_name_in = util.ast.table.get_real_name(expr)
                table_name_out = util.ast.table.get_name(expr)

                # check if the table exists in the catalog
                if self.catalog.has_table(schema_name=schema_name, table_name=table_name_in):
                    # Table exists
                    table_in = self.catalog[schema_name][table_name_in]

                    # Create a copy of the table with the output name
                    table = deepcopy(table_in)
                    table.name = table_name_out
                    result.append(table)
                else:
                    # Table does not exist, add as empty table
                    table = Table(name=table_name_in, schema_name=schema_name)
                    table.name = table_name_out
                    result.append(table)
        
        from_expr = self.ast.args.get('from_')
        

        if from_expr:
            add_tables_from_expression(from_expr.this)
            
        for join in self.ast.args.get('joins', []):
            add_tables_from_expression(join.this)

        return result

    def _get_table_idx_for_column(self, column: exp.Column) -> int | None:
        '''Returns the index of the table that contains the given column.'''

        table_name = util.ast.column.get_table(column)
        col_name = column.alias_or_name
        name = col_name if column.this.quoted else col_name.lower()

        # Resolve which table this column belongs to
        if table_name:
            table_idx = next((i for i, t in enumerate(self.referenced_tables) if t.name == table_name), None)
        else:
            table_idx = next((i for i, t in enumerate(self.referenced_tables)
                            if any(c.name == name for c in t.columns)), None)
        
        return table_idx

    def _get_output_table(self) -> Table:
        '''
        Returns a Table object representing the output of this SELECT query.
        Constraints are not yet computed.
        '''

        if self._output_table is None:
            result = Table('', schema_name=self.search_path)
            if not self.typed_ast:
                self._output_table = result
                return self._output_table

            anonymous_counter = 1

            # ----------------------------------------------------------------------
            # Helper functions
            # ----------------------------------------------------------------------

            def get_anonymous_column_name() -> str:
                '''Generates a unique anonymous column name.'''
                nonlocal anonymous_counter
                name = f'?column_{anonymous_counter}?'
                anonymous_counter += 1
                return name

            def add_star() -> None:
                '''Expand SELECT * by adding all columns from all referenced tables.'''
                for idx, table in enumerate(self.referenced_tables):
                    for col in table.columns:
                        result.add_column(
                            name=col.name,
                            table_idx=idx,
                            column_type=col.column_type,
                            is_nullable=col.is_nullable,
                            is_constant=col.is_constant
                        )

            def add_alias(column: exp.Alias) -> None:
                '''Add an expression with an explicit alias (e.g. SELECT expr AS alias).'''
                alias = column.args['alias']
                name = alias.this if alias.quoted else alias.this.lower()
                real_name = None

                if isinstance(column.this, exp.Column):
                    # If the aliased expression is a column, resolve its table index
                    real_name = util.ast.column.get_real_name(column.this)
                    table_idx = self._get_table_idx_for_column(column.this)
                else:
                    table_idx = None
                res_type = get_type(column.this, catalog=self.catalog, search_path=self.search_path)

                result.add_column(
                    name=name,
                    real_name=real_name,
                    table_idx=table_idx,
                    column_type=res_type.data_type_str,
                    is_nullable=res_type.nullable,
                    is_constant=res_type.constant
                )

            def add_table_star(column: exp.Column) -> None:
                '''Add all columns from a specific table (SELECT table.*).'''
                table_name = util.ast.column.get_table(column)
                table = next((t for t in self.referenced_tables if t.name == table_name), None)
                if table:
                    for col in table.columns:
                        res_type = get_type(col, catalog=self.catalog, search_path=self.search_path)

                        result.add_column(
                            name=col.name,
                            table_idx=self.referenced_tables.index(table),
                            column_type=res_type.data_type_str,
                            is_nullable=res_type.nullable,
                            is_constant=res_type.constant
                        )

            def add_column(column: exp.Column) -> None:
                '''Add a column reference (SELECT column or table.column).'''
                col_name = column.alias_or_name
                name = col_name if column.this.quoted else col_name.lower()
                table_idx = self._get_table_idx_for_column(column)
                res_type = get_type(column, catalog=self.catalog, search_path=self.search_path)
                
                result.add_column(
                    name=name,
                    table_idx=table_idx,
                    column_type=res_type.data_type_str,
                    is_nullable=res_type.nullable,
                    is_constant=res_type.constant
                )

            def add_subquery(column: exp.Subquery) -> None:
                '''Add a column derived from a subquery expression (SELECT (SELECT ...)).'''
                subq = Select(util.sql.remove_parentheses(column.sql()), catalog=self.catalog, search_path=self.search_path)
                
                # Add the first column of the subquery's output
                if subq.output.columns:
                    first_col = subq.output.columns[0]
                    res_type = get_type(first_col, catalog=self.catalog, search_path=self.search_path)
                
                    result.add_column(
                        name=first_col.name,
                        column_type=res_type.data_type_str,
                        is_nullable=res_type.nullable,
                        is_constant=res_type.constant
                    )
                else:
                    result.add_column(name=get_anonymous_column_name(), column_type='None')

            def add_literal(column: exp.Literal | exp.Expression) -> None:
                '''Add a literal or computed expression as a pseudo-column (e.g. SELECT 1, SELECT a+b).'''
                res_type = get_type(column, catalog=self.catalog, search_path=self.search_path)

                result.add_column(
                    name=get_anonymous_column_name(),
                    column_type=res_type.data_type_str,
                    is_nullable=res_type.nullable,
                    is_constant=res_type.constant
                )

            def add_function(column: exp.Func) -> None:
                '''Add a function output column (e.g. SELECT MAX(col)).'''
                res_type = get_type(column, catalog=self.catalog, search_path=self.search_path)

                result.add_column(
                    name=get_anonymous_column_name(),
                    column_type=res_type.data_type_str,
                    is_nullable=res_type.nullable,
                    is_constant=res_type.constant
                )

            # ----------------------------------------------------------------------
            # Main column extraction loop
            # ----------------------------------------------------------------------

            for expr in self.typed_ast.expressions:
                if isinstance(expr, exp.Star):
                    add_star()
                elif isinstance(expr, exp.Alias):
                    add_alias(expr)
                elif isinstance(expr, exp.Column):
                    # Handle SELECT table.* separately
                    if isinstance(expr.this, exp.Star):
                        add_table_star(expr)
                    else:
                        add_column(expr)
                elif isinstance(expr, exp.Subquery):
                    add_subquery(expr)
                elif isinstance(expr, exp.Literal):
                    add_literal(expr)
                elif isinstance(expr, exp.Func):
                    add_function(expr)
                else:
                    add_literal(expr)  # fallback for other expressions
                
            self._output_table = result

        return self._output_table
    # endregion

    def strip_subqueries(self, replacement: str = 'NULL') -> 'Select':
        '''Returns the SQL query with all subqueries removed (replaced by a context-aware placeholder).'''

        stripped_sql = self.sql

        subquery_sqls = extractors.extract_subqueries_tokens(self.sql)

        counter = 1
        for subquery_sql, clause in subquery_sqls:
            repl = replacement  # default safe fallback

            clause_upper = (clause or '').upper()

            if clause_upper in ('FROM', 'JOIN'):
                repl = f'__subq{counter}'
                counter += 1
            elif clause_upper in ('WHERE', 'HAVING', 'ON', 'SELECT', 'COMPARISON'):
                repl = replacement
            elif clause_upper in ('IN', 'EXISTS', 'ALL/ANY'):
                repl = f'({replacement})'

            escaped = re.escape(subquery_sql)
            pattern = rf'\(\s*{escaped}\s*\)'

            # Replace the parentheses and enclosed subquery entirely
            stripped_sql, n = re.subn(pattern, repl, stripped_sql, count=1)

            # Fallback: if not found with parentheses, remove raw subquery text
            if n == 0:
                stripped_sql = re.sub(escaped, repl, stripped_sql, count=1)

        return Select(stripped_sql, catalog=self.catalog, search_path=self.search_path, parent_query=self.parent_query)

    def get_join_conditions(self) -> list[exp.Expression]:
        '''Returns a list of join conditions used in the main query.'''
        if not self.ast:
            return []

        join_conditions = []
        for join in self.ast.args.get('joins', []):
            on_condition = join.args.get('on')
            if on_condition:
                join_conditions.append(on_condition)
        
        return join_conditions
    
    def get_join_equalities(self) -> list[tuple[exp.Column, exp.Column]]:
        '''Returns a list of join equality conditions used in the main query.'''
        result: list[tuple[exp.Column, exp.Column]] = []

        def extract_column_equalities(expr: exp.Expression) -> list[tuple[exp.Column, exp.Column]]:
            equalities = []
            conjuncts = util.ast.extract_CNF(expr)
            for conj in conjuncts:
                if isinstance(conj, exp.EQ):
                    left = conj.args.get('this')
                    right = conj.args.get('expression')
                    if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                        equalities.append((left, right))
            return equalities

        for join_condition in self.get_join_conditions():
            result.extend(extract_column_equalities(join_condition))

        if self.where:
            result.extend(extract_column_equalities(self.where))

        return result

    # region Properties
    
    # NOTE: should this return a SetOp? What if the subquery is a UNION?
    @property
    def subqueries(self) -> list[tuple['Select', str]]:
        '''
            Returns a list of subqueries as TokenizedSQL objects.
        
            Returns:
                list[tuple[Select, str]]: A list of tuples containing subquery Select objects and their associated clause.
        '''
        if self._subqueries is None:
            self._subqueries = []
            # try to find subqueries via sqlglot AST, since it's more reliable
            # if self.ast:
            #     subquery_asts = extractors.extract_subqueries_ast(self.ast)
            #     for subquery_ast in subquery_asts:
            #         while not isinstance(subquery_ast, exp.Select):
            #             subquery_ast = subquery_ast.this
            #         subquery = Select(subquery_ast.sql(), catalog=self.catalog, search_path=self.search_path)
            #         self._subqueries.append(subquery)
            # else:
                # fallback: AST cannot be constructed, try to find subqueries via sqlparse
            subquery_sqls = extractors.extract_subqueries_tokens(self.sql)

            for subquery_sql, clause in subquery_sqls:
                updated_catalog = self.catalog.copy()
                # Include selected tables from the main query into the subquery's catalog (i.e. for correlated subqueries)
                for table in self.referenced_tables:
                    if not updated_catalog.has_table(schema_name=table.schema_name, table_name=table.name):
                        updated_catalog[self.search_path][table.name] = table

                subquery = Select(subquery_sql, catalog=updated_catalog, search_path=self.search_path, parent_query=self)
                self._subqueries.append((subquery, clause))
    
        return self._subqueries

    @property
    def distinct(self) -> bool:
        '''Returns True if the main query has a DISTINCT clause.'''
        if self.ast and self.ast.args.get('distinct', False):
            return True
        return False
    
    @property
    def referenced_tables(self) -> list[Table]:
        if self._referenced_tables is None:
            self._referenced_tables = self._get_referenced_tables()

            if self.parent_query is not None:
                # Include tables from parent query (for correlated subqueries)
                for table in self.parent_query.referenced_tables:
                    if not any(t.name == table.name and t.schema_name == table.schema_name for t in self._referenced_tables):
                        self._referenced_tables.append(table)
        
        return self._referenced_tables
    
    @property
    def all_constraints(self) -> list[Constraint]:
        '''
        Merge unique constraints from all referenced tables.
        When multiple tables are joined, constraints are combined
        by unioning column sets across participating tables.
        '''
        if self._all_constraints is None:
            self._all_constraints = []

            tables = self.referenced_tables
            if not tables:
                return self._all_constraints

            all_constraints = [t.unique_constraints for t in tables]
            
            # Assign base table index
            for constraint in all_constraints[0]:
                c = Constraint()
                self._all_constraints.append(c)
                
                for constraint_column in constraint.columns:
                    c.columns.add(ConstraintColumn(constraint_column.name, table_idx=0))


            # Combine constraints across tables (Cartesian merge)
            for table_idx, constraints in enumerate(all_constraints[1:], start=1):
                merged: list[Constraint] = []

                for c1 in self._all_constraints:
                    for c2 in constraints:
                        c = Constraint()

                        for constraint_column in c2.columns:
                            c.columns.add(ConstraintColumn(constraint_column.name, table_idx=table_idx))
                        merged.append(Constraint(c1.columns.union(c.columns)))

                self._all_constraints = merged

            # If DISTINCT is present, the entire output is unique
            if self.distinct:
                uc_cols = { ConstraintColumn(col.name, col.table_idx) for col in self._get_output_table().columns }
                self._all_constraints.append(Constraint(uc_cols, constraint_type=ConstraintType.DISTINCT))

            # If GROUP BY is present, treat grouped columns as unique.
            if self.group_by:
                group_by_cols: set[ConstraintColumn] = set()
                for col in self.group_by:
                    if isinstance(col, exp.Column):
                        col_name = util.ast.column.get_real_name(col)

                        # Resolve which table this column belongs to
                        table_idx = self._get_table_idx_for_column(col)
                        
                        group_by_cols.add(ConstraintColumn(col_name, table_idx))
                # Add GROUP BY constraint
                self._all_constraints.append(Constraint(group_by_cols, constraint_type=ConstraintType.GROUP_BY))

        return self._all_constraints

    @property
    def output(self) -> Table:
        '''
        Returns a Table object representing the output of this SELECT query.
        It includes inferred columns (name, type, nullability, constancy)
        and merged unique constraints from referenced tables.
        '''
        if self._output is None:
            result = self._get_output_table()
            
            def build_equality_groups(equalities: list[tuple[ConstraintColumn, ConstraintColumn]]) -> list[set[ConstraintColumn]]:
                '''Given a list of equality pairs, return transitive closure groups.'''
                parent = {}

                def find(x):
                    while parent[x] != x:
                        parent[x] = parent[parent[x]]
                        x = parent[x]
                    return x

                def union(x, y):
                    rx, ry = find(x), find(y)
                    if rx != ry:
                        parent[ry] = rx

                # Initialize all columns
                for l, r in equalities:
                    parent.setdefault(l, l)
                    parent.setdefault(r, r)

                # Union connected columns
                for l, r in equalities:
                    union(l, r)

                # Group by representative
                groups = {}
                for col in parent:
                    root = find(col)
                    groups.setdefault(root, set()).add(col)

                return list(groups.values())

            def filter_valid_constraints() -> list[Constraint]:
                constraints: list[Constraint] = []
                all_constraints = self.all_constraints

                equalities = self.get_join_equalities()
                if equalities:
                    def resolve(col):
                        col_name = util.ast.column.get_real_name(col)
                        table_idx = self._get_table_idx_for_column(col)
                        
                        return ConstraintColumn(col_name, table_idx)
                    
                    # Normalize all equalities as UniqueConstraintColumns
                    uc_equalities = [(resolve(left_col), resolve(right_col)) for left_col, right_col in equalities]

                    # Compute transitive closure (equivalence classes)
                    equality_groups = build_equality_groups(uc_equalities)

                    # For each constraint, if it contains any member of a group, extend it with the others
                    new_constraints: list[Constraint] = []
                    for constraint in all_constraints:
                        expanded_columns = set(constraint.columns)
                        for group in equality_groups:
                            if not expanded_columns.isdisjoint(group):
                                expanded_columns |= group
                        new_constraints.append(Constraint(expanded_columns))
                    all_constraints = new_constraints

                    # Merge overlapping sets
                    new_constraints: list[Constraint] = []
                    for equality_group in equality_groups:
                        for col in equality_group:
                            for constraint in all_constraints:
                                if equality_group.isdisjoint(constraint.columns):
                                    continue

                                # For each column in the equality group, create a new constraint with all equivalences replaced by that column
                                new_constraints.append(Constraint(constraint.columns - equality_group | { col }))

                    all_constraints = new_constraints

                # Keep only constraints that are valid for the output columns
                for unique_constraint in all_constraints:
                    valid = all(
                        any(output_column.table_idx == constraint_column.table_idx and output_column.name == constraint_column.name
                            for output_column in result.columns)
                        for constraint_column in unique_constraint.columns
                    )
                    if valid:
                        constraints.append(unique_constraint)

                return constraints

            # ----------------------------------------------------------------------
            # Merge and attach unique constraints
            # ----------------------------------------------------------------------

            result.unique_constraints = filter_valid_constraints()
            self._output = result

        return self._output
    
    @property
    def where(self) -> exp.Expression | None:
        if not self.ast:
            return None
        where = self.ast.args.get('where')
        if not where:
            return None
        
        return where.this

    @property
    def group_by(self) -> list[exp.Expression]:
        if not self.ast:
            return []
        group = self.ast.args.get('group')
        if not group:
            return []
        
        return group.expressions

    @property
    def having(self) -> exp.Expression | None:
        if not self.ast:
            return None
        having = self.ast.args.get('having')
        if not having:
            return None
        
        return having.this
    
    @property
    def order_by(self) -> list[exp.Expression]:
        if not self.ast:
            return []
        order = self.ast.args.get('order')
        if not order:
            return []

        return order.expressions

    @property
    def limit(self) -> int | None:
        if not self.ast:
            return None
        limit_exp = self.ast.args.get('limit')
        if not limit_exp:
            return None
        try:
            return int(limit_exp.expression.this)
        except ValueError:
            return None
        
    @property
    def offset(self) -> int | None:
        if not self.ast:
            return None
        offset_exp = self.ast.args.get('offset')
        if not offset_exp:
            return None
        try:
            return int(offset_exp.expression.this)
        except ValueError:
            return None
        
    @property
    def main_selects(self) -> list['Select']:
        return [self]
    
    @property
    def selects(self) -> list['Select']:
        return [self] + [subquery for subquery, _ in self.subqueries]

    # endregion

    # region Methods
    def print_tree(self, pre: str = '') -> None:
        if self.parsed:
            self.parsed._pprint_tree(_pre=pre)

    def __repr__(self, pre: str = '') -> str:
        return f'{pre}{self.__class__.__name__}(SQL="{self.sql.splitlines()[0]}{"..." if len(self.sql.splitlines()) > 1 else ""}")'
    # endregion
