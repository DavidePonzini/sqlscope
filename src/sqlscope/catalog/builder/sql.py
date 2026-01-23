import sqlglot
from sqlglot import exp
from ..catalog import Catalog
from ..constraint import ConstraintType

def _get_identifier_name(identifier_exp: exp.Identifier) -> str:
    '''Returns the normalized name from an Identifier expression.'''

    if identifier_exp.quoted:
        return identifier_exp.name
    return identifier_exp.name.lower()

def _get_table_name(table_exp: exp.Table) -> str:
    '''Returns the normalized table name from a Table expression.'''

    if isinstance(table_exp.this, exp.Identifier):
        return _get_identifier_name(table_exp.this)
    return str(table_exp.this).lower()

def _get_schema_name(table_exp: exp.Table, default_schema: str) -> str:
    '''Returns the normalized schema name from a Table expression, or the default schema if not specified.'''

    if table_exp.db:
        if isinstance(table_exp.db, exp.Identifier):
            return _get_identifier_name(table_exp.db)
        return str(table_exp.db).lower()
    return default_schema

def _get_column_name(column_exp: exp.ColumnDef) -> str:
    '''Returns the normalized column name from a ColumnDef expression.'''

    if isinstance(column_exp.this, exp.Identifier):
        return _get_identifier_name(column_exp.this)
    return str(column_exp.this).lower()

def _extract_datatype(column_exp: exp.ColumnDef) -> tuple[str, int | None, int | None]:
    '''Extracts datatype information from a ColumnDef expression.'''

    datatype_exp = column_exp.kind
    assert isinstance(datatype_exp, exp.DataType), 'Expected DataType expression in ColumnDef'

    datatype = datatype_exp.this.value

    numeric_precision = None
    numeric_scale = None

    if datatype_exp.expressions:
        if datatype in {'DECIMAL', 'NUMERIC', 'NUMBER', 'FLOAT'}:
            if len(datatype_exp.expressions) >= 1:
                precision_exp = datatype_exp.expressions[0]
                if isinstance(precision_exp, exp.Literal) and precision_exp.is_int:
                    numeric_precision = int(precision_exp.name)
            if len(datatype_exp.expressions) == 2:
                scale_exp = datatype_exp.expressions[1]
                if isinstance(scale_exp, exp.Literal) and scale_exp.is_int:
                    numeric_scale = int(scale_exp.name)

    return datatype, numeric_precision, numeric_scale

def build_catalog_from_sql(sql_string: str, search_path: str = 'public') -> Catalog:
    '''Builds a catalog from the provided SQL string without executing it in a database.'''

    statements = sqlglot.parse(sql_string)

    # Filter to only CREATE TABLE statements
    statements = [stmt for stmt in statements if isinstance(stmt, exp.Create) and stmt.kind and stmt.kind.upper() == 'TABLE']

    catalog = Catalog()

    for statement in statements:
        table_exp = statement.find(exp.Table)

        assert table_exp is not None, 'Expected Table expression in CREATE TABLE statement'

        # "CREATE TABLE <schema_name>.<table_name>" handling
        table_name = _get_table_name(table_exp)
        schema_name = _get_schema_name(table_exp, search_path)

        # Extract other relevant information
        column_exps: list[exp.ColumnDef] = list(statement.find_all(exp.ColumnDef))
        '''Column definitions'''

        pk_exp: exp.PrimaryKey | None = statement.find(exp.PrimaryKey)
        '''PRIMARY KEY defined at table level, e.g., PRIMARY KEY (col1, col2)'''

        fk_exps: list[exp.ForeignKey] = list(statement.find_all(exp.ForeignKey))
        '''FOREIGN KEY defined at table level, e.g., FOREIGN KEY (col1) REFERENCES other_table(other_col)'''

        fks: dict[str, tuple[str, str, str]] = {}
        '''Mapping of foreign key column names to (schema, table, column) tuples'''
        # NOTE: this needs to be filled in before adding columns to the catalog
        
        unique_exps: list[exp.UniqueColumnConstraint] = list(statement.find_all(exp.UniqueColumnConstraint))
        '''UNIQUE constraints defined at table level, e.g., UNIQUE (col1, col2)'''

        pk_col_names: set[str] = set()
        '''Set to keep track of primary key column names'''

        unique_col_names: list[set[str]] = []
        '''List to keep track of unique constraint column name sets'''

        # Process table-level Foreign Key constraints
        for fk_exp in fk_exps:
            fk_id_exps = fk_exp.expressions
            fk_column_names = [_get_identifier_name(col_exp) for col_exp in fk_id_exps]

            ref_exp = fk_exp.find(exp.Reference)
            assert ref_exp is not None, 'Expected Reference expression in Foreign Key definition'

            ref_schema_exp = ref_exp.this
            assert isinstance(ref_schema_exp, exp.Schema), 'Expected Schema expression in Foreign Key reference'

            ref_table_exp = ref_schema_exp.this
            assert isinstance(ref_table_exp, exp.Table), 'Expected Table expression in Foreign Key reference'

            ref_schema_name = _get_schema_name(ref_table_exp, search_path)
            ref_table_name = _get_table_name(ref_table_exp)

            ref_id_exps = ref_schema_exp.expressions
            ref_column_names = [_get_identifier_name(col_exp) for col_exp in ref_id_exps]

            # e.g. "FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, order_id)"
            for fk_col_name, ref_col_name in zip(fk_column_names, ref_column_names):
                fks[fk_col_name] = (ref_schema_name, ref_table_name, ref_col_name)

        # Process columns
        for column_exp in column_exps:
            column_name = _get_column_name(column_exp)

            # Primary Key handling            
            is_pk = any(isinstance(c.kind, exp.PrimaryKeyColumnConstraint) for c in column_exp.constraints)
            if is_pk:
                pk_col_names.add(column_name)

            # Unique handling
            is_unique = any(isinstance(c.kind, exp.UniqueColumnConstraint) for c in column_exp.constraints)
            if is_unique:
                unique_col_names.append({column_name})

            # Not Null handling
            is_not_null = any(isinstance(c.kind, exp.NotNullColumnConstraint) for c in column_exp.constraints)

            # Foreign Key handling
            fk_constraint = next((c for c in column_exp.constraints if isinstance(c.kind, exp.Reference)), None)

            if fk_constraint:
                fk_reference = fk_constraint.kind
                assert isinstance(fk_reference, exp.Reference), 'Expected Reference expression in Foreign Key constraint'

                fk_schema_exp = fk_reference.this
                assert isinstance(fk_schema_exp, exp.Schema), 'Expected Schema expression in Foreign Key constraint'
                
                fk_table_exp = fk_schema_exp.this
                assert isinstance(fk_table_exp, exp.Table), 'Expected Table expression in Foreign Key constraint'

                fk_schema_name = _get_schema_name(fk_table_exp, search_path)
                fk_table_name = _get_table_name(fk_table_exp)

                fk_column_exp = fk_schema_exp.expressions[0]
                assert isinstance(fk_column_exp, exp.Identifier), 'Expected Identifier expression in Foreign Key column'
                fk_column_name = _get_identifier_name(fk_column_exp)
            elif column_name in fks:
                fk_schema_name, fk_table_name, fk_column_name = fks[column_name]
            else:
                fk_schema_name = None
                fk_table_name = None
                fk_column_name = None

            # Datatype handling
            column_type, numeric_precision, numeric_scale = _extract_datatype(column_exp)

            # Add column to catalog
            catalog[schema_name][table_name].add_column(
                name=column_name,
                column_type=column_type,
                real_name=column_name,
                numeric_precision=numeric_precision,
                numeric_scale=numeric_scale,
                is_nullable=not is_not_null,
                fk_schema=fk_schema_name,
                fk_table=fk_table_name,
                fk_column=fk_column_name)
        
        # Process table-level Primary Key constraint
        if pk_exp:
            for ordered_exp in pk_exp.expressions:
                col_exp = ordered_exp.find(exp.Column)
                assert col_exp is not None, 'Expected Column expression in Primary Key definition'
                col_name = _get_column_name(col_exp)
                pk_col_names.add(col_name)

        # Process table-level Unique constraints
        for unique_exp in unique_exps:
            unique_schema_exp = unique_exp.this
            assert isinstance(unique_schema_exp, exp.Schema), 'Expected Schema expression in Unique constraint'

            unique_column_names = set()
            for col_id_exp in unique_exp.expressions:
                col_name = _get_identifier_name(col_id_exp)
                unique_column_names.add(col_name)
            unique_col_names.append(unique_column_names)

        # Add Primary Key constraint to catalog
        # NOTE: needs to be perfomed after all columns have been added, since PKs can be defined at both column and table level
        assert len(pk_col_names) > 0, 'Primary Key columns should have been identified'
        catalog[schema_name][table_name].add_unique_constraint(
            columns=pk_col_names,
            constraint_type=ConstraintType.PRIMARY_KEY
        )

        # Add Unique constraints to catalog
        # NOTE: needs to be perfomed after all columns have been added, since Unique constraints can be defined at both column and table level
        for unique_col_name_set in unique_col_names:
            catalog[schema_name][table_name].add_unique_constraint(
                columns=unique_col_name_set,
                constraint_type=ConstraintType.UNIQUE
            )

    return catalog