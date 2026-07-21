from enum import StrEnum
from sqlglot.dialects import postgres, mysql
from sqlglot.dialects.dialect import Dialect as sqlglot_Dialect

class Dialect(StrEnum):
    POSTGRES = 'postgres'
    MYSQL = 'mysql'

    def get_sqlglot_dialect(self) -> type[sqlglot_Dialect]:
        if self == Dialect.POSTGRES:
            return postgres.Postgres
        elif self == Dialect.MYSQL:
            return mysql.MySQL
        else:
            raise ValueError(f"Unsupported dialect: {self.value}")