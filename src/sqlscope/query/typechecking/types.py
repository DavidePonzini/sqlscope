from sqlglot.expressions import DataType
from dataclasses import dataclass, field


@dataclass
class ResultType:
    messages: list[tuple[str, str, str | None]] = field(default_factory=list)
    '''List of error messages as tuples containing (expected_type, found_type, sql_snippet)'''
    
    data_type: DataType.Type = DataType.Type.UNKNOWN
    nullable: bool = True
    constant: bool = False
    value: str | None = None

    @property
    def data_type_str(self) -> str:
        return self.data_type.value.lower()

@dataclass
class AtomicType(ResultType):

    def __str__(self) -> str:
        return self.data_type_str

    def __eq__(self, other):
        if not isinstance(other, AtomicType):
            return False
        
        if other.data_type == DataType.Type.UNKNOWN or self.data_type == DataType.Type.UNKNOWN:
            return True

        # handle numeric equivalence (e.g. INT and FLOAT are compatible)
        if self.data_type in DataType.NUMERIC_TYPES:
            return other.data_type in DataType.NUMERIC_TYPES
        
        # handle text equivalence (e.g. VARCHAR and TEXT are compatible)
        if self.data_type in DataType.TEXT_TYPES:
            return other.data_type in DataType.TEXT_TYPES

        # handle temporal equivalence (e.g. DATE and TIMESTAMP are compatible)
        if self.data_type in DataType.TEMPORAL_TYPES:
            return other.data_type in DataType.TEMPORAL_TYPES

        return self.data_type == other.data_type
    
@dataclass
class TupleType(ResultType):

    # we use LIST to represent tuples (since we will never use this constructor)
    data_type: DataType.Type = DataType.Type.LIST 
    types: list[ResultType] = field(default_factory=list)

    def __str__(self) -> str:
        return f"tuple({', '.join(str(target_type) for target_type in self.types)})"

    def __eq__(self, other):
        if not isinstance(other, TupleType):
            return False
        return self.types == other.types