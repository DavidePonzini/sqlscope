

from dataclasses import dataclass


@dataclass
class Function:
    '''A database function, with a name, arguments, and return type.'''

    name: str
    arguments: list[str]
    return_type: str
    kind: str  # FUNCTION, PROCEDURE, AGGREGATE, WINDOW

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level

        return f'{indent}Function({self.name}({", ".join(self.arguments)}) -> {self.return_type})'

    # region Serialization
    def to_dict(self) -> dict:
        '''Converts the Function to a dictionary.'''
        return {
            'name': self.name,
            'arguments': self.arguments,
            'return_type': self.return_type,
            'kind': self.kind,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Function':
        '''Creates a Function from a dictionary.'''
        return cls(
            name=data['name'],
            arguments=data.get('arguments', []),
            return_type=data.get('return_type', 'void'),
            kind=data.get('kind', 'FUNCTION')
        )
    # endregion