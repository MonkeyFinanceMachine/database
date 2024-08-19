from abc import ABC
from re import sub

import inflect


p = inflect.engine()


# https://gist.github.com/dubpirate/fdea9a67500a46613ad637269320d272
def to_snake(s: str) -> str:
    s = sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s)
    s = sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    s = s.lower()
    return s


def to_table_name(s: str) -> str:
    snaked = to_snake(s)
    if '_to_' in snaked:
        return snaked
    return p.plural(snaked)


class BaseEntity(ABC):
    __tablename__: str
    __properties__: dict[str, object]

    def __init__(self) -> None:
        self.__tablename__ = to_table_name(type(self).__name__)
        self.__properties__ = self.__annotations__

    def _get_props(self) -> list[str]:
        return list(self.__properties__.keys())

    def _serialize(self) -> dict[str, str]:
        result = dict()
        for property_name in self.__properties__:
            property_value = getattr(self, property_name)
            result[property_name] = str(property_value)
        return result

    def _unserialize(self, data: dict[str, str]) -> object:
        for property_name in data:
            property_value = data[property_name]
            property_type = self.__properties__[property_name]
            setattr(self, property_name, property_type(property_value))
        return self
