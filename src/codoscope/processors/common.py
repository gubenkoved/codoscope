import abc
from enum import StrEnum


class ProcessorType(StrEnum):
    REMAP_USERS = "remap-users"
    ANONYMIZE = "anonymize"
    EXPAND_REFERENCES = "expand-references"


class ProcessorBase(abc.ABC):
    @abc.abstractmethod
    def get_type(self) -> ProcessorType:
        raise NotImplementedError
