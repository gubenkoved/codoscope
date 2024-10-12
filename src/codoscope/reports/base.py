import abc
import enum

from codoscope.state import StateModel


class ReportType(enum.StrEnum):
    OVERVIEW = 'overview'


class ReportBase(abc.ABC):
    @abc.abstractmethod
    def generate(self, config: dict, state: StateModel) -> None:
        raise NotImplementedError

    # TODO: define class property
    @classmethod
    def get_type(cls) -> ReportType:
        raise NotImplementedError
