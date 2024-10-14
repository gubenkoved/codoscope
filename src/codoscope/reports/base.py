import abc
import enum

from codoscope.datasets import Datasets
from codoscope.state import StateModel


class ReportType(enum.StrEnum):
    OVERVIEW = 'overview'
    INTERNAL_STATE = 'internal-state'


class ReportBase(abc.ABC):
    @abc.abstractmethod
    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        raise NotImplementedError

    # TODO: define class property
    @classmethod
    def get_type(cls) -> ReportType:
        raise NotImplementedError
