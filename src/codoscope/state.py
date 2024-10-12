import datetime
import enum
import gzip
import logging
import os
import os.path
import pickle
from typing import Optional

LOGGER = logging.getLogger(__name__)

class SourceType(enum.StrEnum):
    GIT = 'git'
    BITBUCKET = 'bitbucket'
    JIRA = 'jira'


class SourceState:
    def __init__(self, source_type: SourceType):
        self.source_type: SourceType = source_type


class StateModel:
    def __init__(self):
        self.sources: dict[str, SourceState] = {}
        self.version: int = 1
        self.created_at: datetime.datetime = datetime.datetime.now()

    def save(self, path: str) -> None:
        LOGGER.info('saving state to "%s"', path)
        with gzip.open(path + '.tmp', 'wb') as f:
            pickle.dump(self, f)
        os.rename(path + '.tmp', path)

    @staticmethod
    def load(path: str) -> Optional['StateModel']:
        LOGGER.info('loading state from "%s"', path)

        if not os.path.exists(path):
            LOGGER.warning('state file "%s" does not exist', path)
            return None

        with gzip.open(path, 'rb') as f:
            state = pickle.load(f)
            return state
