import datetime
import enum
import logging
import os
import os.path
import pickle

LOGGER = logging.getLogger(__name__)

class SourceType(enum.StrEnum):
    GIT = 'git'
    BITBUCKET = 'bitbucket'


class SourceState:
    def __init__(self, source_type: SourceType):
        self.source_type: SourceType = source_type


class StateModel:
    def __init__(self):
        self.sources: dict[str, SourceState] = {}
        self.version: int = 1
        self.created_at: datetime.datetime = datetime.datetime.now()


# TODO: add simple compression schema
def load_state(path: str) -> StateModel | None:
    LOGGER.info('loading state from "%s"', path)

    if not os.path.exists(path):
        LOGGER.warning('state file "%s" does not exist', path)
        return None

    with open(path, 'rb') as f:
        state = pickle.load(f)
        return state


def save_sate(path: str, state: StateModel):
    LOGGER.info('saving state to "%s"', path)
    with open(path + '.tmp', 'wb') as f:
        pickle.dump(state, f)
    os.rename(path + '.tmp', path)
