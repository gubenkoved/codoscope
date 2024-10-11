import logging

import yaml

LOGGER = logging.getLogger(__name__)


# TODO: use object model for config
def load_config(path: str) -> dict:
    LOGGER.info('loading config from "%s"', path)
    with open(path, 'r') as f:
        return yaml.safe_load(f)