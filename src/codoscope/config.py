import logging
from typing import Any

import yaml

from codoscope.exceptions import ConfigError

LOGGER = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    LOGGER.info('loading config from "%s"', path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def read_mandatory(config: dict, key: str) -> Any:
    if key not in config:
        raise ConfigError('Expected mandatory key "%s" was not found inside the config' % key)
    return config[key]


def read_optional(config: dict, key: str, default: Any = None) -> Any:
    return config.get(key, default)
