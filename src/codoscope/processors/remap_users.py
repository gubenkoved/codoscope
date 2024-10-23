import logging

from codoscope.datasets import Datasets
from codoscope.exceptions import ConfigError
from codoscope.processors.common import ProcessorBase, ProcessorType

LOGGER = logging.getLogger(__name__)


class RemapUsersProcessor(ProcessorBase):
    def __init__(self, processor_config: dict):
        self.config = processor_config

        # compose inverted maps
        self.name_to_canonical_name_map = {}
        self.email_to_canonical_name_map = {}
        for canonical_name, alias_configs in processor_config["canonical-names"].items():
            for alias_config in alias_configs:
                for prop in alias_config:
                    alias = alias_config[prop]
                    if prop == "name":
                        if alias in self.name_to_canonical_name_map:
                            raise ConfigError('Name already mapped: "%s"' % alias)
                        self.name_to_canonical_name_map[alias] = canonical_name
                    elif prop == "email":
                        if alias in self.email_to_canonical_name_map:
                            raise ConfigError('Email already mapped: "%s"' % alias)
                        self.email_to_canonical_name_map[alias] = canonical_name

    def get_type(self) -> ProcessorType:
        return ProcessorType.REMAP_USERS

    def execute(self, datasets: Datasets):
        remapped_items_count = 0
        df = datasets.activity
        for index, row in df.iterrows():
            user = row["user"]
            user_email = row["user_email"]

            if user_email and user_email in self.email_to_canonical_name_map:
                df.at[index, "user"] = self.email_to_canonical_name_map[user_email]
                remapped_items_count += 1
                continue

            if user and user in self.name_to_canonical_name_map:
                df.at[index, "user"] = self.name_to_canonical_name_map[user]
                remapped_items_count += 1
                continue

        LOGGER.info("remapped %d items", remapped_items_count)
