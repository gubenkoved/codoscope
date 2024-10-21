import logging

from codoscope.exceptions import ConfigError
from codoscope.datasets import Datasets

LOGGER = logging.getLogger(__name__)


class RemapUsersProcessor:
    def __init__(self, processor_config: dict):
        self.config = processor_config

        # compose inverted maps
        self.name_to_canonical_name_map = {}
        self.email_to_canonical_name_map = {}
        for canonical_name, alias_configs in processor_config['canonical-names'].items():
            for alias_config in alias_configs:
                for prop in alias_config:
                    alias = alias_config[prop]
                    if prop == 'name':
                        if alias in self.name_to_canonical_name_map:
                            raise ConfigError('Name already mapped: "%s"' % alias)
                        self.name_to_canonical_name_map[alias] = canonical_name
                    elif prop == 'email':
                        if alias in self.email_to_canonical_name_map:
                            raise ConfigError('Email already mapped: "%s"' % alias)
                        self.email_to_canonical_name_map[alias] = canonical_name

    def execute(self, datasets: Datasets):
        remapped_items_count = 0
        df = datasets.activity
        for index, row in df.iterrows():
            author = row['author']
            author_email = row['author_email']

            if author_email and author_email in self.email_to_canonical_name_map:
                df.at[index, 'author'] = self.email_to_canonical_name_map[author_email]
                remapped_items_count += 1
                continue

            if author and author in self.name_to_canonical_name_map:
                df.at[index, 'author'] = self.name_to_canonical_name_map[author]
                remapped_items_count += 1
                continue

        LOGGER.info('remapped %d items', remapped_items_count)
