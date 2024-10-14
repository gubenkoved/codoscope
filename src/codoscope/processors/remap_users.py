import logging

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
                            raise Exception('Name already mapped: "%s"' % alias)
                        self.name_to_canonical_name_map[alias] = canonical_name
                    elif prop == 'email':
                        if alias in self.email_to_canonical_name_map:
                            raise Exception('Email already mapped: "%s"' % alias)
                        self.email_to_canonical_name_map[alias] = canonical_name

    def execute(self, dataset: list[dict]):
        remapped_items_count = 0
        for item in dataset:
            author = item.get('author')
            author_email = item.get('author_email')

            if author_email and author_email in self.email_to_canonical_name_map:
                item['author'] = self.email_to_canonical_name_map[author_email]
                remapped_items_count += 1
                continue

            if author and author in self.name_to_canonical_name_map:
                item['author'] = self.name_to_canonical_name_map[author]
                remapped_items_count += 1
                continue

        LOGGER.info('remapped %d items', remapped_items_count)
