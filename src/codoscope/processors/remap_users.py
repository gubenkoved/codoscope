import logging

import pandas

from codoscope.datasets import Datasets
from codoscope.exceptions import ConfigError
from codoscope.processors.common import ProcessorBase, ProcessorType

LOGGER = logging.getLogger(__name__)


class RemapUsersProcessor(ProcessorBase):
    def __init__(self, processor_config: dict) -> None:
        self.config = processor_config

        # compose inverted maps
        self.name_to_canonical_name_map: dict[str, str] = {}
        self.email_to_canonical_name_map: dict[str, str] = {}
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

    def remap_activity(self, dataset_name: str, activity_df: pandas.DataFrame) -> None:
        remapped_items_count = 0
        for index, row in activity_df.iterrows():
            user = row["user"]
            user_email = row["user_email"]

            if not pandas.isna(user_email) and user_email in self.email_to_canonical_name_map:
                activity_df.at[index, "user"] = self.email_to_canonical_name_map[user_email]
                remapped_items_count += 1
                continue

            if not pandas.isna(user) and user in self.name_to_canonical_name_map:
                activity_df.at[index, "user"] = self.name_to_canonical_name_map[user]
                remapped_items_count += 1
                continue

        LOGGER.info("%s dataset: remapped %d items", dataset_name, remapped_items_count)

    def remap_reviews(self, reviews_df: pandas.DataFrame) -> None:
        remapped_items_count = 0
        for index, row in reviews_df.iterrows():
            reviewer_user = row["reviewer_user"]
            reviewee_user = row["reviewee_user"]

            if reviewer_user and reviewer_user in self.name_to_canonical_name_map:
                reviews_df.at[index, "reviewer_user"] = self.name_to_canonical_name_map[
                    reviewer_user
                ]
                remapped_items_count += 1
                continue

            if reviewee_user and reviewee_user in self.name_to_canonical_name_map:
                reviews_df.at[index, "reviewee_user"] = self.name_to_canonical_name_map[
                    reviewee_user
                ]
                remapped_items_count += 1
                continue

        LOGGER.info("reviews dataset: remapped %d items", remapped_items_count)

    def execute(self, datasets: Datasets) -> None:
        for dataset_name, activity_df in datasets.get_activity_data_frames().items():
            self.remap_activity(dataset_name, activity_df)
        self.remap_reviews(datasets.reviews_df)
