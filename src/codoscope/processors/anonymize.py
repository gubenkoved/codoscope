import logging
from collections import defaultdict

import pandas
from faker import Faker

from codoscope.datasets import Datasets
from codoscope.processors.common import ProcessorBase, ProcessorType

LOGGER = logging.getLogger(__name__)


# TODO: this is obviously not complete anonymization
class AnonymizingProcessor(ProcessorBase):
    def __init__(self, processor_config: dict) -> None:
        self.config = processor_config
        self.user_replacement_map = {}
        self.email_replacement_map = {}
        self.faker = Faker()
        self.replacement_maps = defaultdict(dict)

    def get_type(self) -> ProcessorType:
        return ProcessorType.ANONYMIZE

    def remap(self, type_: str, value: str, factory_fn):
        replacement_map = self.replacement_maps[type_]

        if value not in replacement_map:
            replacement_map[value] = factory_fn()

        return replacement_map[value]

    def remap_activity(self, activity_df: pandas.DataFrame) -> None:
        for index, row in activity_df.iterrows():
            user = row["user"]
            user_email = row["user_email"]

            if user:
                activity_df.at[index, "user"] = self.remap("user", user, self.faker.name)

            if user_email:
                activity_df.at[index, "user_email"] = self.remap(
                    "email", user, self.faker.company_email
                )

    def remap_reviews(self, reviews_df: pandas.DataFrame) -> None:
        for index, row in reviews_df.iterrows():
            reviewer_user = row["reviewer_user"]
            reviewee_user = row["reviewee_user"]

            if reviewer_user:
                reviews_df.at[index, "reviewee_user"] = self.remap(
                    "user", reviewer_user, self.faker.name
                )

            if reviewee_user:
                reviews_df.at[index, "reviewer_user"] = self.remap(
                    "user", reviewee_user, self.faker.name
                )

    def execute(self, datasets: Datasets) -> None:
        self.remap_activity(datasets.activity)
        self.remap_reviews(datasets.reviews)
