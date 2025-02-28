import logging
import re

import pandas as pd

from codoscope.datasets import Datasets
from codoscope.processors.common import ProcessorBase, ProcessorType

LOGGER = logging.getLogger(__name__)


class ExpandReferencesProcessor(ProcessorBase):
    def __init__(self, processor_config: dict) -> None:
        self.config = processor_config

    def get_type(self) -> ProcessorType:
        return ProcessorType.EXPAND_REFERENCES

    def __handle_jira(self, datasets: Datasets) -> None:

        replacements = 0

        def replacer(match: re.Match) -> str:
            nonlocal replacements
            account_id = match.group(1)
            try:
                display_name = datasets.jira_users_df.loc[account_id]["display_name"]
                if not pd.isna(display_name) and display_name:
                    replacements += 1
                    return f"({display_name})"
            except KeyError:
                pass
            return match.group(0)

        def handler(value):
            if pd.isna(value):
                return value
            return re.sub(r"\[~accountid:([^]]+)]", replacer, value)

        for prop in ["jira_message", "jira_description"]:
            datasets.jira_df[prop] = datasets.jira_df[prop].apply(handler)

        LOGGER.info("replaced %d JIRA references", replacements)

    def execute(self, datasets: Datasets) -> None:
        self.__handle_jira(datasets)
