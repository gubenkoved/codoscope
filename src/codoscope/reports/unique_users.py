import logging
import os
import os.path

from pandas import DataFrame

from codoscope.common import ensure_dir_for_path
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.reports.common import ReportBase, ReportType
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class UniqueUsersReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.UNIQUE_USERS

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, "out-path"))
        ensure_dir_for_path(out_path)

        df: DataFrame = datasets.activity.copy()

        # fill user email with replacement string otherwise they won't be included into groupping
        df["user_email"] = df["user_email"].fillna("")

        groupped_df = df.groupby(["user", "user_email"]).size().reset_index(name="count")

        groupped_df.to_csv(out_path)
