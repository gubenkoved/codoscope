import logging
import os
import os.path

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

        df = datasets.activity
        df["author_email"] = df["author_email"].fillna("")

        groupped_df = df.groupby(["author", "author_email"]).size().reset_index(name="count")

        groupped_df.to_csv(out_path)
