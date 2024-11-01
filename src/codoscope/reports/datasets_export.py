import logging
import os
import os.path

from codoscope.common import ensure_dir
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.reports.common import ReportBase, ReportType
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class DatasetsExportReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.DATASETS_EXPORT

    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        out_dir = os.path.abspath(read_mandatory(config, "out-dir"))
        ensure_dir(out_dir)

        datasets_map = {
            "activity": datasets.activity,
            "reviews": datasets.reviews,
        }

        for name, df in datasets_map.items():
            out_path = os.path.join(out_dir, "%s.csv" % name)
            df.to_csv(out_path, index=False)
