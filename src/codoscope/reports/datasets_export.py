import logging
import os
import os.path

from codoscope.common import ensure_dir
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.exceptions import ConfigError
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

        data_format = read_optional(config, "format", default="csv")

        for name, df in datasets.get_all_data_frames().items():
            if data_format == "csv":
                out_path = os.path.join(out_dir, "%s.csv" % name)
                df.to_csv(out_path, index=False)
            elif data_format in ("ndjson", "jsonl"):
                out_path = os.path.join(out_dir, "%s.jsonl" % name)
                df.to_json(out_path, orient="records", lines=True)
            else:
                raise ConfigError(
                    f'Unknown data format "{data_format}" supported: csv, ndjson (jsonl)'
                )
