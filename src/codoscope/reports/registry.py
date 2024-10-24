from codoscope.reports.common import ReportBase, ReportType
from codoscope.reports.datasets_export import DatasetsExportReport
from codoscope.reports.internal_state import InternalStateReport
from codoscope.reports.overview import OverviewReport
from codoscope.reports.per_source_stats import PerSourceStatsReport
from codoscope.reports.per_user_stats import PerUserStatsReport
from codoscope.reports.pr_reviews import PrReviewsReport
from codoscope.reports.unique_users import UniqueUsersReport
from codoscope.reports.word_clouds import WordCloudsReport

REPORTS: list[type[ReportBase]] = [
    OverviewReport,
    InternalStateReport,
    PerUserStatsReport,
    PerSourceStatsReport,
    UniqueUsersReport,
    WordCloudsReport,
    DatasetsExportReport,
    PrReviewsReport,
]

REPORTS_BY_TYPE: dict[ReportType, type[ReportBase]] = {
    report.get_type(): report for report in REPORTS
}
