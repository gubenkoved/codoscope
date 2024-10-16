from codoscope.reports.common import ReportType, ReportBase
from codoscope.reports.internal_state import InternalStateReport
from codoscope.reports.overview import OverviewReport
from codoscope.reports.per_source_stats import PerSourceStatsReport
from codoscope.reports.per_user_stats import PerUserStatsReport

REPORTS: list[type[ReportBase]] = [
    OverviewReport,
    InternalStateReport,
    PerUserStatsReport,
    PerSourceStatsReport,
]

REPORTS_BY_TYPE: dict[ReportType, type[ReportBase]]  = {
    report.get_type(): report
    for report in REPORTS
}
