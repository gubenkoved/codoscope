from codoscope.reports.base import ReportType, ReportBase
from codoscope.reports.overview import OverviewReport
from codoscope.reports.internal_state import InternalStateReport

REPORTS: list[type[ReportBase]] = [
    OverviewReport,
    InternalStateReport,
]

REPORTS_BY_TYPE: dict[ReportType, type[ReportBase]]  = {
    report.get_type(): report
    for report in REPORTS
}
