import logging
import os
import os.path

from codoscope.common import ensure_dir_for_path, render_jinja_template
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import ReportBase, ReportType
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class PrReviewsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.PR_REVIEWS

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, "out-path"))
        ensure_dir_for_path(out_path)

        if len(datasets.reviews_df) == 0:
            LOGGER.warning("no review data available")
            return

        reviews_df = datasets.reviews_df[datasets.reviews_df["is_self_review"] == False].copy()
        # init missing review timestamp from the pr creation date itself just to
        # provide some reasonable time reference
        reviews_df["timestamp"] = reviews_df["timestamp"].fillna(
            reviews_df["bitbucket_pr_created_date"]
        )

        ignored_users = read_optional(config, "ignored-users", [])

        LOGGER.info("users ignored: %s", ignored_users)

        reviews_model = []
        for _, row in reviews_df.iterrows():
            reviewer = row["reviewer_user"]
            reviewee = row["reviewee_user"]
            timestamp = row["timestamp"]
            has_approved = row["has_approved"]

            if reviewer in ignored_users or reviewee in ignored_users:
                continue

            reviews_model.append(
                {
                    "reviewer": reviewer,
                    "reviewee": reviewee,
                    "timestamp": timestamp,
                    "has_approved": has_approved,
                }
            )

        LOGGER.info("items count: %d", len(reviews_model))

        with open(out_path, "w") as out_file:
            rendered_text = render_jinja_template(
                "reviews_v2.jinja2",
                context={
                    "title": "codoscope :: reviewers",
                    "data_model": reviews_model,
                },
            )
            out_file.write(rendered_text)
