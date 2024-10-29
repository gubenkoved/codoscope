import collections
import logging
import math
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

        reviews_df = datasets.reviews

        reviews_df = reviews_df[reviews_df["is_self_review"] == False]
        reviews_df = reviews_df[reviews_df["has_approved"] == True]

        # init missing review timestamp from the pr creation date itself just to
        # provide some reasonable time reference
        reviews_df['timestamp'] = reviews_df['timestamp'].fillna(reviews_df["bitbucker_pr_created_date"])

        ignored_users = read_optional(config, "ignored-users", [])

        LOGGER.info("users ignored: %s", ignored_users)

        reviews_model = []
        for _, row in reviews_df.iterrows():
            reviewer = row["reviewer_user"]
            reviewee = row["reviewee_user"]
            timestamp = row["timestamp"]

            if reviewer in ignored_users or reviewee in ignored_users:
                continue

            reviews_model.append(
                {
                    "reviewer": reviewer,
                    "reviewee": reviewee,
                    "timestamp": timestamp,
                }
            )

        LOGGER.info("items count: %d", len(reviews_model))

        with open(out_path, "w") as out_file:
            rendered_text = render_jinja_template(
                "reviews_v2.html.jinja2",
                context={
                    "title": "codoscope :: reviewers",
                    "data_model": reviews_model,
                },
            )
            out_file.write(rendered_text)
