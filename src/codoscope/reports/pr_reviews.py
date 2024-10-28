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
        reviews_df = reviews_df[reviews_df["has_approved"] == False]

        grouped = (
            reviews_df.groupby(["reviewer_user", "reviewee_user"]).size().reset_index(name="count")
        )
        grouped.columns = ["reviewer_user", "reviewee_user", "count"]

        review_links = []
        user_info_map = collections.defaultdict(
            lambda: {
                "count": 0,
                "count_normalized": 0.0,
                "color": "#3777de",
            }
        )

        ignored_users = read_optional(config, "ignored-users", [])

        LOGGER.info("users ignored: %s", ignored_users)

        for _, row in grouped.iterrows():
            reviewer = row["reviewer_user"]
            reviewee = row["reviewee_user"]
            count = row["count"]

            if reviewer in ignored_users or reviewee in ignored_users:
                continue

            user_info_map[reviewer]["count"] += count
            _ = user_info_map[reviewee]
            review_links.append(
                {
                    "reviewer": reviewer,
                    "reviewee": reviewee,
                    "count": count,
                    "width": min(3, max(1, math.log(count, 2) - 5)),
                    "opacity": 0.3,
                }
            )

        LOGGER.info("links count: %d", len(review_links))

        threshold = read_optional(config, "threshold", 10)
        review_links = [x for x in review_links if x["count"] >= threshold]

        LOGGER.info("links count over threshold: %d", len(review_links))

        for item in user_info_map.values():
            count = item["count"]
            item["size"] = max(4, math.log(1 + count, 1.3))

        max_reviews_in_pair = max(review_links, key=lambda x: x["count"])["count"]
        for link in review_links:
            link["strength"] = 0.1 * link["count"] / max_reviews_in_pair
            link["opacity"] = max(0.1, 0.8 * link["count"] / max_reviews_in_pair)

        with open(out_path, "w") as out_file:
            rendered_text = render_jinja_template(
                "reviews.html.jinja2",
                context={
                    "title": "Reviewers",
                    "review_links": review_links,
                    "user_info_map": user_info_map,
                },
            )
            out_file.write(rendered_text)
