import logging
import os
import os.path

import pandas
import wordcloud

from codoscope.common import convert_timezone, ensure_dir_for_path
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import ReportBase, ReportType, render_html_report
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


def render_word_cloud_html(wordcloud: wordcloud.WordCloud) -> str:
    """
    Render word cloud HTML with SVG inside that will take full page width
    irrespectively from the page width.
    """
    svg = wordcloud.to_svg()

    # TODO: use a better way to customize it via parsing HTML
    svg = svg.replace(
        "<svg ",
        f'<svg viewBox="0 0 {wordcloud.width} {wordcloud.height}" style="width: 100%; height: auto;"',
    )

    return svg


class WordCloudsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.WORD_CLOUDS

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, "out-path"))
        ensure_dir_for_path(out_path)

        width = read_optional(config, "width", 1900)
        height = read_optional(config, "height", 800)
        max_words = read_optional(config, "max-words", 250)
        stop_words = read_optional(config, "stop-words", None)
        grouping_period = read_optional(config, "grouping-period", "Q")

        LOGGER.info(
            'generating word clouds report (%sx%s) grouping period is "%s"',
            width,
            height,
            grouping_period,
        )

        df = datasets.get_all_activity().copy()
        df = convert_timezone(df, timezone_name="utc")

        grouped = df.groupby(df["timestamp"].dt.to_period(grouping_period))

        svgs = []

        # TODO: make fields and weights customizable as well
        # TODO: solve somehow issue with fields multiplication like "bitbucket_pr_title"
        #  is included into the comments as well...
        # TODO: somehow give more granular priority for JIRA RFE for instance
        text_fields = {
            "commit_message": 1,
            "jira_item_key": 1,
            "jira_summary": 5,
            # 'jira_message': 1,
            # 'jira_description': 1,
            "bitbucket_pr_title": 1,
            "bitbucket_pr_description": 3,
            # 'bitbucket_pr_comment': 1,
        }

        for period, group_df in grouped:
            LOGGER.info("processing period %s" % period)
            texts = []
            for idx, row in group_df.iterrows():
                for field, weight in text_fields.items():
                    if field not in row:
                        continue
                    val = row[field]
                    if val and not pandas.isna(val):
                        texts.append(("%s " % row[field]) * weight)

            wc = wordcloud.WordCloud(
                width=width,
                height=height,
                max_words=max_words,
                stopwords=stop_words or [],
                background_color="white",
            )
            text = " ".join(texts)
            wc.generate(text)
            svg = render_word_cloud_html(wc)
            svgs.append((period, svg))

        # write svgs to html
        body_items = []
        for period, svg in svgs:
            body_items.append(f"<h1>{period}</h1>\n")
            body_items.append(svg)

        body = f"""
<div style="padding: 20px">
{'\n'.join(body_items)}
</div>
"""

        render_html_report(out_path, body, "word clouds")
