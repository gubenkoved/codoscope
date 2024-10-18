import logging
import os
import os.path

import pandas
import wordcloud

from codoscope.common import ensure_dir_for_path
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import ReportBase, ReportType, render_html_report
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class WordCloudsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.WORD_CLOUDS

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, "out-path"))
        ensure_dir_for_path(out_path)

        width = read_optional(config, 'width', 1200)
        height = read_optional(config, 'height', 800)
        grouping_period = read_optional(config, 'grouping-period', 'Q')

        LOGGER.info(
            'generating word clouds report (%s x %s) grouping by %s',
            width, height, grouping_period)

        df = pandas.DataFrame(datasets.activity)
        df['timestamp'] = pandas.to_datetime(df['timestamp'], utc=True)
        df['message'] = df['message'].fillna('')

        grouped = df.groupby(df['timestamp'].dt.to_period(grouping_period))

        svgs = []
        for period, group_df in grouped:
            LOGGER.debug('processing period %s' % period)
            texts = []

            for idx, row in group_df.iterrows():
                if row['message']:
                    texts.append(row['message'])

            wc = wordcloud.WordCloud(width=width, height=height, background_color='white')
            wc.generate(' '.join(texts))
            svg = wc.to_svg()
            svgs.append((period, svg))

        # write svgs to html
        body_items = []
        for period, svg in svgs:
            body_items.append(f'<h1>{period}</h1>\n')
            body_items.append(svg)
        render_html_report(out_path, '\n'.join(body_items), 'word clouds')
