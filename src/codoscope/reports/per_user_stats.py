import logging
import os.path

import pandas
import plotly.graph_objects as go
import wordcloud

from codoscope.common import (
    NA_REPLACEMENT,
    apply_filter,
    convert_timezone,
    ensure_dir,
    sanitize_filename,
)
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    render_widgets_report,
    setup_default_layout,
)
from codoscope.reports.overview import activity_scatter
from codoscope.reports.word_clouds import render_word_cloud_html
from codoscope.state import StateModel
from codoscope.widgets import activity_trends
from codoscope.widgets.activity_by_weekday import (
    activity_by_weekday,
    activity_by_weekday_2d,
    activity_offset_hisogram,
)
from codoscope.widgets.activity_heatmap import activity_heatmap
from codoscope.widgets.aggregated_counts import aggregated_counts
from codoscope.widgets.code_ownership_v2 import code_ownership_v2
from codoscope.widgets.common import CompositeWidget, PlotlyFigureWidget, Widget
from codoscope.widgets.line_counts_stats import line_counts_stats

LOGGER = logging.getLogger(__name__)


def separate_merge_commits(activity_df: pandas.DataFrame):
    activity_df = activity_df.copy()
    activity_df.loc[activity_df["commit_is_merge_commit"] == True, "activity_type"] = "merge commit"
    return activity_df


class PerUserStatsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.PER_USER_STATS

    def commit_themes_wordcloud(
        self,
        df: pandas.DataFrame,
    ) -> Widget | None:
        df = df.set_index("timestamp")

        # filter leaving only commits
        df = df[df["activity_type"] == "commit"]

        # remove merge commits as useless
        df = df[df["commit_is_merge_commit"] == False]

        if len(df) == 0:
            return None

        commit_messages = []
        for _, row in df.iterrows():
            if pandas.isna(row["commit_message"]):
                continue
            commit_messages.append(row["commit_message"])

        # TODO: make paramters configurable
        wc = wordcloud.WordCloud(
            width=1900,
            height=800,
            max_words=250,
            # stopwords=stop_words or [],
            background_color="white",
        )
        text = " ".join(commit_messages)
        wc.generate(text)
        svg = render_word_cloud_html(wc)

        return Widget.from_html(
            f"""
<div style="padding: 20px">
    <h2>Commit themes</h2>
    {svg}
</div>
"""
        )

    def emails_timeline(self, df: pandas.DataFrame) -> PlotlyFigureWidget:
        df["user_email"] = df["user_email"].fillna(NA_REPLACEMENT)

        email_stats = (
            df.groupby("user_email").agg({"timestamp": ["count", "min", "max"]}).reset_index()
        )
        email_stats.columns = ["email", "count", "first-used", "last-used"]
        email_stats = email_stats.sort_values("count", ascending=False)

        fig = go.Figure()

        for _, row in email_stats.iterrows():
            email = row["email"]
            fig.add_trace(
                go.Scatter(
                    x=[row["first-used"], row["last-used"]],
                    y=[email, email],
                    mode="lines+markers",
                    name=f"{email} ({row['count']} activities)",
                    text=[
                        f"first: {row['first-used'].strftime('%Y-%m-%d %H:%M:%S')}",
                        f"last: {row['last-used'].strftime('%Y-%m-%d %H:%M:%S')}",
                    ],
                    hoverinfo="text+name",
                    line=dict(width=3),
                )
            )

        setup_default_layout(fig, "Email Usage Timeline")

        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Email",
            yaxis={"categoryorder": "total ascending", "showticklabels": False},
            height=max(250, len(email_stats) * 30),
            showlegend=True,
            margin=dict(
                t=50,
            ),
        )

        return PlotlyFigureWidget(fig)

    def generate_for_user(
        self,
        user_name: str,
        report_path: str,
        df: pandas.DataFrame,
        timezone_name: str,
    ) -> None:

        df_normalized = convert_timezone(
            df,
            timezone_name=timezone_name,
            inplace=False,
        )

        # git commits preserve local timezones
        commits_df: pandas.DataFrame = df[df["activity_type"] == "commit"].copy()

        no_commits_replacement_widget = Widget.centered(
            """
            <div style="padding: 20px; color: gray; text-align: center; font-size: small;">
                <b>No data</b><br>
                <i>no commits to build commit based plot<i>
            </div>
            """
        )

        widgets = [
            activity_scatter(
                separate_merge_commits(df_normalized),
                extended_mode=True,
            ),
            activity_heatmap(
                df_normalized,
            ),
            aggregated_counts(
                df_normalized,
                group_by=["source_name", "activity_type"],
                agg_period="W",
                title="Weekly counts",
            ),
            line_counts_stats(df_normalized, agg_period="W", title="Weekly line counts"),
            self.emails_timeline(df_normalized),
            CompositeWidget(
                [
                    [
                        activity_by_weekday(
                            df_normalized,
                            title=f"Weekday histogram ({timezone_name})",
                        ),
                        activity_by_weekday_2d(
                            df_normalized,
                            title=f"Weekday vs. time heatmap ({timezone_name})",
                        ),
                        activity_by_weekday_2d(
                            commits_df,
                            title="Commit heatmap (local time)",
                        )
                        or no_commits_replacement_widget,
                        activity_offset_hisogram(
                            commits_df,
                            title="Commit time offsets",
                        )
                        or no_commits_replacement_widget,
                    ]
                ]
            ),
            CompositeWidget(
                [
                    [
                        activity_trends.activity_trend(
                            df_normalized,
                            window_period="D",
                            aggregation_period="ME",
                            metrics=[
                                activity_trends.Metric("monthly", "ME", "mean"),
                            ],
                            title="Monthly active days",
                        ),
                        activity_trends.activity_trend(
                            df_normalized,
                            window_period="h",
                            aggregation_period="D",
                            metrics=[
                                activity_trends.Metric(
                                    "weekly", "W", "mean", line_width=1.0, opacity=0.4
                                ),
                                activity_trends.Metric("monthly", "ME", "mean"),
                                activity_trends.Metric("quaterly", "QE", "mean"),
                                activity_trends.Metric("yearly", "YE", "mean", line_width=3.0),
                            ],
                            title="Daily active hours",
                        ),
                    ],
                ]
            ),
        ]

        for (source_name,), per_source_commits_df in commits_df.groupby(["source_name"]):
            widgets.append(
                code_ownership_v2(
                    per_source_commits_df,
                    title=f"Code changes ({source_name})",
                    show_users_breakdown_pane=False,
                )
            )

        widgets.append(self.commit_themes_wordcloud(df_normalized))

        render_widgets_report(
            report_path,
            widgets,
            title=f"user :: {user_name}",
        )

    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        parent_dir_path = os.path.abspath(read_mandatory(config, "out-dir"))
        ensure_dir(parent_dir_path)

        timezone_name = config.get("timezone", "utc")

        activity_df = datasets.get_all_activity()

        filter_expr = read_optional(config, "filter")
        activity_df = apply_filter(activity_df, filter_expr)

        grouped_by_user = activity_df.groupby(["user"])

        processed_count = 0
        for (user_name,), user_df in grouped_by_user:
            LOGGER.debug('rendering report for user "%s"', user_name)

            file_name: str = sanitize_filename(user_name)
            file_path: str = "%s.html" % os.path.join(parent_dir_path, file_name)

            self.generate_for_user(user_name, file_path, user_df, timezone_name)

            processed_count += 1
            if processed_count % 20 == 0:
                LOGGER.info("processed %d of %d users", processed_count, len(grouped_by_user))
