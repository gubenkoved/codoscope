import logging
import os.path

import pandas
import plotly.graph_objects as go
import wordcloud

from codoscope.common import ensure_dir, sanitize_filename
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    render_widgets_report,
    setup_default_layout,
)
from codoscope.reports.overview import activity_scatter, convert_timestamp_timezone
from codoscope.reports.word_clouds import render_word_cloud_html
from codoscope.state import StateModel
from codoscope.widgets.line_counts_stats import line_counts_stats

LOGGER = logging.getLogger(__name__)


class PerUserStatsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.PER_USER_STATS

    def weekly_stats(self, df: pandas.DataFrame) -> go.Figure:
        df = df.set_index("timestamp")
        df["activity_type"] = df["activity_type"].fillna("unspecified")
        grouped = df.groupby(["source_name", "activity_type"])

        fig = go.Figure()
        for (
            source_name,
            activity_type,
        ), group_df in grouped:
            weekly_counts = group_df.resample("W").size().reset_index(name="count")
            fig.add_trace(
                go.Bar(
                    name=f"{source_name} {activity_type}",
                    x=weekly_counts["timestamp"],
                    y=weekly_counts["count"],
                )
            )

        setup_default_layout(fig, "Weekly Counts")

        fig.update_layout(
            barmode="stack",
            showlegend=True,  # ensure legend even for single series
            margin=dict(
                t=50,
            ),
        )

        return fig

    def commit_themes_wordcloud(self, df: pandas.DataFrame) -> str | None:
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

        return f"""
<div style="padding: 20px">
    <h2>Commit themes</h2>
    {svg}
</div>
"""

    def emails_timeline(self, df: pandas.DataFrame) -> go.Figure:
        df["author_email"] = df["author_email"].fillna("unspecified")

        email_stats = (
            df.groupby("author_email").agg({"timestamp": ["count", "min", "max"]}).reset_index()
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
                        f"First: {row['first-used'].strftime('%Y-%m-%d %H:%M:%S')}",
                        f"Last: {row['last-used'].strftime('%Y-%m-%d %H:%M:%S')}",
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

        return fig

    def generate_for_user(self, user_name: str, report_path: str, df: pandas.DataFrame) -> None:
        render_widgets_report(
            report_path,
            [
                activity_scatter(df, extended_mode=True),
                self.weekly_stats(df),
                line_counts_stats(df, agg_period="W", title="Weekly Line Counts"),
                self.emails_timeline(df),
                self.commit_themes_wordcloud(df),
            ],
            title=f"user :: {user_name}",
        )

    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        parent_dir_path = os.path.abspath(read_mandatory(config, "dir-path"))
        ensure_dir(parent_dir_path)

        activity_df = datasets.activity
        activity_df = convert_timestamp_timezone(activity_df, config.get("timezone", "utc"))

        grouped_by_user = activity_df.groupby(["author"])

        processed_count = 0
        for (user_name,), user_df in grouped_by_user:
            file_name = sanitize_filename(user_name)
            file_path = "%s.html" % os.path.join(parent_dir_path, file_name)
            LOGGER.debug('rendering report for user "%s"', user_name)
            self.generate_for_user(user_name, file_path, user_df)

            processed_count += 1
            if processed_count % 20 == 0:
                LOGGER.info("processed %d of %d users", processed_count, len(grouped_by_user))
