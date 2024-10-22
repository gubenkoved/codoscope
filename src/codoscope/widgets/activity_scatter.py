import logging
import textwrap
from typing import Any, Callable

import pandas
import plotly.graph_objects as go

from codoscope.common import (
    NA_REPLACEMENT,
    date_time_minutes_offset,
    format_minutes_offset,
)
from codoscope.reports.common import setup_default_layout, time_axis

LOGGER = logging.getLogger(__name__)

HOVER_TEXT_MAX_ITEM_LEN = 1000
HOVER_TEXT_WRAP_WIDTH = 140


class HoverDataColumnDescriptor:
    def __init__(self, label: str | None, converter: Callable):
        self.label = label
        self.converter = converter


def limit_text_len(text: str, max_len: int) -> str:
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def get_hover_text(
    row: tuple[Any, ...], hover_data_columns_map: dict[str, HoverDataColumnDescriptor]
) -> str:
    text_item = f"<b>{row.source_name}</b><br>"
    for col_name, col_descriptor in hover_data_columns_map.items():
        col_val = getattr(row, col_name, None)
        if col_val is not None and not pandas.isna(col_val):
            col_val = col_descriptor.converter(col_val) if col_descriptor.converter else col_val
            if col_descriptor.label:
                text_item += "<b>%s</b>: %s<br>" % (col_descriptor.label, col_val)
            else:  # no column label
                text_item += "%s<br>" % col_val
            text_item = limit_text_len(text_item, HOVER_TEXT_MAX_ITEM_LEN)
            # split into lines to avoid too long hover text
            text_item_lines = textwrap.wrap(
                text_item,
                break_long_words=False,
                break_on_hyphens=False,
                width=HOVER_TEXT_WRAP_WIDTH,
            )
            text_item = "<br>".join(text_item_lines)
    return text_item


def activity_scatter(
    activity_df: pandas.DataFrame, extended_mode: bool = False, title: str | None = None
) -> go.Figure | None:
    fig = go.Figure()

    title = title or "Activity Overview"

    setup_default_layout(
        fig,
        title=title,
    )

    tickvals, ticktext = time_axis()

    fig.update_layout(
        yaxis=dict(
            title="Time",
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
        ),
        xaxis_title="Timestamp",
    )

    # add time of the day fields
    activity_df["time_of_day_minutes_offset"] = activity_df.apply(
        lambda row: date_time_minutes_offset(row["timestamp"]), axis=1
    )
    activity_df["time_of_day"] = activity_df.apply(
        lambda row: format_minutes_offset(row["time_of_day_minutes_offset"]), axis=1
    )

    # initialize for missing authors
    activity_df["author"] = activity_df["author"].fillna(NA_REPLACEMENT)

    # sort for predictable labels order for traces
    activity_df = activity_df.sort_values(
        by=["author", "source_type", "source_subtype", "timestamp"]
    )

    LOGGER.debug("data points to render: %d", len(activity_df))

    if len(activity_df) == 0:
        LOGGER.debug("no data to show")
        return None

    activity_df["source_subtype"] = activity_df["source_subtype"].fillna("")

    grouped_df = activity_df.groupby(["author", "activity_type"])

    LOGGER.debug("groups count: %s", grouped_df.ngroups)

    def ident(x):
        return x

    def convert_int(x):
        return "%d" % x

    # column name to label map
    hover_data_columns_map = {
        "commit_sha": HoverDataColumnDescriptor(None, ident),
        "bitbucket_pr_title": HoverDataColumnDescriptor(None, ident),
        "jira_item_key": HoverDataColumnDescriptor(None, ident),
    }

    if extended_mode:
        hover_data_columns_map.update(
            {
                "commit_added_lines": HoverDataColumnDescriptor("added lines", convert_int),
                "commit_removed_lines": HoverDataColumnDescriptor("removed lines", convert_int),
                "commit_message": HoverDataColumnDescriptor("commit message", ident),
                "jira_summary": HoverDataColumnDescriptor("summary", ident),
                "jira_message": HoverDataColumnDescriptor("message", ident),
                "bitbucket_pr_comment": HoverDataColumnDescriptor("comment", ident),
                "bitbucket_pr_id": HoverDataColumnDescriptor("PR ID", ident),
            }
        )

    for (author, activity_type), df in grouped_df:
        name = "%s %s" % (author, activity_type)

        # compose hover texts all the fields
        texts = []
        for row in df.itertuples():
            texts.append(get_hover_text(row, hover_data_columns_map))

        trace = go.Scattergl(
            name=name,
            showlegend=True,
            x=df["timestamp"],
            y=df["time_of_day_minutes_offset"],
            mode="markers",
            text=texts,
            hovertemplate="%{x}<br>%{text}",
            opacity=0.9,
            marker=dict(
                size=df["size_class"],
            ),
        )
        fig.add_trace(trace)

    return fig
