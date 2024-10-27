import heapq

import pandas
import plotly.graph_objects as go

from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget
from codoscope.common import convert_timezone


def active_contributors_count(
    activity_df: pandas.DataFrame,
    title: str | None = None,
    height: int | None = None,
) -> PlotlyFigureWidget | None:

    activity_df = convert_timezone(activity_df, timezone_name='utc')

    if len(activity_df) == 0:
        return None

    user_aggregated_df = (
        activity_df.groupby(["user"])
        .agg(
            first_timestamp=("timestamp", "min"),
            last_timestamp=("timestamp", "max"),
        )
        .reset_index()
    )

    events_heap = []

    for _, row in user_aggregated_df.iterrows():
        user = row['user']
        first_timestamp = row["first_timestamp"]
        last_timestamp = row["last_timestamp"]
        heapq.heappush(events_heap, (first_timestamp, user, "first"))
        heapq.heappush(events_heap, (last_timestamp, user, "last"))

    # now compose new dataset with cumulative counts
    counts = []
    current_count = 0
    while events_heap:
        timestamp, user, event = heapq.heappop(events_heap)
        if event == "first":
            current_count += 1
            event_name = f'{user} first contribution',
        elif event == "last":
            current_count -= 1
            event_name = f'{user} last contribution',
        counts.append(
            {
                "timestamp": timestamp,
                "count": current_count,
                "event": event_name,
            }
        )

    counts_df = pandas.DataFrame(counts)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            name='count',
            x=counts_df["timestamp"],
            y=counts_df["count"],
            text=counts_df["event"],
            mode="lines+markers",
            # empty extra hides the trace name
            hovertemplate="%{x}<br>%{text}<extra></extra>",
            hoverlabel=None,
            marker=dict(
                size=2.5,
            ),
            line=dict(
                width=1.5,
            )
        )
    )

    setup_default_layout(fig, title)

    return PlotlyFigureWidget(fig, height)
