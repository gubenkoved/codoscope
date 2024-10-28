import pandas
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget


def line_counts_stats(
    activity_df: pandas.DataFrame,
    agg_period: str = "W",
    title: str | None = None,
    height: int | None = None,
    include_cumulative: bool = True,
) -> PlotlyFigureWidget | None:
    df = activity_df.set_index("timestamp").sort_index()

    # filter leaving only commits
    filtered_df = df[df["activity_type"] == "commit"]

    # do not include merge commits
    filtered_df = filtered_df[filtered_df["commit_is_merge_commit"] == False]

    if len(filtered_df) == 0:
        return None

    resampled_df = filtered_df.resample(agg_period).agg(
        {
            "commit_added_lines": "sum",
            "commit_removed_lines": "sum",
        }
    )

    resampled_df["timestamp"] = resampled_df.index

    if not include_cumulative:
        fig = go.Figure()
    else:
        fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            name=f"lines added",
            x=resampled_df["timestamp"],
            y=resampled_df["commit_added_lines"],
            marker=dict(
                color="#2296bf",  # blue-ish
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            name=f"lines removed",
            x=resampled_df["timestamp"],
            y=resampled_df["commit_removed_lines"],
            marker=dict(
                color="#e25548",  # red-ish
            ),
        )
    )

    if include_cumulative:
        # pandas cumsum requires index to be present
        cumulative_df = filtered_df[["commit_added_lines", "commit_removed_lines"]].copy()
        cumulative_df["lines_delta"] = (
            cumulative_df["commit_added_lines"] - cumulative_df["commit_removed_lines"]
        )
        cumulative_df = cumulative_df.cumsum()
        fig.add_trace(
            go.Scatter(
                name="cumulative lines count",
                x=cumulative_df.index,
                y=cumulative_df["lines_delta"],
                mode="lines",
                line=dict(
                    color="#107194",  # darker blue
                    width=2,
                ),
            ),
            secondary_y=True,
        )

    setup_default_layout(fig, title or "Line counts")

    fig.update_layout(
        barmode="group",
        yaxis=dict(
            type="log",
        ),
    )

    return PlotlyFigureWidget(fig, height)
