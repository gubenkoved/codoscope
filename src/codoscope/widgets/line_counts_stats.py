import pandas
import plotly.graph_objects as go

from codoscope.reports.common import setup_default_layout


def line_counts_stats(
    acvitiy_df: pandas.DataFrame, agg_period: str = "W", title: str | None = None
) -> go.Figure | None:
    df = acvitiy_df.set_index("timestamp")

    # filter leaving only commits
    df = df[df["activity_type"] == "commit"]

    if len(df) == 0:
        return None

    resampled_df = df.resample(agg_period).agg(
        {
            "commit_added_lines": "sum",
            "commit_removed_lines": "sum",
        }
    )

    resampled_df["timestamp"] = resampled_df.index

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name=f"lines added",
            x=resampled_df["timestamp"],
            y=resampled_df["commit_added_lines"],
            marker_color="#2296bf",  # blue-ish
        )
    )
    fig.add_trace(
        go.Bar(
            name=f"lines removed",
            x=resampled_df["timestamp"],
            y=resampled_df["commit_removed_lines"],
            marker_color="#e25548",  # red-ish
        )
    )

    setup_default_layout(fig, title or "Line counts")

    fig.update_layout(
        # yaxis_type="log",
        barmode="group",
        height=600,
        margin=dict(
            t=50,
        ),
        yaxis=dict(
            type="log",
        ),
    )

    return fig
