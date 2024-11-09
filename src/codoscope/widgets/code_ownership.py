from collections import defaultdict

import pandas
import plotly.graph_objects as go

from codoscope.common import Colors, convert_timezone
from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget


# TODO: add a way to only show current code base (how to detect the file movements?)
def code_ownership(
    activity_df: pandas.DataFrame,
    title: str = "Code changes map",
    maxdepth: int = 4,
) -> PlotlyFigureWidget | None:
    # make sure we only have commits
    commits_df = activity_df[activity_df["activity_type"] == "commit"].copy()

    # remove merge commits as useless
    commits_df = commits_df[commits_df["commit_is_merge_commit"] == False]

    if len(commits_df) == 0:
        return None

    # needed just to specify proper types for timestamp and avoid warnings
    commits_df = convert_timezone(commits_df, timezone_name="utc")
    commits_df = commits_df.set_index("timestamp")

    # leaf path -> counts
    path_counts = defaultdict(lambda: {"changed_lines_count": 0})

    for _, row in commits_df.iterrows():
        for path, stat in row["commit_changed_files_map"].items():
            path_counts[path]["changed_lines_count"] += stat["added"]
            path_counts[path]["changed_lines_count"] += stat["deleted"]

    def get_parent_path(path):
        if not path:
            return None
        segments = path.split("/")
        if len(segments) == 1:
            return None
        return "/".join(segments[:-1])

    def get_label(path: str) -> str:
        if not path:
            return "<ROOT>"
        return path.split("/")[-1]

    def aggregate():
        leafs = set(path_counts)
        ids: list[str] = []
        labels: list[str] = []
        parents: list[str] = []
        values: list[int] = []
        path_to_index = {}

        def ensure_item(path, parent_path) -> int:
            if path not in path_to_index:
                path_to_index[path] = len(labels)
                ids.append(path)
                labels.append(get_label(path))
                parents.append(parent_path)
                values.append(0)
            return path_to_index[path]

        def propagate(path, value):
            parent_path = get_parent_path(path)
            idx = ensure_item(path, parent_path)
            values[idx] += value
            if not path:
                return
            propagate(parent_path, value)

        for path in leafs:
            propagate(path, path_counts[path]["changed_lines_count"])

        return ids, labels, parents, values

    ids, labels, parents, values = aggregate()

    fig = go.Figure()

    # TODO: use hovertemplate and custom data to show added vs. removed counts
    fig.add_trace(
        go.Treemap(
            name="changed lines",
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            maxdepth=maxdepth,
            root_color=Colors.ALABASTER,
        )
    )

    setup_default_layout(
        fig,
        title=title,
    )

    return PlotlyFigureWidget(fig)
