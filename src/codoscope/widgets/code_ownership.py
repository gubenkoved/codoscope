from collections import defaultdict

import pandas
import plotly.graph_objects as go

from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget
from codoscope.common import Colors


# TODO: add a way to only show current code base (how to detect the file movements?)
def code_ownership(
    activity_df: pandas.DataFrame,
    title: str = "Code changes map",
    maxdepth: int = 4,
) -> PlotlyFigureWidget:
    df = activity_df.set_index("timestamp")

    commits_df = df[df["activity_type"] == "commit"]

    # user -> leaf path -> counts
    all_users_counts_map = defaultdict(
        lambda: defaultdict(
            lambda: {
                "changed_count": 0,
                "changed_lines_count": 0,
            }
        )
    )

    # leaf path -> counts
    path_counts = defaultdict(lambda: {"changed_lines_count": 0})

    for _, row in commits_df.iterrows():
        user = row["user"]
        user_counts_map = all_users_counts_map[user]
        for path, stat in row["commit_changed_files_map"].items():
            user_counts_map[path]["changed_count"] += 1
            user_counts_map[path]["changed_lines_count"] += stat["added"]
            user_counts_map[path]["changed_lines_count"] += stat["deleted"]
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

    # TODO: upon clicking update another simple bar plot map showing top
    # contributors by changed lines using JS?

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
