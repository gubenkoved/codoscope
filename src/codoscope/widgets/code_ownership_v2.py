from collections import defaultdict

import pandas

from codoscope.common import convert_timezone, render_jinja_template
from codoscope.widgets.common import Widget, generate_html_element_id


def code_ownership_v2(
    activity_df: pandas.DataFrame,
    title: str = "Code changes map",
    max_depth: int = 4,
    height: int | None = None,
    show_users_breakdown_pane: bool = True,
) -> Widget | None:

    # make sure we only have commits
    commits_df = activity_df[activity_df["activity_type"] == "commit"].copy()

    # remove merge commits as useless
    commits_df = commits_df[commits_df["commit_is_merge_commit"] == False]

    if len(commits_df) == 0:
        return None

    # needed just to specify proper types for timestamp and avoid warnings
    commits_df = convert_timezone(commits_df, timezone_name="utc")
    commits_df = commits_df.set_index("timestamp")

    # leaf path -> user -> counts
    path_counts = defaultdict(
        lambda: defaultdict(
            lambda: {
                "added_lines": 0,
                "deleted_lines": 0,
            }
        )
    )

    for _, row in commits_df.iterrows():
        user = row["user"]
        for path, stat in row["commit_changed_files_map"].items():
            path_counts[path][user]["added_lines"] += stat["added"]
            path_counts[path][user]["deleted_lines"] += stat["deleted"]

    html = render_jinja_template(
        "code_tree_map.jinja2",
        {
            "title": title,
            "data": path_counts,
            "max_depth": max_depth,
            "height": height or "100%",
            "container_id": generate_html_element_id(),
            "show_users_breakdown_pane": show_users_breakdown_pane,
            "font": "Ubuntu",
        },
    )

    return Widget.from_html(html)
