import abc
import enum
import typing
from datetime import datetime

import plotly.graph_objects as go
import tzlocal

from codoscope.datasets import Datasets
from codoscope.state import StateModel


class ReportType(enum.StrEnum):
    OVERVIEW = "overview"
    INTERNAL_STATE = "internal-state"
    PER_USER_STATS = "per-user-stats"
    PER_SOURCE_STATS = "per-source-stats"
    UNIQUE_USERS = "unique-users"
    DATASETS_EXPORT = "datasets-export"
    WORD_CLOUDS = "word-clouds"
    INDEX = "index"


class ReportBase(abc.ABC):
    @abc.abstractmethod
    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        raise NotImplementedError

    # TODO: define class property
    @classmethod
    def get_type(cls) -> ReportType:
        raise NotImplementedError


# TODO: move to separate plotly related module to better organize things
def setup_default_layout(fig: go.Figure, title: str | None = None) -> None:
    fig.update_layout(
        title=title,
        title_font_family="Ubuntu",
        # title_font_variant='small-caps',
        font_family="Ubuntu",
        plot_bgcolor="white",
        xaxis=dict(
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=1,
            griddash="dot",
            nticks=30,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=1,
            griddash="dot",
            nticks=20,
        ),
        shapes=[  # Add an outer border
            dict(
                type="rect",
                xref="paper",
                yref="paper",  # Reference the entire paper (plot area)
                x0=0,
                y0=0,
                x1=1,
                y1=1,
                line=dict(
                    color="gray",
                    width=1.2,
                ),
            )
        ],
        legend=dict(
            traceorder="normal",  # use the order in which traces were added
        ),
    )

    fig.update_layout(
        hoverlabel=dict(
            font_size=12,
            font_family="Ubuntu",
        )
    )


def render_html_report(path: str, body: str, title: str) -> None:
    local_tz = tzlocal.get_localzone()
    now = datetime.now(local_tz)
    tz_name = local_tz.tzname(now)

    # TODO: consider using JINJA template instead
    html = """
<html>
    <head>
    <title>codoscope :: ##TITLE##</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Ubuntu+Mono:ital,wght@0,400;0,700;1,400;1,700&family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
    <style>
        body {
            font-family: "Ubuntu";
            overflow-y: scroll; /* Always show vertical scrollbar */
        }

        .loader-wrapper {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
            background-color: #fff;
            z-index: 9999;
        }

        .loader {
            border: 5px solid #f3f3f3;
            border-top: 5px solid #3498db;
            border-radius: 50%;
            width: 120px;
            height: 120px;
            animation: spin 1s linear infinite;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div id="loader-wrapper" class="loader-wrapper">
        <div id="loader" class="loader"></div>
    </div>
    <div id="content">
        ##BODY##
        <div style="color: lightgray; font-size: 11px; text-align: center;">
            <i>generated on ##GENERATED_ON##</i>
        </div>
    </div>

    <script>
        window.addEventListener('load', function() {
            document.getElementById('loader-wrapper').style.display = 'none';
        });
    </script>
</body>
</html>
"""

    html = (
        html.replace("##TITLE##", title)
        .replace("##BODY##", body)
        .replace(
            "##GENERATED_ON##",
            "%s %s" % (now.strftime("%B %d, %Y at %H:%M:%S"), tz_name),
        )
    )

    with open(path, "w") as f:
        f.write(html)


def render_widgets_report(
    path: str, widgets: typing.Iterable[go.Figure | str | None], title: str
) -> None:
    body_items = []
    for widget in widgets:
        if widget is None:
            continue
        if isinstance(widget, go.Figure):
            html = widget.to_html(full_html=False, include_plotlyjs="cdn")
        elif isinstance(widget, str):
            html = widget
        else:
            raise Exception('unknown widget type "%s"' % type(widget))
        body_items.append(html)
    render_html_report(path, "\n".join(body_items), title)


def time_axis(steps=24):
    valess = []
    labels = []

    for offset in range(0, 24 * 60 + 1, 24 * 60 // steps):
        hours = offset // 60
        minutes = offset % 60
        valess.append(offset)
        labels.append(f"{hours:02}:{minutes:02}")

    return valess, labels
