import abc
import enum
import typing
from datetime import datetime

import plotly.graph_objects as go
import tzlocal

from codoscope.datasets import Datasets
from codoscope.state import StateModel


class ReportType(enum.StrEnum):
    OVERVIEW = 'overview'
    INTERNAL_STATE = 'internal-state'
    PER_USER_STATS = 'per-user-stats'
    PER_SOURCE_STATS = 'per-source-stats'
    UNIQUE_USERS = 'unique-users'
    WORD_CLOUDS = 'word-clouds'
    INDEX = 'index'


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
        title_font_family='Ubuntu',
        # title_font_variant='small-caps',
        font_family='Ubuntu',
        plot_bgcolor='white',
        xaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            gridwidth=1,
            griddash='dot',
            nticks=30,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            gridwidth=1,
            griddash='dot',
            nticks=20,
        ),
        shapes=[  # Add an outer border
            dict(
                type='rect',
                xref='paper',
                yref='paper',  # Reference the entire paper (plot area)
                x0=0,
                y0=0,
                x1=1,
                y1=1,
                line=dict(
                    color='gray',
                    width=1.2,
                ),
            )
        ],
        legend=dict(
            traceorder='normal',  # use the order in which traces were added
        ),
    )

    fig.update_layout(
        hoverlabel=dict(
            font_size=12,
            font_family='Ubuntu',
        )
    )


def render_html_report(path: str, body: str, title: str) -> None:
    local_tz = tzlocal.get_localzone()
    now = datetime.now(local_tz)
    tz_name = local_tz.tzname(now)

    with open(path, 'w') as f:
        f.write('<html>\n')
        f.write('<head>\n')
        f.write(f'<title>codoscope :: {title}</title>\n')
        f.write("""
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <link href="https://fonts.googleapis.com/css2?family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
            <link href="https://fonts.googleapis.com/css2?family=Ubuntu+Mono:ital,wght@0,400;0,700;1,400;1,700&family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
        """)
        f.write(
            """<style>
            body {
                font-family: "Ubuntu";
                overflow-y: scroll; /* Always show vertical scrollbar */
            }
            </style>
            """
        )
        f.write('</head>\n')
        f.write('<body>\n')
        f.write(body)
        f.write(
            f"""
            <div style="color: lightgray; font-size: 11px; text-align: center;">
                <i>generated on {now.strftime('%B %d, %Y at %H:%M:%S')} {tz_name}</i>
            </div>
            """
        )
        f.write('</body>\n')
        f.write('</html>\n')


def render_widgets_report(
        path: str, widgets: typing.Iterable[go.Figure | str | None], title: str) -> None:
    body_items = []
    for widget in widgets:
        if widget is None:
            continue
        if isinstance(widget, go.Figure):
            html = widget.to_html(full_html=False, include_plotlyjs='cdn')
        elif isinstance(widget, str):
            html = widget
        else:
            raise Exception('unknown widget type "%s"' % type(widget))
        body_items.append(html)
    render_html_report(path, '\n'.join(body_items), title)
