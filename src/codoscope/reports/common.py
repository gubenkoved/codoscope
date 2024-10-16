import abc
import enum
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
def setup_default_layout(fig, title=None):
    fig.update_layout(
        title=title,
        title_font_family="Ubuntu",
        # title_font_variant="small-caps",
        font_family="Ubuntu",
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
                type="rect",
                xref="paper", yref="paper",  # Reference the entire paper (plot area)
                x0=0, y0=0, x1=1, y1=1,
                line=dict(color="gray", width=1.2)
            )
        ],
    )

    fig.update_layout(
        hoverlabel=dict(
            font_size=12,
            font_family="Ubuntu",
        )
    )


def render_plotly_report(path: str, figures: list[go.Figure], title: str):
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
        f.write('<style>body { font-family: "Ubuntu"; }</style>\n')
        f.write('</head>\n')
        f.write('<body>\n')
        for fig in figures:
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
        f.write(
            f"""
            <div style="color: lightgray; font-size: 11px; text-align: right;">
                <i>last updated on {now.strftime('%B %d, %Y at %H:%M:%S')} {tz_name}</i>\n
            </div>
            """
        )
        f.write('</body>\n')
        f.write('</html>\n')
