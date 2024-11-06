import abc
import enum
import typing
from datetime import datetime

import plotly.graph_objects as go
import tzlocal

from codoscope.common import render_jinja_template
from codoscope.datasets import Datasets
from codoscope.state import StateModel
from codoscope.widgets.common import WidgetBase


class ReportType(enum.StrEnum):
    OVERVIEW = "overview"
    INTERNAL_STATE = "internal-state"
    PER_USER_STATS = "per-user-stats"
    PER_SOURCE_STATS = "per-source-stats"
    UNIQUE_USERS = "unique-users"
    DATASETS_EXPORT = "datasets-export"
    WORD_CLOUDS = "word-clouds"
    PR_REVIEWS = "pr-reviews"
    CODE_OWNERSHIP = "code-ownership"
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
        # TODO: make color scheme customizable by some config section
        # colorway=plotly.colors.qualitative.Prism,
        title=title,
        title_font_family="Ubuntu",
        # title_font_variant='small-caps',
        font_family="Ubuntu",
        plot_bgcolor="white",
        paper_bgcolor="white",  # Outer background (around the plot)
        xaxis=dict(
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=1,
            griddash="dot",
            nticks=30,
            showline=True,
            linewidth=1.5,
            linecolor="gray",
            mirror=True,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=1,
            griddash="dot",
            nticks=20,
            showline=True,
            linewidth=1.5,
            linecolor="gray",
            mirror=True,
        ),
        yaxis2=dict(
            # there is no good way to control the spacing between axis and tick
            # labels, but we can add a prefix to tick labels
            # see https://stackoverflow.com/questions/63457187
            tickprefix=" ",
        ),
        legend=dict(
            traceorder="normal",  # use the order in which traces were added
        ),
        hoverlabel=dict(
            font_size=12,
            font_family="Ubuntu",
            # do not limit the hover label length
            # https://github.com/plotly/plotly.js/issues/460
            namelength=-1,
        ),
    )


def render_html_report(path: str, body: str, title: str) -> None:
    local_tz = tzlocal.get_localzone()
    now = datetime.now(local_tz)
    tz_name = local_tz.tzname(now)

    html: str = render_jinja_template(
        "report.html.jinja2",
        context={
            "title": f"codoscope :: {title}",
            "body": body,
            "generated_on": "%s %s" % (now.strftime("%B %d, %Y at %H:%M:%S"), tz_name),
        },
    )

    with open(path, "w") as f:
        f.write(html)


def render_widgets_report(
    path: str, widgets: typing.Iterable[WidgetBase | go.Figure | str | None], title: str
) -> None:
    body_items = []
    for widget in widgets:
        if widget is None:
            continue
        if isinstance(widget, WidgetBase):
            html = widget.get_html()
        elif isinstance(widget, go.Figure):
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
