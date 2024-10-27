import abc
import uuid

from plotly import graph_objects as go


class WidgetBase(abc.ABC):
    @abc.abstractmethod
    def get_html(self) -> str:
        raise NotImplementedError


class Widget(WidgetBase):
    def __init__(self, html: str):
        self.html = html

    def get_html(self) -> str:
        return self.html

    @classmethod
    def centered(cls, inner: str) -> "Widget":
        html = '<div style="display: flex; justify-content: center; align-items: center; height: 100%;">'
        html += "<div>"
        html += inner
        html += "</div>"
        html += "</div>"
        return Widget(html)


class PlotlyFigureWidget(WidgetBase):
    def __init__(
        self,
        figure: go.Figure,
        height: int | None = None,
    ):
        self.figure = figure

        if height is not None:
            figure.update_layout(
                height=height,
            )

    def get_html(self) -> str:
        return self.figure.to_html(full_html=False, include_plotlyjs="cdn")


class CompositeWidget(WidgetBase):
    def __init__(
        self,
        rows: list[list[WidgetBase | None]],
        padding: int = 1,
    ) -> None:
        self.rows: list[list[WidgetBase | None]] = rows
        self.padding: int = padding

    def get_html(self) -> str:
        id_ = "container-%s" % uuid.uuid4()
        html: str = f'<div id="{id_}">'
        for row in self.rows:
            html += f'<div style="display: flex;">'
            for widget in row:
                html += f'<div style="flex: 1; padding: {self.padding}px;">'
                if widget is None:
                    widget = Widget.centered("<i>No data</i>")
                html += widget.get_html()
                html += "</div>"
            html += f"</div>"

        # trigger resize so that plotly graphs can realize correct width for them
        html += f"""
        <script>
            const plotlyGraphs = document.querySelectorAll('#{id_} .plotly-graph-div');

            plotlyGraphs.forEach(function(graph) {{
                Plotly.Plots.resize(graph);
            }});
        </script>
        """
        html += "</div>"
        return html
