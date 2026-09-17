"""Chart helpers: the palette, the Plotly template and the chart shapes the pages use.

Hues follow one rule each. Categorical slots identify series in fixed order and
are never cycled; one sequential hue carries magnitude; status colors are reserved
for pipeline state and never stand in for a series. All validated for the light
surface with the dataviz palette validator on 2026-09-14.
"""

from enum import Enum

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

CATEGORICAL = [
    "#2a78d6",  # 1 blue
    "#eb6834",  # 2 orange
    "#1baf7a",  # 3 aqua
    "#eda100",  # 4 yellow
    "#e87ba4",  # 5 magenta
    "#008300",  # 6 green
    "#4a3aa7",  # 7 violet
    "#e34948",  # 8 red
]
SEQUENTIAL = "#2a78d6"
INK = "#0b0b0b"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"

pio.templates["steam"] = go.layout.Template(
    layout={
        "font": {
            "family": "system-ui, sans-serif",
            "color": INK,
            "size": 13,
        },
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "colorway": CATEGORICAL,
        "margin": {"l": 8, "r": 8, "t": 8, "b": 8},
        "xaxis": {
            "gridcolor": GRID,
            "linecolor": BASELINE,
            "zeroline": False,
            "tickfont": {"color": INK_MUTED},
            "automargin": True,
        },
        "yaxis": {
            "gridcolor": GRID,
            "linecolor": BASELINE,
            "zeroline": False,
            "tickfont": {"color": INK_MUTED},
            "automargin": True,
        },
        "hoverlabel": {"bgcolor": "#fcfcfb", "bordercolor": GRID, "font": {"color": INK}},
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "x": 0,
            "entrywidth": 110,
            "entrywidthmode": "pixels",
        },
    }
)
pio.templates.default = "steam"


class _NotebookRendererNoMath(pio._base_renderers.NotebookRenderer):
    """Plotly's notebook renderer minus the MathJax loader it adds to every figure

    The loader collides with the copy inside plotly.js 4 and logs an error per
    figure on the published page; nothing on the dashboard is math.
    """

    def to_mimebundle(self, fig_dict):
        bundle = super().to_mimebundle(fig_dict)
        html = bundle["text/html"]
        start = html.find('<script src="https://cdnjs.cloudflare.com/ajax/libs/mathjax/')
        if start >= 0:
            end = html.find("</script>", start) + len("</script>")
            bundle["text/html"] = html[:start] + html[end:]
        return bundle


pio.renderers["notebook_nomath"] = _NotebookRendererNoMath(connected=False)
pio.renderers.default = "notebook_nomath"


def ranked_bars(df: pd.DataFrame, value: str, label: str) -> go.Figure:
    """Horizontal bars for a ranking: one hue, largest on top, the value at each end.

    Values ride on a text-only scatter trace. In Quarto pages plotly.js 4 drops bar
    text and ignores annotation anchors; scatter text anchors where it says it does.
    """
    fig = px.bar(df, x=value, y=label, orientation="h")
    fig.update_traces(
        marker_color=SEQUENTIAL,
        marker_cornerradius=4,
        marker_line_width=0,
        hovertemplate="%{y}<br>%{x:,} players<extra></extra>",
    )
    fig.add_trace(
        go.Scatter(
            x=df[value],
            y=df[label],
            mode="text",
            text=[f"\u2002{v:,}" for v in df[value]],
            textposition="middle right",
            hoverinfo="skip",
            showlegend=False,
            cliponaxis=False,
        )
    )
    fig.update_layout(
        bargap=0.35,
        showlegend=False,
        yaxis={"autorange": "reversed", "title": None, "showgrid": False},
        # Padded so the longest bar's label stays inside the card.
        xaxis={
            "title": None,
            "showticklabels": False,
            "showgrid": False,
            "range": [0, df[value].max() * 1.22],
        },
    )
    return fig


class Status(Enum):
    """Snapshot-hour statuses as fct_snapshot_hours emits them, in legend order."""

    COMPLETE = ("complete", "Complete", SEQUENTIAL, "")
    RAN_SHORT = ("ran_short", "Ran short", "#fab219", "!")
    UNRECORDED = ("unrecorded", "Unrecorded", "#d03b3b", "×")
    UNAUDITED = ("unaudited", "Unaudited", "#e1e0d9", "")

    def __new__(cls, value: str, label: str, color: str, glyph: str):
        member = object.__new__(cls)
        member._value_ = value
        member.label = label
        member.color = color
        member.glyph = glyph
        return member


def status_grid(df: pd.DataFrame) -> go.Figure:
    """Day-by-hour grid of snapshot hours colored by status, with a glyph on the bad ones.

    Expects columns day, hour, status, hover. Pivoting is the only reshaping here.
    """
    unknown = set(df["status"]) - {s.value for s in Status}
    if unknown:
        raise ValueError(
            f"fct_snapshot_hours emits statuses the dashboard does not know: {unknown}"
        )
    order = list(Status)
    df = df.assign(hour=df["hour"].map("{:02d}".format))
    codes = {s.value: i for i, s in enumerate(order)}
    z = df.pivot(index="day", columns="hour", values="status").map(codes.get)
    hover = df.pivot(index="day", columns="hour", values="hover")
    n = len(order)
    colorscale = [
        step for i, s in enumerate(order) for step in ([i / n, s.color], [(i + 1) / n, s.color])
    ]
    fig = go.Figure(
        go.Heatmap(
            z=z.values,
            x=z.columns,
            y=z.index.astype(str),
            customdata=hover.values,
            zmin=-0.5,  # codes 0..n-1 sit centered in n equal bands
            zmax=n - 0.5,
            colorscale=colorscale,
            showscale=False,
            xgap=2,
            ygap=2,
            hoverongaps=False,
            hovertemplate="%{y} %{x}:00 UTC<br>%{customdata}<extra></extra>",
        )
    )
    glyphs = {s.value: s.glyph for s in order if s.glyph}
    flagged = df[df["status"].isin(glyphs)]
    fig.add_trace(
        go.Scatter(
            x=flagged["hour"],
            y=flagged["day"].astype(str),
            mode="text",
            text=flagged["status"].map(glyphs),
            textfont={"color": "#ffffff", "size": 14},
            hoverinfo="skip",
            showlegend=False,
        )
    )
    for s in order:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker={"symbol": "square", "size": 12, "color": s.color},
                name=s.label,
            )
        )
    fig.update_layout(height=120 + 16 * len(z))
    fig.update_layout(
        xaxis={"type": "category", "title": None, "showgrid": False},
        yaxis={"type": "category", "autorange": "reversed", "title": None, "showgrid": False},
    )
    return fig


def lines(
    df: pd.DataFrame, x: str, ys: list[str], names: list[str], log_y: bool = False
) -> go.Figure:
    """Up to four series of one unit on one axis, categorical hues in slot order.

    log_y is for series two or more orders of magnitude apart; one axis, still.
    """
    fig = go.Figure()
    for col, name, color in zip(ys, names, CATEGORICAL, strict=False):
        fig.add_trace(
            go.Scatter(
                x=df[x],
                y=df[col],
                mode="lines",
                name=name,
                line={"width": 2, "color": color},
                hovertemplate="%{y:,}<extra>" + name + "</extra>",
            )
        )
    yaxis = (
        {"title": None, "type": "log", "dtick": 1}
        if log_y
        else {"title": None, "rangemode": "tozero"}
    )
    fig.update_layout(
        hovermode="x unified",
        xaxis={
            "title": None,
            "showspikes": True,
            "spikemode": "across",
            "spikethickness": 1,
            "spikecolor": BASELINE,
            "spikedash": "solid",
        },
        yaxis=yaxis,
    )
    return fig


def small_multiples(
    df: pd.DataFrame,
    x: str,
    ys: list[str],
    names: list[str],
    facet: str,
    wrap: int = 3,
    hours: str | None = None,
) -> go.Figure:
    """One panel per facet value, each with the same series on its own y axis.

    Facets solve the scale problem: one game ten times larger than the rest would
    flatten every other panel on a shared axis, and a second axis is not an option.
    """
    keep = [x, facet] + ([hours] if hours else [])
    long = df.melt(id_vars=keep, value_vars=ys, var_name="series", value_name="value")
    long["series"] = long["series"].map(dict(zip(ys, names, strict=True)))
    fig = px.line(
        long,
        x=x,
        y="value",
        color="series",
        facet_col=facet,
        facet_col_wrap=wrap,
        category_orders={facet: list(df[facet].unique())},
        color_discrete_sequence=CATEGORICAL,
        custom_data=[hours] if hours else None,
    )
    hover = (
        "%{y:,.0f}"
        + (" over %{customdata[0]} h" if hours else "")
        + "<extra>%{fullData.name}</extra>"
    )
    fig.update_traces(line={"width": 2}, hovertemplate=hover)
    fig.update_yaxes(matches=None, title=None, rangemode="tozero", showticklabels=True)
    fig.update_xaxes(title=None)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=", 1)[1]))
    fig.update_layout(
        hovermode="x unified",
        legend={"orientation": "h", "x": 0, "y": -0.18, "yanchor": "top", "title": {"text": ""}},
        margin={"t": 32},
    )
    return fig


def diverging_bars(df: pd.DataFrame, value: str, label: str) -> go.Figure:
    """Horizontal bars around zero: gains in the blue pole, losses in the red pole.

    Values are fractions and are labeled as percentages on a text-only trace,
    right of the gains and left of the losses.
    """
    up, down = CATEGORICAL[0], CATEGORICAL[7]
    colors = [up if v >= 0 else down for v in df[value]]
    fig = go.Figure(
        go.Bar(
            x=df[value],
            y=df[label],
            orientation="h",
            marker_color=colors,
            marker_cornerradius=4,
            marker_line_width=0,
            hovertemplate="%{y}<br>%{x:+.1%} from the previous day<extra></extra>",
        )
    )
    for side, pos in ((df[df[value] >= 0], "middle right"), (df[df[value] < 0], "middle left")):
        pad = "\u2002"
        fig.add_trace(
            go.Scatter(
                x=side[value],
                y=side[label],
                mode="text",
                text=[f"{pad}{v:+.1%}{pad}" for v in side[value]],
                textposition=pos,
                hoverinfo="skip",
                showlegend=False,
                cliponaxis=False,
            )
        )
    span = df[value].abs().max() * 1.6
    fig.update_layout(
        bargap=0.35,
        showlegend=False,
        yaxis={
            "autorange": "reversed",
            "title": None,
            "showgrid": False,
            "tickmode": "linear",
            "dtick": 1,
        },
        xaxis={"title": None, "showticklabels": False, "showgrid": False, "range": [-span, span]},
    )
    return fig


def bars(df: pd.DataFrame, x: str, y: str, unit: str) -> go.Figure:
    """Vertical bars in one hue for a short categorical x, such as the 24 hours of a day."""
    fig = go.Figure(
        go.Bar(
            x=df[x],
            y=df[y],
            marker_color=SEQUENTIAL,
            marker_cornerradius=4,
            marker_line_width=0,
            hovertemplate="%{x}:00 UTC<br>%{y:,.0f} " + unit + "<extra></extra>",
        )
    )
    fig.update_layout(
        bargap=0.3,
        showlegend=False,
        xaxis={"type": "category", "title": None, "showgrid": False},
        yaxis={"title": None, "rangemode": "tozero"},
    )
    return fig
