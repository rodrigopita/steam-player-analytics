"""Chart helpers: the palette, the Plotly template and the chart shapes the pages use.

Hues follow one rule each. Categorical slots identify series in fixed order and
are never cycled; one sequential hue carries magnitude; status colors are reserved
for pipeline state and never stand in for a series. All validated for the light
surface with the dataviz palette validator on 2026-09-14.
"""

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
STATUS = {
    "complete": "#0ca30c",
    "ran_short": "#fab219",
    "unrecorder": "#d03b3b",
    "unaudited": "#c3c2b7",
}

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
        "legend": {"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    }
)
pio.templates.default = "steam"


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
