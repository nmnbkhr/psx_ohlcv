"""Chart helpers for PSX OHLCV Streamlit UI.

Provides reusable Plotly chart components for consistent visualization.
Supports theming via the themes module for Bloomberg Terminal-style charts.
"""

from typing import Literal

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from psx_ohlcv.ui.themes import (
    get_chart_colors,
    get_plotly_layout,
    get_theme,
    ThemeName,
)

# Chart constants
MIN_CHART_HEIGHT = 520
MIN_CANDLESTICK_HEIGHT = 650  # Higher minimum for candlestick pages
VOLUME_ROW_HEIGHT_RATIO = 0.25
PRICE_ROW_HEIGHT_RATIO = 0.75

# Default theme for chart colors (can be overridden per-chart)
_CURRENT_THEME: ThemeName = "bloomberg"


def set_chart_theme(theme_name: ThemeName):
    """Set the global chart theme.

    Args:
        theme_name: Theme name ('default' or 'bloomberg')
    """
    global _CURRENT_THEME
    _CURRENT_THEME = theme_name


def get_colors():
    """Get current theme colors for charts."""
    return get_chart_colors(_CURRENT_THEME)


# Color scheme - Bloomberg Terminal palette (dynamic based on theme)
def _get_color_bullish():
    return get_colors()["bullish"]


def _get_color_bearish():
    return get_colors()["bearish"]


def _get_color_bullish_light():
    return get_colors()["bullish_light"]


def _get_color_bearish_light():
    return get_colors()["bearish_light"]


def _get_color_sma_20():
    return get_colors()["sma_20"]


def _get_color_sma_50():
    return get_colors()["sma_50"]


def _get_color_volume():
    return get_colors()["volume"]


def _get_color_grid():
    return get_colors()["grid"]


def _get_color_neutral():
    return get_colors()["neutral"]


# Backward compatibility - these are now functions returning the current theme colors
COLOR_BULLISH = "#00C853"  # Fallback - actual color from theme
COLOR_BEARISH = "#FF5252"  # Fallback - actual color from theme
COLOR_BULLISH_LIGHT = "rgba(0, 200, 83, 0.12)"
COLOR_BEARISH_LIGHT = "rgba(255, 82, 82, 0.12)"
COLOR_SMA_20 = "#FFB300"   # Amber for SMA 20 (Bloomberg)
COLOR_SMA_50 = "#00B8D4"   # Cyan for SMA 50 (Bloomberg)
COLOR_VOLUME = "#2F81F7"   # Bloomberg blue for volume
COLOR_GRID = "rgba(30, 35, 41, 0.8)"  # Bloomberg muted grid
COLOR_NEUTRAL = "#6B7280"  # Gray for unchanged

# Font settings - Bloomberg monospace-forward
FONT_FAMILY = "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace"
FONT_SIZE_TITLE = 14
FONT_SIZE_AXIS = 11
FONT_SIZE_TICK = 10

# Bloomberg-style layout defaults
PAPER_BG = "#0B0E11"
PLOT_BG = "#0B0E11"
TEXT_COLOR = "#EAECEF"
TEXT_SECONDARY = "#9AA4B2"
BORDER_COLOR = "#1E2329"


def apply_bloomberg_layout(fig: go.Figure) -> go.Figure:
    """Apply Bloomberg Terminal styling to a Plotly figure.

    Args:
        fig: Plotly Figure to style

    Returns:
        Styled Plotly Figure
    """
    colors = get_colors()
    theme = get_theme(_CURRENT_THEME)

    fig.update_layout(
        paper_bgcolor=colors["paper"],
        plot_bgcolor=colors["background"],
        font=dict(
            family=FONT_FAMILY,
            color=colors["text"],
            size=FONT_SIZE_TICK,
        ),
        title=dict(
            font=dict(
                family=FONT_FAMILY,
                color=colors["text"],
                size=FONT_SIZE_TITLE,
            ),
        ),
        hoverlabel=dict(
            bgcolor=theme.bg_elevated,
            bordercolor=theme.border_primary,
            font=dict(
                family=FONT_FAMILY,
                color=colors["text"],
                size=12,
            ),
        ),
        modebar=dict(
            bgcolor="rgba(0,0,0,0)",
            color=colors["text_secondary"],
            activecolor=colors["text"],
        ),
    )

    # Update all axes
    fig.update_xaxes(
        gridcolor=colors["grid"],
        linecolor=theme.border_primary,
        tickfont=dict(color=colors["text_secondary"], size=FONT_SIZE_TICK),
        titlefont=dict(color=colors["text_secondary"], size=FONT_SIZE_AXIS),
        zerolinecolor=colors["grid"],
    )
    fig.update_yaxes(
        gridcolor=colors["grid"],
        linecolor=theme.border_primary,
        tickfont=dict(color=colors["text_secondary"], size=FONT_SIZE_TICK),
        titlefont=dict(color=colors["text_secondary"], size=FONT_SIZE_AXIS),
        zerolinecolor=colors["grid"],
    )

    return fig


def compute_sma(df: pd.DataFrame, column: str, period: int) -> pd.Series:
    """Compute Simple Moving Average.

    Args:
        df: DataFrame with price data.
        column: Column name to compute SMA on.
        period: Number of periods for the moving average.

    Returns:
        Series with SMA values.
    """
    return df[column].rolling(window=period, min_periods=1).mean()


def make_candlestick(
    df: pd.DataFrame,
    title: str,
    date_col: str = "date",
    show_sma: bool = True,
    height: int | None = None,
) -> go.Figure:
    """Create a professional candlestick chart with volume subplot.

    Creates a 2-row subplot figure:
    - Row 1 (75%): Candlestick OHLC with optional SMA overlays
    - Row 2 (25%): Volume bars colored by price direction

    Args:
        df: DataFrame with columns: date/ts, open, high, low, close, volume.
        title: Chart title (displayed in subplot).
        date_col: Name of the date/timestamp column (default: "date").
        show_sma: Whether to show SMA(20) and SMA(50) overlays (default: True).
        height: Chart height in pixels (default: MIN_CANDLESTICK_HEIGHT).

    Returns:
        Plotly Figure object ready for display.

    Raises:
        ValueError: If required columns are missing from the DataFrame.
    """
    # Validate required columns
    required_cols = [date_col, "open", "high", "low", "close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if df.empty:
        # Return empty figure with message
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=16),
        )
        fig.update_layout(height=height or MIN_CANDLESTICK_HEIGHT)
        return fig

    # Sort by date for proper display
    df = df.sort_values(date_col).copy()

    # Compute SMAs if requested
    if show_sma and len(df) >= 20:
        df["sma_20"] = compute_sma(df, "close", 20)
    if show_sma and len(df) >= 50:
        df["sma_50"] = compute_sma(df, "close", 50)

    # Create 2-row subplot
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=(title, "Volume"),
        row_heights=[PRICE_ROW_HEIGHT_RATIO, VOLUME_ROW_HEIGHT_RATIO],
    )

    # Get theme colors
    colors = get_colors()
    bullish = colors["bullish"]
    bearish = colors["bearish"]
    sma_20_color = colors["sma_20"]
    sma_50_color = colors["sma_50"]
    grid_color = colors["grid"]

    # Row 1: Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df[date_col],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="OHLC",
            increasing_line_color=bullish,
            decreasing_line_color=bearish,
            increasing_fillcolor=bullish,
            decreasing_fillcolor=bearish,
        ),
        row=1,
        col=1,
    )

    # Add SMA overlays
    if show_sma and "sma_20" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["sma_20"],
                mode="lines",
                name="SMA(20)",
                line=dict(color=sma_20_color, width=1.5),
                hovertemplate="%{x}<br>SMA(20): %{y:.2f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    if show_sma and "sma_50" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[date_col],
                y=df["sma_50"],
                mode="lines",
                name="SMA(50)",
                line=dict(color=sma_50_color, width=1.5),
                hovertemplate="%{x}<br>SMA(50): %{y:.2f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    # Row 2: Volume bars colored by price direction
    bar_colors = [
        bullish if row["close"] >= row["open"] else bearish
        for _, row in df.iterrows()
    ]
    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df["volume"],
            name="Volume",
            marker_color=bar_colors,
            opacity=0.7,
            hovertemplate="%{x}<br>Volume: %{y:,.0f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # Calculate price range for auto-scaling
    price_min = df[["open", "high", "low", "close"]].min().min()
    price_max = df[["open", "high", "low", "close"]].max().max()
    price_range = price_max - price_min
    # Use at least 1% of price as padding, or 10% of range
    price_padding = max(price_range * 0.1, price_min * 0.01) if price_min > 0 else 1

    # Update layout with Bloomberg styling
    chart_height = height or MIN_CANDLESTICK_HEIGHT
    fig.update_layout(
        height=chart_height,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=FONT_SIZE_TICK, color=colors["text_secondary"]),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=70, t=60, b=50),  # Right margin for Y-axis labels
        hovermode="x unified",
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK, color=colors["text"]),
        paper_bgcolor=colors["paper"],
        plot_bgcolor=colors["background"],
    )

    # Update y-axes (Bloomberg style: axis on right side)
    fig.update_yaxes(
        range=[price_min - price_padding, price_max + price_padding],
        row=1,
        col=1,
        title_text="Price (PKR)",
        title_font=dict(size=FONT_SIZE_AXIS, color=colors["text_secondary"]),
        tickformat=".2f",
        tickfont=dict(size=FONT_SIZE_TICK, color=colors["text_secondary"]),
        gridcolor=grid_color,
        showgrid=True,
        side="right",
        linecolor=BORDER_COLOR,
    )
    fig.update_yaxes(
        row=2,
        col=1,
        title_text="Volume",
        title_font=dict(size=FONT_SIZE_AXIS, color=colors["text_secondary"]),
        tickfont=dict(size=FONT_SIZE_TICK, color=colors["text_secondary"]),
        gridcolor=grid_color,
        showgrid=True,
        side="right",
        linecolor=BORDER_COLOR,
    )

    # Update x-axes
    fig.update_xaxes(
        row=2,
        col=1,
        title_text="Date",
        title_font=dict(size=FONT_SIZE_AXIS, color=colors["text_secondary"]),
        tickfont=dict(size=FONT_SIZE_TICK, color=colors["text_secondary"]),
        gridcolor=grid_color,
        showgrid=True,
        linecolor=BORDER_COLOR,
    )
    fig.update_xaxes(
        row=1,
        col=1,
        tickfont=dict(size=FONT_SIZE_TICK, color=colors["text_secondary"]),
        gridcolor=grid_color,
        showgrid=True,
        linecolor=BORDER_COLOR,
    )

    # Update subplot title font
    for annotation in fig.layout.annotations:
        annotation.font.size = FONT_SIZE_TITLE
        annotation.font.color = colors["text"]

    return fig


def make_price_line(
    df: pd.DataFrame,
    title: str,
    date_col: str = "date",
    price_col: str = "close",
    height: int = 350,
    show_area: bool = True,
) -> go.Figure:
    """Create a simple line chart for price trends.

    Args:
        df: DataFrame with date and price columns.
        title: Chart title.
        date_col: Name of the date/timestamp column.
        price_col: Name of the price column to plot.
        height: Chart height in pixels.
        show_area: Whether to show area fill under the line.

    Returns:
        Plotly Figure object.
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )
        fig.update_layout(height=height, title=title)
        return fig

    df = df.sort_values(date_col).copy()

    # Calculate range for auto-scaling
    price_min = df[price_col].min()
    price_max = df[price_col].max()
    price_range = price_max - price_min
    y_padding = max(price_range * 0.1, price_min * 0.01) if price_min > 0 else 1

    # Get theme colors
    colors = get_colors()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[price_col],
            mode="lines",
            name=title,
            line=dict(color=colors["accent"], width=2),
            fill="tozeroy" if show_area else None,
            fillcolor=colors["info_light"] if show_area else None,
            hovertemplate="%{x}<br>%{y:.2f}<extra></extra>",
        )
    )

    fig.update_layout(
        height=height,
        title=dict(text=title, font=dict(size=FONT_SIZE_TITLE, color=colors["text"])),
        margin=dict(l=10, r=70, t=50, b=50),
        yaxis=dict(
            range=[price_min - y_padding, price_max + y_padding],
            title="Price (PKR)",
            tickformat=".2f",
            gridcolor=colors["grid"],
            showgrid=True,
            tickfont=dict(color=colors["text_secondary"]),
            titlefont=dict(color=colors["text_secondary"]),
            side="right",
            linecolor=BORDER_COLOR,
        ),
        xaxis=dict(
            title="Date",
            gridcolor=colors["grid"],
            showgrid=True,
            tickfont=dict(color=colors["text_secondary"]),
            titlefont=dict(color=colors["text_secondary"]),
            linecolor=BORDER_COLOR,
        ),
        showlegend=False,
        hovermode="x",
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK, color=colors["text"]),
        paper_bgcolor=colors["paper"],
        plot_bgcolor=colors["background"],
    )

    return fig


def make_volume_chart(
    df: pd.DataFrame,
    date_col: str = "date",
    height: int = 250,
) -> go.Figure:
    """Create a standalone volume bar chart.

    Args:
        df: DataFrame with date and volume columns.
        date_col: Name of the date/timestamp column.
        height: Chart height in pixels.

    Returns:
        Plotly Figure object.
    """
    # Get theme colors
    theme_colors = get_colors()

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No volume data",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14, color=theme_colors["text_secondary"]),
        )
        fig.update_layout(
            height=height,
            title=dict(text="Volume", font=dict(size=FONT_SIZE_TITLE, color=theme_colors["text"])),
            paper_bgcolor=theme_colors["paper"],
            plot_bgcolor=theme_colors["background"],
        )
        return fig

    df = df.sort_values(date_col).copy()

    # Color bars by price direction if open/close available
    if "open" in df.columns and "close" in df.columns:
        bar_colors = [
            theme_colors["bullish"] if row["close"] >= row["open"] else theme_colors["bearish"]
            for _, row in df.iterrows()
        ]
    else:
        bar_colors = theme_colors["volume"]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df["volume"],
            name="Volume",
            marker_color=bar_colors,
            opacity=0.85,
            hovertemplate="<b>%{x}</b><br>Volume: %{y:,.0f}<extra></extra>",
        )
    )

    fig.update_layout(
        height=height,
        title=dict(
            text="Volume",
            font=dict(size=FONT_SIZE_TITLE, color=theme_colors["text"]),
            x=0,
            xanchor="left",
        ),
        margin=dict(l=10, r=70, t=50, b=50),
        yaxis=dict(
            title="",
            gridcolor=theme_colors["grid"],
            showgrid=True,
            tickfont=dict(color=theme_colors["text_secondary"]),
            side="right",
            linecolor=BORDER_COLOR,
        ),
        xaxis=dict(
            title="",
            gridcolor=theme_colors["grid"],
            showgrid=True,
            tickfont=dict(color=theme_colors["text_secondary"]),
            linecolor=BORDER_COLOR,
        ),
        showlegend=False,
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK, color=theme_colors["text"]),
        paper_bgcolor=theme_colors["paper"],
        plot_bgcolor=theme_colors["background"],
        bargap=0.2,
    )

    return fig


def make_market_breadth_chart(
    gainers: int,
    losers: int,
    unchanged: int,
    height: int = 300,
) -> go.Figure:
    """Create a market breadth pie/donut chart.

    Args:
        gainers: Number of stocks with positive change.
        losers: Number of stocks with negative change.
        unchanged: Number of stocks with no change.
        height: Chart height in pixels.

    Returns:
        Plotly Figure object.
    """
    # Get theme colors
    theme_colors = get_colors()

    total = gainers + losers + unchanged
    if total == 0:
        # Empty state
        fig = go.Figure()
        fig.add_annotation(
            text="No trading data",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14, color=theme_colors["text_secondary"]),
        )
        fig.update_layout(
            height=height,
            paper_bgcolor=theme_colors["paper"],
            plot_bgcolor=theme_colors["background"],
        )
        return fig

    labels = ["Gainers", "Losers", "Unchanged"]
    values = [gainers, losers, unchanged]
    pie_colors = [theme_colors["bullish"], theme_colors["bearish"], theme_colors["neutral"]]

    # Calculate net sentiment
    net = gainers - losers
    sentiment = "Bullish" if net > 0 else "Bearish" if net < 0 else "Neutral"
    sentiment_color = theme_colors["bullish"] if net > 0 else theme_colors["bearish"] if net < 0 else theme_colors["neutral"]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.5,  # Larger hole for center annotation
                marker=dict(
                    colors=pie_colors,
                    line=dict(color=BORDER_COLOR, width=2)
                ),
                textinfo="value",
                textfont=dict(size=FONT_SIZE_TICK + 2, color=theme_colors["text"]),
                hovertemplate="<b>%{label}</b><br>%{value} stocks<br>%{percent}<extra></extra>",
                pull=[0.02 if v == max(values) else 0 for v in values],  # Pull out the largest
            )
        ]
    )

    # Add center annotation with sentiment
    fig.add_annotation(
        text=f"<b>{sentiment}</b><br><span style='font-size:11px'>Net: {net:+d}</span>",
        x=0.5,
        y=0.5,
        font=dict(size=14, color=sentiment_color),
        showarrow=False,
    )

    fig.update_layout(
        height=height,
        title=dict(
            text="Market Breadth",
            font=dict(size=FONT_SIZE_TITLE, color=theme_colors["text"]),
            x=0,
            xanchor="left",
        ),
        margin=dict(l=20, r=20, t=50, b=40),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5,
            font=dict(size=11, color=theme_colors["text_secondary"]),
            bgcolor="rgba(0,0,0,0)",
        ),
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK, color=theme_colors["text"]),
        paper_bgcolor=theme_colors["paper"],
        plot_bgcolor=theme_colors["background"],
    )

    return fig


def make_top_movers_chart(
    df: pd.DataFrame,
    title: str,
    symbol_col: str = "symbol",
    change_col: str = "change_pct",
    height: int = 300,
    chart_type: Literal["gainers", "losers"] = "gainers",
) -> go.Figure:
    """Create a horizontal bar chart for top gainers/losers.

    Args:
        df: DataFrame with symbol and change_pct columns.
        title: Chart title.
        symbol_col: Column name for symbol.
        change_col: Column name for percentage change.
        height: Chart height in pixels.
        chart_type: "gainers" for green bars, "losers" for red bars.

    Returns:
        Plotly Figure object.
    """
    # Get theme colors
    theme_colors = get_colors()

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No movers data",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14, color=theme_colors["text_secondary"]),
        )
        fig.update_layout(
            height=height,
            title=dict(text=title, font=dict(size=FONT_SIZE_TITLE, color=theme_colors["text"])),
            paper_bgcolor=theme_colors["paper"],
            plot_bgcolor=theme_colors["background"],
        )
        return fig

    base_color = theme_colors["bullish"] if chart_type == "gainers" else theme_colors["bearish"]
    light_color = theme_colors["bullish_light"] if chart_type == "gainers" else theme_colors["bearish_light"]

    # Create gradient effect based on magnitude
    values = df[change_col].abs()
    max_val = values.max() if not values.empty and values.max() > 0 else 1
    # Opacity varies from 0.6 to 1.0 based on magnitude
    opacities = 0.6 + (values / max_val) * 0.4

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df[change_col],
            y=df[symbol_col],
            orientation="h",
            marker=dict(
                color=base_color,
                opacity=opacities.tolist(),
                line=dict(color=base_color, width=1),
            ),
            text=[f"{v:+.2f}%" for v in df[change_col]],
            textposition="outside",
            textfont=dict(size=FONT_SIZE_TICK + 1, color=base_color),
            hovertemplate="<b>%{y}</b><br>Change: %{x:+.2f}%<extra></extra>",
        )
    )

    fig.update_layout(
        height=height,
        title=dict(
            text=title,
            font=dict(size=FONT_SIZE_TITLE, color=theme_colors["text"]),
            x=0,
            xanchor="left",
        ),
        margin=dict(l=70, r=80, t=50, b=40),
        xaxis=dict(
            title="",
            gridcolor=theme_colors["grid"],
            showgrid=True,
            zeroline=True,
            zerolinecolor=BORDER_COLOR,
            zerolinewidth=1,
            tickfont=dict(color=theme_colors["text_secondary"]),
            linecolor=BORDER_COLOR,
        ),
        yaxis=dict(
            title="",
            categoryorder=(
                "total ascending" if chart_type == "losers" else "total descending"
            ),
            tickfont=dict(color=theme_colors["text"], size=12),
            linecolor=BORDER_COLOR,
        ),
        showlegend=False,
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK, color=theme_colors["text"]),
        paper_bgcolor=theme_colors["paper"],
        plot_bgcolor=theme_colors["background"],
        bargap=0.3,
    )

    return fig


def make_intraday_chart(
    df: pd.DataFrame,
    title: str,
    ts_col: str = "ts",
    height: int | None = None,
) -> go.Figure:
    """Create an intraday price chart with high/low range and volume.

    Similar to make_candlestick but optimized for intraday data with
    time-based x-axis.

    Args:
        df: DataFrame with ts, open, high, low, close, volume columns.
        title: Chart title.
        ts_col: Name of the timestamp column (default: "ts").
        height: Chart height in pixels.

    Returns:
        Plotly Figure object.
    """
    # Get theme colors
    theme_colors = get_colors()

    required_cols = [ts_col, "open", "high", "low", "close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=theme_colors["text_secondary"]),
        )
        fig.update_layout(
            height=height or MIN_CANDLESTICK_HEIGHT,
            paper_bgcolor=theme_colors["paper"],
            plot_bgcolor=theme_colors["background"],
        )
        return fig

    df = df.sort_values(ts_col).copy()

    # Create 2-row subplot with shared x-axis
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(title, "Volume"),
        row_heights=[PRICE_ROW_HEIGHT_RATIO, VOLUME_ROW_HEIGHT_RATIO],
    )

    # High/low range as shaded area
    fig.add_trace(
        go.Scatter(
            x=df[ts_col],
            y=df["high"],
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df[ts_col],
            y=df["low"],
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor="rgba(47, 129, 247, 0.1)",  # Bloomberg blue tint
            name="High-Low Range",
            hoverinfo="skip",
        ),
        row=1,
        col=1,
    )

    # Close price line
    fig.add_trace(
        go.Scatter(
            x=df[ts_col],
            y=df["close"],
            mode="lines",
            name="Close",
            line=dict(color=theme_colors["accent"], width=2),
            hovertemplate="%{x}<br>Close: %{y:.2f}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Open price as dotted line
    fig.add_trace(
        go.Scatter(
            x=df[ts_col],
            y=df["open"],
            mode="lines",
            name="Open",
            line=dict(color=theme_colors["warning"], width=1, dash="dot"),
            hovertemplate="%{x}<br>Open: %{y:.2f}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Volume bars
    bar_colors = [
        theme_colors["bullish"] if row["close"] >= row["open"] else theme_colors["bearish"]
        for _, row in df.iterrows()
    ]
    fig.add_trace(
        go.Bar(
            x=df[ts_col],
            y=df["volume"],
            name="Volume",
            marker_color=bar_colors,
            opacity=0.7,
            hovertemplate="%{x}<br>Volume: %{y:,.0f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # Calculate price range
    price_min = df["low"].min()
    price_max = df["high"].max()
    price_range = price_max - price_min
    price_padding = max(price_range * 0.15, price_min * 0.02) if price_min > 0 else 1

    # Update layout with Bloomberg styling
    chart_height = height or MIN_CANDLESTICK_HEIGHT
    fig.update_layout(
        height=chart_height,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=FONT_SIZE_TICK, color=theme_colors["text_secondary"]),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=70, t=80, b=50),
        hovermode="x unified",
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK, color=theme_colors["text"]),
        paper_bgcolor=theme_colors["paper"],
        plot_bgcolor=theme_colors["background"],
    )

    fig.update_yaxes(
        range=[price_min - price_padding, price_max + price_padding],
        row=1,
        col=1,
        title_text="Price (PKR)",
        title_font=dict(color=theme_colors["text_secondary"]),
        tickformat=".2f",
        tickfont=dict(color=theme_colors["text_secondary"]),
        gridcolor=theme_colors["grid"],
        showgrid=True,
        side="right",
        linecolor=BORDER_COLOR,
    )
    fig.update_yaxes(
        row=2,
        col=1,
        title_text="Volume",
        title_font=dict(color=theme_colors["text_secondary"]),
        tickfont=dict(color=theme_colors["text_secondary"]),
        gridcolor=theme_colors["grid"],
        showgrid=True,
        side="right",
        linecolor=BORDER_COLOR,
    )
    fig.update_xaxes(
        row=2,
        col=1,
        title_text="Time",
        title_font=dict(color=theme_colors["text_secondary"]),
        tickfont=dict(color=theme_colors["text_secondary"]),
        gridcolor=theme_colors["grid"],
        showgrid=True,
        linecolor=BORDER_COLOR,
    )
    fig.update_xaxes(
        row=1,
        col=1,
        tickfont=dict(color=theme_colors["text_secondary"]),
        gridcolor=theme_colors["grid"],
        showgrid=True,
        linecolor=BORDER_COLOR,
    )

    for annotation in fig.layout.annotations:
        annotation.font.size = FONT_SIZE_TITLE
        annotation.font.color = theme_colors["text"]

    return fig
