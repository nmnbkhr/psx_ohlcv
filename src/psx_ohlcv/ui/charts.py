"""Chart helpers for PSX OHLCV Streamlit UI.

Provides reusable Plotly chart components for consistent visualization.
"""

from typing import Literal

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Chart constants
MIN_CHART_HEIGHT = 520
MIN_CANDLESTICK_HEIGHT = 650  # Higher minimum for candlestick pages
VOLUME_ROW_HEIGHT_RATIO = 0.25
PRICE_ROW_HEIGHT_RATIO = 0.75

# Color scheme - Professional trading terminal palette
COLOR_BULLISH = "#00C853"  # Bright green for price increases
COLOR_BEARISH = "#FF1744"  # Bright red for price decreases
COLOR_BULLISH_LIGHT = "rgba(0, 200, 83, 0.15)"  # Light green for fills
COLOR_BEARISH_LIGHT = "rgba(255, 23, 68, 0.15)"  # Light red for fills
COLOR_SMA_20 = "#FF9800"   # Orange for 20-period SMA
COLOR_SMA_50 = "#9C27B0"   # Purple for 50-period SMA
COLOR_VOLUME = "#2196F3"   # Blue for volume bars
COLOR_GRID = "rgba(128, 128, 128, 0.15)"
COLOR_NEUTRAL = "#78909C"  # Gray for unchanged

# Font settings
FONT_FAMILY = "Arial, sans-serif"
FONT_SIZE_TITLE = 14
FONT_SIZE_AXIS = 12
FONT_SIZE_TICK = 10


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

    # Row 1: Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df[date_col],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="OHLC",
            increasing_line_color=COLOR_BULLISH,
            decreasing_line_color=COLOR_BEARISH,
            increasing_fillcolor=COLOR_BULLISH,
            decreasing_fillcolor=COLOR_BEARISH,
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
                line=dict(color=COLOR_SMA_20, width=1.5),
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
                line=dict(color=COLOR_SMA_50, width=1.5),
                hovertemplate="%{x}<br>SMA(50): %{y:.2f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    # Row 2: Volume bars colored by price direction
    colors = [
        COLOR_BULLISH if row["close"] >= row["open"] else COLOR_BEARISH
        for _, row in df.iterrows()
    ]
    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df["volume"],
            name="Volume",
            marker_color=colors,
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

    # Update layout
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
            font=dict(size=FONT_SIZE_TICK),
        ),
        xaxis_rangeslider_visible=False,
        margin=dict(l=70, r=50, t=60, b=50),
        hovermode="x unified",
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK),
    )

    # Update y-axes
    fig.update_yaxes(
        range=[price_min - price_padding, price_max + price_padding],
        row=1,
        col=1,
        title_text="Price (PKR)",
        title_font=dict(size=FONT_SIZE_AXIS),
        tickformat=".2f",
        tickfont=dict(size=FONT_SIZE_TICK),
        gridcolor=COLOR_GRID,
        showgrid=True,
    )
    fig.update_yaxes(
        row=2,
        col=1,
        title_text="Volume",
        title_font=dict(size=FONT_SIZE_AXIS),
        tickfont=dict(size=FONT_SIZE_TICK),
        gridcolor=COLOR_GRID,
        showgrid=True,
    )

    # Update x-axes
    fig.update_xaxes(
        row=2,
        col=1,
        title_text="Date",
        title_font=dict(size=FONT_SIZE_AXIS),
        tickfont=dict(size=FONT_SIZE_TICK),
        gridcolor=COLOR_GRID,
        showgrid=True,
    )
    fig.update_xaxes(
        row=1,
        col=1,
        tickfont=dict(size=FONT_SIZE_TICK),
        gridcolor=COLOR_GRID,
        showgrid=True,
    )

    # Update subplot title font
    for annotation in fig.layout.annotations:
        annotation.font.size = FONT_SIZE_TITLE

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

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[price_col],
            mode="lines",
            name=title,
            line=dict(color="#1f77b4", width=2),
            fill="tozeroy" if show_area else None,
            fillcolor="rgba(31, 119, 180, 0.1)" if show_area else None,
            hovertemplate="%{x}<br>%{y:.2f}<extra></extra>",
        )
    )

    fig.update_layout(
        height=height,
        title=dict(text=title, font=dict(size=FONT_SIZE_TITLE)),
        margin=dict(l=70, r=50, t=50, b=50),
        yaxis=dict(
            range=[price_min - y_padding, price_max + y_padding],
            title="Price (PKR)",
            tickformat=".2f",
            gridcolor=COLOR_GRID,
            showgrid=True,
        ),
        xaxis=dict(title="Date", gridcolor=COLOR_GRID, showgrid=True),
        showlegend=False,
        hovermode="x",
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK),
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
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No volume data",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14, color="#888"),
        )
        fig.update_layout(
            height=height,
            title=dict(text="Volume", font=dict(size=FONT_SIZE_TITLE, color="#E0E0E0")),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        return fig

    df = df.sort_values(date_col).copy()

    # Color bars by price direction if open/close available
    if "open" in df.columns and "close" in df.columns:
        colors = [
            COLOR_BULLISH if row["close"] >= row["open"] else COLOR_BEARISH
            for _, row in df.iterrows()
        ]
    else:
        colors = COLOR_VOLUME

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df["volume"],
            name="Volume",
            marker_color=colors,
            opacity=0.85,
            hovertemplate="<b>%{x}</b><br>Volume: %{y:,.0f}<extra></extra>",
        )
    )

    fig.update_layout(
        height=height,
        title=dict(
            text="Volume",
            font=dict(size=FONT_SIZE_TITLE, color="#E0E0E0"),
            x=0.5,
            xanchor="center",
        ),
        margin=dict(l=70, r=50, t=50, b=50),
        yaxis=dict(
            title="",
            gridcolor=COLOR_GRID,
            showgrid=True,
            tickfont=dict(color="#888"),
        ),
        xaxis=dict(
            title="",
            gridcolor=COLOR_GRID,
            showgrid=True,
            tickfont=dict(color="#888"),
        ),
        showlegend=False,
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
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
            font=dict(size=14, color="#888"),
        )
        fig.update_layout(height=height)
        return fig

    labels = ["Gainers", "Losers", "Unchanged"]
    values = [gainers, losers, unchanged]
    colors = [COLOR_BULLISH, COLOR_BEARISH, COLOR_NEUTRAL]

    # Calculate net sentiment
    net = gainers - losers
    sentiment = "Bullish" if net > 0 else "Bearish" if net < 0 else "Neutral"
    sentiment_color = COLOR_BULLISH if net > 0 else COLOR_BEARISH if net < 0 else COLOR_NEUTRAL

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.5,  # Larger hole for center annotation
                marker=dict(
                    colors=colors,
                    line=dict(color='rgba(255,255,255,0.1)', width=2)
                ),
                textinfo="value",
                textfont=dict(size=FONT_SIZE_TICK + 2, color="white"),
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
            font=dict(size=FONT_SIZE_TITLE, color="#E0E0E0"),
            x=0.5,
            xanchor="center",
        ),
        margin=dict(l=20, r=20, t=50, b=40),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5,
            font=dict(size=11),
        ),
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
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
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No movers data",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14, color="#888"),
        )
        fig.update_layout(
            height=height,
            title=dict(text=title, font=dict(size=FONT_SIZE_TITLE, color="#E0E0E0")),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        return fig

    base_color = COLOR_BULLISH if chart_type == "gainers" else COLOR_BEARISH
    light_color = COLOR_BULLISH_LIGHT if chart_type == "gainers" else COLOR_BEARISH_LIGHT

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
            font=dict(size=FONT_SIZE_TITLE, color="#E0E0E0"),
            x=0.5,
            xanchor="center",
        ),
        margin=dict(l=70, r=80, t=50, b=40),
        xaxis=dict(
            title="",
            gridcolor=COLOR_GRID,
            showgrid=True,
            zeroline=True,
            zerolinecolor="rgba(255,255,255,0.2)",
            zerolinewidth=1,
            tickfont=dict(color="#888"),
        ),
        yaxis=dict(
            title="",
            categoryorder=(
                "total ascending" if chart_type == "losers" else "total descending"
            ),
            tickfont=dict(color="#E0E0E0", size=12),
        ),
        showlegend=False,
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
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
        )
        fig.update_layout(height=height or MIN_CANDLESTICK_HEIGHT)
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
            fillcolor="rgba(100, 100, 100, 0.2)",
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
            line=dict(color="#1f77b4", width=2),
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
            line=dict(color="#ff7f0e", width=1, dash="dot"),
            hovertemplate="%{x}<br>Open: %{y:.2f}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Volume bars
    colors = [
        COLOR_BULLISH if row["close"] >= row["open"] else COLOR_BEARISH
        for _, row in df.iterrows()
    ]
    fig.add_trace(
        go.Bar(
            x=df[ts_col],
            y=df["volume"],
            name="Volume",
            marker_color=colors,
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

    # Update layout
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
            font=dict(size=FONT_SIZE_TICK),
        ),
        xaxis_rangeslider_visible=False,
        margin=dict(l=70, r=50, t=80, b=50),
        hovermode="x unified",
        font=dict(family=FONT_FAMILY, size=FONT_SIZE_TICK),
    )

    fig.update_yaxes(
        range=[price_min - price_padding, price_max + price_padding],
        row=1,
        col=1,
        title_text="Price (PKR)",
        tickformat=".2f",
        gridcolor=COLOR_GRID,
        showgrid=True,
    )
    fig.update_yaxes(
        row=2,
        col=1,
        title_text="Volume",
        gridcolor=COLOR_GRID,
        showgrid=True,
    )
    fig.update_xaxes(
        row=2,
        col=1,
        title_text="Time",
        gridcolor=COLOR_GRID,
        showgrid=True,
    )
    fig.update_xaxes(
        row=1,
        col=1,
        gridcolor=COLOR_GRID,
        showgrid=True,
    )

    for annotation in fig.layout.annotations:
        annotation.font.size = FONT_SIZE_TITLE

    return fig
