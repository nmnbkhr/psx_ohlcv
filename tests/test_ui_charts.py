"""Tests for UI chart helpers."""

import pandas as pd
import plotly.graph_objects as go
import pytest

from psx_ohlcv.ui.charts import (
    COLOR_BEARISH,
    COLOR_BULLISH,
    MIN_CHART_HEIGHT,
    compute_sma,
    make_candlestick,
    make_intraday_chart,
    make_market_breadth_chart,
    make_price_line,
    make_top_movers_chart,
    make_volume_chart,
)


@pytest.fixture
def sample_ohlcv_df():
    """Create sample OHLCV DataFrame for testing."""
    return pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=50, freq="D"),
        "open": [100 + i * 0.5 for i in range(50)],
        "high": [101 + i * 0.5 for i in range(50)],
        "low": [99 + i * 0.5 for i in range(50)],
        "close": [100.5 + i * 0.5 for i in range(50)],
        "volume": [1000000 + i * 10000 for i in range(50)],
    })


@pytest.fixture
def sample_intraday_df():
    """Create sample intraday DataFrame for testing."""
    return pd.DataFrame({
        "ts": pd.date_range("2024-01-15 09:00", periods=100, freq="5min"),
        "open": [100 + i * 0.1 for i in range(100)],
        "high": [100.5 + i * 0.1 for i in range(100)],
        "low": [99.5 + i * 0.1 for i in range(100)],
        "close": [100.2 + i * 0.1 for i in range(100)],
        "volume": [50000 + i * 500 for i in range(100)],
    })


class TestComputeSMA:
    """Tests for SMA computation."""

    def test_compute_sma_20(self, sample_ohlcv_df):
        """Compute 20-period SMA."""
        sma = compute_sma(sample_ohlcv_df, "close", 20)

        assert len(sma) == len(sample_ohlcv_df)
        # SMA should be close to the midpoint of the period
        assert sma.iloc[19] == pytest.approx(
            sample_ohlcv_df["close"].iloc[:20].mean(), rel=0.01
        )

    def test_compute_sma_short_period(self, sample_ohlcv_df):
        """Compute SMA with period shorter than data."""
        sma = compute_sma(sample_ohlcv_df, "close", 5)

        assert len(sma) == len(sample_ohlcv_df)
        # First few values should be partial averages
        assert not pd.isna(sma.iloc[0])

    def test_compute_sma_handles_nan(self):
        """SMA handles NaN values gracefully."""
        df = pd.DataFrame({
            "close": [100, 101, None, 103, 104]
        })
        sma = compute_sma(df, "close", 3)

        assert len(sma) == 5
        # SMA should be computed for valid data points
        # min_periods=1 means we get values even with some NaNs
        assert not pd.isna(sma.iloc[0])  # First value is valid
        assert not pd.isna(sma.iloc[4])  # Last value is valid


class TestMakeCandlestick:
    """Tests for make_candlestick function."""

    def test_basic_candlestick(self, sample_ohlcv_df):
        """Create basic candlestick chart."""
        fig = make_candlestick(sample_ohlcv_df, "Test Chart")

        assert isinstance(fig, go.Figure)
        assert fig.layout.height >= MIN_CHART_HEIGHT

    def test_candlestick_with_sma(self, sample_ohlcv_df):
        """Create candlestick with SMA overlays."""
        fig = make_candlestick(sample_ohlcv_df, "Test Chart", show_sma=True)

        # Should have candlestick + SMA traces + volume
        trace_names = [t.name for t in fig.data]
        assert "OHLC" in trace_names
        assert "Volume" in trace_names
        # SMA traces should be present if data is long enough
        if len(sample_ohlcv_df) >= 20:
            assert "SMA(20)" in trace_names

    def test_candlestick_without_sma(self, sample_ohlcv_df):
        """Create candlestick without SMA overlays."""
        fig = make_candlestick(sample_ohlcv_df, "Test Chart", show_sma=False)

        trace_names = [t.name for t in fig.data]
        assert "OHLC" in trace_names
        assert "SMA(20)" not in trace_names
        assert "SMA(50)" not in trace_names

    def test_candlestick_custom_height(self, sample_ohlcv_df):
        """Create candlestick with custom height."""
        custom_height = 800
        fig = make_candlestick(
            sample_ohlcv_df, "Test Chart", height=custom_height
        )

        assert fig.layout.height == custom_height

    def test_candlestick_empty_df(self):
        """Handle empty DataFrame."""
        empty_df = pd.DataFrame(columns=[
            "date", "open", "high", "low", "close", "volume"
        ])
        fig = make_candlestick(empty_df, "Empty Chart")

        assert isinstance(fig, go.Figure)
        # Should have annotation about no data
        assert len(fig.layout.annotations) > 0

    def test_candlestick_missing_columns(self):
        """Raise error for missing columns."""
        df = pd.DataFrame({"date": [1, 2, 3], "close": [100, 101, 102]})

        with pytest.raises(ValueError) as exc_info:
            make_candlestick(df, "Missing Columns")

        assert "Missing required columns" in str(exc_info.value)

    def test_candlestick_custom_date_col(self, sample_ohlcv_df):
        """Use custom date column name."""
        df = sample_ohlcv_df.rename(columns={"date": "timestamp"})
        fig = make_candlestick(df, "Test Chart", date_col="timestamp")

        assert isinstance(fig, go.Figure)

    def test_candlestick_has_volume_subplot(self, sample_ohlcv_df):
        """Verify volume subplot is present."""
        fig = make_candlestick(sample_ohlcv_df, "Test Chart")

        # Should be 2-row subplot
        assert fig.layout.yaxis2 is not None

    def test_candlestick_price_autoscaling(self, sample_ohlcv_df):
        """Verify price axis has appropriate range."""
        fig = make_candlestick(sample_ohlcv_df, "Test Chart")

        # Y-axis should have a range set
        y_range = fig.layout.yaxis.range
        assert y_range is not None
        assert y_range[0] < sample_ohlcv_df["low"].min()
        assert y_range[1] > sample_ohlcv_df["high"].max()


class TestMakeIntradayChart:
    """Tests for make_intraday_chart function."""

    def test_basic_intraday_chart(self, sample_intraday_df):
        """Create basic intraday chart."""
        fig = make_intraday_chart(sample_intraday_df, "Intraday Test")

        assert isinstance(fig, go.Figure)
        assert fig.layout.height >= MIN_CHART_HEIGHT

    def test_intraday_chart_has_traces(self, sample_intraday_df):
        """Verify intraday chart has expected traces."""
        fig = make_intraday_chart(sample_intraday_df, "Intraday Test")

        trace_names = [t.name for t in fig.data]
        assert "Close" in trace_names
        assert "Open" in trace_names
        assert "Volume" in trace_names

    def test_intraday_chart_empty_df(self):
        """Handle empty DataFrame."""
        empty_df = pd.DataFrame(columns=[
            "ts", "open", "high", "low", "close", "volume"
        ])
        fig = make_intraday_chart(empty_df, "Empty Intraday")

        assert isinstance(fig, go.Figure)

    def test_intraday_chart_missing_columns(self):
        """Raise error for missing columns."""
        df = pd.DataFrame({"ts": [1, 2, 3], "close": [100, 101, 102]})

        with pytest.raises(ValueError) as exc_info:
            make_intraday_chart(df, "Missing Columns")

        assert "Missing required columns" in str(exc_info.value)


class TestMakePriceLine:
    """Tests for make_price_line function."""

    def test_basic_price_line(self, sample_ohlcv_df):
        """Create basic price line chart."""
        fig = make_price_line(sample_ohlcv_df, "Price Trend")

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_price_line_with_area(self, sample_ohlcv_df):
        """Create price line with area fill."""
        fig = make_price_line(
            sample_ohlcv_df, "Price Trend", show_area=True
        )

        # Check fill property
        assert fig.data[0].fill is not None

    def test_price_line_without_area(self, sample_ohlcv_df):
        """Create price line without area fill."""
        fig = make_price_line(
            sample_ohlcv_df, "Price Trend", show_area=False
        )

        assert fig.data[0].fill is None

    def test_price_line_custom_column(self, sample_ohlcv_df):
        """Use custom price column."""
        fig = make_price_line(
            sample_ohlcv_df, "Open Price", price_col="open"
        )

        assert isinstance(fig, go.Figure)

    def test_price_line_empty_df(self):
        """Handle empty DataFrame."""
        empty_df = pd.DataFrame(columns=["date", "close"])
        fig = make_price_line(empty_df, "Empty")

        assert isinstance(fig, go.Figure)


class TestMakeVolumeChart:
    """Tests for make_volume_chart function."""

    def test_basic_volume_chart(self, sample_ohlcv_df):
        """Create basic volume chart."""
        fig = make_volume_chart(sample_ohlcv_df)

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_volume_chart_colored_by_direction(self, sample_ohlcv_df):
        """Volume bars colored by price direction."""
        fig = make_volume_chart(sample_ohlcv_df)

        # Bar colors should be set
        assert fig.data[0].marker.color is not None

    def test_volume_chart_empty_df(self):
        """Handle empty DataFrame."""
        empty_df = pd.DataFrame(columns=["date", "volume"])
        fig = make_volume_chart(empty_df)

        assert isinstance(fig, go.Figure)


class TestMakeMarketBreadthChart:
    """Tests for make_market_breadth_chart function."""

    def test_basic_breadth_chart(self):
        """Create basic market breadth chart."""
        fig = make_market_breadth_chart(
            gainers=150, losers=100, unchanged=50
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_breadth_chart_pie_data(self):
        """Verify pie chart data."""
        fig = make_market_breadth_chart(
            gainers=150, losers=100, unchanged=50
        )

        # Should be a pie chart
        assert isinstance(fig.data[0], go.Pie)
        assert sum(fig.data[0].values) == 300

    def test_breadth_chart_colors(self):
        """Verify color scheme."""
        fig = make_market_breadth_chart(
            gainers=150, losers=100, unchanged=50
        )

        colors = fig.data[0].marker.colors
        assert COLOR_BULLISH in colors
        assert COLOR_BEARISH in colors

    def test_breadth_chart_all_zeros(self):
        """Handle all zeros."""
        fig = make_market_breadth_chart(gainers=0, losers=0, unchanged=0)

        assert isinstance(fig, go.Figure)


class TestMakeTopMoversChart:
    """Tests for make_top_movers_chart function."""

    def test_gainers_chart(self):
        """Create top gainers chart."""
        df = pd.DataFrame({
            "symbol": ["HBL", "OGDC", "MCB"],
            "change_pct": [5.5, 4.2, 3.1],
        })
        fig = make_top_movers_chart(
            df, "Top Gainers", chart_type="gainers"
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_losers_chart(self):
        """Create top losers chart."""
        df = pd.DataFrame({
            "symbol": ["ABC", "XYZ", "DEF"],
            "change_pct": [-5.5, -4.2, -3.1],
        })
        fig = make_top_movers_chart(
            df, "Top Losers", chart_type="losers"
        )

        assert isinstance(fig, go.Figure)

    def test_movers_chart_bar_color(self):
        """Verify bar colors match chart type."""
        df = pd.DataFrame({
            "symbol": ["HBL"],
            "change_pct": [5.0],
        })

        gainers_fig = make_top_movers_chart(
            df, "Gainers", chart_type="gainers"
        )
        losers_fig = make_top_movers_chart(
            df, "Losers", chart_type="losers"
        )

        assert gainers_fig.data[0].marker.color == COLOR_BULLISH
        assert losers_fig.data[0].marker.color == COLOR_BEARISH

    def test_movers_chart_empty_df(self):
        """Handle empty DataFrame."""
        empty_df = pd.DataFrame(columns=["symbol", "change_pct"])
        fig = make_top_movers_chart(empty_df, "Empty")

        assert isinstance(fig, go.Figure)

    def test_movers_chart_horizontal_bars(self):
        """Verify bars are horizontal."""
        df = pd.DataFrame({
            "symbol": ["HBL", "OGDC"],
            "change_pct": [5.0, 4.0],
        })
        fig = make_top_movers_chart(df, "Test")

        assert fig.data[0].orientation == "h"


class TestChartStyling:
    """Tests for chart styling consistency."""

    def test_candlestick_has_grid(self, sample_ohlcv_df):
        """Verify charts have grid lines."""
        fig = make_candlestick(sample_ohlcv_df, "Test")

        assert fig.layout.yaxis.showgrid is True

    def test_chart_has_proper_margins(self, sample_ohlcv_df):
        """Verify charts have proper margins."""
        fig = make_candlestick(sample_ohlcv_df, "Test")

        margin = fig.layout.margin
        assert margin.l >= 10
        assert margin.r >= 30
        assert margin.t >= 30
        assert margin.b >= 30

    def test_chart_has_legend(self, sample_ohlcv_df):
        """Verify charts have legend when appropriate."""
        fig = make_candlestick(sample_ohlcv_df, "Test", show_sma=True)

        assert fig.layout.showlegend is True
