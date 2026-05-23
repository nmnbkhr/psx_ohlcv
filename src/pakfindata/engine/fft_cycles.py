"""FFT / Macro Cycles Engine — spectral analysis for cycle detection.

Implements:
  - Hanning-windowed FFT on close prices
  - Power spectrum with dominant cycle identification
  - Low-pass filtered IFFT for zero-lag trendline reconstruction
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.fft import fft, ifft


# ═════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class FFTResult:
    """Result of an FFT cycle analysis."""
    spectrum: pd.DataFrame       # period, amplitude, power columns
    dominant_cycles: list[dict]  # top-N cycles [{period, amplitude, power}, ...]
    ifft_signal: np.ndarray      # reconstructed zero-lag trendline (same length as input)
    prices: np.ndarray           # original close prices (for overlay)
    dates: np.ndarray            # datetime index (for plotting)


# ═════════════════════════════════════════════════════════════════════════════
# FFT COMPUTATION
# ═════════════════════════════════════════════════════════════════════════════

def compute_fft_cycles(
    df: pd.DataFrame,
    top_n: int = 5,
    low_pass_periods: int = 5,
) -> FFTResult:
    """End-to-end FFT cycle analysis on price data.

    Parameters
    ----------
    df : DataFrame with 'close' and 'datetime' columns.
    top_n : Number of dominant cycles to identify from power spectrum.
    low_pass_periods : Keep only the top-N strongest frequencies for IFFT
                       reconstruction (zero-lag trendline).

    Returns
    -------
    FFTResult with spectrum DataFrame, dominant cycles, and IFFT signal.
    """
    prices = df["close"].values.astype(float)
    dates = df["datetime"].values if "datetime" in df.columns else np.arange(len(prices))
    n = len(prices)

    # Detrend: subtract linear trend to isolate cyclical component
    trend = np.linspace(prices[0], prices[-1], n)
    detrended = prices - trend

    # Apply Hanning window to reduce spectral leakage
    window = np.hanning(n)
    windowed = detrended * window

    # FFT
    fft_vals = fft(windowed)

    # Power spectrum (one-sided: only positive frequencies)
    freqs = np.fft.fftfreq(n, d=1.0)  # d=1 day (or 1 bar)
    half = n // 2

    amplitudes = (2.0 / n) * np.abs(fft_vals[:half])
    powers = amplitudes ** 2

    # Convert frequencies to periods (skip DC component at index 0)
    periods = np.zeros(half)
    periods[1:] = 1.0 / np.abs(freqs[1:half])
    amplitudes[0] = 0  # zero out DC
    powers[0] = 0

    # Build spectrum DataFrame
    spectrum = pd.DataFrame({
        "period": periods,
        "amplitude": amplitudes,
        "power": powers,
    }).iloc[1:]  # drop DC row

    # Filter out unreasonably long periods (> half the data length)
    spectrum = spectrum[spectrum["period"] <= n / 2].copy()
    spectrum = spectrum.sort_values("power", ascending=False).reset_index(drop=True)

    # Top-N dominant cycles
    top = spectrum.head(top_n)
    dominant_cycles = top.to_dict("records")

    # ── IFFT Reconstruction (zero-lag trendline) ──────────────────────────
    # Keep only the strongest frequencies for a smooth trendline
    fft_filtered = np.zeros_like(fft_vals)

    # Find indices of top frequencies by power
    amp_full = np.abs(fft_vals)
    amp_full[0] = 0  # exclude DC
    top_indices = np.argsort(amp_full)[-low_pass_periods:]

    for idx in top_indices:
        fft_filtered[idx] = fft_vals[idx]
        # Mirror for negative frequencies (conjugate symmetry)
        if idx != 0 and idx != n // 2:
            fft_filtered[n - idx] = fft_vals[n - idx]

    # Inverse FFT to get the zero-lag trendline
    ifft_signal = np.real(ifft(fft_filtered))

    # Un-window: divide by Hanning to recover amplitude (clip to avoid div/0)
    window_safe = np.clip(window, 0.1, 1.0)
    ifft_signal = ifft_signal / window_safe

    # Add back the linear trend
    ifft_signal = ifft_signal + trend

    return FFTResult(
        spectrum=spectrum,
        dominant_cycles=dominant_cycles,
        ifft_signal=ifft_signal,
        prices=prices,
        dates=dates,
    )
