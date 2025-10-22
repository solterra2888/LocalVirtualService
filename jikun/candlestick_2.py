import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from matplotlib.ticker import FuncFormatter
from matplotlib.lines import Line2D

# --- User-Specific Configuration ---
sys.path.append("/Users/kun/GDrive/TProject/utils")
import keychain

# --- Helper Functions (Low-Level) ---

def volume_formatter(x, pos):
    """Formats volume numbers into human-readable strings with K/M suffixes."""
    if x >= 1_000_000:
        return f'{x/1_000_000:.1f}M'
    elif x >= 1_000:
        return f'{x/1_000:.1f}K'
    else:
        return f'{int(x)}'

def fetch_data_example(symbol, start_date, end_date, api_key, base_url="https://eodhd.com/api"):
    """Fetch EOD data from EODHD API and return standardized DataFrame."""
    url = f"{base_url}/eod/{symbol}"
    params = { "from": start_date, "to": end_date, "period": "d", "api_token": api_key, "fmt": "json" }
    print(f"Fetching data for {symbol} from {start_date} to {end_date}...")
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

    if not isinstance(data, list) or not data:
        print("No data returned from API.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df = df.sort_values("date")
    df['volume'] = pd.to_numeric(df['volume'])
    return df[["date", "open", "high", "low", "close", "volume"]]


# --- Plotting Function (Low-Level) ---

def plot_candlestick(
    df,
    title="Price Action",
    ema_periods=[20, 50, 200],
    figsize=(12, 7),
    output_path=None,
    show_volume=True
):
    """
    Renders and saves a professional candlestick chart.
    This function is now a dedicated plotting utility.
    """
    # (The code for this function remains exactly the same as before)
    df = df.copy()
    if "date" in df.columns: df = df.set_index("date")
    if not pd.api.types.is_datetime64_any_dtype(df.index): df.index = pd.to_datetime(df.index)
    ema_plots = []
    ema_colors = ["#BBBBBB", "#AAAAAA", "#999999"]
    for i, period in enumerate(ema_periods):
        col = f"EMA_{period}"
        df[col] = df["close"].ewm(span=period, adjust=False).mean()
        color = ema_colors[i] if i < len(ema_colors) else "#888888"
        ema_plots.append(mpf.make_addplot(df[col], color=color, width=1.2))
    UP_CANDLE_EDGE, DOWN_RED = "#000000", "#E74C3C"; UP_VOL_GRAY, DOWN_VOL_RED = "#95A5A6", "#F1948A"
    mc = mpf.make_marketcolors(up="#FFFFFF", down=DOWN_RED, edge={"up": UP_CANDLE_EDGE, "down": DOWN_RED}, wick={"up": UP_CANDLE_EDGE, "down": DOWN_RED}, volume={"up": UP_VOL_GRAY, "down": DOWN_VOL_RED})
    s = mpf.make_mpf_style(marketcolors=mc, gridcolor="#E5E5E5", gridstyle="-", gridaxis="horizontal", facecolor="#F8F9FA", figcolor="#F8F9FA", edgecolor="#CCCCCC", y_on_right=True, rc={"font.size": 8, "axes.labelsize": 9, "axes.titlesize": 11, "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 8, "lines.linewidth": 0.8})
    plot_kwargs = {"type": "candle", "style": s, "title": "", "ylabel": "Price", "figsize": figsize, "returnfig": True, "show_nontrading": False, "datetime_format": "%b %d", "xrotation": 0, "scale_padding": {"left": 0.05, "right": 0.35, "top": 0.3, "bottom": 0.3}, "update_width_config": dict(candle_linewidth=0.6, candle_width=0.6, volume_linewidth=0, volume_width=0.6), "addplot": ema_plots if ema_plots else None}
    if show_volume: plot_kwargs.update({"volume": True, "volume_panel": 1, "panel_ratios": (4, 1), "volume_exponent": 0})
    else: plot_kwargs["volume"] = False
    fig, axes = mpf.plot(df, **plot_kwargs)
    for ax in axes: ax.margins(x=0)
    axes[0].set_title(title, loc="left", fontsize=10, pad=8, color="#2C3E50")
    if ema_periods:
        legend_handles = [Line2D([0], [0], color=c, linewidth=2) for c in ema_colors[:len(ema_periods)]]; legend_labels = [f"EMA {p}" for p in ema_periods]
        axes[0].legend(handles=legend_handles, labels=legend_labels, loc="upper left", fontsize=8, framealpha=0.9, edgecolor="#CCCCCC")
    for i, ax in enumerate(axes):
        ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(True); ax.spines["right"].set_color("#DDDDDD"); ax.spines["right"].set_linewidth(1.0); ax.spines["left"].set_visible(False)
        locator = plt.MaxNLocator(nbins=6 if i == 0 else 3, prune="both"); ax.yaxis.set_major_locator(locator)
        if i == 0: ax.spines["bottom"].set_visible(False)
        else: ax.spines["top"].set_visible(False)
    if show_volume and len(axes) > 1:
        axes[1].yaxis.set_major_formatter(FuncFormatter(volume_formatter)); axes[1].set_ylabel("")
        if hasattr(axes[1], "patches") and axes[1].patches:
            def hex_to_rgba(hex_color, alpha=1.0): h = hex_color.lstrip("#"); rgb = tuple(int(h[i:i+2], 16) / 255.0 for i in (0, 2, 4)); return (*rgb, alpha)
            up_rgba, down_rgba = hex_to_rgba(UP_VOL_GRAY, 0.95), hex_to_rgba(DOWN_VOL_RED, 0.40); is_up = (df["close"] >= df["open"]).values; n = min(len(is_up), len(axes[1].patches))
            for idx in range(n): patch = axes[1].patches[idx]; patch.set_edgecolor("none"); patch.set_linewidth(0); patch.set_facecolor(up_rgba if is_up[idx] else down_rgba)
    if output_path:
        print(f"Saving chart to: {output_path}"); fig.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="#F8F9FA")
    fig.tight_layout(rect=[0, 0, 1, 0.95]); plt.show(); return fig, axes


# --- NEW ORCHESTRATOR FUNCTION (High-Level) ---

def create_stock_chart(
    ticker,
    start_date=None,
    end_date=None,
    days=60,
    output_dir="~/Data/Charts/",
    filename=None,
    api_key=None,
    show_volume=True, # <-- PARAMETER ADDED HERE
):
    """
    Orchestrates fetching data and plotting a stock chart based on flexible date parameters.
    """
    if not api_key:
        raise ValueError("API key must be provided.")

    today = datetime.utcnow().date()
    
    # --- 1. Determine Date Range for Fetching ---
    fetch_start = None
    fetch_end = None
    trim_to_days = False
    
    if start_date and end_date:
        fetch_start = start_date
        fetch_end = end_date
    elif start_date:
        fetch_start = start_date
        end_dt = pd.to_datetime(start_date) + timedelta(days=int(days * 1.7))
        fetch_end = end_dt.strftime('%Y-%m-%d')
        trim_to_days = True
    else:
        fetch_end = today.strftime('%Y-%m-%d')
        start_dt = today - timedelta(days=int(days * 1.7))
        fetch_start = start_dt.strftime('%Y-%m-%d')
        trim_to_days = True

    # --- 2. Fetch Data ---
    df = fetch_data_example(ticker, fetch_start, fetch_end, api_key)

    if df.empty:
        print(f"Could not create chart for {ticker} due to data fetching failure.")
        return

    # --- 3. Trim DataFrame if Necessary ---
    if trim_to_days:
        if start_date:
            df = df.head(days)
        else:
            df = df.tail(days)
    
    if df.empty:
        print(f"Not enough data for {ticker} in the specified range.")
        return

    # --- 4. Determine Output Path and Filename ---
    min_date = df['date'].min().strftime('%Y%m%d')
    max_date = df['date'].max().strftime('%Y%m%d')
    
    expanded_dir = os.path.expanduser(output_dir)
    os.makedirs(expanded_dir, exist_ok=True)
    
    if not filename:
        ticker_clean = ticker.split('.')[0].upper()
        filename = f"{ticker_clean}_{min_date}-{max_date}_daily.png"

    output_path = os.path.join(expanded_dir, filename)

    # --- 5. Generate Title and Plot ---
    chart_title = f"{ticker} - Daily Chart ({df['date'].min().strftime('%b %d, %Y')} to {df['date'].max().strftime('%b %d, %Y')})"

    plot_candlestick(
        df, 
        title=chart_title, 
        output_path=output_path, 
        show_volume=show_volume # <-- PARAMETER PASSED HERE
    )

    return output_path


# --- Main Execution Block (Example Usage) ---
if __name__ == "__main__":
    API_KEY = keychain.get_password("PY_INFO", "EODHD_API_KEY")

    print("\n--- Example 1: Default (last 60 trading days) ---")
    create_stock_chart(ticker="AAPL.US", api_key=API_KEY)

    print("\n--- Example 2: Specific start date and number of days ---")
    create_stock_chart(ticker="MSFT.US", start_date="2025-06-02", days=90, api_key=API_KEY)

    print("\n--- Example 3: Specific start and end date ---")
    create_stock_chart(ticker="GOOGL.US", start_date="2025-05-01", end_date="2025-08-29", api_key=API_KEY)

    print("\n--- Example 4: Custom filename and output directory ---")
    create_stock_chart(
        ticker="TSLA.US",
        days=120,
        filename="tesla_q3_2025_analysis.png",
        output_dir="~/Data/Charts/Special_Reports/",
        api_key=API_KEY,
    )

    print("\n--- Example 5: Hiding the volume panel ---")
    create_stock_chart(
        ticker="NVDA.US",
        days=90,
        api_key=API_KEY,
        show_volume=False,
    )