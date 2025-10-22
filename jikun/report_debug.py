import os
import time
import requests
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from matplotlib.ticker import FuncFormatter
from matplotlib.lines import Line2D
import sys
import markdown2
import investpy
import pytz

# WeasyPrint disabled due to system dependency issues
WEASYPRINT_AVAILABLE = False
HTML = None
print("PDF generation is disabled. WeasyPrint requires system-level dependencies.")

# Load environment variables from .env file
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Get EODHD API key from environment variable
EODHD_API_KEY = os.getenv('EODHD_API_TOKEN')
if not EODHD_API_KEY:
    print("Warning: EODHD_API_TOKEN environment variable not set.")
    print("Please set the EODHD_API_TOKEN environment variable to use this script.")
    EODHD_API_KEY = None

# Configuration
BASE_URL = "https://eodhd.com/api"
TODAY = datetime.now(pytz.UTC).date()
START_DATE = (TODAY - relativedelta(months=3)).strftime("%Y-%m-%d")
END_DATE = TODAY.strftime("%Y-%m-%d")
NEXT_WEEK_START = (TODAY + timedelta(days=(7 - TODAY.weekday()))).strftime("%Y-%m-%d")
NEXT_WEEK_END = (TODAY + timedelta(days=(13 - TODAY.weekday()))).strftime("%Y-%m-%d")

INDEX_MAP = {
    "S&P 500": "SPY.US", "Nasdaq 100": "QQQ.US", "Dow Jones": "DIA.US",
    "STOXX 50": "FEZ.US", "DAX (Germany)": "EWG.US", "CAC 40 (France)": "EWQ.US",
    "Nikkei 225": "EWJ.US", "Hang Seng": "2800.HK",
}
FOREX_MAP = {
    "USD/CNH": "USDCNH.FOREX", "EUR/USD": "EURUSD.FOREX",
    "GBP/USD": "GBPUSD.FOREX", "USD/JPY": "USDJPY.FOREX"
}
COMMODITY_MAP = {
    "Gold": "XAUUSD.FOREX", "Silver": "XAGUSD.FOREX", "Copper": "HG.US",
    "Brent Oil (BNO ETF)": "BNO.US", "Natural Gas": "NG.US"
}
BOND_YIELD_MAP = {
    "3-Month": "US3M.INDX", "1-Year": "US1Y.INDX",
    "5-Year": "US5Y.INDX", "10-Year": "US10Y.INDX"
}
SECTOR_MAP = {
    "Technology": "XLK.US",
    "Financials": "XLF.US", 
    "Health Care": "XLV.US",
    "Consumer Discretionary": "XLY.US",
    "Industrials": "XLI.US",
    "Consumer Staples": "XLP.US",
    "Energy": "XLE.US",
    "Utilities": "XLU.US",
    "Real Estate": "XLRE.US",
    "Materials": "XLB.US",
    "Communication Services": "XLC.US"
}
AI_RELATED_MAP = {
    "Compute Layer (Chips & Accelerators)": {
        "US": ["NVDA.US", "AMD.US", "ANET.US"],
        "China": ["688256.SHG", "000977.SHE", "603019.SHG", "300308.SHE"]
    },
    "Cloud & Platform Layer": {
        "US": ["MSFT.US", "AMZN.US", "GOOGL.US"],
        "China": ["9988.HK", "0700.HK", "9888.HK"]
    },
    "Application Layer (AI-native)": {
        "US": ["PLTR.US"],
        "China": ["002230.SHE", "0020.HK", "6682.HK"]
    },
    "Hardware & Devices": {
        "US": ["SMCI.US", "AAPL.US"],
        "China": ["000977.SHE", "1810.HK"]
    },
    "Data Center & Infrastructure": {
        "US": ["EQIX.US", "DLR.US"],
        "China": ["9698.HK", "300383.SHE"]
    },
    "Autonomy & Robotics": {
        "US": ["TSLA.US", "MBLY.US", "LAZR.US"],
        "China": ["9880.HK", "002008.SHE"]
    }
}

REGIONS = {
    "US": ["S&P 500", "Nasdaq 100", "Dow Jones"],
    "Europe": ["STOXX 50", "DAX (Germany)", "CAC 40 (France)"],
    "Asia": ["Nikkei 225", "Hang Seng"]
}

# Styling
def set_broker_style():
    plt.rcParams.update({
        "figure.facecolor": "white", "axes.facecolor": "#FAFAFA", 
        "axes.edgecolor": "#CCCCCC", "axes.linewidth": 1.2, 
        "axes.grid": True, "axes.grid.axis": "y", "grid.color": "#E5E5E5", 
        "grid.linestyle": "-", "grid.linewidth": 0.8, "grid.alpha": 0.6, 
        "axes.spines.top": False, "axes.spines.right": False, 
        "axes.spines.left": True, "axes.spines.bottom": True, 
        "axes.labelsize": 11, "axes.titlesize": 13, "axes.titleweight": "bold", 
        "axes.titlepad": 15, "xtick.labelsize": 9, "ytick.labelsize": 9, 
        "xtick.color": "#666666", "ytick.color": "#666666", 
        "legend.fontsize": 9, "legend.frameon": True, "legend.framealpha": 0.95, 
        "legend.edgecolor": "#CCCCCC", "legend.fancybox": False, 
        "font.family": ["Arial", "Helvetica", "DejaVu Sans"], 
        "font.size": 10, "savefig.dpi": 300, "savefig.bbox": "tight", 
        "figure.dpi": 100
    })

# Data functions
def fetch_eod(symbol, start_date, end_date, api_key, data_type="eod"):
    url = f"{BASE_URL}/{data_type}/{symbol}"
    params = {"from": start_date, "to": end_date, "period": "d", "api_token": api_key, "fmt": "json"}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"  Warning: {symbol} not found (404), skipping...")
            return pd.DataFrame()
        else:
            raise
    except Exception as e:
        print(f"  Error fetching {symbol}: {e}")
        return pd.DataFrame()
    
    if not isinstance(data, list) or not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df = df.sort_values("date")
    if "open" not in df.columns:
        return df[["date", "close"]]
    return df[["date", "open", "high", "low", "close", "volume"]]

def fetch_economic_calendar(start_date, end_date, api_key):
    """Fetch US high/medium impact economic calendar events using investpy"""
    try:
        from_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d/%m/%Y')
        to_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d/%m/%Y')
        
        data = investpy.economic_calendar(from_date=from_date, to_date=to_date)
        df = data.query('currency=="USD" and importance in ["high", "medium"]').reset_index(drop=True)
        
        if df.empty:
            return pd.DataFrame()
        
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        df['time'] = pd.Series(pd.to_datetime(df['time'].astype(str), format='%H:%M')).dt.time
        
        utc_zone = pytz.utc
        est_zone = pytz.timezone('America/New_York')
        today_date = pd.Timestamp.now().date()
        
        df['time'] = df['time'].apply(lambda t: pd.Timestamp.combine(today_date, t).replace(tzinfo=utc_zone))
        df['date'] = df['date'] + pd.to_timedelta(df['time'].dt.hour, unit='h') + pd.to_timedelta(df['time'].dt.minute, unit='m')
        
        if not df.empty and est_zone.dst(df['date'].iloc[0]) != timedelta(0):
            df['date'] = df['date'] - timedelta(hours=1)
        
        return df.sort_values('date')
    except Exception as e:
        print(f"  Error fetching economic calendar: {e}")
        return pd.DataFrame()

def fetch_sp500_constituents(api_key):
    """Fetch S&P 500 constituent list from GSPC.INDX fundamentals"""
    url = f"{BASE_URL}/fundamentals/GSPC.INDX"
    params = {"api_token": api_key, "fmt": "json"}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        components = data.get('Components', {})
        if isinstance(components, dict):
            stocks = [item['Code'].upper() for item in components.values()
                      if isinstance(item, dict) and 'Code' in item]
            return set(stocks)
        return set()
    except Exception as e:
        print(f"  Error fetching S&P 500 components: {e}")
        return {
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'UNH', 'JNJ',
            'V', 'XOM', 'WMT', 'JPM', 'MA', 'PG', 'CVX', 'HD', 'MRK', 'ABBV',
            'KO', 'PEP', 'AVGO', 'COST', 'TMO', 'MCD', 'CSCO', 'ACN', 'ABT', 'LLY',
            'DHR', 'NKE', 'ADBE', 'VZ', 'TXN', 'NEE', 'CRM', 'ORCL', 'INTC', 'AMD',
            'CMCSA', 'DIS', 'WFC', 'PM', 'BAC', 'BMY', 'T', 'NFLX', 'UPS', 'QCOM',
            'HON', 'RTX', 'UNP', 'INTU', 'LOW', 'BA', 'SPGI', 'CAT', 'IBM', 'GS',
            'DE', 'AMGN', 'SBUX', 'AXP', 'BLK', 'MDLZ', 'MMM', 'GILD', 'LMT', 'BKNG',
            'ISRG', 'ADI', 'C', 'PLD', 'NOW', 'AMT', 'TJX', 'SYK', 'CVS', 'VRTX',
            'MO', 'ADP', 'ZTS', 'REGN', 'TMUS', 'CB', 'SO', 'CI', 'PGR', 'DUK',
            'MMC', 'BDX', 'CL', 'SHW', 'ITW', 'BSX', 'EOG', 'ETN', 'APD', 'CME'
        }

def fetch_earnings_calendar(start_date, end_date, api_key):
    """Fetch earnings calendar for S&P 500 companies only"""
    url = f"{BASE_URL}/calendar/earnings"
    params = {"from": start_date, "to": end_date, "api_token": api_key, "fmt": "json"}
    
    sp500_tickers = fetch_sp500_constituents(api_key)
    print(f"  S&P 500 tickers loaded: {len(sp500_tickers)} companies")
    
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data or not isinstance(data, dict):
            print("  No earnings data returned")
            return pd.DataFrame()
        
        earnings_list = []
        for date, companies in data.items():
            if isinstance(companies, list):
                for company in companies:
                    company['date'] = date
                    earnings_list.append(company)
        
        if not earnings_list:
            print("  No earnings found in date range")
            return pd.DataFrame()
        
        df = pd.DataFrame(earnings_list)
        print(f"  Total earnings before filtering: {len(df)}")
        

        # Identify ticker field
        ticker_field = None
        for tf in ['code', 'symbol', 'ticker']:
            if tf in df.columns:
                ticker_field = tf
                break

        if ticker_field:
            before_filter = len(df)
            # Normalize ticker format (strip .US, upper case)
            df['__normalized'] = df[ticker_field].str.upper().str.replace(r'\.US$', '', regex=True)
            df = df[df['__normalized'].isin(sp500_tickers)]
            print(f"  After S&P 500 filter: {len(df)} (removed {before_filter - len(df)})")
            df.drop(columns='__normalized', inplace=True)

        # Optional exchange filter – disable if not needed
        # if 'exchange' in df.columns:
        #     us_exchanges = ['US', 'NYSE', 'NASDAQ', 'AMEX']
        #     before_exchange = len(df)
        #     df = df[df['exchange'].isin(us_exchanges)]
        #     print(f"  After exchange filter: {len(df)} (removed {before_exchange - len(df)})")

        return df.sort_values('date') if 'date' in df.columns and not df.empty else df
    except Exception as e:
        print(f"  Error fetching earnings calendar: {e}")
        return pd.DataFrame()


def fetch_ipo_calendar(start_date, end_date, api_key):
    """Fetch upcoming IPOs for US and HK exchanges"""
    url = f"{BASE_URL}/calendar/ipos"
    params = {
        "from": start_date,
        "to": end_date,
        "api_token": api_key,
        "fmt": "json"
    }
    
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        ipos = data.get('ipos', [])
        
        if not ipos:
            print("  No IPOs found in date range")
            return pd.DataFrame()
        
        df = pd.DataFrame(ipos)
        print(f"  Total IPOs: {len(df)}")
        
        # Rename start_date to date for consistency
        if 'start_date' in df.columns:
            df['date'] = df['start_date']
        
        # Filter for US and HK exchanges
        if 'exchange' in df.columns:
            before_filter = len(df)
            df = df[df['exchange'].str.contains('NYSE|NASDAQ|AMEX|US|HKSE|Hong Kong', case=False, na=False)]
            print(f"  After US/HK filter: {len(df)} (removed {before_filter - len(df)})")
        
        return df.sort_values('date') if 'date' in df.columns and not df.empty else df
    except Exception as e:
        print(f"  Error fetching IPO calendar: {e}")
        return pd.DataFrame()


def get_panel(ticker_map, start_date, end_date, api_key):
    panel = None
    for label, symbol in ticker_map.items():
        df = fetch_eod(symbol, start_date, end_date, api_key, data_type="eod")
        if df.empty:
            continue
        df = df.rename(columns={"close": label})
        if panel is None:
            panel = df[["date", label]]
        else:
            panel = panel.merge(df[["date", label]], on="date", how="outer")
        time.sleep(0.2)
    if panel is None:
        raise RuntimeError("No data fetched")
    return panel.sort_values("date").ffill().bfill()

def compute_wow(panel):
    df = panel.set_index("date").asfreq("B").ffill()
    wk = df.resample("W-FRI").last().dropna(how="all")
    if len(wk) < 2:
        return pd.DataFrame(), {}
    w0, w1 = wk.index[-2], wk.index[-1]
    valid_cols = [c for c in wk.columns if pd.notna(wk.loc[w0, c]) and pd.notna(wk.loc[w1, c])]
    if not valid_cols:
        return pd.DataFrame(), {}
    ret = (wk.loc[w1, valid_cols] / wk.loc[w0, valid_cols] - 1.0) * 100.0
    out = pd.DataFrame({"index": ret.index, "wow_pct": ret.values}).sort_values("wow_pct")
    meta = {"week0": w0.date().isoformat(), "week1": w1.date().isoformat()}
    return out, meta

def relative_panel(panel, base_days=60):
    tail = panel.tail(min(base_days, len(panel)))
    cols = [c for c in tail.columns if c != "date"]
    base = tail.iloc[0][cols]
    rel = tail.copy()
    rel[cols] = (rel[cols] / base) * 100.0
    return rel

def get_region_summary(wow_data, indices):
    """Generate text summary for region"""
    region_data = wow_data[wow_data['index'].isin(indices)]
    if region_data.empty:
        return "insufficient data"
    avg = region_data['wow_pct'].mean()
    if avg > 0.3:
        return f"broad strength (+{avg:.2f}% avg)"
    elif avg < -0.3:
        return f"widespread weakness ({avg:.2f}% avg)"
    else:
        return f"mixed performance ({avg:+.2f}% avg)"

def compute_ai_sector_panels(ai_map, start_date, end_date, api_key):
    """Compute daily average returns for each AI sector"""
    us_sectors = {}
    china_sectors = {}
    
    for sector_name, regions in ai_map.items():
        us_stocks = regions.get("US", [])
        if us_stocks:
            us_panel = None
            for symbol in us_stocks:
                df = fetch_eod(symbol, start_date, end_date, api_key)
                if df.empty:
                    continue
                df = df.rename(columns={"close": symbol})
                if us_panel is None:
                    us_panel = df[["date", symbol]]
                else:
                    us_panel = us_panel.merge(df[["date", symbol]], on="date", how="outer")
                time.sleep(0.2)
            
            if us_panel is not None and len(us_panel.columns) > 1:
                us_panel = us_panel.set_index("date").ffill().bfill()
                us_sectors[sector_name] = us_panel.mean(axis=1).reset_index()
                us_sectors[sector_name].columns = ["date", sector_name]
        
        china_stocks = regions.get("China", [])
        if china_stocks:
            china_panel = None
            for symbol in china_stocks:
                df = fetch_eod(symbol, start_date, end_date, api_key)
                if df.empty:
                    continue
                df = df.rename(columns={"close": symbol})
                if china_panel is None:
                    china_panel = df[["date", symbol]]
                else:
                    china_panel = china_panel.merge(df[["date", symbol]], on="date", how="outer")
                time.sleep(0.2)
            
            if china_panel is not None and len(china_panel.columns) > 1:
                china_panel = china_panel.set_index("date").ffill().bfill()
                china_sectors[sector_name] = china_panel.mean(axis=1).reset_index()
                china_sectors[sector_name].columns = ["date", sector_name]
    
    us_combined = None
    for sector, df in us_sectors.items():
        if us_combined is None:
            us_combined = df
        else:
            us_combined = us_combined.merge(df, on="date", how="outer")
    
    china_combined = None
    for sector, df in china_sectors.items():
        if china_combined is None:
            china_combined = df
        else:
            china_combined = china_combined.merge(df, on="date", how="outer")
    
    return us_combined, china_combined

# Plotting functions
def plot_wow_bar_combined(index_wow, forex_wow, commodity_wow, bond_wow, meta, output_path):
    """Combined WoW bar chart with 2x2 grid for all asset classes"""
    set_broker_style()
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle(f"Weekly Performance Across All Asset Classes\n{meta['week0']} to {meta['week1']}", 
                 fontsize=15, fontweight='bold', color='#2C3E50', y=0.98)
    
    datasets = [
        (index_wow, "Equity Indices", axes[0, 0]),
        (forex_wow, "Foreign Exchange", axes[0, 1]),
        (commodity_wow, "Commodities", axes[1, 0]),
        (bond_wow, "Treasury Yields", axes[1, 1])
    ]
    
    for df_wow, title, ax in datasets:
        if df_wow.empty:
            ax.text(0.5, 0.5, f"No data\navailable", 
                   ha='center', va='center', transform=ax.transAxes, fontsize=10)
            ax.set_title(title, loc="center", pad=10, fontweight="bold", fontsize=11, color="#34495E")
            ax.axis('off')
            continue
        
        colors = ["#2E6C8F" if x > 0 else "#C62828" for x in df_wow["wow_pct"]]
        ax.barh(df_wow["index"], df_wow["wow_pct"], color=colors, 
                edgecolor="white", linewidth=0.5, alpha=0.85)
        
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["bottom"].set_color("#CCCCCC")
        ax.xaxis.grid(False)
        ax.yaxis.grid(False)
        
        max_abs_val = df_wow["wow_pct"].abs().max()
        limit = max_abs_val * 1.3
        ax.set_xlim(-limit, limit)
        
        ax.set_xlabel("WoW Change (%)", fontweight="bold", color="#2C3E50", fontsize=10)
        ax.set_title(title, loc="center", pad=10, fontweight="bold", fontsize=11, color="#34495E")
        ax.axvline(0, color="#CCCCCC", linewidth=1.2, alpha=0.6)
        
        for i, (_, row) in enumerate(df_wow.iterrows()):
            val = row["wow_pct"]
            label = f"{val:+.1f}%"
            x_pos = val + (max_abs_val * 0.08 if val > 0 else -max_abs_val * 0.08)
            ax.text(x_pos, i, label, va="center", ha="left" if val > 0 else "right",
                    fontsize=9, fontweight="bold", color="#2C3E50")
        
        ax.tick_params(axis='y', labelsize=9)
        ax.tick_params(axis='x', labelsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path

def plot_ai_wow_bar(us_wow, china_wow, meta, output_path):
    """AI sectors WoW bar chart with 1x2 layout"""
    set_broker_style()
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle(f"AI-Related Sectors — Weekly Performance\n{meta['week0']} to {meta['week1']}", 
                 fontsize=15, fontweight='bold', color='#2C3E50', y=0.98)
    
    datasets = [
        (us_wow, "US AI Sectors", axes[0]),
        (china_wow, "China AI Sectors", axes[1])
    ]
    
    for df_wow, title, ax in datasets:
        if df_wow.empty:
            ax.text(0.5, 0.5, f"No data\navailable", 
                   ha='center', va='center', transform=ax.transAxes, fontsize=10)
            ax.set_title(title, loc="center", pad=10, fontweight="bold", fontsize=12, color="#34495E")
            ax.axis('off')
            continue
        
        colors = ["#2E6C8F" if x > 0 else "#C62828" for x in df_wow["wow_pct"]]
        ax.barh(df_wow["index"], df_wow["wow_pct"], color=colors, 
                edgecolor="white", linewidth=0.5, alpha=0.85)
        
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["bottom"].set_color("#CCCCCC")
        ax.xaxis.grid(False)
        ax.yaxis.grid(False)
        
        max_abs_val = df_wow["wow_pct"].abs().max()
        limit = max_abs_val * 1.3
        ax.set_xlim(-limit, limit)
        
        ax.set_xlabel("WoW Change (%)", fontweight="bold", color="#2C3E50", fontsize=10)
        ax.set_title(title, loc="center", pad=10, fontweight="bold", fontsize=12, color="#34495E")
        ax.axvline(0, color="#CCCCCC", linewidth=1.2, alpha=0.6)
        
        for i, (_, row) in enumerate(df_wow.iterrows()):
            val = row["wow_pct"]
            label = f"{val:+.1f}%"
            x_pos = val + (max_abs_val * 0.08 if val > 0 else -max_abs_val * 0.08)
            ax.text(x_pos, i, label, va="center", ha="left" if val > 0 else "right",
                    fontsize=9, fontweight="bold", color="#2C3E50")
        
        ax.tick_params(axis='y', labelsize=9)
        ax.tick_params(axis='x', labelsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path

def plot_performance_lines(panel, title, output_path, base_days=65):
    df_rel = relative_panel(panel, base_days=base_days)
    set_broker_style()
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color("#CCCCCC")
    ax.yaxis.grid(True, color="#E5E5E5", linewidth=0.8, alpha=0.7)
    ax.xaxis.grid(False)
    for col in [c for c in df_rel.columns if c != "date"]:
        ax.plot(df_rel["date"], df_rel[col], lw=2.0, label=col, alpha=0.9)
    ax.set_ylabel("Indexed (start = 100)", fontweight="bold", color="#2C3E50")
    ax.set_title(title, loc="left", pad=15, fontweight="bold", color="#2C3E50")
    ax.axhline(100, color="#95A5A6", linewidth=1.2, linestyle="--", alpha=0.6)
    ax.legend(loc="upper left", framealpha=0.98, ncol=min(4, len(df_rel.columns)-1), 
              edgecolor="#CCCCCC", fancybox=False)
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path

def plot_yield_lines(panel, title, output_path, base_days=65):
    df = panel.tail(min(base_days, len(panel)))
    set_broker_style()
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color("#CCCCCC")
    ax.yaxis.grid(True, color="#E5E5E5", linewidth=0.8, alpha=0.7)
    ax.xaxis.grid(False)
    for col in [c for c in df.columns if c != "date"]:
        ax.plot(df["date"], df[col], lw=2.0, label=col, alpha=0.9)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.2f}%'))
    ax.set_ylabel("Yield (%)", fontweight="bold", color="#2C3E50")
    ax.set_title(title, loc="left", pad=15, fontweight="bold", color="#2C3E50")
    ax.legend(loc="upper left", framealpha=0.98, ncol=4, 
              edgecolor="#CCCCCC", fancybox=False)
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path

def plot_candlestick(symbol, label, start_date, end_date, api_key, output_path):
    df = fetch_eod(symbol, start_date, end_date, api_key)
    if df.empty:
        return None
    
    df = df.set_index("date")
    has_ohlc = all(col in df.columns for col in ['open', 'high', 'low', 'close'])
    
    if not has_ohlc:
        set_broker_style()
        fig, ax = plt.subplots(figsize=(12, 6))
        df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
        ax.plot(df.index, df['close'], color='#2C3E50', linewidth=2, label='Close')
        ax.plot(df.index, df['EMA_20'], color='#BBBBBB', linewidth=1.5, label='EMA 20')
        ax.plot(df.index, df['EMA_50'], color='#AAAAAA', linewidth=1.5, label='EMA 50')
        ax.set_ylabel("Price", fontweight="bold", color="#2C3E50")
        ax.set_title(f"{label} — Daily Chart", loc="left", fontsize=11, 
                     pad=8, color="#2C3E50", fontweight="bold")
        ax.legend(loc="upper left", fontsize=8, framealpha=0.9, edgecolor="#CCCCCC")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.autofmt_xdate()
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return output_path
    
    df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['EMA_200'] = df['close'].ewm(span=200, adjust=False).mean()
    ema_colors = ["#BBBBBB", "#AAAAAA", "#999999"]
    ema_plots = [mpf.make_addplot(df[f'EMA_{p}'], color=c, width=1.2) 
                 for p, c in zip([20, 50, 200], ema_colors)]
    mc = mpf.make_marketcolors(
        up="#FFFFFF", down="#E74C3C",
        edge={"up": "#000000", "down": "#E74C3C"},
        wick={"up": "#000000", "down": "#E74C3C"},
        volume={"up": "#95A5A6", "down": "#F1948A"}
    )
    s = mpf.make_mpf_style(
        marketcolors=mc, gridcolor="#E5E5E5", gridstyle="-",
        gridaxis="horizontal", facecolor="#F8F9FA", figcolor="#F8F9FA",
        edgecolor="#CCCCCC", y_on_right=True,
        rc={"font.size": 8, "axes.labelsize": 9, "axes.titlesize": 11,
            "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 8}
    )
    fig, axes = mpf.plot(
        df, type="candle", style=s, title="", ylabel="Price",
        figsize=(12, 6), returnfig=True, show_nontrading=False,
        datetime_format="%b %d", xrotation=0,
        scale_padding={"left": 0.05, "right": 0.35, "top": 0.3, "bottom": 0.3},
        update_width_config=dict(candle_linewidth=0.6, candle_width=0.6),
        addplot=ema_plots, volume=False
    )
    for ax in axes:
        ax.margins(x=0)
    axes[0].set_title(f"{label} — Daily Chart", loc="left", fontsize=11, 
                      pad=8, color="#2C3E50", fontweight="bold")
    legend_handles = [Line2D([0], [0], color=c, linewidth=2) for c in ema_colors]
    axes[0].legend(handles=legend_handles, labels=["EMA 20", "EMA 50", "EMA 200"],
                   loc="upper left", fontsize=8, framealpha=0.9, edgecolor="#CCCCCC")
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path

# Report generation
def generate_executive_summary(wow_data, meta):
    if wow_data.empty:
        return ""
    
    top = wow_data.iloc[-1]
    bottom = wow_data.iloc[0]
    us_summary = get_region_summary(wow_data, REGIONS["US"])
    eu_summary = get_region_summary(wow_data, REGIONS["Europe"])
    asia_summary = get_region_summary(wow_data, REGIONS["Asia"])
    
    return f"""
## Executive Summary

**Key Highlights for week ending {meta['week1']}**

**Top Performer:** {top['index']} posted {top['wow_pct']:+.2f}% gains, leading the week's performance.

**Regional Overview:**
- **US Markets:** {us_summary}
- **European Markets:** {eu_summary}
- **Asian Markets:** {asia_summary}

**Bottom Performer:** {bottom['index']} declined {bottom['wow_pct']:.2f}%, reflecting sector-specific headwinds.

The weekly performance reflects ongoing market dynamics with notable regional divergence. Investors continue to balance growth concerns against supportive policy measures.
"""

def format_economic_calendar(econ_df):
    """Format economic calendar data for HTML display"""
    if econ_df.empty:
        return "<p>No high/medium-impact US economic events scheduled for next week.</p>"
    
    html = '<table class="calendar-table"><thead><tr>'
    html += '<th>Date</th><th>Event</th><th>Actual</th><th>Forecast</th><th>Previous</th>'
    html += '</tr></thead><tbody>'
    
    for _, row in econ_df.iterrows():
        date_str = row['date'].strftime('%a, %b %d') if pd.notna(row['date']) else 'TBD'
        event = row.get('event', 'N/A')
        actual = row.get('actual', '-')
        forecast = row.get('forecast', '-')
        previous = row.get('previous', '-')
        
        html += f'<tr><td>{date_str}</td><td>{event}</td>'
        html += f'<td>{actual}</td><td>{forecast}</td><td>{previous}</td></tr>'
    
    html += '</tbody></table>'
    return html

def format_earnings_calendar(earnings_df):
    """Format earnings calendar data for HTML display"""
    if earnings_df.empty:
        return "<p>No S&P 500 earnings scheduled for next week.</p>"
    
    html = '<table class="calendar-table"><thead><tr>'
    html += '<th>Date</th><th>Time</th><th>Ticker</th><th>Company</th>'
    html += '</tr></thead><tbody>'
    
    for _, row in earnings_df.iterrows():
        try:
            date_str = pd.to_datetime(row.get('report_date', row.get('date', ''))).strftime('%a, %b %d')
        except:
            date_str = 'TBD'
        
        when = row.get('before_after_market', row.get('when', '-'))
        if when == 'BeforeMarket' or when == 'bmo':
            time_str = 'Before Open'
        elif when == 'AfterMarket' or when == 'amc':
            time_str = 'After Close'
        elif when == 'dmh':
            time_str = 'During Hours'
        else:
            time_str = str(when) if when and not pd.isna(when) else '-'
        
        ticker = row.get('code', row.get('symbol', 'N/A'))
        if isinstance(ticker, str) and ticker.endswith('.US'):
            ticker = ticker[:-3]

        name = row.get('name', 'N/A')
        
        html += f'<tr><td>{date_str}</td><td>{time_str}</td>'
        html += f'<td><strong>{ticker}</strong></td><td>{name}</td></tr>'
    
    html += '</tbody></table>'
    return html


def format_ipo_calendar(ipo_df):
    """Format IPO calendar data for HTML display"""
    if ipo_df.empty:
        return "<p>No IPOs scheduled for US or Hong Kong exchanges next week.</p>"
    
    html = '<table class="calendar-table"><thead><tr>'
    html += '<th>Date</th><th>Ticker</th><th>Company</th><th>Exchange</th><th>Deal Type</th><th>Price Range</th><th>Shares</th>'
    html += '</tr></thead><tbody>'
    
    for _, row in ipo_df.iterrows():
        try:
            date_str = pd.to_datetime(row.get('date', row.get('start_date', ''))).strftime('%a, %b %d')
        except:
            date_str = 'TBD'
        
        ticker = row.get('code', 'N/A')
        company = row.get('name', 'N/A')
        exchange = row.get('exchange', '-')
        deal_type = row.get('deal_type', '-')
        
        # Price range
        price_from = row.get('price_from', '')
        price_to = row.get('price_to', '')
        if price_from and price_to and price_from != '-' and price_to != '-':
            price_range = f"${price_from} - ${price_to}"
        elif price_from and price_from != '-':
            price_range = f"${price_from}"
        else:
            price_range = '-'
        
        # Shares
        shares = row.get('shares', '-')
        if shares and shares != '-':
            try:
                shares_formatted = f"{int(shares):,}"
            except:
                shares_formatted = str(shares)
        else:
            shares_formatted = '-'
        
        html += f'<tr><td>{date_str}</td><td><strong>{ticker}</strong></td>'
        html += f'<td>{company}</td><td>{exchange}</td><td>{deal_type}</td>'
        html += f'<td>{price_range}</td><td>{shares_formatted}</td></tr>'
    
    html += '</tbody></table>'
    return html


def build_enhanced_toc(candlestick_charts):
    """Build enhanced TOC with dropdown functionality"""
    
    equity_by_region = {}
    for chart in candlestick_charts:
        if chart['type'] == 'equity':
            for region, indices in REGIONS.items():
                if chart['label'] in indices:
                    if region not in equity_by_region:
                        equity_by_region[region] = []
                    equity_by_region[region].append(chart)
    
    ai_by_region_sector = {'US': {}, 'China': {}}
    for chart in candlestick_charts:
        if chart['type'] == 'ai':
            region = chart.get('region', 'US')
            for sector_name in AI_RELATED_MAP.keys():
                if sector_name not in ai_by_region_sector[region]:
                    ai_by_region_sector[region][sector_name] = []
                for symbol in AI_RELATED_MAP[sector_name].get(region, []):
                    if symbol.split('.')[0] in chart['label']:
                        ai_by_region_sector[region][sector_name].append(chart)
                        break
    
    toc = """
<nav class="toc">
    <h3>CONTENTS</h3>
    <ul>
        <li><a href="#executive-summary">Executive Summary</a></li>
        <li>
            <span class="toc-toggle" onclick="toggleTocSection('weekly-performance-sub')">▾</span>
            <a href="#weekly-performance">Weekly Performance</a>
            <ul id="weekly-performance-sub" style="display:block;">
                <li><a href="#equity-weekly">Equity Indices</a></li>
                <li><a href="#sectors-weekly">US Sectors</a></li>
                <li><a href="#ai-weekly">AI-Related</a></li>
                <li><a href="#forex-weekly">Foreign Exchange</a></li>
                <li><a href="#commodities-weekly">Commodities</a></li>
                <li><a href="#yields-weekly">Treasury Yields</a></li>
            </ul>
        </li>
        <li>
            <span class="toc-toggle" onclick="toggleTocSection('technical-views-sub')">▾</span>
            <a href="#technical-views">Technical Views</a>
            <ul id="technical-views-sub" style="display:block;">
                <li>
                    <span class="toc-toggle" onclick="toggleTocSection('equity-technical-sub')">▸</span>
                    <a href="#equity-technical">Equity Indices</a>
                    <ul id="equity-technical-sub" style="display:none;">
"""
    
    for region, charts in equity_by_region.items():
        region_id = f'{region.lower()}-indices-sub'
        toc += f'<li><span class="toc-toggle" onclick="toggleTocSection(\'{region_id}\')">▸</span>'
        toc += f'<span class="toc-clickable" onclick="toggleTocSection(\'{region_id}\')">{region}</span>'
        toc += f'<ul id="{region_id}" style="display:none;">'
        for chart in charts:
            symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
            toc += f'<li><a href="#chart-{symbol_id}">{chart["label"]}</a></li>'
        toc += '</ul></li>'
    
    toc += """
                    </ul>
                </li>
                <li>
                    <span class="toc-toggle" onclick="toggleTocSection('sectors-technical-sub')">▸</span>
                    <a href="#sectors-technical">US Sectors</a>
                    <ul id="sectors-technical-sub" style="display:none;">
"""
    
    sector_charts = [c for c in candlestick_charts if c['type'] == 'sector']
    for chart in sector_charts:
        symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
        toc += f'<li><a href="#chart-{symbol_id}">{chart["label"]}</a></li>'
    
    toc += """
                    </ul>
                </li>
                <li>
                    <span class="toc-toggle" onclick="toggleTocSection('ai-technical-sub')">▸</span>
                    <a href="#ai-technical">AI Stocks</a>
                    <ul id="ai-technical-sub" style="display:none;">
"""
    
    for region in ['US', 'China']:
        region_id = f'{region.lower()}-ai-sub'
        toc += f'<li><span class="toc-toggle" onclick="toggleTocSection(\'{region_id}\')">▸</span>'
        toc += f'<span class="toc-clickable" onclick="toggleTocSection(\'{region_id}\')">{region}</span>'
        toc += f'<ul id="{region_id}" style="display:none;">'
        for sector_name, charts in ai_by_region_sector[region].items():
            if charts:
                sector_id = sector_name.lower().replace(' ', '-').replace('(', '').replace(')', '').replace('&', '')
                full_sector_id = f'{region.lower()}-{sector_id}-sub'
                toc += f'<li><span class="toc-toggle" onclick="toggleTocSection(\'{full_sector_id}\')">▸</span>'
                toc += f'<span class="toc-clickable" onclick="toggleTocSection(\'{full_sector_id}\')">{sector_name}</span>'
                toc += f'<ul id="{full_sector_id}" style="display:none;">'
                for chart in charts:
                    symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
                    toc += f'<li><a href="#chart-{symbol_id}">{chart["label"]}</a></li>'
                toc += '</ul></li>'
        toc += '</ul></li>'
    
    toc += """
                    </ul>
                </li>
                <li><a href="#forex-technical">Foreign Exchange</a></li>
                <li><a href="#commodities-technical">Commodities</a></li>
                <li><a href="#yields-technical">Treasury Yields</a></li>
            </ul>
        </li>
        <li>
            <span class="toc-toggle" onclick="toggleTocSection('next-week-outlook-sub')">▾</span>
            <a href="#next-week-outlook">Next Week Outlook</a>
            <ul id="next-week-outlook-sub" style="display:block;">
                <li><a href="#economic-calendar">Economic Calendar</a></li>
                <li><a href="#earnings-calendar">Earnings Calendar</a></li>
                <li><a href="#ipo-calendar">IPO Calendar</a></li>
            </ul>
        </li>
    </ul>
</nav>
"""
    
    return toc

def build_html_report(meta, chart_paths, candlestick_charts, output_dir, econ_calendar, earnings_calendar, ipo_calendar):
    wow_data = pd.read_csv(os.path.join(output_dir, "_wow_data.csv")) if os.path.exists(os.path.join(output_dir, "_wow_data.csv")) else pd.DataFrame()
    
    toc = build_enhanced_toc(candlestick_charts)
    
    cover = f"""
<div class="cover-page" id="top">
    <h1 class="report-title">Market Weekly</h1>
    <p class="report-subtitle">Global Markets Analysis & Performance Review</p>
    <div class="period-box">
        <p><strong>Reporting Period</strong></p>
        <p>{meta.get('week0', 'N/A')} to {meta.get('week1', 'N/A')}</p>
        <p class="report-date">Report Date: {TODAY.strftime('%B %d, %Y')}</p>
    </div>
</div>
<div class="page-break"></div>
"""
    
    exec_summary = generate_executive_summary(wow_data, meta) if not wow_data.empty else ""
    summary_page = f"""
<div class="content-page" id="executive-summary">
{markdown2.markdown(exec_summary)}
</div>
<div class="page-break"></div>
""" if exec_summary else ""
    
    content = f"""
<div class="content-page" id="weekly-performance">
<h2>Weekly Performance</h2>

<h3 id="equity-weekly">Equity Indices</h3>
<img src="{os.path.basename(chart_paths['wow'])}" alt="Weekly Performance" class="chart-img">
<p class="chart-caption">Week-over-week performance across all asset classes, ranked by return within each category.</p>

<img src="{os.path.basename(chart_paths['relative_indices'])}" alt="Relative Performance" class="chart-img">
<p class="chart-caption">3-month indexed performance (base=100) showing cumulative trends across global equity markets.</p>

<h3 id="sectors-weekly">US Sectors</h3>
<img src="{os.path.basename(chart_paths.get('wow_sectors', ''))}" alt="Sector Performance" class="chart-img">
<p class="chart-caption">Week-over-week performance across US sectors via SPDR sector ETFs.</p>

<img src="{os.path.basename(chart_paths.get('sectors', ''))}" alt="Sector Trends" class="chart-img">
<p class="chart-caption">3-month sector rotation trends showing relative strength across industries.</p>
"""
    
    if 'wow_ai' in chart_paths:
        content += f"""
<h3 id="ai-weekly">AI-Related Sectors</h3>
<img src="{os.path.basename(chart_paths['wow_ai'])}" alt="AI Sector Performance" class="chart-img">
<p class="chart-caption">Week-over-week performance of AI-related sector aggregates (daily average of constituent stocks).</p>
"""
    
    if 'us_ai' in chart_paths:
        content += f"""
<img src="{os.path.basename(chart_paths['us_ai'])}" alt="US AI Performance" class="chart-img">
<p class="chart-caption">US AI-related sector performance indexed to 100.</p>
"""
    
    if 'china_ai' in chart_paths:
        content += f"""
<img src="{os.path.basename(chart_paths['china_ai'])}" alt="China AI Performance" class="chart-img">
<p class="chart-caption">China AI-related sector performance indexed to 100.</p>
"""
    
    content += f"""
<h3 id="forex-weekly">Foreign Exchange</h3>
<img src="{os.path.basename(chart_paths['forex'])}" alt="Forex Trends" class="chart-img">
<p class="chart-caption">Major currency pairs indexed to 100 at period start, reflecting relative strength movements.</p>

<h3 id="commodities-weekly">Commodities</h3>
<img src="{os.path.basename(chart_paths['commodities'])}" alt="Commodity Trends" class="chart-img">
<p class="chart-caption">Key commodity price movements reflecting supply-demand dynamics and macro trends.</p>

<h3 id="yields-weekly">US Treasury Yields</h3>
<img src="{os.path.basename(chart_paths['bonds'])}" alt="Bond Yields" class="chart-img">
<p class="chart-caption">Yield curve movements across key maturities, signaling market rate expectations.</p>
</div>
<div class="page-break"></div>
"""
    
    content += """
<div class="content-page" id="technical-views">
<h2>Technical Views</h2>

<h3 id="equity-technical">Equity Indices</h3>
"""
    
    for region, indices in REGIONS.items():
        region_charts = [c for c in candlestick_charts if c['label'] in indices and c['type'] == 'equity']
        if region_charts:
            content += f"<h4>{region} Markets</h4>\n"
            for chart in region_charts:
                symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
                content += f"""
<div id="chart-{symbol_id}">
<img src="{os.path.basename(chart['path'])}" alt="{chart['label']}" class="chart-img">
<p class="chart-caption">{chart['label']} technical chart with moving averages.</p>
</div>
"""
    
    content += """
<h3 id="sectors-technical">US Sectors</h3>
"""
    
    sector_charts = [c for c in candlestick_charts if c['type'] == 'sector']
    for chart in sector_charts:
        symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
        content += f"""
<div id="chart-{symbol_id}">
<img src="{os.path.basename(chart['path'])}" alt="{chart['label']}" class="chart-img">
<p class="chart-caption">{chart['label']} sector technical view.</p>
</div>
"""
    
    content += """
<h3 id="ai-technical">AI-Related Stocks</h3>
<h4>US AI Stocks</h4>
"""
    
    us_ai_charts = [c for c in candlestick_charts if c['type'] == 'ai' and c.get('region') == 'US']
    for chart in us_ai_charts:
        symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
        content += f"""
<div id="chart-{symbol_id}">
<img src="{os.path.basename(chart['path'])}" alt="{chart['label']}" class="chart-img">
<p class="chart-caption">{chart['label']} technical analysis.</p>
</div>
"""
    
    content += "<h4>China AI Stocks</h4>\n"
    china_ai_charts = [c for c in candlestick_charts if c['type'] == 'ai' and c.get('region') == 'China']
    for chart in china_ai_charts:
        symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
        content += f"""
<div id="chart-{symbol_id}">
<img src="{os.path.basename(chart['path'])}" alt="{chart['label']}" class="chart-img">
<p class="chart-caption">{chart['label']} technical analysis.</p>
</div>
"""
    
    content += """
<h3 id="forex-technical">Foreign Exchange</h3>
"""
    
    forex_charts = [c for c in candlestick_charts if c['type'] == 'forex']
    for chart in forex_charts:
        symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
        content += f"""
<div id="chart-{symbol_id}">
<img src="{os.path.basename(chart['path'])}" alt="{chart['label']}" class="chart-img">
<p class="chart-caption">{chart['label']} technical chart with moving averages.</p>
</div>
"""
    
    content += """
<h3 id="commodities-technical">Commodities</h3>
"""
    
    commodity_charts = [c for c in candlestick_charts if c['type'] == 'commodity']
    for chart in commodity_charts:
        symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
        content += f"""
<div id="chart-{symbol_id}">
<img src="{os.path.basename(chart['path'])}" alt="{chart['label']}" class="chart-img">
<p class="chart-caption">{chart['label']} technical chart with moving averages.</p>
</div>
"""
    
    content += """
<h3 id="yields-technical">US Treasury Yields</h3>
"""
    
    yield_charts = [c for c in candlestick_charts if c['type'] == 'yield']
    for chart in yield_charts:
        symbol_id = chart['path'].split('candlestick_')[1].replace('.png', '')
        content += f"""
<div id="chart-{symbol_id}">
<img src="{os.path.basename(chart['path'])}" alt="{chart['label']}" class="chart-img">
<p class="chart-caption">{chart['label']} technical chart showing daily yield movements.</p>
</div>
"""
    
    content += "</div>\n<div class=\"page-break\"></div>\n"
    
    outlook_page = f"""
<div class="content-page" id="next-week-outlook">
<h2>Next Week Outlook</h2>

<h3 id="economic-calendar">Economic Calendar</h3>
<p class="section-intro">High/medium-impact economic events scheduled for {NEXT_WEEK_START} to {NEXT_WEEK_END}</p>
{format_economic_calendar(econ_calendar)}

<h3 id="earnings-calendar">Earnings Calendar</h3>
<p class="section-intro">Major US company earnings releases scheduled for next week</p>
{format_earnings_calendar(earnings_calendar)}

<h3 id="ipo-calendar">IPO Calendar</h3>
<p class="section-intro">Upcoming initial public offerings on US and Hong Kong exchanges</p>
{format_ipo_calendar(ipo_calendar)}
</div>
"""
    
    footer = """
<div class="content-page" style="text-align: center; color: #999;">
<hr>
<p><em>This report is for informational purposes only and does not constitute investment advice.</em></p>
</div>
"""
    
    css = """
<style>
    @page {
        size: A4;
        margin: 2cm;
        @bottom-center {
            content: counter(page);
            font-size: 9pt;
            color: #999;
        }
    }
    @media print {
        .page-break { page-break-before: always; }
        body { max-width: 100% !important; }
        .toc { display: none !important; }
        .main-content { margin-left: 0 !important; }
        .lightbox { display: none !important; }
    }
    @media screen {
        body {
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 30px;
            background: white;
            position: relative;
        }
        .toc {
            position: fixed;
            left: calc(50% - 600px);
            top: 40px;
            width: 220px;
            padding: 20px;
            border-right: 1px solid #e0e0e0;
            max-height: calc(100vh - 80px);
            overflow-y: auto;
        }
        .toc h3 {
            font-size: 10pt;
            color: #666;
            margin: 0 0 15px 0;
            font-weight: 600;
            letter-spacing: 0.5px;
        }
        .toc ul {
            list-style: none;
            padding: 0;
            margin: 0;
        }
        .toc ul ul {
            padding-left: 15px;
            margin-top: 5px;
        }
        .toc > ul > li {
            margin: 10px 0;
            font-size: 11pt;
            color: #333;
            font-weight: normal;
        }
        .toc > ul > li > a {
            font-size: 11pt;
            color: #333;
            font-weight: normal;
        }
        .toc > ul > li > ul > li {
            margin: 6px 0;
            font-size: 9.5pt;
            color: #555;
            font-weight: normal;
        }
        .toc > ul > li > ul > li > a {
            font-size: 9.5pt;
            color: #555;
            font-weight: normal;
        }
        .toc > ul > li > ul > li > ul > li {
            margin: 5px 0;
            font-size: 9.5pt;
            color: #555;
            font-weight: normal;
        }
        .toc > ul > li > ul > li > ul > li > a {
            font-size: 9.5pt;
            color: #555;
            font-weight: normal;
        }
        .toc > ul > li > ul > li > ul > li > ul > li {
            margin: 4px 0;
            font-size: 9.5pt;
            color: #555;
            font-weight: normal;
        }
        .toc > ul > li > ul > li > ul > li > ul > li > a {
            font-size: 9.5pt;
            color: #555;
            font-weight: normal;
        }
        .toc a {
            text-decoration: none;
            display: inline-block;
            padding: 4px 0;
            transition: color 0.2s;
        }
        .toc a:hover {
            color: #1a73e8;
        }
        .toc-toggle {
            cursor: pointer;
            color: #999;
            user-select: none;
            display: inline-block;
            width: 12px;
            margin-right: 4px;
            font-size: inherit;
        }
        .toc-toggle:hover {
            color: #1a73e8;
        }
        .toc-clickable {
            cursor: pointer;
            color: inherit;
            user-select: none;
            transition: color 0.2s;
        }
        .toc-clickable:hover {
            color: #1a73e8;
        }
        .main-content {
            margin-left: 280px;
        }
        .content-page, .cover-page {
            background: transparent;
            padding: 0;
            margin-bottom: 40px;
            box-shadow: none;
            border-radius: 0;
        }
        .page-break {
            display: none;
        }
    }
    html {
        scroll-behavior: smooth;
    }
    body {
        font-family: 'Helvetica Neue', Arial, sans-serif;
        line-height: 1.6;
        color: #333;
    }
    .cover-page {
        height: 100vh;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        text-align: center;
    }
    .report-title {
        font-size: 48pt;
        font-weight: bold;
        color: #1a1a1a;
        margin-bottom: 0.3em;
    }
    .report-subtitle {
        font-size: 14pt;
        color: #666;
        margin-bottom: 3em;
    }
    .period-box {
        background: #f5f5f5;
        border: 2px solid #ddd;
        padding: 2em 4em;
        border-radius: 4px;
    }
    .period-box p {
        margin: 0.5em 0;
        font-size: 11pt;
    }
    .report-date {
        color: #666;
        font-size: 10pt !important;
    }
    .content-page {
        padding: 1cm 0;
    }
    h1, h2, h3 {
        color: #2C3E50;
        font-weight: bold;
        page-break-after: avoid;
    }
    h2 {
        font-size: 20pt;
        margin-top: 0;
        border-bottom: 2px solid #2C3E50;
        padding-bottom: 0.3em;
    }
    h3 {
        font-size: 14pt;
        color: #34495E;
        margin-top: 1.5em;
    }
    h4 {
        font-size: 12pt;
        color: #34495E;
        margin-top: 1em;
        font-weight: 600;
    }
    .chart-img {
        width: 100%;
        height: auto;
        margin: 1em 0;
        border: 1px solid #eee;
        cursor: pointer;
        transition: opacity 0.2s;
    }
    .chart-img:hover {
        opacity: 0.9;
    }
    .chart-caption {
        font-size: 9pt;
        color: #666;
        font-style: italic;
        margin-top: -0.5em;
        margin-bottom: 2em;
    }
    .section-intro {
        font-size: 10pt;
        color: #666;
        font-style: italic;
        margin-bottom: 1em;
    }
    .calendar-table {
        width: 100%;
        border-collapse: collapse;
        margin: 1em 0 2em 0;
        font-size: 9pt;
    }
    .calendar-table th {
        background: #f5f5f5;
        padding: 10px;
        text-align: left;
        border-bottom: 2px solid #ddd;
        font-weight: 600;
        color: #2C3E50;
    }
    .calendar-table td {
        padding: 8px 10px;
        border-bottom: 1px solid #eee;
    }
    .calendar-table tr:hover {
        background: #fafafa;
    }
    ul {
        line-height: 1.8;
    }
    strong {
        color: #2C3E50;
    }
    .lightbox {
        display: none;
        position: fixed;
        z-index: 9999;
        left: 0;
        top: 0;
        width: 100%;
        height: 100%;
        background-color: rgba(0, 0, 0, 0.9);
        justify-content: center;
        align-items: center;
    }
    .lightbox.active {
        display: flex;
    }
    .lightbox-content {
        max-width: 95%;
        max-height: 95%;
        object-fit: contain;
    }
    .lightbox-close {
        position: absolute;
        top: 20px;
        right: 40px;
        color: white;
        font-size: 40px;
        font-weight: bold;
        cursor: pointer;
        transition: color 0.2s;
    }
    .lightbox-close:hover {
        color: #ccc;
    }
</style>
"""
    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Market Weekly Report</title>
    {css}
</head>
<body>
{toc}
<div class="main-content">
{cover}
{summary_page}
{content}
{outlook_page}
{footer}
</div>

<div class="lightbox" id="lightbox">
    <span class="lightbox-close" onclick="closeLightbox()">&times;</span>
    <img class="lightbox-content" id="lightbox-img">
</div>

<script>
function toggleTocSection(id) {{
    const element = document.getElementById(id);
    const toggle = event.target;
    if (element.style.display === 'none') {{
        element.style.display = 'block';
        toggle.textContent = '▾';
    }} else {{
        element.style.display = 'none';
        toggle.textContent = '▸';
    }}
}}

document.addEventListener('DOMContentLoaded', function() {{
    const images = document.querySelectorAll('.chart-img');
    images.forEach(img => {{
        img.addEventListener('click', function() {{
            openLightbox(this.src);
        }});
    }});
}});

function openLightbox(src) {{
    const lightbox = document.getElementById('lightbox');
    const lightboxImg = document.getElementById('lightbox-img');
    lightboxImg.src = src;
    lightbox.classList.add('active');
}}

function closeLightbox() {{
    const lightbox = document.getElementById('lightbox');
    lightbox.classList.remove('active');
}}

document.addEventListener('keydown', function(e) {{
    if (e.key === 'Escape') {{
        closeLightbox();
    }}
}});

document.getElementById('lightbox').addEventListener('click', function(e) {{
    if (e.target === this) {{
        closeLightbox();
    }}
}});
</script>
</body>
</html>
"""
    
    return html

def generate_report(meta, chart_paths, candlestick_charts, output_dir, wow_data, econ_calendar, earnings_calendar, ipo_calendar):
    print("\nGenerating report files...")
    if not wow_data.empty:
        wow_data.to_csv(os.path.join(output_dir, "_wow_data.csv"), index=False)
    
    html_content = build_html_report(meta, chart_paths, candlestick_charts, output_dir, 
                                     econ_calendar, earnings_calendar, ipo_calendar)
    html_path = os.path.join(output_dir, "report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"  Saved HTML: {html_path}")
    
    if WEASYPRINT_AVAILABLE:
        pdf_path = os.path.join(output_dir, "report.pdf")
        try:
            HTML(string=html_content, base_url=output_dir).write_pdf(pdf_path)
            print(f"  Saved PDF: {pdf_path}")
        except Exception as e:
            print(f"  PDF generation failed: {e}")
    else:
        print("  WeasyPrint not installed - PDF not generated")

def run_report():
    report_dir = f"weekly_report_{TODAY.strftime('%Y%m%d')}"
    os.makedirs(report_dir, exist_ok=True)
    print(f"Output directory: {report_dir}/")
    
    print("\nFetching economic, earnings, and IPO calendars...")
    econ_calendar = fetch_economic_calendar(NEXT_WEEK_START, NEXT_WEEK_END, EODHD_API_KEY)
    earnings_calendar = fetch_earnings_calendar(NEXT_WEEK_START, NEXT_WEEK_END, EODHD_API_KEY)
    ipo_calendar = fetch_ipo_calendar(NEXT_WEEK_START, NEXT_WEEK_END, EODHD_API_KEY)
    
    print("\nFetching data...")
    index_panel = get_panel(INDEX_MAP, START_DATE, END_DATE, EODHD_API_KEY)
    forex_panel = get_panel(FOREX_MAP, START_DATE, END_DATE, EODHD_API_KEY)
    commodity_panel = get_panel(COMMODITY_MAP, START_DATE, END_DATE, EODHD_API_KEY)
    bond_panel = get_panel(BOND_YIELD_MAP, START_DATE, END_DATE, EODHD_API_KEY)
    sector_panel = get_panel(SECTOR_MAP, START_DATE, END_DATE, EODHD_API_KEY)
    
    print("Computing AI sector averages...")
    us_ai_panel, china_ai_panel = compute_ai_sector_panels(AI_RELATED_MAP, START_DATE, END_DATE, EODHD_API_KEY)
    
    print("Computing metrics...")
    index_wow, meta = compute_wow(index_panel)
    forex_wow, _ = compute_wow(forex_panel)
    commodity_wow, _ = compute_wow(commodity_panel)
    bond_wow, _ = compute_wow(bond_panel)
    sector_wow, _ = compute_wow(sector_panel)
    us_ai_wow, _ = compute_wow(us_ai_panel) if us_ai_panel is not None else (pd.DataFrame(), {})
    china_ai_wow, _ = compute_wow(china_ai_panel) if china_ai_panel is not None else (pd.DataFrame(), {})
    
    print("\nGenerating charts...")
    chart_paths = {
        'wow': plot_wow_bar_combined(index_wow, forex_wow, commodity_wow, bond_wow, meta,
                                     os.path.join(report_dir, "wow_performance.png")),
        'wow_sectors': plot_wow_bar_combined(sector_wow, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), meta,
                                            os.path.join(report_dir, "wow_sectors.png")),
        'relative_indices': plot_performance_lines(index_panel, "3-Month Relative Performance", 
                                                   os.path.join(report_dir, "relative_indices.png")),
        'forex': plot_performance_lines(forex_panel, "Major Forex Trends", 
                                       os.path.join(report_dir, "forex_trends.png")),
        'commodities': plot_performance_lines(commodity_panel, "Commodity Price Trends", 
                                             os.path.join(report_dir, "commodity_trends.png")),
        'bonds': plot_yield_lines(bond_panel, "US Treasury Yields", 
                                 os.path.join(report_dir, "bond_yields.png")),
        'sectors': plot_performance_lines(sector_panel, "US Sector Performance",
                                         os.path.join(report_dir, "sector_performance.png"))
    }
    
    if not us_ai_wow.empty and not china_ai_wow.empty:
        chart_paths['wow_ai'] = plot_ai_wow_bar(us_ai_wow, china_ai_wow, meta,
                                                os.path.join(report_dir, "wow_ai.png"))
    
    if us_ai_panel is not None:
        chart_paths['us_ai'] = plot_performance_lines(us_ai_panel, "US AI-Related Sectors",
                                                      os.path.join(report_dir, "us_ai_performance.png"))
    if china_ai_panel is not None:
        chart_paths['china_ai'] = plot_performance_lines(china_ai_panel, "China AI-Related Sectors",
                                                         os.path.join(report_dir, "china_ai_performance.png"))
    
    print("Generating candlestick charts...")
    candlestick_charts = []
    
    for label, symbol in INDEX_MAP.items():
        output_path = os.path.join(report_dir, f"candlestick_{symbol.replace('.', '_')}.png")
        chart_path = plot_candlestick(symbol, label, START_DATE, END_DATE, EODHD_API_KEY, output_path)
        if chart_path:
            candlestick_charts.append({'label': label, 'path': chart_path, 'type': 'equity'})
    
    for label, symbol in SECTOR_MAP.items():
        output_path = os.path.join(report_dir, f"candlestick_{symbol.replace('.', '_')}.png")
        chart_path = plot_candlestick(symbol, label, START_DATE, END_DATE, EODHD_API_KEY, output_path)
        if chart_path:
            candlestick_charts.append({'label': label, 'path': chart_path, 'type': 'sector'})
    
    for label, symbol in FOREX_MAP.items():
        output_path = os.path.join(report_dir, f"candlestick_{symbol.replace('.', '_')}.png")
        chart_path = plot_candlestick(symbol, label, START_DATE, END_DATE, EODHD_API_KEY, output_path)
        if chart_path:
            candlestick_charts.append({'label': label, 'path': chart_path, 'type': 'forex'})
    
    for label, symbol in COMMODITY_MAP.items():
        output_path = os.path.join(report_dir, f"candlestick_{symbol.replace('.', '_')}.png")
        chart_path = plot_candlestick(symbol, label, START_DATE, END_DATE, EODHD_API_KEY, output_path)
        if chart_path:
            candlestick_charts.append({'label': label, 'path': chart_path, 'type': 'commodity'})
    
    for label, symbol in BOND_YIELD_MAP.items():
        output_path = os.path.join(report_dir, f"candlestick_{symbol.replace('.', '_')}.png")
        chart_path = plot_candlestick(symbol, label, START_DATE, END_DATE, EODHD_API_KEY, output_path)
        if chart_path:
            candlestick_charts.append({'label': label, 'path': chart_path, 'type': 'yield'})
    
    print("Generating AI stock candlestick charts...")
    for sector_name, regions in AI_RELATED_MAP.items():
        for region, symbols in regions.items():
            for symbol in symbols:
                ticker = symbol.split('.')[0]
                label = f"{ticker} ({region})"
                output_path = os.path.join(report_dir, f"candlestick_{symbol.replace('.', '_')}.png")
                chart_path = plot_candlestick(symbol, label, START_DATE, END_DATE, EODHD_API_KEY, output_path)
                if chart_path:
                    candlestick_charts.append({'label': label, 'path': chart_path, 'type': 'ai', 'region': region})
    
    generate_report(meta, chart_paths, candlestick_charts, report_dir, index_wow, 
                    econ_calendar, earnings_calendar, ipo_calendar)
    print("\nReport complete!")

if __name__ == "__main__":
    run_report()