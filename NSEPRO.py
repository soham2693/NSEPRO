#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  NSE SWING TRADER  v10.0  ·  UNIFIED SINGLE FILE                                    ║
║  Terminal Output  +  Streamlit Dashboard  —  ONE file, TWO modes                    ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  INSTALL  :  pip install yfinance pandas numpy rich streamlit plotly                 ║
║                                                                                      ║
║  ── TERMINAL MODE ──────────────────────────────────────────────────────────────────║
║     python nse_trader.py                     # full live scan, Rich output          ║
║     python nse_trader.py --sample            # offline demo                         ║
║     python nse_trader.py --group "FO STOCKS"                                        ║
║     python nse_trader.py --threshold 0.18    # bear market mode                     ║
║     python nse_trader.py --capital 2000000   # ₹20L portfolio                       ║
║                                                                                      ║
║  ── STREAMLIT DASHBOARD ─────────────────────────────────────────────────────────── ║
║     streamlit run nse_trader.py                                                      ║
║                                                                                      ║
║  DISCLAIMER : Research / educational use only.  Not financial advice.               ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# DETECT MODE  (must happen before any Streamlit calls)
# ─────────────────────────────────────────────────────────────────────────────
def _is_streamlit() -> bool:
    """True when this script is executed by the Streamlit runner."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        pass
    try:                                    # older Streamlit versions
        import streamlit.runtime.scriptrunner as _sr
        return _sr.get_script_run_ctx() is not None
    except Exception:
        return False

_STREAMLIT = _is_streamlit()

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT PAGE CONFIG  — MUST be the very first st.* call
# ─────────────────────────────────────────────────────────────────────────────
if _STREAMLIT:
    import streamlit as st
    st.set_page_config(
        page_title="NSE Swing Trader Pro",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )

# ─────────────────────────────────────────────────────────────────────────────
# SHARED STDLIB
# ─────────────────────────────────────────────────────────────────────────────
import argparse
import contextlib
import io
import json
import logging
import os
import sys
import time
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from math import sqrt
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL HEAVY DEPS
# ─────────────────────────────────────────────────────────────────────────────
try:
    import yfinance as yf
    _HAS_YF = True
except ImportError:
    yf = None; _HAS_YF = False

_HAS_TA = False
try:
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        import pandas_ta as _pta
    _HAS_TA = True
except Exception:
    _pta = None

# Rich (terminal mode only)
_HAS_RICH = False
_con = None
if not _STREAMLIT:
    try:
        from rich.align   import Align
        from rich.columns import Columns
        from rich.console import Console
        from rich         import box as rbox
        from rich.padding import Padding
        from rich.panel   import Panel
        from rich.rule    import Rule
        from rich.table   import Table
        from rich.text    import Text
        from rich.progress import (Progress, SpinnerColumn, TextColumn,
                                   BarColumn, TimeElapsedColumn)
        _HAS_RICH = True
        _con = Console(highlight=False)
    except ImportError:
        pass

# Streamlit + Plotly (dashboard mode only)
if _STREAMLIT:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots

LOG = logging.getLogger("NSEv10")
for _nm in ("yfinance","peewee","urllib3","requests","charset_normalizer"):
    logging.getLogger(_nm).setLevel(logging.CRITICAL)


# ══════════════════════════════════════════════════════════════════════════════
# §1  UNIVERSE
# ══════════════════════════════════════════════════════════════════════════════

_YF_OVERRIDE: dict[str,str] = {
    "M&M":       "M%26M",
    "BAJAJ-AUTO":"BAJAJ-AUTO",
    "LTM":       "LTIM",
}

_SKIP_SYMBOLS: set[str] = {
    "APL","ETERNAL","JIOFIN","PATANJALI","ASHOKLEY",
}

_UNIVERSE: dict[str,list[str]] = {
    "NIFTY 50 LEADERS": [
        "RELIANCE","HDFCBANK","ICICIBANK","TCS","INFY","SBIN","BHARTIARTL","LT",
        "AXISBANK","KOTAKBANK","ITC","HINDUNILVR","BAJFINANCE","SUNPHARMA","TITAN",
        "MARUTI","M&M","NTPC","POWERGRID","ADANIPORTS",
    ],
    "NIFTY 50": [
        "RELIANCE","HDFCBANK","BHARTIARTL","SBIN","ICICIBANK","TCS","BAJFINANCE",
        "INFY","HINDUNILVR","LT","SUNPHARMA","MARUTI","M&M","AXISBANK","ITC",
        "KOTAKBANK","NTPC","TITAN","HCLTECH","ONGC","ULTRACEMCO","BEL","ADANIPORTS",
        "COALINDIA","JSWSTEEL","POWERGRID","BAJAJFINSV","ADANIENT","BAJAJ-AUTO",
        "TATASTEEL","NESTLEIND","ASIANPAINT","HINDALCO","WIPRO","SBILIFE","EICHERMOT",
        "SHRIRAMFIN","GRASIM","INDIGO","HDFCLIFE","TECHM","TRENT","TATAMOTORS",
        "DRREDDY","APOLLOHOSP","TATACONSUM","CIPLA","MAXHEALTH",
    ],
    "NIFTY NEXT 50": [
        "LICI","ADANIGREEN","ADANIPOWER","VEDL","HAL","SIEMENS","GODREJCP","DABUR",
        "PIDILITIND","DMART","MARICO","BRITANNIA","HAVELLS","AMBUJACEM","GAIL","BHEL",
        "SAIL","BPCL","HINDPETRO","IOC","PETRONET","CONCOR","NMDC","RECLTD","PFC",
        "IRFC","IREDA","RVNL","NHPC","SUZLON","TATAPOWER","JSWENERGY","POLYCAB",
        "CUMMINSIND","VOLTAS","DLF","LODHA","GODREJPROP","OBEROIRLTY","PRESTIGE",
        "PHOENIXLTD","INDHOTEL","JUBLFOOD","NAUKRI","MPHASIS","COFORGE","PERSISTENT",
        "KPITTECH","LTM",
    ],
    "NIFTY MIDCAP 100": [
        "TVSMOTOR","CHOLAFIN","MUTHOOTFIN","LUPIN","AUROPHARMA","DIVISLAB","ALKEM",
        "TORNTPHARM","BIOCON","GLENMARK","MANKIND","ZYDUSLIFE","LAURUSLABS","FORTIS",
        "SYNGENE","AUBANK","FEDERALBNK","BANDHANBNK","RBLBANK","IDFCFIRSTB","PNB",
        "BANKBARODA","CANBK","INDIANB","UNIONBANK","INDUSINDBK","SRF","ASTRAL",
        "CROMPTON","BLUESTARCO","KEI","ABB","BHARATFORG","BDL","TIINDIA","SONACOMS",
        "UNOMINDA","EXIDEIND","KALYANKJIL","PAGEIND","VBL","MCX","BSE","CDSL","CAMS",
        "HDFCAMC","KFINTECH","ANGELONE","NUVAMA","POLICYBZR","DIXON","AMBER","KAYNES",
        "DALBHARAT","SHREECEM","JKCEMENT","HINDZINC","NATIONALUM","JINDALSTEL",
        "APLAPOLLO","GMRAIRPORT","DELHIVERY",
    ],
    "NIFTY SMALLCAP 250": [
        "IREDA","RVNL","IRFC","NHPC","HUDCO","SJVN","NBCC","PNBHOUSING","LICHSGFIN",
        "MANAPPURAM","ABCAPITAL","LTF","TATAELXSI","OFSS","INOXWIND","WAAREEENER",
        "TATAPOWER","TORNTPOWER","OIL","ZOMATO","NYKAA","PAYTM","COLPAL","EMAMILTD",
        "PIIND","UPL","DEEPAKNTR","SUPREMEIND","SOLARINDS","MAZDOCK","BOSCHLTD","MOTHERSON",
    ],
    "NIFTY BANK": [
        "HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK","INDUSINDBK","BANDHANBNK",
        "FEDERALBNK","AUBANK","IDFCFIRSTB","PNB","BANKBARODA","CANBK","INDIANB",
        "UNIONBANK","RBLBANK",
    ],
    "NIFTY IT": [
        "TCS","INFY","HCLTECH","WIPRO","TECHM","LTM","MPHASIS","COFORGE","PERSISTENT",
        "KPITTECH","TATAELXSI","OFSS","NAUKRI",
    ],
    "NIFTY ENERGY": [
        "RELIANCE","ONGC","NTPC","POWERGRID","TATAPOWER","ADANIGREEN","ADANIPOWER",
        "JSWENERGY","NHPC","IREDA","SUZLON","INOXWIND","WAAREEENER","TORNTPOWER",
        "BPCL","IOC","HINDPETRO","OIL","GAIL","PETRONET","COALINDIA",
    ],
    "NIFTY AUTO": [
        "MARUTI","M&M","TATAMOTORS","BAJAJ-AUTO","EICHERMOT","HEROMOTOCO","TVSMOTOR",
        "MOTHERSON","BOSCHLTD","TIINDIA","SONACOMS","UNOMINDA","EXIDEIND","BHARATFORG",
    ],
    "NIFTY INFRA": [
        "LT","ADANIPORTS","POWERGRID","NTPC","COALINDIA","BHEL","SIEMENS","ABB",
        "HAVELLS","POLYCAB","KEI","CUMMINSIND","RVNL","NBCC","HUDCO","IRFC",
        "GMRAIRPORT","CONCOR","DELHIVERY","DLF","LODHA","GODREJPROP","OBEROIRLTY",
        "PRESTIGE","PHOENIXLTD",
    ],
    "FO STOCKS": [
        "RELIANCE","HDFCBANK","ICICIBANK","SBIN","TCS","INFY","AXISBANK","KOTAKBANK",
        "LT","BAJFINANCE","WIPRO","HCLTECH","SUNPHARMA","MARUTI","M&M","ITC","TITAN",
        "BHARTIARTL","ADANIPORTS","ADANIENT","BAJAJ-AUTO","BAJAJFINSV","NTPC","POWERGRID",
        "COALINDIA","ONGC","JSWSTEEL","TATASTEEL","HINDALCO","GRASIM","NESTLEIND",
        "ASIANPAINT","HINDUNILVR","TRENT","TATAMOTORS","TATACONSUM","DRREDDY","CIPLA",
        "EICHERMOT","SHRIRAMFIN","TECHM","INDUSINDBK","ULTRACEMCO","DIVISLAB","BEL",
        "HDFCLIFE","SBILIFE","MAXHEALTH","APOLLOHOSP","INDIGO","TATAPOWER","RECLTD",
        "PFC","IRFC","IREDA","RVNL","NHPC","SUZLON","JSWENERGY","ADANIGREEN",
        "WAAREEENER","BPCL","IOC","GAIL","PETRONET","TVSMOTOR","HEROMOTOCO","BOSCHLTD",
        "CHOLAFIN","MUTHOOTFIN","AUBANK","FEDERALBNK","BANDHANBNK","RBLBANK",
        "IDFCFIRSTB","PNB","BANKBARODA","CANBK","UNIONBANK","LTM","MPHASIS","COFORGE",
        "PERSISTENT","KPITTECH","TATAELXSI","OFSS","NAUKRI","DLF","LODHA","GODREJPROP",
        "OBEROIRLTY","PRESTIGE","PHOENIXLTD","POLYCAB","HAVELLS","SIEMENS","ABB",
        "CUMMINSIND","BHEL","AMBUJACEM","DMART","PIDILITIND","MARICO","DABUR",
        "BRITANNIA","COLPAL","GODREJCP","VBL","JUBLFOOD","HDFCAMC","CDSL","BSE","CAMS",
        "ANGELONE","MCX","NUVAMA","DIXON","KAYNES","AMBER","HAL","BDL","MAZDOCK",
        "SOLARINDS","JINDALSTEL","SAIL","NMDC","HINDZINC","NATIONALUM","VEDL","CONCOR",
        "DELHIVERY","GMRAIRPORT","INDHOTEL","BHARATFORG","TIINDIA","EXIDEIND","UNOMINDA",
    ],
}

_FO_SET   = set(_UNIVERSE["FO STOCKS"])
_ALL_SYMS = sorted({s for v in _UNIVERSE.values() for s in v} - _SKIP_SYMBOLS)

_SYM_GROUPS: dict[str,list[str]] = defaultdict(list)
for _g, _sl in _UNIVERSE.items():
    for _s in _sl:
        if _s not in _SKIP_SYMBOLS:
            _SYM_GROUPS[_s].append(_g)

_GRP_SHORT = {
    "NIFTY 50 LEADERS":"N50L","NIFTY 50":"N50","NIFTY NEXT 50":"NN50",
    "NIFTY MIDCAP 100":"MC100","NIFTY SMALLCAP 250":"SC250",
    "NIFTY BANK":"BNK","NIFTY IT":"IT","NIFTY ENERGY":"NRG",
    "NIFTY AUTO":"AUTO","NIFTY INFRA":"INFRA","FO STOCKS":"F&O",
}

def symbol_tags(sym: str) -> str:
    tags = list(dict.fromkeys(_GRP_SHORT.get(g, g[:4]) for g in _SYM_GROUPS.get(sym,[])))
    return " · ".join(tags[:4]) or "—"

def yf_ticker(sym: str) -> str:
    return f"{_YF_OVERRIDE.get(sym, sym)}.NS"

# ══════════════════════════════════════════════════════════════════════════════
# §2  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Cfg:
    live_period:        str   = "8mo"
    live_interval:      str   = "1d"
    output_dir:         Path  = Path("nse_v10_output")
    use_sample:         bool  = False
    prices_csv:         Path  = Path("data/prices.csv")
    fetch_fundamentals: bool  = True
    symbols:            list  = field(default_factory=list)
    min_avg_vol:        int   = 1_500_000
    min_price:          float = 30.0
    min_traded_val_cr:  float = 5.0
    top_n:              int   = 10
    ema_spans:          tuple = (9, 21, 50, 200)
    rsi_period:         int   = 14
    atr_period:         int   = 14
    bb_period:          int   = 20
    adx_period:         int   = 14
    breakout_window:    int   = 20
    min_bars:           int   = 60
    base_threshold:     float = 0.22
    bear_threshold:     float = 0.30
    min_categories:     int   = 2
    weights: dict       = field(default_factory=lambda: {
        "trend":0.24,"momentum":0.16,"breakout":0.17,"pullback":0.11,
        "volume":0.10,"pattern":0.10,"fundamental":0.08,"sentiment":0.04,
    })
    min_atr_pct:        float = 0.012
    max_atr_pct:        float = 0.09
    st_sl_mult:         float = 1.0
    st_tp_mult:         float = 1.8
    lt_sl_mult:         float = 1.5
    lt_tp_mult:         float = 3.5
    min_rr:             float = 1.5
    bt_capital:         float = 1_000_000.0
    bt_max_pos:         int   = 5
    bt_pos_pct:         float = 0.20
    bt_sl_pct:          float = 0.04
    bt_tp_pct:          float = 0.09
    bt_max_hold:        int   = 12
    bt_min_hold:        int   = 2
    bt_cost_bps:        float = 12.0
    bt_slip_bps:        float = 5.0
    # display
    capital:            float = 1_000_000.0

# ══════════════════════════════════════════════════════════════════════════════
# §3  DATA LAYER
# ══════════════════════════════════════════════════════════════════════════════

_NIFTY_CACHE: dict = {}

def _norm_dates(s: pd.Series) -> pd.Series:
    p = pd.to_datetime(s)
    return p.dt.tz_convert(None) if getattr(p.dt,"tz",None) is not None else p

def _safe_dl(ticker: str, period: str, interval: str) -> pd.DataFrame:
    if not _HAS_YF: return pd.DataFrame()
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            df = yf.download(ticker, period=period, interval=interval,
                             progress=False, auto_adjust=True, timeout=20)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def nifty50_state() -> dict:
    global _NIFTY_CACHE
    c = _NIFTY_CACHE
    if c.get("ts") and (datetime.now()-c["ts"]).seconds < 3600:
        return c
    df = _safe_dl("^NSEI","4mo","1d")
    if df is None or df.empty: return {}
    try:
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.columns = [str(x).lower().strip() for x in df.columns]
        cl = df["close"].dropna()
        if len(cl)<10: return {}
        e9=cl.ewm(span=9,adjust=False).mean(); e21=cl.ewm(span=21,adjust=False).mean()
        e50=cl.ewm(span=50,adjust=False).mean()
        rsi_val=float(_rsi_s(cl,14).iloc[-1]); last=float(cl.iloc[-1])
        l9=float(e9.iloc[-1]); l21=float(e21.iloc[-1]); l50=float(e50.iloc[-1])
        if   last>l9>l21>l50:  trend,lbl=1.0,"🐂 Strong Bull"
        elif last>l9>l21:       trend,lbl=0.7,"📈 Mild Bull"
        elif last<l9<l21<l50:   trend,lbl=-1.0,"🐻 Strong Bear"
        elif last<l9<l21:       trend,lbl=-0.7,"📉 Mild Bear"
        else:                    trend,lbl=0.0,"↔️  Sideways"
        chg1m=float((cl.iloc[-1]/cl.iloc[-21]-1)*100) if len(cl)>=21 else 0.0
        chg3m=float((cl.iloc[-1]/cl.iloc[0]-1)*100)
        _NIFTY_CACHE=dict(ts=datetime.now(),trend=trend,label=lbl,
                          rsi=round(rsi_val,1),chg_1m=round(chg1m,2),chg_3m=round(chg3m,2),
                          last=round(last,2),ema9=round(l9,2),ema21=round(l21,2),ema50=round(l50,2))
        return _NIFTY_CACHE
    except Exception as e:
        LOG.debug("Nifty: %s",e); return {}

def fetch_ohlcv(sym: str, period: str, interval: str) -> pd.DataFrame:
    df = _safe_dl(yf_ticker(sym), period, interval)
    if df is None or df.empty: return pd.DataFrame()
    try:
        if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.get_level_values(0)
        df.columns=[str(x).lower().strip() for x in df.columns]
        df=df.reset_index()
        dc=next((c for c in df.columns if c.lower() in {"date","datetime"}),df.columns[0])
        df=df.rename(columns={dc:"date"})
        df["date"]=_norm_dates(df["date"]).dt.normalize()
        df["symbol"]=sym.upper()
        df["volume"]=pd.to_numeric(df.get("volume",0),errors="coerce").fillna(0)
        need=["date","symbol","open","high","low","close","volume"]
        if any(c not in df.columns for c in need): return pd.DataFrame()
        return df[need].dropna(subset=["open","high","low","close"]).reset_index(drop=True)
    except Exception as e:
        LOG.debug("OHLCV %s: %s",sym,e); return pd.DataFrame()

def fetch_fundamentals(sym: str) -> dict:
    if not _HAS_YF: return {}
    try:
        buf=io.StringIO()
        with contextlib.redirect_stdout(buf),contextlib.redirect_stderr(buf):
            info=yf.Ticker(yf_ticker(sym)).info
        mc=info.get("marketCap",0) or 0
        return dict(pe=info.get("trailingPE"),pb=info.get("priceToBook"),
                    roe=info.get("returnOnEquity"),eps_g=info.get("earningsGrowth"),
                    rev_g=info.get("revenueGrowth"),de=info.get("debtToEquity"),
                    sector=info.get("sector","N/A"),indust=info.get("industry","N/A"),
                    mcap=round(mc/1e7,1) if mc else None,
                    w52h=info.get("fiftyTwoWeekHigh"),w52l=info.get("fiftyTwoWeekLow"),
                    beta=info.get("beta"),peg=info.get("pegRatio"),
                    div_y=info.get("dividendYield"))
    except Exception: return {}

def sample_ohlcv(syms: list, start: pd.Timestamp, end: pd.Timestamp, seed:int=42) -> pd.DataFrame:
    rng=np.random.default_rng(seed); dates=pd.bdate_range(start,end)
    profiles={"RELIANCE":{"p":2850,"d":0.0010,"v":0.018,"vol":8_000_000},
               "TCS":{"p":4180,"d":0.0006,"v":0.013,"vol":2_500_000},
               "INFY":{"p":1625,"d":0.0007,"v":0.016,"vol":6_000_000},
               "HDFCBANK":{"p":1540,"d":0.0005,"v":0.012,"vol":9_000_000},
               "ICICIBANK":{"p":1125,"d":0.0008,"v":0.017,"vol":10_000_000},
               "SBIN":{"p":800,"d":0.0009,"v":0.020,"vol":15_000_000},
               "BAJFINANCE":{"p":7200,"d":0.0007,"v":0.019,"vol":2_000_000},
               "LT":{"p":3800,"d":0.0008,"v":0.016,"vol":3_000_000},
               "BHARTIARTL":{"p":1700,"d":0.0009,"v":0.015,"vol":5_000_000},
               "KOTAKBANK":{"p":2000,"d":0.0005,"v":0.013,"vol":3_500_000}}
    rows=[]
    for s in syms:
        pr=profiles.get(s,{"p":1000,"d":0.0006,"v":0.018,"vol":2_000_000})
        c=float(pr["p"])
        for i,dt in enumerate(dates):
            rb=0.0009 if i%22<12 else -0.0003
            dr=pr["d"]+rb+rng.normal(0,pr["v"]); op=c*(1+rng.normal(0,pr["v"]/3))
            nc=max(50.0,c*(1+dr)); sp=abs(rng.normal(0.013,pr["v"]/2))
            hi=max(op,nc)*(1+sp); lo=min(op,nc)*max(0.93,1-sp)
            vol=int(max(200_000,pr["vol"]*(1+rng.normal(0,0.25))))
            rows.append({"date":dt,"symbol":s,"open":round(op,2),"high":round(hi,2),
                          "low":round(lo,2),"close":round(nc,2),"volume":vol})
            c=nc
    return pd.DataFrame(rows).sort_values(["date","symbol"]).reset_index(drop=True)

# ══════════════════════════════════════════════════════════════════════════════
# §4  INDICATOR ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _ema(s:pd.Series,n:int)->pd.Series: return s.ewm(span=n,adjust=False).mean()

def _rsi_s(close:pd.Series,n:int=14)->pd.Series:
    d=close.diff(); g=d.clip(lower=0).ewm(alpha=1/n,adjust=False,min_periods=n).mean()
    l=(-d.clip(upper=0)).ewm(alpha=1/n,adjust=False,min_periods=n).mean()
    return (100-100/(1+g/l.replace(0,np.nan))).fillna(50)

def _atr_s(df:pd.DataFrame,n:int=14)->pd.Series:
    pc=df["close"].shift(1)
    tr=pd.concat([df["high"]-df["low"],(df["high"]-pc).abs(),(df["low"]-pc).abs()],axis=1).max(axis=1)
    return tr.ewm(alpha=1/n,adjust=False,min_periods=n).mean()

def _adx_di(df:pd.DataFrame,n:int=14):
    hd=df["high"].diff(); ld=-df["low"].diff()
    pdm=pd.Series(np.where((hd>ld)&(hd>0),hd,0.0),index=df.index)
    mdm=pd.Series(np.where((ld>hd)&(ld>0),ld,0.0),index=df.index)
    pc=df["close"].shift(1)
    tr=pd.concat([df["high"]-df["low"],(df["high"]-pc).abs(),(df["low"]-pc).abs()],axis=1).max(axis=1)
    atr_s=tr.ewm(alpha=1/n,adjust=False,min_periods=n).mean().replace(0,np.nan)
    pdi=pdm.ewm(alpha=1/n,adjust=False,min_periods=n).mean()/atr_s*100
    mdi=mdm.ewm(alpha=1/n,adjust=False,min_periods=n).mean()/atr_s*100
    dx=((pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan)*100)
    return dx.ewm(alpha=1/n,adjust=False,min_periods=n).mean().fillna(20),pdi.fillna(0),mdi.fillna(0)

def _supertrend(df:pd.DataFrame,mult:float=3.0,n:int=10):
    atr_s=_atr_s(df,n); hl2=(df["high"]+df["low"])/2
    up=hl2+mult*atr_s; dn=hl2-mult*atr_s
    fi_up=up.copy(); fi_dn=dn.copy()
    for i in range(1,len(df)):
        pc=df["close"].iat[i-1]
        fi_up.iat[i]=min(up.iat[i],fi_up.iat[i-1]) if pc<=fi_up.iat[i-1] else up.iat[i]
        fi_dn.iat[i]=max(dn.iat[i],fi_dn.iat[i-1]) if pc>=fi_dn.iat[i-1] else dn.iat[i]
    direction=pd.Series(1.0,index=df.index)
    for i in range(1,len(df)):
        pd_=direction.iat[i-1]
        if pd_==-1 and df["close"].iat[i]>fi_up.iat[i]: direction.iat[i]=1
        elif pd_==1 and df["close"].iat[i]<fi_dn.iat[i]: direction.iat[i]=-1
        else: direction.iat[i]=pd_
    flip=((direction==1)&(direction.shift(1).fillna(-1)==-1)).astype(int)
    return direction,flip

def compute_indicators(raw:pd.DataFrame,cfg:Cfg)->pd.DataFrame:
    df=raw.copy().sort_values("date").reset_index(drop=True)
    if len(df)<cfg.min_bars: return pd.DataFrame()
    c=df["close"]; h=df["high"]; l=df["low"]; o=df["open"]; v=df["volume"]
    for sp in cfg.ema_spans: df[f"ema{sp}"]=_ema(c,sp)
    df["ema_gap"]=(df["ema9"]/df["ema21"].replace(0,np.nan)-1)*100
    df["macd"]=_ema(c,12)-_ema(c,26); df["macd_sig"]=_ema(df["macd"],9)
    df["macd_h"]=df["macd"]-df["macd_sig"]; df["macd_h_p"]=df["macd_h"].shift(1)
    df["rsi14"]=_rsi_s(c,14); df["rsi9"]=_rsi_s(c,9)
    df["atr14"]=_atr_s(df,14); df["atr_pct"]=df["atr14"]/c.replace(0,np.nan)*100
    df["adx"],df["plus_di"],df["minus_di"]=_adx_di(df,14)
    df["st_dir"],df["st_flip"]=_supertrend(df,mult=3.0,n=10)
    bb_mid=c.rolling(cfg.bb_period,min_periods=10).mean()
    bb_std=c.rolling(cfg.bb_period,min_periods=10).std()
    df["bb_up"]=bb_mid+2*bb_std; df["bb_dn"]=bb_mid-2*bb_std
    bb_rng=(df["bb_up"]-df["bb_dn"]).replace(0,np.nan)
    df["bb_pct"]=(c-df["bb_dn"])/bb_rng; df["bb_bw"]=bb_rng/bb_mid.replace(0,np.nan)
    lo14=l.rolling(14,min_periods=7).min(); hi14=h.rolling(14,min_periods=7).max()
    df["stoch_k"]=((c-lo14)/(hi14-lo14).replace(0,np.nan)*100).fillna(50)
    df["stoch_d"]=df["stoch_k"].rolling(3,min_periods=1).mean()
    tp=(h+l+c)/3; tp_ma=tp.rolling(20,min_periods=10).mean()
    tp_md=tp.rolling(20,min_periods=10).apply(lambda x:np.mean(np.abs(x-x.mean())),raw=True)
    df["cci"]=((tp-tp_ma)/(0.015*tp_md.replace(0,np.nan))).fillna(0)
    hi14r=h.rolling(14,min_periods=7).max(); lo14r=l.rolling(14,min_periods=7).min()
    df["willr"]=((hi14r-c)/(hi14r-lo14r).replace(0,np.nan)*-100).fillna(-50)
    tp2=(h+l+c)/3; rmf=tp2*v
    pos_mf=rmf.where(tp2>tp2.shift(1),0.0); neg_mf=rmf.where(tp2<tp2.shift(1),0.0)
    mfr=(pos_mf.rolling(14,min_periods=7).sum()/neg_mf.rolling(14,min_periods=7).sum().replace(0,np.nan))
    df["mfi"]=(100-100/(1+mfr)).fillna(50)
    df["obv"]=(np.sign(c.diff())*v).cumsum(); df["obv_ema"]=_ema(df["obv"],20)
    df["avg_vol20"]=v.rolling(20,min_periods=10).mean()
    df["vol_ratio"]=v/df["avg_vol20"].replace(0,np.nan)
    df["vol_z"]=(v-df["avg_vol20"])/v.rolling(20,min_periods=10).std().replace(0,np.nan)
    df["vol_z"]=df["vol_z"].fillna(0)
    df["med_tv20"]=(c*v).rolling(20,min_periods=10).median()
    df["ret1"]=c.pct_change(); df["ret5"]=c.pct_change(5); df["ret20"]=c.pct_change(20)
    df["hi20"]=h.shift(1).rolling(cfg.breakout_window,min_periods=8).max()
    df["lo20"]=l.shift(1).rolling(cfg.breakout_window,min_periods=8).min()
    df["hi50"]=h.shift(1).rolling(50,min_periods=15).max()
    df["hi52"]=h.rolling(252,min_periods=50).max(); df["lo52"]=l.rolling(252,min_periods=50).min()
    df["bo20"]=(c>df["hi20"]).astype(int); df["bo50"]=(c>df["hi50"]).astype(int)
    df["n52h"]=(c>=df["hi52"]*0.97).astype(int); df["n52l"]=(c<=df["lo52"]*1.03).astype(int)
    df["bo_d"]=(c/df["hi20"].replace(0,np.nan)-1)*100; df["bd_d"]=(c/df["lo20"].replace(0,np.nan)-1)*100
    df["pull_slow"]=(c/df["ema21"].replace(0,np.nan)-1)*100
    df["pull_mid"]=(c/df["ema50"].replace(0,np.nan)-1)*100
    po=o.shift(1); pc_=c.shift(1); ph=h.shift(1); pl=l.shift(1)
    body=(c-o).abs(); rng_=(h-l).replace(0,np.nan)
    ls=df[["open","close"]].min(axis=1)-l; us=h-df[["open","close"]].max(axis=1)
    df["cdl_bull_eng"]=((c>o)&(pc_<po)&(o<=pc_)&(c>=po)).astype(int)
    df["cdl_bear_eng"]=((c<o)&(pc_>po)&(o>=pc_)&(c<=po)).astype(int)
    df["cdl_hammer"]=((ls>=2.0*body)&(us<=0.3*body)&(c>o)).astype(int)
    df["cdl_inv_hammer"]=((us>=2.0*body)&(ls<=0.3*body)&(c>o)).astype(int)
    df["cdl_doji"]=((body/rng_).fillna(1.0)<0.10).astype(int)
    df["cdl_marubozu"]=((body/rng_).fillna(0.0)>=0.80).astype(int)
    df["cdl_inside"]=((h<ph)&(l>pl)).astype(int)
    df["cdl_hh"]=((h>ph)&(l>pl)).astype(int)
    df["cdl_morn_star"]=((pc_.shift(1)<po.shift(1))&
                         ((c.shift(1)-o.shift(1)).abs()<(rng_.shift(2).fillna(1)*0.35))&
                         (c>(po.shift(1)+pc_.shift(1))/2)).astype(int)
    df["cdl_3ws"]=((c>o)&(pc_>po)&(c.shift(2)>o.shift(2))&(c>pc_)&(pc_>c.shift(2))).astype(int)
    df["cdl_pier"]=((pc_<po)&(c>o)&(c>(po+pc_)/2)&(c<po)).astype(int)
    df["cdl_harami"]=((h<ph)&(l>pl)&(c>o)&(pc_<po)).astype(int)
    df["cdl_bo_candle"]=((c>df["hi20"])&(df["vol_ratio"].fillna(0)>1.3)).astype(int)
    df["cdl_sup_bounce"]=((df["ret1"].fillna(0)>0.005)&(c>c.shift(1))&(df["bd_d"].fillna(100)<3.0)).astype(int)
    return df.replace([np.inf,-np.inf],np.nan)

def engineer_all(prices:pd.DataFrame,cfg:Cfg)->pd.DataFrame:
    parts=[]
    for sym,grp in prices.groupby("symbol",sort=False):
        r=compute_indicators(grp,cfg)
        if not r.empty: parts.append(r)
    return pd.concat(parts,ignore_index=True) if parts else pd.DataFrame()

# ══════════════════════════════════════════════════════════════════════════════
# §5  PATTERN ENGINE
# ══════════════════════════════════════════════════════════════════════════════

CAT_COL = {
    "Trend":"cyan","Momentum":"yellow","Candlestick":"magenta",
    "Breakout":"bright_green","Volume":"blue","Volatility":"white",
    "Price Action":"bright_white","Structure":"bright_cyan",
}

def _g(row,k:str,d:float=0.0)->float:
    try: v=row[k] if isinstance(row,dict) else getattr(row,k,d)
    except Exception: return d
    if v is None or (isinstance(v,float) and np.isnan(v)): return d
    return float(v)

def detect_patterns(row,tail:pd.DataFrame)->list:
    hits:dict[str,tuple]={}
    def add(sc,lb,cat):
        if lb not in hits or sc>hits[lb][0]: hits[lb]=(sc,lb,cat)
    prev=tail.iloc[-2] if len(tail)>=2 else row
    prev2=tail.iloc[-3] if len(tail)>=3 else prev
    c=_g(row,"close"); e9=_g(row,"ema9"); e21=_g(row,"ema21")
    e50=_g(row,"ema50"); e200=_g(row,"ema200",e50)
    rsi=_g(row,"rsi14",50); mh=_g(row,"macd_h"); macd=_g(row,"macd")
    macds=_g(row,"macd_sig"); mhp=_g(row,"macd_h_p")
    atr=_g(row,"atr14",c*0.02) or c*0.02
    vol=_g(row,"vol_ratio",1.0); adx=_g(row,"adx",20)
    stk=_g(row,"stoch_k",50); std_=_g(row,"stoch_d",50)
    cci=_g(row,"cci",0); wr=_g(row,"willr",-50); mfi=_g(row,"mfi",50)
    bbp=_g(row,"bb_pct",0.5); bbw=_g(row,"bb_bw",0.04)
    st=_g(row,"st_dir",0); stf=_g(row,"st_flip",0)
    obv=_g(row,"obv",0); obve=_g(row,"obv_ema",0); vz=_g(row,"vol_z",0)
    pdi=_g(row,"plus_di",0); mdi=_g(row,"minus_di",0)
    pull=_g(row,"pull_slow",0)
    pc=_g(prev,"close"); pe9=_g(prev,"ema9"); pe21=_g(prev,"ema21")
    pmh=_g(prev,"macd_h"); prsi=_g(prev,"rsi14",50)
    pstk=_g(prev,"stoch_k",50); pbbp=_g(prev,"bb_pct",0.5)
    pobv=_g(prev,"obv",0); p2obv=_g(prev2,"obv",0); pst=_g(prev,"st_dir",0)

    if c>e9>e21>e50>e200: add(0.96,"🌟 Full EMA Bull Stack (All 4 EMAs)","Trend")
    elif c>e9>e21>e50:    add(0.85,"📈 EMA Bull Stack (9>21>50)","Trend")
    elif c>e9>e21:        add(0.70,"📊 Short EMA Bullish (9>21)","Trend")
    if c>e200 and e50>e200: add(0.72,"📊 Price & EMA50 Above 200","Trend")
    if pe9<=pe21 and e9>e21: add(0.92,"⚡ Golden Cross: EMA9/EMA21","Trend")
    if pe21<=e50*1.005 and e9>e21 and e21>e50: add(0.94,"🌟 Full EMA Bullish Alignment","Trend")
    if adx>28 and e9>e21 and pdi>mdi: add(0.88,"💪 Strong Trend: ADX>28, +DI>-DI","Trend")
    if adx>40: add(0.92,"🔥 Very Strong Trend: ADX>40","Trend")
    if stf==1: add(0.97,"🟩 SuperTrend BUY Flip (Bear→Bull!)","Trend")
    elif st==1: add(0.74,"🟩 SuperTrend Bullish Mode","Trend")
    if mhp<=0 and mh>0: add(0.90,"🔀 MACD Histogram Bull Cross","Momentum")
    if macd<0 and mh>0 and macd>macds: add(0.93,"🚀 MACD Bull Cross Below Zero","Momentum")
    if macd>0 and mh>0 and mh>pmh: add(0.76,"✅ MACD Positive + Accelerating","Momentum")
    if prsi<30 and rsi>30: add(0.95,"🚀 RSI Oversold Bounce (prsi<30→rsi>30)","Momentum")
    elif rsi<30: add(0.87,"🟢 RSI Oversold <30","Momentum")
    elif rsi<38: add(0.73,"🟡 RSI Near-Oversold <38","Momentum")
    if pstk<20 and stk>20 and stk>std_: add(0.90,"🔀 Stochastic Bull Cross Oversold","Momentum")
    elif stk<20: add(0.78,"📉 Stochastic Oversold Zone","Momentum")
    if cci<-100: add(0.74,"📉 CCI Oversold <-100","Momentum")
    if _g(prev,"willr",-50)<-80 and wr>-80: add(0.86,"🔀 Williams %R Bounce","Momentum")
    if mfi<25 and vol>1.2: add(0.80,"💰 MFI Oversold <25 + Volume","Momentum")
    if pbbp<0.05 and bbp>0.10: add(0.90,"↩️  BB Lower Band Bounce","Volatility")
    elif bbp<0.05: add(0.80,"📌 BB Lower Band Touch","Volatility")
    if bbw<0.035 and vol>1.2: add(0.84,"💥 BB Squeeze Breakout","Volatility")
    if vol>=2.5 and c>pc: add(0.95,"🔊 Volume Surge 2.5× (Institutional)","Volume")
    elif vol>=2.0 and c>pc: add(0.90,"🔊 Volume 2× on Up Day","Volume")
    elif vol>=1.5 and c>pc: add(0.78,"📢 Volume 1.5× Bullish","Volume")
    if vz>2.5 and c>pc: add(0.88,"🌊 Volume Z-Score >2.5σ","Volume")
    if obv>obve and c>e21: add(0.68,"📊 OBV Above 20d EMA","Volume")
    if obv>pobv>p2obv: add(0.65,"📈 OBV 3-Bar Rising (Accumulation)","Volume")
    if _g(row,"bo50")==1:
        sc=0.95 if vol>1.5 else 0.80
        add(sc,f"🏆 50-Day Breakout{' + Volume' if vol>1.5 else ''}","Breakout")
    if _g(row,"bo20")==1:
        sc=0.90 if vol>1.5 else 0.74
        add(sc,f"🚀 20-Day Breakout{' + Volume' if vol>1.5 else ''}","Breakout")
    if _g(row,"n52h")==1 and vol>1.2: add(0.87,"🏔️  Near 52W High + Volume","Breakout")
    if _g(row,"n52l")==1 and vol>1.3 and rsi<42: add(0.82,"🪃 52W Low Base Accumulation","Breakout")
    if _g(row,"cdl_bo_candle")==1: add(0.89,"💡 Breakout Candle (Vol-Confirmed)","Breakout")
    if _g(row,"cdl_inside")==1 and c>pc and e9>e21: add(0.84,"📦 Inside Bar Breakout","Price Action")
    if _g(row,"cdl_sup_bounce")==1: add(0.82,"🪨 Support Bounce (20D Low Zone)","Price Action")
    if abs(pull)<2.0 and rsi>45 and e9>e50: add(0.76,"📐 Pullback to EMA21 (Retest)","Price Action")
    if _g(row,"cdl_bull_eng")==1: add(0.92,"🕯️  Bullish Engulfing","Candlestick")
    if _g(row,"cdl_hammer")==1: add(0.84,"🔨 Hammer Candle","Candlestick")
    if _g(row,"cdl_morn_star")==1: add(0.95,"🌅 Morning Star (3-Bar Reversal)","Candlestick")
    if _g(row,"cdl_3ws")==1: add(0.93,"⚔️  Three White Soldiers","Candlestick")
    if _g(row,"cdl_pier")==1: add(0.86,"🗡️  Piercing Line","Candlestick")
    if _g(row,"cdl_harami")==1 and e9>e21: add(0.72,"🤱 Bullish Harami","Candlestick")
    if _g(row,"cdl_marubozu")==1 and c>pc: add(0.82,"📊 Bullish Marubozu","Candlestick")
    if _g(row,"cdl_hh")==1 and e9>e21: add(0.66,"📶 Higher High + Higher Low","Structure")
    return sorted(hits.values(),key=lambda x:-x[0])

def pat_confidence(hits:list)->float:
    if not hits: return 0.0
    s=hits[0][0]
    for i,h in enumerate(hits[1:6],1): s+=h[0]*(0.60**i)
    return round(min(s/1.9,0.98),4)

def n_cats(hits:list)->int: return len({h[2] for h in hits})

# ══════════════════════════════════════════════════════════════════════════════
# §6  COMPOSITE SCORE
# ══════════════════════════════════════════════════════════════════════════════

def row_score(row,weights:dict)->float:
    c=_g(row,"close"); e9=_g(row,"ema9"); e21=_g(row,"ema21")
    e50=_g(row,"ema50"); e200=_g(row,"ema200",e50)
    bull=((c>e9)+(e9>e21)+(e21>e50)+(e50>e200))/4.0
    bear=((c<e9)+(e9<e21)+(e21<e50)+(e50<e200))/4.0
    trend=float(np.clip(bull-bear,-1,1))
    rsi=_g(row,"rsi14",50); mh=_g(row,"macd_h")
    atr=_g(row,"atr14",c*0.02) or c*0.02
    rsi_s=float(np.clip((rsi-50)/18,-1,1)); macd_s=float(np.clip((mh/atr)*3,-1,1))
    mom=float(np.clip(0.6*rsi_s+0.4*macd_s,-1,1))
    bd=_g(row,"bo20"); vol=_g(row,"vol_ratio",1.0)
    bod=float(np.clip(_g(row,"bo_d",0)/8,-1,1))
    brk=float(np.clip(max(float(bd),bod)*min(vol/1.5,1.2),-1,1))
    pull=_g(row,"pull_slow",0)
    pull_s=(0.75 if (abs(pull)<2.5 and c>e50 and 40<rsi<62) else
            0.55 if (abs(pull)<2.5 and c>e21 and 40<rsi<62) else 0.0)
    vol_s=float(np.clip((vol-1)/1.5,-1,1))
    eng=_g(row,"cdl_bull_eng"); ham=_g(row,"cdl_hammer")
    morn=_g(row,"cdl_morn_star"); sol=_g(row,"cdl_3ws")
    pat_raw=(1.0 if (morn or sol) else 0.85 if eng else 0.65 if ham else 0.0)
    pat_s=float(np.clip(pat_raw,-1,1))
    pe=_g(row,"_pe",0); roe=_g(row,"_roe",0); eg=_g(row,"_epsg",0)
    fund_s=0.0
    if pe>0: fund_s+=0.35 if pe<22 else(-0.20 if pe>60 else 0.10)
    if roe>0: fund_s+=0.35 if roe>0.18 else(-0.10 if roe<0.05 else 0.10)
    if eg!=0: fund_s+=0.20 if eg>0.12 else(-0.10 if eg<0 else 0.05)
    fund_s=float(np.clip(fund_s,-1,1))
    r5=_g(row,"ret5",0); sent_s=float(np.clip(r5/0.05,-1,1))
    w=weights; ws=sum(abs(v) for v in w.values())
    sc=(w.get("trend",0)*trend+w.get("momentum",0)*mom+w.get("breakout",0)*brk+
        w.get("pullback",0)*pull_s+w.get("volume",0)*vol_s+w.get("pattern",0)*pat_s+
        w.get("fundamental",0)*fund_s+w.get("sentiment",0)*sent_s)
    return float(np.clip(sc/ws,-1,1))

def add_scores(feat:pd.DataFrame,cfg:Cfg,nifty_trend:float)->pd.DataFrame:
    threshold=cfg.bear_threshold if nifty_trend<=-0.5 else cfg.base_threshold
    df=feat.copy()
    df["score"]=df.apply(lambda r:row_score(r,cfg.weights),axis=1)
    df["signal"]=np.where(df["score"]>=threshold,"LONG","NEUTRAL")
    return df

# ══════════════════════════════════════════════════════════════════════════════
# §7  CONFIDENCE MODULES
# ══════════════════════════════════════════════════════════════════════════════

def ai_confidence(row,fund:dict,hits:list)->dict:
    W={"trend":0.24,"momentum":0.16,"breakout":0.17,"pullback":0.11,
       "volume":0.10,"pattern":0.10,"fundamental":0.08,"sentiment":0.04}
    c=_g(row,"close"); e9=_g(row,"ema9"); e21=_g(row,"ema21")
    e50=_g(row,"ema50"); e200=_g(row,"ema200",e50)
    bull=((c>e9)+(e9>e21)+(e21>e50)+(e50>e200))/4.0
    bear=((c<e9)+(e9<e21)+(e21<e50)+(e50<e200))/4.0
    trend_s=float(np.clip(bull-bear,-1,1))
    rsi=_g(row,"rsi14",50); mh=_g(row,"macd_h")
    atr=_g(row,"atr14",c*0.02) or c*0.02
    rsi_s=float(np.clip((rsi-50)/18,-1,1)); macd_s=float(np.clip((mh/atr)*3,-1,1))
    mom_s=float(np.clip(0.6*rsi_s+0.4*macd_s,-1,1))
    bd=_g(row,"bo20"); vol=_g(row,"vol_ratio",1.0)
    brk_s=float(np.clip(bd*(vol/1.5),-1,1))
    pull=_g(row,"pull_slow",0)
    pull_s=0.75 if (abs(pull)<2.5 and c>e50 and 40<rsi<62) else 0.0
    vol_s=float(np.clip((vol-1)/1.5,-1,1))
    pat_s=min(sum(h[0]*0.15 for h in hits[:6]),1.0)
    pe=fund.get("pe"); roe=fund.get("roe"); eg=fund.get("eps_g")
    rg=fund.get("rev_g"); de=fund.get("de"); peg=fund.get("peg")
    fund_s=0.0
    if pe and pe>0: fund_s+=0.30 if pe<18 else(0.15 if pe<28 else(-0.20 if pe>55 else 0.05))
    if roe: fund_s+=0.30 if roe>0.20 else(0.10 if roe>0.12 else(-0.10 if roe<0.05 else 0))
    if eg: fund_s+=0.20 if eg>0.15 else(-0.10 if eg<0 else 0.05)
    if rg: fund_s+=0.10 if rg>0.10 else 0
    if de: fund_s-=0.15 if de>3 else(0.05 if de>1.5 else 0)
    if peg and peg>0: fund_s+=0.10 if peg<1 else(-0.05 if peg>2 else 0)
    fund_s=float(np.clip(fund_s,-1,1))
    r5=_g(row,"ret5",0); r20=_g(row,"ret20",0)
    sent_s=float(np.clip((r5*0.7+r20*0.3)/0.05,-1,1))
    total=float(np.clip(
        W["trend"]*trend_s+W["momentum"]*mom_s+W["breakout"]*brk_s+
        W["pullback"]*pull_s+W["volume"]*vol_s+W["pattern"]*pat_s+
        W["fundamental"]*fund_s+W["sentiment"]*sent_s,-1,1))
    ai_pct=round(((total+1)/2)*100,1)
    bonus=0.0
    if trend_s>0.75: bonus+=0.03
    if vol_s>0.40: bonus+=0.02
    if pat_s>0.50: bonus+=0.02
    ai_pct=min(round(ai_pct+bonus*50,1),99.0)
    return dict(ai_pct=ai_pct,total=round(total,4),trend_s=round(trend_s,3),
                mom_s=round(mom_s,3),brk_s=round(brk_s,3),vol_s=round(vol_s,3),
                fund_s=round(fund_s,3),sent_s=round(sent_s,3),pat_s=round(pat_s,3))

def mkt_confidence(nifty:dict)->dict:
    if not nifty: return dict(pct=50.0,label="Unknown",align="⚪ N/A",nifty_last=0,chg_1m=0,rsi=50)
    nt=nifty.get("trend",0.0); lbl=nifty.get("label","N/A")
    nl=nifty.get("last",0.0); c1m=nifty.get("chg_1m",0.0); nr=nifty.get("rsi",50.0)
    pct=float(np.clip((nt+1)/2*100,0,100))
    align=("✅ Favorable" if nt>0.5 else "🟡 Supportive" if nt>0.2 else
           "⚠️  Headwind" if nt<-0.3 else "🔄 Neutral")
    return dict(pct=round(pct,1),label=lbl,align=align,
                nifty_last=round(nl,2),chg_1m=round(c1m,2),rsi=round(nr,1))

# ══════════════════════════════════════════════════════════════════════════════
# §8  TRADE LEVELS
# ══════════════════════════════════════════════════════════════════════════════

def trade_levels(close:float,atr:float,cfg:Cfg)->Optional[dict]:
    if atr<=0 or close<=0: return None
    def rr(sl,tp): return round(abs(tp-close)/abs(close-sl),2) if abs(close-sl)>0 else 0.0
    st_sl=round(close-cfg.st_sl_mult*atr,2); st_tp=round(close+cfg.st_tp_mult*atr,2)
    lt_sl=round(close-cfg.lt_sl_mult*atr,2); lt_tp=round(close+cfg.lt_tp_mult*atr,2)
    st_rr=rr(st_sl,st_tp); lt_rr=rr(lt_sl,lt_tp)
    if st_rr<cfg.min_rr and lt_rr<cfg.min_rr: return None
    def pkg(sl,tp,rr_v,win):
        return dict(entry=round(close,2),sl=sl,tp=tp,risk=round(abs(close-sl),2),
                    reward=round(abs(tp-close),2),rr=rr_v,rr_str=f"1:{rr_v}",window=win)
    return dict(short_term=pkg(st_sl,st_tp,st_rr,"2–5 trading days"),
                long_term =pkg(lt_sl,lt_tp,lt_rr,"10–20 trading days"))

# ══════════════════════════════════════════════════════════════════════════════
# §9  BACKTEST
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class _Pos:
    sym:str; qty:int; entry_date:pd.Timestamp; entry_p:float; fees_in:float; bars:int=0

def run_backtest(feat:pd.DataFrame,cfg:Cfg)->dict:
    empty=dict(ret=0.0,sharpe=0.0,maxdd=0.0,winrate=0.0,trades=0,
               final=cfg.bt_capital,avg_ret=0.0,avg_bars=0.0,trades_df=pd.DataFrame())
    need={"date","symbol","open","close","score","signal"}
    if not need.issubset(feat.columns): return empty
    data=feat.copy(); data["date"]=_norm_dates(data["date"])
    data=data.sort_values(["date","symbol"]).reset_index(drop=True)
    cost=cfg.bt_cost_bps/10_000; slip=cfg.bt_slip_bps/10_000
    by_d={d:g.set_index("symbol") for d,g in data.groupby("date")}
    dates=sorted(by_d.keys())
    buys=defaultdict(list); sells=defaultdict(list)
    poss:dict[str,_Pos]={}; trades=[]; eq_rows=[]; cash=cfg.bt_capital
    for idx,date in enumerate(dates):
        day=by_d[date]
        for sym,reason in list(sells.pop(date,[])):
            p=poss.get(sym)
            if not p or sym not in day.index: continue
            fp=float(day.loc[sym,"open"])*(1-slip); tv=p.qty*fp; ef=abs(tv)*cost; cash+=tv-ef
            pnl=p.qty*(fp-p.entry_p)-p.fees_in-ef; basis=p.qty*p.entry_p
            trades.append(dict(sym=sym,entry=p.entry_date,exit=date,ep=round(p.entry_p,2),
                               xp=round(fp,2),pnl=round(pnl,2),ret=round(pnl/basis if basis else 0,4),
                               bars=p.bars,reason=reason))
            del poss[sym]
        for sym,conf in sorted(buys.pop(date,[]),key=lambda x:-x[1]):
            if sym in poss or sym not in day.index: continue
            if len(poss)>=cfg.bt_max_pos: break
            fp=float(day.loc[sym,"open"])*(1+slip)
            budget=min(cfg.bt_capital*cfg.bt_pos_pct,max(cash,0)/max(1,cfg.bt_max_pos-len(poss)))
            qty=int(budget//fp)
            if qty<=0 or cash<qty*fp: continue
            tv=qty*fp; ef=abs(tv)*cost; cash-=tv+ef
            poss[sym]=_Pos(sym,qty,date,fp,ef)
        equity=cash+sum(p.qty*float(day.loc[s,"close"]) for s,p in poss.items() if s in day.index)
        eq_rows.append(dict(date=date,equity=round(equity,2)))
        nd=dates[idx+1] if idx+1<len(dates) else None
        if not nd: continue
        for sym,p in list(poss.items()):
            if sym not in day.index: continue
            p.bars+=1; pnl_pct=float(day.loc[sym,"close"])/p.entry_p-1
            sig=str(day.loc[sym,"signal"]) if "signal" in day.columns else "NEUTRAL"
            reason=None
            if pnl_pct<=-cfg.bt_sl_pct: reason="stop_loss"
            elif pnl_pct>=cfg.bt_tp_pct: reason="take_profit"
            elif p.bars>=cfg.bt_max_hold: reason="max_hold"
            elif p.bars>=cfg.bt_min_hold and sig!="LONG": reason="signal_exit"
            if reason and not any(s==sym for s,_ in sells[nd]): sells[nd].append((sym,reason))
        blocked=set(poss)|{s for s,_ in sells[nd]}|{s for s,_ in buys[nd]}
        slots=cfg.bt_max_pos-len(poss)+len({s for s,_ in sells[nd]})-len(buys[nd])
        if slots>0:
            cands=day.reset_index()
            cands=cands[(cands.get("signal","NEUTRAL")=="LONG")&
                        (cands.get("score",pd.Series(dtype=float)).fillna(0)>=cfg.base_threshold)&
                        (~cands["symbol"].isin(blocked))].sort_values("score",ascending=False)
            for r in cands.head(slots).itertuples(index=False):
                buys[nd].append((r.symbol,float(getattr(r,"score",0))))
    ld=by_d[dates[-1]]
    for sym,p in list(poss.items()):
        if sym not in ld.index: continue
        fp=float(ld.loc[sym,"close"])*(1-slip); tv=p.qty*fp; ef=abs(tv)*cost; cash+=tv-ef
        pnl=p.qty*(fp-p.entry_p)-p.fees_in-ef; basis=p.qty*p.entry_p
        trades.append(dict(sym=sym,entry=p.entry_date,exit=dates[-1],ep=round(p.entry_p,2),
                           xp=round(fp,2),pnl=round(pnl,2),ret=round(pnl/basis if basis else 0,4),
                           bars=p.bars,reason="eop"))
    eq=pd.DataFrame(eq_rows); trd=pd.DataFrame(trades)
    if not eq.empty: eq["dr"]=eq["equity"].pct_change().fillna(0); eq["dd"]=(eq["equity"]/eq["equity"].cummax())-1
    std=float(eq["dr"].std(ddof=0)) if not eq.empty and "dr" in eq.columns else 0
    sharpe=float((eq["dr"].mean()/std)*sqrt(252)) if std else 0.0
    final=float(eq["equity"].iloc[-1]) if not eq.empty else cfg.bt_capital
    return dict(ret=round(final/cfg.bt_capital-1,4),sharpe=round(sharpe,3),
                maxdd=round(float(eq["dd"].min()) if not eq.empty and "dd" in eq.columns else 0,4),
                winrate=round(float((trd["pnl"]>0).mean()) if not trd.empty else 0,3),
                trades=len(trd),final=round(final,2),
                avg_ret=round(float(trd["ret"].mean()) if not trd.empty else 0,4),
                avg_bars=round(float(trd["bars"].mean()) if not trd.empty else 0,1),
                trades_df=trd)

# ══════════════════════════════════════════════════════════════════════════════
# §10  ALERT BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _sel_reason(row,hits,fund,ai,mkt)->str:
    parts=[]; e9=_g(row,"ema9"); e21=_g(row,"ema21"); e50=_g(row,"ema50")
    rsi=_g(row,"rsi14",50); vol=_g(row,"vol_ratio",1.0)
    mh=_g(row,"macd_h"); c=_g(row,"close"); adx=_g(row,"adx",0); stf=int(_g(row,"st_flip",0))
    if hits: parts.append(f"Primary: {hits[0][1]} ({hits[0][0]*100:.0f}% conf)")
    if stf==1: parts.append("⚡ SuperTrend just flipped BULLISH")
    if e9>e21>e50: parts.append(f"Full EMA alignment ({e9:.0f}>{e21:.0f}>{e50:.0f})")
    elif e9>e21: parts.append(f"EMA bullish ({e9:.0f}>{e21:.0f})")
    if rsi<35: parts.append(f"RSI={rsi:.1f} oversold")
    elif 45<rsi<65: parts.append(f"RSI={rsi:.1f} healthy")
    if mh>0: parts.append("MACD histogram positive")
    if adx>25: parts.append(f"ADX={adx:.0f} strong trend")
    if vol>=1.5: parts.append(f"Volume {vol:.1f}× avg")
    pe=fund.get("pe"); roe=fund.get("roe")
    if pe and pe>0: parts.append(f"P/E={pe:.1f}")
    if roe and roe>0: parts.append(f"ROE={roe*100:.1f}%")
    parts.append(f"Market: {mkt.get('label','N/A')} | {mkt.get('align','N/A')}")
    return "  •  ".join(parts[:7])

def build_alerts(feat:pd.DataFrame,nifty:dict,fund_cache:dict,cfg:Cfg)->tuple:
    latest=(feat.sort_values("date").groupby("symbol",sort=False).tail(1).reset_index(drop=True))
    results=[]; rej=defaultdict(int)
    for _,row in latest.iterrows():
        sym=str(row["symbol"]); c=float(row["close"]); sig=str(row.get("signal","NEUTRAL"))
        score=float(row.get("score",0)); atr=float(row.get("atr14",c*0.02) or c*0.02)
        atr_p=atr/c*100 if c else 0; avg_v=float(row.get("avg_vol20",0) or 0)
        tv=float(row.get("med_tv20",0) or 0)/1e7
        if sig!="LONG": continue
        if c<cfg.min_price:              rej["price"]+=1;  continue
        if atr_p<cfg.min_atr_pct*100:   rej["atr_lo"]+=1; continue
        if atr_p>cfg.max_atr_pct*100:   rej["atr_hi"]+=1; continue
        if avg_v<cfg.min_avg_vol:        rej["vol"]+=1;    continue
        if tv<cfg.min_traded_val_cr:     rej["tv"]+=1;     continue
        hits=detect_patterns(row,latest[latest["symbol"]==sym].tail(3))
        pc=pat_confidence(hits); cats=n_cats(hits)
        if cats<cfg.min_categories:      rej["cats"]+=1;   continue
        lvl=trade_levels(c,atr,cfg)
        if lvl is None:                  rej["rr"]+=1;     continue
        fund=fund_cache.get(sym,{}); ai=ai_confidence(row,fund,hits); mk=mkt_confidence(nifty)
        sel=_sel_reason(row,hits,fund,ai,mk)
        results.append(dict(
            symbol=sym,last_close=round(c,2),score=round(score,4),
            atr=round(atr,2),atr_pct=round(atr_p,2),
            rsi=round(float(row.get("rsi14",50) or 50),1),
            macd_h=round(float(row.get("macd_h",0) or 0),4),
            adx=round(float(row.get("adx",0) or 0),1),
            vol_ratio=round(float(row.get("vol_ratio",1) or 1),2),
            vol_z=round(float(row.get("vol_z",0) or 0),2),
            avg_vol=int(avg_v),traded_val_cr=round(tv,2),
            ema9=round(float(row.get("ema9",0) or 0),2),
            ema21=round(float(row.get("ema21",0) or 0),2),
            ema50=round(float(row.get("ema50",0) or 0),2),
            ema200=round(float(row.get("ema200",0) or 0),2),
            st_flip=int(_g(row,"st_flip",0)),
            is_fo=sym in _FO_SET,indices=symbol_tags(sym),
            sector=fund.get("sector","N/A"),industry=fund.get("indust","N/A"),
            pe=fund.get("pe"),pb=fund.get("pb"),roe=fund.get("roe"),
            mcap=fund.get("mcap"),w52h=fund.get("w52h"),w52l=fund.get("w52l"),
            beta=fund.get("beta"),
            hits=hits,pat_conf=pc,n_cats=cats,levels=lvl,ai=ai,mkt=mk,
            reason=sel,scan_ts=datetime.now().strftime("%Y-%m-%d %H:%M")))
    results.sort(key=lambda r:(-r["ai"]["ai_pct"],-abs(r["score"])))
    LOG.info("Alerts: %d passed | rej: %s",len(results),dict(rej))
    return results,dict(rej)

# ══════════════════════════════════════════════════════════════════════════════
# §11  SAVE OUTPUTS
# ══════════════════════════════════════════════════════════════════════════════

def save_all(alerts:list,bt:dict,nifty:dict,cfg:Cfg)->dict:
    od=cfg.output_dir; od.mkdir(parents=True,exist_ok=True)
    ts=datetime.now().strftime("%Y%m%d_%H%M")
    rows=[]
    for r in alerts:
        st=r["levels"]["short_term"]; lt=r["levels"]["long_term"]; ai=r["ai"]; mk=r["mkt"]
        rows.append({"scan_ts":r["scan_ts"],"symbol":r["symbol"],"last_close":r["last_close"],
                     "score":r["score"],"rsi":r["rsi"],"adx":r["adx"],"atr_pct":r["atr_pct"],
                     "vol_ratio":r["vol_ratio"],"avg_vol":r["avg_vol"],"traded_val_cr":r["traded_val_cr"],
                     "ai_pct":ai["ai_pct"],"mkt_pct":mk["pct"],
                     "pat_conf_pct":round(r["pat_conf"]*100,1),"n_cats":r["n_cats"],
                     "top_signal":r["hits"][0][1] if r["hits"] else "",
                     "st_entry":st["entry"],"st_target":st["tp"],"st_sl":st["sl"],"st_rr":st["rr"],
                     "lt_entry":lt["entry"],"lt_target":lt["tp"],"lt_sl":lt["sl"],"lt_rr":lt["rr"],
                     "is_fo":r["is_fo"],"indices":r["indices"],"sector":r["sector"],
                     "pe":r["pe"],"roe":r["roe"],"mcap":r["mcap"],"reason":r["reason"][:250]})
    alerts_p=od/f"alerts_{ts}.csv"
    pd.DataFrame(rows).to_csv(alerts_p,index=False)
    bt.get("trades_df",pd.DataFrame()).to_csv(od/f"trades_{ts}.csv",index=False)
    with open(od/f"summary_{ts}.json","w") as f:
        json.dump({"run_ts":ts,"nifty":{k:v for k,v in nifty.items() if k!="ts"},
                   "backtest":{k:v for k,v in bt.items() if k!="trades_df"},
                   "top10":[{"symbol":r["symbol"],"ai_pct":r["ai"]["ai_pct"],
                              "st":r["levels"]["short_term"],"lt":r["levels"]["long_term"]}
                             for r in alerts[:10]]},f,indent=2,default=str)
    return dict(alerts=alerts_p,output_dir=od)

# ══════════════════════════════════════════════════════════════════════════════
# §12  DISPLAY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _sparkline(prices: list, width: int = 28) -> str:
    B = "▁▂▃▄▅▆▇█"
    if len(prices) < 2:
        return "─" * width
    tail = prices[-width:]
    mn, mx = min(tail), max(tail)
    span   = mx - mn if mx != mn else 1
    chars  = [B[int((v - mn) / span * (len(B) - 1))] for v in tail]
    return " " * (width - len(chars)) + "".join(chars)

def _grade(pct: float) -> tuple:
    if pct >= 88: return "A+", "bold bright_green"
    if pct >= 78: return "A",  "bold green"
    if pct >= 68: return "B+", "bold yellow"
    if pct >= 58: return "B",  "bold yellow"
    if pct >= 48: return "C+", "bold red"
    return "C", "bold red"

def _gauge(pct: float, width: int = 18) -> str:
    f = max(0, min(int(pct / 100 * width), width))
    if pct >= 80:   col = "bold bright_green"
    elif pct >= 65: col = "green"
    elif pct >= 50: col = "yellow"
    else:           col = "red"
    return f"[{col}]{'█' * f}[/{col}][dim]{'░' * (width - f)}[/dim]  [{col}]{pct:.1f}%[/{col}]"

def _sc(s: float) -> str:
    if s >= 0.35: return "bold bright_green"
    if s >= 0.22: return "bold green"
    if s >= 0.10: return "yellow"
    return "dim"

def _rsi_r(r: float) -> str:
    if r < 30:  return f"[bold bright_green]{r:.1f}  ◀ OVERSOLD[/bold bright_green]"
    if r < 42:  return f"[green]{r:.1f}  ◀ Near Oversold[/green]"
    if r < 60:  return f"[yellow]{r:.1f}[/yellow]"
    if r < 75:  return f"[orange3]{r:.1f}  ▲ Elevated[/orange3]"
    return f"[bold red]{r:.1f}  ▲ OVERBOUGHT[/bold red]"

def _adx_r(a: float) -> str:
    if a >= 40: return f"[bold bright_green]{a:.0f}  ◆ Very Strong[/bold bright_green]"
    if a >= 28: return f"[green]{a:.0f}  ◆ Strong[/green]"
    if a >= 20: return f"[yellow]{a:.0f}  ◆ Moderate[/yellow]"
    return f"[red]{a:.0f}  ◆ Weak[/red]"

def _vol_r(v: float) -> str:
    if v >= 2.5: return f"[bold bright_green]{v:.2f}×  ▲ SURGE[/bold bright_green]"
    if v >= 1.5: return f"[green]{v:.2f}×  ▲ HIGH[/green]"
    if v >= 1.0: return f"[yellow]{v:.2f}×[/yellow]"
    return f"[red]{v:.2f}×  ▼ LOW[/red]"

def _pct_m(entry: float, target: float) -> str:
    if entry <= 0: return "—"
    pct = (target / entry - 1) * 100
    col = "bright_green" if pct >= 0 else "red"
    return f"[{col}]{pct:+.2f}%[/{col}]"

def _kelly(wr: float, aw: float, al: float) -> float:
    if al == 0: return 0.0
    b = aw / al
    k = wr - (1 - wr) / b if b > 0 else 0
    return round(min(max(k, 0), 0.25), 4)

def _sec_emoji(s: str) -> str:
    M = {"Technology": "💻", "Financial Services": "🏦", "Energy": "⚡",
         "Automobile": "🚗", "Infrastructure": "🏗️", "Consumer": "🛍️",
         "Healthcare": "🏥", "Materials": "⚙️", "Utilities": "💡",
         "Communication": "📡", "Real Estate": "🏢", "Industrial": "🏭"}
    for k, e in M.items():
        if k.lower() in str(s).lower():
            return e
    return "📊"

# ══════════════════════════════════════════════════════════════════════════════
# §13  SEPARATOR HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _rule_major(title: str = "") -> None:
    _con.print()
    _con.print(Rule(
        f"[bright_cyan]{'  ' + title + '  ' if title else ''}[/bright_cyan]",
        style="bright_cyan", characters="═"))
    _con.print()

def _rule_minor(title: str = "") -> None:
    _con.print(Rule(
        f"[dim cyan]{'  ' + title + '  ' if title else ''}[/dim cyan]",
        style="dim cyan", characters="─"))

def _rule_sub() -> None:
    _con.print(Rule(style="dim", characters="·"))

def _section_hdr(icon: str, title: str, sub: str = "", colour: str = "bright_cyan") -> None:
    _con.print()
    _con.print(Rule(characters="▀", style=colour))
    line = f"[bold {colour}]  {icon}  {title.upper()}  [/bold {colour}]"
    if sub:
        line += f"[dim]  ·  {sub}[/dim]"
    _con.print(Align.center(line))
    _con.print(Rule(characters="▄", style=colour))
    _con.print()

def _card_div(sym: str, rank: int, ai_p: float, score: float, ltr: str, lcol: str) -> None:
    _con.print()
    _con.print(Rule(characters="█", style="green"))
    _con.print(Rule(characters="▓", style="bright_green"))
    h = (f"[bold bright_green]  #{rank}  [/bold bright_green]"
         f"[bold bright_white on green]   {sym}   [/bold bright_white on green]"
         f"[bold bright_green]  ●  LONG SIGNAL  [/bold bright_green]"
         f"[dim green]│[/dim green]"
         f"  [bold cyan]AI Confidence: {ai_p:.1f}%[/bold cyan]"
         f"  [bold white]Composite Score: {score:+.4f}[/bold white]"
         f"  [{lcol}]Conviction Grade: {ltr}[/{lcol}]")
    _con.print(Align.center(h))
    _con.print(Rule(characters="▓", style="bright_green"))
    _con.print(Rule(characters="█", style="green"))
    _con.print()

def _sub_label(icon: str, label: str, colour: str = "bright_cyan") -> None:
    _con.print(f"\n  [{colour}]{icon}  {label}[/{colour}]")
    _con.print(f"  [dim]{'─' * 130}[/dim]")

# ══════════════════════════════════════════════════════════════════════════════
# §14  MARKET BANNER
# ══════════════════════════════════════════════════════════════════════════════

def render_market_banner(nifty: dict, bt: dict, alerts: list) -> None:
    _section_hdr("🌐", "Live Market Dashboard", colour="bright_cyan")

    trend = nifty.get("trend", 0); label = nifty.get("label", "N/A")
    last  = nifty.get("last", 0);  rsi_n  = nifty.get("rsi", 50)
    chg1m = nifty.get("chg_1m", 0); chg3m = nifty.get("chg_3m", 0)
    e9    = nifty.get("ema9", 0); e21 = nifty.get("ema21", 0); e50 = nifty.get("ema50", 0)

    if trend >= 0.7:    mc, mi = "bold bright_green", "🟢"
    elif trend >= 0.2:  mc, mi = "bold green",         "🟡"
    elif trend <= -0.7: mc, mi = "bold bright_red",    "🔴"
    elif trend <= -0.2: mc, mi = "bold red",            "🟠"
    else:               mc, mi = "bold yellow",         "⚪"

    rsent = (
        "[bold bright_green]OVERSOLD — High Reversal Probability[/bold bright_green]" if rsi_n < 30 else
        "[green]Recovering — Momentum Building[/green]"  if rsi_n < 45 else
        "[yellow]Neutral Zone[/yellow]"                   if rsi_n < 60 else
        "[orange3]Elevated — Caution[/orange3]"           if rsi_n < 75 else
        "[bold red]OVERBOUGHT — Pullback Risk[/bold red]"
    )
    if last > e9 > e21 > e50:    emas = "[bold bright_green]▲ Full Bullish Stack (9 > 21 > 50)[/bold bright_green]"
    elif last < e9 < e21 < e50:  emas = "[bold bright_red]▼ Full Bearish Stack (9 < 21 < 50)[/bold bright_red]"
    elif last > e21:              emas = "[yellow]◆ Mixed / Recovering Bias[/yellow]"
    else:                          emas = "[red]◆ Mixed / Weakening Bias[/red]"

    c1c = "bright_green" if chg1m >= 0 else "red"
    c3c = "bright_green" if chg3m >= 0 else "red"
    n_sig  = len(alerts)
    avg_ai = sum(r["ai"]["ai_pct"] for r in alerts) / max(n_sig, 1)
    n_fo   = sum(1 for r in alerts if r.get("is_fo"))
    top_s  = alerts[0]["symbol"] if alerts else "—"
    top_ai = alerts[0]["ai"]["ai_pct"] if alerts else 0

    bt_ret = bt.get("ret", 0); bt_sh = bt.get("sharpe", 0)
    bt_dd  = bt.get("maxdd", 0); bt_wr = bt.get("winrate", 0)
    bt_tr  = bt.get("trades", 0); bt_ar = bt.get("avg_ret", 0)
    bc  = "bright_green" if bt_ret >= 0 else "red"
    sc_ = "bright_green" if bt_sh > 1 else "yellow" if bt_sh > 0 else "red"
    wc  = "bright_green" if bt_wr > 0.5 else "red"
    now = datetime.now().strftime("%d %b %Y  %H:%M IST")

    t = Table(box=rbox.SIMPLE_HEAD, expand=True, padding=(0, 3),
              show_header=False, border_style="dim cyan")
    t.add_column("N",  ratio=3)
    t.add_column("S1", width=1, style="dim")
    t.add_column("B",  ratio=3)
    t.add_column("S2", width=1, style="dim")
    t.add_column("R",  ratio=3)

    mkt = (
        f"[{mc}]{mi}  NIFTY 50   ₹{last:,.2f}[/{mc}]\n\n"
        f"  [dim]Trend    [/dim]  [{mc}]{label}[/{mc}]\n"
        f"  [dim]RSI      [/dim]  {rsent}\n"
        f"  [dim]EMA Stack[/dim]  {emas}\n"
        f"  [dim]Levels   [/dim]  [dim]EMA9 {e9:.1f}  ·  EMA21 {e21:.1f}  ·  EMA50 {e50:.1f}[/dim]\n"
        f"  [dim]1M Change[/dim]  [{c1c}]{chg1m:+.2f}%[/{c1c}]"
        f"      [dim]3M Change[/dim]  [{c3c}]{chg3m:+.2f}%[/{c3c}]"
    )
    bkt = (
        f"[bold bright_magenta]📊  BACKTEST SUMMARY (8-Month)[/bold bright_magenta]\n\n"
        f"  [dim]Return      [/dim]  [{bc}]{bt_ret:+.2%}[/{bc}]\n"
        f"  [dim]Sharpe Ratio[/dim]  [{sc_}]{bt_sh:.3f}[/{sc_}]\n"
        f"  [dim]Max Drawdown[/dim]  [bold red]{bt_dd:.2%}[/bold red]\n"
        f"  [dim]Win Rate    [/dim]  [{wc}]{bt_wr:.1%}[/{wc}]\n"
        f"  [dim]Trade Count [/dim]  {bt_tr}      [dim]Avg Return[/dim]  [{bc}]{bt_ar:+.2%}[/{bc}]"
    )
    scn = (
        f"[bold bright_yellow]🔍  SCAN RESULTS[/bold bright_yellow]  [dim]{now}[/dim]\n\n"
        f"  [dim]Signals Found[/dim]  [bold bright_green]{n_sig}[/bold bright_green] LONG alerts\n"
        f"  [dim]F&O Eligible [/dim]  [bold cyan]{n_fo}[/bold cyan] of {n_sig} stocks\n"
        f"  [dim]Avg AI Score [/dim]  [bold yellow]{avg_ai:.1f}%[/bold yellow]\n"
        f"  [dim]Top Pick     [/dim]  [bold bright_white]{top_s}[/bold bright_white]"
        f"   [dim]AI Score[/dim]  [bold yellow]{top_ai:.1f}%[/bold yellow]"
    )

    t.add_row(mkt, "[dim]│[/dim]", bkt, "[dim]│[/dim]", scn)
    _con.print(Panel(t, border_style="bright_cyan", padding=(1, 2),
                     title="[bold bright_cyan]  ── LIVE MARKET STATUS ──  [/bold bright_cyan]"))

# ══════════════════════════════════════════════════════════════════════════════
# §15  SECTOR HEATMAP
# ══════════════════════════════════════════════════════════════════════════════

def render_sector_heatmap(alerts: list) -> None:
    _section_hdr("🗺️", "Sector Signal Heatmap", colour="bright_yellow")

    gs: dict[str, dict] = {}
    for r in alerts:
        for tag in r["indices"].split(" · "):
            tag = tag.strip()
            if not tag or tag == "—":
                continue
            d = gs.setdefault(tag, {"count": 0, "ai": [], "syms": []})
            d["count"] += 1
            d["ai"].append(r["ai"]["ai_pct"])
            d["syms"].append(r["symbol"])

    if not gs:
        _con.print("[dim]  No sector data.[/dim]\n")
        return

    GF = {
        "N50L": "Nifty 50 Leaders",   "N50":  "Nifty 50",
        "NN50": "Nifty Next 50",       "MC100":"Nifty Midcap 100",
        "SC250":"Nifty Smallcap 250",  "BNK":  "Nifty Bank",
        "IT":   "Nifty IT",            "NRG":  "Nifty Energy",
        "AUTO": "Nifty Auto",          "INFRA":"Nifty Infra",
        "F&O":  "F&O Stocks",
    }
    GI = {
        "N50L":"👑","N50":"📊","NN50":"🔵","MC100":"🟡","SC250":"🟠",
        "BNK":"🏦","IT":"💻","NRG":"⚡","AUTO":"🚗","INFRA":"🏗️","F&O":"🔰",
    }

    t = Table(box=rbox.ROUNDED, expand=True, header_style="bold bright_yellow",
              border_style="yellow", show_lines=True, padding=(0, 1))
    t.add_column("",          width=3,  justify="center")
    t.add_column("Index Group", width=22, style="bold white")
    t.add_column("Signals",   width=9,  justify="center")
    t.add_column("Avg AI Score", width=32)
    t.add_column("Signal Heat", width=28)
    t.add_column("Top Picks (Bold = Strongest)",  min_width=40)

    for tag in ["N50L","N50","NN50","MC100","SC250","BNK","IT","NRG","AUTO","INFRA","F&O"]:
        ico  = GI.get(tag, "📊")
        full = GF.get(tag, tag)
        if tag not in gs:
            t.add_row(ico, f"[dim]{full}[/dim]", "[dim]0[/dim]",
                      "[dim]No signals today[/dim]",
                      "[dim]░░░░░░░░░░░░░░░░░░░░[/dim]", "[dim]—[/dim]")
            continue

        d   = gs[tag]; cnt = d["count"]; avg = sum(d["ai"]) / cnt
        if   avg >= 80 and cnt >= 3: hc, hi = "bold bright_green", "🔥"
        elif avg >= 70 or cnt >= 3:  hc, hi = "bold green",         "✅"
        elif avg >= 55 or cnt >= 2:  hc, hi = "yellow",             "🟡"
        else:                         hc, hi = "dim",                "⬜"

        bw     = 20
        filled = max(1, int(min(cnt / max(len(alerts) * 0.3, 1), 1.0) * bw))
        heat   = f"[{hc}]{'█' * filled}[/{hc}][dim]{'░' * (bw - filled)}[/dim]   [{hc}]{avg:.1f}%[/{hc}]"
        syms   = "  ".join(f"[bold bright_white]{s}[/bold bright_white]"
                            for s in d["syms"][:6])
        if len(d["syms"]) > 6:
            syms += f"  [dim]+ {len(d['syms'])-6} more[/dim]"

        t.add_row(
            f"{hi}", f"[bold]{full}[/bold]",
            f"[{hc}]{cnt}[/{hc}]",
            _gauge(avg, 18), heat, syms,
        )
    _con.print(t)

# ══════════════════════════════════════════════════════════════════════════════
# §16  PER-STOCK PRO CARD SECTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _narrative(r: dict) -> str:
    sym  = r["symbol"]; hits = r.get("hits", [])
    ai   = r["ai"]; mk = r["mkt"]
    st   = r["levels"]["short_term"]; lt = r["levels"]["long_term"]
    rsi  = r["rsi"]; adx = r["adx"]; vol = r["vol_ratio"]; atr = r["atr_pct"]
    score = r["score"]; stf = r.get("st_flip", 0)
    e9 = r["ema9"]; e21 = r["ema21"]; e50 = r["ema50"]
    sig = (hits[0][1].lstrip("🟢📈⚡🔀🚀🌟⭐🏆🕯️🔨↩️📊🔊🏔️💡💥🏄📌🌅⚔️").strip()
           if hits else "composite setup")

    ema_w  = ("strong uptrend"      if e9 > e21 > e50 else
              "recovering trend"    if e9 > e21       else "developing base")
    rsi_w  = ("oversold territory — historically a high-probability mean-reversion zone" if rsi < 35 else
              "neutral-to-positive momentum with room to extend"                          if rsi < 58 else
              "building momentum though nearing elevated readings")
    vol_w  = (f"significantly elevated at {vol:.2f}× the 20-day average — institutional accumulation signal"
              if vol >= 1.8 else
              f"above-average at {vol:.2f}× the 20-day average"
              if vol >= 1.2 else
              f"near-average volume ({vol:.2f}×) — no unusual activity")
    st_w   = ("Notably, the [bold bright_green]SuperTrend indicator just flipped to bullish[/bold bright_green] — "
              "a high-conviction buy confirmation signal. " if stf else "")

    return (
        f"[bold bright_white]{sym}[/bold bright_white] is exhibiting a "
        f"[italic bright_cyan]{sig}[/italic bright_cyan] setup within a [bold]{ema_w}[/bold] "
        f"(EMA9 = [bold cyan]{e9:.2f}[/bold cyan]  /  EMA21 = [bold cyan]{e21:.2f}[/bold cyan]  /  "
        f"EMA50 = [bold cyan]{e50:.2f}[/bold cyan]). "
        f"RSI stands at [bold]{rsi:.1f}[/bold], in {rsi_w}. "
        f"The latest session recorded {vol_w}. {st_w}"
        f"ADX = [bold]{adx:.1f}[/bold] — "
        f"{'[green]confirms trend strength sufficient to carry the move[/green]' if adx > 25 else '[yellow]trend is still establishing itself[/yellow]'}. "
        f"ATR = [bold]{atr:.2f}%[/bold] of price — "
        f"{'[green]ideal swing-trade volatility band[/green]' if 1.5 < atr < 6 else '[yellow]elevated — consider smaller position size[/yellow]'}. "
        f"Broader market backdrop: [bold]{mk.get('label','N/A')}[/bold] ({mk.get('align','N/A')}). "
        f"Composite model score [bold bright_yellow]{score:+.4f}[/bold bright_yellow]  "
        f"·  AI conviction [bold bright_cyan]{ai['ai_pct']:.1f}%[/bold bright_cyan].  "
        f"Short-term target [bold bright_green]₹{st['tp']}[/bold bright_green] "
        f"(Stop [bold red]₹{st['sl']}[/bold red], R:R [bold]{st['rr_str']}[/bold])  ·  "
        f"Swing target [bold bright_green]₹{lt['tp']}[/bold bright_green] "
        f"(Stop [bold red]₹{lt['sl']}[/bold red], R:R [bold]{lt['rr_str']}[/bold])."
    )


def _quick_stats(r: dict, spark: str) -> Table:
    c_   = r["last_close"]; sym = r["symbol"]; fo = r["is_fo"]; sec = str(r["sector"])
    mcap = r.get("mcap"); pe = r.get("pe"); roe = r.get("roe"); beta = r.get("beta")
    w52h = r.get("w52h"); w52l = r.get("w52l")
    e9 = r["ema9"]; e21 = r["ema21"]; e50 = r["ema50"]; e200 = r["ema200"]
    ec  = "bright_green" if c_ > e9 > e21 > e50 else "yellow" if c_ > e21 else "red"
    pec = "bright_green" if pe and pe < 20 else "green" if pe and pe < 30 else "yellow" if pe and pe < 45 else "red"
    rec = "bright_green" if roe and roe > 0.18 else "green" if roe and roe > 0.10 else "red"

    t = Table(box=rbox.SIMPLE_HEAD, expand=True, padding=(0, 2),
              show_header=False, border_style="dim bright_cyan")
    t.add_column("Label", style="dim",         ratio=1)
    t.add_column("Value", style="bold white",  ratio=2)
    t.add_column("Label", style="dim",         ratio=1)
    t.add_column("Value", style="bold white",  ratio=2)
    t.add_column("Label", style="dim",         ratio=1)
    t.add_column("Value", style="bold white",  ratio=2)

    t.add_row(
        "Symbol",
        f"[bold bright_white on dark_green]   {sym}   [/bold bright_white on dark_green]",
        "Last Traded Price",
        f"[bold bright_cyan]₹{c_:,.2f}[/bold bright_cyan]",
        "Sector",
        f"{_sec_emoji(sec)}  {sec}",
    )
    t.add_row(
        "20-Bar Sparkline",
        f"[{ec}]{spark}[/{ec}]",
        "Index Groups",
        f"[dim]{r['indices']}[/dim]",
        "F&O Eligible",
        "[bold bright_green]✅  YES[/bold bright_green]" if fo else "[dim]✗  No[/dim]",
    )
    t.add_row(
        "RSI (14)",         _rsi_r(r["rsi"]),
        "ADX Strength",     _adx_r(r["adx"]),
        "Volume Ratio",     _vol_r(r["vol_ratio"]),
    )
    t.add_row(
        "ATR %",
        f"[{'green' if 1.5<r['atr_pct']<4.5 else 'yellow'}]{r['atr_pct']:.2f}%  "
        f"{'✓ Ideal' if 1.5<r['atr_pct']<4.5 else '⚠ High'}[/{'green' if 1.5<r['atr_pct']<4.5 else 'yellow'}]",
        "MACD Histogram",
        f"[{'bright_green' if r['macd_h']>0 else 'red'}]{r['macd_h']:+.4f}  "
        f"({'Rising' if r['macd_h']>0 else 'Falling'})[/{'bright_green' if r['macd_h']>0 else 'red'}]",
        "Composite Score",
        f"[{_sc(r['score'])}]{r['score']:+.4f}[/{_sc(r['score'])}]",
    )
    t.add_row(
        "EMA 9 / 21",
        f"[{ec}]{e9:.2f}[/{ec}]  /  [{ec}]{e21:.2f}[/{ec}]",
        "EMA 50 / 200",
        (f"[{ec}]{e50:.2f}[/{ec}]  /  [dim]{e200:.2f}[/dim]"
         if e200 else f"[{ec}]{e50:.2f}[/{ec}]  /  [dim]N/A[/dim]"),
        "ATR (₹ absolute)",
        f"[dim]₹{r['atr']:.2f}[/dim]",
    )
    t.add_row(
        "P/E Ratio",
        f"[{pec}]{pe:.1f}[/{pec}]" if pe else "[dim]N/A[/dim]",
        "ROE",
        f"[{rec}]{roe*100:.1f}%[/{rec}]" if roe else "[dim]N/A[/dim]",
        "Market Cap / Beta",
        f"[dim]₹{mcap:,.0f} Cr  /  β {beta:.2f}[/dim]" if mcap and beta else "[dim]N/A[/dim]",
    )
    t.add_row(
        "52-Week High",
        f"[dim]₹{w52h:,.2f}[/dim]" if w52h else "[dim]—[/dim]",
        "52-Week Low",
        f"[dim]₹{w52l:,.2f}[/dim]" if w52l else "[dim]—[/dim]",
        "Median Traded Value",
        f"[green]₹{r['traded_val_cr']:.2f} Cr / day[/green]",
    )
    return t


def _price_levels(r: dict) -> Table:
    st  = r["levels"]["short_term"]; lt = r["levels"]["long_term"]
    c_  = r["last_close"]; atr = r["atr"]
    # Dip entry: −2% from current price
    dip_e  = round(c_ * 0.98, 2)
    dip_sl = round(dip_e - atr * 1.2, 2)
    dip_tp = round(dip_e + atr * 2.5, 2)
    dip_ri = round(dip_e - dip_sl, 2)
    dip_rw = round(dip_tp - dip_e, 2)
    dip_rr = round(dip_rw / dip_ri, 2) if dip_ri > 0 else 0

    t = Table(box=rbox.DOUBLE, expand=True,
              header_style="bold bright_cyan", border_style="bright_cyan",
              show_lines=True, padding=(0, 1))
    for col, w, jus in [
        ("Trade Scenario",      28, "left"),
        ("Entry Price ₹",       14, "right"),
        ("Target Price ₹",      14, "right"),
        ("Stop Loss ₹",         14, "right"),
        ("Risk per Share ₹",    16, "right"),
        ("Reward per Share ₹",  18, "right"),
        ("Risk : Reward",       12, "center"),
        ("% Move to Target",    16, "right"),
        ("Holding Window",      22, "left"),
    ]:
        t.add_column(col, width=w, justify=jus)

    t.add_row(
        "[bold bright_yellow]⚡  Aggressive Entry (Short-Term)[/bold bright_yellow]",
        f"[bold cyan]₹{st['entry']:,.2f}[/bold cyan]",
        f"[bold bright_green]₹{st['tp']:,.2f}[/bold bright_green]",
        f"[bold red]₹{st['sl']:,.2f}[/bold red]",
        f"[red]₹{st['risk']:.2f}[/red]",
        f"[green]₹{st['reward']:.2f}[/green]",
        f"[bold white]{st['rr_str']}[/bold white]",
        _pct_m(st["entry"], st["tp"]),
        "[yellow]2 – 5 trading days[/yellow]",
    )
    t.add_row(
        "[bold bright_green]📅  Swing Entry (Long-Term)[/bold bright_green]",
        f"[bold cyan]₹{lt['entry']:,.2f}[/bold cyan]",
        f"[bold bright_green]₹{lt['tp']:,.2f}[/bold bright_green]",
        f"[bold red]₹{lt['sl']:,.2f}[/bold red]",
        f"[red]₹{lt['risk']:.2f}[/red]",
        f"[bright_green]₹{lt['reward']:.2f}[/bright_green]",
        f"[bold white]{lt['rr_str']}[/bold white]",
        _pct_m(lt["entry"], lt["tp"]),
        "[green]10 – 20 trading days[/green]",
    )
    t.add_row(
        "[dim]📌  Limit / Dip Entry (−2% from current)[/dim]",
        f"[dim cyan]₹{dip_e:,.2f}[/dim cyan]",
        f"[dim green]₹{dip_tp:,.2f}[/dim green]",
        f"[dim red]₹{dip_sl:,.2f}[/dim red]",
        f"[dim]₹{dip_ri:.2f}[/dim]",
        f"[dim]₹{dip_rw:.2f}[/dim]",
        f"[dim]1 : {dip_rr}[/dim]",
        _pct_m(dip_e, dip_tp),
        "[dim]Limit order placed at −2%[/dim]",
    )
    return t


def _sizing(r: dict, capital: float) -> Table:
    st  = r["levels"]["short_term"]; c_ = r["last_close"]; sl = st["risk"]
    kf  = _kelly(0.55, 0.06, 0.04)

    rows_data = [
        ("1% Portfolio Risk Rule",      max(1, int(capital * 0.01 / sl)) if sl else 0, "green"),
        ("2% Portfolio Risk Rule",      max(1, int(capital * 0.02 / sl)) if sl else 0, "yellow"),
        (f"Half-Kelly Criterion ({kf:.1%})",
         max(1, int(capital * kf / c_)) if c_ else 0,  "cyan"),
        ("Fixed 20% Capital Allocation",int(capital * 0.20 / c_) if c_ else 0,  "dim"),
    ]

    t = Table(box=rbox.DOUBLE, expand=True,
              header_style="bold bright_magenta", border_style="bright_magenta",
              show_lines=True, padding=(0, 1))
    for col, w, jus in [
        ("Position Sizing Rule",    28, "left"),
        ("Shares (Qty)",            14, "right"),
        ("Capital Deployed",        18, "right"),
        ("Maximum Loss at SL",      20, "right"),
        ("Target Profit (ST)",      20, "right"),
        ("% of Total Portfolio",    20, "right"),
    ]:
        t.add_column(col, width=w, justify=jus)

    for lbl, qty, col in rows_data:
        inv  = qty * c_; ml = qty * sl
        tp_  = qty * st["reward"]; pct = inv / capital * 100
        pc   = "bright_green" if pct <= 20 else "yellow" if pct <= 30 else "red"
        t.add_row(
            f"[bold {col}]{lbl}[/bold {col}]",
            f"[bold cyan]{qty:,}[/bold cyan]",
            f"[white]₹{inv:>14,.2f}[/white]",
            f"[bold red]₹{ml:>12,.2f}[/bold red]",
            f"[bold bright_green]₹{tp_:>12,.2f}[/bold bright_green]",
            f"[{pc}]{pct:.1f}%[/{pc}]",
        )
    return t


def _factor_tbl(r: dict) -> Table:
    ai     = r["ai"]
    pull_s = 0.75 if (
        abs(r.get("ema21", 0) - r["last_close"]) / max(r.get("ema21", 1), 1) < 0.025
        and r["last_close"] > r.get("ema50", 0)
        and 40 < r["rsi"] < 62
    ) else 0.0

    W = {"Trend":0.24,"Momentum":0.16,"Breakout":0.17,"Pullback":0.11,
         "Volume":0.10,"Pattern":0.10,"Fundamental":0.08,"Sentiment":0.04}

    facts = [
        ("📈  Trend",        ai.get("trend_s",  0), 0.24, "Relative EMA positioning — how many of all 4 EMAs are bullishly stacked"),
        ("⚡  Momentum",     ai.get("mom_s",    0), 0.16, "RSI(14) normalised around 50  +  MACD histogram magnitude / ATR scaled"),
        ("🚀  Breakout",     ai.get("brk_s",    0), 0.17, "Distance above 20-day high × volume ratio — measures force of breakout"),
        ("📐  Pullback",     pull_s,                0.11, "Clean retest of slow EMA inside an uptrend — high-quality entry timing"),
        ("🔊  Volume",       ai.get("vol_s",    0), 0.10, "Volume ratio Z-score vs 20-day average — smart-money participation proxy"),
        ("🕯️   Pattern",     ai.get("pat_s",    0), 0.10, "Candlestick + price-action hit density scored across all 8 categories"),
        ("🏦  Fundamental",  ai.get("fund_s",   0), 0.08, "P/E · ROE · EPS growth · Revenue growth · Debt/Equity scoring overlay"),
        ("📡  Sentiment",    ai.get("sent_s",   0), 0.04, "5-day + 20-day price return used as a price-momentum sentiment proxy"),
    ]

    t = Table(box=rbox.DOUBLE, expand=True,
              header_style="bold bright_cyan", border_style="cyan",
              show_lines=True, padding=(0, 1))
    for col, w, jus in [
        ("Factor",          16, "left"),
        ("Score",           8,  "right"),
        ("Direction",       16, "center"),
        ("Weighted Contribution", 20, "center"),
        ("Weight",          8,  "right"),
        ("Score Bar",       22, "left"),
        ("What This Factor Measures", 46, "left"),
    ]:
        t.add_column(col, width=w, justify=jus,
                     style="" if col != "What This Factor Measures" else "dim")

    for name, sc, wt, desc in facts:
        pct    = (float(sc) + 1) / 2 * 100
        bw     = 18; filled = max(0, min(int(pct / 100 * bw), bw))
        if   sc >= 0.50: fc, ds = "bold bright_green", "[bold bright_green]▲▲  STRONG BULLISH[/bold bright_green]"
        elif sc >= 0.20: fc, ds = "green",              "[bold green]▲  BULLISH[/bold green]"
        elif sc >= 0.05: fc, ds = "yellow",             "[yellow]◆  Mild Bullish[/yellow]"
        elif sc >= -0.05:fc, ds = "dim",                "[dim]─  NEUTRAL[/dim]"
        elif sc >= -0.20:fc, ds = "orange3",            "[orange3]▽  Mild Bearish[/orange3]"
        else:             fc, ds = "bold red",           "[bold red]▼▼  STRONG BEARISH[/bold red]"
        bar = f"[{fc}]{'█' * filled}[/{fc}][dim]{'░' * (bw - filled)}[/dim]"
        contrib_val = sc * wt
        cc = "bright_green" if contrib_val > 0 else "red" if contrib_val < 0 else "dim"
        t.add_row(
            name,
            f"[{fc}]{sc:+.3f}[/{fc}]",
            ds,
            f"[{cc}]{contrib_val:+.4f}[/{cc}]",
            f"[dim]{wt:.0%}[/dim]",
            bar,
            desc,
        )
    return t


def _confidence_trio(r: dict) -> Panel:
    ai  = r["ai"]; mk = r["mkt"]
    ai_p = ai["ai_pct"]; mk_p = mk["pct"]; pt_p = r["pat_conf"] * 100
    ltr, col = _grade(ai_p)
    body = Text()
    body.append("  ┌─── Confidence Scores ──────────────────────────────────────────────────────────\n", "dim")
    body.append(f"  │  🤖  AI Confidence        {_gauge(ai_p, 20)}\n", "bold cyan")
    body.append(
        f"  │       Trend:[cyan]{ai['trend_s']:+.3f}[/cyan]   Mom:[cyan]{ai['mom_s']:+.3f}[/cyan]"
        f"   Brk:[cyan]{ai['brk_s']:+.3f}[/cyan]   Vol:[cyan]{ai['vol_s']:+.3f}[/cyan]"
        f"   Fund:[cyan]{ai['fund_s']:+.3f}[/cyan]   Pattern:[cyan]{ai['pat_s']:+.3f}[/cyan]\n",
        "dim",
    )
    body.append("  │\n", "dim")
    body.append(f"  │  📊  Market Confidence     {_gauge(mk_p, 20)}\n", "bold blue")
    body.append(
        f"  │       {mk['label']}   Nifty50 ₹{mk['nifty_last']:.2f}   "
        f"1M Change:[{'green' if mk['chg_1m']>=0 else 'red'}]{mk['chg_1m']:+.2f}%[/{'green' if mk['chg_1m']>=0 else 'red'}]"
        f"   RSI:{mk['rsi']:.1f}   Alignment:[bold]{mk['align']}[/bold]\n",
        "dim",
    )
    body.append("  │\n", "dim")
    body.append(f"  │  🎯  Pattern Confidence    {_gauge(pt_p, 20)}\n", "bold green")
    body.append(
        f"  │       [green]{len(r['hits'])}[/green] signals detected"
        f"   across [green]{r['n_cats']}[/green] distinct categories   "
        f"Top: [white]{r['hits'][0][1] if r['hits'] else '—'}[/white]\n",
        "dim",
    )
    if r.get("st_flip"):
        body.append("  │\n", "dim")
        body.append("  │  ⚡  [bold bright_green]SuperTrend just FLIPPED to BULLISH on this bar — high-conviction confirmation![/bold bright_green]\n")
    body.append("  └────────────────────────────────────────────────────────────────────────────────\n", "dim")
    return Panel(
        body,
        title=f"[bold]📈  Confidence Dashboard[/bold]   [{col}]AI Conviction Grade: {ltr}[/{col}]",
        border_style="cyan", padding=(0, 0),
    )


def _pattern_panel(r: dict) -> Panel:
    hits = r["hits"]
    body = Text()
    body.append("  ┌─── All Detected Signals ───────────────────────────────────────────────────────\n", "dim")
    cm: dict[str, list] = defaultdict(list)
    for sc, lb, cat in hits[:18]:
        cm[cat].append((sc, lb))
    for cat, items in cm.items():
        col = CAT_COL.get(cat, "white")
        body.append(f"  │  [ {cat.upper()} ]\n", f"bold {col}")
        for sc, lb in items:
            bar = f"[{col}]{'█' * int(sc * 10)}[/{col}][dim]{'░' * (10 - int(sc * 10))}[/dim]"
            body.append(f"  │      {lb}\n", "bright_white")
            body.append(f"  │      {bar}  [{col}]{sc * 100:.0f}% confidence[/{col}]\n")
    body.append(
        f"  │\n  │  [bold]Summary:[/bold]  [bright_white]{len(hits)}[/bright_white] total signals"
        f"  ·  [bright_white]{r['n_cats']}[/bright_white] distinct categories activated\n",
        "dim",
    )
    body.append("  └────────────────────────────────────────────────────────────────────────────────\n", "dim")
    return Panel(
        body,
        title="[bold bright_green]🎯  Pattern & Signal Hits[/bold bright_green]",
        border_style="bright_green", padding=(0, 0),
    )


def _conviction_scorecard(r: dict) -> Panel:
    ai  = r["ai"]; mk = r["mkt"]; cats = r["n_cats"]; stf = r.get("st_flip", 0)
    gd  = [
        ("🤖  AI Model",          r["ai"]["ai_pct"]),
        ("📊  Market Alignment",  r["mkt"]["pct"]),
        ("🎯  Pattern Strength",  r["pat_conf"] * 100),
        ("📈  Trend Quality",     (ai.get("trend_s", 0) + 1) / 2 * 100),
        ("🔊  Volume Quality",    (ai.get("vol_s",   0) + 1) / 2 * 100),
        ("🏦  Fundamental Score", (ai.get("fund_s",  0) + 1) / 2 * 100),
    ]
    body = Text()
    body.append("  ┌─── Dimension Grades ───────────────────────────────────────────────────────────\n", "dim cyan")
    for name, pct in gd:
        ltr, col = _grade(pct)
        body.append(f"  │  {name:<30}", "dim")
        body.append(f"[{col}]{ltr:<4}[/{col}]  ")
        body.append(f"{_gauge(pct, 18)}\n")
    body.append("  ├─── Positive Signals ───────────────────────────────────────────────────────────\n", "dim green")
    bonuses = []
    if stf:                    bonuses.append(("🟩", "SuperTrend just flipped BULLISH — rare buy signal"))
    if cats >= 4:              bonuses.append(("📊", f"{cats} distinct pattern categories — strong signal breadth"))
    if r["is_fo"]:             bonuses.append(("🔰", "F&O eligible — position can be hedged with options/futures"))
    if r["adx"] > 30:          bonuses.append(("💪", f"ADX = {r['adx']:.0f} — trend strength well above 30 threshold"))
    if r["vol_ratio"] >= 2:    bonuses.append(("🔊", f"Volume {r['vol_ratio']:.2f}× above average — institutional footprint"))
    if mk.get("trend", 0) >= 0.5: bonuses.append(("🐂", "Nifty50 is bullish — market tailwind supporting trade direction"))
    if r["rsi"] < 35:          bonuses.append(("🟢", f"RSI = {r['rsi']:.1f} deeply oversold — high-probability mean reversion"))
    for ico, txt in bonuses[:6]:
        body.append(f"  │  {ico}  ", "green")
        body.append(f"{txt}\n", "bright_white")
    body.append("  ├─── Risk Flags ─────────────────────────────────────────────────────────────────\n", "dim yellow")
    risks = []
    if mk.get("trend", 0) <= -0.5: risks.append(("⚠️", "Bear market headwind — trade is counter-trend to broader Nifty"))
    if r["atr_pct"] > 5:            risks.append(("⚠️", f"ATR = {r['atr_pct']:.1f}% is elevated — reduce position size accordingly"))
    if r.get("rsi", 50) > 72:       risks.append(("⚠️", f"RSI = {r.get('rsi',50):.1f} — overbought, consider waiting for pullback entry"))
    if not r["is_fo"]:              risks.append(("ℹ️",  "Not F&O listed — cash-only position, no derivative hedge available"))
    for ico, txt in (risks or [("✅", "No major risk flags detected — relatively clean setup")]):
        c2 = "yellow" if ico.startswith("⚠️") else "dim green" if ico.startswith("✅") else "dim"
        body.append(f"  │  {ico}  ", c2)
        body.append(f"{txt}\n", c2)
    overall = sum(v for _, v in gd) / len(gd)
    ltr_o, col_o = _grade(overall)
    body.append("  └────────────────────────────────────────────────────────────────────────────────\n", "dim cyan")
    return Panel(
        body,
        title=f"[bold]📋  Conviction Scorecard[/bold]   [{col_o}]Overall Grade: {ltr_o}  ({overall:.1f}%)[/{col_o}]",
        border_style="bright_cyan", padding=(0, 0),
    )


def render_pro_card(r: dict, rank: int, feat_df: Optional[pd.DataFrame], capital: float) -> None:
    sym  = r["symbol"]; ai_p = r["ai"]["ai_pct"]; score = r["score"]
    ltr, lcol = _grade(ai_p)

    _card_div(sym, rank, ai_p, score, ltr, lcol)

    # Sparkline
    spark = "─" * 28
    if feat_df is not None and not feat_df.empty:
        if all(c in feat_df.columns for c in ("close", "symbol", "date")):
            sub = feat_df[feat_df["symbol"] == sym].sort_values("date")["close"].tolist()
            if sub:
                spark = _sparkline(sub, 28)

    _sub_label("📊", "STOCK OVERVIEW  ·  All Key Metrics at a Glance", "bright_cyan")
    _con.print(Panel(
        _quick_stats(r, spark),
        title=(f"[bold bright_white]  ●  {sym}  [/bold bright_white]"
               f"[dim]  {r['indices']}  ·  {r.get('industry','N/A')}[/dim]"),
        border_style="bright_cyan", padding=(0, 0),
    ))

    _rule_sub()
    _sub_label("📝", "ANALYST NARRATIVE  ·  Quantitative Research Summary", "bright_white")
    _con.print(Panel(
        Padding(_narrative(r), (0, 2)),
        title="[bold dim]  Research Note  [/bold dim]",
        border_style="dim", padding=(0, 0),
    ))

    _rule_sub()
    _sub_label("📈", "CONFIDENCE DASHBOARD  ·  AI  ·  Market  ·  Pattern", "cyan")
    _con.print(_confidence_trio(r))

    _rule_sub()
    _sub_label("📐", "TRADE SCENARIOS  ·  Entry  ·  Target  ·  Stop Loss", "bright_cyan")
    _con.print(Panel(
        _price_levels(r),
        title="[bold bright_cyan]  Entry / Target / Stop Loss  ─  Three Trade Scenarios  [/bold bright_cyan]",
        border_style="bright_cyan", padding=(0, 0),
    ))

    _rule_sub()
    _sub_label("💰", "POSITION SIZING  ·  Risk Management  ·  Kelly Criterion", "bright_magenta")
    _con.print(Panel(
        _sizing(r, capital),
        title=f"[bold bright_magenta]  Position Sizing Calculator  ·  Portfolio: ₹{capital/1e5:.1f}L  [/bold bright_magenta]",
        border_style="bright_magenta", padding=(0, 0),
    ))

    _rule_sub()
    _sub_label("🧮", "8-FACTOR SIGNAL BREAKDOWN  ·  Score Decomposition", "cyan")
    _con.print(Panel(
        _factor_tbl(r),
        title="[bold cyan]  8-Factor Quantitative Signal Decomposition with Weighted Contributions  [/bold cyan]",
        border_style="cyan", padding=(0, 0),
    ))

    _rule_sub()
    _sub_label("🎯  +  📋", "PATTERN HITS  ·  CONVICTION SCORECARD", "bright_green")
    _con.print(Columns([_pattern_panel(r), _conviction_scorecard(r)], expand=True))

    _con.print()
    _con.print(Rule(characters="▁", style="dim green"))

# ══════════════════════════════════════════════════════════════════════════════
# §17  WATCHLIST DIGEST  (split into two tables to prevent column truncation)
# ══════════════════════════════════════════════════════════════════════════════

def render_watchlist(alerts: list, nifty: dict, bt: dict) -> None:
    _section_hdr("⭐", "Top 10 Watchlist Digest", colour="bright_yellow")

    nc = "bright_green" if nifty.get("trend",0) >= 0.3 else "red" if nifty.get("trend",0) <= -0.3 else "yellow"
    _con.print(
        f"  [bold {nc}]Nifty50  ₹{nifty.get('last',0):,.2f}[/bold {nc}]"
        f"  [dim]{nifty.get('label','N/A')}[/dim]"
        f"  RSI: [{'bright_green' if nifty.get('rsi',50)<40 else 'yellow'}]"
        f"{nifty.get('rsi',50):.1f}[/{'bright_green' if nifty.get('rsi',50)<40 else 'yellow'}]"
        f"  1M: [{'bright_green' if nifty.get('chg_1m',0)>=0 else 'red'}]"
        f"{nifty.get('chg_1m',0):+.2f}%[/{'bright_green' if nifty.get('chg_1m',0)>=0 else 'red'}]"
        f"  3M: [{'bright_green' if nifty.get('chg_3m',0)>=0 else 'red'}]"
        f"{nifty.get('chg_3m',0):+.2f}%[/{'bright_green' if nifty.get('chg_3m',0)>=0 else 'red'}]\n"
    )

    top = alerts[:10]

    # ── TABLE A: Identity + Signal Scores + Technical ─────────────────────
    _rule_minor("  Part A — Identity  ·  Grades  ·  Technical Indicators  ")
    ta = Table(box=rbox.DOUBLE_EDGE, header_style="bold bright_yellow",
               border_style="bright_yellow", show_lines=True, expand=True)
    for name, w, jus, sty in [
        ("#",              4,  "center", "dim"),
        ("Stock Symbol",  14,  "left",   "bold bright_white"),
        ("Grade",          7,  "center", "bold"),
        ("AI Score",       9,  "right",  "bold cyan"),
        ("Market %",       9,  "right",  "bold blue"),
        ("Pattern %",      9,  "right",  "bold green"),
        ("Comp Score",    10,  "right",  "bold yellow"),
        ("RSI (14)",       9,  "right",  "dim"),
        ("ADX",            7,  "right",  "dim"),
        ("Volume ×",       9,  "right",  "dim"),
        ("ATR %",          8,  "right",  "dim"),
        ("F&O",            5,  "center", "dim"),
        ("Price ₹",       12,  "right",  "bold bright_cyan"),
        ("Index Groups",  26,  "left",   "dim"),
    ]:
        ta.add_column(name, width=w, justify=jus, style=sty)

    for i, r in enumerate(top, 1):
        ai  = r["ai"]; mk_ = r["mkt"]
        ltr, lcol = _grade(ai["ai_pct"])
        sc_col = _sc(r["score"])
        ta.add_row(
            f"[bold]{i}[/bold]",
            f"[bold bright_white]{r['symbol']}[/bold bright_white]",
            f"[{lcol}]{ltr}[/{lcol}]",
            f"[bold cyan]{ai['ai_pct']:.1f}%[/bold cyan]",
            f"[bold blue]{mk_['pct']:.1f}%[/bold blue]",
            f"[bold green]{r['pat_conf']*100:.1f}%[/bold green]",
            f"[{sc_col}]{r['score']:+.4f}[/{sc_col}]",
            f"{r['rsi']:.1f}",
            f"{r['adx']:.1f}",
            f"{r['vol_ratio']:.2f}",
            f"{r['atr_pct']:.2f}%",
            "[bold bright_green]✅[/bold bright_green]" if r["is_fo"] else "[dim]—[/dim]",
            f"[bold bright_cyan]₹{r['last_close']:,.2f}[/bold bright_cyan]",
            r["indices"],
        )
    _con.print(ta)
    _con.print()

    # ── TABLE B: Trade Levels ─────────────────────────────────────────────
    _rule_minor("  Part B — Trade Levels  ·  Entry  ·  Targets  ·  Stop Losses  ·  R:R  ")
    tb = Table(box=rbox.DOUBLE_EDGE, header_style="bold bright_yellow",
               border_style="bright_yellow", show_lines=True, expand=True)
    for name, w, jus, sty in [
        ("#",              4,  "center", "dim"),
        ("Stock Symbol",  14,  "left",   "bold bright_white"),
        ("Grade",          7,  "center", "bold"),
        ("Top Signal (Full Text)",        44,  "left",   "bright_white"),
        ("ST Entry ₹",    13,  "right",  "bold cyan"),
        ("ST Target ₹",   13,  "right",  "bold bright_green"),
        ("ST Stop Loss ₹",13,  "right",  "bold red"),
        ("ST R:R",         9,  "center", "bold white"),
        ("ST % Upside",   10,  "right",  "bright_green"),
        ("LT Target ₹",   13,  "right",  "bold bright_green"),
        ("LT Stop Loss ₹",13,  "right",  "bold red"),
        ("LT R:R",         9,  "center", "bold white"),
        ("LT % Upside",   10,  "right",  "bright_green"),
    ]:
        tb.add_column(name, width=w, justify=jus, style=sty)

    for i, r in enumerate(top, 1):
        lvl  = r["levels"]; st = lvl["short_term"]; lt = lvl["long_term"]
        ltr, lcol = _grade(r["ai"]["ai_pct"])
        hits = r["hits"]
        # Full signal text — no truncation
        top_sig = hits[0][1] if hits else "No dominant signal"
        tb.add_row(
            f"[bold]{i}[/bold]",
            f"[bold bright_white]{r['symbol']}[/bold bright_white]",
            f"[{lcol}]{ltr}[/{lcol}]",
            top_sig,
            f"[bold cyan]₹{st['entry']:,.2f}[/bold cyan]",
            f"[bold bright_green]₹{st['tp']:,.2f}[/bold bright_green]",
            f"[bold red]₹{st['sl']:,.2f}[/bold red]",
            f"[bold]{st['rr_str']}[/bold]",
            _pct_m(st["entry"], st["tp"]),
            f"[bold bright_green]₹{lt['tp']:,.2f}[/bold bright_green]",
            f"[bold red]₹{lt['sl']:,.2f}[/bold red]",
            f"[bold]{lt['rr_str']}[/bold]",
            _pct_m(lt["entry"], lt["tp"]),
        )
    _con.print(tb)
    _con.print()

    # ── Backtest Performance ──────────────────────────────────────────────
    _rule_minor("  📊  Backtest Performance Summary  ")
    bt_ret=bt.get("ret",0); bt_sh=bt.get("sharpe",0); bt_dd=bt.get("maxdd",0)
    bt_wr=bt.get("winrate",0); bt_tr=bt.get("trades",0); bt_ar=bt.get("avg_ret",0)
    bt_ab=bt.get("avg_bars",0); bt_fin=bt.get("final",0)
    rc  = "bright_green" if bt_ret >= 0 else "red"
    sc_ = "bright_green" if bt_sh > 1 else "yellow" if bt_sh > 0 else "red"
    wc  = "bright_green" if bt_wr > 0.5 else "red"

    bt_t = Table(box=rbox.ROUNDED, expand=False, header_style="bold magenta",
                 border_style="magenta", padding=(0, 3), show_lines=True)
    for col in ["Metric", "Value", "Metric", "Value"]:
        bt_t.add_column(col, ratio=1)

    bt_t.add_row(
        "[dim]Total Return[/dim]",   f"[bold {rc}]{bt_ret:+.2%}[/bold {rc}]",
        "[dim]Win Rate[/dim]",       f"[bold {wc}]{bt_wr:.1%}[/bold {wc}]")
    bt_t.add_row(
        "[dim]Sharpe Ratio[/dim]",   f"[bold {sc_}]{bt_sh:.3f}[/bold {sc_}]",
        "[dim]Trade Count[/dim]",    f"[bold]{bt_tr}[/bold]")
    bt_t.add_row(
        "[dim]Max Drawdown[/dim]",   f"[bold red]{bt_dd:.2%}[/bold red]",
        "[dim]Avg Trade Return[/dim]",f"[{rc}]{bt_ar:+.2%}[/{rc}]")
    bt_t.add_row(
        "[dim]Avg Hold (bars)[/dim]",f"[bold]{bt_ab:.1f}[/bold]",
        "[dim]Final Equity[/dim]",   f"[bold bright_cyan]₹{bt_fin:,.2f}[/bold bright_cyan]")
    _con.print(Align.center(bt_t))

# ══════════════════════════════════════════════════════════════════════════════
# §18  FOOTER
# ══════════════════════════════════════════════════════════════════════════════

def render_footer(alerts: list, elapsed: float) -> None:
    _con.print()
    _con.print(Rule(characters="═", style="bright_cyan"))
    _con.print(
        "[dim]  ⚠️  DISCLAIMER: This output is generated by a quantitative model for research "
        "and educational purposes only. It does not constitute financial advice, investment "
        "recommendation, or solicitation to buy or sell any security. Past backtest performance "
        "is not indicative of future results. Always conduct your own due diligence and consult "
        "a SEBI-registered investment advisor before making any investment decisions.[/dim]"
    )
    _con.print(Rule(characters="═", style="bright_cyan"))
    _con.print(Align.center(
        f"[bold bright_cyan]  ✅  NSE Swing Trader v10.0  ·  {len(alerts)} bullish signal(s)  "
        f"·  {datetime.now().strftime('%d %b %Y  %H:%M IST')}  "
        f"·  Runtime: {elapsed:.1f}s  [/bold bright_cyan]"
    ))
    _con.print(Rule(characters="═", style="bright_cyan"))
    _con.print()


def plain_report(alerts: list, nifty: dict, bt: dict, cfg: Cfg) -> None:
    SEP = "=" * 110
    print(f"\n{SEP}")
    print(f"NSE SWING TRADER v10.0  |  {datetime.now().strftime('%d %b %Y %H:%M')}")
    print(f"Market: {nifty.get('label','N/A')} | Nifty ₹{nifty.get('last',0):,.2f} | "
          f"RSI: {nifty.get('rsi',50):.1f} | 1M: {nifty.get('chg_1m',0):+.2f}%")
    print(f"Backtest — Return: {bt.get('ret',0):+.2%}  "
          f"Sharpe: {bt.get('sharpe',0):.2f}  "
          f"MaxDD: {bt.get('maxdd',0):.2%}  "
          f"WinRate: {bt.get('winrate',0):.1%}  "
          f"Trades: {bt.get('trades',0)}")
    print(SEP)
    for i, r in enumerate(alerts[:cfg.top_n], 1):
        st   = r["levels"]["short_term"]; lt = r["levels"]["long_term"]
        hits = r["hits"]; ai = r["ai"]; mk = r["mkt"]
        print(f"\n[{i:>2}]  *** {r['symbol']} ***  "
              f"Close: ₹{r['last_close']:,.2f}  "
              f"AI: {ai['ai_pct']:.1f}%  "
              f"Market: {mk['pct']:.1f}%  "
              f"ADX: {r['adx']:.1f}  "
              f"RSI: {r['rsi']:.1f}  "
              f"Vol: {r['vol_ratio']:.2f}x")
        print(f"       Signal:    {hits[0][1] if hits else '—'}")
        print(f"       Short-Term: Entry ₹{st['entry']:,.2f}  →  Target ₹{st['tp']:,.2f}"
              f"  |  Stop ₹{st['sl']:,.2f}  |  R:R {st['rr_str']}  |  {st['window']}")
        print(f"       Long-Term:  Entry ₹{lt['entry']:,.2f}  →  Target ₹{lt['tp']:,.2f}"
              f"  |  Stop ₹{lt['sl']:,.2f}  |  R:R {lt['rr_str']}  |  {lt['window']}")
        print(f"       Reason:     {r['reason'][:115]}")
        print(f"       Indices:    {r['indices']}")
        print("-" * 110)

def run(cfg:Cfg)->tuple:
    cfg.output_dir.mkdir(parents=True,exist_ok=True)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)-8s | %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(cfg.output_dir/"nse_v10.log")])
    for nm in ("yfinance","peewee","urllib3","requests","charset_normalizer"):
        logging.getLogger(nm).setLevel(logging.CRITICAL)
    t0=time.time()

    if _HAS_RICH:
        _con.print()
        _con.print(Rule(characters="═",style="bright_cyan"))
        _con.print(Align.center(
            "[bold bright_white on #003366]"
            "   NSE SWING TRADER v10.0  ·  GOD-LEVEL BULLISH EDITION  "
            "·  FULLY STANDALONE   "
            "[/bold bright_white on #003366]"))
        _con.print(Align.center(
            f"[dim]  {datetime.now().strftime('%A, %d %B %Y  |  %H:%M IST')}  "
            f"|  Capital: ₹{cfg.capital/1e5:.0f}L  "
            f"|  Universe: {len(_ALL_SYMS)} symbols  [/dim]"))
        _con.print(Rule(characters="═",style="bright_cyan"))
        _con.print(f"\n[dim]📡 Fetching Nifty50 benchmark...[/dim]")

    nifty=nifty50_state(); nifty_trend=nifty.get("trend",0.0)

    if _HAS_RICH and nifty:
        threshold=cfg.bear_threshold if nifty_trend<=-0.5 else cfg.base_threshold
        _con.print(
            f"[bold blue]📊 Nifty50:[/bold blue]  ₹{nifty.get('last',0):.0f}  "
            f"{nifty.get('label','N/A')}  RSI:{nifty.get('rsi',50):.0f}  "
            f"1M:{nifty.get('chg_1m',0):+.1f}%  3M:{nifty.get('chg_3m',0):+.1f}%")
        if nifty_trend<=-0.5:
            _con.print(f"[bold red]⚠️  Bear market — threshold raised to {threshold:.2f}[/bold red]\n")
        else:
            _con.print()

    syms=cfg.symbols if cfg.symbols else _ALL_SYMS
    syms=sorted(set(syms)-_SKIP_SYMBOLS)
    if _HAS_RICH: _con.print(f"[dim]📋 Scanning {len(syms)} unique symbols...[/dim]\n")

    all_frames:list[pd.DataFrame]=[]; fund_cache:dict[str,dict]={}; ok=0; fail=0

    if cfg.use_sample:
        end=pd.Timestamp.today().normalize(); start=end-pd.Timedelta(days=260)
        demo=syms[:10]; all_frames.append(sample_ohlcv(demo,start,end)); ok=len(demo)
    elif _HAS_YF:
        def _fetch(sym):
            nonlocal ok,fail
            df=fetch_ohlcv(sym,cfg.live_period,cfg.live_interval)
            if not df.empty and len(df)>=cfg.min_bars:
                all_frames.append(df)
                if cfg.fetch_fundamentals: fund_cache[sym]=fetch_fundamentals(sym)
                ok+=1
            else: fail+=1

        if _HAS_RICH:
            with Progress(SpinnerColumn(),TextColumn("[progress.description]{task.description}"),
                          BarColumn(bar_width=24),TextColumn("{task.completed}/{task.total}"),
                          TimeElapsedColumn(),console=_con) as prog:
                task=prog.add_task("[cyan]🔴 Live scanning NSE...",total=len(syms))
                for sym in syms:
                    prog.update(task,description=f"[cyan]🔴 [bold]{sym:<14}[/bold]")
                    _fetch(sym); prog.advance(task)
        else:
            for i,sym in enumerate(syms,1):
                if i%20==0: LOG.info("Progress: %d/%d  ok:%d fail:%d",i,len(syms),ok,fail)
                _fetch(sym)
    elif cfg.prices_csv.exists():
        df=pd.read_csv(cfg.prices_csv); df["date"]=_norm_dates(df["date"])
        df["symbol"]=df["symbol"].str.upper(); all_frames.append(df); ok=df["symbol"].nunique()
    else:
        end=pd.Timestamp.today().normalize(); start=end-pd.Timedelta(days=260)
        all_frames.append(sample_ohlcv(_ALL_SYMS[:10],start,end)); ok=10

    if not all_frames: raise ValueError("No price data loaded.")

    prices=pd.concat(all_frames,ignore_index=True)
    prices=(prices.sort_values(["symbol","date"])
            .drop_duplicates(["date","symbol"],keep="last")
            .reset_index(drop=True))
    LOG.info("Loaded %d bars across %d symbols.",len(prices),prices["symbol"].nunique())

    def _inject(grp):
        sym=grp["symbol"].iloc[0]; fd=fund_cache.get(sym,{})
        grp["_pe"]=fd.get("pe") or 0.0; grp["_roe"]=fd.get("roe") or 0.0
        grp["_epsg"]=fd.get("eps_g") or 0.0; return grp
    prices=prices.groupby("symbol",group_keys=False).apply(_inject).reset_index(drop=True)

    if _HAS_RICH: _con.print("[dim]⚙️  Computing 45+ indicators per symbol...[/dim]")
    feat=engineer_all(prices,cfg)
    if feat.empty: raise ValueError("Feature engineering returned empty frame.")

    feat=add_scores(feat,cfg,nifty_trend)

    if _HAS_RICH: _con.print("[dim]📊 Running backtest...[/dim]")
    bt=run_backtest(feat,cfg)

    alerts,rej=build_alerts(feat,nifty,fund_cache,cfg)
    elapsed=round(time.time()-t0,1)

    if _HAS_RICH:
        _con.print(
            f"\n[dim]🔍 Scan done in [bold]{elapsed}s[/bold]  |  "
            f"Fetched:[bold]{ok}[/bold]  Skipped:[yellow]{fail}[/yellow]  "
            f"Alerts:[bold bright_green]{len(alerts)}[/bold bright_green]  "
            f"Rejected:{dict(rej)}[/dim]\n")

    save_all(alerts,bt,nifty,cfg)

    if not _HAS_RICH:
        plain_report(alerts,nifty,bt,cfg); return alerts,bt

    if not alerts:
        _section_hdr("⚠️","No Signals Found",colour="yellow")
        _con.print("[bold yellow]  No bullish signals passed all quality gates.[/bold yellow]")
        _con.print("[dim]  Try: --threshold 0.18  or  --min-vol 1000000  or  --sample[/dim]")
        render_footer([],elapsed); return alerts,bt

    # ═══════════════════════════════════════════════════════════════════════
    # PRO DISPLAY
    # ═══════════════════════════════════════════════════════════════════════
    render_market_banner(nifty,bt,alerts)
    render_sector_heatmap(alerts)

    _section_hdr("🟢",f"Detailed Bullish Signal Cards  ({len(alerts)} stocks)",
                 sub=f"Capital ₹{cfg.capital/1e5:.0f}L  ·  Min R:R {cfg.min_rr}  ·  Threshold {cfg.base_threshold}",
                 colour="bright_green")
    for i,r in enumerate(alerts[:cfg.top_n],1):
        render_pro_card(r,rank=i,feat_df=feat,capital=cfg.capital)

    render_watchlist(alerts,nifty,bt)
    render_footer(alerts,elapsed)

    return alerts,bt

# ══════════════════════════════════════════════════════════════════════════════
# §20  CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p=argparse.ArgumentParser(
        description="NSE Swing Trader v10.0 — Fully Standalone (engine + pro display)",
        formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--symbols",    type=str,   default="",
                   help="Comma-separated NSE symbols. Default: full universe.")
    p.add_argument("--group",      type=str,   default="",
                   help="Index group: 'NIFTY BANK', 'FO STOCKS', 'NIFTY IT', etc.")
    p.add_argument("--top-n",      type=int,   default=10)
    p.add_argument("--sample",     action="store_true",help="Use synthetic OHLCV (no internet).")
    p.add_argument("--prices-csv", type=Path,  default=Path("data/prices.csv"))
    p.add_argument("--output-dir", type=Path,  default=Path("nse_v10_output"))
    p.add_argument("--period",     type=str,   default="8mo")
    p.add_argument("--min-vol",    type=int,   default=1_500_000)
    p.add_argument("--min-tv",     type=float, default=5.0)
    p.add_argument("--min-rr",     type=float, default=1.5)
    p.add_argument("--threshold",  type=float, default=0.22)
    p.add_argument("--no-fund",    action="store_true",help="Skip fundamental fetch (faster).")
    p.add_argument("--capital",    type=float, default=1_000_000,
                   help="Portfolio capital for position sizing (default ₹10,00,000).")
    a=p.parse_args()

    cfg=Cfg()
    cfg.use_sample         = a.sample
    cfg.output_dir         = a.output_dir
    cfg.prices_csv         = a.prices_csv
    cfg.live_period        = a.period
    cfg.top_n              = a.top_n
    cfg.min_avg_vol        = a.min_vol
    cfg.min_traded_val_cr  = a.min_tv
    cfg.min_rr             = a.min_rr
    cfg.base_threshold     = a.threshold
    cfg.fetch_fundamentals = not a.no_fund
    cfg.capital            = a.capital

    if a.symbols:
        cfg.symbols=[s.strip().upper() for s in a.symbols.split(",") if s.strip()]
    elif a.group:
        gk=a.group.strip().upper()
        matched=[sl for grp,sl in _UNIVERSE.items() if gk in grp.upper()]
        if matched:
            cfg.symbols=sorted({s for sub in matched for s in sub}-_SKIP_SYMBOLS)
        else:
            print(f"Group '{a.group}' not found. Available: {list(_UNIVERSE.keys())}")
            sys.exit(1)

    run(cfg)


# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# §21  STREAMLIT DASHBOARD  —  TradingView-Style Interactive UI
# ══════════════════════════════════════════════════════════════════════════════

import streamlit.components.v1 as components

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS  —  Bloomberg dark + TradingView aesthetic
# ─────────────────────────────────────────────────────────────────────────────
_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;700;800&display=swap');

:root {
    --bg:        #0b0e11;
    --bg1:       #131722;
    --bg2:       #1c2030;
    --bg3:       #252d3d;
    --border:    #2a3347;
    --border2:   #3d4f6b;
    --green:     #26a69a;
    --green2:    #00e676;
    --red:       #ef5350;
    --amber:     #f59e0b;
    --cyan:      #38bdf8;
    --blue:      #3b82f6;
    --purple:    #a855f7;
    --text:      #d1d4dc;
    --text2:     #787b86;
    --text3:     #434651;
}

/* ── Base ─────────────────────────────────────────────── */
.stApp,[data-testid="stAppViewContainer"]{
    background:var(--bg)!important;
    font-family:'JetBrains Mono',monospace!important;
    color:var(--text)!important;
}
[data-testid="stSidebar"]{
    background:var(--bg1)!important;
    border-right:1px solid var(--border)!important;
}
[data-testid="stSidebar"] *{ color:var(--text)!important; }

/* ── Metrics ──────────────────────────────────────────── */
[data-testid="metric-container"]{
    background:var(--bg1)!important;
    border:1px solid var(--border)!important;
    border-radius:4px!important;
    padding:12px 16px!important;
}
[data-testid="stMetricValue"]{
    font-family:'Syne',sans-serif!important;
    font-size:1.35rem!important;
    font-weight:800!important;
}
[data-testid="stMetricLabel"]{
    font-size:.65rem!important;
    letter-spacing:.1em!important;
    text-transform:uppercase!important;
    color:var(--text2)!important;
}

/* ── Tabs ─────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"]{
    background:var(--bg1)!important;
    border-bottom:1px solid var(--border)!important;
    gap:0!important;
    padding:0 8px!important;
}
.stTabs [data-baseweb="tab"]{
    background:transparent!important;
    color:var(--text2)!important;
    font-family:'JetBrains Mono',monospace!important;
    font-size:.75rem!important;
    letter-spacing:.08em!important;
    border-radius:0!important;
    padding:10px 18px!important;
    border-bottom:2px solid transparent!important;
    transition:all .15s!important;
}
.stTabs [aria-selected="true"]{
    color:var(--cyan)!important;
    border-bottom:2px solid var(--cyan)!important;
}

/* ── Buttons ──────────────────────────────────────────── */
.stButton>button{
    background:var(--bg2)!important;
    border:1px solid var(--border2)!important;
    color:var(--cyan)!important;
    font-family:'JetBrains Mono',monospace!important;
    font-size:.75rem!important;
    border-radius:3px!important;
    padding:6px 14px!important;
    transition:all .15s!important;
    letter-spacing:.04em!important;
}
.stButton>button:hover{
    background:var(--border2)!important;
    color:#fff!important;
    border-color:var(--cyan)!important;
}

/* ── Inputs ───────────────────────────────────────────── */
.stSelectbox>div>div,.stMultiSelect>div>div{
    background:var(--bg2)!important;
    border-color:var(--border)!important;
    font-size:.8rem!important;
}
input[type="number"],.stNumberInput input{
    background:var(--bg2)!important;
    border-color:var(--border)!important;
    color:var(--text)!important;
}

/* ── Dataframe ────────────────────────────────────────── */
[data-testid="stDataFrame"]{
    border:1px solid var(--border)!important;
    border-radius:4px!important;
    font-size:.76rem!important;
}

/* ── Expander ─────────────────────────────────────────── */
.streamlit-expanderHeader{
    background:var(--bg2)!important;
    border:1px solid var(--border)!important;
    border-radius:3px!important;
    font-size:.78rem!important;
}

/* ── Custom components ────────────────────────────────── */
.tv-card{
    background:var(--bg1);
    border:1px solid var(--border);
    border-radius:4px;
    padding:16px 18px;
    margin-bottom:10px;
    transition:border-color .15s;
}
.tv-card:hover{ border-color:var(--border2); }
.tv-card-bull{ border-left:2px solid var(--green)!important; }
.tv-card-bear{ border-left:2px solid var(--red)!important; }
.tv-card-warn{ border-left:2px solid var(--amber)!important; }

.tv-badge-sym{
    display:inline-block;
    background:var(--green);
    color:#0b0e11;
    font-family:'Syne',sans-serif;
    font-weight:800;
    font-size:1rem;
    padding:3px 12px;
    border-radius:3px;
    letter-spacing:.05em;
}
.tv-badge-grade{
    display:inline-block;
    font-family:'Syne',sans-serif;
    font-weight:800;
    font-size:1.1rem;
    padding:3px 10px;
    border-radius:3px;
}
.tv-label{
    font-size:.62rem;
    letter-spacing:.1em;
    text-transform:uppercase;
    color:var(--text2);
    margin-bottom:3px;
}
.tv-val{
    font-family:'Syne',sans-serif;
    font-weight:700;
}
.tv-section{
    font-family:'Syne',sans-serif;
    font-weight:700;
    font-size:.9rem;
    letter-spacing:.1em;
    text-transform:uppercase;
    color:var(--cyan);
    padding:6px 0 5px;
    border-bottom:1px solid var(--border);
    margin-bottom:12px;
    margin-top:16px;
}
.tv-divider{ border:none; border-top:1px solid var(--border); margin:12px 0; }
.blink{animation:blink 1.4s step-start infinite;}
@keyframes blink{50%{opacity:0;}}
.pill-green{background:rgba(38,166,154,.18);color:#26a69a;padding:2px 8px;border-radius:2px;font-size:.68rem;font-weight:600;}
.pill-red  {background:rgba(239,83,80,.18); color:#ef5350;padding:2px 8px;border-radius:2px;font-size:.68rem;font-weight:600;}
.pill-cyan {background:rgba(56,189,248,.18);color:#38bdf8;padding:2px 8px;border-radius:2px;font-size:.68rem;font-weight:600;}
.pill-amber{background:rgba(245,158,11,.18);color:#f59e0b;padding:2px 8px;border-radius:2px;font-size:.68rem;font-weight:600;}
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# TRADINGVIEW WIDGETS
# ─────────────────────────────────────────────────────────────────────────────

def tv_chart_widget(symbol: str, height: int = 520) -> str:
    """Full TradingView Advanced Chart widget — live NSE data, all indicators."""
    tv_sym = f"NSE:{symbol}"
    return f"""
    <div id="tv_chart_{symbol}" style="height:{height}px;border-radius:4px;overflow:hidden;border:1px solid #2a3347;"></div>
    <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
    <script type="text/javascript">
    new TradingView.widget({{
        "autosize": true,
        "symbol": "{tv_sym}",
        "interval": "D",
        "timezone": "Asia/Kolkata",
        "theme": "dark",
        "style": "1",
        "locale": "en",
        "toolbar_bg": "#131722",
        "enable_publishing": false,
        "withdateranges": true,
        "hide_side_toolbar": false,
        "allow_symbol_change": true,
        "watchlist": [],
        "details": true,
        "hotlist": false,
        "calendar": false,
        "show_popup_button": true,
        "popup_width": "1000",
        "popup_height": "650",
        "studies": [
            "MASimple@tv-basicstudies",
            "MASimple@tv-basicstudies",
            "RSI@tv-basicstudies",
            "MACD@tv-basicstudies",
            "Volume@tv-basicstudies"
        ],
        "studies_overrides": {{
            "moving average.length": 21,
            "moving average.plot.color": "#f59e0b",
            "moving average.plot.linewidth": 1.5
        }},
        "overrides": {{
            "mainSeriesProperties.candleStyle.upColor": "#26a69a",
            "mainSeriesProperties.candleStyle.downColor": "#ef5350",
            "mainSeriesProperties.candleStyle.borderUpColor": "#26a69a",
            "mainSeriesProperties.candleStyle.borderDownColor": "#ef5350",
            "mainSeriesProperties.candleStyle.wickUpColor": "#26a69a",
            "mainSeriesProperties.candleStyle.wickDownColor": "#ef5350",
            "paneProperties.background": "#0b0e11",
            "paneProperties.backgroundType": "solid",
            "paneProperties.gridLinesMode": "both",
            "paneProperties.vertGridProperties.color": "#2a3347",
            "paneProperties.horzGridProperties.color": "#2a3347",
            "scalesProperties.textColor": "#787b86",
            "scalesProperties.fontSize": 11
        }},
        "container_id": "tv_chart_{symbol}"
    }});
    </script>
    """

def tv_mini_chart(symbol: str, height: int = 180, nonce: str = "") -> str:
    """Compact mini sparkline-style TradingView chart."""
    tv_sym = f"NSE:{symbol}"
    return f"""
    <div style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
    <!-- TradingView Widget BEGIN nonce={nonce} -->
    <div class="tradingview-widget-container" style="height:{height}px;">
      <div class="tradingview-widget-container__widget" style="height:100%;"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-mini-symbol-overview.js" async>
      {{
        "symbol": "{tv_sym}",
        "width": "100%",
        "height": "{height}",
        "locale": "en",
        "dateRange": "3M",
        "colorTheme": "dark",
        "trendLineColor": "rgba(38, 166, 154, 1)",
        "underLineColor": "rgba(38, 166, 154, 0.1)",
        "underLineBottomColor": "rgba(41, 98, 255, 0)",
        "isTransparent": true,
        "autosize": false,
        "largeChartUrl": ""
      }}
      </script>
    </div>
    <!-- TradingView Widget END -->
    </div>
    """

def tv_ticker_tape(symbols: list) -> str:
    """Scrolling ticker tape at the top."""
    syms = [{"proName": f"NSE:{s}", "title": s} for s in symbols[:20]]
    import json
    syms_json = json.dumps(syms)
    return f"""
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container" style="margin-bottom:8px;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
      {{
        "symbols": {syms_json},
        "showSymbolLogo": false,
        "isTransparent": true,
        "displayMode": "adaptive",
        "colorTheme": "dark",
        "locale": "en"
      }}
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_market_overview(nifty_last: float, nifty_label: str) -> str:
    """TradingView market overview for Indian indices."""
    return """
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-market-overview.js" async>
      {
        "colorTheme": "dark",
        "dateRange": "1M",
        "showChart": true,
        "locale": "en",
        "largeChartUrl": "",
        "isTransparent": true,
        "showSymbolLogo": true,
        "showFloatingTooltip": true,
        "width": "100%",
        "height": "400",
        "tabs": [
          {
            "title": "Indian Indices",
            "symbols": [
              {"s": "BSE:SENSEX", "d": "Sensex"},
              {"s": "NSE:NIFTY", "d": "Nifty 50"},
              {"s": "NSE:BANKNIFTY", "d": "Bank Nifty"},
              {"s": "NSE:CNXIT", "d": "Nifty IT"},
              {"s": "NSE:CNXENERGY", "d": "Nifty Energy"},
              {"s": "NSE:CNXAUTO", "d": "Nifty Auto"}
            ],
            "originalTitle": "Indian Indices"
          },
          {
            "title": "F&O Leaders",
            "symbols": [
              {"s": "NSE:RELIANCE"},
              {"s": "NSE:HDFCBANK"},
              {"s": "NSE:ICICIBANK"},
              {"s": "NSE:TCS"},
              {"s": "NSE:INFY"},
              {"s": "NSE:SBIN"}
            ]
          }
        ]
      }
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_heatmap() -> str:
    """TradingView stock heatmap for NSE."""
    return """
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-stock-heatmap.js" async>
      {
        "exchanges": [],
        "dataSource": "SENSEX",
        "grouping": "sector",
        "blockSize": "market_cap_basic",
        "blockColor": "change",
        "locale": "en",
        "symbolUrl": "",
        "colorTheme": "dark",
        "hasTopBar": true,
        "isDataSetEnabled": false,
        "isZoomEnabled": true,
        "hasSymbolTooltip": true,
        "isMonoSize": false,
        "width": "100%",
        "height": "480"
      }
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_economic_calendar() -> str:
    """TradingView Economic Calendar widget."""
    return """
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-events.js" async>
      {
        "colorTheme": "dark",
        "isTransparent": true,
        "width": "100%",
        "height": "450",
        "locale": "en",
        "importanceFilter": "0,1",
        "countryFilter": "in"
      }
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_symbol_info(symbol: str, nonce: str = "") -> str:
    """TradingView Symbol Info bar."""
    return f"""
    <!-- TradingView Widget BEGIN nonce={nonce} -->
    <div class="tradingview-widget-container" style="margin-bottom:10px;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-symbol-info.js" async>
      {{
        "symbol": "NSE:{symbol}",
        "width": "100%",
        "locale": "en",
        "colorTheme": "dark",
        "isTransparent": true
      }}
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_technical_analysis(symbol: str, nonce: str = "") -> str:
    """TradingView Technical Analysis (buy/sell gauge).
    Pass nonce=symbol to force re-render when symbol changes."""
    return f"""
    <!-- TradingView Widget BEGIN nonce={nonce} -->
    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-technical-analysis.js" async>
      {{
        "interval": "1D",
        "width": "100%",
        "isTransparent": true,
        "height": "450",
        "symbol": "NSE:{symbol}",
        "showIntervalTabs": true,
        "displayMode": "multiple",
        "locale": "en",
        "colorTheme": "dark"
      }}
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_financials(symbol: str) -> str:
    """TradingView Financials widget."""
    return f"""
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-financials.js" async>
      {{
        "isTransparent": true,
        "largeChartUrl": "",
        "displayMode": "regular",
        "width": "100%",
        "height": "830",
        "colorTheme": "dark",
        "symbol": "NSE:{symbol}",
        "locale": "en"
      }}
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_news(symbol: str) -> str:
    """TradingView News widget for a symbol."""
    return f"""
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-timeline.js" async>
      {{
        "feedMode": "symbol",
        "isTransparent": true,
        "displayMode": "regular",
        "width": "100%",
        "height": "500",
        "colorTheme": "dark",
        "locale": "en",
        "symbol": "NSE:{symbol}"
      }}
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

def tv_screener() -> str:
    """TradingView Stock Screener for NSE."""
    return """
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
      <div class="tradingview-widget-container__widget"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-screener.js" async>
      {
        "width": "100%",
        "height": "600",
        "defaultColumn": "overview",
        "defaultScreen": "most_capitalized",
        "market": "india",
        "showToolbar": true,
        "colorTheme": "dark",
        "locale": "en",
        "isTransparent": true
      }
      </script>
    </div>
    <!-- TradingView Widget END -->
    """

# ─────────────────────────────────────────────────────────────────────────────
# PLOTLY HELPERS  (no 8-digit hex — all rgba)
# ─────────────────────────────────────────────────────────────────────────────

def _d_grade(pct: float) -> tuple:
    if pct >= 88: return "A+","#26a69a"
    if pct >= 78: return "A", "#4db6ac"
    if pct >= 68: return "B+","#f59e0b"
    if pct >= 58: return "B", "#fbbf24"
    if pct >= 48: return "C+","#ef5350"
    return "C","#e53935"

def _d_rsi_col(r):
    if r < 30: return "#26a69a"
    if r < 45: return "#4db6ac"
    if r < 60: return "#f59e0b"
    if r < 75: return "#ffa726"
    return "#ef5350"

def _d_adx_col(a):
    if a >= 40: return "#26a69a"
    if a >= 28: return "#4db6ac"
    if a >= 20: return "#f59e0b"
    return "#ef5350"

def _d_vol_col(v):
    if v >= 2.0: return "#26a69a"
    if v >= 1.5: return "#4db6ac"
    if v >= 1.0: return "#f59e0b"
    return "#ef5350"

def _d_score_col(s):
    if s >= 0.35: return "#26a69a"
    if s >= 0.22: return "#4db6ac"
    if s >= 0.10: return "#f59e0b"
    return "#787b86"

def gauge_html(pct: float, label: str = "", width: int = 200) -> str:
    pct = max(0, min(pct, 100))
    col = "#26a69a" if pct >= 70 else "#f59e0b" if pct >= 45 else "#ef5350"
    f   = int(pct / 100 * width)
    return (f"<div style='margin:4px 0'>"
            f"<div class='tv-label' style='margin-bottom:3px'>{label}</div>"
            f"<div style='display:flex;align-items:center;gap:8px'>"
            f"<svg width='{width}' height='6' style='border-radius:3px;overflow:hidden'>"
            f"<rect width='{width}' height='6' fill='#2a3347'/>"
            f"<rect width='{f}' height='6' fill='{col}' rx='3'/></svg>"
            f"<span style='color:{col};font-size:.8rem;font-weight:700'>{pct:.1f}%</span>"
            f"</div></div>")

def fmt_inr(v):
    if v is None or (isinstance(v,float) and np.isnan(v)): return "N/A"
    return f"₹{v:,.2f}"

def fmt_cr(v):
    if v is None or (isinstance(v,float) and np.isnan(v)): return "N/A"
    return f"₹{v:,.2f} Cr"

def radar_fig(ai_dict: dict):
    cats = ["Trend","Momentum","Breakout","Pullback","Volume","Pattern","Fundamental","Sentiment"]
    keys = ["trend_s","mom_s","brk_s","trend_s","vol_s","pat_s","fund_s","sent_s"]
    vals = [(float(ai_dict.get(k,0))+1)/2*100 for k in keys]
    vc = vals + [vals[0]]; cc = cats + [cats[0]]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=vc, theta=cc, fill="toself",
        fillcolor="rgba(56,189,248,0.10)", line=dict(color="#38bdf8",width=2), name="Score"))
    fig.update_layout(
        polar=dict(bgcolor="#131722",
            radialaxis=dict(visible=True,range=[0,100],tickfont=dict(size=8,color="#434651"),
                gridcolor="#2a3347",linecolor="#2a3347"),
            angularaxis=dict(tickfont=dict(size=9,color="#787b86"),
                gridcolor="#2a3347",linecolor="#2a3347")),
        paper_bgcolor="#131722",font=dict(family="JetBrains Mono"),
        showlegend=False,height=280,margin=dict(l=20,r=20,t=10,b=10))
    return fig

def factor_bar_fig(ai_dict: dict):
    factors=[("Trend",0.24,"trend_s"),("Momentum",0.16,"mom_s"),("Breakout",0.17,"brk_s"),
             ("Volume",0.10,"vol_s"),("Pattern",0.10,"pat_s"),("Fundamental",0.08,"fund_s"),
             ("Sentiment",0.04,"sent_s"),("Pullback",0.11,"trend_s")]
    names=[f[0] for f in factors]
    scores=[float(ai_dict.get(f[2],0)) for f in factors]
    colors=["#26a69a" if s>0.1 else "#ef5350" if s<-0.1 else "#f59e0b" for s in scores]
    fig=go.Figure(go.Bar(y=names,x=scores,orientation="h",marker_color=colors,
        text=[f"{s:+.3f}" for s in scores],textposition="outside",
        textfont=dict(size=10,color="#787b86")))
    fig.add_vline(x=0,line_color="#434651",line_width=1)
    fig.update_layout(height=280,paper_bgcolor="#131722",plot_bgcolor="#0b0e11",
        font=dict(family="JetBrains Mono",color="#787b86",size=10),
        xaxis=dict(range=[-1.1,1.1],showgrid=True,gridcolor="#2a3347",
                   zeroline=False,tickformat="+.1f"),
        yaxis=dict(showgrid=False),
        margin=dict(l=5,r=60,t=5,b=5),showlegend=False)
    return fig

def equity_curve_fig(bt_dict: dict):
    df=bt_dict.get("trades_df",pd.DataFrame())
    if df.empty: return go.Figure()
    df=df.copy()
    exit_col="exit" if "exit" in df.columns else "exit_date" if "exit_date" in df.columns else None
    if not exit_col: return go.Figure()
    df["_exit"]=pd.to_datetime(df[exit_col]); df=df.dropna(subset=["_exit"]).sort_values("_exit")
    df["cum_pnl"]=df["pnl"].cumsum()
    col="#26a69a" if df["cum_pnl"].iloc[-1]>=0 else "#ef5350"
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=df["_exit"],y=df["cum_pnl"],mode="lines",
        line=dict(color=col,width=2),fill="tozeroy",
        fillcolor="rgba(38,166,154,0.08)" if col=="#26a69a" else "rgba(239,83,80,0.08)",
        name="P&L"))
    fig.add_hline(y=0,line_color="#434651",line_dash="dot",line_width=1)
    fig.update_layout(height=240,paper_bgcolor="#131722",plot_bgcolor="#0b0e11",
        font=dict(family="JetBrains Mono",color="#787b86",size=10),
        margin=dict(l=0,r=0,t=10,b=0),
        xaxis=dict(showgrid=True,gridcolor="#2a3347"),
        yaxis=dict(showgrid=True,gridcolor="#2a3347",tickprefix="₹"),
        showlegend=False)
    return fig

def multi_score_fig(alerts_list: list):
    top=alerts_list[:12]
    syms=[r["symbol"] for r in top]
    ai_v=[r["ai"]["ai_pct"] for r in top]
    mk_v=[r["mkt"]["pct"] for r in top]
    pt_v=[r["pat_conf"]*100 for r in top]
    fig=go.Figure()
    fig.add_trace(go.Bar(name="AI Score",x=syms,y=ai_v,marker_color="#38bdf8",
        text=[f"{v:.0f}%" for v in ai_v],textposition="outside",textfont=dict(size=9)))
    fig.add_trace(go.Bar(name="Market",x=syms,y=mk_v,marker_color="#3b82f6",
        text=[f"{v:.0f}%" for v in mk_v],textposition="outside",textfont=dict(size=9)))
    fig.add_trace(go.Bar(name="Pattern",x=syms,y=pt_v,marker_color="#26a69a",
        text=[f"{v:.0f}%" for v in pt_v],textposition="outside",textfont=dict(size=9)))
    fig.update_layout(barmode="group",height=320,
        paper_bgcolor="#131722",plot_bgcolor="#0b0e11",
        font=dict(family="JetBrains Mono",color="#787b86",size=10),
        legend=dict(bgcolor="rgba(0,0,0,0)",font=dict(size=9)),
        margin=dict(l=0,r=0,t=20,b=0),
        xaxis=dict(showgrid=False,tickfont=dict(size=9)),
        yaxis=dict(showgrid=True,gridcolor="#2a3347",range=[0,115]))
    return fig

def score_distribution_fig(alerts_list: list):
    scores=[r["score"] for r in alerts_list]
    if not scores: return go.Figure()
    fig=go.Figure()
    fig.add_trace(go.Histogram(x=scores,nbinsx=20,
        marker_color="#38bdf8",opacity=0.75,name="Score Distribution"))
    fig.add_vline(x=np.mean(scores),line_color="#f59e0b",line_dash="dash",
        annotation_text=f"Avg {np.mean(scores):.3f}",annotation_font_color="#f59e0b")
    fig.update_layout(height=220,paper_bgcolor="#131722",plot_bgcolor="#0b0e11",
        font=dict(family="JetBrains Mono",color="#787b86",size=10),
        margin=dict(l=0,r=0,t=10,b=0),
        xaxis=dict(showgrid=True,gridcolor="#2a3347",title="Composite Score"),
        yaxis=dict(showgrid=True,gridcolor="#2a3347"),showlegend=False)
    return fig

def rsi_vs_adx_fig(alerts_list: list):
    if not alerts_list: return go.Figure()
    syms=[r["symbol"] for r in alerts_list]
    rsi_v=[r["rsi"] for r in alerts_list]
    adx_v=[r["adx"] for r in alerts_list]
    ai_v=[r["ai"]["ai_pct"] for r in alerts_list]
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=rsi_v,y=adx_v,mode="markers+text",
        marker=dict(size=[a/5 for a in ai_v],color=ai_v,
                    colorscale=[[0,"#ef5350"],[0.5,"#f59e0b"],[1,"#26a69a"]],
                    showscale=True,colorbar=dict(title="AI%",thickness=10,
                        tickfont=dict(size=8,color="#787b86"))),
        text=syms,textposition="top center",textfont=dict(size=8,color="#787b86"),
        hovertemplate="<b>%{text}</b><br>RSI: %{x:.1f}<br>ADX: %{y:.1f}<extra></extra>"))
    fig.add_vline(x=50,line_color="#434651",line_dash="dot",line_width=1)
    fig.add_hline(y=25,line_color="#434651",line_dash="dot",line_width=1)
    fig.update_layout(height=320,paper_bgcolor="#131722",plot_bgcolor="#0b0e11",
        font=dict(family="JetBrains Mono",color="#787b86",size=10),
        margin=dict(l=0,r=30,t=10,b=0),
        xaxis=dict(showgrid=True,gridcolor="#2a3347",title="RSI (14)",range=[0,100]),
        yaxis=dict(showgrid=True,gridcolor="#2a3347",title="ADX"),
        showlegend=False)
    return fig

def waterfall_fig(bt_dict: dict):
    df=bt_dict.get("trades_df",pd.DataFrame())
    if df.empty or len(df)<2: return go.Figure()
    df=df.copy().head(20)
    syms=[str(r.get("sym",r.get("symbol","?"))) for _,r in df.iterrows()]
    pnls=[float(r["pnl"]) for _,r in df.iterrows()]
    colors=["rgba(38,166,154,0.8)" if p>=0 else "rgba(239,83,80,0.8)" for p in pnls]
    fig=go.Figure(go.Bar(x=syms,y=pnls,marker_color=colors,
        text=[f"₹{p:+,.0f}" for p in pnls],textposition="outside",
        textfont=dict(size=8,color="#787b86")))
    fig.add_hline(y=0,line_color="#434651",line_width=1)
    fig.update_layout(height=250,paper_bgcolor="#131722",plot_bgcolor="#0b0e11",
        font=dict(family="JetBrains Mono",color="#787b86",size=9),
        margin=dict(l=0,r=0,t=10,b=40),
        xaxis=dict(showgrid=False,tickangle=-45,tickfont=dict(size=8)),
        yaxis=dict(showgrid=True,gridcolor="#2a3347",tickprefix="₹"),
        showlegend=False)
    return fig

def trade_scenario_html(title: str, lvl: dict, color: str, window: str) -> str:
    e=lvl["entry"]; tp=lvl["tp"]; sl=lvl["sl"]; rr=lvl["rr_str"]
    ri=lvl["risk"]; rw=lvl["reward"]
    up=(tp/e-1)*100 if e else 0; dn=(sl/e-1)*100 if e else 0
    return f"""
    <div class='tv-card' style='border-color:{color}66;text-align:center'>
      <div style='color:{color};font-weight:700;font-size:.82rem;margin-bottom:12px;letter-spacing:.06em'>{title}</div>
      <div class='tv-label'>Entry</div>
      <div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.2rem;color:#38bdf8;margin-bottom:10px'>₹{e:,.2f}</div>
      <div style='display:flex;justify-content:space-around;margin-bottom:10px'>
        <div><div class='tv-label'>Target</div>
          <div style='font-weight:700;color:#26a69a'>₹{tp:,.2f}</div>
          <div style='font-size:.7rem;color:#26a69a'>{up:+.1f}%</div></div>
        <div><div class='tv-label'>Stop Loss</div>
          <div style='font-weight:700;color:#ef5350'>₹{sl:,.2f}</div>
          <div style='font-size:.7rem;color:#ef5350'>{dn:+.1f}%</div></div>
      </div>
      <div style='display:flex;justify-content:space-around;padding-top:8px;border-top:1px solid #2a3347'>
        <div><div class='tv-label'>R:R</div><div style='color:#d1d4dc;font-weight:700'>{rr}</div></div>
        <div><div class='tv-label'>Risk</div><div style='color:#ef5350'>₹{ri:.2f}</div></div>
        <div><div class='tv-label'>Reward</div><div style='color:#26a69a'>₹{rw:.2f}</div></div>
      </div>
      <div style='margin-top:8px;font-size:.68rem;color:#434651'>{window}</div>
    </div>"""

# ─────────────────────────────────────────────────────────────────────────────
# MAIN DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800, show_spinner=False)
def _sector_scan(group_key, threshold, min_vol, period, top_n, fo_only, use_smp=False):
    """Run full engine on a specific sector. Module-level for st.cache_data compatibility."""
    import pandas as pd  # ensure pd available in cached context
    """Run the full engine on a specific sector and return alerts."""
    import io as _io, contextlib as _cl, glob as _glob

    _cfg           = Cfg()
    _cfg.use_sample        = use_smp
    _cfg.base_threshold    = threshold
    _cfg.bear_threshold    = threshold + 0.08
    _cfg.min_avg_vol       = min_vol
    _cfg.min_traded_val_cr = 2.0          # lower for sector scan
    _cfg.min_rr            = 1.3           # slightly looser
    _cfg.live_period       = period
    _cfg.top_n             = top_n
    _cfg.fetch_fundamentals= False         # speed: skip fundamentals
    _cfg.output_dir        = Path("nse_v10_output")

    if group_key:
        gk = group_key.strip().upper()
        matched = [sl for grp, sl in _UNIVERSE.items() if gk in grp.upper()]
        syms = sorted({s for sub in matched for s in sub} - _SKIP_SYMBOLS)
    else:
        syms = _ALL_SYMS

    if fo_only:
        syms = [s for s in syms if s in _FO_SET]

    _cfg.symbols = syms

    _buf = _io.StringIO()
    with _cl.redirect_stdout(_buf), _cl.redirect_stderr(_buf):
        try:
            _alerts, _bt = run(_cfg)
            _nifty = nifty50_state()
        except Exception as _e:
            return [], {}, {}, str(_e)

    return _alerts, _bt, nifty50_state(), ""



# ─────────────────────────────────────────────────────────────────────────────
# POWER SCAN — High Volatility Bullish + Penny Golden Opportunity
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800, show_spinner=False)
def _power_scan_volatile(threshold, period, top_n, use_smp=False):
    """Scan full NSE for high-momentum volatile stocks with strong bullish signals.
    Engine-level gates relaxed; post-filters select high-conviction momentum plays."""
    import pandas as pd
    import io as _io, contextlib as _cl

    cfg = Cfg()
    cfg.use_sample         = False
    cfg.base_threshold     = threshold
    cfg.bear_threshold     = threshold + 0.06
    cfg.min_avg_vol        = 400_000       # mid-caps included
    cfg.min_traded_val_cr  = 1.0
    cfg.min_rr             = 1.2
    cfg.min_atr_pct        = 0.015         # let engine pass all, filter after
    cfg.max_atr_pct        = 0.20
    cfg.min_price          = 30.0
    cfg.min_categories     = 2
    cfg.live_period        = period
    cfg.top_n              = max(top_n * 4, 40)  # scan wide
    cfg.fetch_fundamentals = False
    cfg.output_dir         = Path("nse_v10_output")
    cfg.symbols            = _ALL_SYMS

    buf = _io.StringIO()
    with _cl.redirect_stdout(buf), _cl.redirect_stderr(buf):
        try:
            alerts_out, bt_out = run(cfg)
        except Exception as e:
            return [], {}, {}, str(e)

    # Post-filter: volatile = ATR% > 2.5% AND ADX > 22 AND vol_ratio > 1.2
    volatile = [
        r for r in alerts_out
        if r["atr_pct"]    >= 2.5
        and r["adx"]       >= 22
        and r["vol_ratio"] >= 1.2
        and r["rsi"]       <= 72
    ]
    # Momentum score = ATR% × ADX × vol_ratio × AI%
    volatile.sort(key=lambda r: -(
        r["atr_pct"] * (r["adx"] / 30) * r["vol_ratio"] * r["ai"]["ai_pct"] / 1000
    ))
    return volatile[:top_n], bt_out, nifty50_state(), ""


@st.cache_data(ttl=1800, show_spinner=False)
def _power_scan_penny(threshold, period, top_n, use_smp=False):
    """Multi-bagger penny stock scanner — finds low-price NSE stocks with
    lifetime breakout setups, volume explosions, and strong AI conviction.
    Price range ₹10–₹500. Looks for: ST flip, breakout, vol surge, EMA alignment."""
    import pandas as pd
    import io as _io, contextlib as _cl

    cfg = Cfg()
    cfg.use_sample         = False
    cfg.base_threshold     = max(threshold - 0.06, 0.08)  # very loose: catch everything
    cfg.bear_threshold     = threshold + 0.02
    cfg.min_avg_vol        = 100_000       # penny stocks: allow very low volume
    cfg.min_traded_val_cr  = 0.3
    cfg.min_rr             = 1.1           # minimum R:R
    cfg.min_atr_pct        = 0.015
    cfg.max_atr_pct        = 0.35          # penny stocks can be very volatile
    cfg.min_categories     = 1             # even 1 category is ok for penny
    cfg.min_price          = 10.0          # true penny starts at ₹10
    cfg.live_period        = period
    cfg.top_n              = max(top_n * 5, 50)  # scan very wide
    cfg.fetch_fundamentals = False
    cfg.output_dir         = Path("nse_v10_output")
    cfg.symbols            = _ALL_SYMS

    buf = _io.StringIO()
    with _cl.redirect_stdout(buf), _cl.redirect_stderr(buf):
        try:
            alerts_out, bt_out = run(cfg)
        except Exception as e:
            return [], {}, {}, str(e)

    # Filter for multi-bagger penny stocks — price ₹10–₹500
    penny_golden = []
    for r in alerts_out:
        price = r["last_close"]
        if price > 500 or price < 10:   # out of penny range
            continue
        hits = r["hits"]
        cats = {h[2] for h in hits}
        top_confs = [h[0] for h in hits[:3]]
        avg_top_conf = sum(top_confs) / max(len(top_confs), 1)

        # Golden criteria: ST flip OR breakout + high AI + multi-category
        has_breakout  = any("Breakout" in h[2] for h in hits)
        has_volume    = any("Volume" in h[2] for h in hits)
        has_stf       = r.get("st_flip", 0)
        has_trend     = any("Trend" in h[2] for h in hits)
        golden_score  = (
            (3.0 if has_stf else 0)
            + (2.5 if has_breakout else 0)
            + (1.5 if has_volume else 0)
            + (1.0 if has_trend else 0)
            + r["ai"]["ai_pct"] / 20
            + r["n_cats"] * 0.5
            + avg_top_conf * 2
        )

        # Must have at least 2 of: breakout, volume surge, ST flip, strong trend
        signals_count = sum([has_breakout, has_volume, bool(has_stf), has_trend])
        if signals_count < 1:  # at least 1 strong signal
            continue

        r = dict(r)  # copy
        r["golden_score"]  = round(golden_score, 2)
        r["has_breakout"]  = has_breakout
        r["has_volume_surge"] = has_volume
        r["has_stf"]       = bool(has_stf)
        r["price_category"]= (
            "💎 Ultra Penny (< ₹50)"   if price < 50   else
            "🔶 Penny (₹50–₹150)"      if price < 150  else
            "🔷 Small Cap (₹150–₹300)" if price < 300  else
            "📘 Mid Value (₹300–₹500)"
        )
        penny_golden.append(r)

    penny_golden.sort(key=lambda r: -r["golden_score"])
    return penny_golden[:top_n], bt_out, nifty50_state(), ""


def run_dashboard(alerts_in, bt_in, nifty_in, feat_df_in):
    # Unique-key counter — every chart/component gets _uid() as its key
    _counter = [0]
    def _uid(prefix="el"):
        _counter[0] += 1
        return f"{prefix}_{_counter[0]:04d}"

    alerts  = alerts_in  or []
    bt      = bt_in      or {}
    nifty   = nifty_in   or {}

    st.markdown(_CSS, unsafe_allow_html=True)

    # ── Nifty values ──────────────────────────────────────────────────────
    trend=nifty.get("trend",0); last=nifty.get("last",0)
    rsi_n=nifty.get("rsi",50);  chg1m=nifty.get("chg_1m",0)
    chg3m=nifty.get("chg_3m",0); lbl=nifty.get("label","N/A")
    e9=nifty.get("ema9",0); e21=nifty.get("ema21",0); e50=nifty.get("ema50",0)
    nt_col="#26a69a" if trend>=0.5 else "#ef5350" if trend<=-0.5 else "#f59e0b"

    avg_ai=sum(r["ai"]["ai_pct"] for r in alerts)/max(len(alerts),1)
    n_fo=sum(1 for r in alerts if r.get("is_fo"))
    bt_ret=bt.get("ret",0); bt_sh=bt.get("sharpe",0)
    bt_dd=bt.get("maxdd",0); bt_wr=bt.get("winrate",0); bt_tr=bt.get("trades",0)

    # ── Ticker tape ───────────────────────────────────────────────────────
    if alerts:
        components.html(tv_ticker_tape([r["symbol"] for r in alerts]), height=55, scrolling=False)

    # ── Header ────────────────────────────────────────────────────────────
    st.markdown(f"""
    <div class='tv-card' style='margin-bottom:16px;padding:14px 20px'>
      <div style='display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px'>
        <div>
          <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.4rem;color:#38bdf8;letter-spacing:.08em'>
            📈 NSE SWING TRADER PRO
          </div>
          <div class='tv-label'>{datetime.now().strftime("%d %b %Y  %H:%M IST")}
            &nbsp;·&nbsp; <span class='blink' style='color:#26a69a'>●</span> LIVE
          </div>
        </div>
        <div style='display:flex;gap:22px;flex-wrap:wrap'>
          <div style='text-align:center'>
            <div class='tv-label'>Nifty50</div>
            <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:#38bdf8'>₹{last:,.2f}</div>
            <div style='font-size:.72rem;color:{nt_col}'>{lbl}</div>
          </div>
          <div style='text-align:center'>
            <div class='tv-label'>1M</div>
            <div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.1rem;color:{"#26a69a" if chg1m>=0 else "#ef5350"}'>{chg1m:+.2f}%</div>
          </div>
          <div style='text-align:center'>
            <div class='tv-label'>Signals</div>
            <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:#26a69a'>{len(alerts)}</div>
          </div>
          <div style='text-align:center'>
            <div class='tv-label'>F&O</div>
            <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:#38bdf8'>{n_fo}</div>
          </div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    # ── KPI row ───────────────────────────────────────────────────────────
    kc = st.columns(7)
    kc[0].metric("🟢 Signals",    f"{len(alerts)}", f"F&O: {n_fo}")
    kc[1].metric("🤖 Avg AI",     f"{avg_ai:.1f}%", f"Top: {alerts[0]['ai']['ai_pct']:.1f}%" if alerts else "—")
    kc[2].metric("📊 BT Return",  f"{bt_ret:+.2%}")
    kc[3].metric("📐 Sharpe",     f"{bt_sh:.3f}")
    kc[4].metric("📉 Max DD",     f"{abs(bt_dd):.2%}")
    kc[5].metric("🎯 Win Rate",   f"{bt_wr:.1%}",   f"{bt_tr} trades")
    kc[6].metric("📡 Nifty RSI",  f"{rsi_n:.1f}")

    if not alerts:
        st.warning("⚠️  No signals. Try --threshold 0.18 or lower --min-vol.")
        return

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Main tabs ─────────────────────────────────────────────────────────
    tabs = st.tabs([
        "🌐  Overview",
        "🏭  Sector Intelligence",
        "🎯  Sector Scan",
        "🔥  Power Scan",
        "📋  Signal Cards",
        "📊  Live Charts",
        "🔬  Analysis",
        "📈  Backtest",
        "🔍  Screener",
        "📰  News & Calendar",
    ])

    # ══════════════════════════════════════════════════════════════════════
    # TAB 0 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════
    with tabs[0]:
        # ── Market + Scan summary ─────────────────────────────────────────
        ov_m, ov_r = st.columns([2, 1.2])

        with ov_m:
            st.markdown("<div class='tv-section'>🌐 Indian Market Overview</div>",
                        unsafe_allow_html=True)
            components.html(tv_market_overview(last, lbl), height=420, scrolling=False)

            # ── Grouped Sector Heatmap ────────────────────────────────────
            st.markdown("<div class='tv-section' style='margin-top:20px'>🗺️ Sector Signal Heatmap — All Groups</div>",
                        unsafe_allow_html=True)

            # Build sector stats from alerts
            _ov_gs: dict = {}
            for _r in alerts:
                for _t in _r["indices"].split(" · "):
                    _t = _t.strip()
                    if not _t or _t == "—": continue
                    _d = _ov_gs.setdefault(_t, {"count":0,"ai":[],"syms":[],"stflip":0})
                    _d["count"] += 1; _d["ai"].append(_r["ai"]["ai_pct"])
                    _d["syms"].append(_r["symbol"])
                    if _r.get("st_flip"): _d["stflip"] += 1

            _OV_GF = {"N50L":"Nifty 50 Leaders","N50":"Nifty 50","NN50":"Nifty Next 50",
                      "MC100":"Nifty Midcap 100","SC250":"Nifty Smallcap 250",
                      "BNK":"Nifty Bank","IT":"Nifty IT","NRG":"Nifty Energy",
                      "AUTO":"Nifty Auto","INFRA":"Nifty Infra","F&O":"F&O Stocks"}
            _OV_GI = {"N50L":"👑","N50":"📊","NN50":"🔵","MC100":"🟡","SC250":"🟠",
                      "BNK":"🏦","IT":"💻","NRG":"⚡","AUTO":"🚗","INFRA":"🏗️","F&O":"🔰"}
            _OV_ORDER = ["BNK","IT","NRG","AUTO","INFRA","N50L","N50","NN50","MC100","SC250","F&O"]

            # Group into Large Cap / Sectoral / Broad
            _OV_GROUPS = {
                "🏦 Sectoral Indices (Theme-Based)": ["BNK","IT","NRG","AUTO","INFRA"],
                "👑 Large Cap Indices":              ["N50L","N50","NN50"],
                "📊 Broad Market Indices":           ["MC100","SC250","F&O"],
            }

            for _grp_name, _grp_tags in _OV_GROUPS.items():
                # Check if any sector in this group has signals
                _grp_has = any(t in _ov_gs for t in _grp_tags)
                with st.expander(_grp_name + (" ✅" if _grp_has else " — No signals"), expanded=_grp_has):
                    for _tag in _grp_tags:
                        _ico  = _OV_GI.get(_tag,"📊")
                        _full = _OV_GF.get(_tag, _tag)
                        if _tag not in _ov_gs:
                            st.markdown(
                                f"<div style='display:flex;align-items:center;gap:10px;padding:8px 12px;"
                                f"background:#0b0e11;border-radius:4px;margin-bottom:5px;opacity:.45'>"
                                f"<span style='font-size:1rem'>{_ico}</span>"
                                f"<span style='font-size:.82rem;color:#434651'>{_full}</span>"
                                f"<span style='font-size:.72rem;color:#434651;margin-left:auto'>No signals</span>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                            continue

                        _d   = _ov_gs[_tag]; _cnt = _d["count"]; _avg = sum(_d["ai"]) / _cnt
                        _stf = _d["stflip"]
                        if _avg >= 78 and _cnt >= 3:   _hc, _verdict = "#26a69a","🔥 HOT"
                        elif _avg >= 65 or _cnt >= 3:  _hc, _verdict = "#4db6ac","🟢 BULL"
                        elif _avg >= 50 or _cnt >= 2:  _hc, _verdict = "#f59e0b","🟡 NEUTRAL"
                        else:                           _hc, _verdict = "#ef5350","🔴 WEAK"

                        _bar_w = max(4, int(min(_cnt/max(len(alerts)*0.25,1),1)*200))
                        _syms_html = "  ".join(
                            f"<span style='background:{_hc}22;color:{_hc};padding:1px 7px;"
                            f"border-radius:2px;font-size:.72rem;font-weight:600'>{s}</span>"
                            for s in _d["syms"][:6]
                        ) + (f"<span style='color:#434651;font-size:.7rem'> +{len(_d['syms'])-6}</span>"
                             if len(_d["syms"]) > 6 else "")

                        st.markdown(
                            f"<div style='padding:10px 14px;margin-bottom:6px;background:#131722;"
                            f"border-radius:4px;border-left:3px solid {_hc}'>"
                            # Row 1: icon + name + verdict + count + stflip
                            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:7px'>"
                            f"<span style='font-size:1.1rem'>{_ico}</span>"
                            f"<span style='font-family:Syne,sans-serif;font-weight:700;font-size:.9rem;color:#d1d4dc'>{_full}</span>"
                            f"<span style='font-size:.72rem;color:{_hc};font-weight:700;margin-left:4px'>{_verdict}</span>"
                            f"<span style='font-size:.7rem;color:#787b86;margin-left:auto'>{_cnt} signals</span>"
                            + (f"<span style='background:#a855f722;color:#a855f7;padding:1px 7px;border-radius:2px;font-size:.68rem'>⚡ {_stf} ST Flip</span>" if _stf else "")
                            + f"</div>"
                            # Row 2: heat bar + avg AI
                            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:8px'>"
                            f"<div style='flex:1;height:5px;background:#1c2030;border-radius:3px'>"
                            f"<div style='height:5px;width:{_bar_w}px;max-width:100%;background:{_hc};border-radius:3px'></div></div>"
                            f"<span style='font-size:.78rem;font-weight:700;color:{_hc};min-width:46px'>AI {_avg:.0f}%</span>"
                            f"<div style='min-width:80px;text-align:right'>"
                            f"{gauge_html(_avg,'',80)}"
                            f"</div></div>"
                            # Row 3: symbol pills
                            f"<div style='line-height:1.8'>{_syms_html}</div>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )

            # TradingView NSE heatmap
            st.markdown("<div class='tv-section' style='margin-top:16px'>🌐 TradingView Live NSE Heatmap</div>",
                        unsafe_allow_html=True)
            components.html(tv_heatmap(), height=480, scrolling=False)

        with ov_r:
            st.markdown("<div class='tv-section'>⭐ Top 8 Signals</div>", unsafe_allow_html=True)
            for r in alerts[:8]:
                ai_p   = r["ai"]["ai_pct"]
                ltr, gc = _d_grade(ai_p)
                stl    = r["levels"]["short_term"]
                up     = (stl["tp"] / stl["entry"] - 1) * 100
                fo_tag = "<span class='pill-cyan'>F&amp;O</span>" if r["is_fo"] else ""
                gauge  = gauge_html(ai_p, "AI Score", 180)
                html_card = (
                    "<div class='tv-card tv-card-bull' style='padding:10px 14px;margin-bottom:8px'>"
                    "<div style='display:flex;justify-content:space-between;align-items:center'>"
                    "<div style='display:flex;align-items:center;gap:8px'>"
                    f"<span class='tv-badge-sym' style='font-size:.85rem;padding:2px 10px'>{r['symbol']}</span>"
                    f"<span class='tv-badge-grade' style='background:{gc}22;color:{gc};font-size:.9rem;padding:2px 8px'>{ltr}</span>"
                    f"{fo_tag}"
                    "</div>"
                    "<div style='text-align:right'>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;color:#38bdf8'>&#8377;{r['last_close']:,.2f}</div>"
                    f"<div style='font-size:.7rem;color:#26a69a'>&#9650; {up:+.1f}% to target</div>"
                    "</div></div>"
                    f"<div style='margin-top:5px'>{gauge}</div>"
                    f"<div style='font-size:.68rem;color:#434651;margin-top:3px'>{r['indices']}</div>"
                    "</div>"
                )
                st.markdown(html_card, unsafe_allow_html=True)

            st.markdown("<div class='tv-section' style='margin-top:16px'>📊 Score Distribution</div>",
                        unsafe_allow_html=True)
            st.plotly_chart(score_distribution_fig(alerts), use_container_width=True,
                            config={"displayModeBar": False}, key=_uid("pc"))


    # TAB 1 — SECTOR INTELLIGENCE
    # ══════════════════════════════════════════════════════════════════════
    with tabs[1]:

        # ── SECTOR DEFINITIONS WITH INTEL ────────────────────────────────
        SECTOR_INTEL = {
            "BNK": {
                "name": "Nifty Bank",
                "icon": "🏦",
                "description": "The backbone of Indian equity markets. Banks drive credit growth, NIM expansion, and are highly sensitive to RBI rate decisions and liquidity conditions.",
                "key_drivers": ["RBI rate policy & liquidity","Credit growth (YoY)","NPA & provisioning levels","CASA ratio trends","Net Interest Margins (NIM)"],
                "watch_macro": ["RBI Monetary Policy Committee dates","CPI / WPI inflation prints","10-year G-Sec yield","FII flow into financials","Q results: HDFC Bank, ICICI Bank, SBI"],
                "bull_catalyst": ["Rate cut cycle begins","Credit growth above 14%","NPA ratios declining","Strong CASA growth"],
                "bear_catalyst": ["Rate hikes / prolonged pause","NPA spike","Liquidity tightening","Slowdown in credit"],
                "symbols_all": ["HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK","INDUSINDBK","BANDHANBNK","FEDERALBNK","AUBANK","IDFCFIRSTB","PNB","BANKBARODA","CANBK","INDIANB","UNIONBANK","RBLBANK"],
            },
            "IT": {
                "name": "Nifty IT",
                "icon": "💻",
                "description": "Export-driven sector closely tied to US/Europe tech spending. Revenue in USD makes it a natural hedge against INR depreciation. Margin-sensitive to wage inflation.",
                "key_drivers": ["US IT budgets & deal wins","USD/INR exchange rate","Attrition & wage costs","AI/Cloud adoption pace","Employee utilisation rates"],
                "watch_macro": ["US GDP & unemployment data","Fed rate decisions (USD impact)","Quarterly guidance from Accenture/IBM","TCS & Infosys deal announcements","H-1B visa policy"],
                "bull_catalyst": ["USD strengthens vs INR","US tech spending recovery","Large deal wins","AI-driven revenue acceleration"],
                "bear_catalyst": ["USD weakens","US recession fears","Budget cuts at enterprise clients","Margin pressure from hikes"],
                "symbols_all": ["TCS","INFY","HCLTECH","WIPRO","TECHM","LTM","MPHASIS","COFORGE","PERSISTENT","KPITTECH","TATAELXSI","OFSS","NAUKRI"],
            },
            "NRG": {
                "name": "Nifty Energy",
                "icon": "⚡",
                "description": "Blend of legacy oil & gas and fast-growing renewable energy. Government capex push in solar/wind provides long tailwind while crude prices drive PSU upstream earnings.",
                "key_drivers": ["Global crude oil prices (Brent)","Renewable capacity additions (GW)","Government PLI & subsidy policies","Refining margins (GRM)","Power demand growth"],
                "watch_macro": ["OPEC+ production decisions","INR/USD (crude import cost)","Union Budget energy allocations","REC/IREDA bond issuances","State electricity tariff revisions"],
                "bull_catalyst": ["Crude above $80 (upstream)","Renewable capacity targets raised","Rate cuts boost infra capex","Power demand surge"],
                "bear_catalyst": ["Crude collapse below $65","Subsidy rollback","Interest rate spike hurts capex","Slow renewable execution"],
                "symbols_all": ["RELIANCE","ONGC","NTPC","POWERGRID","TATAPOWER","ADANIGREEN","ADANIPOWER","JSWENERGY","NHPC","IREDA","SUZLON","INOXWIND","WAAREEENER","TORNTPOWER","BPCL","IOC","HINDPETRO","OIL","GAIL","PETRONET","COALINDIA"],
            },
            "AUTO": {
                "name": "Nifty Auto",
                "icon": "🚗",
                "description": "Cyclical sector driven by consumer sentiment, rural income, and the EV transition. Two-wheelers proxy for rural demand; PVs proxy for urban affluence. EV disruption is structural.",
                "key_drivers": ["Monthly auto sales volumes","Rural income & Kharif/Rabi crop output","Fuel prices (petrol/diesel)","EV penetration rate","Input costs (steel, aluminium, semiconductors)"],
                "watch_macro": ["SIAM monthly sales data (2nd of every month)","Monsoon progress (rural sentiment)","Commodity price index","PLI incentives for EV","Budget: excise duty on vehicles"],
                "bull_catalyst": ["Strong festive season sales","Rural recovery post monsoon","EV subsidy expansion","Commodity deflation"],
                "bear_catalyst": ["Weak monsoon → rural slump","Fuel price spike","Commodity cost push","EV disruption to ICE OEMs"],
                "symbols_all": ["MARUTI","M&M","TATAMOTORS","BAJAJ-AUTO","EICHERMOT","HEROMOTOCO","TVSMOTOR","MOTHERSON","BOSCHLTD","TIINDIA","SONACOMS","UNOMINDA","EXIDEIND","BHARATFORG"],
            },
            "INFRA": {
                "name": "Nifty Infra",
                "icon": "🏗️",
                "description": "Government capex-driven sector. National infrastructure pipeline (NIP), roads, railways, urban metro, ports, and housing drive order books. Long cycle, high visibility.",
                "key_drivers": ["Union Budget capex allocation","NHAI & railway order awards","Order book growth & execution","Interest rates (high leverage)","Working capital cycle"],
                "watch_macro": ["Monthly infrastructure output index","NHAI toll collection data","L&T order inflows (industry proxy)","RBI rate trajectory","State capex budgets"],
                "bull_catalyst": ["Budget capex increase","Rate cut cycle","Large order wins","Pre-election government spending"],
                "bear_catalyst": ["Fiscal consolidation → capex cut","Rate hike → higher borrowing cost","Project delays / land acquisition issues","Commodity cost overruns"],
                "symbols_all": ["LT","ADANIPORTS","POWERGRID","NTPC","COALINDIA","BHEL","SIEMENS","ABB","HAVELLS","POLYCAB","KEI","CUMMINSIND","RVNL","NBCC","HUDCO","IRFC","GMRAIRPORT","CONCOR","DELHIVERY","DLF","LODHA"],
            },
            "N50L": {"name":"Nifty 50 Leaders","icon":"👑","description":"Largest-cap blue chips. Lead the broader index. FII ownership is highest here; flows drive sharp moves. Safe haven in risk-off; laggards in risk-on.","key_drivers":["FII net flows","Index rebalancing","Macro GDP outlook","Earnings season consensus"],"watch_macro":["FII/DII daily data","NSE/BSE index changes","RBI policy","US Fed FOMC"],"bull_catalyst":["FII inflows","GDP upgrade","Broad market bull run"],"bear_catalyst":["FII outflows","Global risk-off","Rupee depreciation"],"symbols_all":["RELIANCE","HDFCBANK","ICICIBANK","TCS","INFY","SBIN","BHARTIARTL","LT","AXISBANK","KOTAKBANK","ITC","HINDUNILVR","BAJFINANCE","SUNPHARMA","TITAN","MARUTI","M&M","NTPC","POWERGRID","ADANIPORTS"]},
            "N50":  {"name":"Nifty 50","icon":"📊","description":"India's benchmark index. 50 largest listed companies. Performance benchmark for most funds. Tracks economic cycles closely.","key_drivers":["Broad macro","Earnings growth","Global risk appetite","Domestic flows"],"watch_macro":["PCE / CPI global","RBI bi-monthly policy","Quarterly GDP prints","Q earnings season"],"bull_catalyst":["Earnings upgrade cycle","DII SIP inflows","Global bull market"],"bear_catalyst":["Earnings downgrade","Global recession","FII selling"],"symbols_all":["RELIANCE","HDFCBANK","BHARTIARTL","SBIN","ICICIBANK","TCS","BAJFINANCE","INFY","HINDUNILVR","LT","SUNPHARMA","MARUTI"]},
            "NN50": {"name":"Nifty Next 50","icon":"🔵","description":"Mid-large caps poised for Nifty 50 inclusion. Higher beta than Nifty 50; outperform in bull markets, underperform in corrections.","key_drivers":["Nifty 50 inclusion probability","Index rebalancing flows","Sector rotation"],"watch_macro":["NSE index reconstitution","Mid-cap flows","Earnings visibility"],"bull_catalyst":["Nifty 50 upgrade","Sector bull run","Liquidity expansion"],"bear_catalyst":["Mid-cap selloff","Index exclusion risk","Liquidity tightening"],"symbols_all":["LICI","ADANIGREEN","HAL","SIEMENS","GODREJCP","PIDILITIND","DMART","MARICO","BRITANNIA","HAVELLS","GAIL"]},
            "MC100":{"name":"Nifty Midcap 100","icon":"🟡","description":"High-growth mid-sized companies. Highest earnings growth potential in bull markets. Volatile in corrections but multi-bagger potential in 2–3 year horizon.","key_drivers":["Earnings growth momentum","Domestic institutional flows","Sector-specific tailwinds","Promoter stake changes"],"watch_macro":["BSE Midcap PE vs historical","DII/SIP flows","Smallcap-midcap rotation"],"bull_catalyst":["DII inflows","Domestic consumption boom","Sector re-rating"],"bear_catalyst":["Liquidity crunch","Earnings miss","Mid-cap PE de-rating"],"symbols_all":["TVSMOTOR","CHOLAFIN","MUTHOOTFIN","LUPIN","AUROPHARMA","DIVISLAB","ALKEM","TORNTPHARM","AUBANK","FEDERALBNK","RBLBANK","SRF","ASTRAL"]},
            "SC250":{"name":"Nifty Smallcap 250","icon":"🟠","description":"High-risk, high-reward. Driven by domestic retail flows and SIP money. Illiquid in corrections. Disproportionate upside in broad bull runs.","key_drivers":["Retail investor sentiment","SIP inflows","Stock-specific catalysts","Promoter quality"],"watch_macro":["AMFI SIP data","VIX index","Smallcap vs largecap PE spread","Margin trading data"],"bull_catalyst":["SIP surge","Retail confidence","Momentum factor","Bullish macro"],"bear_catalyst":["VIX spike","Retail panic selling","Liquidity dry-up","Earnings disappoint"],"symbols_all":["IREDA","RVNL","IRFC","NHPC","HUDCO","SJVN","NBCC","PNBHOUSING","MANAPPURAM","INOXWIND","WAAREEENER","OIL","COLPAL","EMAMILTD"]},
            "F&O": {"name":"F&O Stocks","icon":"🔰","description":"Futures & Options eligible stocks — most liquid NSE stocks. Used for hedging, leverage, and directional bets. Option OI data provides sentiment clues.","key_drivers":["Open Interest build-up / unwinding","PCR (Put-Call Ratio)","Max Pain level","IV (Implied Volatility)","Rollover data"],"watch_macro":["Weekly F&O expiry (Thursday)","PCR of Nifty/BankNifty","NSE F&O ban list","VIX trend"],"bull_catalyst":["PCR rising (more puts written)","OI build on calls unwinding","IV compression"],"bear_catalyst":["PCR falling below 0.7","High IV spike","OI build on puts"],"symbols_all":["RELIANCE","HDFCBANK","ICICIBANK","SBIN","TCS","INFY","AXISBANK","KOTAKBANK","LT","BAJFINANCE","WIPRO","HCLTECH"]},
        }

        # ── Build sector stats from alerts ───────────────────────────────
        all_symbols_seen = {r["symbol"] for r in alerts}

        sector_stats: dict = {}
        for r_s in alerts:
            for tag in r_s["indices"].split(" · "):
                tag = tag.strip()
                if not tag or tag == "—": continue
                d = sector_stats.setdefault(tag, {
                    "bullish": [], "all_ai": [], "all_sc": [],
                    "all_rsi": [], "all_adx": [], "all_vol": [],
                })
                d["bullish"].append(r_s)
                d["all_ai"].append(r_s["ai"]["ai_pct"])
                d["all_sc"].append(r_s["score"])
                d["all_rsi"].append(r_s["rsi"])
                d["all_adx"].append(r_s["adx"])
                d["all_vol"].append(r_s["vol_ratio"])

        # Compute sector bullishness scores
        def _sec_bull(d):
            if not d["all_ai"]: return 0
            n = len(d["all_ai"])
            return (
                0.35 * (sum(d["all_ai"]) / n / 100)
                + 0.25 * min(sum(d["all_sc"]) / n / 0.5, 1.0)
                + 0.20 * min(sum(d["all_adx"]) / n / 40, 1.0)
                + 0.10 * min(sum(d["all_vol"]) / n / 2.5, 1.0)
                + 0.10 * max((70 - sum(d["all_rsi"]) / n) / 40, 0)
            )

        TAG_ORDER = ["BNK","IT","NRG","AUTO","INFRA","N50L","N50","NN50","MC100","SC250","F&O"]

        # ── MARKET PULSE HEADER ───────────────────────────────────────────
        bullish_sectors = [t for t in TAG_ORDER if t in sector_stats and _sec_bull(sector_stats[t]) >= 0.55]
        neutral_sectors = [t for t in TAG_ORDER if t in sector_stats and 0.35 <= _sec_bull(sector_stats[t]) < 0.55]
        weak_sectors    = [t for t in TAG_ORDER if t in sector_stats and _sec_bull(sector_stats[t]) < 0.35]
        empty_sectors   = [t for t in TAG_ORDER if t not in sector_stats]

        overall_bias = "#26a69a" if len(bullish_sectors) >= 4 else "#f59e0b" if len(bullish_sectors) >= 2 else "#ef5350"
        overall_txt  = "🐂 BROAD MARKET BULLISH" if len(bullish_sectors) >= 4 else "🔄 MIXED MARKET" if len(bullish_sectors) >= 2 else "🐻 DEFENSIVE — MARKET WEAK"

        st.markdown(f"""
        <div class='tv-card' style='padding:16px 20px;border-color:{overall_bias}99;margin-bottom:16px'>
          <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:{overall_bias};margin-bottom:10px'>
            {overall_txt}
          </div>
          <div style='display:flex;gap:28px;flex-wrap:wrap'>
            <div>
              <div class='tv-label'>Bullish Sectors</div>
              <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.5rem;color:#26a69a'>{len(bullish_sectors)}</div>
              <div style='font-size:.72rem;color:#26a69a'>{'  ·  '.join(SECTOR_INTEL.get(t,{}).get('icon','📊')+' '+t for t in bullish_sectors) or '—'}</div>
            </div>
            <div>
              <div class='tv-label'>Neutral Sectors</div>
              <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.5rem;color:#f59e0b'>{len(neutral_sectors)}</div>
              <div style='font-size:.72rem;color:#f59e0b'>{'  ·  '.join(SECTOR_INTEL.get(t,{}).get('icon','📊')+' '+t for t in neutral_sectors) or '—'}</div>
            </div>
            <div>
              <div class='tv-label'>Weak / No Signals</div>
              <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.5rem;color:#ef5350'>{len(weak_sectors) + len(empty_sectors)}</div>
              <div style='font-size:.72rem;color:#ef5350'>{'  ·  '.join(SECTOR_INTEL.get(t,{}).get('icon','📊')+' '+t for t in weak_sectors+empty_sectors) or '—'}</div>
            </div>
            <div>
              <div class='tv-label'>Total Bullish Stocks</div>
              <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.5rem;color:#38bdf8'>{len(alerts)}</div>
            </div>
          </div>
        </div>""", unsafe_allow_html=True)

        # ── SECTOR FILTER ─────────────────────────────────────────────────
        filter_c1, filter_c2 = st.columns([2, 1])
        with filter_c1:
            view_filter = st.radio(
                "Show", ["🟢 Bullish Only", "🟡 Neutral Too", "📋 All Sectors"],
                horizontal=True, key="sec_filter"
            )
        with filter_c2:
            expand_all = st.toggle("Expand All", value=False, key="sec_expand")

        if view_filter == "🟢 Bullish Only":
            display_order = bullish_sectors + neutral_sectors
        elif view_filter == "🟡 Neutral Too":
            display_order = bullish_sectors + neutral_sectors + weak_sectors
        else:
            display_order = [t for t in TAG_ORDER if t in sector_stats] + empty_sectors

        # ── PER-SECTOR DETAILED CARDS ─────────────────────────────────────
        for tag in display_order:
            intel = SECTOR_INTEL.get(tag, {
                "name": tag, "icon": "📊", "description": "",
                "key_drivers": [], "watch_macro": [], "bull_catalyst": [], "bear_catalyst": [],
                "symbols_all": [],
            })

            has_data  = tag in sector_stats
            d         = sector_stats.get(tag, {"bullish":[],"all_ai":[],"all_sc":[],"all_rsi":[],"all_adx":[],"all_vol":[]})
            bull_pct  = _sec_bull(d) if has_data else 0
            n_bull    = len(d["bullish"])
            avg_ai    = sum(d["all_ai"]) / max(n_bull, 1)
            avg_adx   = sum(d["all_adx"]) / max(n_bull, 1)
            avg_rsi   = sum(d["all_rsi"]) / max(n_bull, 1)
            avg_vol   = sum(d["all_vol"]) / max(n_bull, 1)
            avg_sc    = sum(d["all_sc"])  / max(n_bull, 1)

            sec_col   = "#26a69a" if bull_pct >= 0.55 else "#f59e0b" if bull_pct >= 0.35 else "#ef5350"
            sec_label = "🟢 BULLISH" if bull_pct >= 0.55 else "🟡 NEUTRAL" if bull_pct >= 0.35 else "🔴 WEAK"
            sec_label = "⬜ NO SIGNALS" if not has_data else sec_label

            # Identify which sector symbols are bearish (in universe but NOT in alerts)
            all_sym_in_sector = set(intel.get("symbols_all", [])) - _SKIP_SYMBOLS
            bullish_syms  = {r["symbol"] for r in d["bullish"]}
            bearish_syms  = all_sym_in_sector - all_symbols_seen          # not scanned or no signal
            neutral_syms  = (all_symbols_seen & all_sym_in_sector) - bullish_syms  # scanned but no signal

            expander_title = (
                f"{intel['icon']}  {intel['name']}  ·  {sec_label}"
                f"{'  ·  Bullishness: ' + str(round(bull_pct*100)) + '%' if has_data else ''}"
                f"{'  ·  ' + str(n_bull) + ' bullish stocks' if n_bull else ''}"
            )

            with st.expander(expander_title, expanded=(expand_all or bull_pct >= 0.55)):

                # ── ROW A: Description + Key Drivers + Macro Watch ────────
                rA1, rA2, rA3 = st.columns([2, 1.2, 1.2])

                with rA1:
                    st.markdown(
                        f"<div class='tv-card' style='border-left:3px solid {sec_col};height:100%'>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:.95rem;"
                        f"color:{sec_col};margin-bottom:8px'>{intel['icon']} {intel['name']}</div>"
                        f"<div style='font-size:.82rem;color:#787b86;line-height:1.7'>{intel['description']}</div>"
                        f"<div style='margin-top:10px;padding-top:8px;border-top:1px solid #2a3347;"
                        f"display:flex;gap:16px;flex-wrap:wrap'>"
                        f"<span style='font-size:.72rem'>"
                        f"<b style='color:{sec_col}'>Bullishness:</b> <span style='color:#d1d4dc'>{bull_pct*100:.0f}%</span></span>"
                        f"<span style='font-size:.72rem'>"
                        f"<b style='color:#38bdf8'>Bullish Stocks:</b> <span style='color:#26a69a'>{n_bull}</span></span>"
                        f"<span style='font-size:.72rem'>"
                        f"<b style='color:#f59e0b'>Neutral:</b> <span style='color:#d1d4dc'>{len(neutral_syms)}</span></span>"
                        f"</div></div>",
                        unsafe_allow_html=True,
                    )

                with rA2:
                    drivers_html = "".join(
                        f"<div style='display:flex;align-items:flex-start;gap:6px;margin-bottom:5px'>"
                        f"<span style='color:#38bdf8;font-size:.75rem;margin-top:1px'>▸</span>"
                        f"<span style='font-size:.78rem;color:#787b86'>{d_}</span></div>"
                        for d_ in intel.get("key_drivers", [])
                    )
                    st.markdown(
                        f"<div class='tv-card' style='height:100%'>"
                        f"<div class='tv-label' style='margin-bottom:8px'>📌 KEY DRIVERS</div>"
                        f"{drivers_html}</div>",
                        unsafe_allow_html=True,
                    )

                with rA3:
                    macro_html = "".join(
                        f"<div style='display:flex;align-items:flex-start;gap:6px;margin-bottom:5px'>"
                        f"<span style='color:#f59e0b;font-size:.75rem;margin-top:1px'>◆</span>"
                        f"<span style='font-size:.78rem;color:#787b86'>{m_}</span></div>"
                        for m_ in intel.get("watch_macro", [])
                    )
                    st.markdown(
                        f"<div class='tv-card' style='height:100%'>"
                        f"<div class='tv-label' style='margin-bottom:8px'>👁️ WATCH LIST</div>"
                        f"{macro_html}</div>",
                        unsafe_allow_html=True,
                    )

                # ── ROW B: Bull Catalysts + Bear Catalysts ─────────────────
                rB1, rB2 = st.columns(2)
                with rB1:
                    bull_c_html = "".join(
                        f"<div style='display:flex;align-items:flex-start;gap:6px;margin-bottom:5px'>"
                        f"<span style='color:#26a69a;font-size:.8rem'>✅</span>"
                        f"<span style='font-size:.8rem;color:#d1d4dc'>{c_}</span></div>"
                        for c_ in intel.get("bull_catalyst", [])
                    )
                    st.markdown(
                        f"<div class='tv-card' style='border-left:2px solid #26a69a'>"
                        f"<div class='tv-label' style='margin-bottom:8px;color:#26a69a'>🟢 BULLISH CATALYSTS</div>"
                        f"{bull_c_html}</div>",
                        unsafe_allow_html=True,
                    )
                with rB2:
                    bear_c_html = "".join(
                        f"<div style='display:flex;align-items:flex-start;gap:6px;margin-bottom:5px'>"
                        f"<span style='color:#ef5350;font-size:.8rem'>⚠️</span>"
                        f"<span style='font-size:.8rem;color:#d1d4dc'>{c_}</span></div>"
                        for c_ in intel.get("bear_catalyst", [])
                    )
                    st.markdown(
                        f"<div class='tv-card' style='border-left:2px solid #ef5350'>"
                        f"<div class='tv-label' style='margin-bottom:8px;color:#ef5350'>🔴 BEARISH RISKS</div>"
                        f"{bear_c_html}</div>",
                        unsafe_allow_html=True,
                    )

                # ── ROW C: Sector KPIs ─────────────────────────────────────
                if has_data:
                    k1,k2,k3,k4,k5,k6 = st.columns(6)
                    k1.metric("Bullishness",   f"{bull_pct*100:.1f}%")
                    k2.metric("Avg AI Score",  f"{avg_ai:.1f}%")
                    k3.metric("Avg ADX",       f"{avg_adx:.1f}")
                    k4.metric("Avg RSI",       f"{avg_rsi:.1f}")
                    k5.metric("Avg Volume",    f"{avg_vol:.2f}×")
                    k6.metric("Avg Score",     f"{avg_sc:+.3f}")

                # ── ROW D: BULLISH STOCKS ─────────────────────────────────
                if d["bullish"]:
                    st.markdown(
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:.82rem;"
                        f"letter-spacing:.1em;color:#26a69a;text-transform:uppercase;"
                        f"padding:8px 0 6px;border-bottom:1px solid #1c2030;margin:10px 0 8px'>"
                        f"🟢 BULLISH STOCKS ({n_bull}) — Currently Showing Buy Signals</div>",
                        unsafe_allow_html=True,
                    )

                    sorted_bull = sorted(d["bullish"], key=lambda x: -x["ai"]["ai_pct"])

                    # Show 3-column grid of bullish stock cards
                    cols_per_row = 3
                    rows_b = [sorted_bull[i:i+cols_per_row] for i in range(0, len(sorted_bull), cols_per_row)]
                    for row_b in rows_b:
                        rcols = st.columns(cols_per_row)
                        for ci, r_b in enumerate(row_b):
                            ai_b    = r_b["ai"]["ai_pct"]
                            ltr_b, gc_b = _d_grade(ai_b)
                            stl_b   = r_b["levels"]["short_term"]
                            ltl_b   = r_b["levels"]["long_term"]
                            up_b    = (stl_b["tp"] / stl_b["entry"] - 1) * 100
                            hits_b  = r_b["hits"]
                            sig_b   = hits_b[0][1] if hits_b else "—"
                            fo_b    = "🔰 F&O" if r_b["is_fo"] else ""
                            stf_b   = "⚡ ST FLIP" if r_b.get("st_flip") else ""
                            gb_b    = gauge_html(ai_b, "", 160)
                            rsi_cb  = _d_rsi_col(r_b["rsi"])
                            adx_cb  = _d_adx_col(r_b["adx"])
                            rcols[ci].markdown(
                                f"<div class='tv-card tv-card-bull' style='padding:12px;border-color:{gc_b}55'>"
                                # Header
                                f"<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>"
                                f"<span class='tv-badge-sym' style='font-size:.9rem;padding:3px 10px'>{r_b['symbol']}</span>"
                                f"<span class='tv-badge-grade' style='background:{gc_b}22;color:{gc_b};font-size:.9rem'>{ltr_b}</span>"
                                f"</div>"
                                # Price
                                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.1rem;color:#38bdf8;margin-bottom:4px'>&#8377;{r_b['last_close']:,.2f}</div>"
                                # Tags
                                f"<div style='display:flex;gap:5px;margin-bottom:8px;flex-wrap:wrap'>"
                                + (f"<span class='pill-cyan' style='font-size:.62rem'>{fo_b}</span>" if fo_b else "")
                                + (f"<span class='pill-amber' style='font-size:.62rem'>{stf_b}</span>" if stf_b else "")
                                + f"</div>"
                                # Metrics grid
                                f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:4px;margin-bottom:8px;font-size:.72rem'>"
                                f"<div><span style='color:#434651'>AI:</span> <b style='color:{gc_b}'>{ai_b:.0f}%</b></div>"
                                f"<div><span style='color:#434651'>Score:</span> <b style='color:#d1d4dc'>{r_b['score']:+.3f}</b></div>"
                                f"<div><span style='color:#434651'>RSI:</span> <b style='color:{rsi_cb}'>{r_b['rsi']:.1f}</b></div>"
                                f"<div><span style='color:#434651'>ADX:</span> <b style='color:{adx_cb}'>{r_b['adx']:.1f}</b></div>"
                                f"<div><span style='color:#434651'>Vol:</span> <b style='color:#d1d4dc'>{r_b['vol_ratio']:.2f}&times;</b></div>"
                                f"<div><span style='color:#434651'>ATR:</span> <b style='color:#d1d4dc'>{r_b['atr_pct']:.1f}%</b></div>"
                                f"</div>"
                                # Trade levels
                                f"<div style='background:#1c2030;border-radius:3px;padding:7px;margin-bottom:8px;font-size:.72rem'>"
                                f"<div style='display:flex;justify-content:space-between;margin-bottom:3px'>"
                                f"<span style='color:#434651'>Entry</span><b style='color:#38bdf8'>&#8377;{stl_b['entry']:,.2f}</b></div>"
                                f"<div style='display:flex;justify-content:space-between;margin-bottom:3px'>"
                                f"<span style='color:#434651'>ST Target</span><b style='color:#26a69a'>&#8377;{stl_b['tp']:,.2f} ({up_b:+.1f}%)</b></div>"
                                f"<div style='display:flex;justify-content:space-between;margin-bottom:3px'>"
                                f"<span style='color:#434651'>Stop Loss</span><b style='color:#ef5350'>&#8377;{stl_b['sl']:,.2f}</b></div>"
                                f"<div style='display:flex;justify-content:space-between;margin-bottom:3px'>"
                                f"<span style='color:#434651'>R:R</span><b style='color:#d1d4dc'>{stl_b['rr_str']}</b></div>"
                                f"<div style='display:flex;justify-content:space-between'>"
                                f"<span style='color:#434651'>LT Target</span><b style='color:#4db6ac'>&#8377;{ltl_b['tp']:,.2f}</b></div>"
                                f"</div>"
                                # Gauge + signal
                                f"{gb_b}"
                                f"<div style='font-size:.68rem;color:#434651;margin-top:5px;line-height:1.4'>&#127919; {sig_b[:55]}</div>"
                                "</div>",
                                unsafe_allow_html=True,
                            )

                # ── ROW E: NEUTRAL / NO-SIGNAL STOCKS ─────────────────────
                if neutral_syms:
                    neutral_sorted = sorted(list(neutral_syms))
                    neutral_pills  = "".join(
                        f"<span style='background:#1c2030;color:#787b86;padding:3px 9px;border-radius:3px;"
                        f"font-size:.75rem;margin:2px 3px;display:inline-block'>{sym}</span>"
                        for sym in neutral_sorted
                    )
                    st.markdown(
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:.82rem;"
                        f"letter-spacing:.1em;color:#f59e0b;text-transform:uppercase;"
                        f"padding:8px 0 6px;border-bottom:1px solid #1c2030;margin:10px 0 8px'>"
                        f"🟡 NEUTRAL / WATCHING ({len(neutral_syms)}) — Scanned but no buy signal yet</div>"
                        f"<div style='margin-bottom:8px'>{neutral_pills}</div>",
                        unsafe_allow_html=True,
                    )

                # ── ROW F: BEARISH / NOT SIGNALLING STOCKS ─────────────────
                # Bearish = in sector universe but NOT in our scanned universe (or below all gates)
                if bearish_syms:
                    bear_sorted = sorted(list(bearish_syms))[:20]
                    bear_pills  = "".join(
                        f"<span style='background:#1c203080;color:#434651;padding:3px 9px;border-radius:3px;"
                        f"font-size:.75rem;margin:2px 3px;display:inline-block;text-decoration:line-through'>{sym}</span>"
                        for sym in bear_sorted
                    )
                    st.markdown(
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:.82rem;"
                        f"letter-spacing:.1em;color:#ef5350;text-transform:uppercase;"
                        f"padding:8px 0 6px;border-bottom:1px solid #1c2030;margin:10px 0 8px'>"
                        f"🔴 BEARISH / NO SIGNAL ({len(bearish_syms)}) — Did not pass quality gates</div>"
                        f"<div style='margin-bottom:6px'>{bear_pills}"
                        + (f"<span style='font-size:.7rem;color:#434651;margin-left:6px'>+{len(bearish_syms)-20} more</span>"
                           if len(bearish_syms) > 20 else "") +
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                # ── ROW G: Live TradingView chart for top pick ──────────────
                if d["bullish"]:
                    best_sym = sorted(d["bullish"], key=lambda x: -x["ai"]["ai_pct"])[0]["symbol"]
                    st.markdown(
                        f"<div class='tv-label' style='margin:10px 0 4px'>"
                        f"📊 Live Chart — {best_sym} (Top Pick in {intel['name']})</div>",
                        unsafe_allow_html=True,
                    )
                    components.html(
                        tv_mini_chart(best_sym, height=200, nonce=f"sec_{tag}_{best_sym}"),
                        height=204, scrolling=False,
                    )

                st.markdown("<div style='margin-bottom:8px'></div>", unsafe_allow_html=True)

        # ── EMPTY SECTORS ─────────────────────────────────────────────────
        if empty_sectors and view_filter == "📋 All Sectors":
            for tag in empty_sectors:
                intel = SECTOR_INTEL.get(tag, {"name": tag, "icon": "📊"})
                st.markdown(
                    f"<div style='background:#0b0e11;border:1px solid #1c2030;border-radius:4px;"
                    f"padding:12px 16px;margin-bottom:6px;opacity:.5'>"
                    f"<span style='color:#434651;font-size:.82rem'>{intel['icon']} {intel['name']}</span>"
                    f"<span style='color:#434651;font-size:.72rem;margin-left:12px'>No signals in this scan</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )


    # ══════════════════════════════════════════════════════════════════════
    # TAB 2 — SECTOR SCAN  (engine-backed per-sector signal discovery)
    # ══════════════════════════════════════════════════════════════════════
    with tabs[2]:

        st.markdown("<div class='tv-section'>🎯 Sector-Wise Swing Trade Signal Scan</div>",
                    unsafe_allow_html=True)

        # ── Controls ──────────────────────────────────────────────────────
        sc_ctrl1, sc_ctrl2, sc_ctrl3, sc_ctrl4 = st.columns([1.5, 1, 1, 1])
        with sc_ctrl1:
            scan_sector = st.selectbox(
                "Select Sector to Scan",
                ["All Sectors"] + [
                    "🏦 Nifty Bank",   "💻 Nifty IT",    "⚡ Nifty Energy",
                    "🚗 Nifty Auto",   "🏗️ Nifty Infra",  "👑 Nifty 50 Leaders",
                    "📊 Nifty 50",     "🔵 Nifty Next 50","🟡 Nifty Midcap 100",
                    "🟠 Nifty Smallcap","🔰 F&O Stocks",
                ],
                key="scan_sec_sel",
            )
        with sc_ctrl2:
            scan_threshold = st.slider("Signal Threshold", 0.10, 0.40, 0.18, 0.01,
                                       key="scan_thresh", help="Lower = more signals")
        with sc_ctrl3:
            scan_top_n = st.slider("Max Results", 5, 30, 15, 5, key="scan_top_n")
        with sc_ctrl4:
            scan_period = st.select_slider(
                "Lookback",
                options=["3mo","4mo","6mo","8mo","1y"],
                value="6mo", key="scan_period"
            )

        sc_c1, sc_c2 = st.columns([2, 1])
        with sc_c1:
            scan_min_vol = st.select_slider(
                "Min Avg Daily Volume",
                options=[300_000, 500_000, 750_000, 1_000_000, 1_500_000, 2_000_000],
                value=750_000,
                format_func=lambda x: f"{x/1e5:.1f}L shares",
                key="scan_min_vol",
            )
        with sc_c2:
            scan_fo_only = st.toggle("F&O Eligible Only", value=False, key="scan_fo")

        run_scan_btn = st.button(
            "🚀  Run Sector Scan",
            use_container_width=True,
            key="run_sector_scan",
            help="Fetches live data and runs the full signal engine on the selected sector",
        )

        # ── Map selection to universe ─────────────────────────────────────
        SCAN_MAP = {
            "All Sectors":        None,
            "🏦 Nifty Bank":      "NIFTY BANK",
            "💻 Nifty IT":        "NIFTY IT",
            "⚡ Nifty Energy":    "NIFTY ENERGY",
            "🚗 Nifty Auto":      "NIFTY AUTO",
            "🏗️ Nifty Infra":     "NIFTY INFRA",
            "👑 Nifty 50 Leaders":"NIFTY 50 LEADERS",
            "📊 Nifty 50":        "NIFTY 50",
            "🔵 Nifty Next 50":   "NIFTY NEXT 50",
            "🟡 Nifty Midcap 100":"NIFTY MIDCAP 100",
            "🟠 Nifty Smallcap":  "NIFTY SMALLCAP 250",
            "🔰 F&O Stocks":      "FO STOCKS",
        }

        # ── Cached scan function ──────────────────────────────────────────
        # _sector_scan is defined at module level above run_dashboard
        # ── State management ──────────────────────────────────────────────
        scan_cache_key = (
            scan_sector, scan_threshold, scan_min_vol,
            scan_period, scan_top_n, scan_fo_only,
        )

        if run_scan_btn or ("sector_scan_results" not in st.session_state
                            or st.session_state.get("sector_scan_key") != scan_cache_key):
            if run_scan_btn:
                with st.spinner(f"🔴 Scanning {scan_sector} — fetching live NSE data..."):
                    gk = SCAN_MAP.get(scan_sector)
                    sc_alerts, sc_bt, sc_nifty, sc_err = _sector_scan(
                        gk, scan_threshold, scan_min_vol,
                        scan_period, scan_top_n, scan_fo_only,
                        False,
                    )
                st.session_state["sector_scan_results"] = (sc_alerts, sc_bt, sc_nifty, sc_err)
                st.session_state["sector_scan_key"]     = scan_cache_key
            else:
                sc_alerts, sc_bt, sc_nifty, sc_err = [], {}, {}, ""
        else:
            sc_alerts, sc_bt, sc_nifty, sc_err = st.session_state["sector_scan_results"]

        if sc_err:
            st.error(f"Scan error: {sc_err}")

        if not sc_alerts and not run_scan_btn:
            st.info("👆  Configure parameters above and click **🚀 Run Sector Scan** to discover the best swing trade setups in your chosen sector.", icon="🎯")

        elif sc_alerts:
            sc_nifty_trend = sc_nifty.get("trend", 0)
            sc_avg_ai = sum(r["ai"]["ai_pct"] for r in sc_alerts) / max(len(sc_alerts), 1)
            sc_n_fo   = sum(1 for r in sc_alerts if r.get("is_fo"))
            sc_n_stf  = sum(1 for r in sc_alerts if r.get("st_flip"))

            # ── SCAN RESULT SUMMARY ───────────────────────────────────────
            mkt_col  = "#26a69a" if sc_nifty_trend >= 0.3 else "#ef5350" if sc_nifty_trend <= -0.3 else "#f59e0b"
            st.markdown(
                f"<div class='tv-card' style='padding:14px 20px;border-color:{mkt_col}88;margin-bottom:16px'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px'>"
                f"<div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.1rem;color:{mkt_col}'>"
                f"🎯 {scan_sector} — Scan Complete</div>"
                f"<div class='tv-label'>Threshold {scan_threshold:.2f} · {scan_period} lookback · Vol &gt;{scan_min_vol/1e5:.1f}L</div>"
                f"</div>"
                f"<div style='display:flex;gap:20px;flex-wrap:wrap'>"
                f"<div style='text-align:center'><div class='tv-label'>Signals Found</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.4rem;color:#26a69a'>{len(sc_alerts)}</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>F&O Eligible</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.4rem;color:#38bdf8'>{sc_n_fo}</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>Avg AI Score</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.4rem;color:#f59e0b'>{sc_avg_ai:.1f}%</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>ST Flips</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.4rem;color:#a855f7'>{sc_n_stf}</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>Nifty</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.4rem;color:{mkt_col}'>{sc_nifty.get('label','N/A')}</div></div>"
                f"</div></div></div>",
                unsafe_allow_html=True,
            )

            # ── QUICK SORT + FILTER ───────────────────────────────────────
            qf1, qf2, qf3 = st.columns([1, 1, 1])
            with qf1:
                sort_mode = st.selectbox(
                    "Sort by",
                    ["AI Score", "Composite Score", "Volume", "ADX Strength",
                     "RSI (Lowest First)", "R:R Ratio"],
                    key="sc_sort",
                    label_visibility="collapsed",
                )
            with qf2:
                min_grade = st.selectbox(
                    "Min Grade",
                    ["All Grades", "B+ and above", "A and above", "A+ only"],
                    key="sc_grade_filter",
                    label_visibility="collapsed",
                )
            with qf3:
                show_stf_only = st.toggle("SuperTrend Flips Only", value=False, key="sc_stf_filter")

            # Apply sort
            sort_fn = {
                "AI Score":          lambda r: -r["ai"]["ai_pct"],
                "Composite Score":   lambda r: -r["score"],
                "Volume":            lambda r: -r["vol_ratio"],
                "ADX Strength":      lambda r: -r["adx"],
                "RSI (Lowest First)":lambda r:  r["rsi"],
                "R:R Ratio":         lambda r: -r["levels"]["short_term"]["rr"],
            }
            # Apply grade filter
            grade_min = {"All Grades":0,"B+ and above":68,"A and above":78,"A+ only":88}
            min_pct = grade_min.get(min_grade, 0)

            filtered = [
                r for r in sc_alerts
                if r["ai"]["ai_pct"] >= min_pct
                and (not show_stf_only or r.get("st_flip"))
            ]
            filtered.sort(key=sort_fn.get(sort_mode, sort_fn["AI Score"]))

            st.markdown(
                f"<div class='tv-label' style='margin:4px 0 12px'>"
                f"Showing <b style='color:#38bdf8'>{len(filtered)}</b> of {len(sc_alerts)} signals</div>",
                unsafe_allow_html=True,
            )

            # ── SIGNAL PANEL — COMPACT SUMMARY TABLE ─────────────────────
            st.markdown("<div class='tv-section'>📊 Signal Summary Table</div>",
                        unsafe_allow_html=True)

            tbl_rows = []
            for r_t in filtered:
                ltr_t, _ = _d_grade(r_t["ai"]["ai_pct"])
                stl_t    = r_t["levels"]["short_term"]
                ltl_t    = r_t["levels"]["long_term"]
                up_t     = (stl_t["tp"] / stl_t["entry"] - 1) * 100
                lt_up_t  = (ltl_t["tp"] / ltl_t["entry"] - 1) * 100
                hits_t   = r_t["hits"]
                tbl_rows.append({
                    "Symbol":    r_t["symbol"],
                    "Grade":     ltr_t,
                    "AI%":       round(r_t["ai"]["ai_pct"], 1),
                    "Score":     round(r_t["score"], 4),
                    "RSI":       round(r_t["rsi"], 1),
                    "ADX":       round(r_t["adx"], 1),
                    "Vol×":      round(r_t["vol_ratio"], 2),
                    "ATR%":      round(r_t["atr_pct"], 2),
                    "F&O":       "✅" if r_t["is_fo"] else "—",
                    "ST⚡":      "⚡" if r_t.get("st_flip") else "—",
                    "Price ₹":   r_t["last_close"],
                    "ST Entry":  stl_t["entry"],
                    "ST Tgt":    stl_t["tp"],
                    "ST SL":     stl_t["sl"],
                    "ST R:R":    stl_t["rr_str"],
                    "ST %Up":    round(up_t, 1),
                    "LT Tgt":    ltl_t["tp"],
                    "LT R:R":    ltl_t["rr_str"],
                    "LT %Up":    round(lt_up_t, 1),
                    "Top Signal":hits_t[0][1] if hits_t else "—",
                    "Indices":   r_t["indices"],
                })

            tbl_df = pd.DataFrame(tbl_rows)
            st.dataframe(
                tbl_df,
                use_container_width=True,
                height=360,
                hide_index=True,
                column_config={
                    "AI%":       st.column_config.ProgressColumn("AI%",      min_value=0,max_value=100, format="%.1f%%"),
                    "Price ₹":   st.column_config.NumberColumn("Price ₹",    format="₹%.2f"),
                    "ST Entry":  st.column_config.NumberColumn("ST Entry",   format="₹%.2f"),
                    "ST Tgt":    st.column_config.NumberColumn("ST Target",  format="₹%.2f"),
                    "ST SL":     st.column_config.NumberColumn("ST SL",      format="₹%.2f"),
                    "LT Tgt":    st.column_config.NumberColumn("LT Target",  format="₹%.2f"),
                    "Score":     st.column_config.NumberColumn("Score",      format="%.4f"),
                    "ST %Up":    st.column_config.NumberColumn("ST %Up",     format="+%.1f%%"),
                    "LT %Up":    st.column_config.NumberColumn("LT %Up",     format="+%.1f%%"),
                },
            )

            # CSV export
            csv_sc = tbl_df.to_csv(index=False).encode()
            st.download_button(
                "⬇️  Download Signal List CSV",
                data=csv_sc,
                file_name=f"sector_scan_{scan_sector.replace(' ','_')}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                key="sc_csv_dl",
            )

            st.markdown("<hr style='border-color:#2a3347;margin:16px 0'>",
                        unsafe_allow_html=True)

            # ── DETAILED SIGNAL CARDS ─────────────────────────────────────
            st.markdown(
                f"<div class='tv-section'>📋 Detailed Signal Cards — Top {min(len(filtered), scan_top_n)} Swing Setups</div>",
                unsafe_allow_html=True,
            )

            for rank_sc, r_sc in enumerate(filtered[:scan_top_n], 1):
                ai_sc  = r_sc["ai"]; mk_sc = r_sc["mkt"]
                stl_sc = r_sc["levels"]["short_term"]
                ltl_sc = r_sc["levels"]["long_term"]
                hits_sc= r_sc["hits"]
                ai_p_sc= ai_sc["ai_pct"]
                ltr_sc, gc_sc = _d_grade(ai_p_sc)
                up_sc  = (stl_sc["tp"] / stl_sc["entry"] - 1) * 100
                lt_up_sc= (ltl_sc["tp"] / ltl_sc["entry"] - 1) * 100
                stf_sc = r_sc.get("st_flip", 0)
                sig_sc = hits_sc[0][1] if hits_sc else "—"

                # Pre-compute for HTML
                _rsi_c  = _d_rsi_col(r_sc["rsi"])
                _adx_c  = _d_adx_col(r_sc["adx"])
                _vol_c  = _d_vol_col(r_sc["vol_ratio"])
                _fo_tag = "<span class='pill-cyan' style='font-size:.68rem'>F&O ✅</span>" if r_sc["is_fo"] else ""
                _stf_tag= "<span class='pill-amber' style='font-size:.68rem'>⚡ ST FLIP</span>" if stf_sc else ""
                _gauge_sc = gauge_html(ai_p_sc, "", 220)

                with st.expander(
                    f"#{rank_sc}  {r_sc['symbol']}  ·  Grade: {ltr_sc}  ·  "
                    f"AI: {ai_p_sc:.1f}%  ·  ₹{r_sc['last_close']:,.2f}  ·  "
                    f"ST Target: ₹{stl_sc['tp']:,.2f} ({up_sc:+.1f}%)  ·  {r_sc['indices']}",
                    expanded=(rank_sc <= 3),
                ):
                    # ── TOP: Symbol header ─────────────────────────────────
                    st.markdown(
                        f"<div class='tv-card tv-card-bull' style='border-color:{gc_sc}55;padding:14px 18px'>"
                        f"<div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px'>"
                        f"<div style='display:flex;align-items:center;gap:12px'>"
                        f"<span class='tv-badge-sym' style='font-size:1.2rem;padding:5px 16px'>{r_sc['symbol']}</span>"
                        f"<span class='tv-badge-grade' style='background:{gc_sc}22;color:{gc_sc};font-size:1.3rem'>{ltr_sc}</span>"
                        f"{_fo_tag}{_stf_tag}"
                        f"<div>"
                        f"<div class='tv-label'>{r_sc.get('sector','N/A')}</div>"
                        f"<div style='font-size:.7rem;color:#434651'>{r_sc['indices']}</div>"
                        f"</div></div>"
                        f"<div style='display:flex;gap:20px;flex-wrap:wrap'>"
                        f"<div style='text-align:center'><div class='tv-label'>Price</div>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.6rem;color:#38bdf8'>&#8377;{r_sc['last_close']:,.2f}</div></div>"
                        f"<div style='text-align:center'><div class='tv-label'>AI Score</div>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{gc_sc}'>{ai_p_sc:.1f}%</div></div>"
                        f"<div style='text-align:center'><div class='tv-label'>RSI</div>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_rsi_c}'>{r_sc['rsi']:.1f}</div></div>"
                        f"<div style='text-align:center'><div class='tv-label'>ADX</div>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_adx_c}'>{r_sc['adx']:.1f}</div></div>"
                        f"<div style='text-align:center'><div class='tv-label'>Volume</div>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_vol_c}'>{r_sc['vol_ratio']:.2f}&times;</div></div>"
                        f"</div></div></div>",
                        unsafe_allow_html=True,
                    )

                    # ── CONFIDENCE ROW ─────────────────────────────────────
                    cc_a, cc_b, cc_c = st.columns(3)
                    for _cw, _pct, _lbl, _clr in [
                        (cc_a, ai_p_sc,             "🤖 AI Confidence",     gc_sc),
                        (cc_b, mk_sc["pct"],         "📊 Market Confidence", "#26a69a" if mk_sc["pct"]>=65 else "#f59e0b" if mk_sc["pct"]>=40 else "#ef5350"),
                        (cc_c, r_sc["pat_conf"]*100, "🎯 Pattern Strength",  "#26a69a" if r_sc["pat_conf"]*100>=60 else "#f59e0b" if r_sc["pat_conf"]*100>=40 else "#ef5350"),
                    ]:
                        _gb = gauge_html(_pct, "", 200)
                        _cw.markdown(
                            f"<div class='tv-card' style='text-align:center;padding:12px'>"
                            f"<div class='tv-label'>{_lbl}</div>"
                            f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.7rem;color:{_clr};margin:6px 0'>{_pct:.1f}%</div>"
                            f"{_gb}</div>",
                            unsafe_allow_html=True,
                        )

                    # ── TRADE LEVELS ───────────────────────────────────────
                    st.markdown("<div class='tv-section' style='margin-top:6px'>📐 Trade Setup</div>",
                                unsafe_allow_html=True)
                    tl_a, tl_b, tl_c = st.columns(3)

                    def _sc_trade_col(col_w, title, entry, target, sl, rr_str, up_pct, window, border_col):
                        risk  = round(entry - sl,    2) if entry > sl    else round(sl - entry, 2)
                        reward= round(target - entry, 2) if target > entry else round(entry - target, 2)
                        col_w.markdown(
                            f"<div class='tv-card' style='border-left:3px solid {border_col};padding:12px;text-align:center'>"
                            f"<div style='color:{border_col};font-size:.78rem;font-weight:700;margin-bottom:10px'>{title}</div>"
                            f"<div class='tv-label'>Entry</div>"
                            f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.1rem;color:#38bdf8;margin-bottom:8px'>&#8377;{entry:,.2f}</div>"
                            f"<div style='display:flex;justify-content:space-around;margin-bottom:8px'>"
                            f"<div><div class='tv-label'>Target</div><div style='font-weight:700;color:#26a69a'>&#8377;{target:,.2f}</div>"
                            f"<div style='font-size:.68rem;color:#26a69a'>{up_pct:+.1f}%</div></div>"
                            f"<div><div class='tv-label'>Stop Loss</div><div style='font-weight:700;color:#ef5350'>&#8377;{sl:,.2f}</div>"
                            f"<div style='font-size:.68rem;color:#ef5350'>{(sl/entry-1)*100:+.1f}%</div></div>"
                            f"</div>"
                            f"<div style='display:flex;justify-content:space-around;padding-top:6px;border-top:1px solid #2a3347'>"
                            f"<div><div class='tv-label'>R:R</div><div style='color:#d1d4dc;font-weight:700'>{rr_str}</div></div>"
                            f"<div><div class='tv-label'>Risk ₹</div><div style='color:#ef5350'>&#8377;{risk:.2f}</div></div>"
                            f"<div><div class='tv-label'>Reward ₹</div><div style='color:#26a69a'>&#8377;{reward:.2f}</div></div>"
                            f"</div>"
                            f"<div style='margin-top:6px;font-size:.65rem;color:#434651'>{window}</div>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )

                    _sc_trade_col(tl_a, "⚡ Aggressive (ST)", stl_sc["entry"], stl_sc["tp"], stl_sc["sl"], stl_sc["rr_str"], up_sc,    "2–5 trading days",  "#f59e0b")
                    _sc_trade_col(tl_b, "📅 Swing (LT)",      ltl_sc["entry"], ltl_sc["tp"], ltl_sc["sl"], ltl_sc["rr_str"], lt_up_sc, "10–20 trading days","#26a69a")
                    _dip_e  = round(r_sc["last_close"] * 0.98, 2)
                    _dip_sl = round(_dip_e - r_sc["atr"] * 1.2, 2)
                    _dip_tp = round(_dip_e + r_sc["atr"] * 2.5, 2)
                    _dip_rr = round((_dip_tp - _dip_e) / max(_dip_e - _dip_sl, 0.01), 2)
                    _sc_trade_col(tl_c, "📌 Limit/Dip Entry", _dip_e, _dip_tp, _dip_sl, f"1:{_dip_rr}", (_dip_tp/_dip_e-1)*100, "Limit at −2%", "#3b82f6")

                    # ── 8-FACTOR SCORES ────────────────────────────────────
                    st.markdown("<div class='tv-section' style='margin-top:4px'>🧮 8-Factor Score Breakdown</div>",
                                unsafe_allow_html=True)
                    fa_cols = st.columns(8)
                    for fi, (factor_name, factor_key, factor_wt) in enumerate([
                        ("Trend",       "trend_s", 0.24), ("Momentum", "mom_s",  0.16),
                        ("Breakout",    "brk_s",   0.17), ("Volume",   "vol_s",  0.10),
                        ("Pattern",     "pat_s",   0.10), ("Fund",     "fund_s", 0.08),
                        ("Sentiment",   "sent_s",  0.04), ("Pullback", "trend_s",0.11),
                    ]):
                        _fsc  = float(ai_sc.get(factor_key, 0))
                        _fc   = "#26a69a" if _fsc > 0.1 else "#ef5350" if _fsc < -0.1 else "#f59e0b"
                        _farr = "▲" if _fsc > 0.1 else "▼" if _fsc < -0.1 else "◆"
                        fa_cols[fi].markdown(
                            f"<div style='background:#1c2030;border-radius:3px;padding:8px 6px;text-align:center'>"
                            f"<div class='tv-label'>{factor_name}</div>"
                            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{_fc};font-size:.9rem'>{_fsc:+.2f}</div>"
                            f"<div style='font-size:.75rem;color:{_fc}'>{_farr} {factor_wt:.0%}</div>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )

                    # ── PATTERN HITS + AI GAUGE + RESEARCH NOTE ───────────
                    ph_col, rn_col = st.columns([1, 1.4])

                    with ph_col:
                        st.markdown("<div class='tv-section' style='margin-top:6px'>🎯 Pattern Signals</div>",
                                    unsafe_allow_html=True)
                        CAT_C_SC = {
                            "Trend":"#38bdf8","Momentum":"#f59e0b","Candlestick":"#a855f7",
                            "Breakout":"#26a69a","Volume":"#3b82f6","Volatility":"#787b86",
                            "Price Action":"#d1d4dc","Structure":"#67e8f9",
                        }
                        for _sc_h, _lb_h, _cat_h in hits_sc[:7]:
                            _cc_h = CAT_C_SC.get(_cat_h, "#787b86")
                            _bw_h = int(_sc_h * 140)
                            st.markdown(
                                f"<div style='display:flex;align-items:center;gap:8px;padding:5px 8px;"
                                f"margin-bottom:4px;background:#1c2030;border-radius:3px;"
                                f"border-left:2px solid {_cc_h}'>"
                                f"<span style='font-size:.68rem;color:{_cc_h};min-width:64px'>{_cat_h}</span>"
                                f"<div style='height:4px;width:{_bw_h}px;background:{_cc_h};border-radius:2px'></div>"
                                f"<span style='font-size:.75rem;color:#d1d4dc;flex:1'>{_lb_h[:38]}</span>"
                                f"<span style='font-size:.7rem;color:{_cc_h};font-weight:700'>{_sc_h*100:.0f}%</span>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                        st.markdown(
                            f"<div style='font-size:.7rem;color:#434651;padding:4px 0'>"
                            f"Total: {len(hits_sc)} signals across {r_sc['n_cats']} categories</div>",
                            unsafe_allow_html=True,
                        )

                    with rn_col:
                        st.markdown("<div class='tv-section' style='margin-top:6px'>📝 Research Note</div>",
                                    unsafe_allow_html=True)
                        st.markdown(
                            f"<div class='tv-card' style='font-size:.82rem;color:#787b86;line-height:1.75'>"
                            f"{r_sc['reason'].replace('  •  ', '<br><span style=\"color:#38bdf8\">→</span> ')}"
                            f"<div style='margin-top:10px;padding-top:8px;border-top:1px solid #2a3347;"
                            f"font-size:.72rem;color:#434651;display:grid;grid-template-columns:1fr 1fr;gap:4px'>"
                            f"<span>EMA 9/21/50: <b style='color:#d1d4dc'>{r_sc['ema9']:.1f} / {r_sc['ema21']:.1f} / {r_sc['ema50']:.1f}</b></span>"
                            f"<span>ATR: <b style='color:#d1d4dc'>{r_sc['atr_pct']:.2f}%</b></span>"
                            f"<span>Vol Z-Score: <b style='color:#d1d4dc'>{r_sc.get('vol_z',0):.2f}&sigma;</b></span>"
                            f"<span>Traded: <b style='color:#d1d4dc'>&#8377;{r_sc['traded_val_cr']:.2f} Cr/d</b></span>"
                            f"<span>Composite: <b style='color:#d1d4dc'>{r_sc['score']:+.4f}</b></span>"
                            f"<span>Market: <b style='color:#d1d4dc'>{mk_sc['label']}</b></span>"
                            f"</div></div>",
                            unsafe_allow_html=True,
                        )
                        st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
                        st.markdown(_gauge_sc, unsafe_allow_html=True)

                    # ── TradingView mini chart ─────────────────────────────
                    st.markdown(
                        f"<div class='tv-label' style='margin:8px 0 3px'>📊 Live Chart — {r_sc['symbol']}</div>",
                        unsafe_allow_html=True,
                    )
                    components.html(
                        tv_mini_chart(r_sc["symbol"], height=190,
                                      nonce=f"scscan_{r_sc['symbol']}_{rank_sc}"),
                        height=194, scrolling=False,
                    )

    # TAB 3 — POWER SCAN
    # ══════════════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("""
        <div style='padding:14px 20px;background:linear-gradient(135deg,#ef535018,#a855f718,#38bdf818,#0b0e11);
                    border-left:4px solid #ef5350;border-radius:6px;margin-bottom:16px'>
          <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.25rem;color:#ef5350;letter-spacing:.06em'>
            🔥 POWER SCAN — Live NSE Universe
          </div>
          <div style='font-size:.76rem;color:#787b86;margin-top:3px'>
            ⚡ High-Volatility Momentum Movers &nbsp;·&nbsp; 💎 Multi-Bagger Penny Opportunities
            &nbsp;·&nbsp; Full engine scan across ~200 NSE symbols &nbsp;·&nbsp; Click any result for deep analysis
          </div>
        </div>""", unsafe_allow_html=True)

        # ── Controls ──────────────────────────────────────────────────────
        psc1, psc2, psc3 = st.columns(3)
        with psc1:
            ps_threshold = st.slider("Signal Threshold", 0.08, 0.30, 0.14, 0.01, key="ps_thresh",
                                     help="Lower = more signals. 0.14 recommended for power scan.")
        with psc2:
            ps_period = st.select_slider("Lookback Period",
                                          ["3mo","4mo","6mo","8mo","1y"], value="6mo", key="ps_period")
        with psc3:
            ps_top_n = st.slider("Results to Show", 5, 20, 10, 1, key="ps_top_n")

        run_ps_btn = st.button("🚀  Run Power Scan — Full NSE Universe",
                               use_container_width=True, key="run_power_scan")

        ps_tab1, ps_tab2 = st.tabs(["⚡ High-Volatility Momentum", "💎 Multi-Bagger Penny Picks"])

        # ── State ─────────────────────────────────────────────────────────
        ps_key = (ps_threshold, ps_period, ps_top_n)
        if run_ps_btn or ("ps_results" not in st.session_state
                          or st.session_state.get("ps_key") != ps_key):
            if run_ps_btn:
                with st.spinner("⚡ Scanning high-volatility movers across full NSE universe..."):
                    _ps_vol, _ps_vbt, _ps_vnifty, _ps_verr = _power_scan_volatile(
                        ps_threshold, ps_period, ps_top_n, False)
                with st.spinner("💎 Discovering multi-bagger penny opportunities..."):
                    _ps_pny, _ps_pbt, _ps_pnifty, _ps_perr = _power_scan_penny(
                        ps_threshold, ps_period, ps_top_n, False)
                st.session_state["ps_results"] = (_ps_vol, _ps_pny, _ps_vbt, _ps_pbt)
                st.session_state["ps_key"]     = ps_key
            else:
                _ps_vol = _ps_pny = []; _ps_vbt = _ps_pbt = {}
        else:
            _ps_vol, _ps_pny, _ps_vbt, _ps_pbt = st.session_state["ps_results"]

        if not _ps_vol and not _ps_pny and not run_ps_btn:
            st.info(
                "👆 Click **🚀 Run Power Scan** to discover momentum movers and "
                "multi-bagger penny stocks across the full NSE universe.\n\n"
                "⏱️ Live scan takes **3–8 minutes**. Results cached for 30 minutes.",
                icon="🔥",
            )

        # ══════════════════════════════════════════════════════════════════
        # HELPER: full interactive stock detail panel
        # ══════════════════════════════════════════════════════════════════
        def _render_stock_detail(r_d, source_label="Power Scan"):
            """Render the complete deep-dive panel for one stock."""
            ai_d   = r_d["ai"];  mk_d = r_d["mkt"]
            stl_d  = r_d["levels"]["short_term"]
            ltl_d  = r_d["levels"]["long_term"]
            hits_d = r_d["hits"]
            ai_p_d = ai_d["ai_pct"]
            ltr_d, gc_d = _d_grade(ai_p_d)
            up_d   = (stl_d["tp"] / stl_d["entry"] - 1) * 100
            lt_up_d= (ltl_d["tp"] / ltl_d["entry"] - 1) * 100
            stf_d  = r_d.get("st_flip", 0) or r_d.get("has_stf", False)

            _rsi_cd = _d_rsi_col(r_d["rsi"])
            _adx_cd = _d_adx_col(r_d["adx"])
            _vol_cd = _d_vol_col(r_d["vol_ratio"])

            # ── Header ────────────────────────────────────────────────────
            _fo_h  = "<span class='pill-cyan' style='font-size:.7rem'>F&O ✅</span>" if r_d.get("is_fo") else ""
            _stf_h = "<span class='pill-amber' style='font-size:.7rem'>⚡ ST FLIP</span>" if stf_d else ""
            _brk_h = "<span style='background:#26a69a22;color:#26a69a;padding:2px 8px;border-radius:2px;font-size:.7rem'>🚀 BREAKOUT</span>" if r_d.get("has_breakout") else ""
            _vol_h = "<span style='background:#3b82f622;color:#3b82f6;padding:2px 8px;border-radius:2px;font-size:.7rem'>🔊 VOL SURGE</span>" if r_d.get("has_volume_surge") else ""

            st.markdown(
                f"<div class='tv-card tv-card-bull' style='border-color:{gc_d}66;padding:16px 20px'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:14px'>"
                f"<div style='display:flex;align-items:center;gap:12px;flex-wrap:wrap'>"
                f"<span class='tv-badge-sym' style='font-size:1.3rem;padding:5px 18px'>{r_d['symbol']}</span>"
                f"<span class='tv-badge-grade' style='background:{gc_d}22;color:{gc_d};font-size:1.3rem'>{ltr_d}</span>"
                f"{_fo_h}{_stf_h}{_brk_h}{_vol_h}"
                f"<div><div class='tv-label'>{r_d.get('sector','N/A')}</div>"
                f"<div style='font-size:.7rem;color:#434651'>{r_d['indices']}</div></div>"
                f"</div>"
                f"<div style='display:flex;gap:18px;flex-wrap:wrap'>"
                f"<div style='text-align:center'><div class='tv-label'>Price</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.7rem;color:#38bdf8'>&#8377;{r_d['last_close']:,.2f}</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>AI Score</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{gc_d}'>{ai_p_d:.1f}%</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>RSI</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_rsi_cd}'>{r_d['rsi']:.1f}</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>ADX</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_adx_cd}'>{r_d['adx']:.1f}</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>ATR%</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:#f59e0b'>{r_d['atr_pct']:.2f}%</div></div>"
                f"<div style='text-align:center'><div class='tv-label'>Vol</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_vol_cd}'>{r_d['vol_ratio']:.2f}&times;</div></div>"
                f"</div></div></div>",
                unsafe_allow_html=True,
            )

            # ── Section A: WHY SELECTED ───────────────────────────────────
            st.markdown("""
            <div style='margin:14px 0 10px;padding:8px 16px;background:linear-gradient(90deg,#38bdf815,#0b0e11);
                        border-left:3px solid #38bdf8;border-radius:3px'>
              <span style='font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;color:#38bdf8;letter-spacing:.08em'>
                🎯 WHY THIS STOCK IS SELECTED
              </span>
            </div>""", unsafe_allow_html=True)

            ws1, ws2 = st.columns([1.2, 1])
            with ws1:
                # Pattern hits grouped
                CAT_C_D = {"Trend":"#38bdf8","Momentum":"#f59e0b","Candlestick":"#a855f7",
                           "Breakout":"#26a69a","Volume":"#3b82f6","Volatility":"#ef5350",
                           "Price Action":"#d1d4dc","Structure":"#67e8f9"}
                for sh,slb,scat in hits_d[:8]:
                    scc = CAT_C_D.get(scat,"#787b86")
                    bw  = int(sh*180)
                    st.markdown(
                        f"<div style='display:flex;align-items:center;gap:9px;padding:6px 10px;"
                        f"margin-bottom:4px;background:#131722;border-radius:3px;border-left:2px solid {scc}'>"
                        f"<span style='font-size:.65rem;color:{scc};font-weight:600;min-width:80px'>{scat}</span>"
                        f"<div style='height:4px;width:{bw}px;background:{scc};border-radius:2px;flex-shrink:0'></div>"
                        f"<span style='font-size:.8rem;color:#d1d4dc;flex:1'>{slb}</span>"
                        f"<b style='font-size:.72rem;color:{scc}'>{sh*100:.0f}%</b></div>",
                        unsafe_allow_html=True,
                    )
                st.markdown(
                    f"<div style='font-size:.72rem;color:#434651;margin-top:5px'>"
                    f"Total: {len(hits_d)} signals across {r_d['n_cats']} categories &nbsp;·&nbsp; "
                    f"Pattern confidence: {r_d['pat_conf']*100:.1f}%</div>",
                    unsafe_allow_html=True,
                )
            with ws2:
                st.markdown(
                    f"<div class='tv-card' style='border-left:3px solid #38bdf8;padding:12px;font-size:.82rem;color:#787b86;line-height:1.75'>"
                    f"<div class='tv-label' style='margin-bottom:6px'>📝 Engine Research Note</div>"
                    f"{r_d['reason'].replace('  •  ','<br><span style=\"color:#38bdf8\">→</span> ')}"
                    f"<div style='margin-top:8px;padding-top:7px;border-top:1px solid #2a3347;font-size:.7rem;color:#434651'>"
                    f"EMA9/21/50: {r_d['ema9']:.1f} / {r_d['ema21']:.1f} / {r_d['ema50']:.1f} &nbsp;·&nbsp; "
                    f"Score: {r_d['score']:+.4f} &nbsp;·&nbsp; ATR ₹{r_d['atr']:.2f}"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )

            # ── Section B: CONFIDENCE ─────────────────────────────────────
            st.markdown("""
            <div style='margin:14px 0 10px;padding:8px 16px;background:linear-gradient(90deg,#a855f715,#0b0e11);
                        border-left:3px solid #a855f7;border-radius:3px'>
              <span style='font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;color:#a855f7;letter-spacing:.08em'>
                🤖 AI CONFIDENCE ANALYSIS
              </span>
            </div>""", unsafe_allow_html=True)

            c1,c2,c3,c4 = st.columns(4)
            for cw,pct_,lbl_,clr_ in [
                (c1, ai_p_d,              "🤖 AI Model",    gc_d),
                (c2, mk_d["pct"],         "📊 Market",      "#26a69a" if mk_d["pct"]>=65 else "#f59e0b" if mk_d["pct"]>=40 else "#ef5350"),
                (c3, r_d["pat_conf"]*100, "🎯 Patterns",    "#26a69a" if r_d["pat_conf"]*100>=55 else "#f59e0b"),
                (c4, (ai_d.get("trend_s",0)+1)/2*100, "📈 Trend", "#26a69a" if ai_d.get("trend_s",0)>0.3 else "#f59e0b"),
            ]:
                _gb = gauge_html(pct_,"",160)
                cw.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px;text-align:center;border-top:2px solid {clr_}'>"
                    f"<div class='tv-label'>{lbl_}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:{clr_};margin:6px 0'>{pct_:.1f}%</div>"
                    f"{_gb}</div>",
                    unsafe_allow_html=True,
                )

            # 8-factor mini bars
            st.markdown("<div style='display:flex;gap:6px;margin-top:8px;flex-wrap:wrap'>", unsafe_allow_html=True)
            for fn,fk in [("Trend","trend_s"),("Mom","mom_s"),("Brk","brk_s"),("Vol","vol_s"),("Pat","pat_s"),("Fund","fund_s"),("Sent","sent_s")]:
                fv = float(ai_d.get(fk,0))
                fc_ = "#26a69a" if fv>0.1 else "#ef5350" if fv<-0.1 else "#f59e0b"
                st.markdown(
                    f"<span style='background:#1c2030;border-radius:3px;padding:4px 8px;font-size:.7rem;"
                    f"color:{fc_}'>{fn}: <b>{fv:+.2f}</b></span>",
                    unsafe_allow_html=True,
                )
            st.markdown("</div>", unsafe_allow_html=True)

            # ── Section C: TRADE PLAN ─────────────────────────────────────
            st.markdown("""
            <div style='margin:14px 0 10px;padding:8px 16px;background:linear-gradient(90deg,#26a69a15,#0b0e11);
                        border-left:3px solid #26a69a;border-radius:3px'>
              <span style='font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;color:#26a69a;letter-spacing:.08em'>
                📐 COMPLETE TRADE PLAN
              </span>
            </div>""", unsafe_allow_html=True)

            tp1,tp2,tp3 = st.columns(3)
            _dip_e=round(r_d["last_close"]*0.98,2); _dip_sl=round(_dip_e-r_d["atr"]*1.2,2)
            _dip_tp=round(_dip_e+r_d["atr"]*2.5,2); _dip_rr=round((_dip_tp-_dip_e)/max(_dip_e-_dip_sl,0.01),2)
            for tc_,tit_,e_,tp_,sl_,rr_,up_,win_,brd_ in [
                (tp1,"⚡ Aggressive (ST)",stl_d["entry"],stl_d["tp"],stl_d["sl"],stl_d["rr_str"],up_d,"2–5 days","#f59e0b"),
                (tp2,"📅 Swing (LT)",     ltl_d["entry"],ltl_d["tp"],ltl_d["sl"],ltl_d["rr_str"],lt_up_d,"10–20 days","#26a69a"),
                (tp3,"📌 Limit/Dip",      _dip_e,_dip_tp,_dip_sl,f"1:{_dip_rr}",(_dip_tp/_dip_e-1)*100,"Limit at −2%","#3b82f6"),
            ]:
                tc_.markdown(
                    f"<div style='background:#131722;border-left:3px solid {brd_};border-radius:4px;padding:12px;text-align:center'>"
                    f"<div style='color:{brd_};font-size:.75rem;font-weight:700;margin-bottom:8px'>{tit_}</div>"
                    f"<div class='tv-label'>Entry</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.1rem;color:#38bdf8;margin:5px 0'>&#8377;{e_:,.2f}</div>"
                    f"<div style='display:flex;justify-content:space-around;margin-bottom:7px'>"
                    f"<div><div class='tv-label'>Target</div><div style='font-weight:700;color:#26a69a'>&#8377;{tp_:,.2f}</div><div style='font-size:.68rem;color:#26a69a'>{up_:+.1f}%</div></div>"
                    f"<div><div class='tv-label'>Stop</div><div style='font-weight:700;color:#ef5350'>&#8377;{sl_:,.2f}</div><div style='font-size:.68rem;color:#ef5350'>{(sl_/e_-1)*100:+.1f}%</div></div>"
                    f"</div>"
                    f"<div style='border-top:1px solid #2a3347;padding-top:6px;font-size:.75rem;color:#d1d4dc'>"
                    f"R:R {rr_} &nbsp;·&nbsp; {win_}</div></div>",
                    unsafe_allow_html=True,
                )

            # ── Section D: POSITION SIZING ────────────────────────────────
            st.markdown("""
            <div style='margin:14px 0 10px;padding:8px 16px;background:linear-gradient(90deg,#f59e0b15,#0b0e11);
                        border-left:3px solid #f59e0b;border-radius:3px'>
              <span style='font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;color:#f59e0b;letter-spacing:.08em'>
                💰 POSITION SIZING CALCULATOR
              </span>
            </div>""", unsafe_allow_html=True)

            cap_ = st.session_state.get("capital_val", 1_000_000)
            sl_r_ = stl_d["risk"]
            kf_   = min(max(0.55-(1-0.55)/1.5,0),0.25)
            ps_c1,ps_c2,ps_c3,ps_c4 = st.columns(4)
            for psc_,plbl_,qty_ in [
                (ps_c1,"1% Risk Rule",  max(1,int(cap_*0.01/sl_r_)) if sl_r_ else 0),
                (ps_c2,"2% Risk Rule",  max(1,int(cap_*0.02/sl_r_)) if sl_r_ else 0),
                (ps_c3,f"Half-Kelly",   max(1,int(cap_*kf_/r_d["last_close"])) if r_d["last_close"] else 0),
                (ps_c4,"Fixed 20%",     int(cap_*0.20/r_d["last_close"]) if r_d["last_close"] else 0),
            ]:
                inv_=qty_*r_d["last_close"]; ml_=qty_*sl_r_; tp__=qty_*stl_d["reward"]; pct_=inv_/cap_*100
                pc_="#26a69a" if pct_<=20 else "#f59e0b" if pct_<=30 else "#ef5350"
                psc_.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px;text-align:center;border-bottom:2px solid {pc_}'>"
                    f"<div class='tv-label'>{plbl_}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:#38bdf8;margin:5px 0'>{qty_:,}</div>"
                    f"<div style='font-size:.72rem;line-height:1.6'>"
                    f"<div style='display:flex;justify-content:space-between'><span style='color:#434651'>Capital</span><span>&#8377;{inv_:,.0f}</span></div>"
                    f"<div style='display:flex;justify-content:space-between'><span style='color:#434651'>Risk</span><span style='color:#ef5350'>&#8377;{ml_:,.0f}</span></div>"
                    f"<div style='display:flex;justify-content:space-between'><span style='color:#434651'>Reward</span><span style='color:#26a69a'>&#8377;{tp__:,.0f}</span></div>"
                    f"<div style='display:flex;justify-content:space-between'><span style='color:#434651'>Portfolio%</span><span style='color:{pc_}'>{pct_:.1f}%</span></div>"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )

            # ── Section E: FUTURE GROWTH PREDICTION ──────────────────────
            st.markdown("""
            <div style='margin:14px 0 10px;padding:8px 16px;background:linear-gradient(90deg,#6366f115,#0b0e11);
                        border-left:3px solid #6366f1;border-radius:3px'>
              <span style='font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;color:#6366f1;letter-spacing:.08em'>
                🔮 GROWTH SCENARIO PROJECTIONS
              </span>
              <span style='font-size:.68rem;color:#434651;margin-left:8px'>Based on current momentum &amp; historical ATR patterns</span>
            </div>""", unsafe_allow_html=True)

            _price  = r_d["last_close"]
            _atr    = r_d["atr"]
            _ai     = ai_p_d / 100
            _adx    = r_d["adx"]
            _vol    = r_d["vol_ratio"]
            _stf_   = 1 if stf_d else 0
            _brk_   = 1 if r_d.get("has_breakout") else 0

            # Momentum multiplier
            _mom_mult = 1.0 + (_ai * 0.5) + (_adx / 100) + (_vol / 10) + (_stf_ * 0.3) + (_brk_ * 0.2)
            # Scenario projections
            _sc_base   = round(_price * (1 + _atr / _price * 3 * _mom_mult * 0.8), 2)
            _sc_bull   = round(_price * (1 + _atr / _price * 5 * _mom_mult), 2)
            _sc_super  = round(_price * (1 + _atr / _price * 9 * _mom_mult * 1.3), 2)
            _sc_bear   = round(_price * (1 - _atr / _price * 2), 2)
            _sc_crash  = round(_price * (1 - _atr / _price * 4), 2)

            sc_cols = st.columns(5)
            for scc_,slbl_,sprice_,sicon_,sbrd_ in [
                (sc_cols[0],"🐻 Bear Case",    _sc_crash, "📉", "#ef5350"),
                (sc_cols[1],"⚠️ Soft Stop",    _sc_bear,  "↘️",  "#f59e0b"),
                (sc_cols[2],"📊 Base Case",    _sc_base,  "→",  "#787b86"),
                (sc_cols[3],"🚀 Bull Case",    _sc_bull,  "📈", "#26a69a"),
                (sc_cols[4],"🔥 Super Bull",   _sc_super, "⭐", "#a855f7"),
            ]:
                _pct_ = (sprice_/_price-1)*100
                _col_ = "#26a69a" if _pct_>0 else "#ef5350"
                scc_.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px;text-align:center;border-top:3px solid {sbrd_}'>"
                    f"<div class='tv-label'>{slbl_}</div>"
                    f"<div style='font-size:1.2rem;margin:4px 0'>{sicon_}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{_col_};font-size:.95rem'>&#8377;{sprice_:,.2f}</div>"
                    f"<div style='font-size:.72rem;color:{_col_};font-weight:600'>{_pct_:+.1f}%</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            st.markdown(
                f"<div style='font-size:.68rem;color:#434651;text-align:center;margin-top:4px'>"
                f"⚠️ Projections are momentum-model estimates only, not financial advice. "
                f"Momentum multiplier: {_mom_mult:.2f}× &nbsp;·&nbsp; "
                f"Based on ATR={_atr:.2f}, ADX={_adx:.1f}, Vol={_vol:.2f}×"
                f"</div>",
                unsafe_allow_html=True,
            )

            # ── Section F: LIVE CHART ─────────────────────────────────────
            st.markdown("""
            <div style='margin:14px 0 8px;padding:8px 16px;background:linear-gradient(90deg,#ef535015,#0b0e11);
                        border-left:3px solid #ef5350;border-radius:3px'>
              <span style='font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;color:#ef5350;letter-spacing:.08em'>
                📊 LIVE TRADINGVIEW CHART
              </span>
            </div>""", unsafe_allow_html=True)
            ch1_, ch2_ = st.columns([2,1])
            with ch1_:
                components.html(
                    tv_chart_widget(r_d["symbol"], height=400),
                    height=415, scrolling=False,
                )
            with ch2_:
                components.html(
                    tv_technical_analysis(r_d["symbol"], nonce=f"ps_{r_d['symbol']}"),
                    height=415, scrolling=False,
                )

        # ══════════════════════════════════════════════════════════════════
        # SUB-TAB 1: HIGH VOLATILITY MOVERS
        # ══════════════════════════════════════════════════════════════════
        with ps_tab1:
            if not _ps_vol:
                if run_ps_btn:
                    st.warning("⚠️ No high-volatility signals found. Try lowering threshold to 0.12.", icon="⚡")
                else:
                    st.info("Run Power Scan to see momentum movers.", icon="⚡")
            else:
                # Summary strip
                _avg_atr_v = sum(r["atr_pct"] for r in _ps_vol)/len(_ps_vol)
                _avg_ai_v  = sum(r["ai"]["ai_pct"] for r in _ps_vol)/len(_ps_vol)
                _n_stf_v   = sum(1 for r in _ps_vol if r.get("st_flip"))
                st.markdown(
                    f"<div style='display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap'>"
                    + "".join(
                        f"<div style='background:#131722;border-radius:4px;padding:10px 16px;text-align:center;border-top:2px solid {c_}'>"
                        f"<div class='tv-label'>{l_}</div>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:{c_}'>{v_}</div></div>"
                        for l_,v_,c_ in [
                            ("Signals Found",str(len(_ps_vol)),"#ef5350"),
                            ("Avg ATR%",f"{_avg_atr_v:.1f}%","#f59e0b"),
                            ("Avg AI%",f"{_avg_ai_v:.1f}%","#38bdf8"),
                            ("ST Flips ⚡",str(_n_stf_v),"#a855f7"),
                        ]
                    )
                    + f"</div>",
                    unsafe_allow_html=True,
                )

                # Sort
                v_s1, v_s2 = st.columns([2,1])
                with v_s1:
                    v_sort = st.selectbox("Sort by", [
                        "Momentum Score (ATR×ADX×Vol×AI)",
                        "ATR% Highest", "AI Score", "ADX Strength",
                        "Volume Surge", "RSI Oversold",
                    ], key="v_sort", label_visibility="collapsed")
                with v_s2:
                    v_stf_only = st.toggle("ST Flips Only", False, key="v_stf")

                _vsf = {"Momentum Score (ATR×ADX×Vol×AI)": lambda r:-(r["atr_pct"]*(r["adx"]/30)*r["vol_ratio"]*r["ai"]["ai_pct"]/1000),
                        "ATR% Highest":lambda r:-r["atr_pct"],"AI Score":lambda r:-r["ai"]["ai_pct"],
                        "ADX Strength":lambda r:-r["adx"],"Volume Surge":lambda r:-r["vol_ratio"],"RSI Oversold":lambda r:r["rsi"]}
                _vlist = sorted([r for r in _ps_vol if (not v_stf_only or r.get("st_flip"))],
                                key=_vsf.get(v_sort,_vsf["Momentum Score (ATR×ADX×Vol×AI)"]))

                # Quick summary table
                _vrows=[]
                for rv in _vlist:
                    _lt,_=_d_grade(rv["ai"]["ai_pct"]); _sl=rv["levels"]["short_term"]; _ll=rv["levels"]["long_term"]
                    _vrows.append({"#":_vlist.index(rv)+1,"Symbol":rv["symbol"],"Grade":_lt,
                        "AI%":round(rv["ai"]["ai_pct"],1),"ATR%":round(rv["atr_pct"],2),
                        "ADX":round(rv["adx"],1),"RSI":round(rv["rsi"],1),"Vol×":round(rv["vol_ratio"],2),
                        "⚡":("⚡" if rv.get("st_flip") else "—"),"F&O":("✅" if rv.get("is_fo") else "—"),
                        "Price ₹":rv["last_close"],"ST Tgt":_sl["tp"],"R:R":_sl["rr_str"],
                        "Up%":round((_sl["tp"]/_sl["entry"]-1)*100,1),"LT Tgt":_ll["tp"],
                        "Top Signal":rv["hits"][0][1][:35] if rv["hits"] else "—"})
                _vdf = pd.DataFrame(_vrows)
                st.dataframe(_vdf, use_container_width=True, height=280, hide_index=True,
                    column_config={
                        "AI%":   st.column_config.ProgressColumn("AI%",min_value=0,max_value=100,format="%.1f%%"),
                        "Price ₹":st.column_config.NumberColumn("Price ₹",format="₹%.2f"),
                        "ST Tgt":st.column_config.NumberColumn("ST Tgt",format="₹%.2f"),
                        "LT Tgt":st.column_config.NumberColumn("LT Tgt",format="₹%.2f"),
                        "Up%":  st.column_config.NumberColumn("Upside%",format="+%.1f%%"),
                    })
                _vcsv=_vdf.to_csv(index=False).encode()
                st.download_button("⬇️ Download CSV",data=_vcsv,
                    file_name=f"volatile_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",key="v_csv")

                st.markdown("<hr style='border-color:#2a3347;margin:14px 0'>", unsafe_allow_html=True)
                st.markdown(f"<div class='tv-section'>⚡ Interactive Signal Cards — Click to Expand Full Analysis</div>",
                            unsafe_allow_html=True)

                for rank_v, rv in enumerate(_vlist[:ps_top_n], 1):
                    _ai_v=rv["ai"]["ai_pct"]; _ltr_v,_gc_v=_d_grade(_ai_v)
                    _stl_v=rv["levels"]["short_term"]; _up_v=(_stl_v["tp"]/_stl_v["entry"]-1)*100
                    with st.expander(
                        f"#{rank_v}  {rv['symbol']}  ·  ATR {rv['atr_pct']:.1f}%  ·  "
                        f"AI {_ai_v:.1f}%  ·  ADX {rv['adx']:.1f}  ·  "
                        f"₹{rv['last_close']:,.2f}  →  ₹{_stl_v['tp']:,.2f} ({_up_v:+.1f}%)"
                        + ("  ·  ⚡ ST FLIP" if rv.get("st_flip") else ""),
                        expanded=(rank_v == 1),
                    ):
                        _render_stock_detail(rv, "Volatile Scan")

        # ══════════════════════════════════════════════════════════════════
        # SUB-TAB 2: MULTI-BAGGER PENNY PICKS
        # ══════════════════════════════════════════════════════════════════
        with ps_tab2:
            if not _ps_pny:
                if run_ps_btn:
                    st.warning("⚠️ No penny multi-bagger signals found. Try lowering threshold to 0.10.", icon="💎")
                else:
                    st.info("Run Power Scan to discover multi-bagger penny stocks.", icon="💎")
            else:
                st.markdown("""
                <div style='padding:10px 16px;background:#ef535015;border-left:3px solid #ef5350;border-radius:3px;margin-bottom:12px'>
                  <div style='font-size:.8rem;color:#ef5350;font-weight:700'>⚠️ EXTREME RISK WARNING</div>
                  <div style='font-size:.72rem;color:#787b86;margin-top:2px'>
                    Penny stocks carry extreme risk. Max 0.5–1% portfolio per trade. Stop-losses are mandatory.
                    This is technical analysis only — not financial advice. Always do independent research.
                  </div>
                </div>""", unsafe_allow_html=True)

                _avg_ai_p=sum(r["ai"]["ai_pct"] for r in _ps_pny)/len(_ps_pny)
                _n_stf_p=sum(1 for r in _ps_pny if r.get("has_stf"))
                _n_brk_p=sum(1 for r in _ps_pny if r.get("has_breakout"))
                _categories_p={}
                for r in _ps_pny:
                    c_=r["price_category"]; _categories_p[c_]=_categories_p.get(c_,0)+1

                st.markdown(
                    "<div style='display:flex;gap:10px;margin-bottom:12px;flex-wrap:wrap'>"
                    + "".join(
                        f"<div style='background:#131722;border-radius:4px;padding:10px 16px;text-align:center;border-top:2px solid {c_}'>"
                        f"<div class='tv-label'>{l_}</div>"
                        f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:{c_}'>{v_}</div></div>"
                        for l_,v_,c_ in [
                            ("Golden Picks",str(len(_ps_pny)),"#a855f7"),
                            ("Avg AI Score",f"{_avg_ai_p:.1f}%","#38bdf8"),
                            ("ST Flips ⚡",str(_n_stf_p),"#f59e0b"),
                            ("Breakouts 🚀",str(_n_brk_p),"#26a69a"),
                            ("Top Score",f"{_ps_pny[0]['golden_score']:.1f}⭐","#67e8f9"),
                        ]
                    )
                    + "</div>",
                    unsafe_allow_html=True,
                )

                # Category pills
                _cat_pills="".join(
                    f"<span style='background:#1c2030;color:#d1d4dc;padding:3px 10px;border-radius:2px;font-size:.76rem;margin:2px'>{c_} — {n_}</span>"
                    for c_,n_ in sorted(_categories_p.items())
                )
                st.markdown(f"<div style='margin-bottom:10px'>{_cat_pills}</div>", unsafe_allow_html=True)

                # Sort + filter
                pp1,pp2,pp3 = st.columns([2,1,1])
                with pp1:
                    p_sort=st.selectbox("Sort by",[
                        "Golden Score","AI Score","Price Low First","ST Flip First","Breakout First"
                    ],key="p_sort",label_visibility="collapsed")
                with pp2:
                    p_max_price=st.number_input("Max Price ₹",min_value=10,max_value=500,value=300,step=50,key="p_maxp",label_visibility="collapsed")
                with pp3:
                    p_ultra=st.toggle("Under ₹100 Only",False,key="p_ultra")

                _psf={"Golden Score":lambda r:-r["golden_score"],"AI Score":lambda r:-r["ai"]["ai_pct"],
                      "Price Low First":lambda r:r["last_close"],"ST Flip First":lambda r:-(r.get("has_stf",0)*100+r["ai"]["ai_pct"]),
                      "Breakout First":lambda r:-(r.get("has_breakout",0)*100+r["ai"]["ai_pct"])}
                _plist=sorted([r for r in _ps_pny if r["last_close"]<=p_max_price and (not p_ultra or r["last_close"]<100)],
                              key=_psf.get(p_sort,_psf["Golden Score"]))

                # Summary table
                _prows=[]
                for rp in _plist:
                    _lt,_=_d_grade(rp["ai"]["ai_pct"]); _sl=rp["levels"]["short_term"]; _ll=rp["levels"]["long_term"]
                    _up_p2=(_sl["tp"]/_sl["entry"]-1)*100
                    _prows.append({"Symbol":rp["symbol"],"Category":rp["price_category"],
                        "Price ₹":rp["last_close"],"Golden⭐":round(rp["golden_score"],1),
                        "AI%":round(rp["ai"]["ai_pct"],1),"Grade":_lt,
                        "Breakout":("🚀" if rp.get("has_breakout") else "—"),
                        "Vol Surge":("🔊" if rp.get("has_volume_surge") else "—"),
                        "ST Flip":("⚡" if rp.get("has_stf") else "—"),
                        "ADX":round(rp["adx"],1),"RSI":round(rp["rsi"],1),
                        "ST Tgt":_sl["tp"],"R:R":_sl["rr_str"],"Up%":round(_up_p2,1),
                        "LT Tgt":_ll["tp"],"Top Signal":rp["hits"][0][1][:35] if rp["hits"] else "—"})
                _pdf=pd.DataFrame(_prows)
                st.dataframe(_pdf,use_container_width=True,height=300,hide_index=True,
                    column_config={
                        "AI%":     st.column_config.ProgressColumn("AI%",min_value=0,max_value=100,format="%.1f%%"),
                        "Golden⭐":st.column_config.NumberColumn("Golden⭐",format="%.1f"),
                        "Price ₹": st.column_config.NumberColumn("Price ₹",format="₹%.2f"),
                        "ST Tgt":  st.column_config.NumberColumn("ST Tgt",format="₹%.2f"),
                        "LT Tgt":  st.column_config.NumberColumn("LT Tgt",format="₹%.2f"),
                        "Up%":     st.column_config.NumberColumn("Upside%",format="+%.1f%%"),
                    })
                _pcsv=_pdf.to_csv(index=False).encode()
                st.download_button("⬇️ Download Penny CSV",data=_pcsv,
                    file_name=f"penny_multibagger_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",key="p_csv")

                st.markdown("<hr style='border-color:#2a3347;margin:14px 0'>", unsafe_allow_html=True)
                st.markdown(f"<div class='tv-section'>💎 Interactive Multi-Bagger Cards — Click to Expand Full Analysis</div>",
                            unsafe_allow_html=True)

                for rank_p, rp in enumerate(_plist[:ps_top_n], 1):
                    _ai_p=rp["ai"]["ai_pct"]; _ltr_p,_gc_p=_d_grade(_ai_p)
                    _stl_p=rp["levels"]["short_term"]; _up_p=(_stl_p["tp"]/_stl_p["entry"]-1)*100
                    _gold=rp["golden_score"]; _stars="⭐"*min(5,max(1,int(_gold/3)))
                    with st.expander(
                        f"#{rank_p}  {rp['symbol']}  ·  {rp['price_category']}  ·  "
                        f"₹{rp['last_close']:.2f}  ·  Golden {_gold:.1f} {_stars}  ·  AI {_ai_p:.1f}%"
                        + ("  ·  ⚡ ST FLIP" if rp.get("has_stf") else "")
                        + ("  ·  🚀 BREAKOUT" if rp.get("has_breakout") else ""),
                        expanded=(rank_p == 1),
                    ):
                        # Golden score header
                        g1,g2,g3,g4,g5 = st.columns(5)
                        for gc_w_,gl_,gv_,gc_c_ in [
                            (g1,"Golden Score",f"{_gold:.1f} {_stars}","#a855f7"),
                            (g2,"AI Score",f"{_ai_p:.1f}%",_gc_p),
                            (g3,"Breakout","🚀 YES" if rp.get("has_breakout") else "—","#26a69a" if rp.get("has_breakout") else "#434651"),
                            (g4,"Vol Surge","🔊 YES" if rp.get("has_volume_surge") else "—","#3b82f6" if rp.get("has_volume_surge") else "#434651"),
                            (g5,"ST Flip","⚡ YES" if rp.get("has_stf") else "—","#f59e0b" if rp.get("has_stf") else "#434651"),
                        ]:
                            gc_w_.markdown(
                                f"<div style='background:#131722;border-radius:4px;padding:10px;text-align:center;border-top:3px solid {gc_c_}'>"
                                f"<div class='tv-label'>{gl_}</div>"
                                f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{gc_c_};font-size:.95rem;margin:6px 0'>{gv_}</div>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                        _render_stock_detail(rp, "Penny Multi-Bagger")


    # TAB 4 — SIGNAL CARDS
    # ══════════════════════════════════════════════════════════════════════
    with tabs[4]:

        # ── Collapsible bullish stock list ───────────────────────────────
        st.markdown("<div class='tv-section'>🟢 All Bullish Signals — Click to Open</div>",
                    unsafe_allow_html=True)

        for idx_r, r_li in enumerate(alerts):
            ai_li   = r_li["ai"]["ai_pct"]
            ltr_li, gc_li = _d_grade(ai_li)
            stl_li  = r_li["levels"]["short_term"]
            ltl_li  = r_li["levels"]["long_term"]
            up_li   = (stl_li["tp"] / stl_li["entry"] - 1) * 100
            fo_li   = "✅ F&O" if r_li["is_fo"] else ""
            stf_li  = "⚡ ST Flip" if r_li.get("st_flip") else ""
            top_sig_li = r_li["hits"][0][1] if r_li["hits"] else "—"

            expander_label = (
                f"#{idx_r+1}  {r_li['symbol']}   "
                f"Grade: {ltr_li}   AI: {ai_li:.1f}%   "
                f"₹{r_li['last_close']:,.2f}   "
                f"ST Tgt: ₹{stl_li['tp']:,.2f} ({up_li:+.1f}%)   "
                f"{r_li['indices']}   {fo_li}  {stf_li}"
            )

            with st.expander(expander_label, expanded=False):
                ec1, ec2, ec3, ec4 = st.columns(4)
                ec1.metric("AI Score",    f"{ai_li:.1f}%")
                ec2.metric("Market",      f"{r_li['mkt']['pct']:.1f}%")
                ec3.metric("Pattern",     f"{r_li['pat_conf']*100:.1f}%")
                ec4.metric("Comp Score",  f"{r_li['score']:+.4f}")

                lc1, lc2, lc3 = st.columns(3)
                lc1.metric("ST Entry ₹",  f"₹{stl_li['entry']:,.2f}")
                lc1.metric("ST Target ₹", f"₹{stl_li['tp']:,.2f}",
                           f"{up_li:+.1f}%")
                lc2.metric("ST Stop ₹",   f"₹{stl_li['sl']:,.2f}",
                           f"{(stl_li['sl']/stl_li['entry']-1)*100:+.1f}%")
                lc2.metric("ST R:R",      stl_li["rr_str"])
                lc3.metric("LT Target ₹", f"₹{ltl_li['tp']:,.2f}",
                           f"{(ltl_li['tp']/ltl_li['entry']-1)*100:+.1f}%")
                lc3.metric("LT R:R",      ltl_li["rr_str"])

                st.markdown(
                    f"<div style='font-size:.78rem;color:#787b86;line-height:1.7;padding:8px 0'>"
                    f"<b style='color:#38bdf8'>Top Signal:</b> {top_sig_li}<br>"
                    f"<b style='color:#38bdf8'>RSI:</b> {r_li['rsi']:.1f} &nbsp;·&nbsp; "
                    f"<b style='color:#38bdf8'>ADX:</b> {r_li['adx']:.1f} &nbsp;·&nbsp; "
                    f"<b style='color:#38bdf8'>Volume:</b> {r_li['vol_ratio']:.2f}× &nbsp;·&nbsp; "
                    f"<b style='color:#38bdf8'>ATR:</b> {r_li['atr_pct']:.2f}%<br>"
                    f"<b style='color:#38bdf8'>Sector:</b> {r_li.get('sector','N/A')} &nbsp;·&nbsp; "
                    f"<b style='color:#38bdf8'>EMA9/21/50:</b> {r_li['ema9']:.1f} / {r_li['ema21']:.1f} / {r_li['ema50']:.1f}"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                if st.button(f"📊 Open Full Card — {r_li['symbol']}", key=f"open_{idx_r}"):
                    st.session_state["card_sym"] = r_li["symbol"]
                    st.rerun()

        st.markdown("<hr style='border-color:#2a3347;margin:20px 0'>", unsafe_allow_html=True)
        st.markdown("<div class='tv-section'>📋 Detailed Signal Card</div>", unsafe_allow_html=True)

        # ── Inline filter bar ─────────────────────────────────────────────
        fc1,fc2,fc3,fc4 = st.columns([2,1,1,1])
        with fc1:
            sel_sym = st.selectbox("Select Stock", [r["symbol"] for r in alerts],
                format_func=lambda s: f"{s}  —  AI: {next(r['ai']['ai_pct'] for r in alerts if r['symbol']==s):.1f}%",
                key="card_sym")
        with fc2:
            chart_interval = st.selectbox("Chart Interval", ["D","W","60","15","5"], key="ci")
        with fc3:
            show_tv_ta = st.toggle("Show TV Analysis", value=True, key="tv_ta")
        with fc4:
            show_tv_fin = st.toggle("Show Financials", value=False, key="tv_fin")

        r = next(x for x in alerts if x["symbol"] == sel_sym)
        ai=r["ai"]; mk=r["mkt"]; stl=r["levels"]["short_term"]; ltl=r["levels"]["long_term"]
        hits=r["hits"]; ai_p=ai["ai_pct"]; mk_p=mk["pct"]; pt_p=r["pat_conf"]*100
        ltr,gc=_d_grade(ai_p)

        # ── Symbol info bar ────────────────────────────────────────────────
        components.html(tv_symbol_info(sel_sym, nonce=sel_sym), height=80, scrolling=False)

        # ── Stock header ───────────────────────────────────────────────────
        _fo_tag_card = "<span class='pill-cyan' style='font-size:.75rem'>F&amp;O ✅</span>" if r["is_fo"] else ""
        _rsi_col_card = _d_rsi_col(r["rsi"])
        _adx_col_card = _d_adx_col(r["adx"])
        _vol_col_card = _d_vol_col(r["vol_ratio"])
        _card_html = (
            f"<div class='tv-card tv-card-bull' style='border-color:{gc}66'>"
            "<div style='display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:14px'>"
            "<div style='display:flex;align-items:center;gap:14px'>"
            f"<span class='tv-badge-sym' style='font-size:1.3rem;padding:5px 16px'>{r['symbol']}</span>"
            f"<span class='tv-badge-grade' style='background:{gc}22;color:{gc};font-size:1.4rem'>{ltr}</span>"
            f"{_fo_tag_card}"
            "<div>"
            f"<div class='tv-label'>{r.get('sector','N/A')}</div>"
            f"<div style='font-size:.72rem;color:#434651'>{r['indices']}</div>"
            "</div></div>"
            "<div style='display:flex;gap:22px;flex-wrap:wrap'>"
            "<div style='text-align:center'>"
            "<div class='tv-label'>Last Price</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.8rem;color:#38bdf8'>&#8377;{r['last_close']:,.2f}</div>"
            "</div>"
            "<div style='text-align:center'>"
            "<div class='tv-label'>RSI (14)</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_rsi_col_card}'>{r['rsi']:.1f}</div>"
            "</div>"
            "<div style='text-align:center'>"
            "<div class='tv-label'>ADX</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_adx_col_card}'>{r['adx']:.1f}</div>"
            "</div>"
            "<div style='text-align:center'>"
            "<div class='tv-label'>Volume</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.3rem;color:{_vol_col_card}'>{r['vol_ratio']:.2f}&times;</div>"
            "</div>"
            "</div></div></div>"
        )
        st.markdown(_card_html, unsafe_allow_html=True)

        # ── Confidence row ─────────────────────────────────────────────────
        cc1, cc2, cc3 = st.columns(3)
        _conf_data = [
            (cc1, ai_p,  "🤖 AI Confidence",
             f"T:{ai['trend_s']:+.2f}  M:{ai['mom_s']:+.2f}  B:{ai['brk_s']:+.2f}  V:{ai['vol_s']:+.2f}  F:{ai['fund_s']:+.2f}",
             gc),
            (cc2, mk_p, "📊 Market Confidence",
             f"{mk['label']} · {mk['align']}",
             "#26a69a" if mk_p >= 65 else "#f59e0b" if mk_p >= 40 else "#ef5350"),
            (cc3, pt_p, "🎯 Pattern Confidence",
             f"{len(hits)} signals · {r['n_cats']} categories",
             "#26a69a" if pt_p >= 60 else "#f59e0b" if pt_p >= 40 else "#ef5350"),
        ]
        for _col_w, _pct, _label, _extra, _col_hex in _conf_data:
            _gauge_bar = gauge_html(_pct, "", 220)
            _conf_html = (
                "<div class='tv-card' style='text-align:center'>"
                f"<div class='tv-label'>{_label}</div>"
                f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.9rem;"
                f"color:{_col_hex};margin:8px 0'>{_pct:.1f}%</div>"
                f"{_gauge_bar}"
                f"<div style='font-size:.68rem;color:#434651;margin-top:5px'>{_extra}</div>"
                "</div>"
            )
            _col_w.markdown(_conf_html, unsafe_allow_html=True)

        # ── Trade scenarios ────────────────────────────────────────────────
        st.markdown("<div class='tv-section'>📐 Trade Scenarios</div>", unsafe_allow_html=True)
        t1,t2,t3 = st.columns(3)
        dip_e=round(r["last_close"]*0.98,2); dip_sl=round(dip_e-r["atr"]*1.2,2)
        dip_tp=round(dip_e+r["atr"]*2.5,2);  dip_ri=round(dip_e-dip_sl,2)
        dip_rw=round(dip_tp-dip_e,2);        dip_rr=round(dip_rw/dip_ri,2) if dip_ri>0 else 0
        t1.markdown(trade_scenario_html("⚡ Aggressive (Short-Term)", stl, "#f59e0b","2–5 trading days"), unsafe_allow_html=True)
        t2.markdown(trade_scenario_html("📅 Swing (Long-Term)",       ltl, "#26a69a","10–20 trading days"), unsafe_allow_html=True)
        t3.markdown(trade_scenario_html("📌 Limit/Dip Entry (−2%)",
            dict(entry=dip_e,tp=dip_tp,sl=dip_sl,rr_str=f"1:{dip_rr}",risk=dip_ri,reward=dip_rw),
            "#3b82f6","Limit at −2%"), unsafe_allow_html=True)

        # ── Position sizing ────────────────────────────────────────────────
        st.markdown("<div class='tv-section'>💰 Position Sizing</div>", unsafe_allow_html=True)
        capital = st.session_state.get("capital_val",1_000_000)
        sl_risk = stl["risk"]
        kf = min(max(0.55-(1-0.55)/1.5,0),0.25)
        ps_cols = st.columns(4)
        for idx,(lbl_sz,qty) in enumerate([
            ("1% Risk Rule",  max(1,int(capital*0.01/sl_risk)) if sl_risk else 0),
            ("2% Risk Rule",  max(1,int(capital*0.02/sl_risk)) if sl_risk else 0),
            (f"Half-Kelly ({kf:.1%})", max(1,int(capital*kf/r["last_close"])) if r["last_close"] else 0),
            ("Fixed 20%",     int(capital*0.20/r["last_close"]) if r["last_close"] else 0),
        ]):
            inv=qty*r["last_close"]; ml=qty*sl_risk; tp_=qty*stl["reward"]; pct=inv/capital*100
            pc="#26a69a" if pct<=20 else "#f59e0b" if pct<=30 else "#ef5350"
            ps_cols[idx].markdown(f"""
            <div class='tv-card' style='text-align:center'>
              <div class='tv-label'>{lbl_sz}</div>
              <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.4rem;color:#38bdf8;margin:6px 0'>{qty:,}</div>
              <div style='font-size:.74rem;margin-top:8px;line-height:1.6'>
                <div style='display:flex;justify-content:space-between'><span class='tv-label'>Capital</span><span>₹{inv:,.0f}</span></div>
                <div style='display:flex;justify-content:space-between'><span class='tv-label'>Max Loss</span><span style='color:#ef5350'>₹{ml:,.0f}</span></div>
                <div style='display:flex;justify-content:space-between'><span class='tv-label'>Target P&L</span><span style='color:#26a69a'>₹{tp_:,.0f}</span></div>
                <div style='display:flex;justify-content:space-between'><span class='tv-label'>% Portfolio</span><span style='color:{pc}'>{pct:.1f}%</span></div>
              </div>
            </div>""", unsafe_allow_html=True)

        # ── Fundamentals + Research note ───────────────────────────────────
        st.markdown("<div class='tv-section'>🏦 Fundamentals & Research Note</div>", unsafe_allow_html=True)
        fn_col, note_col = st.columns([1, 2])
        pe=r.get("pe"); roe=r.get("roe"); mcap=r.get("mcap"); beta=r.get("beta")
        w52h=r.get("w52h"); w52l=r.get("w52l")
        pec="#26a69a" if pe and pe<20 else "#f59e0b" if pe and pe<35 else "#ef5350"
        rec="#26a69a" if roe and roe>0.18 else "#f59e0b" if roe and roe>0.10 else "#ef5350"
        fn_col.markdown(f"""
        <div class='tv-card'>
          <div style='display:grid;gap:6px;font-size:.82rem'>
            {''.join(f"<div style='display:flex;justify-content:space-between;border-bottom:1px solid #2a3347;padding-bottom:5px'><span class='tv-label'>{lb2}</span><span style='color:{vc};font-weight:600'>{vv}</span></div>"
            for lb2,vc,vv in [
              ("P/E Ratio",pec,f"{pe:.1f}" if pe else "N/A"),
              ("ROE",rec,f"{roe*100:.1f}%" if roe else "N/A"),
              ("Market Cap","#d1d4dc",fmt_cr(mcap)),
              ("Beta","#d1d4dc",f"{beta:.2f}" if beta else "N/A"),
              ("52W High","#26a69a",fmt_inr(w52h)),
              ("52W Low","#f59e0b",fmt_inr(w52l)),
              ("Traded Val","#38bdf8",f"₹{r['traded_val_cr']:.2f} Cr/d"),
            ])}
          </div>
        </div>""", unsafe_allow_html=True)
        note_col.markdown(f"""
        <div class='tv-card' style='line-height:1.75;font-size:.82rem;color:#787b86'>
          <div class='tv-label' style='margin-bottom:8px'>📝 Research Summary</div>
          {r["reason"].replace("  •  ","<br>→ ")}
          <div style='margin-top:10px;padding-top:8px;border-top:1px solid #2a3347;font-size:.7rem;color:#434651'>
            Score: <span style='color:{_d_score_col(r["score"])};font-weight:700'>{r["score"]:+.4f}</span>
            &nbsp;·&nbsp; ATR: {r["atr_pct"]:.2f}%
            &nbsp;·&nbsp; Vol Z: {r.get("vol_z",0):.2f}σ
            &nbsp;·&nbsp; Traded: ₹{r["traded_val_cr"]:.2f} Cr/day
          </div>
        </div>""", unsafe_allow_html=True)

        # ── Pattern hits ───────────────────────────────────────────────────
        st.markdown("<div class='tv-section'>🎯 Pattern Hits</div>", unsafe_allow_html=True)
        CAT_C = {"Trend":"#38bdf8","Momentum":"#f59e0b","Candlestick":"#a855f7",
                 "Breakout":"#26a69a","Volume":"#3b82f6","Volatility":"#787b86",
                 "Price Action":"#d1d4dc","Structure":"#67e8f9"}
        cm: dict = defaultdict(list)
        for sc,lb,cat in hits: cm[cat].append((sc,lb))
        for cat, items in cm.items():
            cc_=CAT_C.get(cat,"#787b86")
            rows_html = "".join([
                f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:6px'>"
                f"<div style='width:{int(sc*120)}px;height:4px;background:{cc_};border-radius:2px'></div>"
                f"<span style='font-size:.8rem'>{lb}</span>"
                f"<span style='font-size:.7rem;color:{cc_};margin-left:auto'>{sc*100:.0f}%</span></div>"
                for sc,lb in items
            ])
            with st.expander(f"  {cat}  ({len(items)})", expanded=len(items)>=2):
                st.markdown(f"<div style='padding:4px 0'>{rows_html}</div>", unsafe_allow_html=True)

        # ── TradingView Technical Analysis ─────────────────────────────────
        if show_tv_ta:
            st.markdown("<div class='tv-section'>🧮 TradingView Technical Analysis</div>", unsafe_allow_html=True)
            components.html(tv_technical_analysis(sel_sym, nonce=sel_sym), height=470, scrolling=False)

        # ── TradingView Financials ─────────────────────────────────────────
        if show_tv_fin:
            st.markdown("<div class='tv-section'>💹 Financial Statements</div>", unsafe_allow_html=True)
            components.html(tv_financials(sel_sym), height=850, scrolling=False)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 5 — LIVE CHARTS (TradingView)
    # ══════════════════════════════════════════════════════════════════════
    with tabs[5]:
        ch_c1, ch_c2 = st.columns([3, 1])
        with ch_c1:
            chart_sym = st.selectbox("Select Stock for Chart",
                [r["symbol"] for r in alerts], key="tv_chart_sym")
        with ch_c2:
            chart_h = st.select_slider("Chart Height",
                options=[400,480,560,640,720], value=560, key="chart_h")

        # Main TradingView chart
        r_chart = next(x for x in alerts if x["symbol"] == chart_sym)
        st.markdown("<div class='tv-section'>📊 TradingView Live Chart</div>", unsafe_allow_html=True)

        # Signal level overlay info
        stl_c = r_chart["levels"]["short_term"]; ltl_c = r_chart["levels"]["long_term"]
        info_c1, info_c2, info_c3, info_c4, info_c5 = st.columns(5)
        info_c1.metric("Entry",     f"₹{stl_c['entry']:,.2f}")
        info_c2.metric("ST Target", f"₹{stl_c['tp']:,.2f}",    f"{(stl_c['tp']/stl_c['entry']-1)*100:+.1f}%")
        info_c3.metric("ST SL",     f"₹{stl_c['sl']:,.2f}",    f"{(stl_c['sl']/stl_c['entry']-1)*100:+.1f}%")
        info_c4.metric("LT Target", f"₹{ltl_c['tp']:,.2f}",    f"{(ltl_c['tp']/ltl_c['entry']-1)*100:+.1f}%")
        info_c5.metric("R:R (ST)",  stl_c["rr_str"])

        components.html(tv_chart_widget(chart_sym, height=chart_h), height=chart_h+10, scrolling=False)

        # ── Mini charts grid ───────────────────────────────────────────────
        st.markdown("<div class='tv-section' style='margin-top:20px'>📊 All Signal Mini Charts</div>", unsafe_allow_html=True)
        grid_cols = st.columns(3)
        for i, r_g in enumerate(alerts[:9]):
            with grid_cols[i % 3]:
                ai_g=r_g["ai"]["ai_pct"]; ltr_g,gc_g=_d_grade(ai_g)
                st_g=r_g["levels"]["short_term"]
                st.markdown(f"""
                <div class='tv-card tv-card-bull' style='padding:8px 12px;margin-bottom:4px'>
                  <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:4px'>
                    <span class='tv-badge-sym' style='font-size:.8rem;padding:2px 8px'>{r_g["symbol"]}</span>
                    <span style='font-family:Syne,sans-serif;font-weight:700;color:#38bdf8'>₹{r_g["last_close"]:,.2f}</span>
                  </div>
                  <div style='display:flex;justify-content:space-between;font-size:.7rem;color:#434651'>
                    <span>AI: <span style='color:{gc_g}'>{ai_g:.0f}%</span></span>
                    <span>Tgt: <span style='color:#26a69a'>₹{st_g["tp"]:,.0f}</span></span>
                    <span>{st_g["rr_str"]}</span>
                  </div>
                </div>""", unsafe_allow_html=True)
                components.html(tv_mini_chart(r_g["symbol"], height=160, nonce=r_g["symbol"]), height=164, scrolling=False)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 6 — ANALYSIS
    # ══════════════════════════════════════════════════════════════════════
    with tabs[6]:
        # ── Stock + View selector ─────────────────────────────────────────
        anc1, anc2 = st.columns([2, 2])
        with anc1:
            an_sym = st.selectbox(
                "Stock for Analysis",
                [r["symbol"] for r in alerts], key="an_sym",
                format_func=lambda s: f"{s}  —  AI: {next(r['ai']['ai_pct'] for r in alerts if r['symbol']==s):.1f}%"
            )
        with anc2:
            an_view = st.radio(
                "View",
                ["📊 Basic Analysis", "🔬 Deep Analysis", "📺 TradingView TA"],
                horizontal=True, key="an_view"
            )

        r_an   = next(x for x in alerts if x["symbol"] == an_sym)
        ai_an  = r_an["ai"]; mk_an  = r_an["mkt"]
        stl_an = r_an["levels"]["short_term"]; ltl_an = r_an["levels"]["long_term"]
        ltr_an, gc_an = _d_grade(ai_an["ai_pct"])

        # ── Quick KPI strip ───────────────────────────────────────────────
        _kpi_rsi_col = _d_rsi_col(r_an["rsi"])
        _kpi_adx_col = _d_adx_col(r_an["adx"])
        _kpi_vol_col = _d_vol_col(r_an["vol_ratio"])
        _kpi_html = (
            f"<div class='tv-card tv-card-bull' style='padding:12px 18px;border-color:{gc_an}55;margin-bottom:4px'>"
            f"<div style='display:flex;gap:24px;flex-wrap:wrap;align-items:center'>"
            f"<span class='tv-badge-sym' style='font-size:.95rem;padding:3px 12px'>{an_sym}</span>"
            f"<span class='tv-badge-grade' style='background:{gc_an}22;color:{gc_an};font-size:1.1rem'>{ltr_an}</span>"
            f"<div style='text-align:center'><div class='tv-label'>Price</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:#38bdf8'>&#8377;{r_an['last_close']:,.2f}</div></div>"
            f"<div style='text-align:center'><div class='tv-label'>AI Score</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{gc_an}'>{ai_an['ai_pct']:.1f}%</div></div>"
            f"<div style='text-align:center'><div class='tv-label'>RSI</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{_kpi_rsi_col}'>{r_an['rsi']:.1f}</div></div>"
            f"<div style='text-align:center'><div class='tv-label'>ADX</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{_kpi_adx_col}'>{r_an['adx']:.1f}</div></div>"
            f"<div style='text-align:center'><div class='tv-label'>Volume</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{_kpi_vol_col}'>{r_an['vol_ratio']:.2f}&times;</div></div>"
            f"<div style='text-align:center'><div class='tv-label'>ATR %</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:#f59e0b'>{r_an['atr_pct']:.2f}%</div></div>"
            f"<div style='text-align:center'><div class='tv-label'>ST Target</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:#26a69a'>&#8377;{stl_an['tp']:,.2f}</div></div>"
            f"<div style='text-align:center'><div class='tv-label'>R:R</div>"
            f"<div style='font-family:Syne,sans-serif;font-weight:700;color:#d1d4dc'>{stl_an['rr_str']}</div></div>"
            f"</div></div>"
        )
        st.markdown(_kpi_html, unsafe_allow_html=True)

        # ══════════════════════════════════════════════════════════════════
        # VIEW 1 — BASIC ANALYSIS (Technical + Fundamental clearly separated)
        # ══════════════════════════════════════════════════════════════════
        if an_view == "📊 Basic Analysis":

            # ────────────────────────────────────────────────────────────────
            # SECTION A: TECHNICAL ANALYSIS
            # ────────────────────────────────────────────────────────────────
            st.markdown("""
            <div style='margin:18px 0 12px;padding:10px 18px;
                        background:linear-gradient(90deg,#38bdf820,#0b0e11);
                        border-left:4px solid #38bdf8;border-radius:4px'>
              <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.05rem;
                          color:#38bdf8;letter-spacing:.1em'>
                📊 TECHNICAL ANALYSIS
              </div>
              <div style='font-size:.72rem;color:#787b86;margin-top:2px'>
                Price action, indicators, momentum and trend analysis
              </div>
            </div>""", unsafe_allow_html=True)

            # ── T1: Trend & EMA Analysis ──────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#38bdf8;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin-bottom:10px'>📈 Trend & EMA Structure</div>", unsafe_allow_html=True)

            e9=r_an["ema9"]; e21=r_an["ema21"]; e50=r_an["ema50"]; e200=r_an.get("ema200",0)
            c_=r_an["last_close"]
            ema_stack = "🟢 Full Bull (9>21>50>200)" if c_>e9>e21>e50>e200 and e200>0 else \
                        "🟢 Bull Stack (9>21>50)" if c_>e9>e21>e50 else \
                        "🟡 Partial Bull (9>21)" if c_>e9>e21 else \
                        "🔴 Bearish Stack"
            ema_col_  = "#26a69a" if "Bull" in ema_stack else "#ef5350"

            te1,te2,te3,te4 = st.columns(4)
            for tc, lbl_, val_, ref_, is_bull in [
                (te1,"EMA 9",  f"₹{e9:.2f}",  f"{'Above' if c_>e9 else 'Below'} price", c_>e9),
                (te2,"EMA 21", f"₹{e21:.2f}", f"{'Above' if c_>e21 else 'Below'} price",c_>e21),
                (te3,"EMA 50", f"₹{e50:.2f}", f"{'Above' if c_>e50 else 'Below'} price",c_>e50),
                (te4,"EMA 200",f"₹{e200:.2f}" if e200 else "N/A","Long-term trend",c_>e200),
            ]:
                _col = "#26a69a" if is_bull else "#ef5350"
                tc.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px 12px;"
                    f"border-left:3px solid {_col};text-align:center'>"
                    f"<div class='tv-label'>{lbl_}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1rem;color:#d1d4dc;margin:4px 0'>{val_}</div>"
                    f"<div style='font-size:.72rem;color:{_col}'>{'✅' if is_bull else '❌'} {ref_}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            st.markdown(
                f"<div style='background:#131722;border-radius:4px;padding:10px 16px;margin:8px 0;display:flex;gap:14px;align-items:center'>"
                f"<span style='font-size:.82rem;color:#787b86'>EMA Stack:</span>"
                f"<span style='font-weight:700;color:{ema_col_}'>{ema_stack}</span>"
                f"<span style='margin-left:auto;font-size:.75rem;color:#787b86'>SuperTrend:</span>"
                f"<span style='font-weight:700;color:{'#a855f7' if r_an.get('st_flip') else '#26a69a'}'>{'⚡ JUST FLIPPED BULLISH' if r_an.get('st_flip') else '🟢 Bullish Mode'}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

            # ── T2: Momentum Indicators ───────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#38bdf8;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin:14px 0 10px'>⚡ Momentum Indicators</div>", unsafe_allow_html=True)

            rsi_=r_an["rsi"]; adx_=r_an["adx"]; macd_=r_an["macd_h"]; vol_=r_an["vol_ratio"]

            mom_data = [
                ("RSI (14)", rsi_, f"{rsi_:.1f}",
                 "OVERSOLD 🟢" if rsi_<30 else "Near Oversold" if rsi_<42 else "Neutral" if rsi_<58 else "Elevated ⚠️" if rsi_<72 else "OVERBOUGHT 🔴",
                 _d_rsi_col(rsi_)),
                ("ADX", adx_, f"{adx_:.1f}",
                 "Very Strong 💪" if adx_>=40 else "Strong" if adx_>=28 else "Moderate" if adx_>=20 else "Weak ⚠️",
                 _d_adx_col(adx_)),
                ("MACD Hist", None, f"{macd_:+.4f}",
                 "Positive — Bullish ✅" if macd_>0 else "Negative — Bearish",
                 "#26a69a" if macd_>0 else "#ef5350"),
                ("Volume Ratio", None, f"{vol_:.2f}×",
                 "SURGE 🔊" if vol_>=2.5 else "High 📈" if vol_>=1.5 else "Average" if vol_>=0.8 else "Low ⬇️",
                 _d_vol_col(vol_)),
                ("ATR %", None, f"{r_an['atr_pct']:.2f}%",
                 "Ideal ✓" if 1.5<r_an['atr_pct']<5 else "High ⚠️",
                 "#26a69a" if 1.5<r_an['atr_pct']<5 else "#f59e0b"),
            ]

            mc1,mc2,mc3,mc4,mc5 = st.columns(5)
            for mc, (lbl_m, num_m, val_m, status_m, col_m) in zip([mc1,mc2,mc3,mc4,mc5], mom_data):
                bar_w = int(min(max((num_m if num_m else 50)/100,0),1)*100) if num_m is not None else 50
                mc.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px 10px;border-bottom:3px solid {col_m}'>"
                    f"<div class='tv-label'>{lbl_m}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.1rem;color:{col_m};margin:5px 0'>{val_m}</div>"
                    f"<div style='font-size:.68rem;color:#787b86'>{status_m}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # ── T3: Confidence Gauges ─────────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#38bdf8;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin:14px 0 10px'>🎯 Signal Confidence</div>", unsafe_allow_html=True)

            cg1,cg2,cg3 = st.columns(3)
            for gc_w, pct_, lbl_g, clr_g, extra_g in [
                (cg1, ai_an["ai_pct"],          "🤖 AI Model Score",      gc_an,
                 f"T:{ai_an['trend_s']:+.2f}  M:{ai_an['mom_s']:+.2f}  B:{ai_an['brk_s']:+.2f}  V:{ai_an['vol_s']:+.2f}"),
                (cg2, mk_an["pct"],              "📊 Market Confidence",
                 "#26a69a" if mk_an["pct"]>=65 else "#f59e0b" if mk_an["pct"]>=40 else "#ef5350",
                 f"{mk_an['label']}  ·  {mk_an['align']}"),
                (cg3, r_an["pat_conf"]*100,      "🎯 Pattern Confidence",
                 "#26a69a" if r_an["pat_conf"]*100>=60 else "#f59e0b" if r_an["pat_conf"]*100>=40 else "#ef5350",
                 f"{len(r_an['hits'])} signals  ·  {r_an['n_cats']} categories"),
            ]:
                _gb = gauge_html(pct_, "", 220)
                gc_w.markdown(
                    f"<div class='tv-card' style='text-align:center;padding:14px 12px;border-top:3px solid {clr_g}'>"
                    f"<div class='tv-label'>{lbl_g}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.8rem;color:{clr_g};margin:8px 0'>{pct_:.1f}%</div>"
                    f"{_gb}"
                    f"<div style='font-size:.68rem;color:#434651;margin-top:6px'>{extra_g}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # ── T4: Trade Setup ───────────────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#38bdf8;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin:14px 0 10px'>📐 Trade Setup</div>", unsafe_allow_html=True)
            ts1,ts2,ts3 = st.columns(3)
            _dip_e2=round(c_*0.98,2); _dip_sl2=round(_dip_e2-r_an["atr"]*1.2,2)
            _dip_tp2=round(_dip_e2+r_an["atr"]*2.5,2); _dip_rr2=round((_dip_tp2-_dip_e2)/max(_dip_e2-_dip_sl2,0.01),2)
            for ts_col,title_,entry_,tp_,sl_,rr_,wind_,brd_ in [
                (ts1,"⚡ Aggressive (Short-Term)",stl_an["entry"],stl_an["tp"],stl_an["sl"],stl_an["rr_str"],"2–5 days","#f59e0b"),
                (ts2,"📅 Swing (Long-Term)",       ltl_an["entry"],ltl_an["tp"],ltl_an["sl"],ltl_an["rr_str"],"10–20 days","#26a69a"),
                (ts3,"📌 Limit / Dip Entry",       _dip_e2,_dip_tp2,_dip_sl2,f"1:{_dip_rr2}","Limit at −2%","#3b82f6"),
            ]:
                _up_ = (tp_/entry_-1)*100; _dn_ = (sl_/entry_-1)*100
                ts_col.markdown(
                    f"<div class='tv-card' style='border-top:3px solid {brd_};text-align:center;padding:12px'>"
                    f"<div style='color:{brd_};font-size:.78rem;font-weight:700;margin-bottom:10px'>{title_}</div>"
                    f"<div class='tv-label'>Entry</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.15rem;color:#38bdf8;margin-bottom:8px'>&#8377;{entry_:,.2f}</div>"
                    f"<div style='display:flex;justify-content:space-around;margin-bottom:8px'>"
                    f"<div><div class='tv-label'>Target</div><div style='font-weight:700;color:#26a69a'>&#8377;{tp_:,.2f}</div>"
                    f"<div style='font-size:.68rem;color:#26a69a'>{_up_:+.1f}%</div></div>"
                    f"<div><div class='tv-label'>Stop</div><div style='font-weight:700;color:#ef5350'>&#8377;{sl_:,.2f}</div>"
                    f"<div style='font-size:.68rem;color:#ef5350'>{_dn_:+.1f}%</div></div>"
                    f"</div>"
                    f"<div style='display:flex;justify-content:space-around;border-top:1px solid #2a3347;padding-top:7px'>"
                    f"<div><div class='tv-label'>R:R</div><div style='font-weight:700;color:#d1d4dc'>{rr_}</div></div>"
                    f"<div><div class='tv-label'>Window</div><div style='font-size:.72rem;color:#787b86'>{wind_}</div></div>"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )

            # ── T5: Pattern Hits ──────────────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#38bdf8;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin:14px 0 10px'>🎯 Detected Patterns & Signals</div>", unsafe_allow_html=True)
            hits_an = r_an["hits"]
            CAT_C_AN = {"Trend":"#38bdf8","Momentum":"#f59e0b","Candlestick":"#a855f7",
                        "Breakout":"#26a69a","Volume":"#3b82f6","Volatility":"#787b86",
                        "Price Action":"#d1d4dc","Structure":"#67e8f9"}
            for sc_h, lb_h, cat_h in hits_an[:8]:
                _cc_ = CAT_C_AN.get(cat_h,"#787b86"); _bw_ = int(sc_h*200)
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:10px;padding:7px 12px;"
                    f"margin-bottom:5px;background:#131722;border-radius:4px;border-left:3px solid {_cc_}'>"
                    f"<span style='font-size:.68rem;font-weight:600;color:{_cc_};min-width:90px'>{cat_h}</span>"
                    f"<div style='height:4px;width:{_bw_}px;background:{_cc_};border-radius:2px;min-width:4px'></div>"
                    f"<span style='font-size:.82rem;color:#d1d4dc;flex:1'>{lb_h}</span>"
                    f"<span style='font-size:.75rem;color:{_cc_};font-weight:700;min-width:34px;text-align:right'>{sc_h*100:.0f}%</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # ────────────────────────────────────────────────────────────────
            # SECTION B: FUNDAMENTAL ANALYSIS
            # ────────────────────────────────────────────────────────────────
            st.markdown("""
            <div style='margin:24px 0 12px;padding:10px 18px;
                        background:linear-gradient(90deg,#a855f720,#0b0e11);
                        border-left:4px solid #a855f7;border-radius:4px'>
              <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.05rem;
                          color:#a855f7;letter-spacing:.1em'>
                🏦 FUNDAMENTAL ANALYSIS
              </div>
              <div style='font-size:.72rem;color:#787b86;margin-top:2px'>
                Financial health, valuation, ownership and business quality
              </div>
            </div>""", unsafe_allow_html=True)

            pe_an=r_an.get("pe"); roe_an=r_an.get("roe"); mcap_an=r_an.get("mcap")
            beta_an=r_an.get("beta"); w52h_an=r_an.get("w52h"); w52l_an=r_an.get("w52l")
            pb_an=r_an.get("pb")

            # ── F1: Valuation Metrics ─────────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#a855f7;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin-bottom:10px'>💰 Valuation & Size</div>", unsafe_allow_html=True)

            fv1,fv2,fv3,fv4,fv5 = st.columns(5)
            val_cards = [
                (fv1,"P/E Ratio",   f"{pe_an:.1f}" if pe_an else "N/A",
                 "Cheap" if pe_an and pe_an<15 else "Fair" if pe_an and pe_an<28 else "Expensive" if pe_an and pe_an<50 else "N/A",
                 "#26a69a" if pe_an and pe_an<20 else "#f59e0b" if pe_an and pe_an<35 else "#ef5350"),
                (fv2,"P/B Ratio",   f"{pb_an:.2f}" if pb_an else "N/A",
                 "Cheap" if pb_an and pb_an<1.5 else "Fair" if pb_an and pb_an<4 else "Expensive" if pb_an else "N/A",
                 "#26a69a" if pb_an and pb_an<2 else "#f59e0b" if pb_an and pb_an<5 else "#ef5350"),
                (fv3,"Market Cap",  fmt_cr(mcap_an) if mcap_an else "N/A",
                 "Large Cap" if mcap_an and mcap_an>2000 else "Mid Cap" if mcap_an and mcap_an>500 else "Small Cap" if mcap_an else "N/A",
                 "#38bdf8"),
                (fv4,"Beta",        f"{beta_an:.2f}" if beta_an else "N/A",
                 "Low risk" if beta_an and beta_an<0.8 else "Moderate" if beta_an and beta_an<1.3 else "High risk" if beta_an else "N/A",
                 "#26a69a" if beta_an and beta_an<0.8 else "#f59e0b" if beta_an and beta_an<1.3 else "#ef5350"),
                (fv5,"ROE",         f"{roe_an*100:.1f}%" if roe_an else "N/A",
                 "Excellent" if roe_an and roe_an>0.20 else "Good" if roe_an and roe_an>0.12 else "Weak" if roe_an else "N/A",
                 "#26a69a" if roe_an and roe_an>0.18 else "#f59e0b" if roe_an and roe_an>0.10 else "#ef5350"),
            ]
            for (vcol,vlbl,vval,vstatus,vcol_) in val_cards:
                vcol.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px 10px;border-bottom:3px solid {vcol_}'>"
                    f"<div class='tv-label'>{vlbl}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;font-size:1.05rem;color:{vcol_};margin:5px 0'>{vval}</div>"
                    f"<div style='font-size:.68rem;color:#787b86'>{vstatus}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # ── F2: 52-Week Price Range ───────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#a855f7;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin:14px 0 10px'>📅 52-Week Price Range</div>", unsafe_allow_html=True)

            if w52h_an and w52l_an and c_:
                _rng   = w52h_an - w52l_an
                _pos   = (c_ - w52l_an) / _rng * 100 if _rng > 0 else 50
                _pct_from_low  = (c_/w52l_an - 1)*100
                _pct_from_high = (c_/w52h_an - 1)*100
                pr1,pr2,pr3 = st.columns([1,2,1])
                pr1.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px 14px;text-align:center'>"
                    f"<div class='tv-label'>52W Low</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;color:#26a69a'>&#8377;{w52l_an:,.2f}</div>"
                    f"<div style='font-size:.68rem;color:#26a69a'>{_pct_from_low:+.1f}% from here</div></div>",
                    unsafe_allow_html=True,
                )
                _pos_col = "#26a69a" if _pos < 40 else "#f59e0b" if _pos < 70 else "#ef5350"
                pr2.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:14px 16px'>"
                    f"<div style='display:flex;justify-content:space-between;margin-bottom:6px'>"
                    f"<span style='font-size:.7rem;color:#787b86'>Current: <b style='color:#38bdf8'>&#8377;{c_:,.2f}</b></span>"
                    f"<span style='font-size:.7rem;color:{_pos_col};font-weight:700'>{_pos:.0f}% of range</span>"
                    f"</div>"
                    f"<div style='background:#2a3347;border-radius:4px;height:10px;position:relative'>"
                    f"<div style='background:{_pos_col};border-radius:4px;height:10px;width:{_pos:.0f}%'></div>"
                    f"<div style='position:absolute;left:{_pos:.0f}%;top:-4px;transform:translateX(-50%);width:3px;height:18px;background:#fff;border-radius:2px'></div>"
                    f"</div>"
                    f"<div style='font-size:.68rem;color:#434651;margin-top:4px;text-align:center'>"
                    f"{'Near 52W Low — potential accumulation zone 🟢' if _pos<25 else '52W High zone — momentum play ⚠️' if _pos>75 else 'Mid-range — balanced risk/reward'}"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )
                pr3.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px 14px;text-align:center'>"
                    f"<div class='tv-label'>52W High</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;color:#ef5350'>&#8377;{w52h_an:,.2f}</div>"
                    f"<div style='font-size:.68rem;color:#ef5350'>{_pct_from_high:+.1f}% from here</div></div>",
                    unsafe_allow_html=True,
                )
            else:
                st.info("52-week range data not available — enable fundamentals fetch.")

            # ── F3: Traded Volume & Liquidity ─────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#a855f7;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin:14px 0 10px'>💧 Liquidity & Traded Data</div>", unsafe_allow_html=True)

            liq1,liq2,liq3,liq4 = st.columns(4)
            liq_data = [
                (liq1,"Avg Daily Volume",  f"{r_an['avg_vol']/1e5:.1f}L shares",
                 "✅ Liquid" if r_an["avg_vol"]>=1_500_000 else "⚠️ Low liquidity","#26a69a"),
                (liq2,"Median Traded Value",f"₹{r_an['traded_val_cr']:.2f} Cr/day",
                 "✅ High liquidity" if r_an["traded_val_cr"]>=5 else "⚠️ Limited","#26a69a"),
                (liq3,"Vol Z-Score",       f"{r_an.get('vol_z',0):.2f}σ",
                 "Unusual activity 🔊" if abs(r_an.get('vol_z',0))>2 else "Normal range","#f59e0b"),
                (liq4,"F&O Eligible",      "YES ✅" if r_an["is_fo"] else "NO",
                 "Can hedge with options" if r_an["is_fo"] else "Cash segment only",
                 "#38bdf8" if r_an["is_fo"] else "#787b86"),
            ]
            for (lc,ll,lv,ls,lcl) in liq_data:
                lc.markdown(
                    f"<div style='background:#131722;border-radius:4px;padding:10px 10px;border-left:3px solid {lcl}'>"
                    f"<div class='tv-label'>{ll}</div>"
                    f"<div style='font-family:Syne,sans-serif;font-weight:700;color:{lcl};margin:5px 0'>{lv}</div>"
                    f"<div style='font-size:.68rem;color:#787b86'>{ls}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # ── F4: Research Note ─────────────────────────────────────────
            st.markdown("<div style='font-size:.75rem;font-weight:700;color:#a855f7;letter-spacing:.08em;text-transform:uppercase;padding:6px 0;border-bottom:1px solid #1c2030;margin:14px 0 10px'>📝 Integrated Research Note</div>", unsafe_allow_html=True)
            st.markdown(
                f"<div class='tv-card' style='border-left:3px solid #a855f7;font-size:.83rem;color:#787b86;line-height:1.8'>"
                f"<div style='font-size:.72rem;color:#a855f7;font-weight:600;margin-bottom:8px'>"
                f"SECTOR: {r_an.get('sector','N/A')}  ·  INDUSTRY: {r_an.get('industry','N/A')}  ·  INDICES: {r_an['indices']}</div>"
                f"{r_an['reason'].replace('  •  ', '<br><span style=\"color:#a855f7\">→</span> ')}"
                f"</div>",
                unsafe_allow_html=True,
            )

        # ══════════════════════════════════════════════════════════════════
        # VIEW 2 — DEEP ANALYSIS
        # ══════════════════════════════════════════════════════════════════
        elif an_view == "🔬 Deep Analysis":
            st.markdown("<div class='tv-section'>🧮 8-Factor Score Decomposition</div>", unsafe_allow_html=True)
            af1, af2 = st.columns([1.5, 1])
            with af1:
                st.plotly_chart(factor_bar_fig(r_an["ai"]), use_container_width=True,
                                config={"displayModeBar": False}, key=_uid("pc"))
            with af2:
                st.plotly_chart(radar_fig(r_an["ai"]), use_container_width=True,
                                config={"displayModeBar": False}, key=_uid("pc"))

            st.markdown("<div class='tv-section'>📊 All Signals — Multi-Stock Comparison</div>", unsafe_allow_html=True)
            st.plotly_chart(multi_score_fig(alerts), use_container_width=True,
                            config={"displayModeBar": False}, key=_uid("pc"))

            an2c1, an2c2 = st.columns(2)
            with an2c1:
                st.markdown("<div class='tv-section'>🎯 RSI vs ADX — Bubble Size = AI Score</div>", unsafe_allow_html=True)
                st.plotly_chart(rsi_vs_adx_fig(alerts), use_container_width=True,
                                config={"displayModeBar": False}, key=_uid("pc"))
            with an2c2:
                st.markdown("<div class='tv-section'>📐 Score Distribution</div>", unsafe_allow_html=True)
                st.plotly_chart(score_distribution_fig(alerts), use_container_width=True,
                                config={"displayModeBar": False}, key=_uid("pc"))

            st.markdown("<div class='tv-section'>📋 Factor Detail Table</div>", unsafe_allow_html=True)
            W_deep = {"trend_s":0.24,"mom_s":0.16,"brk_s":0.17,"vol_s":0.10,
                      "pat_s":0.10,"fund_s":0.08,"sent_s":0.04}
            factor_labels = {
                "trend_s":"📈 Trend","mom_s":"⚡ Momentum","brk_s":"🚀 Breakout",
                "vol_s":"🔊 Volume","pat_s":"🎯 Pattern","fund_s":"🏦 Fundamental",
                "sent_s":"📡 Sentiment",
            }
            factor_df_rows = []
            for key, label in factor_labels.items():
                sc_f = float(ai_an.get(key, 0))
                wt_f = W_deep.get(key, 0)
                direction = "▲▲ Strong Bull" if sc_f > 0.5 else "▲ Bullish" if sc_f > 0.2 else \
                            "◆ Neutral" if sc_f > -0.2 else "▽ Bearish"
                factor_df_rows.append({
                    "Factor": label, "Score": round(sc_f, 3),
                    "Weight": f"{wt_f:.0%}",
                    "Contribution": round(sc_f * wt_f, 4),
                    "Direction": direction,
                })
            st.dataframe(
                pd.DataFrame(factor_df_rows),
                use_container_width=True, hide_index=True,
                column_config={
                    "Score": st.column_config.NumberColumn("Score", format="%.3f"),
                    "Contribution": st.column_config.NumberColumn("Contribution", format="%.4f"),
                }
            )

        # ══════════════════════════════════════════════════════════════════
        # VIEW 3 — TRADINGVIEW TA
        # ══════════════════════════════════════════════════════════════════
        else:
            st.markdown("<div class='tv-section'>📺 TradingView Technical Analysis — Live Data</div>", unsafe_allow_html=True)
            st.info(f"📊 Showing live TradingView analysis for **NSE:{an_sym}** — switch stock above to update", icon="ℹ️")

            tv_interval = st.select_slider(
                "TradingView Interval",
                options=["1", "5", "15", "60", "240", "1D", "1W", "1M"],
                value="1D", key="tv_interval_sel"
            )

            tv_ta_html = f"""
            <!-- TradingView Widget BEGIN nonce={an_sym}_{tv_interval} -->
            <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
              <div class="tradingview-widget-container__widget"></div>
              <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-technical-analysis.js" async>
              {{"interval":"{tv_interval}","width":"100%","isTransparent":true,"height":"500",
               "symbol":"NSE:{an_sym}","showIntervalTabs":true,"displayMode":"multiple",
               "locale":"en","colorTheme":"dark"}}
              </script>
            </div>"""
            components.html(tv_ta_html, height=520, scrolling=False)

            st.markdown("<div class='tv-section' style='margin-top:16px'>📊 Symbol Info + Financials</div>", unsafe_allow_html=True)
            sf1, sf2 = st.columns([1, 1.5])
            with sf1:
                components.html(
                    f"""<!-- nonce={an_sym} -->
                    <div class="tradingview-widget-container">
                      <div class="tradingview-widget-container__widget"></div>
                      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-symbol-info.js" async>
                      {{"symbol":"NSE:{an_sym}","width":"100%","locale":"en","colorTheme":"dark","isTransparent":true}}
                      </script>
                    </div>""",
                    height=180, scrolling=False
                )
                pe_v=r_an.get("pe"); roe_v=r_an.get("roe"); mcap_v=r_an.get("mcap")
                beta_v=r_an.get("beta"); w52h_v=r_an.get("w52h"); w52l_v=r_an.get("w52l")
                for lbl_f,col_f,val_f in [
                    ("P/E","#26a69a" if pe_v and pe_v<20 else "#f59e0b",f"{pe_v:.1f}" if pe_v else "N/A"),
                    ("ROE","#26a69a" if roe_v and roe_v>0.18 else "#f59e0b",f"{roe_v*100:.1f}%" if roe_v else "N/A"),
                    ("Mkt Cap","#d1d4dc",fmt_cr(mcap_v)),
                    ("Beta","#d1d4dc",f"{beta_v:.2f}" if beta_v else "N/A"),
                    ("52W High","#26a69a",fmt_inr(w52h_v)),
                    ("52W Low","#f59e0b",fmt_inr(w52l_v)),
                    ("Traded Val","#38bdf8",f"₹{r_an['traded_val_cr']:.2f} Cr/d"),
                ]:
                    st.markdown(
                        f"<div style='display:flex;justify-content:space-between;padding:5px 10px;"
                        f"margin-bottom:4px;background:#1c2030;border-radius:3px'>"
                        f"<span style='font-size:.75rem;color:#787b86'>{lbl_f}</span>"
                        f"<span style='font-size:.8rem;color:{col_f};font-weight:600'>{val_f}</span>"
                        f"</div>", unsafe_allow_html=True,
                    )
            with sf2:
                components.html(
                    f"""<!-- nonce={an_sym}_fin -->
                    <div class="tradingview-widget-container" style="border:1px solid #2a3347;border-radius:4px;overflow:hidden;">
                      <div class="tradingview-widget-container__widget"></div>
                      <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-financials.js" async>
                      {{"isTransparent":true,"displayMode":"regular","width":"100%","height":"600",
                       "colorTheme":"dark","symbol":"NSE:{an_sym}","locale":"en"}}
                      </script>
                    </div>""",
                    height=620, scrolling=False
                )


    # TAB 7 — BACKTEST
    # ══════════════════════════════════════════════════════════════════════
    with tabs[7]:
        st.markdown("<div class='tv-section'>📈 Backtest Performance Summary</div>", unsafe_allow_html=True)
        bt1,bt2,bt3,bt4,bt5 = st.columns(5)
        bt1.metric("Total Return", f"{bt_ret:+.2%}")
        bt2.metric("Sharpe Ratio", f"{bt_sh:.3f}")
        bt3.metric("Max Drawdown", f"{abs(bt_dd):.2%}")
        bt4.metric("Win Rate",     f"{bt_wr:.1%}")
        bt5.metric("Total Trades", f"{bt_tr}")

        bte_c1, bte_c2 = st.columns([2,1])
        with bte_c1:
            st.markdown("<div class='tv-section'>📊 Cumulative P&L Curve</div>", unsafe_allow_html=True)
            ef=equity_curve_fig(bt)
            if ef.data: st.plotly_chart(ef, use_container_width=True, config={"displayModeBar":False}, key=_uid("pc"))
            else: st.info("No closed trades yet.")
        with bte_c2:
            st.markdown("<div class='tv-section'>💸 Trade P&L (Last 20)</div>", unsafe_allow_html=True)
            st.plotly_chart(waterfall_fig(bt), use_container_width=True, config={"displayModeBar":False}, key=_uid("pc"))

        trd_df=bt.get("trades_df",pd.DataFrame())
        if not trd_df.empty:
            st.markdown("<div class='tv-section'>📋 Trade Log</div>", unsafe_allow_html=True)
            disp=trd_df.copy()
            disp["pnl_fmt"]=disp["pnl"].apply(lambda v:f"₹{v:+,.2f}")
            disp["ret_fmt"]=disp["ret"].apply(lambda v:f"{v:+.2%}")
            avail=[c for c in ["sym","entry","exit","ep","xp","bars","reason","pnl_fmt","ret_fmt"] if c in disp.columns]
            rename={"sym":"Symbol","entry":"Entry","exit":"Exit","ep":"Entry ₹","xp":"Exit ₹",
                    "bars":"Bars","reason":"Reason","pnl_fmt":"P&L","ret_fmt":"Return"}
            st.dataframe(disp[avail].rename(columns=rename),
                         use_container_width=True, height=300, hide_index=True)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 8 — SCREENER
    # ══════════════════════════════════════════════════════════════════════
    with tabs[8]:
        sc_c1, sc_c2, sc_c3 = st.columns([1,1,1])
        with sc_c1:
            sort_by=st.selectbox("Sort by",
                ["AI Score","Market Score","Pattern Score","Composite Score","RSI","ADX","Volume"],
                label_visibility="collapsed", key="scr_sort")
        with sc_c2:
            min_ai=st.slider("Min AI Score", 0, 100, 0, 5, key="scr_ai")
        with sc_c3:
            fo_only=st.toggle("F&O Only", value=False, key="scr_fo")

        smap={"AI Score":lambda r:-r["ai"]["ai_pct"],"Market Score":lambda r:-r["mkt"]["pct"],
              "Pattern Score":lambda r:-r["pat_conf"],"Composite Score":lambda r:-abs(r["score"]),
              "RSI":lambda r:r["rsi"],"ADX":lambda r:-r["adx"],"Volume":lambda r:-r["vol_ratio"]}
        filtered=[r for r in alerts if r["ai"]["ai_pct"]>=min_ai and (not fo_only or r["is_fo"])]
        sorted_a=sorted(filtered, key=smap.get(sort_by,smap["AI Score"]))

        rows=[]
        for r in sorted_a:
            ltr,_=_d_grade(r["ai"]["ai_pct"])
            stl2=r["levels"]["short_term"]; ltl2=r["levels"]["long_term"]
            rows.append({"Symbol":r["symbol"],"Grade":ltr,"AI%":round(r["ai"]["ai_pct"],1),
                "Mkt%":round(r["mkt"]["pct"],1),"Pat%":round(r["pat_conf"]*100,1),
                "Score":round(r["score"],4),"RSI":round(r["rsi"],1),"ADX":round(r["adx"],1),
                "Vol×":round(r["vol_ratio"],2),"ATR%":round(r["atr_pct"],2),
                "F&O":"✅" if r["is_fo"] else "—","Price ₹":r["last_close"],
                "ST Entry":stl2["entry"],"ST Target":stl2["tp"],"ST SL":stl2["sl"],
                "ST R:R":stl2["rr_str"],"LT Target":ltl2["tp"],"LT R:R":ltl2["rr_str"],
                "Top Signal":r["hits"][0][1] if r["hits"] else "—","Sector":r.get("sector","N/A"),
                "Indices":r["indices"]})

        df_sc=pd.DataFrame(rows)
        st.markdown(f"<div class='tv-label' style='margin-bottom:8px'>{len(df_sc)} signals shown</div>",
                    unsafe_allow_html=True)
        st.dataframe(df_sc, use_container_width=True, height=480, hide_index=True,
            column_config={
                "AI%":    st.column_config.ProgressColumn("AI%",   min_value=0,max_value=100,format="%.1f%%"),
                "Mkt%":   st.column_config.ProgressColumn("Mkt%",  min_value=0,max_value=100,format="%.1f%%"),
                "Pat%":   st.column_config.ProgressColumn("Pat%",  min_value=0,max_value=100,format="%.1f%%"),
                "Price ₹":st.column_config.NumberColumn("Price ₹", format="₹%.2f"),
                "ST Entry":st.column_config.NumberColumn("ST Entry",format="₹%.2f"),
                "ST Target":st.column_config.NumberColumn("ST Target",format="₹%.2f"),
                "ST SL":  st.column_config.NumberColumn("ST SL",   format="₹%.2f"),
                "LT Target":st.column_config.NumberColumn("LT Target",format="₹%.2f"),
                "Score":  st.column_config.NumberColumn("Score",    format="%.4f"),
            })

        st.markdown("<div class='tv-section' style='margin-top:8px'>🌐 TradingView NSE Screener</div>",
                    unsafe_allow_html=True)
        components.html(tv_screener(), height=620, scrolling=False)

        csv=df_sc.to_csv(index=False).encode()
        st.download_button("⬇️  Download CSV", data=csv,
            file_name=f"nse_signals_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv")

    # ══════════════════════════════════════════════════════════════════════
    # TAB 9 — NEWS & CALENDAR
    # ══════════════════════════════════════════════════════════════════════
    with tabs[9]:
        nc_sym=st.selectbox("Select Stock for News",
            [r["symbol"] for r in alerts], key="news_sym")
        nn1, nn2 = st.columns([1.2, 1])
        with nn1:
            st.markdown("<div class='tv-section'>📰 Latest News</div>", unsafe_allow_html=True)
            components.html(tv_news(nc_sym), height=520, scrolling=False)
        with nn2:
            st.markdown("<div class='tv-section'>📅 India Economic Calendar</div>", unsafe_allow_html=True)
            components.html(tv_economic_calendar(), height=520, scrolling=False)

    # ── Footer ─────────────────────────────────────────────────────────────
    st.markdown("""
    <hr class='tv-divider' style='margin-top:30px'>
    <div style='text-align:center;font-size:.65rem;color:#434651;padding:10px 0;letter-spacing:.05em'>
      ⚠️  Research &amp; Educational Use Only · Not Financial Advice ·
      Consult a SEBI-registered advisor before investing.
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# §22  STREAMLIT SIDEBAR + RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def _streamlit_sidebar() -> tuple:
    with st.sidebar:
        st.markdown("""
        <div style='text-align:center;padding:16px 0 8px'>
          <div style='font-family:Syne,sans-serif;font-weight:800;font-size:1.3rem;color:#38bdf8;letter-spacing:.1em'>📈 NSE PRO</div>
          <div style='font-size:.65rem;color:#434651;letter-spacing:.1em'>SWING TRADER v10</div>
        </div>""", unsafe_allow_html=True)
        st.divider()

        st.markdown("<p style='font-size:.65rem;letter-spacing:.1em;text-transform:uppercase;color:#787b86;margin-top:10px'>Universe</p>", unsafe_allow_html=True)
        group_sel=st.selectbox("Index Group",
            ["All Groups","NIFTY 50","NIFTY BANK","NIFTY IT","NIFTY ENERGY",
             "NIFTY AUTO","NIFTY INFRA","FO STOCKS","NIFTY NEXT 50","NIFTY MIDCAP 100"],
            label_visibility="collapsed")
        custom_syms=st.text_input("Or enter symbols",placeholder="TCS,INFY,HDFCBANK",
                                   label_visibility="collapsed")
        st.markdown("<p style='font-size:.65rem;letter-spacing:.1em;text-transform:uppercase;color:#787b86;margin-top:10px'>Signal</p>", unsafe_allow_html=True)
        threshold=st.slider("Score Threshold",0.10,0.40,0.22,0.01)
        min_rr=st.slider("Min Risk:Reward",1.0,3.0,1.5,0.1)
        st.markdown("<p style='font-size:.65rem;letter-spacing:.1em;text-transform:uppercase;color:#787b86;margin-top:10px'>Volume</p>", unsafe_allow_html=True)
        min_vol=st.select_slider("Min Daily Volume",
            options=[500_000,750_000,1_000_000,1_500_000,2_000_000,3_000_000],
            value=1_500_000,format_func=lambda x:f"{x/1e5:.0f}L shares",
            label_visibility="collapsed")
        min_tv2=st.slider("Min Traded Value (₹Cr/d)",1.0,20.0,5.0,0.5)
        period=st.select_slider("Lookback Period",["3mo","4mo","6mo","8mo","1y"],value="8mo",
                                 label_visibility="collapsed")
        capital_l=st.number_input("Portfolio ₹ (Lakhs)",min_value=1.0,max_value=1000.0,
                                   value=10.0,step=1.0,label_visibility="collapsed")
        capital=capital_l*1e5
        st.session_state["capital_val"]=capital
        top_n=st.slider("Top N Alerts",3,20,10)
        st.divider()
        run_btn=st.button("🚀  Run Scan", use_container_width=True)
        st.markdown("<div style='font-size:.62rem;text-align:center;color:#434651;margin-top:6px'>Results cached 1 hour</div>",
                    unsafe_allow_html=True)

    group_key="" if group_sel=="All Groups" else group_sel
    syms_key=custom_syms.strip()

    cfg=Cfg()
    cfg.use_sample=False; cfg.live_period=period; cfg.top_n=top_n
    cfg.min_avg_vol=min_vol; cfg.min_traded_val_cr=min_tv2; cfg.min_rr=min_rr
    cfg.base_threshold=threshold; cfg.bear_threshold=threshold+0.08
    cfg.capital=capital; cfg.output_dir=Path("nse_v10_output")

    if syms_key:
        cfg.symbols=[s.strip().upper() for s in syms_key.split(",") if s.strip()]
    elif group_key:
        gk=group_key.strip().upper()
        matched=[sl for grp,sl in _UNIVERSE.items() if gk in grp.upper()]
        if matched:
            cfg.symbols=sorted({s for sub in matched for s in sub}-_SKIP_SYMBOLS)

    return cfg, run_btn


def _run_scan_cached(cfg: Cfg):
    import glob
    buf=io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        alerts_out, bt_out = run(cfg)
    nifty_out=nifty50_state()
    feat_df_out=pd.DataFrame()
    try:
        cfg.output_dir.mkdir(parents=True,exist_ok=True)
        csvs=sorted(glob.glob(str(cfg.output_dir/"alerts_*.csv")))
        if csvs: feat_df_out=pd.read_csv(csvs[-1])
    except Exception:
        pass
    return alerts_out, bt_out, nifty_out, feat_df_out


def _streamlit_main():
    cfg, run_btn = _streamlit_sidebar()
    cache_key=(str(sorted(cfg.symbols)),cfg.base_threshold,cfg.min_avg_vol,
               cfg.min_traded_val_cr,cfg.min_rr,cfg.live_period,cfg.top_n)

    if run_btn or "scan_data" not in st.session_state or st.session_state.get("scan_key")!=cache_key:
        with st.spinner("🔴  Running live scan..."):
            alerts,bt,nifty,feat_df=_run_scan_cached(cfg)
        st.session_state["scan_data"]=(alerts,bt,nifty,feat_df)
        st.session_state["scan_key"]=cache_key
    else:
        alerts,bt,nifty,feat_df=st.session_state["scan_data"]

    if alerts is None:
        st.error("Scan failed — check yfinance is installed."); return

    run_dashboard(alerts,bt,nifty,feat_df)


# ══════════════════════════════════════════════════════════════════════════════
# §23  TERMINAL CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p=argparse.ArgumentParser(
        description="NSE Swing Trader v10.0 — Unified (terminal + Streamlit)",
        formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--symbols",   type=str,   default="")
    p.add_argument("--group",     type=str,   default="")
    p.add_argument("--top-n",     type=int,   default=10)
    p.add_argument("--sample",    action="store_true")
    p.add_argument("--prices-csv",type=Path,  default=Path("data/prices.csv"))
    p.add_argument("--output-dir",type=Path,  default=Path("nse_v10_output"))
    p.add_argument("--period",    type=str,   default="8mo")
    p.add_argument("--min-vol",   type=int,   default=1_500_000)
    p.add_argument("--min-tv",    type=float, default=5.0)
    p.add_argument("--min-rr",    type=float, default=1.5)
    p.add_argument("--threshold", type=float, default=0.22)
    p.add_argument("--no-fund",   action="store_true")
    p.add_argument("--capital",   type=float, default=1_000_000)
    a=p.parse_args()
    cfg=Cfg(); cfg.use_sample=a.sample; cfg.output_dir=a.output_dir
    cfg.prices_csv=a.prices_csv; cfg.live_period=a.period; cfg.top_n=a.top_n
    cfg.min_avg_vol=a.min_vol; cfg.min_traded_val_cr=a.min_tv; cfg.min_rr=a.min_rr
    cfg.base_threshold=a.threshold; cfg.fetch_fundamentals=not a.no_fund; cfg.capital=a.capital
    if a.symbols:
        cfg.symbols=[s.strip().upper() for s in a.symbols.split(",") if s.strip()]
    elif a.group:
        gk=a.group.strip().upper()
        matched=[sl for grp,sl in _UNIVERSE.items() if gk in grp.upper()]
        if matched:
            cfg.symbols=sorted({s for sub in matched for s in sub}-_SKIP_SYMBOLS)
        else:
            print(f"Group '{a.group}' not found."); sys.exit(1)
    run(cfg)


# ══════════════════════════════════════════════════════════════════════════════
# §24  ENTRY POINT DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

if _STREAMLIT:
    _streamlit_main()
elif __name__ == "__main__":
    main()
