import time
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pandas as pd
import akshare as ak
import requests

from ..db import get_db_connection
from ..config import Config

def normalize_asset_code(code: str) -> str:
    """
    Normalize user input to canonical symbol.
    HK examples:
      HK3441 -> 03441
      3441.HK -> 03441
      hk03441 -> 03441
    """
    raw = str(code or "").strip()
    if not raw:
        return ""

    upper = raw.upper()
    m = re.match(r"^HK\s*0*(\d{4,5})$", upper)
    if m:
        return m.group(1).zfill(5)

    m = re.match(r"^0*(\d{4,5})\.HK$", upper)
    if m:
        return m.group(1).zfill(5)

    return raw


def is_hk_code(code: str) -> bool:
    c = normalize_asset_code(code)
    return c.isdigit() and len(c) == 5


def _get_sina_hk_quote(code: str) -> Dict[str, Any]:
    """Free HK quote source from Sina."""
    norm = normalize_asset_code(code)
    if not is_hk_code(norm):
        return {}
    url = f"http://hq.sinajs.cn/list=hk{norm}"
    headers = {"Referer": "http://finance.sina.com.cn"}
    try:
        response = requests.get(url, headers=headers, timeout=8)
        if response.status_code != 200:
            return {}
        text = response.text
        match = re.search(r'="(.*)"', text)
        if not match or not match.group(1):
            return {}
        parts = match.group(1).split(",")
        if len(parts) < 9:
            return {}
        en_name = str(parts[0] or "").strip()
        cn_name = str(parts[1] or "").strip()
        prev_close = float(parts[3] or 0.0)
        last = float(parts[6] or 0.0)
        change_pct = float(parts[8] or 0.0)
        date_s = str(parts[17] or "").strip() if len(parts) > 17 else ""
        time_s = str(parts[18] or "").strip() if len(parts) > 18 else ""
        return {
            "name": cn_name or en_name or norm,
            "estimate": last if last > 0 else prev_close,
            "nav": last if last > 0 else prev_close,
            "estRate": change_pct,
            "time": f"{date_s} {time_s}".strip(),
        }
    except Exception:
        return {}


def _get_ak_hk_history(code: str, limit: int) -> List[Dict[str, Any]]:
    """Free HK daily history source via AkShare."""
    norm = normalize_asset_code(code)
    if not is_hk_code(norm):
        return []
    try:
        df = ak.stock_hk_daily(symbol=norm)
        if df is None or df.empty:
            return []
        if "date" not in df.columns or "close" not in df.columns:
            return []
        df = df.copy().sort_values(by="date", ascending=True)
        if limit < 9999:
            df = df.tail(limit)
        out = []
        for _, row in df.iterrows():
            d = row.get("date")
            close = float(row.get("close") or 0.0)
            if close <= 0:
                continue
            date_str = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
            out.append({"date": date_str, "nav": close})
        return out
    except Exception:
        return []


def _get_ak_hk_latest(code: str) -> Dict[str, Any]:
    hist = _get_ak_hk_history(code, limit=5)
    if not hist:
        return {}
    last = float(hist[-1]["nav"])
    prev = float(hist[-2]["nav"]) if len(hist) >= 2 else last
    est_rate = ((last - prev) / prev * 100.0) if prev > 0 else 0.0
    return {
        "name": normalize_asset_code(code),
        "estimate": last,
        "nav": last,
        "estRate": round(est_rate, 4),
        "time": hist[-1]["date"],
    }


def _get_yahoo_hk_history(code: str, limit: int) -> List[Dict[str, Any]]:
    """Fallback HK history via Yahoo Finance chart endpoint."""
    norm = normalize_asset_code(code)
    if not is_hk_code(norm):
        return []
    symbol = f"{int(norm)}.HK"
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?range=10y&interval=1d"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return []
        body = resp.json()
        result = (((body or {}).get("chart") or {}).get("result") or [None])[0]
        if not result:
            return []
        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
        closes = quote.get("close") or []
        out = []
        for ts, close in zip(timestamps, closes):
            if close is None:
                continue
            d = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
            out.append({"date": d, "nav": float(close)})
        out = sorted(out, key=lambda x: x["date"])
        if limit < 9999:
            out = out[-limit:]
        return out
    except Exception:
        return []


def _get_yahoo_hk_latest(code: str) -> Dict[str, Any]:
    hist = _get_yahoo_hk_history(code, limit=5)
    if not hist:
        return {}
    last = float(hist[-1]["nav"])
    prev = float(hist[-2]["nav"]) if len(hist) >= 2 else last
    est_rate = ((last - prev) / prev * 100.0) if prev > 0 else 0.0
    return {
        "name": normalize_asset_code(code),
        "estimate": last,
        "nav": last,
        "estRate": round(est_rate, 4),
        "time": hist[-1]["date"],
    }


def _get_hk_name(code: str) -> str:
    data = _get_sina_hk_quote(code)
    return str(data.get("name") or "")


def get_fund_type(code: str, name: str) -> str:
    """
    Get fund type from database official_type field.
    Fallback to name-based heuristics if official_type is empty.

    Args:
        code: Fund code
        name: Fund name

    Returns:
        Fund type string
    """
    norm_code = normalize_asset_code(code)
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT type FROM funds WHERE code = ?", (norm_code,))
        row = cursor.fetchone()

        if row and row["type"]:
            return row["type"]
    except Exception as e:
        print(f"DB query error for {code}: {e}")
    finally:
        if conn:
            conn.close()

    # Fallback: simple heuristics based on name
    if "债" in name or "纯债" in name or "固收" in name:
        return "债券"
    if "QDII" in name or "纳斯达克" in name or "标普" in name or "恒生" in name:
        return "QDII"
    if "货币" in name:
        return "货币"

    return "未知"


def get_eastmoney_valuation(code: str) -> Dict[str, Any]:
    """
    Fetch real-time valuation from Tiantian Jijin (Eastmoney) API.
    """
    url = f"http://fundgz.1234567.com.cn/js/{code}.js?rt={int(time.time()*1000)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36)"
    }
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            text = response.text
            # Regex to capture JSON content inside jsonpgz(...)
            # Allow optional semicolon at end
            match = re.search(r"jsonpgz\((.*)\)", text)
            if match and match.group(1):
                data = json.loads(match.group(1))
                return {
                    "name": data.get("name"),
                    "nav": float(data.get("dwjz", 0.0)),
                    "estimate": float(data.get("gsz", 0.0)),
                    "estRate": float(data.get("gszzl", 0.0)),
                    "time": data.get("gztime")
                }
    except Exception as e:
        print(f"Eastmoney API error for {code}: {e}")
    return {}


def get_sina_valuation(code: str) -> Dict[str, Any]:
    """
    Backup source: Sina Fund API.
    Format: Name, Time, Estimate, NAV, ..., Rate, Date
    """
    url = f"http://hq.sinajs.cn/list=fu_{code}"
    headers = {"Referer": "http://finance.sina.com.cn"}
    try:
        response = requests.get(url, headers=headers, timeout=5)
        text = response.text
        # var hq_str_fu_005827="Name,15:00:00,1.234,1.230,...";
        match = re.search(r'="(.*)"', text)
        if match and match.group(1):
            parts = match.group(1).split(',')
            if len(parts) >= 8:
                return {
                    # parts[0] is name (GBK), often garbled in utf-8 env, ignore it
                    "estimate": float(parts[2]),
                    "nav": float(parts[3]),
                    "estRate": float(parts[6]),
                    "time": f"{parts[7]} {parts[1]}"
                }
    except Exception as e:
        print(f"Sina Valuation API error for {code}: {e}")
    return {}


def get_combined_valuation(code: str) -> Dict[str, Any]:
    """
    Try Eastmoney first, fallback to Sina.
    """
    norm_code = normalize_asset_code(code)
    if is_hk_code(norm_code):
        # HK: free sources first
        data = _get_sina_hk_quote(norm_code)
        if data and float(data.get("estimate", 0.0) or 0.0) > 0:
            return data
        data = _get_ak_hk_latest(norm_code)
        if data and float(data.get("estimate", 0.0) or 0.0) > 0:
            return data
        return _get_yahoo_hk_latest(norm_code)

    data = get_eastmoney_valuation(norm_code)
    if not data or data.get("estimate") == 0.0:
        # Fallback to Sina
        sina_data = get_sina_valuation(norm_code)
        if sina_data:
            # Merge Sina info into Eastmoney structure
            data.update(sina_data)
    return data


def search_funds(q: str) -> List[Dict[str, Any]]:
    """
    Search funds by keyword using local SQLite DB.
    """
    if not q:
        return []

    q_clean = q.strip()
    q_norm = normalize_asset_code(q_clean)
    pattern = f"%{q_clean}%"
    prefix_pattern = f"{q_norm}%"
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """
            SELECT code, name, type,
                   CASE WHEN code = ? THEN 1 ELSE 0 END AS exact_hit,
                   CASE WHEN code LIKE ? THEN 1 ELSE 0 END AS prefix_hit
            FROM funds
            WHERE code LIKE ? OR name LIKE ?
            ORDER BY exact_hit DESC, prefix_hit DESC, code ASC
            LIMIT 20
            """,
            (q_norm, prefix_pattern, pattern, pattern),
        )
        
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                "id": str(row["code"]),
                "name": row["name"],
                "type": row["type"] or "未知"
            })
        if is_hk_code(q_norm):
            name = _get_hk_name(q_norm) or f"港股标的 {q_norm}"
            hk_item = {
                "id": q_norm,
                "name": name,
                "type": "港股"
            }
            if not any(item["id"] == q_norm for item in results):
                results.insert(0, hk_item)
        return results
    finally:
        conn.close()


def get_eastmoney_pingzhong_data(code: str) -> Dict[str, Any]:
    """
    Fetch static detailed data from Eastmoney (PingZhongData).
    """
    url = Config.EASTMONEY_DETAILED_API_URL.format(code=code)
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            text = response.text
            data = {}
            name_match = re.search(r'fS_name\s*=\s*"(.*?)";', text)
            if name_match: data["name"] = name_match.group(1)
            
            code_match = re.search(r'fS_code\s*=\s*"(.*?)";', text)
            if code_match: data["code"] = code_match.group(1)
            
            manager_match = re.search(r'Data_currentFundManager\s*=\s*(\[.+?\])\s*;\s*/\*', text)
            if manager_match:
                try:
                    managers = json.loads(manager_match.group(1))
                    if managers:
                        data["manager"] = ", ".join([m["name"] for m in managers])
                except:
                    pass

            # Extract Performance Metrics
            for key in ["syl_1n", "syl_6y", "syl_3y", "syl_1y"]:
                m = re.search(rf'{key}\s*=\s*"(.*?)";', text)
                if m: data[key] = m.group(1)

            # Extract Performance Evaluation (Capability Scores)
            # var Data_performanceEvaluation = {"avr":"72.25","categories":[...],"data":[80.0,70.0...]};
            # Match until `};`
            perf_match = re.search(r'Data_performanceEvaluation\s*=\s*(\{.+?\})\s*;\s*/\*', text)
            if perf_match:
                try:
                    perf = json.loads(perf_match.group(1))
                    if perf and "data" in perf and "categories" in perf:
                        data["performance"] = dict(zip(perf["categories"], perf["data"]))
                except:
                    pass

            # Extract Full History (Data_netWorthTrend)
            # var Data_netWorthTrend = [{"x":1536076800000,"y":1.0,...},...];
            history_match = re.search(r'Data_netWorthTrend\s*=\s*(\[.+?\])\s*;\s*/\*', text)
            if history_match:
                try:
                    raw_hist = json.loads(history_match.group(1))
                    # Convert to standard format: [{"date": "YYYY-MM-DD", "nav": 1.23}, ...]
                    # x is ms timestamp
                    data["history"] = [
                        {
                            "date": time.strftime('%Y-%m-%d', time.localtime(item['x']/1000)),
                            "nav": float(item['y'])
                        }
                        for item in raw_hist
                    ]
                except:
                    pass

            return data
    except Exception as e:
        print(f"PingZhong API error for {code}: {e}")
    return {}


def _get_fund_info_from_db(code: str) -> Dict[str, Any]:
    """
    Get fund basic info from local SQLite cache.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        norm_code = normalize_asset_code(code)
        cursor.execute("SELECT name, type FROM funds WHERE code = ?", (norm_code,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {"name": row["name"], "type": row["type"]}
    except Exception as e:
        print(f"DB fetch error for {code}: {e}")
    return {}


def _fetch_stock_spots_sina(codes: List[str]) -> Dict[str, float]:
    """
    Fetch real-time stock prices from Sina API in batch.
    Supports A-share (sh/sz), HK (hk), US (gb_).
    """
    if not codes:
        return {}
    
    formatted = []
    # Map cleaned code back to original for result dict
    code_map = {} 
    
    for c in codes:
        if not c: continue
        c_str = str(c).strip()
        prefix = ""
        clean_c = c_str
        
        # Detect Market
        if c_str.isdigit():
            if len(c_str) == 6:
                # A-share
                prefix = "sh" if c_str.startswith(('60', '68', '90', '11')) else "sz"
            elif len(c_str) == 5:
                # HK
                prefix = "hk"
        elif c_str.isalpha():
            # US
            prefix = "gb_"
            clean_c = c_str.lower()
        
        if prefix:
            sina_code = f"{prefix}{clean_c}"
            formatted.append(sina_code)
            code_map[sina_code] = c_str
            
    if not formatted:
        return {}

    url = f"http://hq.sinajs.cn/list={','.join(formatted)}"
    headers = {"Referer": "http://finance.sina.com.cn"}
    
    try:
        response = requests.get(url, headers=headers, timeout=5)
        results = {}
        for line in response.text.strip().split('\n'):
            if not line or '=' not in line or '"' not in line: continue
            
            # var hq_str_sh600519="..."
            line_key = line.split('=')[0].split('_str_')[-1] # sh600519 or hk00700 or gb_nvda
            original_code = code_map.get(line_key)
            if not original_code: continue

            data_part = line.split('"')[1]
            if not data_part: continue
            parts = data_part.split(',')
            
            change = 0.0
            try:
                if line_key.startswith("gb_"):
                    # US: name, price, change_percent, ...
                    # Example: "英伟达,135.20,2.55,..."
                    if len(parts) > 2:
                        change = float(parts[2])
                elif line_key.startswith("hk"):
                    # HK: en, ch, open, prev_close, high, low, last, ...
                    if len(parts) > 6:
                        prev_close = float(parts[3])
                        last = float(parts[6])
                        if prev_close > 0:
                            change = round((last - prev_close) / prev_close * 100, 2)
                else:
                    # A-share: name, open, prev_close, last, ...
                    if len(parts) > 3:
                        prev_close = float(parts[2])
                        last = float(parts[3])
                        if prev_close > 0:
                            change = round((last - prev_close) / prev_close * 100, 2)
                
                results[original_code] = change
            except:
                continue
                
        return results
    except Exception as e:
        print(f"Sina fetch failed: {e}")
        return {}


def get_fund_history(code: str, limit: int = 30) -> List[Dict[str, Any]]:
    """
    Get historical NAV data with database caching.
    If limit >= 9999, fetch all available history.
    """
    from ..db import get_db_connection
    import time

    norm_code = normalize_asset_code(code)

    if is_hk_code(norm_code):
        # HK symbols use free HK data sources directly.
        hk_history = _get_ak_hk_history(norm_code, limit)
        if not hk_history:
            hk_history = _get_yahoo_hk_history(norm_code, limit)
        if not hk_history:
            return []

        conn = get_db_connection()
        cursor = conn.cursor()
        for item in hk_history:
            cursor.execute(
                """
                INSERT OR REPLACE INTO fund_history (code, date, nav, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (norm_code, item["date"], float(item["nav"])),
            )
        conn.commit()
        conn.close()
        return hk_history

    # 1. Try to get from database cache first
    conn = get_db_connection()
    cursor = conn.cursor()

    # If limit is very large, get all data
    if limit >= 9999:
        cursor.execute("""
            SELECT date, nav, updated_at FROM fund_history
            WHERE code = ?
            ORDER BY date DESC
        """, (norm_code,))
    else:
        cursor.execute("""
            SELECT date, nav, updated_at FROM fund_history
            WHERE code = ?
            ORDER BY date DESC
            LIMIT ?
        """, (norm_code, limit))

    rows = cursor.fetchall()

    # Check if cache is fresh
    cache_valid = False
    if rows:
        latest_update = rows[0]["updated_at"]
        latest_nav_date = rows[0]["date"]
        # Parse timestamp
        try:
            from datetime import datetime
            update_time = datetime.fromisoformat(latest_update)
            age_hours = (datetime.now() - update_time).total_seconds() / 3600

            # Get today's date
            today_str = datetime.now().strftime("%Y-%m-%d")
            current_hour = datetime.now().hour

            # For "all history" requests, require more data to consider cache valid
            min_rows = 10 if limit < 9999 else 100

            # Cache invalidation logic:
            # 1. If it's after 16:00 on a trading day and cache doesn't have today's NAV, invalidate
            # 2. Otherwise, use 24-hour cache
            if current_hour >= 16 and latest_nav_date < today_str:
                # After 16:00, if we don't have today's NAV, force refresh
                cache_valid = False
            else:
                # Normal 24-hour cache
                cache_valid = age_hours < 24 and len(rows) >= min(limit, min_rows)
        except:
            pass

    if cache_valid:
        conn.close()
        # Reverse to ascending order (oldest to newest) for chart display
        return [{"date": row["date"], "nav": float(row["nav"])} for row in reversed(rows)]

    # 2. Cache miss or stale, fetch from API
    try:
        df = ak.fund_open_fund_info_em(symbol=norm_code, indicator="单位净值走势")
        if df is None or df.empty:
            conn.close()
            return []

        # If limit < 9999, take only the most recent N records
        if limit < 9999:
            df = df.sort_values(by="净值日期", ascending=False).head(limit)

        # Sort ascending for chart display
        df = df.sort_values(by="净值日期", ascending=True)

        results = []
        for _, row in df.iterrows():
            d = row["净值日期"]
            date_str = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
            nav_value = float(row["单位净值"])
            results.append({"date": date_str, "nav": nav_value})

            # 3. Save to database cache
            cursor.execute("""
                INSERT OR REPLACE INTO fund_history (code, date, nav, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (norm_code, date_str, nav_value))

        conn.commit()
        conn.close()
        return results
    except Exception as e:
        print(f"History fetch error for {code}: {e}")
        conn.close()
        return []


def get_nav_on_date(code: str, date_str: str) -> float | None:
    """
    Get fund NAV on a specific date (YYYY-MM-DD). Used for T+1 confirm.
    Returns None if that date's NAV is not yet available.
    """
    history = get_fund_history(code, limit=90)
    for item in history:
        if item["date"][:10] == date_str[:10]:
            return item["nav"]
    return None


def _calculate_technical_indicators(history: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate real technical indicators from NAV history.
    """
    if not history or len(history) < 10:
        return {
            "sharpe": "--",
            "volatility": "--",
            "max_drawdown": "--",
            "annual_return": "--"
        }
    
    try:
        import numpy as np
        # Convert to numpy array of NAVs
        navs = np.array([item['nav'] for item in history])
        
        # 1. Returns (Daily)
        daily_returns = np.diff(navs) / navs[:-1]
        
        # 2. Annualized Return
        total_return = (navs[-1] - navs[0]) / navs[0]
        # Approximate years based on history length
        years = len(history) / 250.0
        annual_return = (1 + total_return)**(1/years) - 1 if years > 0 else 0
        
        # 3. Annualized Volatility
        volatility = np.std(daily_returns) * np.sqrt(250)
        
        # 4. Sharpe Ratio (Risk-free rate = 2%)
        rf = 0.02
        sharpe = (annual_return - rf) / volatility if volatility > 0 else 0
        
        # 5. Max Drawdown
        # Running max
        rolling_max = np.maximum.accumulate(navs)
        drawdowns = (navs - rolling_max) / rolling_max
        max_drawdown = np.min(drawdowns)
        
        return {
            "sharpe": round(float(sharpe), 2),
            "volatility": f"{round(float(volatility) * 100, 2)}%",
            "max_drawdown": f"{round(float(max_drawdown) * 100, 2)}%",
            "annual_return": f"{round(float(annual_return) * 100, 2)}%"
        }
    except Exception as e:
        print(f"Indicator calculation error: {e}")
        return {
            "sharpe": "--",
            "volatility": "--",
            "max_drawdown": "--",
            "annual_return": "--"
        }

def get_fund_intraday(code: str) -> Dict[str, Any]:
    """
    Get fund holdings + real-time valuation estimate.
    """
    norm_code = normalize_asset_code(code)

    # 1) Get real-time valuation (Multi-source)
    em_data = get_combined_valuation(norm_code)
    
    name = em_data.get("name")
    nav = float(em_data.get("nav", 0.0))
    estimate = float(em_data.get("estimate", 0.0))
    est_rate = float(em_data.get("estRate", 0.0))
    update_time = em_data.get("time", time.strftime("%H:%M:%S"))

    # 1.5) Enrich with detailed info
    pz_data = get_eastmoney_pingzhong_data(norm_code)
    extra_info = {}
    if pz_data.get("name"): extra_info["full_name"] = pz_data["name"]
    if pz_data.get("manager"): extra_info["manager"] = pz_data["manager"]
    for k in ["syl_1n", "syl_6y", "syl_3y", "syl_1y"]:
        if pz_data.get(k): extra_info[k] = pz_data[k]
    
    db_info = _get_fund_info_from_db(norm_code)
    if db_info:
        if not extra_info.get("full_name"): extra_info["full_name"] = db_info["name"]
        extra_info["official_type"] = db_info["type"]

    if not name:
        name = extra_info.get("full_name", _get_hk_name(norm_code) or f"基金 {norm_code}")
    manager = extra_info.get("manager", "--")

    # 2) Use history from PingZhong for Indicators
    # We take last 250 trading days (approx 1 year)
    history_data = pz_data.get("history", [])
    if history_data:
        # Indicators need 1 year
        tech_indicators = _calculate_technical_indicators(history_data[-250:])
    else:
        # Fallback to AkShare if PingZhong missed it (unlikely)
        history_data = get_fund_history(norm_code, limit=250)
        tech_indicators = _calculate_technical_indicators(history_data)

    # 3) Get holdings from AkShare
    holdings = []
    concentration_rate = 0.0
    try:
        current_year = str(time.localtime().tm_year)
        holdings_df = ak.fund_portfolio_hold_em(symbol=norm_code, date=current_year)
        if holdings_df is None or holdings_df.empty:
             prev_year = str(time.localtime().tm_year - 1)
             holdings_df = ak.fund_portfolio_hold_em(symbol=norm_code, date=prev_year)
             
        if not holdings_df.empty:
            holdings_df = holdings_df.copy()
            if "占净值比例" in holdings_df.columns:
                holdings_df["占净值比例"] = (
                    holdings_df["占净值比例"].astype(str).str.replace("%", "", regex=False)
                )
                holdings_df["占净值比例"] = pd.to_numeric(holdings_df["占净值比例"], errors="coerce").fillna(0.0)
            
            sorted_holdings = holdings_df.sort_values(by="占净值比例", ascending=False)
            top10 = sorted_holdings.head(10)
            concentration_rate = top10["占净值比例"].sum()

            stock_codes = [str(c) for c in holdings_df["股票代码"].tolist() if c]
            spot_map = _fetch_stock_spots_sina(stock_codes)
            
            seen_codes = set()
            for _, row in sorted_holdings.iterrows():
                stock_code = str(row.get("股票代码"))
                percent = float(row.get("占净值比例", 0.0))
                if stock_code in seen_codes or percent < 0.01: continue
                seen_codes.add(stock_code)
                holdings.append({
                    "name": row.get("股票名称"),
                    "percent": percent,
                    "change": spot_map.get(stock_code, 0.0), 
                })
            holdings = holdings[:20]
    except:
        pass

    # 4) Determine sector/type
    sector = get_fund_type(norm_code, name)
    
    response = {
        "id": str(norm_code),
        "name": name,
        "type": sector, 
        "manager": manager,
        "nav": nav,
        "estimate": estimate,
        "estRate": est_rate,
        "time": update_time,
        "holdings": holdings,
        "indicators": {
            "returns": {
                "1M": extra_info.get("syl_1y", "--"),
                "3M": extra_info.get("syl_3y", "--"),
                "6M": extra_info.get("syl_6y", "--"),
                "1Y": extra_info.get("syl_1n", "--")
            },
            "concentration": round(concentration_rate, 2),
            "technical": tech_indicators
        }
    }
    return response
