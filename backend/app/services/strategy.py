import json
import logging
import re
import time
import hashlib
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import akshare as ak
import numpy as np
import pandas as pd
import requests

from ..db import get_db_connection
from .fund import get_combined_valuation, get_fund_history, normalize_asset_code
from .account import upsert_position, remove_position
from ..config import Config

logger = logging.getLogger(__name__)

RISK_FREE_RATE = 0.02
TRADING_DAYS_PER_YEAR = 252
PERF_CACHE_TTL_SECONDS = 60
BACKTEST_CACHE_TTL_SECONDS = 300
BACKTEST_ENGINE_VERSION = 2

_PERF_CACHE: Dict[Tuple[int, int], Tuple[float, Dict[str, Any]]] = {}
_FUND_NAME_CACHE: Dict[str, str] = {}
_FUND_NAME_CACHE_TS: float = 0.0
_FUND_NAME_SEARCH_CACHE: Dict[str, str] = {}
_FUND_NAME_SEARCH_CACHE_TS: float = 0.0
_BACKTEST_MEM_CACHE: Dict[Tuple[int, int, str], Tuple[float, Dict[str, Any]]] = {}

def _parse_weight(value: Any) -> float:
    w = float(value)
    if w <= 0:
        raise ValueError("weight must be > 0")
    if w > 1:
        w = w / 100.0
    if w <= 0 or w > 1:
        raise ValueError("weight must be in (0, 1] or percentage")
    return w


def _normalize_holdings(holdings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not holdings:
        raise ValueError("holdings is required")

    merged: Dict[str, float] = {}
    for item in holdings:
        code = normalize_asset_code(str(item.get("code", "")).strip())
        if not code:
            raise ValueError("holding code is required")
        w = _parse_weight(item.get("weight", 0))
        merged[code] = merged.get(code, 0.0) + w

    total = sum(merged.values())
    if total <= 0:
        raise ValueError("total target weight must be > 0")

    return [{"code": code, "weight": weight / total} for code, weight in merged.items()]


def _normalize_codes(codes: Optional[List[str]]) -> List[str]:
    if not codes:
        return []
    normalized = []
    for c in codes:
        nc = normalize_asset_code(str(c).strip())
        if nc and len(nc) >= 5:
            normalized.append(nc)
    out = sorted(set(normalized))
    return out


def _parse_scope_codes(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return _normalize_codes(value)
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return _normalize_codes(parsed)
    except Exception:
        pass
    return []


def _parse_float_maybe(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    s = s.replace(",", "")
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return None


def recognize_holdings_from_image(image_data_url: str) -> Dict[str, Any]:
    api_base = str(Config.OPENAI_API_BASE or "").strip().rstrip("/")
    api_key = str(Config.OPENAI_API_KEY or "").strip()
    if not api_key:
        raise ValueError("未配置 API Key，请先在设置里配置 OPENAI_API_KEY")
    if not api_base:
        raise ValueError("未配置 API Base，请先在设置里配置 OPENAI_API_BASE")
    if not image_data_url or not str(image_data_url).startswith("data:image/"):
        raise ValueError("无效的图片数据")

    vlm_model = str(Config.OCR_MODEL_NAME or "").strip() or "Qwen/Qwen3-VL-32B-Instruct"
    prompt = (
        "你是一个持仓识别助手。请从图片中提取所有可识别的持仓条目，"
        "每个条目尽量返回: code(5-6位数字), name(可选), weight_pct(百分比数字), shares(份额数字)。"
        "输出 JSON 对象: {\"holdings\":[...],\"notes\":\"...\"}。"
        "不要输出 markdown。"
    )

    payload = {
        "model": vlm_model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": "你只输出有效 JSON。"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_data_url}},
                ],
            },
        ],
        "max_tokens": 1200,
        "response_format": {"type": "json_object"},
    }

    endpoint = f"{api_base}/chat/completions"
    try:
        resp = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=90,
        )
    except Exception as e:
        raise ValueError(f"识别请求失败: {e}")

    if resp.status_code >= 400:
        raise ValueError(f"识别接口调用失败: HTTP {resp.status_code} {resp.text[:200]}")

    try:
        body = resp.json()
        raw_content = body["choices"][0]["message"]["content"]
    except Exception:
        raise ValueError("识别接口响应格式异常")

    if not raw_content:
        raise ValueError("识别结果为空")

    text = str(raw_content).strip()
    if "```json" in text:
        text = text.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in text:
        text = text.split("```", 1)[1].split("```", 1)[0].strip()

    try:
        parsed = json.loads(text)
    except Exception:
        raise ValueError("识别结果不是有效 JSON")

    items = parsed.get("holdings") if isinstance(parsed, dict) else None
    if not isinstance(items, list):
        items = []

    cleaned: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_code = str(item.get("code", "")).strip()
        code = ""
        # HK formats: HK3441 / 3441.HK / 03441
        m_hk = re.search(r"\bHK\s*0*(\d{4,5})\b", raw_code, flags=re.IGNORECASE)
        if m_hk:
            code = normalize_asset_code(f"HK{m_hk.group(1)}")
        if not code:
            m_hk_dot = re.search(r"\b0*(\d{4,5})\.HK\b", raw_code, flags=re.IGNORECASE)
            if m_hk_dot:
                code = normalize_asset_code(f"{m_hk_dot.group(1)}.HK")
        if not code:
            m = re.search(r"\b(\d{5,6})\b", raw_code)
            if m:
                code = normalize_asset_code(m.group(1))
        if not code:
            # Try infer from name text if code field is messy.
            joined = f"{item.get('name', '')} {raw_code}"
            m_hk = re.search(r"\bHK\s*0*(\d{4,5})\b", joined, flags=re.IGNORECASE)
            if m_hk:
                code = normalize_asset_code(f"HK{m_hk.group(1)}")
        if not code:
            m = re.search(r"\b(\d{5,6})\b", f"{item.get('name', '')} {raw_code}")
            if m:
                code = normalize_asset_code(m.group(1))
        if not code:
            continue

        name = str(item.get("name", "")).strip() or _resolve_fund_name(code)
        weight_pct = _parse_float_maybe(item.get("weight_pct"))
        if weight_pct is None:
            weight_pct = _parse_float_maybe(item.get("weight"))
        shares = _parse_float_maybe(item.get("shares"))

        cleaned.append(
            {
                "code": code,
                "name": name or code,
                "weight_pct": weight_pct,
                "shares": shares,
            }
        )

    # Deduplicate by code, latest wins.
    dedup = {}
    for it in cleaned:
        dedup[it["code"]] = it
    cleaned = list(dedup.values())

    if not cleaned:
        raise ValueError("未识别到有效持仓代码，请尝试更清晰截图")

    # If no explicit weight, derive from shares; else equal weight fallback.
    valid_weight_count = len([x for x in cleaned if x.get("weight_pct") is not None and x["weight_pct"] > 0])
    if valid_weight_count == 0:
        share_sum = sum(float(x.get("shares") or 0.0) for x in cleaned if (x.get("shares") or 0.0) > 0)
        if share_sum > 0:
            for x in cleaned:
                s = float(x.get("shares") or 0.0)
                x["weight_pct"] = round((s / share_sum) * 100.0, 4) if s > 0 else None
        else:
            equal = round(100.0 / len(cleaned), 4)
            for x in cleaned:
                x["weight_pct"] = equal

    # Normalize weight total to 100 when weights exist.
    weights = [float(x["weight_pct"]) for x in cleaned if x.get("weight_pct") is not None and x["weight_pct"] > 0]
    if weights:
        total = sum(weights)
        if total > 0:
            for x in cleaned:
                w = float(x.get("weight_pct") or 0.0)
                x["weight_pct"] = round((w / total) * 100.0, 4) if w > 0 else None

    return {
        "model": vlm_model,
        "holdings": cleaned,
        "notes": parsed.get("notes", "") if isinstance(parsed, dict) else "",
    }


def _get_price_and_name(code: str) -> Tuple[Optional[float], str, float]:
    data = get_combined_valuation(code) or {}
    name = data.get("name") or code

    estimate = float(data.get("estimate", 0.0) or 0.0)
    nav = float(data.get("nav", 0.0) or 0.0)
    est_rate = float(data.get("estRate", 0.0) or 0.0)

    price = estimate if estimate > 0 else nav
    if price > 0 and name and name != code:
        return price, name, est_rate

    # Try local fund metadata for name fallback.
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM funds WHERE code = ?", (code,))
        row = cursor.fetchone()
        conn.close()
        if row and row["name"]:
            name = row["name"]
    except Exception:
        pass

    # Fallback to global fund name cache from AkShare.
    global _FUND_NAME_CACHE_TS
    try:
        now = time.time()
        if now - _FUND_NAME_CACHE_TS > 6 * 3600 or not _FUND_NAME_CACHE:
            df = ak.fund_name_em()
            if df is not None and not df.empty and "基金代码" in df.columns and "基金简称" in df.columns:
                cache = {
                    str(row["基金代码"]).strip(): str(row["基金简称"]).strip()
                    for _, row in df.iterrows()
                    if str(row.get("基金代码", "")).strip() and str(row.get("基金简称", "")).strip()
                }
                if cache:
                    _FUND_NAME_CACHE.clear()
                    _FUND_NAME_CACHE.update(cache)
                    _FUND_NAME_CACHE_TS = now
        if code in _FUND_NAME_CACHE:
            name = _FUND_NAME_CACHE[code]
    except Exception:
        pass

    # Fallback to Eastmoney full list (covers some ETF/LOF not in local cache).
    global _FUND_NAME_SEARCH_CACHE_TS
    try:
        now = time.time()
        if now - _FUND_NAME_SEARCH_CACHE_TS > 12 * 3600 or not _FUND_NAME_SEARCH_CACHE:
            resp = requests.get(
                Config.EASTMONEY_ALL_FUNDS_API_URL,
                timeout=8,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code == 200 and resp.text:
                # Format: ["000001","abbr","中文名","类型","拼音全称"]
                cache = {}
                for m in re.finditer(r'\["(\d{5,6})","[^"]*","([^"]+)"', resp.text):
                    c = str(m.group(1)).strip()
                    n = str(m.group(2)).strip()
                    if c and n:
                        cache[c] = n
                if cache:
                    _FUND_NAME_SEARCH_CACHE.clear()
                    _FUND_NAME_SEARCH_CACHE.update(cache)
                    _FUND_NAME_SEARCH_CACHE_TS = now
        if code in _FUND_NAME_SEARCH_CACHE and _FUND_NAME_SEARCH_CACHE[code]:
            name = _FUND_NAME_SEARCH_CACHE[code]
    except Exception:
        pass

    if price > 0:
        return price, name, est_rate

    history = get_fund_history(code, limit=1)
    if history:
        return float(history[-1]["nav"]), name, 0.0

    return None, name, 0.0


def _resolve_fund_name(code: str) -> str:
    _, name, _ = _get_price_and_name(code)
    return name or code


def _to_date(d: Optional[str]) -> date:
    if not d:
        return date.today()
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return date.today()


def _invalidate_perf_cache(portfolio_id: Optional[int] = None):
    if portfolio_id is None:
        _PERF_CACHE.clear()
        _BACKTEST_MEM_CACHE.clear()
        return

    keys = [k for k in _PERF_CACHE.keys() if k[0] == portfolio_id]
    for k in keys:
        _PERF_CACHE.pop(k, None)

    bk_keys = [k for k in _BACKTEST_MEM_CACHE.keys() if k[0] == portfolio_id]
    for k in bk_keys:
        _BACKTEST_MEM_CACHE.pop(k, None)


def _fetch_account_positions(account_id: int, code_scope: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()

    if code_scope:
        cursor.execute(
            """
            SELECT code, shares, cost
            FROM positions
            WHERE account_id = ? AND shares > 0
            """,
            (account_id,),
        )
        wanted = {normalize_asset_code(c) for c in code_scope if normalize_asset_code(c)}
        rows = [
            row for row in cursor.fetchall()
            if normalize_asset_code(str(row["code"])) in wanted
        ]
    else:
        cursor.execute(
            """
            SELECT code, shares, cost
            FROM positions
            WHERE account_id = ? AND shares > 0
            """,
            (account_id,),
        )
        rows = cursor.fetchall()
    conn.close()
    out = []
    for row in rows:
        code = normalize_asset_code(str(row["code"]))
        if not code:
            continue
        out.append({"code": code, "shares": float(row["shares"]), "cost": float(row["cost"])})
    return out


def list_portfolios(account_id: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()

    if account_id:
        cursor.execute(
            """
            SELECT p.id, p.name, p.account_id, p.benchmark, p.fee_rate, p.scope_codes,
                   p.created_at, p.updated_at,
                   v.id as active_version_id, v.version_no, v.effective_date
            FROM strategy_portfolios p
            LEFT JOIN strategy_versions v ON p.id = v.portfolio_id AND v.is_active = 1
            WHERE p.account_id = ?
            ORDER BY p.id DESC
            """,
            (account_id,),
        )
    else:
        cursor.execute(
            """
            SELECT p.id, p.name, p.account_id, p.benchmark, p.fee_rate, p.scope_codes,
                   p.created_at, p.updated_at,
                   v.id as active_version_id, v.version_no, v.effective_date
            FROM strategy_portfolios p
            LEFT JOIN strategy_versions v ON p.id = v.portfolio_id AND v.is_active = 1
            ORDER BY p.id DESC
            """
        )

    rows = []
    for row in cursor.fetchall():
        item = dict(row)
        item["scope_codes"] = _parse_scope_codes(item.get("scope_codes"))
        rows.append(item)

    conn.close()
    return rows


def get_portfolio_detail(portfolio_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, name, account_id, benchmark, fee_rate, scope_codes, created_at, updated_at
        FROM strategy_portfolios
        WHERE id = ?
        """,
        (portfolio_id,),
    )
    portfolio = cursor.fetchone()
    if not portfolio:
        conn.close()
        raise ValueError("strategy portfolio not found")

    cursor.execute(
        """
        SELECT id, version_no, effective_date, note, is_active, created_at
        FROM strategy_versions
        WHERE portfolio_id = ?
        ORDER BY version_no DESC
        """,
        (portfolio_id,),
    )
    versions = [dict(row) for row in cursor.fetchall()]
    conn.close()

    active = next((v for v in versions if v["is_active"]), None)
    active_holdings = _get_version_holdings(active["id"]) if active else []

    p = dict(portfolio)
    p["scope_codes"] = _parse_scope_codes(p.get("scope_codes"))

    return {
        "portfolio": p,
        "versions": versions,
        "active_version": active,
        "active_holdings": active_holdings,
    }


def _get_version_holdings(version_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT fund_code, target_weight
        FROM strategy_holdings
        WHERE version_id = ?
        ORDER BY fund_code
        """,
        (version_id,),
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "code": row["fund_code"],
            "weight": float(row["target_weight"]),
            "name": _resolve_fund_name(row["fund_code"]),
        }
        for row in rows
    ]


def create_portfolio(
    name: str,
    account_id: int,
    holdings: List[Dict[str, Any]],
    benchmark: str = "000300",
    fee_rate: float = 0.001,
    effective_date: Optional[str] = None,
    note: str = "",
    scope_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    normalized = _normalize_holdings(holdings)
    normalized_scope = _normalize_codes(scope_codes) or [h["code"] for h in normalized]
    effective_date = effective_date or date.today().strftime("%Y-%m-%d")

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO strategy_portfolios (name, account_id, benchmark, fee_rate, scope_codes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, account_id, benchmark, fee_rate, json.dumps(normalized_scope, ensure_ascii=False)),
        )
        portfolio_id = cursor.lastrowid

        cursor.execute(
            """
            INSERT INTO strategy_versions (portfolio_id, version_no, effective_date, note, is_active)
            VALUES (?, 1, ?, ?, 1)
            """,
            (portfolio_id, effective_date, note),
        )
        version_id = cursor.lastrowid

        cursor.executemany(
            """
            INSERT INTO strategy_holdings (version_id, fund_code, target_weight)
            VALUES (?, ?, ?)
            """,
            [(version_id, h["code"], h["weight"]) for h in normalized],
        )

        conn.commit()
        _invalidate_perf_cache(portfolio_id)
        return {
            "id": portfolio_id,
            "active_version_id": version_id,
            "holdings": normalized,
            "scope_codes": normalized_scope,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_strategy_version(
    portfolio_id: int,
    holdings: List[Dict[str, Any]],
    effective_date: Optional[str] = None,
    note: str = "",
    activate: bool = True,
    scope_codes: Optional[List[str]] = None,
    benchmark: Optional[str] = None,
    fee_rate: Optional[float] = None,
) -> Dict[str, Any]:
    normalized = _normalize_holdings(holdings)
    normalized_scope = _normalize_codes(scope_codes)
    effective_date = effective_date or date.today().strftime("%Y-%m-%d")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM strategy_portfolios WHERE id = ?", (portfolio_id,))
    if not cursor.fetchone():
        conn.close()
        raise ValueError("strategy portfolio not found")

    cursor.execute("SELECT COALESCE(MAX(version_no), 0) AS max_version FROM strategy_versions WHERE portfolio_id = ?", (portfolio_id,))
    version_no = int(cursor.fetchone()["max_version"]) + 1

    try:
        if activate:
            cursor.execute("UPDATE strategy_versions SET is_active = 0 WHERE portfolio_id = ?", (portfolio_id,))

        if normalized_scope:
            cursor.execute(
                "UPDATE strategy_portfolios SET scope_codes = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (json.dumps(normalized_scope, ensure_ascii=False), portfolio_id),
            )

        updates = []
        params: List[Any] = []
        if benchmark:
            updates.append("benchmark = ?")
            params.append(str(benchmark).strip())
        if fee_rate is not None:
            updates.append("fee_rate = ?")
            params.append(float(fee_rate))
        if updates:
            params.append(portfolio_id)
            cursor.execute(
                f"UPDATE strategy_portfolios SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                tuple(params),
            )

        cursor.execute(
            """
            INSERT INTO strategy_versions (portfolio_id, version_no, effective_date, note, is_active)
            VALUES (?, ?, ?, ?, ?)
            """,
            (portfolio_id, version_no, effective_date, note, 1 if activate else 0),
        )
        version_id = cursor.lastrowid

        cursor.executemany(
            """
            INSERT INTO strategy_holdings (version_id, fund_code, target_weight)
            VALUES (?, ?, ?)
            """,
            [(version_id, h["code"], h["weight"]) for h in normalized],
        )

        conn.commit()
        _invalidate_perf_cache(portfolio_id)
        return {
            "id": version_id,
            "version_no": version_no,
            "holdings": normalized,
            "is_active": activate,
            "scope_codes": normalized_scope,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_portfolio_scope(portfolio_id: int, scope_codes: List[str]) -> Dict[str, Any]:
    normalized_scope = _normalize_codes(scope_codes)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM strategy_portfolios WHERE id = ?", (portfolio_id,))
    if not cursor.fetchone():
        conn.close()
        raise ValueError("strategy portfolio not found")

    try:
        cursor.execute(
            "UPDATE strategy_portfolios SET scope_codes = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (json.dumps(normalized_scope, ensure_ascii=False), portfolio_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    _invalidate_perf_cache(portfolio_id)
    return {"ok": True, "scope_codes": normalized_scope}


def get_portfolio_scope_candidates(portfolio_id: int, account_id: int) -> List[Dict[str, Any]]:
    detail = get_portfolio_detail(portfolio_id)
    linked_scope = set(detail["portfolio"].get("scope_codes") or [])
    active_codes = {str(h["code"]).strip() for h in detail.get("active_holdings", [])}
    positions = _fetch_account_positions(account_id)
    account_codes = {str(p["code"]).strip() for p in positions}

    all_codes = sorted({c for c in (linked_scope | active_codes | account_codes) if c})
    position_map = {str(p["code"]).strip(): p for p in positions}
    rows = []
    for code in all_codes:
        rows.append(
            {
                "code": code,
                "name": _resolve_fund_name(code),
                "selected": code in linked_scope,
                "shares": round(float(position_map.get(code, {}).get("shares", 0.0)), 4),
            }
        )
    return rows


def generate_rebalance_orders(
    portfolio_id: int,
    account_id: int,
    min_deviation: float = 0.005,
    fee_rate: Optional[float] = None,
    lot_size: int = 1,
    capital_adjustment: float = 0.0,
    batch_title: Optional[str] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    detail = get_portfolio_detail(portfolio_id)
    active_version = detail.get("active_version")
    if not active_version:
        raise ValueError("strategy has no active version")

    holdings = detail.get("active_holdings", [])
    target_map = {h["code"]: float(h["weight"]) for h in holdings}

    scope_codes = set(detail["portfolio"].get("scope_codes") or [])
    all_codes = sorted(scope_codes | set(target_map.keys())) if scope_codes else sorted(set(target_map.keys()))
    if not all_codes:
        raise ValueError("strategy has no symbols")

    positions = _fetch_account_positions(account_id, all_codes)
    position_map = {p["code"]: p for p in positions}
    current_shares = {code: float(position_map.get(code, {}).get("shares", 0.0)) for code in all_codes}

    price_map: Dict[str, float] = {}
    name_map: Dict[str, str] = {}
    for code in all_codes:
        price, name, _ = _get_price_and_name(code)
        if price is None or price <= 0:
            continue
        price_map[code] = price
        name_map[code] = name

    if not price_map:
        raise ValueError("无法获取标的价格，无法生成调仓指令")

    total_value = sum(current_shares.get(code, 0.0) * price_map.get(code, 0.0) for code in all_codes)
    if total_value <= 0:
        raise ValueError("策略关联持仓当前市值为 0，请先关联并录入持仓")

    applied_fee_rate = float(fee_rate if fee_rate is not None else detail["portfolio"]["fee_rate"] or 0.0)
    applied_lot_size = int(max(1, lot_size or 1))
    force_reallocate = abs(float(capital_adjustment or 0.0)) > 1e-8
    target_total_value = total_value + float(capital_adjustment or 0.0)
    if target_total_value <= 0:
        raise ValueError("目标调仓后的总资产必须大于 0")

    orders: List[Dict[str, Any]] = []
    total_buy = 0.0
    total_sell = 0.0
    total_fee = 0.0

    for code in all_codes:
        price = price_map.get(code)
        if not price:
            continue

        c_shares = current_shares.get(code, 0.0)
        c_value = c_shares * price
        c_weight = c_value / total_value if total_value > 0 else 0.0

        t_weight = target_map.get(code, 0.0)
        t_value = target_total_value * t_weight
        t_shares = t_value / price if price > 0 else 0.0

        raw_delta_shares = t_shares - c_shares
        deviation = c_weight - t_weight

        action = "hold"
        # If user explicitly adjusts capital, always generate trades by target weights.
        # Otherwise, keep threshold-based rebalance behavior.
        if force_reallocate and abs(raw_delta_shares) >= 0.01:
            action = "buy" if raw_delta_shares > 0 else "sell"
        elif abs(deviation) >= min_deviation and abs(raw_delta_shares) >= 0.01:
            action = "buy" if raw_delta_shares > 0 else "sell"

        delta_shares = raw_delta_shares
        if action != "hold" and applied_lot_size > 1:
            lots = int(abs(raw_delta_shares) // applied_lot_size)
            if lots <= 0:
                action = "hold"
                delta_shares = 0.0
            else:
                delta_shares = float(lots * applied_lot_size)
                if raw_delta_shares < 0:
                    delta_shares = -delta_shares

        delta_value = delta_shares * price

        trade_amount = abs(delta_shares) * price if action != "hold" else 0.0
        fee = trade_amount * applied_fee_rate if action != "hold" else 0.0

        if action == "buy":
            total_buy += trade_amount
            total_fee += fee
        elif action == "sell":
            total_sell += trade_amount
            total_fee += fee

        orders.append(
            {
                "fund_code": code,
                "fund_name": name_map.get(code, code),
                "action": action,
                "target_weight": round(t_weight, 6),
                "current_weight": round(c_weight, 6),
                "deviation": round(deviation, 6),
                "target_shares": round(t_shares, 4),
                "current_shares": round(c_shares, 4),
                "delta_shares": round(delta_shares, 4),
                "raw_delta_shares": round(raw_delta_shares, 4),
                "delta_value": round(delta_value, 2),
                "price": round(price, 4),
                "trade_amount": round(trade_amount, 2),
                "fee": round(fee, 2),
            }
        )

    orders.sort(key=lambda x: abs(x["delta_shares"]), reverse=True)
    actionable = [o for o in orders if o["action"] != "hold"]

    batch_id: Optional[int] = None
    if persist:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            normalized_title = str(batch_title or "").strip() or "智能调仓批次"
            cursor.execute(
                """
                INSERT INTO rebalance_batches (portfolio_id, account_id, version_id, source, status, title, note)
                VALUES (?, ?, ?, 'smart_rebalance', 'pending', ?, ?)
                """,
                (
                    portfolio_id,
                    account_id,
                    active_version["id"],
                    normalized_title,
                    json.dumps(
                        {
                            "min_deviation": min_deviation,
                            "lot_size": applied_lot_size,
                            "capital_adjustment": round(float(capital_adjustment or 0.0), 2),
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            batch_id = cursor.lastrowid

            cursor.executemany(
                """
                INSERT INTO rebalance_orders (
                    portfolio_id, version_id, account_id, fund_code, fund_name,
                    action, target_weight, current_weight, target_shares,
                    current_shares, delta_shares, price, trade_amount, fee, status, batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        portfolio_id,
                        active_version["id"],
                        account_id,
                        item["fund_code"],
                        item["fund_name"],
                        item["action"],
                        item["target_weight"],
                        item["current_weight"],
                        item["target_shares"],
                        item["current_shares"],
                        item["delta_shares"],
                        item["price"],
                        item["trade_amount"],
                        item["fee"],
                        "skipped" if item["action"] == "hold" else "suggested",
                        batch_id,
                    )
                    for item in orders
                ],
            )
            if len(actionable) == 0:
                cursor.execute(
                    "UPDATE rebalance_batches SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id = ?",
                    (batch_id,),
                )
            conn.commit()
            _invalidate_perf_cache(portfolio_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return {
        "batch_id": batch_id,
        "orders": orders,
        "summary": {
            "total_assets": round(total_value, 2),
            "actionable_count": len(actionable),
            "total_buy": round(total_buy, 2),
            "total_sell": round(total_sell, 2),
            "estimated_fee": round(total_fee, 2),
            "fee_rate": applied_fee_rate,
            "lot_size": applied_lot_size,
            "capital_adjustment": round(float(capital_adjustment or 0.0), 2),
            "target_total_assets": round(target_total_value, 2),
            "scope_size": len(all_codes),
        },
}


def refresh_rebalance_batch(batch_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, portfolio_id, account_id, version_id, status, title, note
        FROM rebalance_batches
        WHERE id = ?
        """,
        (batch_id,),
    )
    batch = cursor.fetchone()
    if not batch:
        conn.close()
        raise ValueError("batch not found")
    if batch["status"] == "completed":
        conn.close()
        raise ValueError("该批次已归档，无法刷新")

    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM rebalance_orders
        WHERE batch_id = ? AND action IN ('buy','sell') AND status != 'suggested'
        """,
        (batch_id,),
    )
    edited_count = int(cursor.fetchone()["cnt"] or 0)
    if edited_count > 0:
        conn.close()
        raise ValueError("该批次已有执行/跳过记录，无法整批刷新")

    params = {"min_deviation": 0.005, "lot_size": 100, "capital_adjustment": 0.0}
    try:
        note_obj = json.loads(batch["note"] or "{}")
        if isinstance(note_obj, dict):
            if note_obj.get("min_deviation") is not None:
                params["min_deviation"] = float(note_obj.get("min_deviation"))
            if note_obj.get("lot_size") is not None:
                params["lot_size"] = int(max(1, round(float(note_obj.get("lot_size")))))
            if note_obj.get("capital_adjustment") is not None:
                params["capital_adjustment"] = float(note_obj.get("capital_adjustment"))
    except Exception:
        pass
    if params["lot_size"] <= 1:
        params["lot_size"] = 100

    calc = generate_rebalance_orders(
        portfolio_id=int(batch["portfolio_id"]),
        account_id=int(batch["account_id"]),
        min_deviation=float(params["min_deviation"]),
        lot_size=int(params["lot_size"]),
        capital_adjustment=float(params["capital_adjustment"]),
        batch_title=str(batch["title"] or "").strip() or "智能调仓批次",
        persist=False,
    )
    orders = calc.get("orders", [])
    actionable_count = int(calc.get("summary", {}).get("actionable_count", 0) or 0)

    try:
        cursor.execute("DELETE FROM rebalance_orders WHERE batch_id = ?", (batch_id,))
        cursor.executemany(
            """
            INSERT INTO rebalance_orders (
                portfolio_id, version_id, account_id, fund_code, fund_name,
                action, target_weight, current_weight, target_shares,
                current_shares, delta_shares, price, trade_amount, fee, status, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    int(batch["portfolio_id"]),
                    int(batch["version_id"]),
                    int(batch["account_id"]),
                    item["fund_code"],
                    item["fund_name"],
                    item["action"],
                    item["target_weight"],
                    item["current_weight"],
                    item["target_shares"],
                    item["current_shares"],
                    item["delta_shares"],
                    item["price"],
                    item["trade_amount"],
                    item["fee"],
                    "skipped" if item["action"] == "hold" else "suggested",
                    int(batch_id),
                )
                for item in orders
            ],
        )
        if actionable_count == 0:
            cursor.execute(
                "UPDATE rebalance_batches SET status='completed', completed_at=CURRENT_TIMESTAMP, note=? WHERE id = ?",
                (json.dumps(params, ensure_ascii=False), batch_id),
            )
        else:
            cursor.execute(
                "UPDATE rebalance_batches SET status='pending', completed_at=NULL, note=? WHERE id = ?",
                (json.dumps(params, ensure_ascii=False), batch_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    _invalidate_perf_cache(int(batch["portfolio_id"]))
    return {"ok": True, "batch_id": int(batch_id), "actionable_count": actionable_count}


def list_rebalance_orders(
    portfolio_id: int,
    account_id: int,
    status: Optional[str] = None,
    batch_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()

    if status and batch_id:
        cursor.execute(
            """
            SELECT id, portfolio_id, version_id, account_id, fund_code, fund_name,
                   action, target_weight, current_weight, target_shares,
                   current_shares, delta_shares, price, trade_amount, fee,
                   batch_id,
                   status, created_at, executed_at, executed_shares, executed_price,
                   executed_amount, execution_note
            FROM rebalance_orders
            WHERE portfolio_id = ? AND account_id = ? AND status = ? AND batch_id = ?
            ORDER BY id ASC
            """,
            (portfolio_id, account_id, status, batch_id),
        )
    elif status:
        cursor.execute(
            """
            SELECT id, portfolio_id, version_id, account_id, fund_code, fund_name,
                   action, target_weight, current_weight, target_shares,
                   current_shares, delta_shares, price, trade_amount, fee,
                   batch_id,
                   status, created_at, executed_at, executed_shares, executed_price,
                   executed_amount, execution_note
            FROM rebalance_orders
            WHERE portfolio_id = ? AND account_id = ? AND status = ?
            ORDER BY id DESC
            """,
            (portfolio_id, account_id, status),
        )
    elif batch_id:
        cursor.execute(
            """
            SELECT id, portfolio_id, version_id, account_id, fund_code, fund_name,
                   action, target_weight, current_weight, target_shares,
                   current_shares, delta_shares, price, trade_amount, fee,
                   batch_id,
                   status, created_at, executed_at, executed_shares, executed_price,
                   executed_amount, execution_note
            FROM rebalance_orders
            WHERE portfolio_id = ? AND account_id = ? AND batch_id = ?
            ORDER BY id ASC
            """,
            (portfolio_id, account_id, batch_id),
        )
    else:
        cursor.execute(
            """
            SELECT id, portfolio_id, version_id, account_id, fund_code, fund_name,
                   action, target_weight, current_weight, target_shares,
                   current_shares, delta_shares, price, trade_amount, fee,
                   batch_id,
                   status, created_at, executed_at, executed_shares, executed_price,
                   executed_amount, execution_note
            FROM rebalance_orders
            WHERE portfolio_id = ? AND account_id = ?
            ORDER BY id DESC
            """,
            (portfolio_id, account_id),
        )

    rows = [dict(row) for row in cursor.fetchall()]
    updated = False
    for row in rows:
        if row.get("action") == "hold" and row.get("status") == "suggested":
            row["status"] = "skipped"
            updated = True
            try:
                cursor.execute(
                    "UPDATE rebalance_orders SET status = 'skipped' WHERE id = ?",
                    (row["id"],),
                )
            except Exception:
                pass
        if not row.get("fund_name"):
            row["fund_name"] = _resolve_fund_name(str(row.get("fund_code") or ""))
            updated = True
            try:
                cursor.execute(
                    "UPDATE rebalance_orders SET fund_name = ? WHERE id = ?",
                    (row["fund_name"], row["id"]),
                )
            except Exception:
                pass
    if updated:
        conn.commit()
    conn.close()
    return rows


def _refresh_batch_status(batch_id: Optional[int]):
    if not batch_id:
        return
    # Keep for compatibility, but batch completion is now an explicit action.
    return


def list_rebalance_batches(portfolio_id: int, account_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, portfolio_id, account_id, version_id, source, status, title, note, created_at, completed_at
        FROM rebalance_batches
        WHERE portfolio_id = ? AND account_id = ?
        ORDER BY id DESC
        """,
        (portfolio_id, account_id),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    for row in rows:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN action IN ('buy','sell') THEN 1 ELSE 0 END) AS actionable_total,
                SUM(CASE WHEN action IN ('buy','sell') AND status='suggested' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN action IN ('buy','sell') AND status='executed' THEN 1 ELSE 0 END) AS executed,
                SUM(CASE WHEN action IN ('buy','sell') AND status='skipped' THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN action='buy' THEN trade_amount ELSE 0 END) AS buy_amount,
                SUM(CASE WHEN action='sell' THEN trade_amount ELSE 0 END) AS sell_amount
            FROM rebalance_orders
            WHERE batch_id = ?
            """,
            (row["id"],),
        )
        stats = cursor.fetchone()
        row["total_orders"] = int(stats["total"] or 0)
        row["actionable_orders"] = int(stats["actionable_total"] or 0)
        row["pending_orders"] = int(stats["pending"] or 0)
        row["executed_orders"] = int(stats["executed"] or 0)
        row["skipped_orders"] = int(stats["skipped"] or 0)
        row["completed_orders"] = row["executed_orders"] + row["skipped_orders"]
        row["buy_amount"] = round(float(stats["buy_amount"] or 0.0), 2)
        row["sell_amount"] = round(float(stats["sell_amount"] or 0.0), 2)
        row["net_amount"] = round(row["buy_amount"] - row["sell_amount"], 2)
    conn.close()
    return rows


def complete_rebalance_batch(batch_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, portfolio_id, status FROM rebalance_batches WHERE id = ?",
        (batch_id,),
    )
    batch = cursor.fetchone()
    if not batch:
        conn.close()
        raise ValueError("batch not found")

    if batch["status"] == "completed":
        conn.close()
        return {"ok": True, "batch_id": batch_id, "status": "completed"}

    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM rebalance_orders
        WHERE batch_id = ? AND action IN ('buy','sell') AND status = 'suggested'
        """,
        (batch_id,),
    )
    pending = int(cursor.fetchone()["cnt"] or 0)
    if pending > 0:
        conn.close()
        raise ValueError("仍有待执行指令，无法完成批次")

    try:
        cursor.execute(
            "UPDATE rebalance_batches SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?",
            (batch_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    _invalidate_perf_cache(int(batch["portfolio_id"]))
    return {"ok": True, "batch_id": batch_id, "status": "completed"}


def _assert_batch_editable(cursor, batch_id: Optional[int]):
    if not batch_id:
        return
    cursor.execute("SELECT status FROM rebalance_batches WHERE id = ?", (batch_id,))
    row = cursor.fetchone()
    if row and row["status"] == "completed":
        raise ValueError("该批次已归档，禁止再修改")


def update_rebalance_order_status(order_id: int, status: str) -> Dict[str, Any]:
    if status not in {"suggested", "executed", "skipped"}:
        raise ValueError("invalid status")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, portfolio_id, batch_id FROM rebalance_orders WHERE id = ?", (order_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise ValueError("order not found")
    _assert_batch_editable(cursor, row["batch_id"])

    executed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if status == "executed" else None
    cursor.execute(
        """
        UPDATE rebalance_orders
        SET status = ?, executed_at = ?
        WHERE id = ?
        """,
        (status, executed_at, order_id),
    )
    conn.commit()
    conn.close()
    _invalidate_perf_cache(int(row["portfolio_id"]))
    _refresh_batch_status(row["batch_id"])
    return {"ok": True}


def execute_rebalance_order(
    order_id: int,
    executed_shares: float,
    executed_price: float,
    note: str = "",
) -> Dict[str, Any]:
    if executed_shares <= 0:
        raise ValueError("executed_shares must be > 0")
    if executed_price <= 0:
        raise ValueError("executed_price must be > 0")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, portfolio_id, account_id, fund_code, action, fee
               , batch_id
        FROM rebalance_orders
        WHERE id = ?
        """,
        (order_id,),
    )
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise ValueError("order not found")
    _assert_batch_editable(cursor, row["batch_id"])

    if row["action"] not in {"buy", "sell"}:
        conn.close()
        raise ValueError("only buy/sell orders can be executed")

    account_id = int(row["account_id"])
    code = row["fund_code"]
    action = row["action"]
    fee = float(row["fee"] or 0.0)

    cursor.execute(
        "SELECT shares, cost FROM positions WHERE account_id = ? AND code = ?",
        (account_id, code),
    )
    pos = cursor.fetchone()
    old_shares = float(pos["shares"]) if pos else 0.0
    old_cost = float(pos["cost"]) if pos else 0.0

    trade_amount = round(executed_shares * executed_price, 2)

    try:
        if action == "buy":
            new_shares = round(old_shares + executed_shares, 4)
            if new_shares <= 0:
                raise ValueError("invalid resulting shares")
            if old_shares > 0:
                new_cost = round((old_cost * old_shares + executed_price * executed_shares) / new_shares, 4)
            else:
                new_cost = round(executed_price, 4)
            upsert_position(account_id, code, new_cost, new_shares)
        else:
            if old_shares <= 0:
                raise ValueError("position not found for sell execution")
            if executed_shares > old_shares:
                raise ValueError(f"executed_shares exceeds current shares ({old_shares})")
            new_shares = round(old_shares - executed_shares, 4)
            if new_shares <= 0:
                remove_position(account_id, code)
                new_cost = 0.0
            else:
                new_cost = old_cost
                upsert_position(account_id, code, new_cost, new_shares)

        cursor.execute(
            """
            UPDATE rebalance_orders
            SET status = 'executed',
                executed_at = CURRENT_TIMESTAMP,
                executed_shares = ?,
                executed_price = ?,
                executed_amount = ?,
                execution_note = ?
            WHERE id = ?
            """,
            (executed_shares, executed_price, trade_amount, note, order_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    _invalidate_perf_cache(int(row["portfolio_id"]))
    _refresh_batch_status(row["batch_id"])
    return {
        "ok": True,
        "order_id": order_id,
        "account_id": account_id,
        "code": code,
        "action": action,
        "executed_shares": round(executed_shares, 4),
        "executed_price": round(executed_price, 4),
        "executed_amount": trade_amount,
        "estimated_fee": fee,
    }


def delete_portfolio(portfolio_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM strategy_portfolios WHERE id = ?", (portfolio_id,))
    if not cursor.fetchone():
        conn.close()
        raise ValueError("strategy portfolio not found")

    try:
        # Explicit delete to avoid relying on foreign_key pragma.
        cursor.execute("SELECT id FROM strategy_versions WHERE portfolio_id = ?", (portfolio_id,))
        version_ids = [int(r["id"]) for r in cursor.fetchall()]

        if version_ids:
            placeholders = ",".join(["?"] * len(version_ids))
            cursor.execute(
                f"DELETE FROM strategy_holdings WHERE version_id IN ({placeholders})",
                version_ids,
            )
            cursor.execute(
                f"DELETE FROM rebalance_orders WHERE version_id IN ({placeholders})",
                version_ids,
            )

        cursor.execute("DELETE FROM rebalance_orders WHERE portfolio_id = ?", (portfolio_id,))
        cursor.execute("DELETE FROM strategy_versions WHERE portfolio_id = ?", (portfolio_id,))
        cursor.execute("DELETE FROM strategy_portfolios WHERE id = ?", (portfolio_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    _invalidate_perf_cache(portfolio_id)
    return {"ok": True}


def get_portfolio_positions_view(portfolio_id: int, account_id: int) -> Dict[str, Any]:
    detail = get_portfolio_detail(portfolio_id)
    active_holdings = detail.get("active_holdings", [])
    target_map = {h["code"]: float(h["weight"]) for h in active_holdings}

    linked_scope = set(detail["portfolio"].get("scope_codes") or [])
    scope = sorted(linked_scope | set(target_map.keys()))

    positions = _fetch_account_positions(account_id, scope if scope else None)
    position_map = {p["code"]: p for p in positions}

    price_map: Dict[str, float] = {}
    name_map: Dict[str, str] = {}
    for code in scope:
        price, name, _ = _get_price_and_name(code)
        if price and price > 0:
            price_map[code] = price
        name_map[code] = name

    total_value = sum(position_map.get(code, {}).get("shares", 0.0) * price_map.get(code, 0.0) for code in scope)
    if total_value <= 0:
        total_value = 0.0

    rows = []
    for code in scope:
        pos = position_map.get(code, {"shares": 0.0, "cost": 0.0})
        shares = float(pos.get("shares", 0.0))
        cost = float(pos.get("cost", 0.0))
        price = float(price_map.get(code, 0.0))
        market_value = shares * price
        cost_basis = shares * cost
        profit = market_value - cost_basis
        profit_rate = (profit / cost_basis) if cost_basis > 0 else 0.0
        current_weight = market_value / total_value if total_value > 0 else 0.0
        target_weight = float(target_map.get(code, 0.0))

        rows.append({
            "code": code,
            "name": name_map.get(code, code),
            "shares": round(shares, 4),
            "cost": round(cost, 4),
            "price": round(price, 4),
            "cost_basis": round(cost_basis, 2),
            "market_value": round(market_value, 2),
            "profit": round(profit, 2),
            "profit_rate": round(profit_rate, 6),
            "current_weight": round(current_weight, 6),
            "target_weight": round(target_weight, 6),
            "deviation": round(current_weight - target_weight, 6),
        })

    rows.sort(key=lambda x: x["market_value"], reverse=True)
    return {
        "rows": rows,
        "summary": {
            "scope_codes": scope,
            "total_market_value": round(total_value, 2),
            "position_count": len([r for r in rows if r["shares"] > 0]),
        }
    }


def _get_db_history_series(code: str, start_date: pd.Timestamp, end_date: Optional[pd.Timestamp] = None) -> pd.Series:
    conn = get_db_connection()
    cursor = conn.cursor()
    if end_date is None:
        cursor.execute(
            """
            SELECT date, nav
            FROM fund_history
            WHERE code = ? AND date >= ?
            ORDER BY date ASC
            """,
            (code, start_date.strftime("%Y-%m-%d")),
        )
    else:
        cursor.execute(
            """
            SELECT date, nav
            FROM fund_history
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            (code, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
        )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return pd.Series(dtype=float)

    return pd.Series(
        [float(r["nav"]) for r in rows],
        index=pd.to_datetime([r["date"] for r in rows]),
        dtype=float,
    )


def _get_cached_history_series(
    code: str,
    start_date: pd.Timestamp,
    end_date: Optional[pd.Timestamp] = None,
    fetch_missing: bool = True,
) -> pd.Series:
    db_series = _get_db_history_series(code, start_date, end_date=end_date)
    if len(db_series) >= 30 or not fetch_missing:
        return db_series

    history = get_fund_history(code, limit=1500)
    if not history:
        return db_series

    filtered = [h for h in history if h["date"] >= start_date.strftime("%Y-%m-%d")]
    if end_date is not None:
        end_str = end_date.strftime("%Y-%m-%d")
        filtered = [h for h in filtered if h["date"] <= end_str]
    if not filtered:
        return db_series

    return pd.Series(
        [float(h["nav"]) for h in filtered],
        index=pd.to_datetime([h["date"] for h in filtered]),
        dtype=float,
    )


def _build_portfolio_return_series(
    weight_map: Dict[str, float],
    start_date: pd.Timestamp,
    end_date: Optional[pd.Timestamp] = None,
    fetch_missing: bool = True,
) -> pd.Series:
    if not weight_map:
        return pd.Series(dtype=float)

    nav_frames = {}
    for code, _ in weight_map.items():
        series = _get_cached_history_series(code, start_date, end_date=end_date, fetch_missing=fetch_missing)
        if not series.empty:
            nav_frames[code] = series

    if not nav_frames:
        return pd.Series(dtype=float)

    nav_df = pd.concat(nav_frames, axis=1).sort_index().ffill().dropna(how="all")
    available_cols = [c for c in nav_df.columns if not nav_df[c].isna().all()]
    if not available_cols:
        return pd.Series(dtype=float)

    nav_df = nav_df[available_cols].ffill().dropna(how="all")
    usable_weights = {c: weight_map[c] for c in available_cols if c in weight_map}
    weight_sum = sum(usable_weights.values())
    if weight_sum <= 0:
        return pd.Series(dtype=float)

    weights = pd.Series({k: v / weight_sum for k, v in usable_weights.items()}, dtype=float)
    daily_returns = nav_df.pct_change().fillna(0.0)
    portfolio_returns = daily_returns.mul(weights, axis=1).sum(axis=1)
    return portfolio_returns


def _fetch_hs300_returns(start_date: pd.Timestamp, end_date: Optional[pd.Timestamp] = None) -> pd.Series:
    start = start_date.strftime("%Y%m%d")
    end = (end_date or pd.Timestamp(datetime.now())).strftime("%Y%m%d")

    try:
        df = ak.index_zh_a_hist(symbol="000300", period="daily", start_date=start, end_date=end)
        if df is not None and not df.empty:
            s = pd.Series(df["收盘"].astype(float).values, index=pd.to_datetime(df["日期"]))
            return s.pct_change().fillna(0.0)
    except Exception as e:
        logger.warning(f"Failed to fetch HS300 by index_zh_a_hist: {e}")

    try:
        df = ak.stock_zh_index_daily_em(symbol="sh000300")
        if df is not None and not df.empty:
            s = pd.Series(df["close"].astype(float).values, index=pd.to_datetime(df["date"]))
            s = s[s.index >= start_date]
            if end_date is not None:
                s = s[s.index <= end_date]
            return s.pct_change().fillna(0.0)
    except Exception as e:
        logger.warning(f"Failed to fetch HS300 by stock_zh_index_daily_em: {e}")

    # Sina index fallback: more stable in some network/proxy environments.
    try:
        df = ak.stock_zh_index_daily(symbol="sh000300")
        if df is not None and not df.empty:
            s = pd.Series(df["close"].astype(float).values, index=pd.to_datetime(df["date"]))
            s = s[s.index >= start_date]
            if end_date is not None:
                s = s[s.index <= end_date]
            return s.pct_change().fillna(0.0)
    except Exception as e:
        logger.warning(f"Failed to fetch HS300 by stock_zh_index_daily: {e}")

    return pd.Series(dtype=float)


def _calculate_period_returns(nav: pd.Series, as_of: Optional[pd.Timestamp] = None) -> Dict[str, Optional[float]]:
    if nav.empty:
        return {"week": None, "month": None, "quarter": None, "year": None, "ytd": None}

    latest_value = float(nav.iloc[-1])
    now = (as_of or pd.Timestamp.now()).normalize()

    week_start = now - pd.Timedelta(days=now.weekday())
    month_start = now.replace(day=1)
    quarter_month = ((now.month - 1) // 3) * 3 + 1
    quarter_start = now.replace(month=quarter_month, day=1)
    year_start = now.replace(month=1, day=1)

    def _ret(start_dt: pd.Timestamp) -> Optional[float]:
        hist = nav[nav.index <= start_dt]
        base = float(hist.iloc[-1]) if not hist.empty else float(nav.iloc[0])
        if base <= 0:
            return None
        return latest_value / base - 1

    return {
        "week": _ret(week_start),
        "month": _ret(month_start),
        "quarter": _ret(quarter_start),
        "year": _ret(now - pd.Timedelta(days=365)),
        "ytd": _ret(year_start),
    }


def _calculate_metrics(portfolio_returns: pd.Series, benchmark_returns: pd.Series) -> Dict[str, Optional[float]]:
    if portfolio_returns.empty:
        return {
            "annual_return": None,
            "annual_volatility": None,
            "sharpe": None,
            "alpha": None,
            "beta": None,
            "calmar": None,
            "information_ratio": None,
            "max_drawdown": None,
        }

    portfolio_returns = portfolio_returns.dropna()
    if len(portfolio_returns) < 2:
        return {
            "annual_return": None,
            "annual_volatility": None,
            "sharpe": None,
            "alpha": None,
            "beta": None,
            "calmar": None,
            "information_ratio": None,
            "max_drawdown": None,
        }

    cum_nav = (1 + portfolio_returns).cumprod()
    total_return = float(cum_nav.iloc[-1] - 1)
    years = max(len(portfolio_returns) / TRADING_DAYS_PER_YEAR, 1 / TRADING_DAYS_PER_YEAR)
    annual_return = (1 + total_return) ** (1 / years) - 1

    annual_vol = float(portfolio_returns.std(ddof=0) * np.sqrt(TRADING_DAYS_PER_YEAR))
    sharpe = (annual_return - RISK_FREE_RATE) / annual_vol if annual_vol > 1e-8 else None

    running_max = cum_nav.cummax()
    drawdown = cum_nav / running_max - 1
    max_drawdown = float(drawdown.min()) if not drawdown.empty else None
    calmar = annual_return / abs(max_drawdown) if max_drawdown and max_drawdown < 0 else None

    alpha = None
    beta = None
    info_ratio = None

    if not benchmark_returns.empty:
        joined = pd.concat([portfolio_returns, benchmark_returns], axis=1, join="inner").dropna()
        if len(joined) >= 20:
            joined.columns = ["p", "b"]
            var_b = float(joined["b"].var(ddof=0))
            if var_b > 1e-12:
                cov_pb = float(np.cov(joined["p"], joined["b"], ddof=0)[0, 1])
                beta = cov_pb / var_b
                alpha_daily = float(joined["p"].mean() - beta * joined["b"].mean())
                alpha = alpha_daily * TRADING_DAYS_PER_YEAR

            active = joined["p"] - joined["b"]
            active_std = float(active.std(ddof=0))
            if active_std > 1e-12:
                info_ratio = float(active.mean() / active_std * np.sqrt(TRADING_DAYS_PER_YEAR))

    return {
        "annual_return": annual_return,
        "annual_volatility": annual_vol,
        "sharpe": sharpe,
        "alpha": alpha,
        "beta": beta,
        "calmar": calmar,
        "information_ratio": info_ratio,
        "max_drawdown": max_drawdown,
    }


def _to_return_points(returns: pd.Series) -> List[Dict[str, Any]]:
    if returns.empty:
        return []
    nav = (1 + returns).cumprod()
    return [{"date": idx.strftime("%Y-%m-%d"), "return": round(float(val - 1), 6)} for idx, val in nav.items()]


def _estimate_intraday_rate(weight_map: Dict[str, float]) -> Optional[float]:
    if not weight_map:
        return None

    weighted = 0.0
    used = 0.0
    for code, weight in weight_map.items():
        _, _, est_rate = _get_price_and_name(code)
        weighted += weight * est_rate / 100.0
        used += weight

    if used <= 0:
        return None
    return weighted / used


def _infer_lot_size(code: str, name: str) -> int:
    # ETF/场内通常按 100 份交易；普通场外基金按 1 份。
    hint = f"{code} {name}".upper()
    if "ETF" in hint:
        return 100
    return 1


def _json_hash(obj: Dict[str, Any]) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _build_history_data_tag(codes: List[str], start_date: pd.Timestamp, end_date: pd.Timestamp) -> str:
    conn = get_db_connection()
    cursor = conn.cursor()
    parts: List[str] = []
    try:
        for code in sorted(set(codes)):
            cursor.execute(
                """
                SELECT COALESCE(MAX(updated_at), ''), COUNT(*) AS cnt
                FROM fund_history
                WHERE code = ? AND date >= ? AND date <= ?
                """,
                (code, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
            )
            row = cursor.fetchone()
            ts = str(row[0] or "")
            cnt = int(row[1] or 0)
            parts.append(f"{code}:{ts}:{cnt}")
    finally:
        conn.close()
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _load_backtest_cache(
    portfolio_id: int,
    account_id: int,
    version_id: Optional[int],
    query_hash: str,
    data_tag: str,
) -> Optional[Dict[str, Any]]:
    mem_key = (portfolio_id, account_id, f"{version_id}:{query_hash}:{data_tag}")
    now = time.time()
    cached = _BACKTEST_MEM_CACHE.get(mem_key)
    if cached and now - cached[0] <= BACKTEST_CACHE_TTL_SECONDS:
        return cached[1]

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT result_json, created_at
            FROM strategy_backtest_cache
            WHERE portfolio_id = ? AND account_id = ?
              AND COALESCE(version_id, 0) = COALESCE(?, 0)
              AND query_hash = ? AND data_tag = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (portfolio_id, account_id, version_id, query_hash, data_tag),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if not row:
        return None
    try:
        result = json.loads(row["result_json"])
    except Exception:
        return None
    _BACKTEST_MEM_CACHE[mem_key] = (now, result)
    return result


def _save_backtest_cache(
    portfolio_id: int,
    account_id: int,
    version_id: Optional[int],
    query_hash: str,
    data_tag: str,
    result: Dict[str, Any],
) -> None:
    mem_key = (portfolio_id, account_id, f"{version_id}:{query_hash}:{data_tag}")
    _BACKTEST_MEM_CACHE[mem_key] = (time.time(), result)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT OR REPLACE INTO strategy_backtest_cache (
                portfolio_id, account_id, version_id, query_hash, data_tag, result_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (portfolio_id, account_id, version_id, query_hash, data_tag, json.dumps(result, ensure_ascii=False)),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_backtest(
    portfolio_id: int,
    account_id: int,
    start_date: str,
    end_date: str,
    initial_capital: float,
    rebalance_mode: str = "threshold",
    threshold: float = 0.005,
    periodic_days: int = 20,
    fee_rate: Optional[float] = None,
    cache_only: bool = False,
) -> Dict[str, Any]:
    if initial_capital <= 0:
        raise ValueError("initial_capital must be > 0")

    mode = str(rebalance_mode or "threshold").strip().lower()
    if mode not in {"none", "threshold", "periodic", "hybrid"}:
        raise ValueError("invalid rebalance_mode")

    start_ts = pd.Timestamp(_to_date(start_date))
    end_ts = pd.Timestamp(_to_date(end_date))
    if end_ts < start_ts:
        raise ValueError("end_date must be >= start_date")

    detail = get_portfolio_detail(portfolio_id)
    active_version = detail.get("active_version")
    holdings = detail.get("active_holdings", [])
    if not holdings:
        raise ValueError("strategy has no active holdings")

    weights = {h["code"]: float(h["weight"]) for h in holdings}
    weight_sum = sum(weights.values())
    if weight_sum <= 0:
        raise ValueError("invalid target holdings")
    weights = {k: v / weight_sum for k, v in weights.items()}

    applied_fee = float(detail["portfolio"]["fee_rate"] if fee_rate is None else fee_rate)
    threshold = max(0.0, float(threshold or 0.0))
    periodic_days = max(1, int(periodic_days or 1))

    query_payload = {
        "engine_version": BACKTEST_ENGINE_VERSION,
        "portfolio_id": portfolio_id,
        "account_id": account_id,
        "version_id": active_version["id"] if active_version else None,
        "start_date": start_ts.strftime("%Y-%m-%d"),
        "end_date": end_ts.strftime("%Y-%m-%d"),
        "initial_capital": round(float(initial_capital), 4),
        "rebalance_mode": mode,
        "threshold": round(threshold, 6),
        "periodic_days": periodic_days,
        "fee_rate": round(applied_fee, 8),
        "cache_only": bool(cache_only),
    }
    query_hash = _json_hash(query_payload)
    data_tag = _build_history_data_tag(list(weights.keys()), start_ts, end_ts)
    cached = _load_backtest_cache(portfolio_id, account_id, active_version["id"] if active_version else None, query_hash, data_tag)
    if cached:
        return cached

    nav_frames: Dict[str, pd.Series] = {}
    name_map: Dict[str, str] = {}
    lot_map: Dict[str, int] = {}
    for code in sorted(weights.keys()):
        s = _get_cached_history_series(code, start_ts, end_date=end_ts, fetch_missing=not cache_only)
        if s.empty:
            continue
        nav_frames[code] = s
        nm = _resolve_fund_name(code)
        name_map[code] = nm
        lot_map[code] = _infer_lot_size(code, nm)

    if not nav_frames:
        raise ValueError("回测区间没有可用净值数据")

    nav_df = pd.concat(nav_frames, axis=1).sort_index().ffill().dropna(how="all")
    nav_df = nav_df[(nav_df.index >= start_ts) & (nav_df.index <= end_ts)]
    usable_codes = [c for c in nav_df.columns if not nav_df[c].isna().all()]
    if not usable_codes:
        raise ValueError("回测区间没有可用标的净值")
    nav_df = nav_df[usable_codes].ffill().dropna(how="all")

    weights = {c: weights[c] for c in usable_codes}
    wsum = sum(weights.values())
    weights = {c: weights[c] / wsum for c in usable_codes}

    dates = list(nav_df.index)
    first_prices = nav_df.iloc[0].to_dict()
    shares: Dict[str, float] = {}
    for code in usable_codes:
        px = float(first_prices.get(code, 0.0) or 0.0)
        if px <= 0:
            shares[code] = 0.0
            continue
        shares[code] = (initial_capital * weights[code]) / px
    cash = 0.0

    equity_track: List[Tuple[pd.Timestamp, float]] = []
    trades: List[Dict[str, Any]] = []
    rebalance_count = 0
    total_fee = 0.0
    turnover = 0.0
    last_rebalance_idx = 0

    def mark_to_market(prices: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
        pos_values: Dict[str, float] = {}
        total = cash
        for c in usable_codes:
            px = float(prices.get(c, 0.0) or 0.0)
            val = float(shares.get(c, 0.0)) * px if px > 0 else 0.0
            pos_values[c] = val
            total += val
        return total, pos_values

    for i, dt in enumerate(dates):
        prices = nav_df.loc[dt].to_dict()
        total_equity, pos_values = mark_to_market(prices)
        equity_track.append((dt, total_equity))
        if total_equity <= 0:
            continue

        cur_weights = {c: (pos_values.get(c, 0.0) / total_equity) for c in usable_codes}
        threshold_hit = any(abs(cur_weights[c] - weights[c]) >= threshold for c in usable_codes)
        periodic_hit = (i - last_rebalance_idx) >= periodic_days and i > 0

        should_rebalance = False
        reason = ""
        if mode == "threshold" and threshold_hit:
            should_rebalance = True
            reason = "threshold"
        elif mode == "periodic" and periodic_hit:
            should_rebalance = True
            reason = "periodic"
        elif mode == "hybrid" and (threshold_hit or periodic_hit):
            should_rebalance = True
            reason = "hybrid"

        if not should_rebalance:
            continue

        sell_orders: List[Tuple[str, float, float, float]] = []
        buy_orders: List[Tuple[str, float, float, float]] = []
        for code in usable_codes:
            px = float(prices.get(code, 0.0) or 0.0)
            if px <= 0:
                continue
            target_value = total_equity * weights[code]
            current_value = pos_values.get(code, 0.0)
            delta_value = target_value - current_value
            raw_delta_shares = delta_value / px
            lot = lot_map.get(code, 1)
            if lot <= 1:
                exec_shares = raw_delta_shares
            else:
                lots = int(abs(raw_delta_shares) // lot)
                exec_shares = float(lots * lot)
                if raw_delta_shares < 0:
                    exec_shares = -exec_shares
            if abs(exec_shares) < 1e-8:
                continue
            trade_amount = abs(exec_shares) * px
            if exec_shares < 0:
                sell_orders.append((code, exec_shares, px, trade_amount))
            else:
                buy_orders.append((code, exec_shares, px, trade_amount))

        if not sell_orders and not buy_orders:
            continue

        rebalance_count += 1
        last_rebalance_idx = i

        for code, exec_shares, px, trade_amount in sell_orders:
            fee = trade_amount * applied_fee
            available = shares.get(code, 0.0)
            sell_shares = min(abs(exec_shares), available)
            if sell_shares <= 0:
                continue
            sell_amount = sell_shares * px
            fee = sell_amount * applied_fee
            shares[code] = max(0.0, available - sell_shares)
            cash += sell_amount - fee
            total_fee += fee
            turnover += sell_amount
            trades.append(
                {
                    "date": dt.strftime("%Y-%m-%d"),
                    "code": code,
                    "name": name_map.get(code, code),
                    "action": "sell",
                    "shares": round(sell_shares, 4),
                    "price": round(px, 4),
                    "amount": round(sell_amount, 2),
                    "fee": round(fee, 2),
                    "reason": reason,
                }
            )

        for code, exec_shares, px, _ in buy_orders:
            plan_amount = abs(exec_shares) * px
            max_affordable = cash / (1.0 + applied_fee) if applied_fee >= 0 else cash
            buy_amount = min(plan_amount, max_affordable)
            if buy_amount <= 0:
                continue
            buy_shares = buy_amount / px
            lot = lot_map.get(code, 1)
            if lot > 1:
                lots = int(buy_shares // lot)
                buy_shares = float(lots * lot)
                buy_amount = buy_shares * px
            if buy_shares <= 0:
                continue

            fee = buy_amount * applied_fee
            if buy_amount + fee > cash + 1e-8:
                continue
            shares[code] = shares.get(code, 0.0) + buy_shares
            cash -= (buy_amount + fee)
            total_fee += fee
            turnover += buy_amount
            trades.append(
                {
                    "date": dt.strftime("%Y-%m-%d"),
                    "code": code,
                    "name": name_map.get(code, code),
                    "action": "buy",
                    "shares": round(buy_shares, 4),
                    "price": round(px, 4),
                    "amount": round(buy_amount, 2),
                    "fee": round(fee, 2),
                    "reason": reason,
                }
            )

    equity_series = pd.Series([v for _, v in equity_track], index=[d for d, _ in equity_track], dtype=float)
    if equity_series.empty or len(equity_series) < 2:
        raise ValueError("回测样本不足，无法计算")
    strategy_returns = equity_series.pct_change().fillna(0.0)
    benchmark_returns = _fetch_hs300_returns(start_ts, end_date=end_ts)
    if benchmark_returns.empty:
        benchmark_returns = pd.Series(dtype=float)
    else:
        if not isinstance(benchmark_returns.index, pd.DatetimeIndex):
            benchmark_returns.index = pd.to_datetime(benchmark_returns.index, errors="coerce")
            benchmark_returns = benchmark_returns[~benchmark_returns.index.isna()]
        if not benchmark_returns.empty:
            benchmark_returns = benchmark_returns[
                (benchmark_returns.index >= strategy_returns.index.min())
                & (benchmark_returns.index <= strategy_returns.index.max())
            ]

    strategy_metrics = _calculate_metrics(strategy_returns, benchmark_returns)
    strategy_periods = _calculate_period_returns((1 + strategy_returns).cumprod(), as_of=end_ts)

    strategy_curve = _to_return_points(strategy_returns)
    if benchmark_returns.empty:
        benchmark_for_chart = pd.Series(0.0, index=strategy_returns.index, dtype=float)
    else:
        benchmark_for_chart = benchmark_returns.reindex(strategy_returns.index).ffill().fillna(0.0)
    benchmark_curve = _to_return_points(benchmark_for_chart)
    if not benchmark_curve:
        benchmark_curve = [{"date": start_ts.strftime("%Y-%m-%d"), "return": 0.0}]

    final_value = float(equity_series.iloc[-1])
    result = {
        "portfolio": detail["portfolio"],
        "active_version": active_version,
        "params": {
            "start_date": start_ts.strftime("%Y-%m-%d"),
            "end_date": end_ts.strftime("%Y-%m-%d"),
            "initial_capital": round(float(initial_capital), 2),
            "rebalance_mode": mode,
            "threshold": threshold,
            "periodic_days": periodic_days,
            "fee_rate": applied_fee,
            "cache_only": bool(cache_only),
        },
        "capital": {
            "principal": round(float(initial_capital), 2),
            "market_value": round(final_value, 2),
            "profit": round(final_value - float(initial_capital), 2),
            "profit_rate": round((final_value / float(initial_capital) - 1.0), 6),
            "cash": round(float(cash), 2),
        },
        "period_returns": {"strategy": strategy_periods},
        "metrics": {"strategy": strategy_metrics},
        "series": {
            "strategy": strategy_curve,
            "benchmark": benchmark_curve,
        },
        "rebalance_summary": {
            "rebalance_count": rebalance_count,
            "trade_count": len(trades),
            "turnover": round(float(turnover), 2),
            "fee_total": round(float(total_fee), 2),
        },
        "trades": trades,
    }

    _save_backtest_cache(
        portfolio_id,
        account_id,
        active_version["id"] if active_version else None,
        query_hash,
        data_tag,
        result,
    )
    return result


def get_performance(portfolio_id: int, account_id: int) -> Dict[str, Any]:
    cache_key = (portfolio_id, account_id)
    now = time.time()
    cached = _PERF_CACHE.get(cache_key)
    if cached and now - cached[0] <= PERF_CACHE_TTL_SECONDS:
        return cached[1]

    detail = get_portfolio_detail(portfolio_id)
    holdings = detail.get("active_holdings", [])
    strategy_weights = {h["code"]: float(h["weight"]) for h in holdings}

    active_version = detail.get("active_version")
    start_date = pd.Timestamp(_to_date(active_version.get("effective_date") if active_version else None))

    strategy_returns = _build_portfolio_return_series(strategy_weights, start_date)
    benchmark_returns = _fetch_hs300_returns(start_date)

    strategy_metrics = _calculate_metrics(strategy_returns, benchmark_returns)
    strategy_periods = _calculate_period_returns((1 + strategy_returns).cumprod()) if not strategy_returns.empty else {}

    linked_scope = set(detail["portfolio"].get("scope_codes") or [])
    scope = sorted(linked_scope | set(strategy_weights.keys()))
    positions = _fetch_account_positions(account_id, scope if scope else None)

    value_map = {}
    principal = 0.0
    for p in positions:
        price, _, _ = _get_price_and_name(p["code"])
        if price and price > 0:
            value_map[p["code"]] = p["shares"] * price
            principal += p["shares"] * p["cost"]

    market_value = sum(value_map.values())
    profit = market_value - principal

    actual_weights = {code: v / market_value for code, v in value_map.items()} if market_value > 0 else {}
    actual_returns = _build_portfolio_return_series(actual_weights, start_date)
    actual_metrics = _calculate_metrics(actual_returns, benchmark_returns)
    actual_periods = _calculate_period_returns((1 + actual_returns).cumprod()) if not actual_returns.empty else {}

    strategy_curve = _to_return_points(strategy_returns)
    benchmark_curve = _to_return_points(benchmark_returns)
    if not strategy_curve:
        strategy_curve = [{"date": start_date.strftime("%Y-%m-%d"), "return": 0.0}]
    if not benchmark_curve:
        benchmark_curve = [{"date": start_date.strftime("%Y-%m-%d"), "return": 0.0}]

    result = {
        "portfolio": detail["portfolio"],
        "active_version": detail.get("active_version"),
        "target_holdings": holdings,
        "scope_codes": sorted(linked_scope),
        "calculation_universe": scope,
        "capital": {
            "principal": round(principal, 2),
            "market_value": round(market_value, 2),
            "profit": round(profit, 2),
            "profit_rate": round(profit / principal, 6) if principal > 0 else None,
        },
        "period_returns": {
            "strategy": strategy_periods,
            "actual": actual_periods,
        },
        "metrics": {
            "strategy": strategy_metrics,
            "actual": actual_metrics,
        },
        "intraday_est_return": {
            "strategy": _estimate_intraday_rate(strategy_weights),
            "actual": _estimate_intraday_rate(actual_weights),
        },
        "series": {
            "strategy": strategy_curve,
            "benchmark": benchmark_curve,
        },
    }
    _PERF_CACHE[cache_key] = (now, result)
    return result
