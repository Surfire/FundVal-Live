import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import akshare as ak
import numpy as np
import pandas as pd

from ..db import get_db_connection
from .fund import get_combined_valuation, get_fund_history

logger = logging.getLogger(__name__)

RISK_FREE_RATE = 0.02
TRADING_DAYS_PER_YEAR = 252


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
        code = str(item.get("code", "")).strip()
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
    out = sorted({str(c).strip() for c in codes if str(c).strip()})
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


def _get_price_and_name(code: str) -> Tuple[Optional[float], str, float]:
    data = get_combined_valuation(code) or {}
    name = data.get("name") or code

    estimate = float(data.get("estimate", 0.0) or 0.0)
    nav = float(data.get("nav", 0.0) or 0.0)
    est_rate = float(data.get("estRate", 0.0) or 0.0)

    price = estimate if estimate > 0 else nav
    if price > 0:
        return price, name, est_rate

    history = get_fund_history(code, limit=1)
    if history:
        return float(history[-1]["nav"]), name, 0.0

    return None, name, 0.0


def _to_date(d: Optional[str]) -> date:
    if not d:
        return date.today()
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return date.today()


def _fetch_account_positions(account_id: int, code_scope: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()

    if code_scope:
        placeholders = ",".join(["?"] * len(code_scope))
        cursor.execute(
            f"""
            SELECT code, shares, cost
            FROM positions
            WHERE account_id = ? AND shares > 0 AND code IN ({placeholders})
            """,
            [account_id, *code_scope],
        )
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
    return [{"code": row["code"], "shares": float(row["shares"]), "cost": float(row["cost"])} for row in rows]


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
    return [{"code": row["fund_code"], "weight": float(row["target_weight"])} for row in rows]


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


def generate_rebalance_orders(
    portfolio_id: int,
    account_id: int,
    min_deviation: float = 0.005,
    fee_rate: Optional[float] = None,
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
        t_value = total_value * t_weight
        t_shares = t_value / price if price > 0 else 0.0

        delta_shares = t_shares - c_shares
        delta_value = t_value - c_value
        deviation = c_weight - t_weight

        action = "hold"
        if abs(deviation) >= min_deviation and abs(delta_shares) >= 0.01:
            action = "buy" if delta_shares > 0 else "sell"

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
                "delta_value": round(delta_value, 2),
                "price": round(price, 4),
                "trade_amount": round(trade_amount, 2),
                "fee": round(fee, 2),
            }
        )

    orders.sort(key=lambda x: abs(x["delta_shares"]), reverse=True)

    if persist:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                DELETE FROM rebalance_orders
                WHERE portfolio_id = ? AND account_id = ? AND status = 'suggested'
                """,
                (portfolio_id, account_id),
            )

            cursor.executemany(
                """
                INSERT INTO rebalance_orders (
                    portfolio_id, version_id, account_id, fund_code, fund_name,
                    action, target_weight, current_weight, target_shares,
                    current_shares, delta_shares, price, trade_amount, fee, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'suggested')
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
                    )
                    for item in orders
                ],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    actionable = [o for o in orders if o["action"] != "hold"]
    return {
        "orders": orders,
        "summary": {
            "total_assets": round(total_value, 2),
            "actionable_count": len(actionable),
            "total_buy": round(total_buy, 2),
            "total_sell": round(total_sell, 2),
            "estimated_fee": round(total_fee, 2),
            "fee_rate": applied_fee_rate,
            "scope_size": len(all_codes),
        },
    }


def list_rebalance_orders(portfolio_id: int, account_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()

    if status:
        cursor.execute(
            """
            SELECT id, portfolio_id, version_id, account_id, fund_code, fund_name,
                   action, target_weight, current_weight, target_shares,
                   current_shares, delta_shares, price, trade_amount, fee,
                   status, created_at, executed_at
            FROM rebalance_orders
            WHERE portfolio_id = ? AND account_id = ? AND status = ?
            ORDER BY id DESC
            """,
            (portfolio_id, account_id, status),
        )
    else:
        cursor.execute(
            """
            SELECT id, portfolio_id, version_id, account_id, fund_code, fund_name,
                   action, target_weight, current_weight, target_shares,
                   current_shares, delta_shares, price, trade_amount, fee,
                   status, created_at, executed_at
            FROM rebalance_orders
            WHERE portfolio_id = ? AND account_id = ?
            ORDER BY id DESC
            """,
            (portfolio_id, account_id),
        )

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def update_rebalance_order_status(order_id: int, status: str) -> Dict[str, Any]:
    if status not in {"suggested", "executed", "skipped"}:
        raise ValueError("invalid status")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM rebalance_orders WHERE id = ?", (order_id,))
    if not cursor.fetchone():
        conn.close()
        raise ValueError("order not found")

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
    return {"ok": True}


def _get_cached_history_series(code: str, start_date: pd.Timestamp) -> pd.Series:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT date, nav
        FROM fund_history
        WHERE code = ? AND date >= ?
        ORDER BY date ASC
        """,
        (code, start_date.strftime("%Y-%m-%d")),
    )
    rows = cursor.fetchall()
    conn.close()

    if len(rows) >= 30:
        return pd.Series(
            [float(r["nav"]) for r in rows],
            index=pd.to_datetime([r["date"] for r in rows]),
            dtype=float,
        )

    history = get_fund_history(code, limit=1500)
    if not history:
        return pd.Series(dtype=float)

    filtered = [h for h in history if h["date"] >= start_date.strftime("%Y-%m-%d")]
    if not filtered:
        return pd.Series(dtype=float)

    return pd.Series(
        [float(h["nav"]) for h in filtered],
        index=pd.to_datetime([h["date"] for h in filtered]),
        dtype=float,
    )


def _build_portfolio_return_series(weight_map: Dict[str, float], start_date: pd.Timestamp) -> pd.Series:
    if not weight_map:
        return pd.Series(dtype=float)

    nav_frames = {}
    for code, _ in weight_map.items():
        series = _get_cached_history_series(code, start_date)
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


def _fetch_hs300_returns(start_date: pd.Timestamp) -> pd.Series:
    start = start_date.strftime("%Y%m%d")
    end = datetime.now().strftime("%Y%m%d")

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
            return s.pct_change().fillna(0.0)
    except Exception as e:
        logger.warning(f"Failed to fetch HS300 by stock_zh_index_daily_em: {e}")

    return pd.Series(dtype=float)


def _calculate_period_returns(nav: pd.Series) -> Dict[str, Optional[float]]:
    if nav.empty:
        return {"week": None, "month": None, "quarter": None, "year": None, "ytd": None}

    latest_value = float(nav.iloc[-1])
    now = pd.Timestamp.now().normalize()

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


def get_performance(portfolio_id: int, account_id: int) -> Dict[str, Any]:
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

    return {
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
