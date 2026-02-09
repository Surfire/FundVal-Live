import datetime
import logging
from typing import List, Dict, Any, Optional
from ..db import get_db_connection

logger = logging.getLogger(__name__)

# Import order based on dependencies
IMPORT_ORDER = ["settings", "ai_prompts", "accounts", "positions", "transactions", "subscriptions", "strategy"]

# Sensitive fields that should be masked on export
SENSITIVE_MASK = "***"


def export_data(modules: List[str]) -> Dict[str, Any]:
    """
    Export selected modules to JSON format.

    Args:
        modules: List of module names to export

    Returns:
        Dict containing version, exported_at, metadata, and module data
    """
    if not modules:
        raise ValueError("No modules selected for export")

    result = {
        "version": "1.0",
        "exported_at": datetime.datetime.utcnow().isoformat() + "Z",
        "metadata": {},
        "modules": {}
    }

    # Export each module
    for module in modules:
        if module == "settings":
            result["modules"]["settings"] = _export_settings()
            result["metadata"]["total_settings"] = len(result["modules"]["settings"])
        elif module == "ai_prompts":
            result["modules"]["ai_prompts"] = _export_ai_prompts()
            result["metadata"]["total_ai_prompts"] = len(result["modules"]["ai_prompts"])
        elif module == "accounts":
            result["modules"]["accounts"] = _export_accounts()
            result["metadata"]["total_accounts"] = len(result["modules"]["accounts"])
        elif module == "positions":
            result["modules"]["positions"] = _export_positions()
            result["metadata"]["total_positions"] = len(result["modules"]["positions"])
        elif module == "transactions":
            result["modules"]["transactions"] = _export_transactions()
            result["metadata"]["total_transactions"] = len(result["modules"]["transactions"])
        elif module == "subscriptions":
            result["modules"]["subscriptions"] = _export_subscriptions()
            result["metadata"]["total_subscriptions"] = len(result["modules"]["subscriptions"])
        elif module == "strategy":
            result["modules"]["strategy"] = _export_strategy()
            result["metadata"]["total_strategy_portfolios"] = len(result["modules"]["strategy"].get("portfolios", []))

    return result


def import_data(data: Dict[str, Any], modules: List[str], mode: str) -> Dict[str, Any]:
    """
    Import selected modules from JSON data.

    Args:
        data: JSON data containing modules
        modules: List of module names to import
        mode: "merge" or "replace"

    Returns:
        Dict containing import results
    """
    if not modules:
        raise ValueError("No modules selected for import")

    if "version" not in data:
        raise ValueError("Missing version field in import data")

    # Initialize result
    result = {
        "success": True,
        "total_records": 0,
        "imported": 0,
        "skipped": 0,
        "failed": 0,
        "deleted": 0,
        "details": {}
    }

    conn = get_db_connection()
    try:
        # Import modules in dependency order
        ordered_modules = [m for m in IMPORT_ORDER if m in modules]

        for module in ordered_modules:
            if module not in data.get("modules", {}):
                continue

            module_data = data["modules"][module]

            # Skip empty modules
            if not module_data:
                continue

            # Import module
            if module == "settings":
                module_result = _import_settings(conn, module_data, mode)
            elif module == "ai_prompts":
                module_result = _import_ai_prompts(conn, module_data, mode)
            elif module == "accounts":
                module_result = _import_accounts(conn, module_data, mode)
            elif module == "positions":
                module_result = _import_positions(conn, module_data, mode)
            elif module == "transactions":
                module_result = _import_transactions(conn, module_data, mode)
            elif module == "subscriptions":
                module_result = _import_subscriptions(conn, module_data, mode)
            elif module == "strategy":
                module_result = _import_strategy(conn, module_data, mode)
            else:
                continue

            # Aggregate results
            result["details"][module] = module_result
            result["total_records"] += module_result.get("total", 0)
            result["imported"] += module_result.get("imported", 0)
            result["skipped"] += module_result.get("skipped", 0)
            result["failed"] += module_result.get("failed", 0)
            result["deleted"] += module_result.get("deleted", 0)

        conn.commit()

    except Exception as e:
        conn.rollback()
        result["success"] = False
        result["error"] = str(e)
        logger.error(f"Import failed: {e}")
        raise
    finally:
        conn.close()

    return result


# Export functions

def _export_settings() -> Dict[str, str]:
    """Export settings (mask sensitive fields)"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT key, value, encrypted FROM settings")

        settings = {}
        for row in cursor.fetchall():
            key = row["key"]
            value = row["value"]
            encrypted = row["encrypted"]

            # Mask sensitive fields
            if encrypted:
                settings[key] = SENSITIVE_MASK
            else:
                settings[key] = value

        return settings
    finally:
        conn.close()


def _export_ai_prompts() -> List[Dict[str, Any]]:
    """Export AI prompts"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name, system_prompt, user_prompt, is_default, created_at, updated_at
            FROM ai_prompts
            ORDER BY id
        """)

        prompts = []
        for row in cursor.fetchall():
            prompts.append({
                "name": row["name"],
                "system_prompt": row["system_prompt"],
                "user_prompt": row["user_prompt"],
                "is_default": bool(row["is_default"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            })

        return prompts
    finally:
        conn.close()


def _export_accounts() -> List[Dict[str, Any]]:
    """Export accounts"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, description, created_at, updated_at
            FROM accounts
            ORDER BY id
        """)

        accounts = []
        for row in cursor.fetchall():
            accounts.append({
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            })

        return accounts
    finally:
        conn.close()


def _export_positions() -> List[Dict[str, Any]]:
    """Export positions"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT account_id, code, cost, shares, updated_at
            FROM positions
            ORDER BY account_id, code
        """)

        positions = []
        for row in cursor.fetchall():
            positions.append({
                "account_id": row["account_id"],
                "code": row["code"],
                "cost": row["cost"],
                "shares": row["shares"],
                "updated_at": row["updated_at"]
            })

        return positions
    finally:
        conn.close()


def _export_transactions() -> List[Dict[str, Any]]:
    """Export transactions"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, account_id, code, op_type, amount_cny, shares_redeemed,
                   confirm_date, confirm_nav, shares_added, cost_after,
                   created_at, applied_at
            FROM transactions
            ORDER BY id
        """)

        transactions = []
        for row in cursor.fetchall():
            transactions.append({
                "id": row["id"],
                "account_id": row["account_id"],
                "code": row["code"],
                "op_type": row["op_type"],
                "amount_cny": row["amount_cny"],
                "shares_redeemed": row["shares_redeemed"],
                "confirm_date": row["confirm_date"],
                "confirm_nav": row["confirm_nav"],
                "shares_added": row["shares_added"],
                "cost_after": row["cost_after"],
                "created_at": row["created_at"],
                "applied_at": row["applied_at"]
            })

        return transactions
    finally:
        conn.close()


def _export_subscriptions() -> List[Dict[str, Any]]:
    """Export subscriptions"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, code, email, threshold_up, threshold_down,
                   enable_digest, digest_time, enable_volatility,
                   last_notified_at, last_digest_at, created_at
            FROM subscriptions
            ORDER BY id
        """)

        subscriptions = []
        for row in cursor.fetchall():
            subscriptions.append({
                "id": row["id"],
                "code": row["code"],
                "email": row["email"],
                "threshold_up": row["threshold_up"],
                "threshold_down": row["threshold_down"],
                "enable_digest": bool(row["enable_digest"]),
                "digest_time": row["digest_time"],
                "enable_volatility": bool(row["enable_volatility"]),
                "last_notified_at": row["last_notified_at"],
                "last_digest_at": row["last_digest_at"],
                "created_at": row["created_at"]
            })

        return subscriptions
    finally:
        conn.close()


def _export_strategy() -> Dict[str, Any]:
    """Export strategy-related data."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, name, account_id, benchmark, fee_rate, scope_codes, created_at, updated_at
            FROM strategy_portfolios
            ORDER BY id
            """
        )
        portfolios = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT id, portfolio_id, version_no, effective_date, note, is_active, created_at
            FROM strategy_versions
            ORDER BY id
            """
        )
        versions = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT id, version_id, fund_code, target_weight, created_at
            FROM strategy_holdings
            ORDER BY id
            """
        )
        holdings = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT id, portfolio_id, account_id, version_id, source, status, title, note, created_at, completed_at
            FROM rebalance_batches
            ORDER BY id
            """
        )
        batches = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT id, portfolio_id, version_id, account_id, fund_code, fund_name, action,
                   target_weight, current_weight, target_shares, current_shares, delta_shares,
                   price, trade_amount, fee, status, created_at, executed_at, executed_price,
                   executed_shares, executed_amount, execution_note, batch_id
            FROM rebalance_orders
            ORDER BY id
            """
        )
        orders = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT id, portfolio_id, account_id, version_id, query_hash, data_tag, result_json, created_at
            FROM strategy_backtest_cache
            ORDER BY id
            """
        )
        backtest_cache = [dict(row) for row in cursor.fetchall()]

        return {
            "portfolios": portfolios,
            "versions": versions,
            "holdings": holdings,
            "rebalance_batches": batches,
            "rebalance_orders": orders,
            "backtest_cache": backtest_cache,
        }
    finally:
        conn.close()


# Import functions (to be continued in next chunk)


# Import functions

def _import_settings(conn, data: Dict[str, str], mode: str) -> Dict[str, Any]:
    """Import settings (always merge mode, skip *** values)"""
    cursor = conn.cursor()
    result = {"total": len(data), "imported": 0, "skipped": 0, "failed": 0, "deleted": 0, "errors": []}

    # Settings always use merge mode
    for key, value in data.items():
        # Skip masked sensitive fields
        if value == SENSITIVE_MASK:
            result["skipped"] += 1
            continue

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO settings (key, value)
                VALUES (?, ?)
            """, (key, value))
            result["imported"] += 1
        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"Failed to import setting {key}: {str(e)}")
            logger.error(f"Failed to import setting {key}: {e}")

    return result


def _import_ai_prompts(conn, data: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    """Import AI prompts"""
    cursor = conn.cursor()
    result = {"total": len(data), "imported": 0, "skipped": 0, "failed": 0, "deleted": 0, "errors": []}

    # Replace mode: delete all existing prompts
    if mode == "replace":
        cursor.execute("DELETE FROM ai_prompts")
        deleted_count = cursor.rowcount
        result["deleted"] = deleted_count

    for prompt in data:
        try:
            name = prompt.get("name")
            if not name:
                result["skipped"] += 1
                result["errors"].append("Missing name field")
                continue

            # Check if prompt with same name exists (merge mode)
            if mode == "merge":
                cursor.execute("SELECT id FROM ai_prompts WHERE name = ?", (name,))
                if cursor.fetchone():
                    result["skipped"] += 1
                    continue

            cursor.execute("""
                INSERT INTO ai_prompts (name, system_prompt, user_prompt, is_default)
                VALUES (?, ?, ?, ?)
            """, (
                name,
                prompt.get("system_prompt", ""),
                prompt.get("user_prompt", ""),
                1 if prompt.get("is_default") else 0
            ))
            result["imported"] += 1

        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"Failed to import prompt {prompt.get('name')}: {str(e)}")
            logger.error(f"Failed to import prompt: {e}")

    return result


def _import_accounts(conn, data: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    """Import accounts"""
    cursor = conn.cursor()
    result = {"total": len(data), "imported": 0, "skipped": 0, "failed": 0, "deleted": 0, "errors": []}

    # Replace mode: delete all existing accounts
    if mode == "replace":
        cursor.execute("DELETE FROM accounts")
        deleted_count = cursor.rowcount
        result["deleted"] = deleted_count

    for account in data:
        try:
            name = account.get("name")
            if not name:
                result["skipped"] += 1
                result["errors"].append("Missing name field")
                continue

            # Check if account with same name exists (merge mode)
            if mode == "merge":
                cursor.execute("SELECT id FROM accounts WHERE name = ?", (name,))
                if cursor.fetchone():
                    result["skipped"] += 1
                    continue

            cursor.execute("""
                INSERT INTO accounts (name, description)
                VALUES (?, ?)
            """, (name, account.get("description", "")))
            result["imported"] += 1

        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"Failed to import account {account.get('name')}: {str(e)}")
            logger.error(f"Failed to import account: {e}")

    return result


def _import_positions(conn, data: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    """Import positions"""
    cursor = conn.cursor()
    result = {"total": len(data), "imported": 0, "skipped": 0, "failed": 0, "deleted": 0, "errors": []}

    # Replace mode: delete all existing positions
    if mode == "replace":
        cursor.execute("DELETE FROM positions")
        deleted_count = cursor.rowcount
        result["deleted"] = deleted_count

    for position in data:
        try:
            account_id = position.get("account_id")
            code = position.get("code")

            if not account_id or not code:
                result["skipped"] += 1
                result["errors"].append("Missing account_id or code field")
                continue

            # Check if account exists
            cursor.execute("SELECT id FROM accounts WHERE id = ?", (account_id,))
            if not cursor.fetchone():
                result["skipped"] += 1
                result["errors"].append(f"account_id={account_id} does not exist")
                continue

            # Check if position exists (merge mode)
            if mode == "merge":
                cursor.execute("SELECT 1 FROM positions WHERE account_id = ? AND code = ?", (account_id, code))
                if cursor.fetchone():
                    result["skipped"] += 1
                    continue

            cursor.execute("""
                INSERT INTO positions (account_id, code, cost, shares)
                VALUES (?, ?, ?, ?)
            """, (account_id, code, position.get("cost", 0.0), position.get("shares", 0.0)))
            result["imported"] += 1

        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"Failed to import position: {str(e)}")
            logger.error(f"Failed to import position: {e}")

    return result


def _import_transactions(conn, data: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    """Import transactions"""
    cursor = conn.cursor()
    result = {"total": len(data), "imported": 0, "skipped": 0, "failed": 0, "deleted": 0, "errors": []}

    # Replace mode: delete all existing transactions
    if mode == "replace":
        cursor.execute("DELETE FROM transactions")
        deleted_count = cursor.rowcount
        result["deleted"] = deleted_count

    for transaction in data:
        try:
            account_id = transaction.get("account_id")
            code = transaction.get("code")

            if not account_id or not code:
                result["skipped"] += 1
                result["errors"].append("Missing account_id or code field")
                continue

            # Check if account exists
            cursor.execute("SELECT id FROM accounts WHERE id = ?", (account_id,))
            if not cursor.fetchone():
                result["skipped"] += 1
                result["errors"].append(f"account_id={account_id} does not exist")
                continue

            cursor.execute("""
                INSERT INTO transactions (
                    account_id, code, op_type, amount_cny, shares_redeemed,
                    confirm_date, confirm_nav, shares_added, cost_after, applied_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                account_id,
                code,
                transaction.get("op_type"),
                transaction.get("amount_cny"),
                transaction.get("shares_redeemed"),
                transaction.get("confirm_date"),
                transaction.get("confirm_nav"),
                transaction.get("shares_added"),
                transaction.get("cost_after"),
                transaction.get("applied_at")
            ))
            result["imported"] += 1

        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"Failed to import transaction: {str(e)}")
            logger.error(f"Failed to import transaction: {e}")

    return result


def _import_subscriptions(conn, data: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    """Import subscriptions"""
    cursor = conn.cursor()
    result = {"total": len(data), "imported": 0, "skipped": 0, "failed": 0, "deleted": 0, "errors": []}

    # Replace mode: delete all existing subscriptions
    if mode == "replace":
        cursor.execute("DELETE FROM subscriptions")
        deleted_count = cursor.rowcount
        result["deleted"] = deleted_count

    for subscription in data:
        try:
            code = subscription.get("code")
            email = subscription.get("email")

            if not code or not email:
                result["skipped"] += 1
                result["errors"].append("Missing code or email field")
                continue

            # Check if subscription exists (merge mode)
            if mode == "merge":
                cursor.execute("SELECT id FROM subscriptions WHERE code = ? AND email = ?", (code, email))
                if cursor.fetchone():
                    result["skipped"] += 1
                    continue

            cursor.execute("""
                INSERT INTO subscriptions (
                    code, email, threshold_up, threshold_down,
                    enable_digest, digest_time, enable_volatility
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                code,
                email,
                subscription.get("threshold_up"),
                subscription.get("threshold_down"),
                1 if subscription.get("enable_digest") else 0,
                subscription.get("digest_time", "14:45"),
                1 if subscription.get("enable_volatility") else 0
            ))
            result["imported"] += 1

        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"Failed to import subscription: {str(e)}")
            logger.error(f"Failed to import subscription: {e}")

    return result


def _import_strategy(conn, data: Dict[str, Any], mode: str) -> Dict[str, Any]:
    """Import strategy-related data."""
    cursor = conn.cursor()
    portfolios = data.get("portfolios") or []
    versions = data.get("versions") or []
    holdings = data.get("holdings") or []
    batches = data.get("rebalance_batches") or []
    orders = data.get("rebalance_orders") or []
    backtest_cache = data.get("backtest_cache") or []

    total = len(portfolios) + len(versions) + len(holdings) + len(batches) + len(orders) + len(backtest_cache)
    result = {"total": total, "imported": 0, "skipped": 0, "failed": 0, "deleted": 0, "errors": []}

    if mode == "replace":
        for table in [
            "strategy_backtest_cache",
            "rebalance_orders",
            "rebalance_batches",
            "strategy_holdings",
            "strategy_versions",
            "strategy_portfolios",
        ]:
            cursor.execute(f"DELETE FROM {table}")
            result["deleted"] += int(cursor.rowcount or 0)

    def _upsert_portfolio(p: Dict[str, Any]):
        cursor.execute(
            """
            INSERT INTO strategy_portfolios (id, name, account_id, benchmark, fee_rate, scope_codes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              account_id=excluded.account_id,
              benchmark=excluded.benchmark,
              fee_rate=excluded.fee_rate,
              scope_codes=excluded.scope_codes,
              updated_at=excluded.updated_at
            """,
            (
                p.get("id"),
                p.get("name", ""),
                p.get("account_id"),
                p.get("benchmark", "000300"),
                p.get("fee_rate", 0.001),
                p.get("scope_codes", "[]"),
                p.get("created_at"),
                p.get("updated_at"),
            ),
        )

    def _upsert_version(v: Dict[str, Any]):
        cursor.execute(
            """
            INSERT INTO strategy_versions (id, portfolio_id, version_no, effective_date, note, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT(id) DO UPDATE SET
              portfolio_id=excluded.portfolio_id,
              version_no=excluded.version_no,
              effective_date=excluded.effective_date,
              note=excluded.note,
              is_active=excluded.is_active
            """,
            (
                v.get("id"),
                v.get("portfolio_id"),
                v.get("version_no"),
                v.get("effective_date"),
                v.get("note"),
                v.get("is_active", 1),
                v.get("created_at"),
            ),
        )

    def _upsert_holding(h: Dict[str, Any]):
        cursor.execute(
            """
            INSERT INTO strategy_holdings (id, version_id, fund_code, target_weight, created_at)
            VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT(id) DO UPDATE SET
              version_id=excluded.version_id,
              fund_code=excluded.fund_code,
              target_weight=excluded.target_weight
            """,
            (
                h.get("id"),
                h.get("version_id"),
                h.get("fund_code"),
                h.get("target_weight"),
                h.get("created_at"),
            ),
        )

    def _upsert_batch(b: Dict[str, Any]):
        cursor.execute(
            """
            INSERT INTO rebalance_batches (id, portfolio_id, account_id, version_id, source, status, title, note, created_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
            ON CONFLICT(id) DO UPDATE SET
              portfolio_id=excluded.portfolio_id,
              account_id=excluded.account_id,
              version_id=excluded.version_id,
              source=excluded.source,
              status=excluded.status,
              title=excluded.title,
              note=excluded.note,
              completed_at=excluded.completed_at
            """,
            (
                b.get("id"),
                b.get("portfolio_id"),
                b.get("account_id"),
                b.get("version_id"),
                b.get("source", "auto"),
                b.get("status", "pending"),
                b.get("title"),
                b.get("note"),
                b.get("created_at"),
                b.get("completed_at"),
            ),
        )

    def _upsert_order(o: Dict[str, Any]):
        cursor.execute(
            """
            INSERT INTO rebalance_orders (
              id, portfolio_id, version_id, account_id, fund_code, fund_name, action,
              target_weight, current_weight, target_shares, current_shares, delta_shares,
              price, trade_amount, fee, status, created_at, executed_at, executed_price,
              executed_shares, executed_amount, execution_note, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              portfolio_id=excluded.portfolio_id,
              version_id=excluded.version_id,
              account_id=excluded.account_id,
              fund_code=excluded.fund_code,
              fund_name=excluded.fund_name,
              action=excluded.action,
              target_weight=excluded.target_weight,
              current_weight=excluded.current_weight,
              target_shares=excluded.target_shares,
              current_shares=excluded.current_shares,
              delta_shares=excluded.delta_shares,
              price=excluded.price,
              trade_amount=excluded.trade_amount,
              fee=excluded.fee,
              status=excluded.status,
              executed_at=excluded.executed_at,
              executed_price=excluded.executed_price,
              executed_shares=excluded.executed_shares,
              executed_amount=excluded.executed_amount,
              execution_note=excluded.execution_note,
              batch_id=excluded.batch_id
            """,
            (
                o.get("id"),
                o.get("portfolio_id"),
                o.get("version_id"),
                o.get("account_id"),
                o.get("fund_code"),
                o.get("fund_name"),
                o.get("action"),
                o.get("target_weight"),
                o.get("current_weight"),
                o.get("target_shares"),
                o.get("current_shares"),
                o.get("delta_shares"),
                o.get("price"),
                o.get("trade_amount"),
                o.get("fee"),
                o.get("status", "suggested"),
                o.get("created_at"),
                o.get("executed_at"),
                o.get("executed_price"),
                o.get("executed_shares"),
                o.get("executed_amount"),
                o.get("execution_note"),
                o.get("batch_id"),
            ),
        )

    def _upsert_backtest_cache(c: Dict[str, Any]):
        cursor.execute(
            """
            INSERT INTO strategy_backtest_cache (
              id, portfolio_id, account_id, version_id, query_hash, data_tag, result_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT(id) DO UPDATE SET
              portfolio_id=excluded.portfolio_id,
              account_id=excluded.account_id,
              version_id=excluded.version_id,
              query_hash=excluded.query_hash,
              data_tag=excluded.data_tag,
              result_json=excluded.result_json
            """,
            (
                c.get("id"),
                c.get("portfolio_id"),
                c.get("account_id"),
                c.get("version_id"),
                c.get("query_hash"),
                c.get("data_tag"),
                c.get("result_json"),
                c.get("created_at"),
            ),
        )

    for row in portfolios:
        try:
            _upsert_portfolio(row)
            result["imported"] += 1
        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"strategy_portfolio import failed: {e}")
    for row in versions:
        try:
            _upsert_version(row)
            result["imported"] += 1
        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"strategy_version import failed: {e}")
    for row in holdings:
        try:
            _upsert_holding(row)
            result["imported"] += 1
        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"strategy_holding import failed: {e}")
    for row in batches:
        try:
            _upsert_batch(row)
            result["imported"] += 1
        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"rebalance_batch import failed: {e}")
    for row in orders:
        try:
            _upsert_order(row)
            result["imported"] += 1
        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"rebalance_order import failed: {e}")
    for row in backtest_cache:
        try:
            _upsert_backtest_cache(row)
            result["imported"] += 1
        except Exception as e:
            result["failed"] += 1
            result["errors"].append(f"backtest_cache import failed: {e}")

    return result
