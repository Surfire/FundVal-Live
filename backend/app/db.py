import sqlite3
import logging
import os
from pathlib import Path
from .config import Config

logger = logging.getLogger(__name__)

def get_db_connection():
    # 确保数据库目录存在
    db_dir = Path(Config.DB_PATH).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    """Initialize the database schema with migration support."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check database version
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("SELECT MAX(version) FROM schema_version")
    current_version = cursor.fetchone()[0] or 0

    logger.info(f"Current database schema version: {current_version}")

    # Funds table - simplistic design, exactly what we need
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS funds (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create an index for searching names, it's cheap and speeds up "LIKE" queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_funds_name ON funds(name);
    """)

    # Positions table - store user holdings
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            code TEXT PRIMARY KEY,
            cost REAL NOT NULL DEFAULT 0.0,
            shares REAL NOT NULL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Subscriptions table - store email alert settings
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            email TEXT NOT NULL,
            threshold_up REAL,
            threshold_down REAL,
            enable_digest INTEGER DEFAULT 0,
            digest_time TEXT DEFAULT '14:45',
            enable_volatility INTEGER DEFAULT 1,
            last_notified_at TIMESTAMP,
            last_digest_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, email)
        )
    """)

    # Settings table - store user configuration (for client/desktop)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            encrypted INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 初始化默认配置（如果不存在）
    default_settings = [
        ('OPENAI_API_KEY', '', 1),
        ('OPENAI_API_BASE', 'https://api.openai.com/v1', 0),
        ('AI_MODEL_NAME', 'gpt-3.5-turbo', 0),
        ('SMTP_HOST', 'smtp.gmail.com', 0),
        ('SMTP_PORT', '587', 0),
        ('SMTP_USER', '', 0),
        ('SMTP_PASSWORD', '', 1),
        ('EMAIL_FROM', 'noreply@fundval.live', 0),
        ('INTRADAY_COLLECT_INTERVAL', '5', 0),  # 分时数据采集间隔（分钟）
    ]

    cursor.executemany("""
        INSERT OR IGNORE INTO settings (key, value, encrypted) VALUES (?, ?, ?)
    """, default_settings)

    # Transactions table - add/reduce position log (T+1 confirm by real NAV)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            op_type TEXT NOT NULL,
            amount_cny REAL,
            shares_redeemed REAL,
            confirm_date TEXT NOT NULL,
            confirm_nav REAL,
            shares_added REAL,
            cost_after REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            applied_at TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_code ON transactions(code);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_confirm_date ON transactions(confirm_date);")

    # Fund history table - cache historical NAV data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fund_history (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            nav REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, date)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fund_history_code ON fund_history(code);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fund_history_date ON fund_history(date);")

    # Intraday snapshots table - store intraday valuation data for charts
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fund_intraday_snapshots (
            fund_code TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            estimate REAL NOT NULL,
            PRIMARY KEY (fund_code, date, time)
        )
    """)

    # Migration: Drop old incompatible tables
    if current_version < 1:
        logger.info("Running migration: dropping old incompatible tables")
        cursor.execute("DROP TABLE IF EXISTS valuation_accuracy")
        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (1)")

    # Migration: Multi-account support
    if current_version < 2:
        logger.info("Running migration: adding multi-account support")

        # 1. Create accounts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. Insert default account
        cursor.execute("""
            INSERT OR IGNORE INTO accounts (id, name, description)
            VALUES (1, '默认账户', '系统默认账户')
        """)

        # 3. Check if positions table needs migration
        cursor.execute("PRAGMA table_info(positions)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'account_id' not in columns:
            logger.info("Migrating positions table to multi-account")

            # Backup old data
            cursor.execute("SELECT code, cost, shares, updated_at FROM positions")
            old_positions = cursor.fetchall()

            # Drop old table
            cursor.execute("DROP TABLE positions")

            # Create new table with account_id
            cursor.execute("""
                CREATE TABLE positions (
                    account_id INTEGER NOT NULL DEFAULT 1,
                    code TEXT NOT NULL,
                    cost REAL NOT NULL DEFAULT 0.0,
                    shares REAL NOT NULL DEFAULT 0.0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (account_id, code),
                    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
                )
            """)

            # Restore data with default account_id = 1
            for row in old_positions:
                cursor.execute("""
                    INSERT INTO positions (account_id, code, cost, shares, updated_at)
                    VALUES (1, ?, ?, ?, ?)
                """, row)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_account ON positions(account_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_code ON positions(code)")

        # 4. Check if transactions table needs migration
        cursor.execute("PRAGMA table_info(transactions)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'account_id' not in columns:
            logger.info("Migrating transactions table to multi-account")

            # Backup old data
            cursor.execute("""
                SELECT id, code, op_type, amount_cny, shares_redeemed,
                       confirm_date, confirm_nav, shares_added, cost_after,
                       created_at, applied_at
                FROM transactions
            """)
            old_transactions = cursor.fetchall()

            # Drop old table
            cursor.execute("DROP TABLE transactions")

            # Create new table with account_id
            cursor.execute("""
                CREATE TABLE transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL DEFAULT 1,
                    code TEXT NOT NULL,
                    op_type TEXT NOT NULL,
                    amount_cny REAL,
                    shares_redeemed REAL,
                    confirm_date TEXT NOT NULL,
                    confirm_nav REAL,
                    shares_added REAL,
                    cost_after REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    applied_at TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
                )
            """)

            # Restore data with default account_id = 1
            for row in old_transactions:
                cursor.execute("""
                    INSERT INTO transactions
                    (id, account_id, code, op_type, amount_cny, shares_redeemed,
                     confirm_date, confirm_nav, shares_added, cost_after,
                     created_at, applied_at)
                    VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_code ON transactions(code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_confirm_date ON transactions(confirm_date)")

        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (2)")

    # Migration: AI Prompts
    if current_version < 3:
        logger.info("Running migration: adding ai_prompts table")

        # Create ai_prompts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ai_prompts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                system_prompt TEXT NOT NULL,
                user_prompt TEXT NOT NULL,
                is_default INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert default Linus-style prompt
        cursor.execute("""
            INSERT OR IGNORE INTO ai_prompts (name, system_prompt, user_prompt, is_default)
            VALUES (?, ?, ?, 1)
        """, (
            "Linus 风格（默认）",
            """角色设定
你是 Linus Torvalds，专注于基金的技术面与估值审计。
你极度厌恶情绪化叙事、无关噪音和模棱两可的废话。
你只输出基于数据的逻辑审计结果。

风格要求
- 禁用"首先、其次"、"第一、第二"等解析步骤。
- 句子短，判断极其明确。
- 语气：分析过程冷酷，投资建议务实。
- 核心关注：估值偏差、技术形态、风险收益比。

技术指标合理范围（重要！）
- 夏普比率：0.5-1.0 正常，1.0-1.5 良好，1.5-2.5 优秀，>2.5 异常优秀（罕见但可能）
- 夏普比率计算公式：(年化回报 - 无风险利率) / 年化波动率，其中无风险利率通常为 2-3%
- 最大回撤与年化回报的关系：回撤/回报比 < 0.5 为优秀，0.5-1.0 正常，>1.0 风险较高
- 数据一致性检查：验证夏普比率是否与年化回报、波动率数学一致（允许 ±0.3 误差）

判断逻辑
1. 先验证数据自洽性：夏普比率 ≈ (年化回报 - 2%) / 波动率
2. 如果数据一致，则分析风险收益比是否合理
3. 如果数据不一致，则标记为异常并说明原因""",
            """请对以下基金数据进行逻辑审计，并直接输出审计结果。

【输入数据】
基金代码: {fund_code}
基金名称: {fund_name}
基金类型: {fund_type}
基金经理: {manager}
最新净值: {nav}
实时估值: {estimate} ({est_rate}%)
夏普比率: {sharpe}
年化波动率: {volatility}
最大回撤: {max_drawdown}
年化收益: {annual_return}
持仓集中度: {concentration}%
前10大持仓: {holdings}
历史走势: {history_summary}

【输出要求（严禁分步骤描述分析过程，直接合并为一段精简报告）】
1. 逻辑审计：重点分析技术指标（夏普比率、最大回撤、波动率）、技术位阶（高/低位）及风险特征。
2. 最终结论：一句话总结当前基金的状态（高风险/低风险/正常/异常）。
3. 操作建议：给出 1-2 条冷静、务实的操作指令（持有/止盈/观望/定投）。

请输出纯 JSON 格式（不要用 Markdown 代码块包裹），包含字段:
- summary: 毒舌一句话总结
- risk_level: 风险等级（低风险/中风险/高风险/极高风险）
- analysis_report: 精简综合报告（200字以内）
- suggestions: 操作建议列表（1-3条）"""
        ))

        # Insert a gentle-style prompt as alternative
        cursor.execute("""
            INSERT OR IGNORE INTO ai_prompts (name, system_prompt, user_prompt, is_default)
            VALUES (?, ?, ?, 0)
        """, (
            "温和风格",
            """你是一位专业的基金分析师，擅长用通俗易懂的语言解读基金数据。
你的分析客观、理性，注重风险提示，但语气温和友善。

分析要点：
- 用简单的语言解释技术指标的含义
- 客观评估基金的风险收益特征
- 给出实用的投资建议
- 避免过于激进或保守的判断""",
            """请分析以下基金数据：

【基金信息】
代码: {fund_code}
名称: {fund_name}
类型: {fund_type}
经理: {manager}

【净值数据】
最新净值: {nav}
实时估值: {estimate} ({est_rate}%)

【技术指标】
夏普比率: {sharpe}
年化波动率: {volatility}
最大回撤: {max_drawdown}
年化收益: {annual_return}

【持仓情况】
集中度: {concentration}%
前10大持仓: {holdings}

【历史走势】
{history_summary}

请输出纯 JSON 格式（不要用 Markdown 代码块包裹），包含：
- summary: 一句话总结
- risk_level: 风险等级（低风险/中风险/高风险/极高风险）
- analysis_report: 详细分析报告（300字左右）
- suggestions: 投资建议列表（2-4条）"""
        ))

        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (3)")

    # Migration: Add unique constraint to ai_prompts.name
    if current_version < 4:
        logger.info("Running migration: adding unique constraint to ai_prompts.name")

        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_prompts_name ON ai_prompts(name)
        """)

        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (4)")

    # Migration: Strategy portfolio support
    if current_version < 5:
        logger.info("Running migration: adding strategy portfolio tables")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS strategy_portfolios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                account_id INTEGER NOT NULL,
                benchmark TEXT DEFAULT '000300',
                fee_rate REAL DEFAULT 0.001,
                scope_codes TEXT DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_portfolios_account ON strategy_portfolios(account_id)")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS strategy_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                version_no INTEGER NOT NULL,
                effective_date TEXT NOT NULL,
                note TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (portfolio_id) REFERENCES strategy_portfolios(id) ON DELETE CASCADE,
                UNIQUE(portfolio_id, version_no)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_versions_portfolio ON strategy_versions(portfolio_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_versions_active ON strategy_versions(portfolio_id, is_active)")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS strategy_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                fund_code TEXT NOT NULL,
                target_weight REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (version_id) REFERENCES strategy_versions(id) ON DELETE CASCADE,
                UNIQUE(version_id, fund_code)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_holdings_version ON strategy_holdings(version_id)")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rebalance_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                version_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                fund_code TEXT NOT NULL,
                fund_name TEXT,
                action TEXT NOT NULL,
                target_weight REAL NOT NULL,
                current_weight REAL NOT NULL,
                target_shares REAL NOT NULL,
                current_shares REAL NOT NULL,
                delta_shares REAL NOT NULL,
                price REAL NOT NULL,
                trade_amount REAL NOT NULL,
                fee REAL DEFAULT 0,
                status TEXT DEFAULT 'suggested',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                executed_at TIMESTAMP,
                FOREIGN KEY (portfolio_id) REFERENCES strategy_portfolios(id) ON DELETE CASCADE,
                FOREIGN KEY (version_id) REFERENCES strategy_versions(id) ON DELETE CASCADE,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_rebalance_orders_portfolio ON rebalance_orders(portfolio_id, account_id, status)")

        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (5)")

    # Migration: strategy scope codes
    if current_version < 6:
        logger.info("Running migration: adding scope_codes to strategy_portfolios")
        cursor.execute("PRAGMA table_info(strategy_portfolios)")
        columns = [row[1] for row in cursor.fetchall()]
        if "scope_codes" not in columns:
            cursor.execute("ALTER TABLE strategy_portfolios ADD COLUMN scope_codes TEXT DEFAULT '[]'")
            cursor.execute("UPDATE strategy_portfolios SET scope_codes = '[]' WHERE scope_codes IS NULL")
        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (6)")

    # Migration: rebalance execution detail columns
    if current_version < 7:
        logger.info("Running migration: adding execution columns to rebalance_orders")
        cursor.execute("PRAGMA table_info(rebalance_orders)")
        columns = [row[1] for row in cursor.fetchall()]

        if "executed_price" not in columns:
            cursor.execute("ALTER TABLE rebalance_orders ADD COLUMN executed_price REAL")
        if "executed_shares" not in columns:
            cursor.execute("ALTER TABLE rebalance_orders ADD COLUMN executed_shares REAL")
        if "executed_amount" not in columns:
            cursor.execute("ALTER TABLE rebalance_orders ADD COLUMN executed_amount REAL")
        if "execution_note" not in columns:
            cursor.execute("ALTER TABLE rebalance_orders ADD COLUMN execution_note TEXT")

        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (7)")

    # Migration: rebalance batches
    if current_version < 8:
        logger.info("Running migration: adding rebalance batches")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rebalance_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                version_id INTEGER,
                source TEXT DEFAULT 'auto',
                status TEXT DEFAULT 'pending',
                title TEXT,
                note TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (portfolio_id) REFERENCES strategy_portfolios(id) ON DELETE CASCADE,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_rebalance_batches_portfolio ON rebalance_batches(portfolio_id, account_id, status)")

        cursor.execute("PRAGMA table_info(rebalance_orders)")
        columns = [row[1] for row in cursor.fetchall()]
        if "batch_id" not in columns:
            cursor.execute("ALTER TABLE rebalance_orders ADD COLUMN batch_id INTEGER")

        cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (8)")

    conn.commit()
    conn.close()
    logger.info("Database initialized.")
