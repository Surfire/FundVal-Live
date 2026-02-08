import os
import re
import datetime
import json
import numpy as np
import pandas as pd
from typing import Optional, Dict, Any, List
from duckduckgo_search import DDGS
import requests

from ..config import Config
from .fund import get_fund_history, _calculate_technical_indicators
from ..db import get_db_connection


class AIService:
    _FALLBACK_SYSTEM_PROMPT = (
        "你是专业基金分析助手。请基于输入数据输出客观、简洁的分析结论，并只返回 JSON。"
    )
    _FALLBACK_USER_PROMPT = (
        "请分析以下基金数据：\n"
        "基金代码: {fund_code}\n基金名称: {fund_name}\n基金类型: {fund_type}\n基金经理: {manager}\n"
        "最新净值: {nav}\n实时估值: {estimate}\n估值涨跌: {est_rate}\n"
        "夏普: {sharpe}\n波动率: {volatility}\n最大回撤: {max_drawdown}\n年化收益: {annual_return}\n"
        "持仓集中度: {concentration}\n持仓摘要: {holdings}\n历史摘要: {history_summary}\n"
        "输出 JSON，字段: summary, risk_level, analysis_report, suggestions"
    )

    def __init__(self):
        # 不在初始化时创建 LLM，而是每次调用时动态创建
        pass

    def _get_prompt_texts(self, prompt_id: Optional[int] = None):
        """
        Get prompt template from database.
        If prompt_id is None, use the default template.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        if prompt_id:
            cursor.execute("""
                SELECT system_prompt, user_prompt FROM ai_prompts WHERE id = ?
            """, (prompt_id,))
        else:
            cursor.execute("""
                SELECT system_prompt, user_prompt FROM ai_prompts WHERE is_default = 1 LIMIT 1
            """)

        row = cursor.fetchone()
        conn.close()

        if row:
            return row["system_prompt"], row["user_prompt"]

        # Fallback to built-in prompt text
        return self._FALLBACK_SYSTEM_PROMPT, self._FALLBACK_USER_PROMPT

    def _call_chat_completions(self, system_prompt: str, user_prompt: str, timeout_sec: int = 90) -> str:
        api_base = str(Config.OPENAI_API_BASE or "").strip().rstrip("/")
        api_key = str(Config.OPENAI_API_KEY or "").strip()
        model = str(Config.AI_MODEL_NAME or "").strip() or "gpt-3.5-turbo"
        if not api_key:
            raise ValueError("未配置 API Key")
        if not api_base:
            raise ValueError("未配置 API Base")

        url = f"{api_base}/chat/completions"
        payload = {
            "model": model,
            "temperature": 0.3,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": 1200,
            "response_format": {"type": "json_object"},
        }
        # trust_env=False avoids local SOCKS proxy env and eliminates socksio dependency issues.
        with requests.Session() as s:
            s.trust_env = False
            resp = s.post(
                url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=timeout_sec,
            )
        if resp.status_code >= 400:
            raise ValueError(f"LLM 接口失败: HTTP {resp.status_code} {resp.text[:200]}")
        body = resp.json()
        return str(body["choices"][0]["message"]["content"] or "")

    def search_news(self, query: str) -> str:
        try:
            # Simple wrapper to fetch news
            ddgs = DDGS(verify=False)
            results = ddgs.text(
                keywords=query,
                region="cn-zh",
                safesearch="off",
                timelimit="w", # last week
                max_results=5,
            )
            
            if not results:
                return "暂无相关近期新闻。"
            
            output = ""
            for i, res in enumerate(results, 1):
                output += f"{i}. {res.get('title')} - {res.get('body')}\n"
            return output
        except Exception as e:
            print(f"Search error: {e}")
            return "新闻搜索服务暂时不可用。"

    def _calculate_indicators(self, history: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Calculate simple technical indicators based on recent history.
        """
        if not history or len(history) < 5:
            return {"status": "数据不足", "desc": "新基金或数据缺失"}

        navs = [item['nav'] for item in history]
        current_nav = navs[-1]
        max_nav = max(navs)
        min_nav = min(navs)
        avg_nav = sum(navs) / len(navs)

        # Position in range
        position = (current_nav - min_nav) / (max_nav - min_nav) if max_nav > min_nav else 0.5

        status = "正常"
        if position > 0.9: status = "高位"
        elif position < 0.1: status = "低位"
        elif current_nav > avg_nav * 1.05: status = "偏高"
        elif current_nav < avg_nav * 0.95: status = "偏低"

        return {
            "status": status,
            "desc": f"近30日最高{max_nav:.4f}, 最低{min_nav:.4f}, 现价处于{'高位' if position>0.8 else '低位' if position<0.2 else '中位'}区间 ({int(position*100)}%)"
        }

    async def analyze_fund(self, fund_info: Dict[str, Any], prompt_id: Optional[int] = None) -> Dict[str, Any]:
        if not Config.OPENAI_API_KEY:
            return {
                "summary": "未配置 LLM API Key，无法进行分析。",
                "risk_level": "未知",
                "analysis_report": "请在设置页面配置 OpenAI API Key 以启用 AI 分析功能。",
                "timestamp": datetime.datetime.now().strftime("%H:%M:%S")
            }

        fund_id = fund_info.get("id")
        fund_name = fund_info.get("name", "未知基金")

        # 1. Gather Data
        # History (Last 250 days for technical indicators)
        history = get_fund_history(fund_id, limit=250)
        indicators = self._calculate_indicators(history[:30] if len(history) >= 30 else history)

        # Calculate technical indicators (Sharpe, Volatility, Max Drawdown)
        technical_indicators = _calculate_technical_indicators(history)

        # 1.5 Data Consistency Check
        consistency_note = ""
        try:
            sharpe = technical_indicators.get("sharpe")
            annual_return_str = technical_indicators.get("annual_return", "")
            volatility_str = technical_indicators.get("volatility", "")

            if sharpe != "--" and annual_return_str != "--" and volatility_str != "--":
                # Parse percentage strings
                annual_return = float(annual_return_str.rstrip('%')) / 100.0
                volatility = float(volatility_str.rstrip('%')) / 100.0
                sharpe_val = float(sharpe)

                # Expected Sharpe = (annual_return - rf) / volatility
                rf = 0.02
                expected_sharpe = (annual_return - rf) / volatility if volatility > 0 else 0
                sharpe_diff = abs(expected_sharpe - sharpe_val)

                if sharpe_diff > 0.3:
                    consistency_note = f"\n⚠️ 数据一致性警告：夏普比率 {sharpe_val} 与计算值 {expected_sharpe:.2f} 偏差 {sharpe_diff:.2f}，可能存在数据异常。"
                else:
                    consistency_note = f"\n✓ 数据自洽性验证通过：夏普比率与年化回报/波动率数学一致（偏差 {sharpe_diff:.2f}）。"
        except:
            pass

        history_summary = "暂无历史数据"
        if history:
            recent_history = history[:30]
            history_summary = f"近30日走势: 起始{recent_history[0]['nav']} -> 结束{recent_history[-1]['nav']}. {indicators['desc']}"

        # Prepare variables for template replacement
        holdings_str = ""
        if fund_info.get("holdings"):
            holdings_str = "\n".join([
                f"- {h['name']}: {h['percent']}% (涨跌: {h['change']:+.2f}%)"
                for h in fund_info["holdings"][:10]
            ])

        variables = {
            "fund_code": fund_id,
            "fund_name": fund_name,
            "fund_type": fund_info.get("type", "未知"),
            "manager": fund_info.get("manager", "未知"),
            "nav": fund_info.get("nav", "--"),
            "estimate": fund_info.get("estimate", "--"),
            "est_rate": fund_info.get("estRate", 0),
            "sharpe": technical_indicators.get("sharpe", "--"),
            "volatility": technical_indicators.get("volatility", "--"),
            "max_drawdown": technical_indicators.get("max_drawdown", "--"),
            "annual_return": technical_indicators.get("annual_return", "--"),
            "concentration": fund_info.get("indicators", {}).get("concentration", "--"),
            "holdings": holdings_str or "暂无持仓数据",
            "history_summary": history_summary
        }

        # 2. Build prompt text
        system_prompt, user_template = self._get_prompt_texts(prompt_id)
        try:
            user_prompt = user_template.format(**variables)
        except Exception:
            user_prompt = (
                f"基金信息: {fund_name}({fund_id})\n"
                f"技术指标: {technical_indicators}\n"
                f"历史: {history_summary}\n"
                "请输出 JSON: {summary, risk_level, analysis_report, suggestions}"
            )

        try:
            # 3. Invoke LLM through direct HTTP to avoid proxy/socks runtime issues.
            raw_result = self._call_chat_completions(system_prompt, user_prompt, timeout_sec=90)

            # 4. Parse Result
            clean_json = raw_result.strip()
            if "```json" in clean_json:
                clean_json = clean_json.split("```json")[1].split("```")[0]
            elif "```" in clean_json:
                clean_json = clean_json.split("```")[1].split("```")[0]

            result = json.loads(clean_json)

            # Enrich with indicators for frontend display
            result["indicators"] = indicators
            result["timestamp"] = datetime.datetime.now().strftime("%H:%M:%S")

            return result

        except Exception as e:
            print(f"AI Analysis Error: {e}")
            return {
                "summary": "分析生成失败",
                "risk_level": "未知",
                "analysis_report": f"LLM 调用或解析失败: {str(e)}",
                "indicators": indicators,
                "timestamp": datetime.datetime.now().strftime("%H:%M:%S")
            }

ai_service = AIService()
