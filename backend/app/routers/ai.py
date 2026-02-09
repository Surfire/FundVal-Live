from fastapi import APIRouter, Body, HTTPException
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from ..services.ai import ai_service
from ..db import get_db_connection

router = APIRouter()

class PromptModel(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    system_prompt: str = Field(..., min_length=1, max_length=10000)
    user_prompt: str = Field(..., min_length=1, max_length=10000)
    is_default: bool = False


class BacktestAnalyzeModel(BaseModel):
    backtest_result: Dict[str, Any]
    style: str = "hardcore_audit"

@router.post("/ai/analyze_fund")
async def analyze_fund(fund_info: Dict[str, Any] = Body(...), prompt_id: int = Body(None)):
    return await ai_service.analyze_fund(fund_info, prompt_id=prompt_id)


@router.get("/ai/backtest-prompts")
def get_backtest_prompts():
    return {"prompts": ai_service.get_backtest_prompt_presets()}


@router.post("/ai/analyze_backtest")
async def analyze_backtest(data: BacktestAnalyzeModel):
    return await ai_service.analyze_backtest(data.backtest_result, style=data.style)

@router.get("/ai/prompts")
def get_prompts():
    """获取所有 AI 提示词模板"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, system_prompt, user_prompt, is_default, created_at, updated_at
            FROM ai_prompts
            ORDER BY is_default DESC, id ASC
        """)
        prompts = [dict(row) for row in cursor.fetchall()]
        return {"prompts": prompts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.post("/ai/prompts")
def create_prompt(data: PromptModel):
    """创建新的 AI 提示词模板"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # If this is set as default, unset other defaults
        if data.is_default:
            cursor.execute("UPDATE ai_prompts SET is_default = 0")

        cursor.execute("""
            INSERT INTO ai_prompts (name, system_prompt, user_prompt, is_default)
            VALUES (?, ?, ?, ?)
        """, (data.name, data.system_prompt, data.user_prompt, 1 if data.is_default else 0))

        prompt_id = cursor.lastrowid
        conn.commit()

        return {"ok": True, "id": prompt_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.put("/ai/prompts/{prompt_id}")
def update_prompt(prompt_id: int, data: PromptModel):
    """更新 AI 提示词模板"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # If this is set as default, unset other defaults
        if data.is_default:
            cursor.execute("UPDATE ai_prompts SET is_default = 0 WHERE id != ?", (prompt_id,))

        cursor.execute("""
            UPDATE ai_prompts
            SET name = ?, system_prompt = ?, user_prompt = ?, is_default = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (data.name, data.system_prompt, data.user_prompt, 1 if data.is_default else 0, prompt_id))

        conn.commit()

        return {"ok": True}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.delete("/ai/prompts/{prompt_id}")
def delete_prompt(prompt_id: int):
    """删除 AI 提示词模板"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # Check if it's the default prompt
        cursor.execute("SELECT is_default FROM ai_prompts WHERE id = ?", (prompt_id,))
        row = cursor.fetchone()

        if row and row["is_default"]:
            raise HTTPException(status_code=400, detail="不能删除默认模板")

        cursor.execute("DELETE FROM ai_prompts WHERE id = ?", (prompt_id,))
        conn.commit()

        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
