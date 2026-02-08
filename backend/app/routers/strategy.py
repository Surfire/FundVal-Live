from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from ..services.strategy import (
    create_portfolio,
    create_strategy_version,
    delete_portfolio,
    generate_rebalance_orders,
    get_performance,
    get_portfolio_detail,
    get_portfolio_positions_view,
    list_portfolios,
    list_rebalance_orders,
    update_rebalance_order_status,
)

router = APIRouter()


class HoldingModel(BaseModel):
    code: str = Field(..., min_length=1, max_length=20)
    weight: float = Field(..., gt=0)


class CreatePortfolioModel(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    account_id: int = Field(..., ge=1)
    holdings: List[HoldingModel] = Field(..., min_length=1)
    benchmark: str = "000300"
    fee_rate: float = Field(0.001, ge=0, le=0.02)
    effective_date: Optional[str] = None
    note: str = ""
    scope_codes: Optional[List[str]] = None


class CreateVersionModel(BaseModel):
    holdings: List[HoldingModel] = Field(..., min_length=1)
    effective_date: Optional[str] = None
    note: str = ""
    activate: bool = True
    scope_codes: Optional[List[str]] = None


class RebalanceModel(BaseModel):
    account_id: int = Field(..., ge=1)
    min_deviation: float = Field(0.005, ge=0, le=0.2)
    fee_rate: Optional[float] = Field(None, ge=0, le=0.02)
    persist: bool = True


class OrderStatusModel(BaseModel):
    status: str = Field(..., pattern="^(suggested|executed|skipped)$")


@router.get("/strategy/portfolios")
def api_list_portfolios(account_id: Optional[int] = Query(None)):
    try:
        return {"portfolios": list_portfolios(account_id=account_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategy/portfolios")
def api_create_portfolio(data: CreatePortfolioModel):
    try:
        result = create_portfolio(
            name=data.name,
            account_id=data.account_id,
            holdings=[h.model_dump() for h in data.holdings],
            benchmark=data.benchmark,
            fee_rate=data.fee_rate,
            effective_date=data.effective_date,
            note=data.note,
            scope_codes=data.scope_codes,
        )
        return {"ok": True, **result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategy/portfolios/{portfolio_id}")
def api_get_portfolio(portfolio_id: int):
    try:
        return get_portfolio_detail(portfolio_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/strategy/portfolios/{portfolio_id}")
def api_delete_portfolio(portfolio_id: int):
    try:
        return delete_portfolio(portfolio_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategy/portfolios/{portfolio_id}/delete")
def api_delete_portfolio_post(portfolio_id: int):
    try:
        return delete_portfolio(portfolio_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategy/portfolios/{portfolio_id}/versions")
def api_create_version(portfolio_id: int, data: CreateVersionModel):
    try:
        result = create_strategy_version(
            portfolio_id=portfolio_id,
            holdings=[h.model_dump() for h in data.holdings],
            effective_date=data.effective_date,
            note=data.note,
            activate=data.activate,
            scope_codes=data.scope_codes,
        )
        return {"ok": True, **result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategy/portfolios/{portfolio_id}/performance")
def api_get_performance(portfolio_id: int, account_id: int = Query(..., ge=1)):
    try:
        return get_performance(portfolio_id, account_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategy/portfolios/{portfolio_id}/positions")
def api_get_positions_view(portfolio_id: int, account_id: int = Query(..., ge=1)):
    try:
        return get_portfolio_positions_view(portfolio_id, account_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategy/portfolios/{portfolio_id}/rebalance")
def api_generate_rebalance(portfolio_id: int, data: RebalanceModel):
    try:
        return generate_rebalance_orders(
            portfolio_id=portfolio_id,
            account_id=data.account_id,
            min_deviation=data.min_deviation,
            fee_rate=data.fee_rate,
            persist=data.persist,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategy/portfolios/{portfolio_id}/rebalance-orders")
def api_list_rebalance_orders(
    portfolio_id: int,
    account_id: int = Query(..., ge=1),
    status: Optional[str] = Query(None),
):
    try:
        return {
            "orders": list_rebalance_orders(
                portfolio_id=portfolio_id,
                account_id=account_id,
                status=status,
            )
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/strategy/rebalance-orders/{order_id}/status")
def api_update_order_status(order_id: int, data: OrderStatusModel):
    try:
        return update_rebalance_order_status(order_id, data.status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
