# app/routers/sample.py
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Literal, Optional
from ..services import sqlite_service

router = APIRouter(prefix="/db", tags=["db-sample"])

@router.get("/{name}/sample", summary="Sample rows (raw | 1h | 24h, first-per-bucket)")
async def sample_rows(
    name: str,
    table: str = Query(..., description="Nome tabella/view (es. measuresNormalized)"),
    time_col: str = Query("timeEpoch", description="Colonna tempo (es. timeEpoch o time)"),
    bucket: Literal["none", "1h", "24h"] = Query("none"),
    order_by: Optional[str] = Query(None, description="Colonna per ordinare (default: time_col)"),
    desc: bool = Query(True),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    try:
        result = await sqlite_service.sample_rows(
            db_name=name,
            table=table,
            time_col=time_col,
            bucket=bucket,
            order_by=order_by or time_col,
            desc=desc,
            limit=limit,
            offset=offset,
        )
        return result  # { "columns": [...], "rows": [[...], ...] }
    except sqlite_service.DbValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Database non trovato")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Errore: {e}")


@router.get("/{name}/count", summary="Count rows (raw | 1h | 24h)")
async def count_rows(
    name: str,
    table: str,
    time_col: str = "timeEpoch",
    bucket: Literal["none", "1h", "24h"] = "none",
):
    try:
        total = await sqlite_service.count_rows(
            db_name=name, table=table, time_col=time_col, bucket=bucket
        )
        return {"total": total}
    except sqlite_service.DbValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Database non trovato")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Errore: {e}")