from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional
from ..security import require_api_key
from ..services import sqlite_service
import numpy as np
from datetime import datetime
from ..services import downsample as ds

router = APIRouter(prefix="/db", tags=["db"])

@router.get("/{name}/meta")
async def db_meta(name: str, _=Depends(require_api_key)):
    return await sqlite_service.get_meta(name)

@router.get("/{name}/preview")
async def db_preview(
    name: str,
    table: str = Query(..., min_length=1),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    order_by: Optional[str] = None,
    desc: bool = True,
    _=Depends(require_api_key),
):
    cols, rows, next_off = await sqlite_service.get_preview(name, table, limit, offset, order_by, desc)
    return {"columns": cols, "rows": rows, "next_offset": next_off}

def _parse_iso_to_epoch(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    # supporta '2025-09-19T08:00:00Z' o senza Z (assume UTC)
    s2 = s.replace("Z", "+00:00") if "Z" in s else s
    return datetime.fromisoformat(s2).timestamp()

@router.get("/{name}/chart")
async def db_chart(
    name: str,
    table: str,
    time_col: str,
    y: str,                       # es. "temp,hum"
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    down: str = Query("lttb", alias="downsample"),  # "lttb" | "minmax"
    points: int = 2000,           # target punti per serie
    _=Depends(require_api_key),
):
    ycols = [c.strip() for c in y.split(",") if c.strip()]
    if not ycols:
        raise HTTPException(status_code=400, detail="Missing y columns")
    if points <= 0 or points > 20000:
        raise HTTPException(status_code=400, detail="Invalid points")

    tfrom = _parse_iso_to_epoch(from_ts)
    tto = _parse_iso_to_epoch(to_ts)

    data = await sqlite_service.get_chart(name, table, time_col, ycols, tfrom, tto)
    cols = data["columns"]           # [time_col, y1, y2, ...]
    rows = data["rows"]              # [[t, v1, v2,...], ...]

    if not rows:
        return {"series": []}

    ts = np.array([float(r[0]) for r in rows])

    series = []
    for j, col in enumerate(cols[1:], start=1):
        vals = []
        for r in rows:
            v = r[j]
            if v is None:
                vals.append(np.nan)
            else:
                try:
                    vals.append(float(v))
                except Exception:
                    vals.append(np.nan)
        yarr = np.array(vals, dtype=float)
        mask = ~np.isnan(yarr)
        if mask.sum() == 0:
            series.append({"name": col, "points": []})
            continue

        xy = np.vstack([ts[mask], yarr[mask]]).T
        if xy.shape[0] > points:
            if down == "lttb":
                xy_ds = ds.lttb(xy, points)
            else:
                buckets = max(1, points // 2)
                xy_ds = ds.minmax_bucket(xy, buckets)
        else:
            xy_ds = xy

        series.append({"name": col, "points": xy_ds.tolist()})

    return {"series": series}