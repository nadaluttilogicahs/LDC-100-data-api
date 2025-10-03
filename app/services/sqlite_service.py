import aiosqlite
from pathlib import Path
from typing import List, Tuple, Any, Dict, Optional
from fastapi import HTTPException
from ..services.fs_service import _safe_path

SQL_IDENT = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")

def _ok_ident(s: str) -> bool:
    return bool(s) and set(s) <= SQL_IDENT

def _db_path(name: str) -> Path:
    p = _safe_path(name)
    if p.suffix.lower() not in (".db", ".sqlite", ".sqlite3"):
        raise HTTPException(status_code=400, detail="Not a sqlite file")
    return p

async def _connect_ro(path: Path) -> aiosqlite.Connection:
    # read-only, condiviso, non blocca i writer (con WAL attivo)
    uri = f"file:{path}?mode=ro&cache=shared"
    # conn = await aiosqlite.connect(uri, uri=True)
    conn = await aiosqlite.connect(uri, uri=True, timeout=5.0)
    # await conn.execute("PRAGMA busy_timeout = 3000")
    return conn


# async def get_meta(name: str) -> Dict[str, Any]:
#     path = _db_path(name)
#     # async with await _connect_ro(path) as db:
#     db = await _connect_ro(path)
    
#     print(f"DEBUG: Opening database at {path}")
    
#     tables = []
#     # !! ONLY TABLES, non views
#     # async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'") as cur:
#     #     tnames = [r[0] async for r in cur]
#     async with db.execute("""SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name""") as cur:
#         entries = [ (r[0], r[1]) async for r in cur ]   # (name, type)
        
#         # DEBUG: stampa le tabelle trovate
#         print(f"DEBUG: Found tables/views: {entries}")
        
#         tnames = [e[0] for e in entries]
#     for t in tnames:
        
#         print(f"DEBUG: Processing table '{t}'")
        
#         cols = []
#         async with db.execute(f"PRAGMA table_info('{t}')") as cur:
#             async for cid, cname, ctype, notnull, dflt, pk in cur:
#                 cols.append({"name": cname, "type": ctype or ""})
#         # conteggio veloce (se grande, può essere lento: ok per archivi)
#         rows_approx = None
#         try:
#             async with db.execute(f"SELECT COUNT(*) FROM '{t}'") as cur:
#                 rows_approx = (await cur.fetchone())[0]
#         except Exception:
#             pass
#         # tables.append({"name": t, "rows_approx": rows_approx, "columns": cols})
#         # !! "table" | "view"
#         tables.append({"name": t, "rows_approx": rows_approx, "columns": cols, "kind": next((k for n,k in entries if n == t), "table")})
#     return {"tables": tables}

async def get_meta(name: str) -> Dict[str, Any]:
    path = _db_path(name)
    db = await _connect_ro(path)
    
    tables = []
    async with db.execute("""SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name""") as cur:
        entries = [ (r[0], r[1]) async for r in cur ]
        tnames = [e[0] for e in entries]
    
    for t in tnames:
        kind = next((k for n,k in entries if n == t), "table")
        cols = []
        rows_approx = None
        
        try:
            # Per le view, usa un approccio diverso
            if kind == "view":
                # Ottieni le colonne eseguendo una query LIMIT 0
                async with db.execute(f"SELECT * FROM '{t}' LIMIT 0") as cur:
                    cols = [{"name": desc[0], "type": ""} for desc in cur.description]
            else:
                # Per le tabelle usa PRAGMA
                async with db.execute(f"PRAGMA table_info('{t}')") as cur:
                    async for cid, cname, ctype, notnull, dflt, pk in cur:
                        cols.append({"name": cname, "type": ctype or ""})
            
            # Conta le righe (solo per tabelle, può essere lento per view)
            if kind == "table":
                try:
                    async with db.execute(f"SELECT COUNT(*) FROM '{t}'") as cur:
                        rows_approx = (await cur.fetchone())[0]
                except Exception:
                    pass
        
        except Exception as e:
            # Se fallisce, salta questa tabella/view
            print(f"WARNING: Skipping {kind} '{t}': {e}")
            continue
        
        tables.append({"name": t, "rows_approx": rows_approx, "columns": cols, "kind": kind})
    
    await db.close()
    return {"tables": tables}

async def get_preview(
    name: str, table: str, limit: int, offset: int, order_by: Optional[str], desc: bool
) -> Tuple[List[str], List[List[Any]], Optional[int]]:
    if not _ok_ident(table):
        raise HTTPException(status_code=400, detail="Invalid table")
    if order_by and not _ok_ident(order_by):
        raise HTTPException(status_code=400, detail="Invalid order_by")

    path = _db_path(name)
    # async with await _connect_ro(path) as db:
    db = await _connect_ro(path)
    # colonne
    cols: List[str] = []
    async with db.execute(f"PRAGMA table_info('{table}')") as cur:
        async for cid, cname, ctype, notnull, dflt, pk in cur:
            cols.append(cname)
    if not cols:
        raise HTTPException(status_code=404, detail="Table not found")

    order_clause = f" ORDER BY {order_by} {'DESC' if desc else 'ASC'}" if order_by else ""
    q = f"SELECT * FROM '{table}'{order_clause} LIMIT ? OFFSET ?"
    async with db.execute(q, (limit, offset)) as cur:
        rows = [list(r) async for r in cur]
    next_offset = offset + len(rows) if len(rows) == limit else None
    return cols, rows, next_offset

async def get_chart(
    name: str,
    table: str,
    time_col: str,
    ycols: List[str],
    tfrom: Optional[float],
    tto: Optional[float],
) -> Dict[str, Any]:
    # Validazioni nomi (niente SQL injection)
    if not _ok_ident(table) or not _ok_ident(time_col):
        raise HTTPException(status_code=400, detail="Invalid identifiers")
    if not all(_ok_ident(c) for c in ycols):
        raise HTTPException(status_code=400, detail="Invalid y columns")

    path = _db_path(name)
    where = []
    params: List[Any] = []
    if tfrom is not None:
        where.append(f"{time_col} >= ?")
        params.append(tfrom)
    if tto is not None:
        where.append(f"{time_col} <= ?")
        params.append(tto)
    where_clause = (" WHERE " + " AND ".join(where)) if where else ""

    select_cols = [time_col] + ycols
    q = f"SELECT {', '.join(select_cols)} FROM '{table}'{where_clause} ORDER BY {time_col} ASC"

    db = await _connect_ro(path)
    try:
        async with db.execute(q, params) as cur:
            rows = [list(r) async for r in cur]
    finally:
        await db.close()

    return {"columns": select_cols, "rows": rows}



class DbValidationError(Exception):
    pass


async def _ensure_table_and_columns(db: aiosqlite.Connection, table: str, cols: List[str]) -> None:
    # Verifica esistenza tabella/view e colonne
    # (sqlite_master copre anche le view)
    async with db.execute(
        "SELECT name FROM sqlite_master WHERE name = ? AND type IN ('table','view')", (table,)
    ) as cur:
        row = await cur.fetchone()
        if not row:
            raise DbValidationError(f"Tabella/view '{table}' inesistente.")

    async with db.execute(f"PRAGMA table_info('{table}')") as cur:
        have = {r["name"] async for r in cur}
    missing = [c for c in cols if c not in have]
    if missing:
        raise DbValidationError(f"Colonne non trovate in '{table}': {', '.join(missing)}")

def _time_expr(time_col: str) -> str:
    # Se la colonna è già epoch numeric: usala; altrimenti converti ISO -> epoch sec
    return time_col if time_col == "timeEpoch" else f"strftime('%s',{time_col})"



# !! Quindi il problema era che async with db: non funziona su una connessione già aperta restituita da _connect_ro()
# ✅ Funziona: try/finally con await db.close()
# db = await _connect_ro(db_path)
# try:
#     # ... lavora con db
# finally:
#     await db.close()

# ❌ Non funziona: async with db: su connessione già aperta
# db = await _connect_ro(db_path)
# async with db:  # ← causa "threads can only be started once"
#     # ...

# Nota: Le altre funzioni (get_meta, get_preview) che pensavi funzionassero probabilmente hanno lo stesso problema latente 
# ma magari non le hai ancora testate abbastanza o hanno carichi diversi. Ti consiglio di uniformare tutte le funzioni usando 
# try/finally come get_chart() e sample_rows() per evitare sorprese future.

async def sample_rows(
    db_name: str,
    table: str,
    time_col: str,
    bucket: str,            # "none" | "1h" | "24h"
    order_by: str,
    desc: bool,
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    db_path = _db_path(db_name)

    db = await _connect_ro(db_path)     # APRI (una sola volta)
    try:
        # rows come dict-like
        db.row_factory = aiosqlite.Row

        # Verifica tabella/colonne
        await _ensure_table_and_columns(db, table, [time_col, order_by])

        texpr = time_col if time_col == "timeEpoch" else f"strftime('%s',{time_col})"
        order_expr = order_by

        if bucket == "none":
            sql = f"""
                SELECT *
                FROM {table}
                ORDER BY {order_expr} {"DESC" if desc else "ASC"}
                LIMIT ? OFFSET ?
            """
            async with db.execute(sql, (limit, offset)) as cur:
                rows = await cur.fetchall()

        elif bucket == "1h":
            sql = f"""
                WITH w AS (
                  SELECT *,
                         CAST({texpr}/3600 AS INTEGER) AS h,
                         ROW_NUMBER() OVER (
                           PARTITION BY CAST({texpr}/3600 AS INTEGER)
                           ORDER BY {texpr} ASC
                         ) AS rn
                  FROM {table}
                )
                SELECT * FROM w
                WHERE rn = 1
                ORDER BY {texpr} {"DESC" if desc else "ASC"}
                LIMIT ? OFFSET ?
            """
            async with db.execute(sql, (limit, offset)) as cur:
                rows = await cur.fetchall()

        elif bucket == "24h":
            sql = f"""
                WITH w AS (
                  SELECT *,
                         CAST({texpr}/86400 AS INTEGER) AS d,
                         ROW_NUMBER() OVER (
                           PARTITION BY CAST({texpr}/86400 AS INTEGER)
                           ORDER BY {texpr} ASC
                         ) AS rn
                  FROM {table}
                )
                SELECT * FROM w
                WHERE rn = 1
                ORDER BY {texpr} {"DESC" if desc else "ASC"}
                LIMIT ? OFFSET ?
            """
            async with db.execute(sql, (limit, offset)) as cur:
                rows = await cur.fetchall()

        else:
            raise DbValidationError("Valore 'bucket' non valido. Usa: none, 1h, 24h.")

        if rows:
            cols = list(rows[0].keys())
            data_rows = [[r[c] for c in cols] for r in rows]
        else:
            async with db.execute(f"PRAGMA table_info('{table}')") as cur:
                cols = [r["name"] async for r in cur]
            data_rows = []

        return {"columns": cols, "rows": data_rows}

    finally:
        await db.close()  # CHIUDI SEMPRE


async def count_rows(db_name: str, table: str, time_col: str, bucket: str) -> int:
    db_path = _db_path(db_name)
    db = await _connect_ro(db_path)
    try:
        db.row_factory = aiosqlite.Row
        await _ensure_table_and_columns(db, table, [time_col])
        texpr = time_col if time_col == "timeEpoch" else f"strftime('%s',{time_col})"

        if bucket == "none":
            sql = f"SELECT COUNT(*) AS total FROM {table}"
        elif bucket == "1h":
            sql = f"SELECT COUNT(DISTINCT CAST({texpr}/3600 AS INTEGER)) AS total FROM {table}"
        elif bucket == "24h":
            sql = f"SELECT COUNT(DISTINCT CAST({texpr}/86400 AS INTEGER)) AS total FROM {table}"
        else:
            raise DbValidationError("Valore 'bucket' non valido. Usa: none, 1h, 24h.")

        async with db.execute(sql) as cur:
            row = await cur.fetchone()
            return int(row["total"] if isinstance(row, dict) else row[0])
    finally:
        await db.close()
