from fastapi import APIRouter, Depends, Query, Header, Response, HTTPException, status
from typing import List, Optional
from pathlib import Path

from ..security import require_api_key
from ..services import fs_service
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/files", tags=["files"])

@router.get("")
async def list_files(
    ext: Optional[str] = Query(None, description="es: db,sqlite,csv"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _=Depends(require_api_key),
):
    ext_list = [e.strip() for e in ext.split(",")] if ext else []
    return fs_service.list_files(ext_list, limit, offset)

@router.get("/{name}/download")
async def download(
    name: str,
    range_header: Optional[str] = Header(None, alias="Range"),
    if_none_match: Optional[str] = Header(None, alias="If-None-Match"),
    _=Depends(require_api_key),
):
    path, size, mtime, etag = fs_service.open_for_download(name)

    # 304 Not Modified (client already has same ETag)
    if if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers={"ETag": etag})

    # Calcolo range (se richiesto)
    start = 0
    end = size - 1
    status_code = status.HTTP_200_OK
    headers = {
        "Accept-Ranges": "bytes",
        "ETag": etag,
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f'attachment; filename="{path.name}"',
    }

    if range_header and range_header.startswith("bytes="):
        try:
            rng = range_header.split("=", 1)[1]
            s, e = rng.split("-", 1)
            start = int(s) if s else 0
            end = int(e) if e else (size - 1)
            if start > end or end >= size:
                raise ValueError()
            status_code = status.HTTP_206_PARTIAL_CONTENT
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
            headers["Content-Length"] = str(end - start + 1)
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid Range")
    else:
        headers["Content-Length"] = str(size)

    def streamer(p: Path, start: int, end: int):
        with open(p, "rb") as f:
            f.seek(start)
            remaining = end - start + 1
            chunk = 1024 * 256
            while remaining > 0:
                data = f.read(min(chunk, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

    # return Response(streamer(path, start, end), status_code=status_code, headers=headers)
    return StreamingResponse(streamer(path, start, end), status_code=status_code, headers=headers)


@router.delete("/{name}")
async def delete_file(
    name: str,
    if_match: Optional[str] = Header(None, alias="If-Match"),
    _=Depends(require_api_key),
):
    """
    Cancella un file dalla sandbox.
    Se `If-Match` è presente, cancella solo se l'ETag coincide (412 altrimenti).
    """
    meta = fs_service.delete_file(name, if_match)
    return meta  # 200 OK con info sul file cancellato
