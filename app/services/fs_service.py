from pathlib import Path
from datetime import datetime, timezone
import mimetypes
from typing import List, Tuple, Dict, Any
from ..config import settings
from typing import Optional
from datetime import datetime, timezone
from fastapi import HTTPException, status

BASE = Path(settings.data_base_dir).resolve()
ALLOWED_EXT = {".db", ".sqlite", ".sqlite3", ".csv"}

# def _safe_path(name: str) -> Path:
#     # Impedisce path traversal
#     if "/" in name or "\\" in name:
#         raise HTTPException(status_code=400, detail="Invalid file name")
#     p = (BASE / name).resolve()
#     if not str(p).startswith(str(BASE)):
#         raise HTTPException(status_code=403, detail="Forbidden path")
#     return p
def _safe_path(name: str) -> Path:
    """
    Gestisce sia file nella root (/data) che nelle sottocartelle (/data/archives).
    Se il file non esiste nella root, cerca automaticamente in archives/.
    Impedisce path traversal.
    """
    # Consenti un solo livello di sottocartella
    parts = name.split("/")
    if len(parts) > 2 or "\\" in name or ".." in parts:
        raise HTTPException(status_code=400, detail="Invalid file name")
    
    p = (BASE / name).resolve()
    
    # Verifica sicurezza
    if not str(p).startswith(str(BASE)):
        raise HTTPException(status_code=403, detail="Forbidden path")
    
    # Se il file non esiste e non contiene già un path, prova in archives/
    if not p.exists() and len(parts) == 1:
        p_archives = (BASE / "archives" / name).resolve()
        if str(p_archives).startswith(str(BASE)) and p_archives.exists():
            return p_archives
    
    return p

def list_files(ext: List[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    # Lista solo i file in /data/archives
    archives_dir = BASE / "archives"
    
    if not archives_dir.exists():
        return []
    
    # normalizza estensioni richieste
    req = {("." + e.strip(".").lower()) for e in ext} if ext else None
    items = []
    
    for entry in sorted(archives_dir.iterdir(), key=lambda x: x.name):
        if not entry.is_file():
            continue
        suffix = entry.suffix.lower()
        if suffix not in ALLOWED_EXT:
            continue
        if req and suffix not in req:
            continue
        stat = entry.stat()
        items.append({
            "name": entry.name,
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "mime": mimetypes.guess_type(entry.name)[0] or "application/octet-stream",
            "extension": suffix
        })
    return items[offset: offset + limit]

def open_for_download(name: str) -> Tuple[Path, int, float, str]:
    p = _safe_path(name)
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    st = p.stat()
    # ETag leggero: nome + size + mtime (va bene per cache/resume)
    etag = f"\"{p.name}-{st.st_size}-{int(st.st_mtime)}\""
    return p, st.st_size, st.st_mtime, etag


def delete_file(name: str, if_match: Optional[str] = None) -> dict:
    """
    Cancella il file se esiste nella sandbox.
    Se `if_match` è passato, verifica l'ETag prima di cancellare (pre-condizione).
    """
    path, size, mtime, etag = open_for_download(name)  # riusa i controlli e l'ETag
    if if_match and if_match != etag:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED, detail="ETag mismatch (If-Match failed)")

    try:
        path.unlink()
    except FileNotFoundError:
        # Race: qualcuno l'ha già cancellato
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File already removed")
    return {
        "deleted": path.name,
        "size_bytes": size,
        "etag": etag,
        "deleted_at": datetime.now(timezone.utc).isoformat(),
    }
