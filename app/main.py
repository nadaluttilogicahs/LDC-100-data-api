from fastapi import FastAPI

from .routers import sample
from .routers import files
from .routers import db as db_router

app = FastAPI(title="LDC-100 HTTP Server", version="0.1")

app.include_router(files.router)
app.include_router(db_router.router)
app.include_router(sample.router)

@app.get("/health")
def health():
    return {"status": "ok"}
