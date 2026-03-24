"""ReNew Capital Partners - AI Portfolio Intelligence App."""
import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from server.routes.chat import router as chat_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("ReNew AI Week App starting...")
    yield
    print("ReNew AI Week App shutting down...")


app = FastAPI(title="ReNew AI Portfolio Intelligence", lifespan=lifespan)

# API routes
app.include_router(chat_router, prefix="/api")

# Serve React frontend
frontend_dist = Path(__file__).parent / "frontend" / "dist"

if frontend_dist.exists():
    # Serve static assets (JS, CSS, images)
    assets_dir = frontend_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    # Serve any other static files at root level (favicon, etc.)
    @app.get("/vite.svg")
    async def vite_svg():
        f = frontend_dist / "vite.svg"
        if f.exists():
            return FileResponse(str(f))
        return {"error": "not found"}

    # SPA fallback - serve index.html for all non-API routes
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        if full_path.startswith("api/"):
            return {"error": "Not found"}, 404
        index_file = frontend_dist / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))
        return {"error": "Frontend not built. Run: cd frontend && npm run build"}
else:
    @app.get("/")
    async def root():
        return {"error": "Frontend not built. Run: cd frontend && npm run build"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
