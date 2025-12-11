# =============================================================================
# FASTAPI MAIN - Prospection Pro v5.1
# =============================================================================
# Compatible avec Railway, Render, Heroku et local
# =============================================================================

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import socketio
import uvicorn
import os
import time

from app.api import prospects, emails, bots, campaigns, proxies, stats, scraping, export
from app.core.database import init_db
from app.core.websocket import sio
from app.core.logger import logger

# =============================================================================
# CONFIGURATION CHEMINS
# =============================================================================

# Detecter le chemin du frontend
# 1. Variable d'environnement FRONTEND_PATH
# 2. Dossier 'static' a cote du backend
# 3. Aucun frontend (API only mode)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")
FRONTEND_PATH = os.environ.get('FRONTEND_PATH', STATIC_DIR if os.path.exists(STATIC_DIR) else None)

# Port depuis variable d'environnement (Railway, Render, etc.)
PORT = int(os.environ.get("PORT", 8000))

# =============================================================================
# APP CONFIGURATION
# =============================================================================

app = FastAPI(
    title="Prospection Pro API",
    description="API pour la prospection immobiliere en Suisse",
    version="5.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Gestionnaire d'erreurs global
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Une erreur interne est survenue. Veuillez reessayer plus tard."}
    )

# CORS - Permettre les requetes cross-origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Socket.IO - Monter sur l'app principale
socket_app = socketio.ASGIApp(sio, other_asgi_app=app, socketio_path="/socket.io")

# =============================================================================
# ROUTES API
# =============================================================================

app.include_router(prospects.router, prefix="/api/prospects", tags=["Prospects"])
app.include_router(emails.router, prefix="/api/emails", tags=["Emails"])
app.include_router(bots.router, prefix="/api/bots", tags=["Bots"])
app.include_router(campaigns.router, prefix="/api/campaigns", tags=["Campaigns"])
app.include_router(proxies.router, prefix="/api/proxies", tags=["Proxies"])
app.include_router(stats.router, prefix="/api/stats", tags=["Stats"])
app.include_router(scraping.router, prefix="/api/scraping", tags=["Scraping"])
app.include_router(export.router, prefix="/api/export", tags=["Export"])

# =============================================================================
# FRONTEND SERVING
# =============================================================================

if FRONTEND_PATH and os.path.exists(FRONTEND_PATH):
    logger.info(f"[OK] Frontend active depuis: {FRONTEND_PATH}")
    
    # Monter les assets statiques
    assets_path = os.path.join(FRONTEND_PATH, "assets")
    if os.path.exists(assets_path):
        app.mount("/assets", StaticFiles(directory=assets_path), name="assets")
        logger.info(f"[OK] Assets montes depuis: {assets_path}")
else:
    logger.info("[INFO] Mode API uniquement - pas de frontend")

# =============================================================================
# EVENTS
# =============================================================================

@app.on_event("startup")
async def startup():
    """Initialisation au demarrage"""
    logger.info("[START] Demarrage de Prospection Pro API v5.1...")
    try:
        await init_db()
        logger.info("[OK] Base de donnees initialisee")
        logger.info(f"[OK] API prete sur le port {PORT}")
    except Exception as e:
        logger.critical(f"[ERREUR] Echec du demarrage: {e}", exc_info=True)
        raise e

@app.on_event("shutdown")
async def shutdown():
    """Nettoyage a l'arret"""
    logger.info("[STOP] Arret du serveur...")

# =============================================================================
# ROUTES PRINCIPALES
# =============================================================================

@app.get("/api/health")
async def health():
    """Health check pour Railway/Render"""
    return {"status": "ok", "version": "5.1.0"}

@app.get("/")
async def root():
    """Route racine - sert index.html si frontend configure, sinon info API"""
    if FRONTEND_PATH and os.path.exists(FRONTEND_PATH):
        index_path = os.path.join(FRONTEND_PATH, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
    return {
        "name": "Prospection Pro API",
        "version": "5.1.0",
        "status": "running",
        "docs": "/api/docs"
    }

# Route catch-all pour le SPA React (doit etre APRES les routes API)
@app.get("/{full_path:path}")
async def catch_all(full_path: str):
    """Catch-all pour servir le frontend React"""
    # Ne jamais intercepter les routes API ou socket.io
    if full_path.startswith("api/") or full_path.startswith("socket.io"):
        raise HTTPException(status_code=404, detail="Not found")
    
    # Servir le frontend si configure
    if FRONTEND_PATH and os.path.exists(FRONTEND_PATH):
        # Verifier si c'est un fichier physique (ex: favicon.ico)
        file_path = os.path.join(FRONTEND_PATH, full_path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        
        # Sinon, renvoyer index.html pour que React gere la route
        index_path = os.path.join(FRONTEND_PATH, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
    
    # Pas de frontend, 404
    raise HTTPException(status_code=404, detail="Not found")

# =============================================================================
# MAIN (developpement local uniquement)
# =============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "app.main:socket_app",
        host="0.0.0.0",
        port=PORT,
        reload=True
    )
