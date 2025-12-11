# =============================================================================
# MAIN STANDALONE - Point d'entree pour l'executable Windows
# =============================================================================
# Ce fichier lance le serveur FastAPI avec le frontend integre.
# Utilise par PyInstaller pour creer ProspectionPro.exe
# Fonctionne sur Windows 10/11 avec gestion correcte de l'encodage UTF-8
# =============================================================================

import os
import sys
import io
import traceback

# =============================================================================
# CRASH LOGGING - Capture les erreurs dans un fichier
# =============================================================================

def get_crash_log_path():
    """Retourne le chemin du fichier crash.log a cote de l'executable"""
    if getattr(sys, 'frozen', False):
        return os.path.join(os.path.dirname(sys.executable), "crash.log")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash.log")

def write_crash_log(error_message):
    """Ecrit l'erreur dans crash.log"""
    try:
        crash_path = get_crash_log_path()
        with open(crash_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("PROSPECTIONPRO - RAPPORT D'ERREUR\n")
            f.write("=" * 60 + "\n\n")
            f.write(error_message)
        return crash_path
    except:
        return None

# =============================================================================
# CONFIGURATION ENCODAGE WINDOWS ET MODE SANS CONSOLE
# =============================================================================

class NullWriter:
    """Flux factice pour le mode sans console"""
    def write(self, s): pass
    def flush(self): pass
    def isatty(self): return False

def setup_streams():
    """Configure les flux stdout/stderr pour Windows et le mode sans console."""
    # Si pas de console (mode GUI), creer des flux factices
    if sys.stdout is None:
        sys.stdout = NullWriter()
    if sys.stderr is None:
        sys.stderr = NullWriter()
    
    # Sur Windows avec console, configurer UTF-8
    if sys.platform == 'win32' and hasattr(sys.stdout, 'buffer'):
        try:
            sys.stdout = io.TextIOWrapper(
                sys.stdout.buffer,
                encoding='utf-8',
                errors='replace',
                line_buffering=True
            )
            sys.stderr = io.TextIOWrapper(
                sys.stderr.buffer,
                encoding='utf-8',
                errors='replace',
                line_buffering=True
            )
        except (AttributeError, TypeError):
            pass

setup_streams()

# =============================================================================
# FONCTION PRINCIPALE AVEC GESTION D'ERREURS
# =============================================================================

def main():
    """Point d'entree principal avec gestion complete des erreurs"""
    
    import webbrowser
    import threading
    import time
    
    # =========================================================================
    # CONFIGURATION DES CHEMINS
    # =========================================================================
    
    if getattr(sys, 'frozen', False):
        BASE_DIR = sys._MEIPASS
        DATA_DIR = os.path.join(os.path.dirname(sys.executable), "data")
        LOGS_DIR = os.path.join(os.path.dirname(sys.executable), "logs")
    else:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        DATA_DIR = os.path.join(BASE_DIR, "data")
        LOGS_DIR = os.path.join(BASE_DIR, "logs")
    
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.chdir(os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else BASE_DIR)
    
    DB_PATH = os.path.join(DATA_DIR, 'prospection.db')
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{DB_PATH}"
    
    print(f"[INFO] BASE_DIR: {BASE_DIR}")
    print(f"[INFO] DATA_DIR: {DATA_DIR}")
    print(f"[INFO] DB_PATH: {DB_PATH}")
    
    # =========================================================================
    # IMPORTS APPLICATION
    # =========================================================================
    
    print("[INFO] Chargement des modules...")
    
    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    
    print("[INFO] FastAPI charge")
    
    from app.core.database import init_db
    from app.core.websocket import sio, socket_app
    from app.core.logger import logger
    from app.api import stats, prospects, scraping, export, emails, bots, proxies, campaigns
    
    print("[INFO] Modules application charges")
    
    # =========================================================================
    # APPLICATION FASTAPI
    # =========================================================================
    
    app = FastAPI(
        title="ProspectionPro",
        description="CRM de prospection immobiliere",
        version="5.1.0"
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.mount("/socket.io", socket_app)
    
    app.include_router(stats.router, prefix="/api/stats", tags=["Stats"])
    app.include_router(prospects.router, prefix="/api/prospects", tags=["Prospects"])
    app.include_router(scraping.router, prefix="/api/scraping", tags=["Scraping"])
    app.include_router(export.router, prefix="/api/export", tags=["Export"])
    app.include_router(emails.router, prefix="/api/emails", tags=["Emails"])
    app.include_router(bots.router, prefix="/api/bots", tags=["Bots"])
    app.include_router(proxies.router, prefix="/api/proxies", tags=["Proxies"])
    app.include_router(campaigns.router, prefix="/api/campaigns", tags=["Campaigns"])
    
    @app.get("/api/health")
    async def health_check():
        return {"status": "ok", "version": "5.1.0"}
    
    # =========================================================================
    # FRONTEND STATIQUE
    # =========================================================================
    
    STATIC_DIR = os.path.join(BASE_DIR, "static")
    print(f"[INFO] STATIC_DIR: {STATIC_DIR}")
    print(f"[INFO] STATIC_DIR exists: {os.path.exists(STATIC_DIR)}")
    
    if os.path.exists(STATIC_DIR):
        assets_dir = os.path.join(STATIC_DIR, "assets")
        if os.path.exists(assets_dir):
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
            print(f"[INFO] Assets montes depuis: {assets_dir}")
        logger.info(f"Frontend charge depuis: {STATIC_DIR}")
    else:
        logger.warning(f"Dossier frontend introuvable: {STATIC_DIR}")
    
    @app.get("/")
    async def serve_frontend():
        index_path = os.path.join(STATIC_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return {"message": "ProspectionPro API", "version": "5.1.0", "docs": "/docs"}
    
    @app.get("/{path:path}")
    async def catch_all(path: str):
        if path.startswith("api/"):
            return {"error": "Not found"}
        file_path = os.path.join(STATIC_DIR, path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        index_path = os.path.join(STATIC_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return {"error": "Not found"}
    
    # =========================================================================
    # EVENEMENTS DE CYCLE DE VIE
    # =========================================================================
    
    @app.on_event("startup")
    async def startup():
        logger.info("[START] Demarrage de ProspectionPro v5.1...")
        try:
            await init_db()
            logger.info("[OK] Base de donnees initialisee")
        except Exception as e:
            logger.error(f"[ERREUR] Initialisation DB: {e}")
        
        print("")
        print("=" * 50)
        print("  ProspectionPro v5.1 - Serveur demarre")
        print("=" * 50)
        print("")
        print(f"  Interface: http://localhost:8000")
        print(f"  API Docs:  http://localhost:8000/docs")
        print(f"  Database:  {DB_PATH}")
        print("")
        print("  Appuyez sur Ctrl+C pour arreter")
        print("=" * 50)
        print("")
        
        logger.info("[OK] Serveur pret sur http://localhost:8000")
    
    @app.on_event("shutdown")
    async def shutdown():
        logger.info("[STOP] Arret du serveur...")
    
    # =========================================================================
    # OUVERTURE AUTOMATIQUE DU NAVIGATEUR
    # =========================================================================
    
    def open_browser():
        time.sleep(2.5)
        try:
            webbrowser.open("http://localhost:8000")
            logger.info("[OK] Navigateur ouvert")
        except Exception as e:
            logger.warning(f"Impossible d'ouvrir le navigateur: {e}")
    
    # =========================================================================
    # DEMARRAGE DU SERVEUR
    # =========================================================================
    
    print("[INFO] Demarrage du serveur uvicorn...")
    threading.Thread(target=open_browser, daemon=True).start()
    
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="warning",
        access_log=False,
        log_config=None  # Desactive la config logging uvicorn (evite erreur sans console)
    )

# =============================================================================
# POINT D'ENTREE AVEC GESTION GLOBALE DES ERREURS
# =============================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Capturer l'erreur complete
        error_msg = traceback.format_exc()
        
        # Ecrire dans crash.log
        crash_path = write_crash_log(error_msg)
        
        # Afficher dans la console
        print("")
        print("=" * 60)
        print("  ERREUR FATALE - ProspectionPro")
        print("=" * 60)
        print("")
        print(f"  {type(e).__name__}: {e}")
        print("")
        if crash_path:
            print(f"  Details complets dans: {crash_path}")
        print("")
        print("=" * 60)
        print("")
        print(error_msg)
        print("")
        
        # Garder la fenetre ouverte
        input("Appuyez sur Entree pour fermer...")
