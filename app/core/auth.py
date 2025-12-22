# =============================================================================
# AUTH - Authentification Supabase
# =============================================================================
# Système d'authentification basé sur Supabase Auth
# Support JWT, sessions, et Row Level Security
# =============================================================================

from __future__ import annotations

import os
import jwt
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.logger import logger


# =============================================================================
# CONFIGURATION
# =============================================================================

# Supabase config (à définir dans les variables d'environnement)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")  # Service role key
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")

# Fallback pour développement local
LOCAL_DEV_MODE = os.getenv("LOCAL_DEV_MODE", "true").lower() == "true"
LOCAL_DEV_USER = {
    "id": "local-dev-user",
    "email": "dev@localhost",
    "role": "admin",
}


# =============================================================================
# MODELS
# =============================================================================

@dataclass
class User:
    """Utilisateur authentifié."""
    id: str
    email: str
    role: str = "user"  # user, admin
    metadata: Dict[str, Any] = None
    
    def is_admin(self) -> bool:
        return self.role == "admin"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "email": self.email,
            "role": self.role,
            "metadata": self.metadata or {},
        }


# =============================================================================
# JWT VERIFICATION
# =============================================================================

security = HTTPBearer(auto_error=False)


def verify_jwt(token: str) -> Optional[Dict[str, Any]]:
    """
    Vérifie et décode un JWT Supabase.
    
    Args:
        token: Le JWT à vérifier
        
    Returns:
        Payload du token si valide, None sinon
    """
    if not SUPABASE_JWT_SECRET:
        # Mode développement local
        if LOCAL_DEV_MODE:
            return {"sub": LOCAL_DEV_USER["id"], "email": LOCAL_DEV_USER["email"], "role": LOCAL_DEV_USER["role"]}
        return None
    
    try:
        # Décoder le JWT avec la clé secrète Supabase
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated",
        )
        
        # Vérifier l'expiration
        exp = payload.get("exp")
        if exp and datetime.fromtimestamp(exp) < datetime.utcnow():
            logger.warning("[Auth] Token expiré")
            return None
        
        return payload
        
    except jwt.ExpiredSignatureError:
        logger.warning("[Auth] Token expiré")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"[Auth] Token invalide: {e}")
        return None
    except Exception as e:
        logger.error(f"[Auth] Erreur vérification JWT: {e}")
        return None


# =============================================================================
# DEPENDENCY INJECTION
# =============================================================================

async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> User:
    """
    Dépendance FastAPI pour récupérer l'utilisateur authentifié.
    
    Usage:
        @router.get("/protected")
        async def protected_route(user: User = Depends(get_current_user)):
            return {"message": f"Bonjour {user.email}"}
    """
    # Mode développement local
    if LOCAL_DEV_MODE and not SUPABASE_JWT_SECRET:
        return User(
            id=LOCAL_DEV_USER["id"],
            email=LOCAL_DEV_USER["email"],
            role=LOCAL_DEV_USER["role"],
        )
    
    # Vérifier la présence du token
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token d'authentification manquant",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Vérifier le token
    payload = verify_jwt(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide ou expiré",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Construire l'utilisateur
    user_id = payload.get("sub")
    email = payload.get("email", "")
    role = payload.get("role", "user")
    
    # Métadonnées utilisateur
    user_metadata = payload.get("user_metadata", {})
    app_metadata = payload.get("app_metadata", {})
    
    # Le rôle peut être dans app_metadata
    if not role or role == "user":
        role = app_metadata.get("role", "user")
    
    return User(
        id=user_id,
        email=email,
        role=role,
        metadata={**user_metadata, **app_metadata},
    )


async def get_current_user_optional(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> Optional[User]:
    """
    Dépendance pour routes optionnellement protégées.
    Retourne None si pas authentifié au lieu de lever une erreur.
    """
    if not credentials:
        return None
    
    payload = verify_jwt(credentials.credentials)
    if not payload:
        return None
    
    return User(
        id=payload.get("sub"),
        email=payload.get("email", ""),
        role=payload.get("role", "user"),
        metadata=payload.get("user_metadata", {}),
    )


async def require_admin(user: User = Depends(get_current_user)) -> User:
    """
    Dépendance pour routes réservées aux administrateurs.
    
    Usage:
        @router.delete("/users/{id}")
        async def delete_user(id: str, admin: User = Depends(require_admin)):
            ...
    """
    if not user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Accès réservé aux administrateurs",
        )
    return user


# =============================================================================
# SUPABASE CLIENT (optionnel, pour opérations serveur)
# =============================================================================

_supabase_client = None

def get_supabase_client():
    """
    Retourne le client Supabase pour opérations serveur.
    Utilise la service role key (accès complet).
    """
    global _supabase_client
    
    if _supabase_client is not None:
        return _supabase_client
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("[Auth] Supabase non configuré")
        return None
    
    try:
        from supabase import create_client, Client
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("[Auth] Client Supabase initialisé")
        return _supabase_client
    except ImportError:
        logger.warning("[Auth] Package supabase non installé")
        return None
    except Exception as e:
        logger.error(f"[Auth] Erreur création client Supabase: {e}")
        return None


# =============================================================================
# HELPERS
# =============================================================================

def create_access_token(user_id: str, email: str, role: str = "user", expires_delta: timedelta = None) -> str:
    """
    Crée un JWT d'accès (pour tests ou usage interne).
    En production, utiliser Supabase Auth.
    """
    if not SUPABASE_JWT_SECRET:
        # Mode développement
        return "dev-token"
    
    if expires_delta is None:
        expires_delta = timedelta(hours=1)
    
    expire = datetime.utcnow() + expires_delta
    
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "aud": "authenticated",
        "exp": expire,
        "iat": datetime.utcnow(),
    }
    
    return jwt.encode(payload, SUPABASE_JWT_SECRET, algorithm="HS256")


def is_authenticated(request: Request) -> bool:
    """Vérifie rapidement si une requête est authentifiée."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False
    
    token = auth_header[7:]  # Retirer "Bearer "
    return verify_jwt(token) is not None

