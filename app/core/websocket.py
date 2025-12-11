# =============================================================================
# WEBSOCKET - Socket.IO pour temps réel
# =============================================================================

import socketio
import logging
from datetime import datetime

# Configuration du logging
logger = logging.getLogger("websocket")

# Creer le serveur Socket.IO
sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins='*'
)

# Application ASGI pour Socket.IO (utilisee par main.py et main_standalone.py)
socket_app = socketio.ASGIApp(sio)

# =============================================================================
# EVENTS HANDLERS
# =============================================================================

@sio.event
async def connect(sid, environ):
    """Client connecté"""
    logger.info(f"Client connected: {sid}")
    await sio.emit('connected', {'sid': sid}, to=sid)

@sio.event
async def disconnect(sid):
    """Client déconnecté"""
    logger.info(f"Client disconnected: {sid}")

@sio.event
async def subscribe(sid, data):
    """Abonnement à un canal"""
    channel = data.get('channel', 'general')
    await sio.enter_room(sid, channel)
    logger.info(f"{sid} subscribed to {channel}")

@sio.event
async def unsubscribe(sid, data):
    """Désabonnement d'un canal"""
    channel = data.get('channel', 'general')
    await sio.leave_room(sid, channel)
    logger.info(f"{sid} unsubscribed from {channel}")

# =============================================================================
# EMIT HELPERS
# =============================================================================

async def emit_bot_status(bot_id: int, status: str, data: dict = None):
    """Émet le statut d'un bot"""
    await sio.emit('bot_status', {
        'bot_id': bot_id,
        'status': status,
        'data': data or {},
        'timestamp': datetime.utcnow().isoformat()
    }, room='bots')

async def emit_bot_log(bot_id: int, message: str, level: str = 'info'):
    """Émet un log de bot"""
    await sio.emit('bot_log', {
        'bot_id': bot_id,
        'message': message,
        'level': level,
        'timestamp': datetime.utcnow().isoformat()
    }, room='bots')

async def emit_prospect_found(prospect: dict):
    """Émet quand un prospect est trouvé"""
    await sio.emit('prospect_found', {
        'prospect': prospect,
        'timestamp': datetime.utcnow().isoformat()
    }, room='prospects')

async def emit_email_sent(email_id: int, to: str, success: bool):
    """Émet quand un email est envoyé"""
    await sio.emit('email_sent', {
        'email_id': email_id,
        'to': to,
        'success': success,
        'timestamp': datetime.utcnow().isoformat()
    }, room='emails')

async def emit_campaign_progress(campaign_id: int, progress: int, total: int):
    """Émet la progression d'une campagne"""
    await sio.emit('campaign_progress', {
        'campaign_id': campaign_id,
        'progress': progress,
        'total': total,
        'percentage': round(progress / max(total, 1) * 100, 1),
        'timestamp': datetime.utcnow().isoformat()
    }, room='campaigns')

async def emit_activity(type: str, message: str, details: dict = None):
    """Émet une activité générale"""
    await sio.emit('activity', {
        'type': type,
        'message': message,
        'details': details or {},
        'timestamp': datetime.utcnow().isoformat()
    }, room='general')

async def emit_stats_update(stats: dict):
    """Émet une mise à jour des stats"""
    await sio.emit('stats_update', {
        'stats': stats,
        'timestamp': datetime.utcnow().isoformat()
    }, room='dashboard')

