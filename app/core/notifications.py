# =============================================================================
# NOTIFICATIONS - Gestionnaire de notifications (Telegram, Email)
# =============================================================================

import aiohttp
import asyncio
from app.core.logger import logger

# Configuration Telegram (à déplacer dans .env)
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

class NotificationManager:
    """Gestionnaire de notifications"""
    
    def __init__(self):
        self.telegram_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        
    async def send_telegram(self, message: str):
        """Envoie un message Telegram"""
        if not self.telegram_token or not self.chat_id or self.telegram_token == "YOUR_BOT_TOKEN":
            logger.warning("Telegram non configuré. Notification ignorée.")
            return False
            
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as response:
                    if response.status == 200:
                        logger.info("Notification Telegram envoyée")
                        return True
                    else:
                        logger.error(f"Erreur Telegram: {response.status}")
                        return False
        except Exception as e:
            logger.error(f"Erreur envoi Telegram: {e}")
            return False

    async def notify_new_prospect(self, prospect):
        """Notifie qu'un prospect intéressant a été trouvé"""
        if prospect.score < 70:
            return # Ne pas spammer pour les scores faibles
            
        emoji = "🔥" if prospect.score >= 90 else "✨"
        message = (
            f"{emoji} <b>Nouveau Prospect Qualifié !</b>\n\n"
            f"👤 <b>{prospect.nom} {prospect.prenom or ''}</b>\n"
            f"📍 {prospect.ville} ({prospect.adresse})\n"
            f"📊 Score: <b>{prospect.score}/100</b>\n"
            f"📱 {prospect.telephone or 'Pas de tél'}\n\n"
            f"<i>Source: {prospect.source}</i>"
        )
        await self.send_telegram(message)

    async def notify_bot_finished(self, bot_name, stats):
        """Notifie la fin d'un bot"""
        message = (
            f"🤖 <b>Bot {bot_name} Terminé</b>\n\n"
            f"✅ Succès: {stats.get('success', 0)}\n"
            f"❌ Erreurs: {stats.get('errors', 0)}\n"
            f"⏱ Durée: {stats.get('duration', 'N/A')}"
        )
        await self.send_telegram(message)

# Instance globale
notification_manager = NotificationManager()







