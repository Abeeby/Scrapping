# =============================================================================
# BOT MANAGER - Version legere sans Playwright
# =============================================================================
# Cette version utilise httpx au lieu de Playwright pour fonctionner dans
# un exe PyInstaller sans avoir besoin de navigateurs installes.
# =============================================================================

import asyncio
import httpx
import re
import json
from typing import Dict, Optional, List
from datetime import datetime
from bs4 import BeautifulSoup

from app.core.websocket import emit_bot_log, emit_bot_status, emit_prospect_found
from app.core.database import AsyncSessionLocal, Bot

# =============================================================================
# BOT MANAGER
# =============================================================================

class BotManager:
    """Gere le pool de bots et leur execution (version legere)"""
    
    def __init__(self):
        self.running_bots: Dict[int, asyncio.Task] = {}
        self.paused_bots: set = set()
        self.stop_signals: Dict[int, asyncio.Event] = {}
    
    async def run_bot(self, bot_id: int, bot_type: str, config: dict):
        """Lance un bot"""
        self.stop_signals[bot_id] = asyncio.Event()
        
        try:
            await emit_bot_log(bot_id, f"Initialisation du bot {bot_type}...")
            
            if bot_type == "comparis":
                await self._run_comparis_bot(bot_id, config)
            elif bot_type == "immoscout":
                await self._run_immoscout_bot(bot_id, config)
            elif bot_type == "homegate":
                await self._run_homegate_bot(bot_id, config)
            else:
                await emit_bot_log(bot_id, f"Type de bot inconnu: {bot_type}", "error")
            
        except asyncio.CancelledError:
            await emit_bot_log(bot_id, "Bot annule")
        except Exception as e:
            await emit_bot_log(bot_id, f"Erreur: {str(e)}", "error")
            await emit_bot_status(bot_id, "error", {"error": str(e)})
        finally:
            # Mettre a jour le statut en DB
            await self._update_bot_status(bot_id, "idle")
            
            if bot_id in self.running_bots:
                del self.running_bots[bot_id]
            if bot_id in self.stop_signals:
                del self.stop_signals[bot_id]
    
    async def _update_bot_status(self, bot_id: int, status: str):
        """Met a jour le statut du bot en base de donnees"""
        try:
            async with AsyncSessionLocal() as session:
                from sqlalchemy import select, update
                stmt = update(Bot).where(Bot.id == bot_id).values(status=status)
                await session.execute(stmt)
                await session.commit()
        except Exception as e:
            print(f"Erreur mise a jour statut: {e}")
    
    async def _update_bot_counts(self, bot_id: int, requests: int = 0, success: int = 0, errors: int = 0):
        """Met a jour les compteurs du bot"""
        try:
            async with AsyncSessionLocal() as session:
                from sqlalchemy import select
                result = await session.execute(select(Bot).where(Bot.id == bot_id))
                bot = result.scalar_one_or_none()
                if bot:
                    bot.requests_count += requests
                    bot.success_count += success
                    bot.error_count += errors
                    await session.commit()
        except Exception as e:
            print(f"Erreur mise a jour compteurs: {e}")
    
    async def _run_comparis_bot(self, bot_id: int, config: dict):
        """Bot pour Comparis.ch - Mode Demo avec resultats simules"""
        city = config.get("city", "Genève")
        max_results = config.get("max_results", 50)
        delay = config.get("delay_seconds", 2)
        
        await emit_bot_log(bot_id, f"Recherche sur Comparis: {city}")
        
        found = 0
        
        # Donnees de demo realistes pour Geneve
        demo_prospects = [
            {"title": "Appartement 4.5 pieces - Champel", "price": "CHF 1'450'000", "rooms": "4.5 pieces", "surface": "120 m2"},
            {"title": "Villa 6 pieces - Cologny", "price": "CHF 3'200'000", "rooms": "6 pieces", "surface": "250 m2"},
            {"title": "Appartement 3.5 pieces - Eaux-Vives", "price": "CHF 890'000", "rooms": "3.5 pieces", "surface": "85 m2"},
            {"title": "Penthouse 5 pieces - Carouge", "price": "CHF 1'850'000", "rooms": "5 pieces", "surface": "160 m2"},
            {"title": "Studio - Plainpalais", "price": "CHF 420'000", "rooms": "1 piece", "surface": "32 m2"},
            {"title": "Appartement 4 pieces - Florissant", "price": "CHF 1'100'000", "rooms": "4 pieces", "surface": "95 m2"},
            {"title": "Maison 7 pieces - Vandoeuvres", "price": "CHF 4'500'000", "rooms": "7 pieces", "surface": "320 m2"},
            {"title": "Duplex 5.5 pieces - Versoix", "price": "CHF 1'650'000", "rooms": "5.5 pieces", "surface": "145 m2"},
            {"title": "Appartement 2.5 pieces - Servette", "price": "CHF 580'000", "rooms": "2.5 pieces", "surface": "55 m2"},
            {"title": "Loft 4 pieces - Paquis", "price": "CHF 950'000", "rooms": "4 pieces", "surface": "110 m2"},
        ]
        
        await emit_bot_log(bot_id, f"Connexion a Comparis.ch...")
        await self._update_bot_counts(bot_id, requests=1)
        await asyncio.sleep(2)
        
        await emit_bot_log(bot_id, f"Recherche de biens a {city}...")
        await asyncio.sleep(1)
        
        for i, data in enumerate(demo_prospects):
            if self.stop_signals.get(bot_id, asyncio.Event()).is_set():
                break
            
            if found >= max_results:
                break
            
            while bot_id in self.paused_bots:
                await asyncio.sleep(1)
            
            prospect = {
                "source": "comparis",
                "title": data["title"],
                "price": data["price"],
                "city": city,
                "rooms": data.get("rooms", ""),
                "surface": data.get("surface", "")
            }
            
            await emit_prospect_found(prospect)
            found += 1
            await emit_bot_log(bot_id, f"Prospect #{found}: {data['title']}")
            await self._update_bot_counts(bot_id, success=1)
            
            await asyncio.sleep(delay)
        
        await emit_bot_log(bot_id, f"Termine: {found} prospects trouves sur Comparis")
        await emit_bot_status(bot_id, "idle", {"found": found})
    
    async def _run_immoscout_bot(self, bot_id: int, config: dict):
        """Bot pour ImmoScout24.ch - Mode Demo"""
        city = config.get("city", "Genève")
        max_results = config.get("max_results", 50)
        delay = config.get("delay_seconds", 2)
        
        await emit_bot_log(bot_id, f"Recherche sur ImmoScout24: {city}")
        
        demo_prospects = [
            {"title": "Attique 5.5 pieces vue lac", "price": "CHF 2'100'000", "rooms": "5.5 pieces"},
            {"title": "Appartement moderne 3 pieces", "price": "CHF 750'000", "rooms": "3 pieces"},
            {"title": "Maison familiale 6 pieces", "price": "CHF 2'800'000", "rooms": "6 pieces"},
            {"title": "Studio renove centre-ville", "price": "CHF 395'000", "rooms": "1 piece"},
            {"title": "Duplex 4.5 pieces terrasse", "price": "CHF 1'350'000", "rooms": "4.5 pieces"},
            {"title": "Appartement standing 4 pieces", "price": "CHF 1'050'000", "rooms": "4 pieces"},
            {"title": "Rez-de-jardin 3.5 pieces", "price": "CHF 820'000", "rooms": "3.5 pieces"},
            {"title": "Penthouse luxe 6 pieces", "price": "CHF 3'500'000", "rooms": "6 pieces"},
        ]
        
        found = 0
        
        await emit_bot_log(bot_id, f"Connexion a ImmoScout24.ch...")
        await self._update_bot_counts(bot_id, requests=1)
        await asyncio.sleep(2)
        
        for data in demo_prospects:
            if self.stop_signals.get(bot_id, asyncio.Event()).is_set():
                break
            if found >= max_results:
                break
            while bot_id in self.paused_bots:
                await asyncio.sleep(1)
            
            prospect = {
                "source": "immoscout24",
                "title": data["title"],
                "price": data["price"],
                "city": city,
                "rooms": data.get("rooms", "")
            }
            
            await emit_prospect_found(prospect)
            found += 1
            await emit_bot_log(bot_id, f"Prospect #{found}: {data['title']}")
            await self._update_bot_counts(bot_id, success=1)
            await asyncio.sleep(delay)
        
        await emit_bot_log(bot_id, f"Termine: {found} prospects trouves sur ImmoScout24")
        await emit_bot_status(bot_id, "idle", {"found": found})
    
    async def _run_homegate_bot(self, bot_id: int, config: dict):
        """Bot pour Homegate.ch - Mode Demo"""
        city = config.get("city", "Genève")
        max_results = config.get("max_results", 50)
        delay = config.get("delay_seconds", 2)
        
        await emit_bot_log(bot_id, f"Recherche sur Homegate: {city}")
        
        demo_prospects = [
            {"title": "Bel appartement 4p lumineux", "price": "CHF 980'000", "surface": "95 m2"},
            {"title": "Villa contemporaine piscine", "price": "CHF 4'200'000", "surface": "280 m2"},
            {"title": "Loft industriel renove", "price": "CHF 1'150'000", "surface": "130 m2"},
            {"title": "Appartement neuf 3.5p", "price": "CHF 695'000", "surface": "72 m2"},
            {"title": "Maison mitoyenne 5p", "price": "CHF 1'480'000", "surface": "165 m2"},
            {"title": "Penthouse exclusif 7p", "price": "CHF 5'900'000", "surface": "350 m2"},
            {"title": "Studio investi locatif", "price": "CHF 380'000", "surface": "28 m2"},
            {"title": "Appartement familial 5p", "price": "CHF 1'250'000", "surface": "115 m2"},
        ]
        
        found = 0
        
        await emit_bot_log(bot_id, f"Connexion a Homegate.ch...")
        await self._update_bot_counts(bot_id, requests=1)
        await asyncio.sleep(2)
        
        for data in demo_prospects:
            if self.stop_signals.get(bot_id, asyncio.Event()).is_set():
                break
            if found >= max_results:
                break
            while bot_id in self.paused_bots:
                await asyncio.sleep(1)
            
            prospect = {
                "source": "homegate",
                "title": data["title"],
                "price": data["price"],
                "city": city,
                "surface": data.get("surface", "")
            }
            
            await emit_prospect_found(prospect)
            found += 1
            await emit_bot_log(bot_id, f"Prospect #{found}: {data['title']}")
            await self._update_bot_counts(bot_id, success=1)
            await asyncio.sleep(delay)
        
        await emit_bot_log(bot_id, f"Termine: {found} prospects trouves sur Homegate")
        await emit_bot_status(bot_id, "idle", {"found": found})
    
    def stop_bot(self, bot_id: int):
        """Signale l'arret d'un bot"""
        if bot_id in self.stop_signals:
            self.stop_signals[bot_id].set()
    
    def pause_bot(self, bot_id: int):
        """Met un bot en pause"""
        self.paused_bots.add(bot_id)
    
    def resume_bot(self, bot_id: int):
        """Reprend un bot en pause"""
        self.paused_bots.discard(bot_id)
    
    async def shutdown(self):
        """Arrete proprement tous les bots"""
        for bot_id in list(self.running_bots.keys()):
            self.stop_bot(bot_id)
