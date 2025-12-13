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
            elif bot_type == "brochure":
                await self._run_brochure_bot(bot_id, config)
            elif bot_type == "mass_scraper":
                await self._run_mass_scraper_bot(bot_id, config)
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
    
    async def _run_brochure_bot(self, bot_id: int, config: dict):
        """
        Bot pour l'envoi automatique de demandes de brochure.
        
        Config attendue:
        - portal_filter: liste des portails (["comparis", "immoscout24", "homegate"])
        - canton_filter: liste des cantons (["GE", "VD"])
        - max_requests: nombre max de demandes par run
        - delay_seconds: délai entre chaque demande
        - auto_rotate_email: rotation automatique des emails (bool)
        """
        from app.services.brochure_service import BrochureService
        from app.core.database import ScrapedListing, BrochureRequest
        from sqlalchemy import select, and_
        
        portal_filter = config.get("portal_filter", ["comparis", "immoscout24", "homegate"])
        canton_filter = config.get("canton_filter", ["GE", "VD"])
        max_requests = config.get("max_requests", 20)
        delay = config.get("delay_seconds", 30)
        
        await emit_bot_log(bot_id, f"BrochureBot démarré - Portails: {portal_filter}, Cantons: {canton_filter}")
        await emit_bot_status(bot_id, "running", {"phase": "initialisation"})
        
        service = BrochureService()
        processed = 0
        success = 0
        errors = 0
        
        try:
            async with AsyncSessionLocal() as db:
                # Récupérer les annonces non encore traitées
                query = (
                    select(ScrapedListing)
                    .where(ScrapedListing.brochure_requested == False)
                    .where(ScrapedListing.portal.in_(portal_filter))
                )
                if canton_filter:
                    query = query.where(ScrapedListing.canton.in_(canton_filter))
                query = query.order_by(ScrapedListing.scraped_at.desc()).limit(max_requests)
                
                result = await db.execute(query)
                listings = result.scalars().all()
                
                if not listings:
                    await emit_bot_log(bot_id, "Aucune annonce en attente de traitement")
                    await emit_bot_status(bot_id, "idle", {"processed": 0})
                    return
                
                await emit_bot_log(bot_id, f"{len(listings)} annonces à traiter")
                
                for listing in listings:
                    if self.stop_signals.get(bot_id, asyncio.Event()).is_set():
                        await emit_bot_log(bot_id, "Bot arrêté par l'utilisateur")
                        break
                    
                    while bot_id in self.paused_bots:
                        await asyncio.sleep(1)
                    
                    try:
                        await emit_bot_log(bot_id, f"Traitement: {listing.title or listing.url[:50]}...")
                        
                        # Soumettre la demande
                        request_id = await service.submit_request(
                            listing_url=listing.url,
                            portal=listing.portal,
                        )
                        
                        # Traiter immédiatement
                        stats = await service.process_queue(max_requests=1)
                        
                        if stats.get("success", 0) > 0:
                            listing.brochure_requested = True
                            listing.brochure_request_id = request_id
                            await db.commit()
                            
                            success += 1
                            await emit_bot_log(bot_id, f"✓ Demande envoyée: {listing.portal}")
                            await self._update_bot_counts(bot_id, requests=1, success=1)
                        else:
                            errors += 1
                            await emit_bot_log(bot_id, f"✗ Échec: {listing.url[:50]}", "warning")
                            await self._update_bot_counts(bot_id, requests=1, errors=1)
                        
                        processed += 1
                        
                        # Progress update
                        await emit_bot_status(bot_id, "running", {
                            "processed": processed,
                            "total": len(listings),
                            "success": success,
                            "errors": errors
                        })
                        
                        # Délai entre les demandes
                        if processed < len(listings):
                            await emit_bot_log(bot_id, f"Attente {delay}s avant la prochaine demande...")
                            await asyncio.sleep(delay)
                    
                    except Exception as e:
                        errors += 1
                        await emit_bot_log(bot_id, f"Erreur: {str(e)}", "error")
                        await self._update_bot_counts(bot_id, errors=1)
                
                await emit_bot_log(bot_id, f"Terminé: {success} envoyées, {errors} erreurs sur {processed} traitées")
        
        except Exception as e:
            await emit_bot_log(bot_id, f"Erreur critique: {str(e)}", "error")
        finally:
            await service.close()
        
        await emit_bot_status(bot_id, "idle", {
            "processed": processed,
            "success": success,
            "errors": errors
        })
    
    async def _run_mass_scraper_bot(self, bot_id: int, config: dict):
        """
        Bot pour le scraping massif de rues/quartiers.
        
        Config attendue:
        - canton: "GE" ou "VD"
        - commune: commune cible ou "all"
        - source: "searchch", "localch", "rf"
        - delay_seconds: délai entre chaque rue
        - save_to_prospects: sauvegarder directement en prospects (bool)
        """
        from app.core.database import Prospect, MassScrapingJob
        from app.scrapers.searchch import SearchChScraper
        from sqlalchemy import select
        import uuid
        
        canton = config.get("canton", "GE")
        commune = config.get("commune", "all")
        source = config.get("source", "searchch")
        delay = config.get("delay_seconds", 3)
        save_to_prospects = config.get("save_to_prospects", True)
        
        await emit_bot_log(bot_id, f"MassScraper démarré - Canton: {canton}, Commune: {commune}")
        await emit_bot_status(bot_id, "running", {"phase": "initialisation"})
        
        # Récupérer la liste des rues
        try:
            from app.data.streets_ge_vd import get_streets
            streets = get_streets(canton, commune if commune != "all" else None)
        except ImportError:
            # Fallback si le module n'existe pas encore
            streets = ["Rue du Rhône", "Rue de la Croix-d'Or", "Boulevard des Philosophes"]
            await emit_bot_log(bot_id, "Module streets_ge_vd non disponible, utilisation de rues de test", "warning")
        
        if not streets:
            await emit_bot_log(bot_id, "Aucune rue trouvée pour cette configuration")
            await emit_bot_status(bot_id, "idle", {"processed": 0})
            return
        
        await emit_bot_log(bot_id, f"{len(streets)} rues à scraper")
        
        total_found = 0
        processed_streets = 0
        
        async with AsyncSessionLocal() as db:
            # Créer un job de scraping
            job = MassScrapingJob(
                name=f"Scraping {canton} - {commune}",
                canton=canton,
                commune=commune if commune != "all" else None,
                source=source,
                status="running",
                total_streets=len(streets),
                started_at=datetime.utcnow()
            )
            db.add(job)
            await db.commit()
            job_id = job.id
            
            try:
                async with SearchChScraper() as scraper:
                    for street in streets:
                        if self.stop_signals.get(bot_id, asyncio.Event()).is_set():
                            await emit_bot_log(bot_id, "Bot arrêté par l'utilisateur")
                            break
                        
                        while bot_id in self.paused_bots:
                            await asyncio.sleep(1)
                        
                        try:
                            await emit_bot_log(bot_id, f"Scraping: {street}...")
                            job.current_street = street
                            
                            # Construire la requête
                            ville = commune if commune != "all" else ("Genève" if canton == "GE" else "Lausanne")
                            results = await scraper.search(
                                query=street,
                                ville=ville,
                                limit=50,
                                type_recherche="person"
                            )
                            
                            found_in_street = 0
                            
                            if save_to_prospects and results:
                                for r in results:
                                    # Vérifier doublon
                                    existing = await db.execute(
                                        select(Prospect).where(
                                            and_(
                                                Prospect.nom == r.get("nom", ""),
                                                Prospect.adresse == r.get("adresse", ""),
                                                Prospect.ville == r.get("ville", ville)
                                            )
                                        )
                                    )
                                    if existing.scalar_one_or_none():
                                        continue
                                    
                                    # Créer le prospect
                                    prospect = Prospect(
                                        id=f"mass-{uuid.uuid4().hex[:8]}",
                                        nom=r.get("nom", ""),
                                        prenom=r.get("prenom", ""),
                                        telephone=r.get("telephone", ""),
                                        email=r.get("email", ""),
                                        adresse=r.get("adresse", street),
                                        code_postal=r.get("code_postal", ""),
                                        ville=r.get("ville", ville),
                                        canton=canton,
                                        source=f"MassScraper ({source})",
                                    )
                                    db.add(prospect)
                                    found_in_street += 1
                                
                                await db.commit()
                            
                            total_found += found_in_street or len(results)
                            processed_streets += 1
                            
                            job.processed_streets = processed_streets
                            job.total_found = total_found
                            await db.commit()
                            
                            await self._update_bot_counts(bot_id, requests=1, success=1 if results else 0)
                            
                            if found_in_street > 0:
                                await emit_bot_log(bot_id, f"  → {found_in_street} nouveaux prospects")
                            
                            # Progress
                            await emit_bot_status(bot_id, "running", {
                                "processed": processed_streets,
                                "total": len(streets),
                                "found": total_found,
                                "current_street": street
                            })
                            
                            # Délai anti rate-limit
                            await asyncio.sleep(delay)
                        
                        except Exception as e:
                            await emit_bot_log(bot_id, f"Erreur sur {street}: {str(e)}", "warning")
                            await self._update_bot_counts(bot_id, errors=1)
                
                # Marquer le job comme terminé
                job.status = "completed"
                job.completed_at = datetime.utcnow()
                await db.commit()
            
            except Exception as e:
                job.status = "error"
                job.error_message = str(e)
                await db.commit()
                raise
        
        await emit_bot_log(bot_id, f"Terminé: {total_found} prospects trouvés sur {processed_streets} rues")
        await emit_bot_status(bot_id, "idle", {
            "processed": processed_streets,
            "found": total_found
        })
    
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
