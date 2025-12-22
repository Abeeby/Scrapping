# =============================================================================
# SCHEDULER SERVICE - Automatisation du scraping
# =============================================================================
# Permet de planifier des scrapes automatiques à intervalles réguliers
# Recommandation: scraper tôt le matin pour détecter les nouvelles annonces
# =============================================================================

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import uuid

from sqlalchemy import Column, String, Integer, DateTime, Boolean, Text, JSON
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database import Base, async_session
from app.core.logger import logger
from app.core.websocket import emit_activity


class ScheduleFrequency(str, Enum):
    """Fréquence de planification."""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    CUSTOM = "custom"


class ScheduleStatus(str, Enum):
    """État d'une planification."""
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


# =============================================================================
# MODEL - Table des planifications
# =============================================================================

class ScrapingSchedule(Base):
    """Planification de scraping automatique."""
    __tablename__ = "scraping_schedules"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    description = Column(Text)
    
    # Configuration du scraping
    source = Column(String, nullable=False)  # anibis, tutti, homegate, etc.
    parameters = Column(JSON, default=dict)  # Paramètres du scraper
    
    # Planification
    frequency = Column(String, default=ScheduleFrequency.DAILY)
    hour = Column(Integer, default=6)  # Heure d'exécution (0-23)
    minute = Column(Integer, default=0)
    days_of_week = Column(JSON, default=list)  # [0,1,2,3,4,5,6] = Lun-Dim
    
    # État
    status = Column(String, default=ScheduleStatus.ACTIVE)
    last_run = Column(DateTime)
    next_run = Column(DateTime)
    last_result = Column(JSON)  # Résultat de la dernière exécution
    
    # Statistiques
    total_runs = Column(Integer, default=0)
    total_leads_found = Column(Integer, default=0)
    success_rate = Column(Integer, default=100)  # %
    
    # Métadonnées
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String)


# =============================================================================
# SCHEDULER ENGINE
# =============================================================================

class ScrapingScheduler:
    """
    Gestionnaire de planifications de scraping.
    
    Usage:
        scheduler = ScrapingScheduler()
        await scheduler.start()
        
        # Créer une planification
        schedule = await scheduler.create_schedule(
            name="Anibis Genève Quotidien",
            source="anibis",
            parameters={"canton": "GE", "transaction_type": "vente"},
            frequency="daily",
            hour=6,
        )
        
        # Arrêter le scheduler
        await scheduler.stop()
    """
    
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._scrapers: Dict[str, Callable] = {}
        self._register_scrapers()
    
    def _register_scrapers(self):
        """Enregistre les scrapers disponibles."""
        self._scrapers = {
            "anibis": self._run_anibis,
            "tutti": self._run_tutti,
            "homegate": self._run_homegate,
            "immoscout24": self._run_immoscout24,
            "searchch": self._run_searchch,
            "scanner": self._run_scanner,
            "swiss_addresses": self._run_swiss_addresses,
        }
    
    async def start(self):
        """Démarre le scheduler."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
        logger.info("[Scheduler] Démarré")
    
    async def stop(self):
        """Arrête le scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[Scheduler] Arrêté")
    
    async def _scheduler_loop(self):
        """Boucle principale du scheduler."""
        while self._running:
            try:
                await self._check_and_run_schedules()
            except Exception as e:
                logger.error(f"[Scheduler] Erreur: {e}")
            
            # Vérifier toutes les minutes
            await asyncio.sleep(60)
    
    async def _check_and_run_schedules(self):
        """Vérifie et exécute les planifications dues."""
        async with async_session() as session:
            now = datetime.utcnow()
            
            # Récupérer les planifications actives dont next_run est passé
            result = await session.execute(
                select(ScrapingSchedule).where(
                    ScrapingSchedule.status == ScheduleStatus.ACTIVE,
                    ScrapingSchedule.next_run <= now
                )
            )
            schedules = result.scalars().all()
            
            for schedule in schedules:
                try:
                    await self._execute_schedule(session, schedule)
                except Exception as e:
                    logger.error(f"[Scheduler] Erreur exécution {schedule.name}: {e}")
    
    async def _execute_schedule(self, session: AsyncSession, schedule: ScrapingSchedule):
        """Exécute une planification."""
        logger.info(f"[Scheduler] Exécution: {schedule.name} ({schedule.source})")
        await emit_activity("scheduler", f"Exécution planifiée: {schedule.name}")
        
        start_time = datetime.utcnow()
        success = False
        leads_found = 0
        error_message = None
        
        try:
            # Récupérer le scraper approprié
            scraper_func = self._scrapers.get(schedule.source)
            if not scraper_func:
                raise ValueError(f"Scraper inconnu: {schedule.source}")
            
            # Exécuter le scraper
            results = await scraper_func(schedule.parameters or {})
            leads_found = len(results) if results else 0
            success = True
            
            logger.info(f"[Scheduler] {schedule.name}: {leads_found} leads trouvés")
            await emit_activity("success", f"Scraping planifié terminé: {leads_found} leads")
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"[Scheduler] Erreur {schedule.name}: {e}")
            await emit_activity("error", f"Erreur scraping planifié: {error_message}")
        
        # Mettre à jour la planification
        schedule.last_run = start_time
        schedule.next_run = self._calculate_next_run(schedule)
        schedule.total_runs += 1
        schedule.total_leads_found += leads_found
        
        # Calculer le taux de succès
        if success:
            schedule.success_rate = int(
                (schedule.success_rate * (schedule.total_runs - 1) + 100) / schedule.total_runs
            )
        else:
            schedule.success_rate = int(
                (schedule.success_rate * (schedule.total_runs - 1)) / schedule.total_runs
            )
        
        schedule.last_result = {
            "success": success,
            "leads_found": leads_found,
            "error": error_message,
            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            "executed_at": start_time.isoformat(),
        }
        
        await session.commit()
    
    def _calculate_next_run(self, schedule: ScrapingSchedule) -> datetime:
        """Calcule la prochaine exécution."""
        now = datetime.utcnow()
        
        if schedule.frequency == ScheduleFrequency.HOURLY:
            # Prochaine heure
            next_run = now.replace(minute=schedule.minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(hours=1)
            return next_run
        
        elif schedule.frequency == ScheduleFrequency.DAILY:
            # Demain à l'heure spécifiée
            next_run = now.replace(
                hour=schedule.hour, 
                minute=schedule.minute, 
                second=0, 
                microsecond=0
            )
            if next_run <= now:
                next_run += timedelta(days=1)
            return next_run
        
        elif schedule.frequency == ScheduleFrequency.WEEKLY:
            # Prochain jour de la semaine spécifié
            days = schedule.days_of_week or [0]  # Lundi par défaut
            next_run = now.replace(
                hour=schedule.hour, 
                minute=schedule.minute, 
                second=0, 
                microsecond=0
            )
            
            current_weekday = now.weekday()
            days_until_next = None
            
            for day in sorted(days):
                if day > current_weekday or (day == current_weekday and next_run > now):
                    days_until_next = day - current_weekday
                    break
            
            if days_until_next is None:
                # Aller à la semaine suivante
                days_until_next = 7 - current_weekday + min(days)
            
            next_run += timedelta(days=days_until_next)
            return next_run
        
        else:
            # Par défaut: demain
            return now + timedelta(days=1)
    
    # =========================================================================
    # SCRAPERS WRAPPERS
    # =========================================================================
    
    async def _run_anibis(self, params: Dict) -> List[Dict]:
        from app.scrapers.anibis import scrape_anibis
        return await scrape_anibis(
            canton=params.get("canton", "GE"),
            transaction_type=params.get("transaction_type", "vente"),
            only_private=params.get("only_private", True),
            limit=params.get("limit", 100),
        )
    
    async def _run_tutti(self, params: Dict) -> List[Dict]:
        from app.scrapers.tutti import scrape_tutti
        return await scrape_tutti(
            canton=params.get("canton", "GE"),
            transaction_type=params.get("transaction_type", "vente"),
            property_type=params.get("property_type", "appartement"),
            only_private=params.get("only_private", True),
            limit=params.get("limit", 100),
        )
    
    async def _run_homegate(self, params: Dict) -> List[Dict]:
        from app.scrapers.homegate import scrape_homegate
        return await scrape_homegate(
            location=params.get("location", "Genève"),
            transaction_type=params.get("transaction_type", "rent"),
            limit=params.get("limit", 100),
        )
    
    async def _run_immoscout24(self, params: Dict) -> List[Dict]:
        from app.scrapers.immoscout24 import scrape_immoscout24
        return await scrape_immoscout24(
            location=params.get("location", "Genève"),
            transaction_type=params.get("transaction_type", "rent"),
            limit=params.get("limit", 100),
        )
    
    async def _run_searchch(self, params: Dict) -> List[Dict]:
        from app.scrapers.searchch import SearchChScraper
        async with SearchChScraper() as scraper:
            return await scraper.search(
                query=params.get("query", ""),
                ville=params.get("ville", "Genève"),
                limit=params.get("limit", 100),
                type_recherche=params.get("type_recherche", "person"),
            )
    
    async def _run_scanner(self, params: Dict) -> List[Dict]:
        from app.scrapers.scanner import scrape_neighborhood
        return await scrape_neighborhood(
            commune=params.get("commune", "Genève"),
            rue=params.get("rue", "all"),
            limit=params.get("limit", 50),
            type_recherche=params.get("type_recherche", "person"),
        )
    
    async def _run_swiss_addresses(self, params: Dict) -> List[Dict]:
        from app.scrapers.swiss_realestate import scrape_swiss_addresses
        return await scrape_swiss_addresses(
            location=params.get("location", "Genève"),
            limit=params.get("limit", 100),
        )
    
    # =========================================================================
    # CRUD API
    # =========================================================================
    
    async def create_schedule(
        self,
        name: str,
        source: str,
        parameters: Dict = None,
        frequency: str = "daily",
        hour: int = 6,
        minute: int = 0,
        days_of_week: List[int] = None,
        created_by: str = None,
    ) -> ScrapingSchedule:
        """Crée une nouvelle planification."""
        async with async_session() as session:
            schedule = ScrapingSchedule(
                name=name,
                source=source,
                parameters=parameters or {},
                frequency=frequency,
                hour=hour,
                minute=minute,
                days_of_week=days_of_week or [],
                created_by=created_by,
                next_run=self._calculate_next_run(ScrapingSchedule(
                    frequency=frequency,
                    hour=hour,
                    minute=minute,
                    days_of_week=days_of_week or [],
                )),
            )
            session.add(schedule)
            await session.commit()
            await session.refresh(schedule)
            
            logger.info(f"[Scheduler] Planification créée: {name}")
            return schedule
    
    async def get_schedules(self, active_only: bool = False) -> List[ScrapingSchedule]:
        """Récupère toutes les planifications."""
        async with async_session() as session:
            query = select(ScrapingSchedule)
            if active_only:
                query = query.where(ScrapingSchedule.status == ScheduleStatus.ACTIVE)
            
            result = await session.execute(query.order_by(ScrapingSchedule.created_at.desc()))
            return result.scalars().all()
    
    async def get_schedule(self, schedule_id: str) -> Optional[ScrapingSchedule]:
        """Récupère une planification par ID."""
        async with async_session() as session:
            result = await session.execute(
                select(ScrapingSchedule).where(ScrapingSchedule.id == schedule_id)
            )
            return result.scalar_one_or_none()
    
    async def update_schedule(
        self,
        schedule_id: str,
        **updates
    ) -> Optional[ScrapingSchedule]:
        """Met à jour une planification."""
        async with async_session() as session:
            result = await session.execute(
                select(ScrapingSchedule).where(ScrapingSchedule.id == schedule_id)
            )
            schedule = result.scalar_one_or_none()
            
            if not schedule:
                return None
            
            for key, value in updates.items():
                if hasattr(schedule, key):
                    setattr(schedule, key, value)
            
            # Recalculer next_run si les paramètres de timing ont changé
            if any(k in updates for k in ["frequency", "hour", "minute", "days_of_week"]):
                schedule.next_run = self._calculate_next_run(schedule)
            
            await session.commit()
            await session.refresh(schedule)
            return schedule
    
    async def delete_schedule(self, schedule_id: str) -> bool:
        """Supprime une planification."""
        async with async_session() as session:
            result = await session.execute(
                select(ScrapingSchedule).where(ScrapingSchedule.id == schedule_id)
            )
            schedule = result.scalar_one_or_none()
            
            if not schedule:
                return False
            
            await session.delete(schedule)
            await session.commit()
            logger.info(f"[Scheduler] Planification supprimée: {schedule.name}")
            return True
    
    async def run_now(self, schedule_id: str) -> Dict[str, Any]:
        """Exécute immédiatement une planification."""
        async with async_session() as session:
            result = await session.execute(
                select(ScrapingSchedule).where(ScrapingSchedule.id == schedule_id)
            )
            schedule = result.scalar_one_or_none()
            
            if not schedule:
                raise ValueError(f"Planification non trouvée: {schedule_id}")
            
            await self._execute_schedule(session, schedule)
            return schedule.last_result


# Instance globale du scheduler
scheduler = ScrapingScheduler()

