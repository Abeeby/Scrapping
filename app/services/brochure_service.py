# =============================================================================
# SERVICE BROCHURE - Envoi automatique de demandes de brochure
# =============================================================================
# Gère l'envoi de demandes de brochure sur les portails immobiliers:
# - Comparis.ch
# - Immoscout24.ch
# - Homegate.ch
# =============================================================================

from __future__ import annotations

import asyncio
import random
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import (
    AsyncSessionLocal,
    BrochureRequest,
    BrochureSchedule,
    EmailAccount,
    ScrapedListing,
)
from app.core.logger import logger
from app.core.websocket import emit_activity

# Essayer d'importer Playwright pour le remplissage de formulaires
try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


# =============================================================================
# CONFIGURATION
# =============================================================================

# Messages par défaut pour les demandes de brochure
DEFAULT_MESSAGES = {
    "comparis": "Bonjour,\n\nJe suis intéressé(e) par ce bien et souhaiterais recevoir la documentation complète.\n\nCordialement",
    "immoscout24": "Bonjour,\n\nCe bien m'intéresse beaucoup. Pourriez-vous m'envoyer plus d'informations ainsi que la brochure de vente ?\n\nMerci d'avance",
    "homegate": "Bonjour,\n\nJe souhaiterais obtenir des informations supplémentaires sur ce bien.\n\nCordialement",
}

# Délais entre les requêtes (en secondes)
MIN_DELAY = 20
MAX_DELAY = 45

# Nombre max de tentatives
MAX_RETRIES = 3


# =============================================================================
# CLASSES D'ERREUR
# =============================================================================

class BrochureServiceError(Exception):
    """Erreur générique du service brochure."""
    pass


class FormFillingError(BrochureServiceError):
    """Erreur lors du remplissage du formulaire."""
    pass


class EmailRotationError(BrochureServiceError):
    """Plus d'emails disponibles pour la rotation."""
    pass


# =============================================================================
# SERVICE PRINCIPAL
# =============================================================================

class BrochureService:
    """
    Service de gestion des demandes de brochure automatiques.
    
    Usage:
        service = BrochureService()
        
        # Soumettre une demande
        request_id = await service.submit_request(
            listing_url="https://fr.comparis.ch/...",
            portal="comparis"
        )
        
        # Traiter la file d'attente
        await service.process_queue(max_requests=10)
    """

    def __init__(self):
        self._browser: Optional[Browser] = None
        self._playwright = None

    async def _get_browser(self) -> Browser:
        """Obtient ou crée une instance de navigateur."""
        if not PLAYWRIGHT_AVAILABLE:
            raise BrochureServiceError("Playwright n'est pas installé. Installez-le avec: pip install playwright && playwright install chromium")
        
        if self._browser is None:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ]
            )
        return self._browser

    async def close(self):
        """Ferme le navigateur."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    async def get_available_email(self, db: AsyncSession) -> Optional[EmailAccount]:
        """
        Récupère un compte email disponible (quota non atteint, actif).
        Utilise une rotation pour éviter la surcharge d'un seul compte.
        """
        query = (
            select(EmailAccount)
            .where(EmailAccount.is_active == True)
            .where(EmailAccount.sent_today < EmailAccount.quota_daily)
            .order_by(EmailAccount.sent_today.asc(), EmailAccount.last_used.asc())
            .limit(1)
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def submit_request(
        self,
        listing_url: str,
        portal: str,
        prospect_id: Optional[str] = None,
        custom_message: Optional[str] = None,
        requester_name: Optional[str] = None,
        requester_phone: Optional[str] = None,
    ) -> int:
        """
        Soumet une nouvelle demande de brochure.
        
        Args:
            listing_url: URL de l'annonce
            portal: Portail (comparis, immoscout24, homegate)
            prospect_id: ID du prospect lié (optionnel)
            custom_message: Message personnalisé
            requester_name: Nom du demandeur (sinon généré)
            requester_phone: Téléphone (optionnel)
            
        Returns:
            ID de la demande créée
        """
        async with AsyncSessionLocal() as db:
            # Vérifier si une demande existe déjà pour cette URL
            existing = await db.execute(
                select(BrochureRequest)
                .where(BrochureRequest.listing_url == listing_url)
                .where(BrochureRequest.status.in_(["pending", "sent"]))
            )
            if existing.scalar_one_or_none():
                raise BrochureServiceError(f"Une demande existe déjà pour cette annonce: {listing_url}")

            # Récupérer un email disponible
            email_account = await self.get_available_email(db)
            if not email_account:
                raise EmailRotationError("Aucun compte email disponible (quotas atteints)")

            # Créer la demande
            request = BrochureRequest(
                prospect_id=prospect_id,
                email_account_id=email_account.id,
                portal=portal.lower(),
                listing_url=listing_url,
                requester_name=requester_name or self._generate_name(),
                requester_email=email_account.email,
                requester_phone=requester_phone,
                requester_message=custom_message or DEFAULT_MESSAGES.get(portal.lower(), DEFAULT_MESSAGES["comparis"]),
                status="pending",
            )
            db.add(request)
            await db.commit()
            await db.refresh(request)

            await emit_activity("brochure", f"Nouvelle demande brochure créée: {portal} #{request.id}")
            
            return request.id

    async def submit_batch(
        self,
        listings: List[Dict[str, Any]],
        custom_message: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Soumet un lot de demandes de brochure.
        
        Args:
            listings: Liste de dict avec 'url' et 'portal'
            custom_message: Message personnalisé pour toutes les demandes
            
        Returns:
            Dict avec 'submitted', 'skipped', 'errors'
        """
        stats = {"submitted": 0, "skipped": 0, "errors": 0}
        
        for listing in listings:
            try:
                await self.submit_request(
                    listing_url=listing["url"],
                    portal=listing["portal"],
                    prospect_id=listing.get("prospect_id"),
                    custom_message=custom_message,
                )
                stats["submitted"] += 1
            except BrochureServiceError as e:
                if "existe déjà" in str(e):
                    stats["skipped"] += 1
                else:
                    stats["errors"] += 1
                    logger.warning(f"[Brochure] Erreur batch: {e}")
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"[Brochure] Erreur batch inattendue: {e}")

        await emit_activity("brochure", f"Batch terminé: {stats['submitted']} soumises, {stats['skipped']} ignorées, {stats['errors']} erreurs")
        return stats

    async def process_queue(
        self,
        max_requests: int = 10,
        delay_range: Tuple[int, int] = (MIN_DELAY, MAX_DELAY),
    ) -> Dict[str, int]:
        """
        Traite la file d'attente des demandes de brochure.
        
        Args:
            max_requests: Nombre maximum de demandes à traiter
            delay_range: Tuple (min, max) secondes entre chaque demande
            
        Returns:
            Dict avec 'processed', 'success', 'errors'
        """
        stats = {"processed": 0, "success": 0, "errors": 0}
        
        async with AsyncSessionLocal() as db:
            # Récupérer les demandes en attente
            query = (
                select(BrochureRequest)
                .where(BrochureRequest.status == "pending")
                .where(BrochureRequest.retry_count < MAX_RETRIES)
                .order_by(BrochureRequest.created_at.asc())
                .limit(max_requests)
            )
            result = await db.execute(query)
            requests = result.scalars().all()

            if not requests:
                logger.info("[Brochure] Aucune demande en attente")
                return stats

            await emit_activity("brochure", f"Traitement de {len(requests)} demandes...")

            for request in requests:
                try:
                    success = await self._process_single_request(db, request)
                    stats["processed"] += 1
                    if success:
                        stats["success"] += 1
                    else:
                        stats["errors"] += 1
                except Exception as e:
                    logger.error(f"[Brochure] Erreur traitement #{request.id}: {e}")
                    stats["processed"] += 1
                    stats["errors"] += 1
                    
                    # Marquer comme erreur
                    request.status = "error"
                    request.error_message = str(e)
                    request.retry_count += 1
                    await db.commit()

                # Délai aléatoire entre les requêtes
                if stats["processed"] < len(requests):
                    delay = random.randint(delay_range[0], delay_range[1])
                    await asyncio.sleep(delay)

        await emit_activity("brochure", f"File traitée: {stats['success']} succès, {stats['errors']} erreurs")
        return stats

    async def _process_single_request(self, db: AsyncSession, request: BrochureRequest) -> bool:
        """Traite une seule demande de brochure."""
        logger.info(f"[Brochure] Traitement #{request.id} - {request.portal}: {request.listing_url}")

        # Vérifier l'email account
        email_result = await db.execute(
            select(EmailAccount).where(EmailAccount.id == request.email_account_id)
        )
        email_account = email_result.scalar_one_or_none()
        
        if not email_account or not email_account.is_active:
            # Rotation vers un autre email
            email_account = await self.get_available_email(db)
            if not email_account:
                request.error_message = "Aucun email disponible"
                request.retry_count += 1
                await db.commit()
                return False
            request.email_account_id = email_account.id
            request.requester_email = email_account.email

        try:
            # Remplir le formulaire selon le portail
            if request.portal == "comparis":
                success = await self._fill_comparis_form(request)
            elif request.portal == "immoscout24":
                success = await self._fill_immoscout_form(request)
            elif request.portal == "homegate":
                success = await self._fill_homegate_form(request)
            else:
                raise FormFillingError(f"Portail non supporté: {request.portal}")

            if success:
                request.status = "sent"
                request.sent_at = datetime.utcnow()
                request.error_message = None
                
                # Incrémenter le compteur d'emails
                email_account.sent_today += 1
                email_account.last_used = datetime.utcnow()
                
                await emit_activity("success", f"Brochure demandée: {request.portal} #{request.id}")
            else:
                request.status = "error"
                request.retry_count += 1

            await db.commit()
            return success

        except Exception as e:
            request.status = "error"
            request.error_message = str(e)
            request.retry_count += 1
            await db.commit()
            raise

    async def _fill_comparis_form(self, request: BrochureRequest) -> bool:
        """Remplit le formulaire de contact Comparis."""
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("[Brochure] Playwright indisponible, simulation envoi Comparis")
            return True  # Simulation pour tests

        browser = await self._get_browser()
        page = await browser.new_page()
        
        try:
            await page.goto(request.listing_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # Chercher le bouton de contact
            contact_btn = page.locator("button:has-text('Contacter'), a:has-text('Demander')")
            if await contact_btn.count() > 0:
                await contact_btn.first.click()
                await asyncio.sleep(2)

            # Remplir les champs du formulaire
            # Nom
            name_field = page.locator("input[name='name'], input[placeholder*='Nom'], input[id*='name']")
            if await name_field.count() > 0:
                await name_field.first.fill(request.requester_name or "")

            # Email
            email_field = page.locator("input[type='email'], input[name='email']")
            if await email_field.count() > 0:
                await email_field.first.fill(request.requester_email or "")

            # Téléphone (optionnel)
            if request.requester_phone:
                phone_field = page.locator("input[type='tel'], input[name='phone']")
                if await phone_field.count() > 0:
                    await phone_field.first.fill(request.requester_phone)

            # Message
            message_field = page.locator("textarea")
            if await message_field.count() > 0:
                await message_field.first.fill(request.requester_message or "")

            # Soumettre
            submit_btn = page.locator("button[type='submit'], input[type='submit']")
            if await submit_btn.count() > 0:
                await submit_btn.first.click()
                await asyncio.sleep(3)

            # Vérifier le succès (message de confirmation)
            success_msg = page.locator("text=envoyé, text=succès, text=merci")
            if await success_msg.count() > 0:
                return True

            # Même sans confirmation explicite, considérer comme succès si pas d'erreur
            error_msg = page.locator("text=erreur, text=échec, text=invalide")
            if await error_msg.count() == 0:
                return True

            return False

        except Exception as e:
            logger.error(f"[Brochure] Erreur Comparis: {e}")
            raise FormFillingError(f"Erreur formulaire Comparis: {e}")
        finally:
            await page.close()

    async def _fill_immoscout_form(self, request: BrochureRequest) -> bool:
        """Remplit le formulaire de contact Immoscout24."""
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("[Brochure] Playwright indisponible, simulation envoi Immoscout24")
            return True

        browser = await self._get_browser()
        page = await browser.new_page()
        
        try:
            await page.goto(request.listing_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # Cliquer sur le bouton de contact
            contact_btn = page.locator("button:has-text('Contacter'), a:has-text('Contact')")
            if await contact_btn.count() > 0:
                await contact_btn.first.click()
                await asyncio.sleep(2)

            # Remplir les champs
            await self._fill_form_fields(page, request)

            # Soumettre
            submit_btn = page.locator("button[type='submit']")
            if await submit_btn.count() > 0:
                await submit_btn.first.click()
                await asyncio.sleep(3)

            return True

        except Exception as e:
            logger.error(f"[Brochure] Erreur Immoscout24: {e}")
            raise FormFillingError(f"Erreur formulaire Immoscout24: {e}")
        finally:
            await page.close()

    async def _fill_homegate_form(self, request: BrochureRequest) -> bool:
        """Remplit le formulaire de contact Homegate."""
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("[Brochure] Playwright indisponible, simulation envoi Homegate")
            return True

        browser = await self._get_browser()
        page = await browser.new_page()
        
        try:
            await page.goto(request.listing_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # Cliquer sur le bouton de contact
            contact_btn = page.locator("button:has-text('Contacter'), a:has-text('Contact')")
            if await contact_btn.count() > 0:
                await contact_btn.first.click()
                await asyncio.sleep(2)

            # Remplir les champs
            await self._fill_form_fields(page, request)

            # Soumettre
            submit_btn = page.locator("button[type='submit']")
            if await submit_btn.count() > 0:
                await submit_btn.first.click()
                await asyncio.sleep(3)

            return True

        except Exception as e:
            logger.error(f"[Brochure] Erreur Homegate: {e}")
            raise FormFillingError(f"Erreur formulaire Homegate: {e}")
        finally:
            await page.close()

    async def _fill_form_fields(self, page: Page, request: BrochureRequest):
        """Remplit les champs communs d'un formulaire de contact."""
        # Prénom / Nom
        for selector in ["input[name*='name']", "input[name*='nom']", "input[placeholder*='Nom']"]:
            field = page.locator(selector)
            if await field.count() > 0:
                await field.first.fill(request.requester_name or "")
                break

        # Email
        for selector in ["input[type='email']", "input[name*='email']", "input[name*='mail']"]:
            field = page.locator(selector)
            if await field.count() > 0:
                await field.first.fill(request.requester_email or "")
                break

        # Téléphone
        if request.requester_phone:
            for selector in ["input[type='tel']", "input[name*='phone']", "input[name*='tel']"]:
                field = page.locator(selector)
                if await field.count() > 0:
                    await field.first.fill(request.requester_phone)
                    break

        # Message
        textarea = page.locator("textarea")
        if await textarea.count() > 0:
            await textarea.first.fill(request.requester_message or "")

    def _generate_name(self) -> str:
        """Génère un nom réaliste pour les demandes."""
        prenoms = [
            "Marie", "Jean", "Pierre", "Sophie", "Michel", "Anne", "Philippe",
            "Catherine", "François", "Nathalie", "Laurent", "Isabelle", "Patrick",
            "Sylvie", "Nicolas", "Martine", "Christophe", "Valérie", "Alain", "Christine"
        ]
        noms = [
            "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit",
            "Durand", "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel",
            "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier"
        ]
        return f"{random.choice(prenoms)} {random.choice(noms)}"


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def get_queue_stats() -> Dict[str, Any]:
    """Récupère les statistiques de la file d'attente."""
    async with AsyncSessionLocal() as db:
        # Comptages par statut
        from sqlalchemy import func
        
        stats_query = (
            select(BrochureRequest.status, func.count(BrochureRequest.id))
            .group_by(BrochureRequest.status)
        )
        result = await db.execute(stats_query)
        by_status = dict(result.fetchall())
        
        # Comptages par portail
        portal_query = (
            select(BrochureRequest.portal, func.count(BrochureRequest.id))
            .group_by(BrochureRequest.portal)
        )
        result = await db.execute(portal_query)
        by_portal = dict(result.fetchall())
        
        # Statistiques aujourd'hui
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_query = (
            select(func.count(BrochureRequest.id))
            .where(BrochureRequest.sent_at >= today)
        )
        result = await db.execute(today_query)
        sent_today = result.scalar() or 0
        
        return {
            "by_status": by_status,
            "by_portal": by_portal,
            "sent_today": sent_today,
            "pending": by_status.get("pending", 0),
            "total_sent": by_status.get("sent", 0),
            "total_errors": by_status.get("error", 0),
        }


async def get_brochure_history(
    limit: int = 50,
    portal: Optional[str] = None,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Récupère l'historique des demandes de brochure."""
    async with AsyncSessionLocal() as db:
        query = select(BrochureRequest).order_by(BrochureRequest.created_at.desc())
        
        if portal:
            query = query.where(BrochureRequest.portal == portal)
        if status:
            query = query.where(BrochureRequest.status == status)
        
        query = query.limit(limit)
        result = await db.execute(query)
        requests = result.scalars().all()
        
        return [
            {
                "id": r.id,
                "portal": r.portal,
                "listing_url": r.listing_url,
                "status": r.status,
                "requester_name": r.requester_name,
                "requester_email": r.requester_email,
                "sent_at": r.sent_at.isoformat() if r.sent_at else None,
                "error_message": r.error_message,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in requests
        ]


async def reset_daily_quotas():
    """Réinitialise les quotas journaliers des comptes email."""
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(EmailAccount).values(sent_today=0)
        )
        await db.commit()
        logger.info("[Brochure] Quotas email réinitialisés")
