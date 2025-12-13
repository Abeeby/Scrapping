# =============================================================================
# SERVICE EMAIL PARSER - Parsing IMAP des réponses brochures
# =============================================================================
# Parse automatiquement les emails de réponse des portails immobiliers
# pour extraire les adresses des biens et déclencher le matching propriétaire
# =============================================================================

from __future__ import annotations

import asyncio
import email
import imaplib
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.header import decode_header
from typing import Any, Dict, List, Optional, Tuple
import base64

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, BrochureRequest, EmailAccount, ScrapedListing
from app.core.logger import logger
from app.core.websocket import emit_activity


# =============================================================================
# CONFIGURATION
# =============================================================================

# Patterns pour détecter les emails de réponse brochure
SENDER_PATTERNS = {
    "comparis": [
        r"@comparis\.ch",
        r"@immobilier\.comparis\.ch",
        r"noreply.*comparis",
    ],
    "immoscout24": [
        r"@immoscout24\.ch",
        r"@scout24\.ch",
        r"noreply.*immoscout",
    ],
    "homegate": [
        r"@homegate\.ch",
        r"noreply.*homegate",
    ],
    "generic_agency": [
        r"@.*immobil.*\.ch",
        r"@.*realestate.*\.ch",
        r"@.*immo.*\.ch",
    ],
}

# Patterns pour extraire les adresses suisses
ADDRESS_PATTERNS = [
    # Format: "Rue du Lac 12, 1000 Lausanne"
    r"((?:rue|avenue|chemin|route|place|boulevard|quai|allée|impasse|passage)\s+[^,\n]+\s+\d+[a-z]?),?\s*(\d{4})\s+([A-Za-zÀ-ÿ\-\s]+)",
    # Format: "12, rue du Lac, 1000 Lausanne"
    r"(\d+[a-z]?),?\s*((?:rue|avenue|chemin|route|place|boulevard|quai)\s+[^,\n]+),?\s*(\d{4})\s+([A-Za-zÀ-ÿ\-\s]+)",
    # Format générique: "Rue du Lac 12\n1000 Lausanne"
    r"([A-Za-zÀ-ÿ\-\s]+\s+\d+[a-z]?)\s*[\n,]\s*(\d{4})\s+([A-Za-zÀ-ÿ\-\s]+)",
    # Format avec "Adresse:"
    r"[Aa]dresse\s*[:\-]\s*([^\n]+\d+[a-z]?[,\s]+\d{4}\s+[A-Za-zÀ-ÿ\-\s]+)",
    # Format avec "Situation:" ou "Localisation:"
    r"(?:Situation|Localisation|Emplacement)\s*[:\-]\s*([^\n]+)",
    # Format NPA ville en début
    r"(\d{4})\s+([A-Za-zÀ-ÿ\-\s]+),\s*([^,\n]+\d+[a-z]?)",
]

# Patterns pour extraire des informations supplémentaires
PRICE_PATTERNS = [
    r"(?:Prix|Price|CHF|Fr\.)\s*[:\-]?\s*([\d\''´]+(?:\.\d{2})?)\s*(?:CHF|Fr\.)?",
    r"([\d\''´]+)\s*(?:CHF|Fr\.)\s*/\s*(?:mois|m|month)",
]

ROOMS_PATTERNS = [
    r"(\d+(?:[.,]\d)?)\s*(?:pièces?|rooms?|Zimmer)",
    r"(?:pièces?|rooms?)\s*[:\-]?\s*(\d+(?:[.,]\d)?)",
]

SURFACE_PATTERNS = [
    r"(\d+)\s*m[²2]",
    r"Surface\s*[:\-]?\s*(\d+)",
]


@dataclass
class ParsedEmail:
    """Email parsé avec les données extraites."""
    # Métadonnées email
    message_id: str
    subject: str
    sender: str
    received_at: datetime
    body_text: str
    body_html: str = ""
    
    # Données extraites
    portal: str = ""
    is_brochure_response: bool = False
    
    # Adresse extraite
    extracted_address: str = ""
    extracted_npa: str = ""
    extracted_city: str = ""
    extracted_full_address: str = ""
    
    # Détails du bien
    extracted_price: Optional[float] = None
    extracted_rooms: Optional[float] = None
    extracted_surface: Optional[float] = None
    
    # Pièces jointes
    attachments: List[Dict[str, Any]] = field(default_factory=list)
    has_pdf_brochure: bool = False
    
    # Matching
    matched_request_id: Optional[int] = None
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "subject": self.subject,
            "sender": self.sender,
            "received_at": self.received_at.isoformat() if self.received_at else None,
            "portal": self.portal,
            "is_brochure_response": self.is_brochure_response,
            "extracted_address": self.extracted_full_address,
            "extracted_npa": self.extracted_npa,
            "extracted_city": self.extracted_city,
            "extracted_price": self.extracted_price,
            "extracted_rooms": self.extracted_rooms,
            "extracted_surface": self.extracted_surface,
            "has_pdf_brochure": self.has_pdf_brochure,
            "attachments_count": len(self.attachments),
            "matched_request_id": self.matched_request_id,
            "confidence": self.confidence,
        }


@dataclass
class EmailParseResult:
    """Résultat du parsing d'emails."""
    total_emails: int
    brochure_responses: int
    addresses_extracted: int
    requests_matched: int
    parsed_emails: List[ParsedEmail]
    errors: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_emails": self.total_emails,
            "brochure_responses": self.brochure_responses,
            "addresses_extracted": self.addresses_extracted,
            "requests_matched": self.requests_matched,
            "emails": [e.to_dict() for e in self.parsed_emails],
            "errors_count": len(self.errors),
        }


class EmailParserService:
    """
    Service de parsing des emails de réponse brochure.
    
    Fonctionnalités:
    - Connexion IMAP aux comptes email
    - Détection des réponses de portails immobiliers
    - Extraction automatique des adresses
    - Extraction des pièces jointes (brochures PDF)
    - Matching avec les demandes de brochure
    
    Usage:
        service = EmailParserService()
        
        # Parser les emails d'un compte
        result = await service.parse_account_emails(email_account_id=1)
        
        # Parser tous les comptes actifs
        result = await service.parse_all_accounts()
    """

    def __init__(self):
        self._connections: Dict[int, imaplib.IMAP4_SSL] = {}

    async def parse_account_emails(
        self,
        email_account_id: int,
        days_back: int = 7,
        mark_as_read: bool = False,
        folder: str = "INBOX",
    ) -> EmailParseResult:
        """
        Parse les emails d'un compte.
        
        Args:
            email_account_id: ID du compte email
            days_back: Nombre de jours à remonter
            mark_as_read: Marquer les emails comme lus
            folder: Dossier IMAP à parser
            
        Returns:
            EmailParseResult
        """
        result = EmailParseResult(
            total_emails=0,
            brochure_responses=0,
            addresses_extracted=0,
            requests_matched=0,
            parsed_emails=[],
            errors=[],
        )
        
        async with AsyncSessionLocal() as db:
            # Récupérer le compte email
            account_result = await db.execute(
                select(EmailAccount).where(EmailAccount.id == email_account_id)
            )
            account = account_result.scalar_one_or_none()
            
            if not account:
                result.errors.append(f"Compte email {email_account_id} non trouvé")
                return result
            
            if not account.imap_server:
                result.errors.append(f"Serveur IMAP non configuré pour {account.email}")
                return result
            
            try:
                # Connexion IMAP
                imap = await self._connect_imap(
                    server=account.imap_server,
                    email=account.email,
                    password=account.password,
                )
                
                # Sélectionner le dossier
                imap.select(folder)
                
                # Rechercher les emails récents
                since_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%d-%b-%Y")
                _, message_ids = imap.search(None, f'(SINCE "{since_date}")')
                
                if not message_ids[0]:
                    logger.info(f"[EmailParser] Aucun email récent pour {account.email}")
                    return result
                
                ids = message_ids[0].split()
                result.total_emails = len(ids)
                
                logger.info(f"[EmailParser] {len(ids)} emails à parser pour {account.email}")
                
                # Parser chaque email
                for msg_id in ids[-100:]:  # Limiter aux 100 derniers
                    try:
                        parsed = await self._parse_single_email(imap, msg_id, db)
                        
                        if parsed:
                            result.parsed_emails.append(parsed)
                            
                            if parsed.is_brochure_response:
                                result.brochure_responses += 1
                                
                                if parsed.extracted_full_address:
                                    result.addresses_extracted += 1
                                
                                if parsed.matched_request_id:
                                    result.requests_matched += 1
                                    
                                    # Mettre à jour la demande de brochure
                                    await self._update_brochure_request(
                                        db, parsed
                                    )
                            
                            if mark_as_read:
                                imap.store(msg_id, '+FLAGS', '\\Seen')
                                
                    except Exception as e:
                        logger.warning(f"[EmailParser] Erreur email {msg_id}: {e}")
                        result.errors.append(str(e))
                
                # Fermer la connexion
                imap.close()
                imap.logout()
                
            except Exception as e:
                logger.error(f"[EmailParser] Erreur IMAP {account.email}: {e}")
                result.errors.append(str(e))
        
        return result

    async def parse_all_accounts(
        self,
        days_back: int = 7,
    ) -> Dict[str, Any]:
        """Parse les emails de tous les comptes actifs."""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(EmailAccount).where(EmailAccount.is_active == True)
            )
            accounts = result.scalars().all()
        
        global_stats = {
            "accounts_processed": 0,
            "total_emails": 0,
            "brochure_responses": 0,
            "addresses_extracted": 0,
            "requests_matched": 0,
            "errors": [],
        }
        
        for account in accounts:
            try:
                result = await self.parse_account_emails(
                    email_account_id=account.id,
                    days_back=days_back,
                )
                
                global_stats["accounts_processed"] += 1
                global_stats["total_emails"] += result.total_emails
                global_stats["brochure_responses"] += result.brochure_responses
                global_stats["addresses_extracted"] += result.addresses_extracted
                global_stats["requests_matched"] += result.requests_matched
                global_stats["errors"].extend(result.errors)
                
            except Exception as e:
                logger.error(f"[EmailParser] Erreur compte {account.email}: {e}")
                global_stats["errors"].append(f"{account.email}: {e}")
        
        await emit_activity(
            "email_parser",
            f"Parsing terminé: {global_stats['addresses_extracted']} adresses extraites"
        )
        
        return global_stats

    async def _connect_imap(
        self,
        server: str,
        email: str,
        password: str,
    ) -> imaplib.IMAP4_SSL:
        """Connecte au serveur IMAP."""
        # Exécuter en thread pour ne pas bloquer
        loop = asyncio.get_event_loop()
        
        def connect():
            imap = imaplib.IMAP4_SSL(server)
            imap.login(email, password)
            return imap
        
        return await loop.run_in_executor(None, connect)

    async def _parse_single_email(
        self,
        imap: imaplib.IMAP4_SSL,
        msg_id: bytes,
        db: AsyncSession,
    ) -> Optional[ParsedEmail]:
        """Parse un email individuel."""
        loop = asyncio.get_event_loop()
        
        def fetch_email():
            _, msg_data = imap.fetch(msg_id, '(RFC822)')
            return msg_data
        
        msg_data = await loop.run_in_executor(None, fetch_email)
        
        if not msg_data or not msg_data[0]:
            return None
        
        # Parser le message
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)
        
        # Extraire les headers
        subject = self._decode_header(msg.get("Subject", ""))
        sender = self._decode_header(msg.get("From", ""))
        date_str = msg.get("Date", "")
        message_id = msg.get("Message-ID", "")
        
        # Parser la date
        received_at = datetime.utcnow()
        try:
            from email.utils import parsedate_to_datetime
            received_at = parsedate_to_datetime(date_str)
        except:
            pass
        
        # Extraire le corps
        body_text, body_html = self._extract_body(msg)
        
        # Extraire les pièces jointes
        attachments = self._extract_attachments(msg)
        
        # Créer l'objet ParsedEmail
        parsed = ParsedEmail(
            message_id=message_id,
            subject=subject,
            sender=sender,
            received_at=received_at,
            body_text=body_text,
            body_html=body_html,
            attachments=attachments,
            has_pdf_brochure=any(
                a.get("content_type", "").lower() == "application/pdf"
                for a in attachments
            ),
        )
        
        # Détecter si c'est une réponse brochure
        parsed.portal = self._detect_portal(sender, subject)
        parsed.is_brochure_response = self._is_brochure_response(
            sender, subject, body_text
        )
        
        # Extraire l'adresse
        if parsed.is_brochure_response:
            address_info = self._extract_address(body_text + " " + body_html)
            if address_info:
                parsed.extracted_address = address_info.get("street", "")
                parsed.extracted_npa = address_info.get("npa", "")
                parsed.extracted_city = address_info.get("city", "")
                parsed.extracted_full_address = address_info.get("full", "")
                parsed.confidence = address_info.get("confidence", 0.5)
            
            # Extraire les détails du bien
            parsed.extracted_price = self._extract_price(body_text)
            parsed.extracted_rooms = self._extract_rooms(body_text)
            parsed.extracted_surface = self._extract_surface(body_text)
            
            # Matcher avec une demande de brochure
            matched_id = await self._match_brochure_request(
                db, sender, subject, body_text
            )
            parsed.matched_request_id = matched_id
        
        return parsed

    def _decode_header(self, header: str) -> str:
        """Décode un header email."""
        if not header:
            return ""
        
        try:
            decoded_parts = decode_header(header)
            decoded = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    decoded += part.decode(encoding or "utf-8", errors="replace")
                else:
                    decoded += part
            return decoded
        except:
            return header

    def _extract_body(self, msg: email.message.Message) -> Tuple[str, str]:
        """Extrait le corps texte et HTML."""
        text_body = ""
        html_body = ""
        
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        decoded = payload.decode(charset, errors="replace")
                        
                        if content_type == "text/plain":
                            text_body += decoded
                        elif content_type == "text/html":
                            html_body += decoded
                except:
                    pass
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                content_type = msg.get_content_type()
                
                try:
                    decoded = payload.decode(charset, errors="replace")
                    if content_type == "text/html":
                        html_body = decoded
                    else:
                        text_body = decoded
                except:
                    pass
        
        # Nettoyer le HTML si pas de texte
        if not text_body and html_body:
            text_body = self._html_to_text(html_body)
        
        return text_body, html_body

    def _html_to_text(self, html: str) -> str:
        """Convertit HTML en texte simple."""
        # Supprimer les tags
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        
        # Décoder les entités HTML
        import html as html_module
        text = html_module.unescape(text)
        
        return text

    def _extract_attachments(
        self,
        msg: email.message.Message,
    ) -> List[Dict[str, Any]]:
        """Extrait les informations sur les pièces jointes."""
        attachments = []
        
        if msg.is_multipart():
            for part in msg.walk():
                content_disposition = str(part.get("Content-Disposition", ""))
                
                if "attachment" in content_disposition or "inline" in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        filename = self._decode_header(filename)
                    
                    content_type = part.get_content_type()
                    size = len(part.get_payload(decode=True) or b"")
                    
                    attachments.append({
                        "filename": filename,
                        "content_type": content_type,
                        "size": size,
                    })
        
        return attachments

    def _detect_portal(self, sender: str, subject: str) -> str:
        """Détecte le portail immobilier source."""
        combined = f"{sender} {subject}".lower()
        
        for portal, patterns in SENDER_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, combined, re.IGNORECASE):
                    return portal
        
        return ""

    def _is_brochure_response(
        self,
        sender: str,
        subject: str,
        body: str,
    ) -> bool:
        """Détermine si l'email est une réponse de brochure."""
        combined = f"{sender} {subject} {body[:500]}".lower()
        
        # Mots-clés positifs
        positive_keywords = [
            "brochure", "documentation", "dossier",
            "bien immobilier", "appartement", "maison", "villa",
            "annonce", "objet", "propriété",
            "contact demandé", "demande d'information",
            "votre demande", "suite à votre",
            "prospectus", "plaquette",
        ]
        
        # Mots-clés négatifs (spam, newsletters)
        negative_keywords = [
            "unsubscribe", "désabonner", "newsletter",
            "promotion", "offre spéciale", "soldes",
        ]
        
        positive_score = sum(1 for kw in positive_keywords if kw in combined)
        negative_score = sum(1 for kw in negative_keywords if kw in combined)
        
        # Détection par expéditeur connu
        for portal, patterns in SENDER_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, sender, re.IGNORECASE):
                    return positive_score > 0 or portal != "generic_agency"
        
        return positive_score >= 2 and negative_score == 0

    def _extract_address(self, text: str) -> Optional[Dict[str, str]]:
        """Extrait l'adresse du bien depuis le texte."""
        best_match = None
        best_confidence = 0.0
        
        for pattern in ADDRESS_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            
            for match in matches:
                if isinstance(match, tuple):
                    # Reconstruire l'adresse selon le pattern
                    if len(match) >= 3:
                        # Essayer différentes combinaisons
                        candidates = []
                        
                        # Format: street, npa, city
                        if re.match(r"\d{4}", match[1] if len(match) > 1 else ""):
                            candidates.append({
                                "street": match[0].strip(),
                                "npa": match[1].strip(),
                                "city": match[2].strip() if len(match) > 2 else "",
                            })
                        # Format: npa, city, street
                        elif re.match(r"\d{4}", match[0]):
                            candidates.append({
                                "npa": match[0].strip(),
                                "city": match[1].strip(),
                                "street": match[2].strip() if len(match) > 2 else "",
                            })
                        
                        for candidate in candidates:
                            # Calculer la confiance
                            confidence = 0.5
                            
                            if candidate.get("npa") and re.match(r"\d{4}", candidate["npa"]):
                                confidence += 0.2
                            
                            if candidate.get("city") and len(candidate["city"]) > 2:
                                confidence += 0.15
                            
                            if candidate.get("street") and any(
                                kw in candidate["street"].lower()
                                for kw in ["rue", "avenue", "chemin", "route", "place"]
                            ):
                                confidence += 0.15
                            
                            if confidence > best_confidence:
                                best_confidence = confidence
                                full_addr = f"{candidate.get('street', '')}, {candidate.get('npa', '')} {candidate.get('city', '')}".strip(", ")
                                best_match = {
                                    **candidate,
                                    "full": full_addr,
                                    "confidence": confidence,
                                }
                else:
                    # Match simple (string)
                    full_addr = match.strip()
                    confidence = 0.4
                    
                    # Extraire NPA et ville
                    npa_match = re.search(r"(\d{4})\s+([A-Za-zÀ-ÿ\-\s]+)$", full_addr)
                    if npa_match:
                        confidence += 0.2
                        if confidence > best_confidence:
                            best_confidence = confidence
                            best_match = {
                                "street": full_addr[:npa_match.start()].strip(", "),
                                "npa": npa_match.group(1),
                                "city": npa_match.group(2).strip(),
                                "full": full_addr,
                                "confidence": confidence,
                            }
        
        return best_match

    def _extract_price(self, text: str) -> Optional[float]:
        """Extrait le prix du bien."""
        for pattern in PRICE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    price_str = match.group(1).replace("'", "").replace("´", "").replace("'", "")
                    return float(price_str)
                except:
                    pass
        return None

    def _extract_rooms(self, text: str) -> Optional[float]:
        """Extrait le nombre de pièces."""
        for pattern in ROOMS_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    rooms_str = match.group(1).replace(",", ".")
                    return float(rooms_str)
                except:
                    pass
        return None

    def _extract_surface(self, text: str) -> Optional[float]:
        """Extrait la surface."""
        for pattern in SURFACE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except:
                    pass
        return None

    async def _match_brochure_request(
        self,
        db: AsyncSession,
        sender: str,
        subject: str,
        body: str,
    ) -> Optional[int]:
        """
        Tente de matcher l'email avec une demande de brochure.
        
        Stratégies:
        1. Recherche par URL dans le corps
        2. Recherche par titre de l'annonce
        3. Recherche par date (emails récents)
        """
        # Rechercher les URLs de portails dans le corps
        url_patterns = [
            r"(https?://[^\s]+comparis[^\s]+)",
            r"(https?://[^\s]+immoscout[^\s]+)",
            r"(https?://[^\s]+homegate[^\s]+)",
        ]
        
        for pattern in url_patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                url = match.group(1)
                # Chercher une demande avec cette URL
                result = await db.execute(
                    select(BrochureRequest)
                    .where(BrochureRequest.listing_url.contains(url[:50]))
                    .where(BrochureRequest.status == "sent")
                    .order_by(BrochureRequest.sent_at.desc())
                    .limit(1)
                )
                request = result.scalar_one_or_none()
                if request:
                    return request.id
        
        # Recherche par portail + date récente
        portal = self._detect_portal(sender, subject)
        if portal and portal not in ["generic_agency"]:
            week_ago = datetime.utcnow() - timedelta(days=7)
            result = await db.execute(
                select(BrochureRequest)
                .where(BrochureRequest.portal == portal)
                .where(BrochureRequest.status == "sent")
                .where(BrochureRequest.sent_at >= week_ago)
                .where(BrochureRequest.response_received == False)
                .order_by(BrochureRequest.sent_at.desc())
                .limit(1)
            )
            request = result.scalar_one_or_none()
            if request:
                return request.id
        
        return None

    async def _update_brochure_request(
        self,
        db: AsyncSession,
        parsed: ParsedEmail,
    ):
        """Met à jour une demande de brochure avec les données extraites."""
        if not parsed.matched_request_id:
            return
        
        # Mettre à jour la demande
        await db.execute(
            update(BrochureRequest)
            .where(BrochureRequest.id == parsed.matched_request_id)
            .values(
                response_received=True,
                response_at=parsed.received_at,
                listing_address=parsed.extracted_full_address or None,
            )
        )
        
        # Si on a trouvé l'adresse, mettre à jour le ScrapedListing lié
        if parsed.extracted_full_address:
            result = await db.execute(
                select(BrochureRequest)
                .where(BrochureRequest.id == parsed.matched_request_id)
            )
            request = result.scalar_one_or_none()
            
            if request and request.listing_url:
                # Trouver le listing correspondant
                listing_result = await db.execute(
                    select(ScrapedListing)
                    .where(ScrapedListing.url == request.listing_url)
                )
                listing = listing_result.scalar_one_or_none()
                
                if listing:
                    # Conserver l'adresse scrapée (souvent masquée) et stocker
                    # l'adresse extraite dans le champ dédié.
                    listing.extracted_address = parsed.extracted_full_address
                    if not listing.address:
                        listing.address = parsed.extracted_full_address

                    listing.npa = parsed.extracted_npa
                    listing.city = parsed.extracted_city
                    listing.updated_at = datetime.utcnow()
                    if listing.match_status in (None, "", "no_match"):
                        listing.match_status = "pending"
                    
                    if parsed.extracted_price:
                        listing.price = parsed.extracted_price
                    if parsed.extracted_rooms:
                        listing.rooms = parsed.extracted_rooms
                    if parsed.extracted_surface:
                        listing.surface = parsed.extracted_surface
        
        await db.commit()
        
        await emit_activity(
            "brochure",
            f"Réponse brochure traitée: {parsed.extracted_full_address or 'adresse non extraite'}"
        )


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def parse_emails_for_addresses(days_back: int = 7) -> Dict[str, Any]:
    """
    Helper pour parser les emails et extraire les adresses.
    """
    service = EmailParserService()
    return await service.parse_all_accounts(days_back=days_back)


async def get_parsed_emails_stats() -> Dict[str, Any]:
    """
    Retourne les statistiques des emails parsés.
    """
    async with AsyncSessionLocal() as db:
        from sqlalchemy import func
        
        # Compter les réponses reçues
        result = await db.execute(
            select(func.count(BrochureRequest.id))
            .where(BrochureRequest.response_received == True)
        )
        total_responses = result.scalar() or 0
        
        # Compter les adresses extraites
        result = await db.execute(
            select(func.count(BrochureRequest.id))
            .where(BrochureRequest.response_received == True)
            .where(BrochureRequest.listing_address.isnot(None))
        )
        addresses_extracted = result.scalar() or 0
        
        return {
            "total_responses": total_responses,
            "addresses_extracted": addresses_extracted,
            "extraction_rate": (
                addresses_extracted / total_responses * 100
                if total_responses > 0 else 0
            ),
        }

