"""Pipeline d'amélioration qualité prospect (post-import / post-création).

Objectifs:
- Normalisation + validation (téléphone/email/adresse)
- Enrichissement (Search.ch / Local.ch) avec erreurs explicites
- Déduplication hybride:
  - auto-merge sur match exact (phone_norm/email_norm/lien_rf)
  - suggestions si match partiel (nom/prénom/ville/adresse)
- Scoring qualité + flags
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import (
    AsyncSessionLocal,
    Prospect,
    ProspectDuplicateCandidate,
    ProspectMergeLog,
)
from app.core.logger import logger
from app.core.websocket import emit_activity
from app.scrapers.localch import LocalChScraper
from app.scrapers.searchch import SearchChScraper, SearchChScraperError

# Nouveaux clients API
try:
    from app.scrapers.zefix import ZefixClient, ZefixError
    ZEFIX_AVAILABLE = True
except ImportError:
    ZEFIX_AVAILABLE = False

try:
    from app.scrapers.geoadmin import GeoAdminClient, GeoAdminError
    GEOADMIN_AVAILABLE = True
except ImportError:
    GEOADMIN_AVAILABLE = False


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_BUSINESS_KWS = (
    " sa",
    " sàrl",
    " sarl",
    " gmbh",
    " ag",
    " inc",
    " ltd",
    " llc",
    " société",
    " entreprise",
    " immobilier",
    " immobilière",
    " régie",
    " agence",
)


def normalize_email(raw: Optional[str]) -> str:
    if not raw:
        return ""
    return raw.strip().lower()


def normalize_phone(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = raw.strip()
    if not s:
        return ""
    # garder chiffres et '+' (au début)
    s = re.sub(r"[^\d+]", "", s)
    if s.startswith("00"):
        s = "+" + s[2:]
    if s.startswith("+"):
        s = "+" + re.sub(r"\D", "", s[1:])
    else:
        s = re.sub(r"\D", "", s)

    # Normalisation Suisse (E.164)
    if s.startswith("0") and len(s) == 10:
        return "+41" + s[1:]
    if s.startswith("41") and len(s) == 11:
        return "+41" + s[2:]
    return s


def is_valid_email(email_norm: str) -> bool:
    if not email_norm:
        return False
    if len(email_norm) > 254:
        return False
    return _EMAIL_RE.match(email_norm) is not None


def is_valid_phone(phone_norm: str) -> bool:
    if not phone_norm:
        return False
    digits = re.sub(r"\D", "", phone_norm)
    # +41 + 9 digits
    if phone_norm.startswith("+41") and len(digits) == 11:
        return True
    return 10 <= len(digits) <= 15


def normalize_address(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = " ".join(raw.strip().split())
    return s


def is_likely_business(nom: str) -> bool:
    n = f" {nom.lower()} "
    return any(kw in n for kw in _BUSINESS_KWS)


def _completeness_score(p: Prospect) -> int:
    """Score de complétude (pour choisir le meilleur 'master' en cas de merge)."""
    score = 0
    for attr in ("nom", "prenom", "telephone", "email", "adresse", "code_postal", "ville", "canton", "lien_rf"):
        v = getattr(p, attr, None)
        if isinstance(v, str) and v.strip():
            score += 1
        elif v not in (None, "", 0):
            score += 1
    return score


def compute_quality(prospect: Prospect) -> Tuple[int, Dict[str, Any]]:
    flags: Dict[str, Any] = {}
    score = 0

    # Identité
    if prospect.nom and prospect.nom.strip():
        score += 15
    else:
        flags["missing_nom"] = True
    if prospect.prenom and prospect.prenom.strip():
        score += 8
    else:
        flags["missing_prenom"] = True

    # Adresse
    if prospect.ville and prospect.ville.strip():
        score += 8
    else:
        flags["missing_ville"] = True
    if prospect.code_postal and prospect.code_postal.strip():
        score += 7
    else:
        flags["missing_code_postal"] = True
    if prospect.adresse and prospect.adresse.strip():
        score += 10
    else:
        flags["missing_adresse"] = True

    # Canton
    if prospect.canton and prospect.canton.strip():
        score += 5

    # Contact - le plus important
    phone_ok = is_valid_phone(prospect.telephone_norm or "")
    email_ok = is_valid_email(prospect.email_norm or "")
    if phone_ok:
        score += 25
        flags["has_valid_phone"] = True
    else:
        flags["missing_or_invalid_phone"] = True
    if email_ok:
        score += 15
        flags["has_valid_email"] = True
    else:
        flags["missing_or_invalid_email"] = True

    # Source / lien
    if prospect.lien_rf and prospect.lien_rf.strip():
        score += 5

    # Enrichissement réussi
    enrichment_status = getattr(prospect, "enrichment_status", "pending") or "pending"
    if enrichment_status == "ok":
        score += 5
        flags["enrichment_success"] = True
    elif enrichment_status == "zefix_enriched":
        score += 7  # Bonus Zefix (données officielles)
        flags["zefix_enriched"] = True
    elif enrichment_status == "geoadmin_validated":
        score += 3  # Bonus adresse validée
        flags["geoadmin_validated"] = True
    elif enrichment_status == "cross_enriched":
        score += 6  # Bonus croisement multi-sources
        flags["cross_enriched"] = True

    # Ciblage
    if prospect.nom and is_likely_business(prospect.nom):
        flags["likely_business"] = True

    # Notes indiquant Zefix UID
    if prospect.notes and "Zefix UID:" in (prospect.notes or ""):
        flags["has_zefix_uid"] = True

    # Dédup
    if prospect.merged_into_id:
        flags["merged_into_id"] = prospect.merged_into_id
        flags["is_duplicate"] = True
        # Un prospect fusionné n'est plus prioritaire
        score = min(score, 20)

    return min(score, 100), flags


async def _pick_best_match(prospect: Prospect, results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not results:
        return None
    nom = (prospect.nom or "").lower().strip()
    prenom = (prospect.prenom or "").lower().strip()

    def rank(r: Dict[str, Any]) -> Tuple[int, int, int]:
        rnom = (r.get("nom") or "").lower()
        # priorité: contient le nom, contient le prénom, a un téléphone
        return (
            1 if nom and nom in rnom else 0,
            1 if prenom and prenom in rnom else 0,
            1 if r.get("telephone") else 0,
        )

    return sorted(results, key=rank, reverse=True)[0]


async def enrich_from_directories(prospect: Prospect) -> Tuple[bool, Optional[str]]:
    """Enrichit téléphone/email/adresse/lien_rf. Retourne (changed, message)."""
    query = f"{prospect.prenom or ''} {prospect.nom or ''}".strip()
    ville = prospect.ville or ""
    if not query or not ville:
        return False, "Pas assez d'infos (nom/ville) pour enrichir"

    # Search.ch
    async with SearchChScraper() as scraper:
        results = await scraper.search(query=query, ville=ville, limit=10, type_recherche="person")
    best = await _pick_best_match(prospect, results)

    # Fallback Local.ch si rien (ou pas de téléphone)
    if not best or (not best.get("telephone") and not best.get("email")):
        try:
            async with LocalChScraper() as ls:
                local_res = await ls.search(query=query, ville=ville, limit=10, type_recherche="person")
            best = await _pick_best_match(prospect, local_res) or best
        except Exception:
            # Local.ch peut être indispo (Playwright). On n'échoue pas la pipeline.
            pass

    if not best:
        return False, "Aucun résultat annuaire"

    changed = False
    if best.get("telephone") and not (prospect.telephone or "").strip():
        prospect.telephone = best["telephone"]
        changed = True
    if best.get("email") and not (prospect.email or "").strip():
        prospect.email = best["email"]
        changed = True
    if best.get("adresse") and len(best.get("adresse", "")) > len(prospect.adresse or ""):
        prospect.adresse = best.get("adresse") or prospect.adresse
        changed = True
    if best.get("code_postal") and not (prospect.code_postal or "").strip():
        prospect.code_postal = best.get("code_postal") or prospect.code_postal
        changed = True
    if best.get("ville") and not (prospect.ville or "").strip():
        prospect.ville = best.get("ville") or prospect.ville
        changed = True
    if best.get("lien_rf") and not (prospect.lien_rf or "").strip():
        prospect.lien_rf = best.get("lien_rf") or prospect.lien_rf
        changed = True

    return changed, "Enrichissement OK" if changed else "Résultat trouvé mais rien à compléter"


async def enrich_from_zefix(prospect: Prospect) -> Tuple[bool, Optional[str]]:
    """Enrichit via Zefix (registre du commerce) pour les entreprises."""
    if not ZEFIX_AVAILABLE:
        return False, "Zefix non disponible"
    
    nom = (prospect.nom or "").strip()
    if not nom:
        return False, "Pas de nom pour Zefix"
    
    # Ne traiter que si c'est probablement une entreprise
    if not is_likely_business(nom):
        return False, "Pas une entreprise (Zefix ignoré)"
    
    canton = (prospect.canton or "").strip().upper()
    
    try:
        async with ZefixClient() as client:
            companies = await client.search(nom, canton=canton if canton else None, limit=5)
            
            if not companies:
                return False, "Aucune entreprise Zefix"
            
            # Trouver la meilleure correspondance
            best = None
            best_score = 0
            nom_lower = nom.lower()
            
            for company in companies:
                score = 0
                if nom_lower in company.name.lower():
                    score += 2
                if company.city and (prospect.ville or "").lower() in company.city.lower():
                    score += 1
                if score > best_score:
                    best_score = score
                    best = company
            
            if not best:
                best = companies[0]
            
            changed = False
            
            # Enrichir l'adresse si meilleure
            if best.address and (not prospect.adresse or len(best.address) > len(prospect.adresse)):
                prospect.adresse = best.address
                changed = True
            
            if best.zip_code and not (prospect.code_postal or "").strip():
                prospect.code_postal = best.zip_code
                changed = True
            
            if best.city and not (prospect.ville or "").strip():
                prospect.ville = best.city
                changed = True
            
            if best.canton and not (prospect.canton or "").strip():
                prospect.canton = best.canton
                changed = True
            
            # Stocker l'UID Zefix dans les notes ou un champ dédié
            if best.uid and not (prospect.notes or "").strip():
                prospect.notes = f"Zefix UID: {best.uid}"
                changed = True
            
            return changed, f"Zefix: {best.name} ({best.uid})"
            
    except ZefixError as e:
        return False, f"Erreur Zefix: {e}"
    except Exception as e:
        return False, f"Erreur Zefix inattendue: {e}"


async def cross_enrich_prospect(prospect: Prospect) -> Tuple[bool, Optional[str]]:
    """
    Enrichissement croisé multi-sources:
    1. Si nom propriétaire connu (RF) -> chercher dans Search.ch/Local.ch
    2. Si adresse connue -> chercher propriétaire dans cadastre
    3. Fusionner les informations (téléphone, email, etc.)
    
    Retourne (changed, message).
    """
    changed = False
    messages = []
    
    nom = (prospect.nom or "").strip()
    adresse = (prospect.adresse or "").strip()
    ville = (prospect.ville or "").strip()
    lien_rf = (prospect.lien_rf or "").strip()
    
    # ==== Stratégie 1: Nom connu -> enrichir contacts ====
    if nom and ville:
        try:
            # Recherche dans annuaires avec nom complet
            query = f"{prospect.prenom or ''} {nom}".strip()
            
            async with SearchChScraper() as scraper:
                results = await scraper.search(
                    query=query, 
                    ville=ville, 
                    limit=20, 
                    type_recherche="person"
                )
            
            # Filtrer les résultats pertinents
            best_matches = []
            nom_lower = nom.lower()
            
            for r in results:
                r_nom = (r.get("nom") or "").lower()
                if nom_lower in r_nom or r_nom in nom_lower:
                    # Bonus si adresse aussi match
                    score = 1
                    if adresse:
                        r_adresse = (r.get("adresse") or "").lower()
                        if adresse.lower() in r_adresse or r_adresse in adresse.lower():
                            score += 2
                    best_matches.append((score, r))
            
            if best_matches:
                best_matches.sort(reverse=True, key=lambda x: x[0])
                _, best = best_matches[0]
                
                # Enrichir les champs manquants
                if best.get("telephone") and not (prospect.telephone or "").strip():
                    prospect.telephone = best["telephone"]
                    changed = True
                    messages.append("tel trouvé")
                
                if best.get("email") and not (prospect.email or "").strip():
                    prospect.email = best["email"]
                    changed = True
                    messages.append("email trouvé")
                
                # Enrichir adresse si on n'en a pas
                if not adresse and best.get("adresse"):
                    prospect.adresse = best.get("adresse", "")
                    changed = True
                    messages.append("adresse trouvée")
        
        except Exception as e:
            logger.warning(f"[cross_enrich] Erreur stratégie 1: {e}")
    
    # ==== Stratégie 2: Adresse connue sans nom -> chercher propriétaire ====
    if adresse and ville and not nom:
        try:
            # Recherche inversée par adresse
            async with SearchChScraper() as scraper:
                results = await scraper.search(
                    query=adresse, 
                    ville=ville, 
                    limit=10, 
                    type_recherche="person"
                )
            
            if results:
                # Prendre le premier résultat (le plus pertinent)
                best = results[0]
                
                if best.get("nom") and not (prospect.nom or "").strip():
                    prospect.nom = best.get("nom", "")
                    changed = True
                    messages.append("nom trouvé")
                
                if best.get("prenom") and not (prospect.prenom or "").strip():
                    prospect.prenom = best.get("prenom", "")
                    changed = True
                    messages.append("prénom trouvé")
                
                if best.get("telephone") and not (prospect.telephone or "").strip():
                    prospect.telephone = best["telephone"]
                    changed = True
                    messages.append("tel trouvé")
        
        except Exception as e:
            logger.warning(f"[cross_enrich] Erreur stratégie 2: {e}")
    
    # ==== Stratégie 3: Lien RF -> extraire infos propriétaire ====
    if lien_rf and "rf.ge.ch" in lien_rf and not nom:
        try:
            # Extraction des paramètres de l'URL RF
            import re as regex
            # Format typique: https://ge.ch/terrgd/e-commune.asp?EGRID=xxx
            egrid_match = regex.search(r'EGRID=([A-Za-z0-9]+)', lien_rf)
            if egrid_match:
                egrid = egrid_match.group(1)
                messages.append(f"RF EGRID: {egrid}")
                # Note: L'extraction réelle nécessiterait un scraping de la page RF
                # ce qui est hors scope pour le croisement simple
        
        except Exception as e:
            logger.warning(f"[cross_enrich] Erreur stratégie 3: {e}")
    
    # ==== Stratégie 4: Fallback Local.ch si Search.ch n'a rien donné ====
    if nom and ville and not (prospect.telephone or "").strip():
        try:
            async with LocalChScraper() as scraper:
                query = f"{prospect.prenom or ''} {nom}".strip()
                results = await scraper.search(
                    query=query, 
                    ville=ville, 
                    limit=10, 
                    type_recherche="person"
                )
            
            nom_lower = nom.lower()
            for r in results:
                if nom_lower in (r.get("nom") or "").lower():
                    if r.get("telephone") and not (prospect.telephone or "").strip():
                        prospect.telephone = r["telephone"]
                        changed = True
                        messages.append("tel Local.ch")
                        break
        
        except Exception as e:
            # Local.ch peut être indispo (Playwright)
            logger.warning(f"[cross_enrich] Erreur Local.ch: {e}")
    
    if changed:
        return True, "Cross-enrichi: " + ", ".join(messages)
    
    return False, "Pas de croisement possible"


async def validate_with_geoadmin(prospect: Prospect) -> Tuple[bool, Optional[str]]:
    """Valide et normalise l'adresse via GeoAdmin (Swisstopo)."""
    if not GEOADMIN_AVAILABLE:
        return False, "GeoAdmin non disponible"
    
    street = (prospect.adresse or "").strip()
    zip_code = (prospect.code_postal or "").strip()
    city = (prospect.ville or "").strip()
    
    if not street and not city:
        return False, "Pas d'adresse à valider"
    
    try:
        async with GeoAdminClient() as client:
            normalized = await client.normalize_address(street, zip_code, city)
            
            if not normalized:
                return False, "Adresse non trouvée GeoAdmin"
            
            if normalized.confidence < 0.5:
                return False, f"Confiance trop faible GeoAdmin ({normalized.confidence:.0%})"
            
            changed = False
            
            # Mettre à jour avec l'adresse normalisée
            if normalized.street and normalized.street != street:
                full_street = normalized.street
                if normalized.house_number:
                    full_street += f" {normalized.house_number}"
                if len(full_street) >= len(street):
                    prospect.adresse = full_street
                    prospect.adresse_norm = full_street
                    changed = True
            
            if normalized.zip_code and normalized.zip_code != zip_code:
                prospect.code_postal = normalized.zip_code
                changed = True
            
            if normalized.city and normalized.city != city:
                prospect.ville = normalized.city
                changed = True
            
            if normalized.canton and not (prospect.canton or "").strip():
                prospect.canton = normalized.canton
                changed = True
            
            return changed, f"GeoAdmin validé ({normalized.confidence:.0%})"
            
    except GeoAdminError as e:
        return False, f"Erreur GeoAdmin: {e}"
    except Exception as e:
        return False, f"Erreur GeoAdmin inattendue: {e}"


async def find_exact_duplicates(db: AsyncSession, prospect: Prospect) -> List[Prospect]:
    filters = []
    if prospect.telephone_norm:
        filters.append(Prospect.telephone_norm == prospect.telephone_norm)
    if prospect.email_norm:
        filters.append(Prospect.email_norm == prospect.email_norm)
    if prospect.lien_rf:
        filters.append(Prospect.lien_rf == prospect.lien_rf)
    if not filters:
        return []

    q = (
        select(Prospect)
        .where(Prospect.id != prospect.id)
        .where(Prospect.merged_into_id.is_(None))
        .where(or_(*filters))
        .limit(10)
    )
    res = await db.execute(q)
    return res.scalars().all()


def merge_into(source: Prospect, target: Prospect) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}

    def move_if_empty(attr: str):
        src = getattr(source, attr, None)
        dst = getattr(target, attr, None)
        if (dst is None or (isinstance(dst, str) and not dst.strip())) and src not in (None, ""):
            setattr(target, attr, src)
            merged[attr] = src

    for attr in ("telephone", "email", "adresse", "code_postal", "ville", "canton", "type_bien", "lien_rf", "source"):
        move_if_empty(attr)

    # Notes: concat
    if (source.notes or "").strip():
        if not (target.notes or "").strip():
            target.notes = source.notes
            merged["notes"] = "copied"
        else:
            target.notes = (target.notes or "") + "\n\n---\n\n" + (source.notes or "")
            merged["notes"] = "appended"

    # Tags: union
    try:
        src_tags = list(source.tags or [])
        dst_tags = list(target.tags or [])
        union = sorted(set(dst_tags + src_tags))
        if union != dst_tags:
            target.tags = union
            merged["tags"] = union
    except Exception:
        pass

    # Marquer source comme doublon fusionné
    source.is_duplicate = True
    source.merged_into_id = target.id
    source.duplicate_group_id = target.id
    if not (target.duplicate_group_id or "").strip():
        target.duplicate_group_id = target.id

    now = datetime.utcnow()
    source.updated_at = now
    target.updated_at = now

    return merged


async def refresh_duplicate_candidates(db: AsyncSession, prospect: Prospect) -> int:
    """Recalcule les suggestions de doublons (match partiel)."""
    await db.execute(delete(ProspectDuplicateCandidate).where(ProspectDuplicateCandidate.prospect_id == prospect.id))

    if prospect.merged_into_id:
        return 0

    nom = (prospect.nom or "").strip()
    ville = (prospect.ville or "").strip()
    prenom = (prospect.prenom or "").strip()
    adresse_norm = (prospect.adresse_norm or "").strip()

    if not nom or not ville:
        return 0

    candidates_q = (
        select(Prospect)
        .where(Prospect.id != prospect.id)
        .where(Prospect.merged_into_id.is_(None))
        .where(func.lower(Prospect.nom) == nom.lower())
        .where(func.lower(Prospect.ville) == ville.lower())
        .limit(10)
    )
    res = await db.execute(candidates_q)
    candidates = res.scalars().all()

    created = 0
    for c in candidates:
        conf = 0.0
        conf += 0.7  # même nom + ville (déjà filtré)
        if prenom and (c.prenom or "").strip().lower() == prenom.lower():
            conf += 0.15
        if adresse_norm and (c.adresse_norm or "").strip().lower() == adresse_norm.lower():
            conf += 0.15
        conf = min(conf, 1.0)

        if conf < 0.7:
            continue

        db.add(
            ProspectDuplicateCandidate(
                prospect_id=prospect.id,
                candidate_id=c.id,
                reason="match_nom_ville" + ("_prenom" if prenom else "") + ("_adresse" if adresse_norm else ""),
                confidence=conf,
            )
        )
        created += 1

    return created


async def run_quality_pipeline_task(prospect_id: str, db_session_factory=AsyncSessionLocal) -> None:
    """Tâche de fond: normaliser/enrichir/dédoublonner/scorer un prospect."""
    async with db_session_factory() as db:
        try:
            res = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
            prospect = res.scalar_one_or_none()
            if not prospect:
                return

            # Skip si déjà fusionné
            if prospect.merged_into_id:
                return

            await emit_activity("quality", f"Qualité: traitement {prospect.nom} ({prospect_id})")

            # Normalisation
            prospect.email_norm = normalize_email(prospect.email)
            prospect.telephone_norm = normalize_phone(prospect.telephone)
            prospect.adresse_norm = normalize_address(prospect.adresse)

            # Enrichissement (si manque téléphone/email)
            now = datetime.utcnow()
            try:
                needs_enrich = (not (prospect.telephone or "").strip()) or (not (prospect.email or "").strip())
                if not needs_enrich and prospect.enrichment_status == "pending":
                    prospect.enrichment_status = "skipped"

                if prospect.enrichment_status == "rate_limited" and prospect.last_enriched_at:
                    if now - prospect.last_enriched_at < timedelta(minutes=2):
                        # On évite de spammer Search.ch
                        pass
                    else:
                        prospect.enrichment_status = "pending"

                if needs_enrich and prospect.enrichment_status in ("pending", "error"):
                    changed, msg = await enrich_from_directories(prospect)
                    prospect.last_enriched_at = now
                    prospect.last_enrichment_error = None
                    prospect.enrichment_status = "ok"

                    if changed:
                        await emit_activity("success", f"Qualité: enrichi {prospect.nom}")
                    else:
                        await emit_activity("info", f"Qualité: {msg}")

            except SearchChScraperError as e:
                prospect.last_enriched_at = now
                prospect.last_enrichment_error = str(e)
                if getattr(e, "status_code", None) == 429:
                    prospect.enrichment_status = "rate_limited"
                else:
                    prospect.enrichment_status = "error"
            except Exception as e:
                prospect.last_enriched_at = now
                prospect.last_enrichment_error = str(e)
                prospect.enrichment_status = "error"

            # Enrichissement Zefix (entreprises uniquement)
            try:
                if is_likely_business(prospect.nom or ""):
                    zefix_changed, zefix_msg = await enrich_from_zefix(prospect)
                    if zefix_changed:
                        prospect.enrichment_status = "zefix_enriched"
                        await emit_activity("success", f"Qualité: Zefix {zefix_msg}")
            except Exception as e:
                # Zefix est un bonus, on ne fait pas échouer le pipeline
                logger.warning(f"[quality] Zefix error: {e}")

            # Validation GeoAdmin (si adresse présente)
            try:
                if (prospect.adresse or "").strip() or (prospect.ville or "").strip():
                    geo_changed, geo_msg = await validate_with_geoadmin(prospect)
                    if geo_changed:
                        if prospect.enrichment_status not in ("zefix_enriched",):
                            prospect.enrichment_status = "geoadmin_validated"
                        await emit_activity("info", f"Qualité: GeoAdmin {geo_msg}")
            except Exception as e:
                # GeoAdmin est un bonus aussi
                logger.warning(f"[quality] GeoAdmin error: {e}")

            # Enrichissement croisé multi-sources (RF + annuaires)
            try:
                # Si on manque encore des infos après les enrichissements de base
                needs_cross = (
                    (not (prospect.telephone or "").strip()) or 
                    (not (prospect.nom or "").strip() and (prospect.adresse or "").strip())
                )
                if needs_cross:
                    cross_changed, cross_msg = await cross_enrich_prospect(prospect)
                    if cross_changed:
                        if prospect.enrichment_status not in ("zefix_enriched", "geoadmin_validated"):
                            prospect.enrichment_status = "cross_enriched"
                        await emit_activity("success", f"Qualité: {cross_msg}")
            except Exception as e:
                # Cross-enrichment est un bonus
                logger.warning(f"[quality] Cross-enrich error: {e}")

            # Re-normaliser après enrichissement
            prospect.email_norm = normalize_email(prospect.email)
            prospect.telephone_norm = normalize_phone(prospect.telephone)
            prospect.adresse_norm = normalize_address(prospect.adresse)

            # Dédup exact (auto-merge)
            exact_dups = await find_exact_duplicates(db, prospect)
            if exact_dups:
                # Choisir le meilleur master (complet)
                all_candidates = exact_dups + [prospect]
                master = sorted(all_candidates, key=_completeness_score, reverse=True)[0]
                if master.id != prospect.id:
                    merged_fields = merge_into(source=prospect, target=master)
                    db.add(
                        ProspectMergeLog(
                            source_id=prospect.id,
                            target_id=master.id,
                            reason="exact_match(phone/email/lien_rf)",
                            merged_fields=merged_fields,
                        )
                    )
                    await db.commit()
                    await emit_activity("warning", f"Qualité: doublon fusionné ({prospect.nom})")
                    return

            # Suggestions doublons
            candidates_count = await refresh_duplicate_candidates(db, prospect)
            if candidates_count:
                await emit_activity("info", f"Qualité: {candidates_count} doublons possibles pour {prospect.nom}")

            # Scoring + flags
            q_score, q_flags = compute_quality(prospect)
            prospect.quality_score = q_score
            prospect.quality_flags = q_flags
            prospect.updated_at = datetime.utcnow()

            await db.commit()

        except Exception as e:
            logger.error(f"[quality] Erreur pipeline prospect {prospect_id}: {e}", exc_info=True)


# Compat: ancien nom utilisé par certains appels (saisie rapide)
async def enrich_prospect_task(prospect_id: str) -> None:
    await run_quality_pipeline_task(prospect_id, db_session_factory=AsyncSessionLocal)

