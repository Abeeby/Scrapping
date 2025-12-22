# =============================================================================
# LEAD SCORING SERVICE - Qualification des prospects
# =============================================================================
# Système de scoring pour prioriser les leads de qualité
# Détection particulier vs agence, score de contactabilité, etc.
# =============================================================================

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from datetime import datetime

from app.core.logger import logger


# =============================================================================
# CONFIGURATION SCORING
# =============================================================================

# Points par critère (total max = 100)
SCORING_WEIGHTS = {
    # Contactabilité (max 40 points)
    "has_phone": 20,
    "has_mobile": 10,  # Bonus si mobile (pas fixe)
    "has_email": 10,
    
    # Type de vendeur (max 25 points)
    "is_private": 25,
    "likely_private": 15,
    "professional": 0,
    
    # Qualité des données (max 20 points)
    "has_full_name": 10,
    "has_address": 5,
    "has_price": 5,
    
    # Fraîcheur (max 15 points)
    "recent_7_days": 15,
    "recent_30_days": 10,
    "recent_90_days": 5,
    "older": 0,
}


# =============================================================================
# DÉTECTION PARTICULIER VS AGENCE
# =============================================================================

# Mots-clés indiquant une agence
AGENCY_KEYWORDS_FR = [
    "agence", "immobilier", "immobilière", "régie", "gestion",
    "courtage", "courtier", "promotion", "promoteur", "fiduciaire",
    "conseil", "consulting", "services", "group", "groupe",
    "partners", "partenaires", "invest", "capital", "holding",
    "sàrl", "sarl", "s.à.r.l", "s.a", "sa",
]

AGENCY_KEYWORDS_DE = [
    "immobilien", "agentur", "makler", "verwaltung", "treuhand",
    "beratung", "consulting", "gmbh", "ag", "partner",
]

AGENCY_KEYWORDS_IT = [
    "agenzia", "immobiliare", "gestione", "consulenza",
]

AGENCY_PATTERNS = [
    r"\b(sàrl|sarl|s\.à\.r\.l\.?|gmbh|ag|s\.a\.?)\b",
    r"\b(immobili[eè]re?|immobilien|agenzia)\b",
    r"\b(r[eé]gie|courtage|verwaltung|makler)\b",
    r"\b(group[e]?|partners?|consulting)\b",
    r"\b(promotion[s]?|promoteur)\b",
    r"@.+\.(ch|com|fr|de)",  # Email professionnel
    r"www\..+",
    r"https?://",
]

# Mots-clés indiquant un particulier
PRIVATE_KEYWORDS = [
    "particulier", "privé", "privat", "privato",
    "de particulier à particulier", "sans agence",
    "agences s'abstenir", "pas d'agences", "keine makler",
    "proprietaire", "propriétaire", "eigentümer",
    "privat-verkauf", "privatverkauf",
]

# Noms d'agences connues (à enrichir)
KNOWN_AGENCIES = {
    "naef", "wüest", "cardis", "bonnard", "regiedubignon",
    "gerofinance", "comptoir immobilier", "cbre", "jll",
    "barnes", "engel & völkers", "century 21", "remax",
    "immoscout", "homegate", "comparis",
}


@dataclass
class SellerAnalysis:
    """Résultat de l'analyse du vendeur."""
    seller_type: str  # private, likely_private, professional, unknown
    is_private: bool
    confidence: float  # 0-1
    indicators: List[str]
    agency_name: Optional[str] = None


def analyze_seller(
    name: str = "",
    description: str = "",
    email: str = "",
    phone: str = "",
    url: str = "",
) -> SellerAnalysis:
    """
    Analyse si le vendeur est un particulier ou une agence.
    
    Args:
        name: Nom du vendeur/contact
        description: Description de l'annonce
        email: Email de contact
        phone: Téléphone de contact
        url: URL de l'annonce
        
    Returns:
        SellerAnalysis avec le type, la confiance et les indicateurs
    """
    # Combiner tous les textes pour analyse
    text = f"{name} {description} {email} {phone} {url}".lower()
    
    indicators = []
    private_score = 0
    agency_score = 0
    
    # Vérifier les indicateurs de particulier (prioritaires)
    for keyword in PRIVATE_KEYWORDS:
        if keyword in text:
            private_score += 30
            indicators.append(f"Mot-clé particulier: '{keyword}'")
    
    # Vérifier les patterns d'agence
    for pattern in AGENCY_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            agency_score += 15
            indicators.append(f"Pattern agence: {pattern}")
    
    # Vérifier les mots-clés d'agence
    all_agency_keywords = AGENCY_KEYWORDS_FR + AGENCY_KEYWORDS_DE + AGENCY_KEYWORDS_IT
    for keyword in all_agency_keywords:
        if keyword in text:
            agency_score += 10
            indicators.append(f"Mot-clé agence: '{keyword}'")
    
    # Vérifier les agences connues
    for agency in KNOWN_AGENCIES:
        if agency in text:
            agency_score += 40
            indicators.append(f"Agence connue: '{agency}'")
    
    # Analyser le nom
    if name:
        name_lower = name.lower().strip()
        
        # Un nom avec prénom+nom = plus probablement particulier
        name_parts = name_lower.split()
        if len(name_parts) == 2 and not any(kw in name_lower for kw in all_agency_keywords):
            private_score += 10
            indicators.append("Format nom: Prénom Nom")
        
        # Nom trop long = probablement une entreprise
        if len(name_parts) >= 4:
            agency_score += 5
            indicators.append("Nom long (>3 mots)")
    
    # Analyser l'email
    if email:
        email_lower = email.lower()
        
        # Email personnel (gmail, outlook, etc.)
        personal_domains = ["gmail", "outlook", "hotmail", "yahoo", "bluewin", "sunrise"]
        if any(domain in email_lower for domain in personal_domains):
            private_score += 15
            indicators.append("Email personnel (Gmail, etc.)")
        
        # Email professionnel avec domaine d'entreprise
        if "@" in email_lower:
            domain = email_lower.split("@")[1]
            if not any(d in domain for d in personal_domains):
                agency_score += 10
                indicators.append(f"Email professionnel ({domain})")
    
    # Analyser le téléphone
    if phone:
        phone_clean = re.sub(r"[^\d]", "", phone)
        
        # Numéro de service / central (0800, etc.)
        if phone_clean.startswith("0800") or phone_clean.startswith("0848"):
            agency_score += 20
            indicators.append("Numéro de service (0800/0848)")
    
    # Calculer le résultat final
    total_score = private_score - agency_score
    
    if private_score > 0 and agency_score == 0:
        seller_type = "private"
        is_private = True
        confidence = min(1.0, private_score / 50)
    elif total_score >= 20:
        seller_type = "private"
        is_private = True
        confidence = min(1.0, total_score / 50)
    elif total_score >= 0:
        seller_type = "likely_private"
        is_private = True
        confidence = 0.5 + (total_score / 100)
    elif total_score >= -20:
        seller_type = "likely_professional"
        is_private = False
        confidence = 0.5 - (total_score / 100)
    else:
        seller_type = "professional"
        is_private = False
        confidence = min(1.0, abs(total_score) / 50)
    
    # Si aucun indicateur, marquer comme inconnu mais probablement particulier
    if not indicators:
        seller_type = "unknown"
        is_private = True  # Optimiste par défaut
        confidence = 0.3
        indicators.append("Aucun indicateur trouvé")
    
    return SellerAnalysis(
        seller_type=seller_type,
        is_private=is_private,
        confidence=round(confidence, 2),
        indicators=indicators,
    )


# =============================================================================
# SCORING DES LEADS
# =============================================================================

@dataclass
class LeadScore:
    """Score d'un lead avec détails."""
    total_score: int  # 0-100
    contactability_score: int  # 0-40
    seller_score: int  # 0-25
    data_quality_score: int  # 0-20
    freshness_score: int  # 0-15
    
    details: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    
    # Classification
    priority: str = "medium"  # high, medium, low


def calculate_lead_score(
    nom: str = "",
    prenom: str = "",
    telephone: str = "",
    email: str = "",
    adresse: str = "",
    prix: Optional[float] = None,
    description: str = "",
    source: str = "",
    created_at: Optional[datetime] = None,
    seller_type: str = "",
) -> LeadScore:
    """
    Calcule le score d'un lead pour priorisation.
    
    Args:
        nom: Nom du prospect
        prenom: Prénom du prospect
        telephone: Numéro de téléphone
        email: Email
        adresse: Adresse
        prix: Prix du bien
        description: Description de l'annonce
        source: Source du lead (anibis, tutti, etc.)
        created_at: Date de création/découverte
        seller_type: Type de vendeur si déjà analysé
        
    Returns:
        LeadScore avec les détails du scoring
    """
    details = {}
    recommendations = []
    
    # 1. CONTACTABILITÉ (max 40 points)
    contactability = 0
    
    if telephone:
        contactability += SCORING_WEIGHTS["has_phone"]
        details["has_phone"] = True
        
        # Bonus si mobile
        phone_clean = re.sub(r"[^\d]", "", telephone)
        if phone_clean.startswith(("076", "077", "078", "079")):
            contactability += SCORING_WEIGHTS["has_mobile"]
            details["is_mobile"] = True
        elif phone_clean.startswith("41"):
            # Format international
            if phone_clean[2:5] in ("76", "77", "78", "79"):
                contactability += SCORING_WEIGHTS["has_mobile"]
                details["is_mobile"] = True
    else:
        recommendations.append("Rechercher le numéro de téléphone")
    
    if email:
        contactability += SCORING_WEIGHTS["has_email"]
        details["has_email"] = True
    
    # 2. TYPE DE VENDEUR (max 25 points)
    seller = 0
    
    if not seller_type:
        # Analyser le vendeur
        analysis = analyze_seller(
            name=f"{prenom} {nom}".strip() if prenom or nom else "",
            description=description,
            email=email,
            phone=telephone,
        )
        seller_type = analysis.seller_type
        details["seller_analysis"] = {
            "type": analysis.seller_type,
            "is_private": analysis.is_private,
            "confidence": analysis.confidence,
            "indicators": analysis.indicators[:5],
        }
    
    if seller_type == "private":
        seller = SCORING_WEIGHTS["is_private"]
    elif seller_type in ("likely_private", "unknown"):
        seller = SCORING_WEIGHTS["likely_private"]
    else:
        seller = SCORING_WEIGHTS["professional"]
        recommendations.append("⚠️ Potentiellement une agence, pas un particulier")
    
    # 3. QUALITÉ DES DONNÉES (max 20 points)
    data_quality = 0
    
    if nom and prenom:
        data_quality += SCORING_WEIGHTS["has_full_name"]
        details["has_full_name"] = True
    elif nom or prenom:
        data_quality += SCORING_WEIGHTS["has_full_name"] // 2
        details["has_partial_name"] = True
        recommendations.append("Rechercher le prénom/nom complet")
    
    if adresse:
        data_quality += SCORING_WEIGHTS["has_address"]
        details["has_address"] = True
    
    if prix and prix > 0:
        data_quality += SCORING_WEIGHTS["has_price"]
        details["has_price"] = True
    
    # 4. FRAÎCHEUR (max 15 points)
    freshness = 0
    
    if created_at:
        days_ago = (datetime.utcnow() - created_at).days
        
        if days_ago <= 7:
            freshness = SCORING_WEIGHTS["recent_7_days"]
            details["freshness"] = "recent_7_days"
        elif days_ago <= 30:
            freshness = SCORING_WEIGHTS["recent_30_days"]
            details["freshness"] = "recent_30_days"
        elif days_ago <= 90:
            freshness = SCORING_WEIGHTS["recent_90_days"]
            details["freshness"] = "recent_90_days"
        else:
            freshness = SCORING_WEIGHTS["older"]
            details["freshness"] = "older"
            recommendations.append("Annonce ancienne, vérifier si toujours d'actualité")
    else:
        freshness = SCORING_WEIGHTS["recent_30_days"]  # Valeur par défaut
        details["freshness"] = "unknown"
    
    # CALCUL FINAL
    total = contactability + seller + data_quality + freshness
    
    # Classification priorité
    if total >= 75:
        priority = "high"
    elif total >= 50:
        priority = "medium"
    else:
        priority = "low"
        if not recommendations:
            recommendations.append("Lead de faible qualité, données manquantes")
    
    return LeadScore(
        total_score=total,
        contactability_score=contactability,
        seller_score=seller,
        data_quality_score=data_quality,
        freshness_score=freshness,
        details=details,
        recommendations=recommendations,
        priority=priority,
    )


# =============================================================================
# DÉTECTION DE DOUBLONS (même téléphone/email = potentiellement agence)
# =============================================================================

class DuplicateDetector:
    """
    Détecte les doublons de leads (même téléphone/email sur plusieurs annonces).
    Un même numéro sur plusieurs annonces = probablement une agence.
    """
    
    def __init__(self):
        self._phone_counts: Dict[str, int] = {}
        self._email_counts: Dict[str, int] = {}
        self._phone_listings: Dict[str, List[str]] = {}
        self._email_listings: Dict[str, List[str]] = {}
    
    def add_lead(self, lead_id: str, phone: str = "", email: str = ""):
        """Ajoute un lead pour tracking des doublons."""
        if phone:
            phone_norm = self._normalize_phone(phone)
            self._phone_counts[phone_norm] = self._phone_counts.get(phone_norm, 0) + 1
            if phone_norm not in self._phone_listings:
                self._phone_listings[phone_norm] = []
            self._phone_listings[phone_norm].append(lead_id)
        
        if email:
            email_norm = email.lower().strip()
            self._email_counts[email_norm] = self._email_counts.get(email_norm, 0) + 1
            if email_norm not in self._email_listings:
                self._email_listings[email_norm] = []
            self._email_listings[email_norm].append(lead_id)
    
    def _normalize_phone(self, phone: str) -> str:
        """Normalise un numéro de téléphone."""
        return re.sub(r"[^\d]", "", phone)[-10:]  # Garder les 10 derniers chiffres
    
    def is_likely_agency(self, phone: str = "", email: str = "", threshold: int = 3) -> tuple[bool, List[str]]:
        """
        Vérifie si le contact apparaît sur plusieurs annonces (signe d'agence).
        
        Args:
            phone: Numéro de téléphone
            email: Email
            threshold: Nombre d'annonces à partir duquel c'est suspect
            
        Returns:
            (is_likely_agency, reasons)
        """
        reasons = []
        
        if phone:
            phone_norm = self._normalize_phone(phone)
            count = self._phone_counts.get(phone_norm, 0)
            if count >= threshold:
                reasons.append(f"Téléphone utilisé sur {count} annonces")
        
        if email:
            email_norm = email.lower().strip()
            count = self._email_counts.get(email_norm, 0)
            if count >= threshold:
                reasons.append(f"Email utilisé sur {count} annonces")
        
        return (len(reasons) > 0, reasons)
    
    def get_duplicate_listings(self, phone: str = "", email: str = "") -> List[str]:
        """Récupère les IDs des annonces avec le même contact."""
        listings = set()
        
        if phone:
            phone_norm = self._normalize_phone(phone)
            listings.update(self._phone_listings.get(phone_norm, []))
        
        if email:
            email_norm = email.lower().strip()
            listings.update(self._email_listings.get(email_norm, []))
        
        return list(listings)
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de doublons."""
        multi_phone = sum(1 for c in self._phone_counts.values() if c > 1)
        multi_email = sum(1 for c in self._email_counts.values() if c > 1)
        
        return {
            "total_unique_phones": len(self._phone_counts),
            "phones_with_duplicates": multi_phone,
            "total_unique_emails": len(self._email_counts),
            "emails_with_duplicates": multi_email,
        }


# Instance globale du détecteur
duplicate_detector = DuplicateDetector()


# =============================================================================
# EXPORT API
# =============================================================================

async def score_leads(leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Score une liste de leads et ajoute les informations de scoring.
    
    Args:
        leads: Liste de dictionnaires avec les données des leads
        
    Returns:
        Liste enrichie avec les scores
    """
    scored_leads = []
    
    for lead in leads:
        score = calculate_lead_score(
            nom=lead.get("nom", ""),
            prenom=lead.get("prenom", ""),
            telephone=lead.get("telephone", ""),
            email=lead.get("email", ""),
            adresse=lead.get("adresse", ""),
            prix=lead.get("prix"),
            description=lead.get("description", ""),
            source=lead.get("source", ""),
            created_at=lead.get("created_at"),
            seller_type=lead.get("seller_type", ""),
        )
        
        # Vérifier les doublons
        duplicate_detector.add_lead(
            lead.get("id", ""),
            lead.get("telephone", ""),
            lead.get("email", ""),
        )
        is_duplicate, dup_reasons = duplicate_detector.is_likely_agency(
            lead.get("telephone", ""),
            lead.get("email", ""),
        )
        
        if is_duplicate:
            score.recommendations.extend(dup_reasons)
            score.priority = "low"  # Dégrader la priorité
        
        # Enrichir le lead
        lead["score"] = score.total_score
        lead["priority"] = score.priority
        lead["scoring"] = {
            "total": score.total_score,
            "contactability": score.contactability_score,
            "seller": score.seller_score,
            "data_quality": score.data_quality_score,
            "freshness": score.freshness_score,
            "details": score.details,
            "recommendations": score.recommendations,
        }
        lead["is_duplicate"] = is_duplicate
        
        scored_leads.append(lead)
    
    # Trier par score décroissant
    scored_leads.sort(key=lambda x: x.get("score", 0), reverse=True)
    
    return scored_leads

