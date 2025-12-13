# =============================================================================
# SERVICE DE FUSION DE PROFILS - Merge intelligent multi-sources
# =============================================================================
# Combine les données de plusieurs sources pour créer des profils complets:
#   - Registre Foncier (priorité haute)
#   - Annuaires (Search.ch, Local.ch)
#   - Réseaux sociaux (LinkedIn, Facebook)
#   - Réponses brochures
# =============================================================================

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, Prospect, ProspectMergeLog
from app.core.logger import logger
from app.core.websocket import emit_activity


# =============================================================================
# CONFIGURATION
# =============================================================================

# Priorité des sources (plus haut = plus fiable)
SOURCE_PRIORITY = {
    "RF Genève": 100,
    "RF Vaud": 100,
    "Registre Foncier": 100,
    "FOSC": 95,
    "FAO": 90,
    "Search.ch": 80,
    "Local.ch": 75,
    "Brochure Response": 70,
    "LinkedIn": 60,
    "Facebook": 50,
    "Instagram": 45,
    "Truecaller": 55,
    "Sync.me": 55,
    "Manual": 85,
    "Import": 65,
    "MassScraper": 70,
}

# Seuils de matching
MATCH_THRESHOLD_EXACT = 0.95
MATCH_THRESHOLD_HIGH = 0.85
MATCH_THRESHOLD_MEDIUM = 0.70
MATCH_THRESHOLD_LOW = 0.55


@dataclass
class ProfileData:
    """Données d'un profil à fusionner."""
    # Identité
    nom: str = ""
    prenom: str = ""
    date_naissance: Optional[str] = None
    
    # Contact
    telephone: str = ""
    telephone_mobile: str = ""
    email: str = ""
    
    # Adresse
    adresse: str = ""
    code_postal: str = ""
    ville: str = ""
    canton: str = ""
    
    # Immobilier
    egrid: str = ""
    numero_parcelle: str = ""
    lien_rf: str = ""
    type_bien: str = ""
    surface: float = 0
    
    # Métadonnées
    source: str = ""
    source_priority: int = 0
    confidence: float = 0.0
    raw_data: Dict[str, Any] = field(default_factory=dict)
    collected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def __post_init__(self):
        """Calcule la priorité source après initialisation."""
        if self.source and not self.source_priority:
            self.source_priority = SOURCE_PRIORITY.get(self.source, 50)

    @property
    def has_phone(self) -> bool:
        return bool(self.telephone or self.telephone_mobile)

    @property
    def has_email(self) -> bool:
        return bool(self.email)

    @property
    def has_address(self) -> bool:
        return bool(self.adresse and self.ville)

    @property
    def completeness_score(self) -> int:
        """Score de complétude du profil."""
        score = 0
        if self.nom:
            score += 20
        if self.prenom:
            score += 10
        if self.telephone:
            score += 25
        if self.telephone_mobile:
            score += 30  # Mobile plus précieux
        if self.email:
            score += 15
        if self.adresse:
            score += 10
        if self.code_postal:
            score += 5
        if self.ville:
            score += 5
        if self.canton:
            score += 3
        if self.egrid:
            score += 10
        if self.lien_rf:
            score += 5
        return min(score, 100)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nom": self.nom,
            "prenom": self.prenom,
            "date_naissance": self.date_naissance,
            "telephone": self.telephone,
            "telephone_mobile": self.telephone_mobile,
            "email": self.email,
            "adresse": self.adresse,
            "code_postal": self.code_postal,
            "ville": self.ville,
            "canton": self.canton,
            "egrid": self.egrid,
            "numero_parcelle": self.numero_parcelle,
            "lien_rf": self.lien_rf,
            "type_bien": self.type_bien,
            "surface": self.surface,
            "source": self.source,
            "confidence": self.confidence,
            "completeness_score": self.completeness_score,
        }


@dataclass
class MergeResult:
    """Résultat d'une fusion de profils."""
    merged_profile: ProfileData
    source_profiles: List[ProfileData]
    merge_confidence: float
    merged_fields: Dict[str, str]  # champ -> source qui a fourni la valeur
    conflicts: List[Dict[str, Any]]  # Conflits détectés
    prospect_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "merged_profile": self.merged_profile.to_dict(),
            "source_count": len(self.source_profiles),
            "merge_confidence": self.merge_confidence,
            "merged_fields": self.merged_fields,
            "conflicts_count": len(self.conflicts),
            "prospect_id": self.prospect_id,
        }


class ProfileMergeService:
    """
    Service de fusion intelligente de profils.
    
    Fonctionnalités:
    - Matching flou (nom, adresse, téléphone)
    - Fusion basée sur priorité des sources
    - Détection de conflits
    - Historique des fusions
    
    Usage:
        service = ProfileMergeService()
        
        # Fusionner plusieurs profils
        result = await service.merge_profiles([profile1, profile2, profile3])
        
        # Chercher et fusionner avec la DB
        result = await service.find_and_merge(new_profile)
    """

    def __init__(self):
        pass

    # =========================================================================
    # MATCHING
    # =========================================================================

    def calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calcule la similarité entre deux noms."""
        if not name1 or not name2:
            return 0.0
        
        # Normaliser
        n1 = self._normalize_name(name1)
        n2 = self._normalize_name(name2)
        
        if n1 == n2:
            return 1.0
        
        # Similarité de séquence
        return SequenceMatcher(None, n1, n2).ratio()

    def calculate_phone_similarity(self, phone1: str, phone2: str) -> float:
        """Calcule la similarité entre deux téléphones."""
        if not phone1 or not phone2:
            return 0.0
        
        # Normaliser
        p1 = self._normalize_phone(phone1)
        p2 = self._normalize_phone(phone2)
        
        if p1 == p2:
            return 1.0
        
        # Vérifier les derniers chiffres (parfois préfixe différent)
        if len(p1) >= 7 and len(p2) >= 7:
            if p1[-7:] == p2[-7:]:
                return 0.9
        
        return 0.0

    def calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """Calcule la similarité entre deux adresses."""
        if not addr1 or not addr2:
            return 0.0
        
        # Normaliser
        a1 = self._normalize_address(addr1)
        a2 = self._normalize_address(addr2)
        
        if a1 == a2:
            return 1.0
        
        # Similarité de séquence
        return SequenceMatcher(None, a1, a2).ratio()

    def calculate_profile_similarity(
        self,
        profile1: ProfileData,
        profile2: ProfileData,
    ) -> Tuple[float, Dict[str, float]]:
        """
        Calcule la similarité globale entre deux profils.
        
        Returns:
            Tuple (score global, détail par champ)
        """
        scores = {}
        weights = {
            "nom": 0.30,
            "prenom": 0.15,
            "telephone": 0.25,
            "email": 0.15,
            "adresse": 0.10,
            "egrid": 0.05,
        }
        
        # Nom
        nom_full1 = f"{profile1.prenom} {profile1.nom}".strip()
        nom_full2 = f"{profile2.prenom} {profile2.nom}".strip()
        scores["nom"] = self.calculate_name_similarity(nom_full1, nom_full2)
        
        # Prénom séparé (bonus)
        scores["prenom"] = self.calculate_name_similarity(
            profile1.prenom, profile2.prenom
        )
        
        # Téléphone (fixe ou mobile)
        phone1 = profile1.telephone or profile1.telephone_mobile
        phone2 = profile2.telephone or profile2.telephone_mobile
        scores["telephone"] = self.calculate_phone_similarity(phone1, phone2)
        
        # Email
        if profile1.email and profile2.email:
            scores["email"] = 1.0 if profile1.email.lower() == profile2.email.lower() else 0.0
        else:
            scores["email"] = 0.0
        
        # Adresse
        addr1 = f"{profile1.adresse} {profile1.code_postal} {profile1.ville}".strip()
        addr2 = f"{profile2.adresse} {profile2.code_postal} {profile2.ville}".strip()
        scores["adresse"] = self.calculate_address_similarity(addr1, addr2)
        
        # EGRID (match exact requis)
        if profile1.egrid and profile2.egrid:
            scores["egrid"] = 1.0 if profile1.egrid == profile2.egrid else 0.0
        else:
            scores["egrid"] = 0.0
        
        # Score global pondéré
        total_weight = 0
        weighted_sum = 0
        
        for field, weight in weights.items():
            if scores.get(field, 0) > 0 or (
                getattr(profile1, field, None) and getattr(profile2, field, None)
            ):
                weighted_sum += scores.get(field, 0) * weight
                total_weight += weight
        
        global_score = weighted_sum / total_weight if total_weight > 0 else 0.0
        
        # Bonus pour EGRID match (identifiant unique)
        if scores.get("egrid", 0) == 1.0:
            global_score = max(global_score, 0.95)
        
        # Bonus pour téléphone exact match
        if scores.get("telephone", 0) == 1.0:
            global_score = max(global_score, 0.90)
        
        return global_score, scores

    # =========================================================================
    # FUSION
    # =========================================================================

    def merge_profiles(self, profiles: List[ProfileData]) -> MergeResult:
        """
        Fusionne plusieurs profils en un seul.
        
        Stratégie:
        1. Trier par priorité de source
        2. Pour chaque champ, prendre la valeur de la source la plus fiable
        3. Combiner les téléphones (fixe + mobile)
        4. Détecter les conflits
        """
        if not profiles:
            raise ValueError("Au moins un profil requis")
        
        if len(profiles) == 1:
            return MergeResult(
                merged_profile=profiles[0],
                source_profiles=profiles,
                merge_confidence=1.0,
                merged_fields={},
                conflicts=[],
            )
        
        # Trier par priorité
        sorted_profiles = sorted(
            profiles,
            key=lambda p: (p.source_priority, p.completeness_score),
            reverse=True,
        )
        
        merged = ProfileData()
        merged_fields = {}
        conflicts = []
        
        # Fusionner chaque champ
        fields_to_merge = [
            "nom", "prenom", "date_naissance",
            "telephone", "telephone_mobile", "email",
            "adresse", "code_postal", "ville", "canton",
            "egrid", "numero_parcelle", "lien_rf",
            "type_bien", "surface",
        ]
        
        for field in fields_to_merge:
            value, source, field_conflicts = self._merge_field(
                field, sorted_profiles
            )
            if value:
                setattr(merged, field, value)
                merged_fields[field] = source
            if field_conflicts:
                conflicts.extend(field_conflicts)
        
        # Combiner les téléphones
        all_phones = set()
        all_mobiles = set()
        
        for p in sorted_profiles:
            if p.telephone:
                norm_phone = self._normalize_phone(p.telephone)
                if norm_phone.startswith("+4179") or norm_phone.startswith("+4178") or norm_phone.startswith("+4177"):
                    all_mobiles.add(norm_phone)
                else:
                    all_phones.add(norm_phone)
            if p.telephone_mobile:
                all_mobiles.add(self._normalize_phone(p.telephone_mobile))
        
        # Prendre le premier de chaque
        if all_phones and not merged.telephone:
            merged.telephone = list(all_phones)[0]
        if all_mobiles:
            merged.telephone_mobile = list(all_mobiles)[0]
        
        # Source combinée
        sources = list(set(p.source for p in sorted_profiles if p.source))
        merged.source = " + ".join(sources[:3])
        
        # Calculer la confiance du merge
        merge_confidence = self._calculate_merge_confidence(sorted_profiles)
        merged.confidence = merge_confidence
        
        return MergeResult(
            merged_profile=merged,
            source_profiles=profiles,
            merge_confidence=merge_confidence,
            merged_fields=merged_fields,
            conflicts=conflicts,
        )

    def _merge_field(
        self,
        field: str,
        profiles: List[ProfileData],
    ) -> Tuple[Any, str, List[Dict]]:
        """
        Fusionne un champ spécifique.
        
        Returns:
            (valeur, source, conflits)
        """
        values_by_source = []
        
        for p in profiles:
            value = getattr(p, field, None)
            if value and (not isinstance(value, (int, float)) or value > 0):
                values_by_source.append((value, p.source, p.source_priority))
        
        if not values_by_source:
            return None, "", []
        
        # Trier par priorité
        values_by_source.sort(key=lambda x: x[2], reverse=True)
        
        # Détecter les conflits
        conflicts = []
        unique_values = set()
        
        for val, src, _ in values_by_source:
            if isinstance(val, str):
                normalized = val.strip().lower()
            else:
                normalized = str(val)
            
            if normalized not in unique_values and len(unique_values) > 0:
                # Vérifier si c'est vraiment un conflit
                existing = list(unique_values)[0]
                if field in ["nom", "prenom", "adresse"]:
                    sim = SequenceMatcher(None, existing, normalized).ratio()
                    if sim < 0.8:
                        conflicts.append({
                            "field": field,
                            "values": [v[0] for v in values_by_source],
                            "sources": [v[1] for v in values_by_source],
                        })
                        break
                elif field == "telephone":
                    # Téléphones différents ne sont pas des conflits
                    pass
                else:
                    conflicts.append({
                        "field": field,
                        "values": [v[0] for v in values_by_source],
                        "sources": [v[1] for v in values_by_source],
                    })
                    break
            
            unique_values.add(normalized)
        
        # Retourner la valeur de la source la plus fiable
        best_value, best_source, _ = values_by_source[0]
        return best_value, best_source, conflicts

    def _calculate_merge_confidence(self, profiles: List[ProfileData]) -> float:
        """Calcule la confiance globale du merge."""
        if len(profiles) < 2:
            return 1.0
        
        # Calculer la similarité moyenne entre tous les profils
        similarities = []
        for i, p1 in enumerate(profiles):
            for p2 in profiles[i + 1:]:
                sim, _ = self.calculate_profile_similarity(p1, p2)
                similarities.append(sim)
        
        avg_similarity = sum(similarities) / len(similarities) if similarities else 0
        
        # Bonus pour les sources fiables
        max_priority = max(p.source_priority for p in profiles)
        priority_bonus = min(max_priority / 100 * 0.1, 0.1)
        
        return min(avg_similarity + priority_bonus, 1.0)

    # =========================================================================
    # DATABASE OPERATIONS
    # =========================================================================

    async def find_matching_prospects(
        self,
        profile: ProfileData,
        threshold: float = MATCH_THRESHOLD_MEDIUM,
        limit: int = 10,
    ) -> List[Tuple[Prospect, float]]:
        """
        Trouve les prospects existants qui matchent un profil.
        
        Returns:
            Liste de (Prospect, score) triée par score décroissant
        """
        async with AsyncSessionLocal() as db:
            # Construire les filtres de recherche
            filters = []
            
            # Recherche par téléphone (match exact)
            if profile.telephone:
                norm_phone = self._normalize_phone(profile.telephone)
                filters.append(Prospect.telephone_norm == norm_phone)
            
            if profile.telephone_mobile:
                norm_mobile = self._normalize_phone(profile.telephone_mobile)
                filters.append(Prospect.telephone_norm == norm_mobile)
            
            # Recherche par email (match exact)
            if profile.email:
                filters.append(Prospect.email_norm == profile.email.lower())
            
            # Recherche par lien RF (match exact)
            if profile.lien_rf:
                filters.append(Prospect.lien_rf == profile.lien_rf)
            
            # Recherche par nom + ville
            if profile.nom and profile.ville:
                filters.append(
                    and_(
                        Prospect.nom.ilike(f"%{profile.nom}%"),
                        Prospect.ville.ilike(f"%{profile.ville}%"),
                    )
                )
            
            if not filters:
                return []
            
            # Exécuter la requête
            query = (
                select(Prospect)
                .where(Prospect.merged_into_id.is_(None))
                .where(or_(*filters))
                .limit(limit * 2)  # Prendre plus pour le scoring
            )
            
            result = await db.execute(query)
            prospects = result.scalars().all()
            
            # Scorer chaque prospect
            scored_prospects = []
            for prospect in prospects:
                prospect_profile = self._prospect_to_profile(prospect)
                score, _ = self.calculate_profile_similarity(profile, prospect_profile)
                if score >= threshold:
                    scored_prospects.append((prospect, score))
            
            # Trier par score
            scored_prospects.sort(key=lambda x: x[1], reverse=True)
            
            return scored_prospects[:limit]

    async def find_and_merge(
        self,
        profile: ProfileData,
        auto_merge_threshold: float = MATCH_THRESHOLD_HIGH,
    ) -> MergeResult:
        """
        Trouve un prospect existant et fusionne, ou crée un nouveau.
        
        Args:
            profile: Profil à intégrer
            auto_merge_threshold: Seuil pour merge automatique
            
        Returns:
            MergeResult avec prospect_id
        """
        # Chercher des matches
        matches = await self.find_matching_prospects(
            profile,
            threshold=MATCH_THRESHOLD_LOW,
        )
        
        if not matches:
            # Pas de match, créer un nouveau prospect
            prospect_id = await self._create_prospect(profile)
            return MergeResult(
                merged_profile=profile,
                source_profiles=[profile],
                merge_confidence=1.0,
                merged_fields={},
                conflicts=[],
                prospect_id=prospect_id,
            )
        
        # Prendre le meilleur match
        best_prospect, best_score = matches[0]
        
        if best_score >= auto_merge_threshold:
            # Auto-merge
            existing_profile = self._prospect_to_profile(best_prospect)
            merge_result = self.merge_profiles([profile, existing_profile])
            
            # Mettre à jour le prospect
            await self._update_prospect(best_prospect.id, merge_result.merged_profile)
            merge_result.prospect_id = best_prospect.id
            
            # Logger la fusion
            await self._log_merge(
                source_profile=profile,
                target_id=best_prospect.id,
                score=best_score,
                merged_fields=merge_result.merged_fields,
            )
            
            await emit_activity(
                "merge",
                f"Profil fusionné avec {best_prospect.nom} (score: {best_score:.2f})",
            )
            
            return merge_result
        
        else:
            # Score insuffisant, créer un nouveau prospect
            # mais signaler le match potentiel
            prospect_id = await self._create_prospect(profile)
            
            return MergeResult(
                merged_profile=profile,
                source_profiles=[profile],
                merge_confidence=1.0,
                merged_fields={},
                conflicts=[{
                    "type": "potential_duplicate",
                    "prospect_id": best_prospect.id,
                    "prospect_name": f"{best_prospect.prenom} {best_prospect.nom}",
                    "match_score": best_score,
                }],
                prospect_id=prospect_id,
            )

    async def _create_prospect(self, profile: ProfileData) -> str:
        """Crée un nouveau prospect depuis un profil."""
        async with AsyncSessionLocal() as db:
            prospect_id = f"pm-{uuid.uuid4().hex[:10]}"
            
            prospect = Prospect(
                id=prospect_id,
                nom=profile.nom,
                prenom=profile.prenom,
                telephone=profile.telephone or profile.telephone_mobile,
                telephone_norm=self._normalize_phone(
                    profile.telephone or profile.telephone_mobile
                ),
                email=profile.email,
                email_norm=profile.email.lower() if profile.email else None,
                adresse=profile.adresse,
                adresse_norm=self._normalize_address(profile.adresse),
                code_postal=profile.code_postal,
                ville=profile.ville,
                canton=profile.canton or "GE",
                lien_rf=profile.lien_rf,
                type_bien=profile.type_bien,
                surface=profile.surface,
                source=profile.source,
                quality_score=profile.completeness_score,
                notes=f"Mobile: {profile.telephone_mobile}" if profile.telephone_mobile else None,
            )
            
            db.add(prospect)
            await db.commit()
            
            return prospect_id

    async def _update_prospect(self, prospect_id: str, profile: ProfileData):
        """Met à jour un prospect avec les données du profil fusionné."""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Prospect).where(Prospect.id == prospect_id)
            )
            prospect = result.scalar_one_or_none()
            
            if not prospect:
                return
            
            # Mettre à jour les champs non vides
            if profile.nom:
                prospect.nom = profile.nom
            if profile.prenom:
                prospect.prenom = profile.prenom
            if profile.telephone:
                prospect.telephone = profile.telephone
                prospect.telephone_norm = self._normalize_phone(profile.telephone)
            if profile.email:
                prospect.email = profile.email
                prospect.email_norm = profile.email.lower()
            if profile.adresse:
                prospect.adresse = profile.adresse
                prospect.adresse_norm = self._normalize_address(profile.adresse)
            if profile.code_postal:
                prospect.code_postal = profile.code_postal
            if profile.ville:
                prospect.ville = profile.ville
            if profile.canton:
                prospect.canton = profile.canton
            if profile.lien_rf:
                prospect.lien_rf = profile.lien_rf
            if profile.type_bien:
                prospect.type_bien = profile.type_bien
            if profile.surface:
                prospect.surface = profile.surface
            
            # Ajouter le mobile dans les notes si présent
            if profile.telephone_mobile:
                mobile_note = f"Mobile: {profile.telephone_mobile}"
                if prospect.notes:
                    if mobile_note not in prospect.notes:
                        prospect.notes += f"\n{mobile_note}"
                else:
                    prospect.notes = mobile_note
            
            # Mettre à jour le score
            prospect.quality_score = max(
                prospect.quality_score or 0,
                profile.completeness_score,
            )
            
            # Mettre à jour la source
            if profile.source and profile.source not in (prospect.source or ""):
                prospect.source = f"{prospect.source or ''} + {profile.source}".strip(" +")
            
            prospect.updated_at = datetime.utcnow()
            await db.commit()

    async def _log_merge(
        self,
        source_profile: ProfileData,
        target_id: str,
        score: float,
        merged_fields: Dict[str, str],
    ):
        """Log une fusion pour audit."""
        async with AsyncSessionLocal() as db:
            log = ProspectMergeLog(
                source_id=f"temp-{uuid.uuid4().hex[:8]}",
                target_id=target_id,
                reason=f"Auto-merge (score: {score:.2f})",
                merged_fields={
                    "source": source_profile.source,
                    "fields": merged_fields,
                    "confidence": score,
                },
            )
            db.add(log)
            await db.commit()

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _normalize_name(self, name: str) -> str:
        """Normalise un nom pour comparaison."""
        if not name:
            return ""
        # Lowercase, supprimer accents et caractères spéciaux
        import unicodedata
        normalized = unicodedata.normalize("NFKD", name.lower())
        normalized = "".join(c for c in normalized if not unicodedata.combining(c))
        normalized = re.sub(r"[^a-z\s]", "", normalized)
        return " ".join(normalized.split())

    def _normalize_phone(self, phone: str) -> str:
        """Normalise un téléphone pour comparaison."""
        if not phone:
            return ""
        # Garder uniquement les chiffres et +
        cleaned = re.sub(r"[^\d+]", "", phone)
        # Normalisation suisse
        if cleaned.startswith("00"):
            cleaned = "+" + cleaned[2:]
        if cleaned.startswith("0") and len(cleaned) == 10:
            cleaned = "+41" + cleaned[1:]
        if cleaned.startswith("41") and len(cleaned) == 11:
            cleaned = "+41" + cleaned[2:]
        return cleaned

    def _normalize_address(self, address: str) -> str:
        """Normalise une adresse pour comparaison."""
        if not address:
            return ""
        # Lowercase, supprimer ponctuation excessive
        normalized = address.lower()
        normalized = re.sub(r"[,.]", " ", normalized)
        normalized = " ".join(normalized.split())
        return normalized

    def _prospect_to_profile(self, prospect: Prospect) -> ProfileData:
        """Convertit un Prospect en ProfileData."""
        return ProfileData(
            nom=prospect.nom or "",
            prenom=prospect.prenom or "",
            telephone=prospect.telephone or "",
            email=prospect.email or "",
            adresse=prospect.adresse or "",
            code_postal=prospect.code_postal or "",
            ville=prospect.ville or "",
            canton=prospect.canton or "",
            lien_rf=prospect.lien_rf or "",
            type_bien=prospect.type_bien or "",
            surface=prospect.surface or 0,
            source=prospect.source or "Database",
        )


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def merge_profile_into_db(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Helper pour fusionner un profil dans la DB.
    
    Args:
        profile_data: Dict avec les données du profil
        
    Returns:
        Résultat de la fusion
    """
    service = ProfileMergeService()
    
    profile = ProfileData(
        nom=profile_data.get("nom", ""),
        prenom=profile_data.get("prenom", ""),
        telephone=profile_data.get("telephone", ""),
        telephone_mobile=profile_data.get("telephone_mobile", ""),
        email=profile_data.get("email", ""),
        adresse=profile_data.get("adresse", ""),
        code_postal=profile_data.get("code_postal", ""),
        ville=profile_data.get("ville", ""),
        canton=profile_data.get("canton", ""),
        egrid=profile_data.get("egrid", ""),
        numero_parcelle=profile_data.get("numero_parcelle", ""),
        lien_rf=profile_data.get("lien_rf", ""),
        type_bien=profile_data.get("type_bien", ""),
        source=profile_data.get("source", "Import"),
    )
    
    result = await service.find_and_merge(profile)
    return result.to_dict()


async def batch_merge_profiles(profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Fusionne un batch de profils dans la DB.
    
    Returns:
        Stats du batch
    """
    service = ProfileMergeService()
    
    stats = {
        "total": len(profiles),
        "created": 0,
        "merged": 0,
        "errors": 0,
    }
    
    for profile_data in profiles:
        try:
            profile = ProfileData(**profile_data)
            result = await service.find_and_merge(profile)
            
            if len(result.source_profiles) > 1:
                stats["merged"] += 1
            else:
                stats["created"] += 1
                
        except Exception as e:
            logger.error(f"[ProfileMerge] Erreur batch: {e}")
            stats["errors"] += 1
    
    return stats
