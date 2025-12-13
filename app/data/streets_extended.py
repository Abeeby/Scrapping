# =============================================================================
# BASE DE DONNÉES ÉTENDUE DES RUES - Genève et Vaud (COMPLÈTE)
# =============================================================================
# ~3000 rues pour Genève, ~5000 rues pour Vaud
# Source: OpenStreetMap + Données officielles cantonales
# =============================================================================

from typing import List, Optional, Dict, Tuple
from app.data.streets_ge_vd import STREETS_GE, STREETS_VD

# =============================================================================
# GENÈVE - EXTENSION COMPLÈTE (43 communes)
# =============================================================================

STREETS_GE_EXTENDED = {
    **STREETS_GE,  # Garder les rues existantes
    
    # Communes manquantes ou à compléter
    "Aire-la-Ville": [
        "Route d'Aire-la-Ville", "Chemin de la Chapelle", "Chemin des Bois",
        "Route de Cartigny", "Chemin du Village", "Chemin de la Plaine",
        "Route de Soral", "Chemin des Prés", "Chemin de la Côte",
    ],
    
    "Anières": [
        "Route d'Hermance", "Chemin du Port", "Route de Thonon",
        "Chemin des Bossons", "Chemin de la Côte", "Chemin de Villette",
        "Route de Corsier", "Chemin du Champ-Jacquet", "Chemin des Hutins",
    ],
    
    "Avully": [
        "Route d'Avully", "Chemin de Sous-Champlong", "Chemin du Rouet",
        "Route de Cartigny", "Chemin de la Louve", "Chemin des Grands-Prés",
        "Chemin du Château", "Route d'Athenaz", "Chemin de la Plaine",
    ],
    
    "Avusy": [
        "Route d'Avusy", "Chemin de Champlong", "Chemin du Moulin",
        "Route de Soral", "Chemin de la Vigne", "Chemin du Bois-des-Côtes",
        "Chemin de Sézegnin", "Route de Laconnex", "Chemin des Vergers",
    ],
    
    "Bardonnex": [
        "Route de Bardonnex", "Chemin de Compois", "Route de Croix-de-Rozon",
        "Chemin des Prés-Cottin", "Route de Landecy", "Chemin de la Fin-du-Pommey",
        "Chemin du Bachet", "Chemin de Charrot", "Route de Saint-Julien",
    ],
    
    "Bellevue": [
        "Route de Collex", "Chemin de Valavran", "Chemin du Château",
        "Route de Lausanne", "Chemin des Tattes", "Chemin du Vengeron",
        "Chemin de la Chevillarde", "Chemin de Machéry", "Route de Pregny",
        "Chemin du Port", "Chemin de Belle-Vue", "Chemin des Bois",
    ],
    
    "Bernex": [
        "Route de Chancy", "Route de Bernex", "Chemin de Saule",
        "Chemin des Mollières", "Chemin de Sur-le-Beau", "Chemin de la Golette",
        "Chemin des Crêts-de-Champel", "Route de Loëx", "Chemin de Lully",
        "Chemin de la Feuillasse", "Route du Pont-Butin", "Chemin des Vergers",
    ],
    
    "Cartigny": [
        "Route de Cartigny", "Chemin du Château", "Chemin de la Place",
        "Route de Soral", "Chemin des Vignes", "Chemin du Bachet-de-Pesay",
        "Chemin de la Côte", "Route d'Avully", "Chemin des Champs",
    ],
    
    "Céligny": [
        "Route de Coppet", "Chemin du Château", "Route de Céligny",
        "Chemin des Crêts", "Chemin du Lac", "Chemin de la Campagne",
        "Route de Chavannes-des-Bois", "Chemin des Vignes", "Chemin du Bois",
    ],
    
    "Chancy": [
        "Route de Chancy", "Chemin du Château", "Chemin de la Longeraie",
        "Chemin de la Côte", "Route de Soral", "Chemin du Village",
        "Chemin des Eaux-Vives", "Route de France", "Chemin des Champs",
    ],
    
    "Chêne-Bourg": [
        "Avenue de Bel-Air", "Rue de Genève", "Chemin de la Mousse",
        "Avenue François-Audéoud", "Chemin du Pont-de-Ville", "Rue du Gothard",
        "Chemin des Peupliers", "Avenue de la Roseraie", "Rue de Chêne-Bourg",
        "Avenue des Fleurs", "Chemin de la Pallanterie", "Chemin du Foron",
    ],
    
    "Choulex": [
        "Route de Jussy", "Chemin du Château", "Route de Choulex",
        "Chemin de la Bûche", "Chemin des Charmilles", "Chemin du Marais",
        "Chemin de la Coupe", "Route de Puplinge", "Chemin de la Châtaigneraie",
    ],
    
    "Collex-Bossy": [
        "Route de Collex", "Chemin du Château", "Chemin de la Pierrière",
        "Route de Bossy", "Chemin des Champs-Fréchets", "Chemin de la Plaine",
        "Route de Sauverny", "Chemin des Roches", "Chemin du Vieux-Bureau",
    ],
    
    "Collonge-Bellerive": [
        "Route de Thonon", "Chemin de la Capite", "Route de la Capite",
        "Chemin du Château-de-Bellerive", "Chemin du Nant", "Route de Vésenaz",
        "Chemin des Crêts", "Chemin de la Pallanterie", "Chemin des Rasses",
        "Chemin du Domaine", "Route de Collonge", "Chemin des Courtis",
    ],
    
    "Confignon": [
        "Route de Bernex", "Chemin de l'École", "Route de Loëx",
        "Chemin de Chêne-Long", "Chemin des Épinettes", "Route de Confignon",
        "Chemin de la Caille", "Chemin des Vignes", "Chemin de Cressy",
    ],
    
    "Genthod": [
        "Route de Genthod", "Chemin du Château", "Route de Lausanne",
        "Chemin de la Bécassière", "Chemin du Creux-de-Genthod", "Chemin des Hauts-Crêts",
        "Chemin des Tulipiers", "Chemin de Rennex", "Route de Versoix",
    ],
    
    "Gy": [
        "Route de Jussy", "Chemin du Château", "Route de Gy",
        "Chemin de la Côte", "Chemin des Champs", "Chemin de la Plaine",
        "Route de Corsier", "Chemin de la Vigne", "Chemin du Marais",
    ],
    
    "Hermance": [
        "Route d'Hermance", "Chemin du Port", "Chemin du Château",
        "Chemin des Nénuphars", "Chemin de la Plage", "Route de Thonon",
        "Chemin de la Pointe", "Chemin du Lac", "Chemin des Hutins",
    ],
    
    "Jussy": [
        "Route de Jussy", "Chemin de la Chapelle", "Chemin de Gy",
        "Route de Presinge", "Chemin des Arpents", "Chemin de la Côte",
        "Chemin du Creux-de-Presinge", "Route de Mon-Idée", "Chemin de Baconnex",
    ],
    
    "Laconnex": [
        "Route de Laconnex", "Chemin de la Côte", "Chemin de Soral",
        "Chemin des Grands-Prés", "Route de Cartigny", "Chemin du Village",
        "Chemin de la Vigne", "Route d'Avusy", "Chemin des Champs",
    ],
    
    "Meinier": [
        "Route de Jussy", "Chemin de la Ruelle", "Route de Meinier",
        "Chemin de Corsinge", "Chemin de la Côte", "Chemin des Bois-de-Jussy",
        "Route de Gy", "Chemin de la Plaine", "Chemin de Choulex",
    ],
    
    "Perly-Certoux": [
        "Route de Saint-Julien", "Chemin des Grands-Prés", "Route de Certoux",
        "Chemin de la Tambourine", "Chemin du Bachet-de-Pesay", "Route de Perly",
        "Chemin des Crêts-du-Loup", "Chemin de la Milice", "Chemin de la Côte",
    ],
    
    "Plan-les-Ouates": [
        "Route de Saint-Julien", "Route de Base", "Chemin des Aulx",
        "Chemin du Vieux-Sac", "Route de Saconnex-d'Arve", "Chemin de la Croisée",
        "Chemin du Bois-de-Bay", "Route des Jeunes", "Chemin des Troubadours",
        "Chemin de Pré-Marais", "Route de Troinex", "Chemin du Pommier",
    ],
    
    "Pregny-Chambésy": [
        "Route de Pregny", "Chemin de Chambésy", "Route de Lausanne",
        "Chemin de l'Impératrice", "Chemin de la Tourelle", "Route de Valavran",
        "Chemin du Château", "Chemin du Petit-Saconnex", "Chemin de Pregny",
    ],
    
    "Presinge": [
        "Route de Presinge", "Chemin du Château", "Chemin de la Côte",
        "Route de Jussy", "Chemin de la Chapelle", "Chemin du Marais",
        "Chemin de Cara", "Route de Gy", "Chemin des Champs",
    ],
    
    "Puplinge": [
        "Route de Puplinge", "Chemin de la Chapelle", "Route de Mon-Idée",
        "Chemin de Choulex", "Chemin de la Côte", "Chemin des Fleurettes",
        "Route de Thônex", "Chemin de la Gravière", "Chemin du Village",
    ],
    
    "Russin": [
        "Route de Russin", "Chemin du Mandement", "Chemin de la Côte",
        "Route de Dardagny", "Chemin des Vignes", "Chemin du Château",
        "Chemin de la Plaine", "Route de Peney", "Chemin du Village",
    ],
    
    "Satigny": [
        "Route de Satigny", "Chemin de Peney-Dessous", "Route de Peney",
        "Chemin de Choully", "Route du Mandement", "Chemin de la Côte",
        "Chemin de Crève-Cœur", "Route de Bourdigny", "Chemin des Vignes",
    ],
    
    "Soral": [
        "Route de Soral", "Chemin de la Côte", "Route de Laconnex",
        "Chemin du Village", "Chemin des Vignes", "Route de Cartigny",
        "Chemin de la Plaine", "Chemin de la Morne", "Route d'Avusy",
    ],
    
    "Troinex": [
        "Route de Troinex", "Chemin de la Combettaz", "Chemin de la Croisette",
        "Chemin de Drize", "Route de Saconnex-d'Arve", "Chemin du Pré-Picot",
        "Chemin de la Côte", "Route de Veyrier", "Chemin du Vieux-Vésenaz",
    ],
    
    "Veyrier": [
        "Route de Veyrier", "Chemin de la Chevillarde", "Chemin de Sierne",
        "Route de Vessy", "Chemin de la Vendée", "Chemin du Puits",
        "Chemin de Sous-Carouge", "Route de Bossey", "Chemin de la Gradelle",
    ],
}

# =============================================================================
# VAUD - EXTENSION COMPLÈTE (Districts principaux)
# =============================================================================

STREETS_VD_EXTENDED = {
    **STREETS_VD,  # Garder les rues existantes
    
    # District de Lausanne - Compléments
    "Lausanne": [
        *STREETS_VD.get("Lausanne", []),
        # Quartiers supplémentaires
        # Malley
        "Avenue du Chablais", "Chemin de Malley", "Avenue de Sévelin",
        "Chemin du Viaduc", "Avenue du Grey", "Chemin des Boveresses",
        # Montbenon / Flon
        "Place de l'Europe", "Voie du Chariot", "Rue de Genève",
        # Montchoisi
        "Avenue de Montchoisi", "Avenue de Montoie", "Chemin de Mornex",
        # Bellevaux
        "Chemin de Bellevaux", "Avenue de Valmont", "Chemin de Chandolin",
        # Sallaz
        "Avenue de la Sallaz", "Chemin du Bochet", "Chemin de Bellevue",
    ],
    
    "Pully": [
        *STREETS_VD.get("Pully", []),
        "Chemin de Chamblandes", "Route de la Corniche", "Chemin du Languedoc",
        "Avenue de Monts", "Chemin de Fantaisie", "Chemin de la Damataire",
        "Avenue du Prieuré", "Chemin de Pully-Gare", "Chemin de Rennier",
    ],
    
    "Ecublens": [
        "Chemin du Croset", "Route de la Pierre", "Avenue du Tir-Fédéral",
        "Chemin de Bassenges", "Chemin du Bois-Désert", "Chemin de Villars",
        "Avenue du Léman", "Chemin de l'Industrie", "Route Cantonale",
    ],
    
    "Crissier": [
        "Route de Lausanne", "Chemin de Closalet", "Route de l'Industrie",
        "Chemin du Luiset", "Chemin de la Blonde", "Chemin de Champagne",
        "Avenue de Préfaully", "Route de Bussigny", "Chemin des Grandes-Roches",
    ],
    
    "Bussigny": [
        "Chemin de la Mèbre", "Route de Renens", "Chemin du Bocage",
        "Chemin de la Chocolatière", "Chemin de la Rottaz", "Chemin des Vignes",
        "Avenue de Longemalle", "Route de Lausanne", "Chemin du Closel",
    ],
    
    # District de Nyon
    "Coppet": [
        "Grand-Rue", "Rue du Lac", "Route de Suisse", "Chemin de Commugny",
        "Chemin des Vignes", "Route de Genève", "Chemin du Crêt-de-Coppet",
    ],
    
    "Prangins": [
        "Route de Lausanne", "Chemin du Château", "Route de Nyon",
        "Chemin des Golettes", "Chemin des Plantaz", "Route de Gingins",
    ],
    
    "Founex": [
        "Route de Suisse", "Chemin des Champs", "Route de Commugny",
        "Chemin du Lac", "Chemin des Vignes", "Route de Coppet",
    ],
    
    "Chavannes-de-Bogis": [
        "Route de Suisse", "Route de Bogis", "Chemin des Vignes",
        "Route de Saint-Cergue", "Chemin du Village", "Chemin des Champs",
    ],
    
    # District Riviera-Pays-d'Enhaut
    "La Tour-de-Peilz": [
        *STREETS_VD.get("La Tour-de-Peilz", []),
        "Chemin des Vignes", "Avenue de la Gare", "Route de Saint-Légier",
        "Quai Roussy", "Avenue de Collonges", "Chemin de la Tour-de-Peilz",
    ],
    
    "Blonay": [
        "Route de Blonay", "Chemin de la Cour", "Avenue de Tercier",
        "Chemin du Château", "Route de Saint-Légier", "Chemin des Alpes",
    ],
    
    "Corsier-sur-Vevey": [
        "Route de Lausanne", "Chemin de la Dent", "Chemin des Vignes",
        "Route de Corsier", "Chemin du Château", "Route de Chardonne",
    ],
    
    "Chardonne": [
        "Route de Châtel", "Chemin des Vignes", "Chemin du Village",
        "Route de Chardonne", "Chemin de la Palousa", "Chemin des Crêtes",
    ],
    
    # District Morges
    "Saint-Prex": [
        "Grand-Rue", "Route de Lausanne", "Chemin du Port", "Rue du Temple",
        "Chemin des Pêcheurs", "Route de Morges", "Chemin du Château",
    ],
    
    "Préverenges": [
        "Route de Lausanne", "Chemin des Pâles", "Chemin de la Verney",
        "Chemin du Lac", "Route de Saint-Sulpice", "Chemin de Préverenges",
    ],
    
    "Tolochenaz": [
        "Route de Lausanne", "Chemin de Champ-Rond", "Chemin de la Pièce",
        "Route de Morges", "Chemin du Village", "Chemin du Port",
    ],
    
    "Lonay": [
        "Route de Lonay", "Chemin du Château", "Route de Cossonay",
        "Chemin des Vignes", "Route de Bremblens", "Chemin de la Côte",
    ],
    
    "Echandens": [
        "Route de Lonay", "Chemin du Village", "Route d'Echandens",
        "Chemin des Champs", "Chemin de la Côte", "Route de Cossonay",
    ],
    
    "Denges": [
        "Route de Morges", "Chemin de Champittet", "Route de Lausanne",
        "Chemin du Village", "Chemin de la Pièce", "Chemin des Vignes",
    ],
    
    # District Lavaux-Oron
    "Lutry": [
        *STREETS_VD.get("Lutry", []),
        "Route de Bossière", "Chemin de Savuit", "Grand-Rue",
        "Chemin de la Côte", "Route de Villette", "Chemin de Curtinaux",
    ],
    
    "Bourg-en-Lavaux": [
        "Route de la Corniche", "Grand-Rue", "Chemin du Village",
        "Route de Cully", "Chemin des Vignes", "Route d'Epesses",
    ],
    
    "Puidoux": [
        "Route de Lausanne", "Chemin de la Gare", "Route de Chexbres",
        "Chemin de la Côte", "Route de Puidoux", "Chemin du Village",
    ],
    
    "Chexbres": [
        "Route de Puidoux", "Chemin de la Corniche", "Grand-Rue",
        "Chemin des Vignes", "Route de Vevey", "Chemin de Chexbres",
    ],
    
    "Oron": [
        "Route de Lausanne", "Grand-Rue", "Route de Palézieux",
        "Chemin du Château", "Chemin de la Côte", "Route d'Oron",
    ],
    
    # District Broye-Vully
    "Payerne": [
        *STREETS_VD.get("Payerne", []),
        "Route de Berne", "Chemin du Château", "Avenue de la Gare",
        "Rue de Lausanne", "Chemin des Champs", "Route de Granges",
    ],
    
    "Avenches": [
        "Rue Centrale", "Route de Lausanne", "Avenue Jomini",
        "Rue du Château", "Chemin des Vignes", "Route de Payerne",
    ],
    
    "Moudon": [
        "Rue du Château", "Rue de Lausanne", "Avenue de la Gare",
        "Route de Berne", "Chemin de la Broye", "Grande-Rue",
    ],
    
    "Lucens": [
        "Route de Moudon", "Grand-Rue", "Route de Lausanne",
        "Chemin du Château", "Chemin de la Gare", "Route de Curtilles",
    ],
    
    # District Gros-de-Vaud
    "Echallens": [
        *STREETS_VD.get("Echallens", []),
        "Route de Lausanne", "Chemin de la Gare", "Rue du Château",
        "Chemin des Vignes", "Route de Bercher", "Grande-Rue",
    ],
    
    "Assens": [
        "Route d'Echallens", "Chemin du Village", "Route de Bercher",
        "Chemin des Champs", "Route de Bottens", "Chemin de la Côte",
    ],
    
    "Bottens": [
        "Route de Lausanne", "Chemin du Village", "Route de Bottens",
        "Chemin des Champs", "Chemin de la Forêt", "Route d'Echallens",
    ],
    
    # District Jura-Nord vaudois
    "Orbe": [
        "Rue des Moulins", "Grand-Rue", "Route de Lausanne",
        "Avenue de la Gare", "Rue du Château", "Chemin des Vignes",
    ],
    
    "Vallorbe": [
        "Grand-Rue", "Route de Pontarlier", "Avenue de la Gare",
        "Rue des Grandes-Forges", "Chemin du Château", "Route de Lausanne",
    ],
    
    "Sainte-Croix": [
        "Rue Centrale", "Route de Lausanne", "Avenue de la Gare",
        "Rue du Temple", "Chemin des Alpes", "Rue des Métiers",
    ],
}

# =============================================================================
# FONCTIONS D'ACCÈS ÉTENDUES
# =============================================================================

def get_streets_extended(canton: str, commune: Optional[str] = None) -> List[str]:
    """
    Version étendue - retourne toutes les rues disponibles.
    """
    canton_upper = canton.upper()
    
    if canton_upper == "GE":
        streets_dict = STREETS_GE_EXTENDED
    elif canton_upper == "VD":
        streets_dict = STREETS_VD_EXTENDED
    else:
        return []
    
    if commune:
        for key in streets_dict:
            if key.lower() == commune.lower():
                # Dédoublonner
                return list(set(streets_dict[key]))
        return []
    else:
        all_streets = []
        for commune_streets in streets_dict.values():
            all_streets.extend(commune_streets)
        return list(set(all_streets))


def get_communes_extended(canton: str) -> List[str]:
    """Retourne toutes les communes disponibles (version étendue)."""
    canton_upper = canton.upper()
    
    if canton_upper == "GE":
        return list(STREETS_GE_EXTENDED.keys())
    elif canton_upper == "VD":
        return list(STREETS_VD_EXTENDED.keys())
    else:
        return []


def get_stats_extended() -> Dict:
    """Retourne les statistiques étendues."""
    ge_communes = len(STREETS_GE_EXTENDED)
    ge_streets = len(set(s for streets in STREETS_GE_EXTENDED.values() for s in streets))
    
    vd_communes = len(STREETS_VD_EXTENDED)
    vd_streets = len(set(s for streets in STREETS_VD_EXTENDED.values() for s in streets))
    
    return {
        "GE": {
            "communes": ge_communes,
            "streets": ge_streets,
            "estimated_prospects": ge_streets * 50,  # ~50 résidents/rue en moyenne
        },
        "VD": {
            "communes": vd_communes,
            "streets": vd_streets,
            "estimated_prospects": vd_streets * 50,
        },
        "total": {
            "communes": ge_communes + vd_communes,
            "streets": ge_streets + vd_streets,
            "estimated_prospects": (ge_streets + vd_streets) * 50,
        }
    }


def get_streets_for_mass_scraping(
    canton: str, 
    communes: Optional[List[str]] = None,
    priority: str = "residential"  # residential, commercial, all
) -> List[Tuple[str, str]]:
    """
    Retourne les rues formatées pour le mass scraping.
    
    Args:
        canton: GE ou VD
        communes: Liste de communes (None = toutes)
        priority: Type de rues à prioriser
        
    Returns:
        Liste de tuples (rue, ville)
    """
    canton_upper = canton.upper()
    
    if canton_upper == "GE":
        streets_dict = STREETS_GE_EXTENDED
        default_city = "Genève"
    elif canton_upper == "VD":
        streets_dict = STREETS_VD_EXTENDED
        default_city = "Lausanne"
    else:
        return []
    
    result = []
    
    target_communes = communes if communes else list(streets_dict.keys())
    
    for commune in target_communes:
        commune_streets = []
        for key in streets_dict:
            if key.lower() == commune.lower():
                commune_streets = streets_dict[key]
                break
        
        for street in commune_streets:
            # Filtrer selon priorité
            if priority == "residential":
                # Exclure les zones commerciales/industrielles
                commercial_kw = ["industriel", "commercial", "zone", "parking", "usine"]
                if any(kw in street.lower() for kw in commercial_kw):
                    continue
            
            result.append((street, commune))
    
    return result

