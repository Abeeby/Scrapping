# =============================================================================
# BASE DE DONNÉES DES RUES - Genève et Vaud
# =============================================================================
# Liste des rues résidentielles pour le scraping massif
# Source: Données officielles cantonales + OpenStreetMap
# =============================================================================

from typing import List, Optional, Dict

# =============================================================================
# GENÈVE - Rues principales par commune
# =============================================================================

STREETS_GE = {
    "Genève": [
        # Centre-ville / Vieille-Ville
        "Rue du Rhône", "Rue de la Croix-d'Or", "Rue du Marché", "Rue de la Confédération",
        "Rue de Rive", "Rue du Conseil-Général", "Grand-Rue", "Rue de la Cité", "Place du Bourg-de-Four",
        "Rue de l'Hôtel-de-Ville", "Rue Calvin", "Rue Jean-Calvin", "Rue de la Fontaine",
        "Rue des Granges", "Rue Verdaine", "Rue Etienne-Dumont", "Rue du Perron",
        "Rue de la Tertasse", "Rue du Puits-Saint-Pierre", "Place de Neuve",
        
        # Plainpalais / Jonction
        "Boulevard des Philosophes", "Boulevard du Pont-d'Arve", "Rue de Carouge",
        "Rue des Bains", "Rue de l'École-de-Médecine", "Rue Prévost-Martin",
        "Avenue Henri-Dunant", "Rue Dancet", "Rue des Voisins", "Boulevard Carl-Vogt",
        "Rue de la Coulouvrenière", "Boulevard de Saint-Georges", "Rue Leschot",
        "Rue des Rois", "Rue des Minoteries", "Rue François-Dussaud",
        "Rue de Monthoux", "Rue Voltaire", "Rue de la Synagogue",
        
        # Eaux-Vives
        "Rue de la Terrassière", "Rue du 31-Décembre", "Rue de Montchoisy",
        "Avenue Pictet-de-Rochemont", "Rue des Eaux-Vives", "Rue du Nant",
        "Rue Sillem", "Rue François-Versonnex", "Rue Ami-Lullin",
        "Rue de la Mairie", "Rue de Villereuse", "Place des Eaux-Vives",
        "Boulevard Helvétique", "Rue de Jargonnant", "Chemin de Roches",
        "Rue du Lac", "Rue de la Cloche", "Rue Chandieu",
        
        # Champel
        "Avenue de Champel", "Avenue Krieg", "Chemin de la Roseraie",
        "Avenue de Miremont", "Chemin De-Normandie", "Chemin du Velours",
        "Chemin Malombré", "Chemin de la Colline", "Chemin de Grange-Canal",
        "Avenue Peschier", "Chemin des Clochettes", "Rue Micheli-du-Crest",
        "Avenue Dumas", "Chemin Thury", "Chemin de la Montagne",
        
        # Florissant / Malagnou
        "Route de Florissant", "Avenue de Frontenex", "Chemin de la Montagne",
        "Route de Malagnou", "Chemin du Nant-d'Argent", "Chemin de Ruth",
        "Avenue de Châtelaine", "Avenue William-Favre", "Chemin Rieu",
        "Chemin Frank-Thomas", "Chemin des Corbillettes", "Chemin des Cèdres",
        
        # Pâquis / Nations
        "Rue de Lausanne", "Rue du Mont-Blanc", "Rue de Berne",
        "Rue de Zurich", "Rue des Alpes", "Rue de Monthoux",
        "Rue de Neuchâtel", "Rue Sismondi", "Rue de Fribourg",
        "Rue Pellegrino-Rossi", "Rue de l'Industrie", "Rue du Prieuré",
        "Rue Butini", "Rue de la Navigation", "Rue des Pâquis",
        "Avenue de France", "Rue de Vermont", "Place des Nations",
        
        # Servette / Petit-Saconnex
        "Rue de la Servette", "Avenue de Châtelaine", "Rue de Lyon",
        "Rue de Montbrillant", "Rue Hoffmann", "Rue de Burlington",
        "Rue Eugène-Marziano", "Avenue Wendt", "Rue de Moillebeau",
        "Rue des Charmilles", "Avenue Blanc", "Rue Vautier",
        
        # Grottes / Saint-Gervais
        "Rue des Grottes", "Rue de la Prairie", "Rue Rousseau",
        "Place Cornavin", "Rue du Stand", "Rue de Coutance",
        "Rue Lissignol", "Boulevard James-Fazy", "Rue Vallin",
    ],
    
    "Carouge": [
        "Rue Ancienne", "Rue Saint-Joseph", "Rue Jacques-Dalphin",
        "Avenue Cardinal-Mermillod", "Place du Marché", "Rue de la Filature",
        "Rue du Tunnel", "Rue Caroline", "Rue Vautier",
        "Avenue Vibert", "Rue de la Fontenette", "Route de Saint-Julien",
        "Rue du Collège", "Rue des Moraines", "Chemin de Pinchat",
        "Rue du Pont-Neuf", "Avenue de la Praille", "Rue Blavignac",
    ],
    
    "Lancy": [
        "Route de Chancy", "Route du Grand-Lancy", "Avenue des Communes-Réunies",
        "Chemin de la Vendée", "Chemin des Palettes", "Avenue du Petit-Lancy",
        "Chemin des Troènes", "Route de Saint-Julien", "Avenue des Morgines",
        "Chemin du Centenaire", "Avenue Eugène-Lance", "Chemin des Sports",
    ],
    
    "Vernier": [
        "Route de Vernier", "Route de Meyrin", "Chemin de l'Etang",
        "Avenue Louis-Casaï", "Route du Nant-d'Avril", "Chemin de la Golette",
        "Chemin du Grand-Puits", "Avenue de Châtelaine", "Chemin des Corbillettes",
        "Chemin de Blandonnet", "Route de Peney", "Chemin de Maisonneuve",
    ],
    
    "Meyrin": [
        "Avenue de Vaudagne", "Route de Meyrin", "Avenue de Mategnin",
        "Rue de Livron", "Avenue Riant-Parc", "Chemin de la Golette",
        "Avenue de Feuillasse", "Chemin du Grand-Puits", "Route du Mandement",
    ],
    
    "Thônex": [
        "Route de Mon-Idée", "Route d'Ambilly", "Avenue de Thônex",
        "Chemin de la Mousse", "Chemin du Bois-des-Arts", "Route de Jussy",
        "Chemin des Beaux-Champs", "Avenue Industrielle", "Chemin de Grange-Falquet",
    ],
    
    "Chêne-Bougeries": [
        "Route de Chêne", "Avenue de Thônex", "Chemin de la Montagne",
        "Chemin de Grange-Canal", "Avenue de Bel-Air", "Chemin Naville",
        "Route de Malagnou", "Chemin du Velours", "Chemin de Conches",
    ],
    
    "Grand-Saconnex": [
        "Route de Ferney", "Route de Colovrex", "Chemin du Grand-Pré",
        "Avenue Giuseppe-Motta", "Chemin du Jonc", "Route de l'Aéroport",
        "Chemin des Fins", "Chemin du Pommier", "Chemin de la Perrière",
    ],
    
    "Cologny": [
        "Route de Thonon", "Chemin de Ruth", "Chemin Byron",
        "Route de la Capite", "Chemin du Nant-d'Argent", "Chemin des Hauts-Crêts",
        "Chemin de Valérie", "Chemin de la Gradelle", "Route de Frontenex",
    ],
    
    "Vandœuvres": [
        "Route de Vandœuvres", "Chemin de la Blonde", "Chemin de Crest",
        "Chemin des Buclines", "Route de Meinier", "Chemin de la Brunette",
        "Chemin de Narly", "Chemin du Vallon", "Route du Vallon",
    ],
    
    "Onex": [
        "Route de Chancy", "Avenue des Grandes-Communes", "Chemin du Bois-de-Bay",
        "Avenue du Bois-de-la-Chapelle", "Chemin de Rondeau", "Chemin de la Caroline",
        "Avenue de la Praille", "Chemin des Tacons", "Chemin de Cressy",
    ],
    
    "Versoix": [
        "Route de Suisse", "Chemin de la Boisserette", "Chemin du Château-de-Versoix",
        "Avenue de Choiseul", "Rue des Moulins", "Avenue Benteli",
        "Chemin des Colombettes", "Route de Saint-Loup", "Chemin de Richelien",
    ],
}

# =============================================================================
# VAUD - Rues principales par commune
# =============================================================================

STREETS_VD = {
    "Lausanne": [
        # Centre-ville
        "Place de la Palud", "Rue de Bourg", "Rue Centrale", "Place Saint-François",
        "Avenue Benjamin-Constant", "Rue du Grand-Pont", "Rue du Petit-Chêne",
        "Rue de la Louve", "Rue Saint-Laurent", "Rue du Pont",
        "Avenue du Théâtre", "Place de la Riponne", "Rue de la Mercerie",
        
        # Ouchy / Sous-Gare
        "Avenue de Cour", "Place de la Navigation", "Quai d'Ouchy",
        "Avenue de Rhodanie", "Chemin de Bellerive", "Avenue d'Ouchy",
        "Avenue de Montchoisi", "Chemin du Denantou", "Avenue de Villardin",
        
        # Chailly / Rovéréaz
        "Avenue de Chailly", "Avenue de Béthusy", "Chemin de Rovéréaz",
        "Avenue des Figuiers", "Chemin des Croisettes", "Avenue de Beaumont",
        
        # Pully / Limite Est
        "Avenue de Lavaux", "Route de Berne", "Avenue de Rumine",
        "Avenue Ruchonnet", "Chemin de Chandolin", "Avenue Victor-Ruffy",
        
        # Montriond / Florimont
        "Avenue de Montriond", "Chemin de Chandolin", "Avenue de Florimont",
        "Avenue de Rumine", "Avenue de la Rasude", "Chemin de Montolivet",
        
        # Prilly / Ouest
        "Route de Cossonay", "Avenue du Léman", "Route de Renens",
        "Chemin de Bossons", "Avenue de Morges", "Chemin des Grandes-Roches",
    ],
    
    "Nyon": [
        "Rue de la Gare", "Rue de Rive", "Grand-Rue", "Place du Château",
        "Rue de la Porcelaine", "Route de Saint-Cergue", "Chemin de la Vuarpillière",
        "Avenue Perdtemps", "Rue du Marché", "Route de Divonne",
        "Chemin de Changins", "Avenue de Viollier", "Rue du Temple",
        "Chemin du Reposoir", "Route de Crans", "Avenue Alfred-Cortot",
    ],
    
    "Morges": [
        "Grand-Rue", "Rue Louis-de-Savoie", "Rue de la Gare",
        "Avenue de Marcelin", "Rue de Lausanne", "Quai du Mont-Blanc",
        "Avenue de Riond-Bosson", "Chemin de la Grosse-Pierre", "Route de Denges",
        "Avenue Paderewski", "Rue du Sablon", "Place du Casino",
    ],
    
    "Vevey": [
        "Grande Place", "Rue du Lac", "Avenue de la Gare", "Rue du Simplon",
        "Rue du Conseil", "Avenue Reller", "Quai Perdonnet", "Rue du Théâtre",
        "Avenue de Gilamont", "Rue du Centre", "Rue du Torrent",
        "Avenue Général-Guisan", "Rue de la Madeleine", "Chemin de la Crottaz",
    ],
    
    "Montreux": [
        "Grand-Rue", "Avenue des Alpes", "Avenue Claude-Nobs", "Avenue de Belmont",
        "Rue du Marché", "Avenue du Casino", "Rue de la Gare", "Rue du Lac",
        "Avenue de Chillon", "Rue du Temple", "Avenue Nestlé",
        "Chemin du Châtelard", "Avenue de Collonge", "Route de Villard",
    ],
    
    "Renens": [
        "Rue de Lausanne", "Avenue du 14-Avril", "Chemin de la Mèbre",
        "Avenue de Florissant", "Rue du Midi", "Chemin du Closel",
        "Avenue du Silo", "Rue de l'Industrie", "Chemin de la Chocolatière",
    ],
    
    "Pully": [
        "Avenue de Lavaux", "Avenue du Prieuré", "Avenue de Rochettaz",
        "Chemin de la Damataire", "Avenue Général-Guisan", "Chemin de Monts",
        "Avenue de la Rosiaz", "Chemin des Fleurettes", "Chemin de Rennier",
    ],
    
    "Prilly": [
        "Chemin de la Chèvrerie", "Chemin du Bief", "Avenue de Longemalle",
        "Chemin de Florissant", "Route de Cossonay", "Chemin des Boveresses",
        "Avenue de la Gare", "Chemin du Daillard", "Route d'Yverdon",
    ],
    
    "Yverdon-les-Bains": [
        "Rue du Lac", "Rue du Casino", "Place Pestalozzi", "Rue de la Plaine",
        "Avenue des Bains", "Rue du Midi", "Rue du Four", "Rue du Valentin",
        "Avenue de Grandson", "Rue de Neuchâtel", "Rue de la Maison-Rouge",
        "Avenue Haldimand", "Rue des Moulins", "Route de Lausanne",
    ],
    
    "Gland": [
        "Route de Lausanne", "Chemin du Signal", "Route de l'Etraz",
        "Chemin du Grand-Champ", "Chemin de Montoly", "Rue du Village",
        "Chemin des Vignes", "Chemin de la Ballastière", "Route de Nyon",
    ],
    
    "Rolle": [
        "Grand-Rue", "Rue du Port", "Route de Lausanne", "Rue du Chablais",
        "Chemin des Bosquets", "Route de Mont-sur-Rolle", "Rue du Temple",
        "Place du Marché", "Chemin du Château", "Rue du Lac",
    ],
    
    "Aigle": [
        "Rue du Bourg", "Avenue du Chamossaire", "Rue de la Gare",
        "Place du Marché", "Rue du Cloître", "Avenue des Ormonts",
        "Route Industrielle", "Chemin de la Fontaine", "Rue de Jérusalem",
    ],
    
    "Bex": [
        "Avenue de la Gare", "Grand-Rue", "Route des Mines",
        "Rue Centrale", "Chemin des Posses", "Route de Frenières",
        "Avenue du Simplon", "Rue du Temple", "Chemin des Salines",
    ],
}


# =============================================================================
# FONCTIONS D'ACCÈS
# =============================================================================

def get_streets(canton: str, commune: Optional[str] = None) -> List[str]:
    """
    Retourne la liste des rues pour un canton et optionnellement une commune.
    
    Args:
        canton: Code canton (GE ou VD)
        commune: Nom de la commune (optionnel, sinon toutes les communes)
        
    Returns:
        Liste des noms de rues
    """
    canton_upper = canton.upper()
    
    if canton_upper == "GE":
        streets_dict = STREETS_GE
    elif canton_upper == "VD":
        streets_dict = STREETS_VD
    else:
        return []
    
    if commune:
        # Recherche insensible à la casse
        for key in streets_dict:
            if key.lower() == commune.lower():
                return streets_dict[key]
        return []
    else:
        # Toutes les rues du canton
        all_streets = []
        for commune_streets in streets_dict.values():
            all_streets.extend(commune_streets)
        return list(set(all_streets))  # Dédoublonner


def get_communes(canton: str) -> List[str]:
    """
    Retourne la liste des communes disponibles pour un canton.
    
    Args:
        canton: Code canton (GE ou VD)
        
    Returns:
        Liste des noms de communes
    """
    canton_upper = canton.upper()
    
    if canton_upper == "GE":
        return list(STREETS_GE.keys())
    elif canton_upper == "VD":
        return list(STREETS_VD.keys())
    else:
        return []


def get_all_streets_ge() -> List[str]:
    """Retourne toutes les rues de Genève."""
    return get_streets("GE")


def get_all_streets_vd() -> List[str]:
    """Retourne toutes les rues de Vaud."""
    return get_streets("VD")


def get_street_count(canton: str, commune: Optional[str] = None) -> int:
    """Retourne le nombre de rues pour un canton/commune."""
    return len(get_streets(canton, commune))


# Statistiques
STATS = {
    "GE": {
        "communes": len(STREETS_GE),
        "streets": sum(len(s) for s in STREETS_GE.values()),
    },
    "VD": {
        "communes": len(STREETS_VD),
        "streets": sum(len(s) for s in STREETS_VD.values()),
    },
}


def get_stats() -> Dict:
    """Retourne les statistiques de la base de rues."""
    return STATS

