# =============================================================================
# SCRAPER SEARCH.CH - Extraction reelle via API XML
# =============================================================================

import asyncio
import random
import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from dataclasses import dataclass

import aiohttp

# =============================================================================
# USER AGENTS
# =============================================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Namespace Atom
NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'tel': 'http://tel.search.ch/api/spec/result/1.0/',
    'openSearch': 'http://a9.com/-/spec/opensearchrss/1.0/'
}

# =============================================================================
# SCRAPER CLASS
# =============================================================================

class SearchChScraper:
    """Scraper pour Search.ch via API XML"""
    
    def __init__(self):
        pass
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
                
    async def search(
        self,
        query: str,
        ville: str = "",
        limit: int = 50,
        type_recherche: str = "person"
    ) -> List[Dict]:
        """
        Recherche sur Search.ch via l'API XML
        """
        search_mode = (type_recherche or "person").lower()
        #region agent log
        import json; open(r"c:\Users\admin10\Desktop\Scrapping data\.cursor\debug.log", "a").write(json.dumps({"hypothesisId":"H3","location":"searchch.py:search","message":"Scraper demarre","data":{"query":query,"ville":ville,"type_recherche":search_mode},"timestamp":__import__("time").time()*1000,"sessionId":"debug-session"})+"\n")
        #endregion
        print(f"[Search.ch] Recherche: '{query}' a '{ville}' (limite: {limit}, mode: {search_mode})")
        
        results = await self._api_search(query, ville, limit, search_mode)
        
        print(f"[Search.ch] {len(results)} resultats trouves")
        return results
    
    async def _api_search(self, query: str, ville: str, limit: int, type_recherche: str) -> List[Dict]:
        """Recherche via l'API tel.search.ch (format XML Atom)"""
        results = []
        
        try:
            # Construire les parametres de recherche
            search_term = query
            
            # URL de base
            url = f"https://tel.search.ch/api/"
            params = {
                'was': search_term,
                'maxnum': min(limit, 50)  # Max 50 par requete
            }
            if ville:
                params['wo'] = ville
            
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'application/xml, text/xml, */*',
                'Accept-Language': 'fr-CH,fr;q=0.9,de;q=0.8',
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        text = await response.text()
                        results = self._parse_atom_feed(text, ville, type_recherche)
                        print(f"[Search.ch API] {len(results)} entrees parsees")
                    else:
                        print(f"[Search.ch API] Status {response.status}")
                        
        except asyncio.TimeoutError:
            print("[Search.ch API] Timeout")
        except Exception as e:
            print(f"[Search.ch API] Erreur: {e}")
            
        return results
    
    def _parse_atom_feed(self, xml_text: str, default_ville: str, type_recherche: str) -> List[Dict]:
        """Parse le flux Atom retourne par l'API Search.ch"""
        results = []
        
        try:
            root = ET.fromstring(xml_text)
            
            # Trouver toutes les entrees <entry>
            entries = root.findall('atom:entry', NS)
            if not entries:
                entries = root.findall('{http://www.w3.org/2005/Atom}entry')
            
            print(f"[Search.ch] {len(entries)} entrees XML trouvees")
            #region agent log
            filtered_count = 0
            accepted_count = 0
            #endregion
            for entry in entries:
                result = self._extract_from_entry(entry, default_ville, type_recherche)
                if result and result.get('nom'):
                    results.append(result)
                    #region agent log
                    accepted_count += 1
                    #endregion
                else:
                    #region agent log
                    filtered_count += 1
                    #endregion
            #region agent log
            import json; open(r"c:\Users\admin10\Desktop\Scrapping data\.cursor\debug.log", "a").write(json.dumps({"hypothesisId":"H3","location":"searchch.py:parse","message":"Resultats filtres","data":{"total_entries":len(entries),"accepted":accepted_count,"filtered":filtered_count,"type_recherche":type_recherche},"timestamp":__import__("time").time()*1000,"sessionId":"debug-session"})+"\n")
            #endregion
                    
        except ET.ParseError as e:
            print(f"[Search.ch] Erreur parsing XML: {e}")
        except Exception as e:
            print(f"[Search.ch] Erreur traitement: {e}")
            
        return results
    
    def _extract_from_entry(self, entry, default_ville: str, type_recherche: str) -> Optional[Dict]:
        """Extrait les donnees d'une entree Atom"""
        result = {
            'nom': '',
            'prenom': '',
            'adresse': '',
            'code_postal': '',
            'ville': default_ville,
            'telephone': '',
            'email': '',
            'source': 'Search.ch',
            'lien_rf': ''  # Lien vers la fiche originale
        }
        
        try:
            # Helper pour trouver un element avec namespace
            def find_text(tag):
                el = entry.find(f'atom:{tag}', NS)
                if el is None:
                    el = entry.find(f'{{{NS["atom"]}}}{tag}')
                return el.text.strip() if el is not None and el.text else ""
            
            # Titre = Nom complet
            title = find_text('title')
            if title and title.lower() != 'title':
                result['nom'] = title
                
            # Content = Details (adresse, etc.)
            content = find_text('content')
            if content and content.lower() != 'content':
                lines = [l.strip() for l in content.split('\n') if l.strip()]
                
                for i, line in enumerate(lines):
                    # Ignorer la premiere ligne si c'est le nom (deja capture)
                    if i == 0 and line == title:
                        continue
                        
                    # Detecter code postal + ville (format suisse: 1200 Geneve)
                    npa_match = re.match(r'^(\d{4})\s*(.*)$', line)
                    if npa_match:
                        result['code_postal'] = npa_match.group(1)
                        ville_text = npa_match.group(2).strip()
                        if ville_text:
                            result['ville'] = ville_text
                        continue
                    
                    # Detecter telephone (commence par 0 ou +)
                    if re.match(r'^[\+0][\d\s]+$', line.replace(' ', '')):
                        if not result['telephone']:
                            result['telephone'] = line.replace(' ', '')
                        continue
                    
                    # Sinon c'est probablement l'adresse ou une description
                    if not result['adresse'] and line != title:
                        # Eviter les descriptions (contiennent souvent des mots cles)
                        if not any(kw in line.lower() for kw in ['études', 'réalisations', 'services', 'installation']):
                            result['adresse'] = line
                        
            # Liens - chercher le telephone et le lien source
            for link in entry.findall('atom:link', NS) or entry.findall(f'{{{NS["atom"]}}}link'):
                href = link.get('href', '')
                rel = link.get('rel', '')
                link_type = link.get('type', '')
                
                if 'tel:' in href:
                    result['telephone'] = href.replace('tel:', '').strip()
                elif 'mailto:' in href:
                    result['email'] = href.replace('mailto:', '').strip()
                elif rel == 'alternate' and 'text/html' in link_type:
                    # Lien vers la fiche originale sur search.ch
                    result['lien_rf'] = href
            
            # FILTRE STRICT POUR PRIVES UNIQUEMENT
            if type_recherche == "person":
                # Liste exhaustive des mots-cles d'entreprises
                keywords_to_exclude = [
                    # Formes juridiques
                    ' SA', ' S.A.', ' AG', ' Ltd', ' LLC', ' Inc', ' Corp',
                    ' Sàrl', ' Sarl', ' GmbH', ' Sagl', ' SNC', ' SCS',
                    ' & Co', ' & Cie', ' et Fils', ' et Filles',
                    # Commerces et restauration
                    'Restaurant', 'Café', 'Bistrot', 'Bar', 'Pub', 'Brasserie',
                    'Hotel', 'Hôtel', 'Auberge', 'Pension', 'Motel', 'Hostel',
                    'Pizza', 'Pizzeria', 'Burger', 'Kebab', 'Sushi', 'Tacos',
                    'Boulangerie', 'Patisserie', 'Confiserie', 'Epicerie',
                    'Supermarché', 'Magasin', 'Boutique', 'Store', 'Shop',
                    # Services professionnels
                    'Cabinet', 'Etude', 'Bureau', 'Agence', 'Atelier', 'Studio',
                    'Fiduciaire', 'Comptable', 'Avocat', 'Notaire', 'Huissier',
                    'Architecte', 'Ingénieur', 'Consultant', 'Conseiller',
                    # Sante
                    'Clinique', 'Centre', 'Médical', 'Dentaire', 'Optique',
                    'Pharmacie', 'Droguerie', 'Institut', 'Praxis', 'Therapie',
                    'Physiothérapie', 'Chiropracteur', 'Ostéopathe',
                    # Beaute et bien-etre
                    'Coiffure', 'Coiffeur', 'Salon', 'Spa', 'Massage', 'Esthétique',
                    'Onglerie', 'Barbier', 'Beauté',
                    # Commerce et artisanat
                    'Garage', 'Carrosserie', 'Mécanique', 'Auto', 'Moto',
                    'Menuiserie', 'Plomberie', 'Electricité', 'Chauffage',
                    'Peinture', 'Rénovation', 'Construction', 'Bâtiment',
                    # Education et associations
                    'Ecole', 'School', 'Academy', 'Cours', 'Formation',
                    'Association', 'Fondation', 'Stiftung', 'Genossenschaft',
                    'Club', 'Verein', 'Société', 'Groupe', 'Holding',
                    # Finance et immobilier
                    'Banque', 'Bank', 'Assurance', 'Insurance', 'Courtier',
                    'Immobilier', 'Régie', 'Gérance', 'Property', 'Estate',
                    # IT et media
                    'Informatique', 'Software', 'Digital', 'Tech', 'Web',
                    'Media', 'Communication', 'Marketing', 'Publicité',
                    # Autres
                    'Kiosk', 'Pressing', 'Laverie', 'Nettoyage', 'Cleaning',
                    'Transport', 'Taxi', 'Livraison', 'Déménagement',
                    'Pompes funèbres', 'Funéraire', 'Fleuriste', 'Jardinerie',
                    'Service', 'Services', 'Solutions', 'Entreprise', 'Company'
                ]
                
                full_text = (result['nom'] + ' ' + result.get('adresse', '')).lower()
                nom_original = result['nom']
                
                # Exclure si contient un mot-cle d'entreprise
                for kw in keywords_to_exclude:
                    if kw.lower() in full_text:
                        return None
                
                # Verification supplementaire : le nom doit ressembler a un nom de personne
                # Un nom de personne privee a generalement 2-3 mots (prenom + nom)
                # et ne contient pas de chiffres ni de caracteres speciaux
                name_parts = nom_original.split()
                
                # Trop de mots = probablement une entreprise
                if len(name_parts) > 4:
                    return None
                    
                # Contient des chiffres = probablement une entreprise
                if any(char.isdigit() for char in nom_original):
                    return None
                    
                # Tout en majuscules = probablement une entreprise
                if nom_original.isupper() and len(nom_original) > 10:
                    return None
                    
                # Contient des caracteres speciaux suspects
                special_chars = ['@', '#', '$', '&', '*', '+', '=', '|', '<', '>', '{', '}', '[', ']']
                if any(char in nom_original for char in special_chars):
                    return None
                
                # Nom trop court (moins de 3 caracteres) = suspect
                if len(nom_original.replace(' ', '')) < 3:
                    return None
                    
        except Exception as e:
            print(f"[Search.ch] Erreur extraction entree: {e}")
            
        return result if result['nom'] else None


# =============================================================================
# FONCTION UTILITAIRE
# =============================================================================

async def scrape_searchch(
    query: str,
    ville: str = "",
    limit: int = 50,
    type_recherche: str = "person"
) -> List[Dict]:
    """
    Fonction utilitaire pour scraper Search.ch
    
    Usage:
        results = await scrape_searchch("Muller", "Geneve", 50, "person")
    """
    async with SearchChScraper() as scraper:
        return await scraper.search(query, ville, limit, type_recherche=type_recherche)
