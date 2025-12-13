# =============================================================================
# SCRAPER LOCAL.CH - Extraction reelle avec Playwright + API fallback
# =============================================================================

import asyncio
import random
import re
from typing import List, Dict, Optional
from dataclasses import dataclass

try:
    from playwright.async_api import async_playwright, Page, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("[Local.ch] Playwright non disponible, utilisation de l'API uniquement")

import aiohttp

# =============================================================================
# USER AGENTS
# =============================================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# =============================================================================
# SCRAPER CLASS
# =============================================================================

class LocalChScraper:
    """Scraper pour Local.ch avec Playwright"""
    
    def __init__(self):
        self.browser = None
        self.page = None
        self._playwright = None
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def start(self):
        """Demarre le navigateur si disponible"""
        if not PLAYWRIGHT_AVAILABLE:
            return
            
        try:
            self._playwright = await async_playwright().start()
            self.browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                ]
            )
            
            context = await self.browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={'width': 1920, 'height': 1080},
                locale='fr-CH',
            )
            
            self.page = await context.new_page()
            self.page.set_default_timeout(15000)
        except Exception as e:
            print(f"[Local.ch] Erreur demarrage Playwright: {e}")
            self.browser = None
            self.page = None
            
    async def close(self):
        """Ferme le navigateur"""
        if self.browser:
            try:
                await self.browser.close()
            except:
                pass
        if self._playwright:
            try:
                await self._playwright.stop()
            except:
                pass
                
    async def search(
        self,
        query: str,
        ville: str = "",
        limit: int = 50,
        type_recherche: str = "person"
    ) -> List[Dict]:
        """
        Recherche sur Local.ch
        """
        search_mode = (type_recherche or "person").lower()
        print(f"[Local.ch] Recherche: '{query}' a '{ville}' (limite: {limit}, mode: {search_mode})")
        
        results = []
        
        # Local.ch n'a pas d'API publique, on utilise le scraping HTML
        if PLAYWRIGHT_AVAILABLE:
            results = await self._scrape_html(query, ville, limit, search_mode)
            
        # Fallback: utiliser l'API search.ch (meme base de donnees)
        if not results:
            print("[Local.ch] Utilisation de Search.ch comme fallback...")
            from app.scrapers.searchch import SearchChScraper
            async with SearchChScraper() as scraper:
                results = await scraper.search(query, ville, limit, type_recherche=search_mode)
                # Changer la source
                for r in results:
                    r['source'] = 'Local.ch (via Search.ch)'
                    
        print(f"[Local.ch] {len(results)} resultats trouves")
        return results
    
    async def _scrape_html(
        self,
        query: str,
        ville: str,
        limit: int,
        type_recherche: str
    ) -> List[Dict]:
        """Scrape la page HTML de Local.ch"""
        results = []
        
        if not self.page:
            await self.start()
            
        if not self.page:
            return results
            
        try:
            # Construire l'URL
            search_term = query.replace(' ', '-')
            ville_slug = ville.replace(' ', '-').lower() if ville else ''
            
            # Format URL Local.ch
            if ville_slug:
                url = f"https://www.local.ch/fr/q/{ville_slug}/{search_term}.html"
            else:
                url = f"https://www.local.ch/fr/q/{search_term}.html"
            
            print(f"[Local.ch] Navigation vers {url}")
            
            await self.page.goto(url, wait_until='domcontentloaded', timeout=15000)
            await asyncio.sleep(2)
            
            # Accepter les cookies si necessaire
            try:
                cookie_btn = await self.page.query_selector('button[id*="accept"], .cc-accept, [data-testid="accept-all"]')
                if cookie_btn:
                    await cookie_btn.click()
                    await asyncio.sleep(1)
            except:
                pass
            
            # Selectors pour les resultats Local.ch
            selectors = [
                '[data-testid="result-item"]',
                '.ListElement',
                '.result-item',
                'article[itemtype*="LocalBusiness"]',
                '.entry',
            ]
            
            entries = []
            for selector in selectors:
                entries = await self.page.query_selector_all(selector)
                if entries:
                    print(f"[Local.ch] {len(entries)} entrees trouvees avec '{selector}'")
                    break
            
            for i, entry in enumerate(entries[:limit]):
                try:
                    result = await self._extract_html_entry(entry, type_recherche)
                    if result and result.get('nom'):
                        result['ville'] = result.get('ville') or ville
                        results.append(result)
                except Exception as e:
                    print(f"[Local.ch] Erreur extraction: {e}")
                    continue
                    
        except Exception as e:
            print(f"[Local.ch HTML] Erreur: {e}")
            
        return results
    
    async def _extract_html_entry(self, entry, type_recherche: str) -> Optional[Dict]:
        """Extrait les donnees d'un element HTML"""
        result = {
            'nom': '',
            'prenom': '',
            'adresse': '',
            'code_postal': '',
            'ville': '',
            'telephone': '',
            'email': '',
            'source': 'Local.ch'
        }
        
        try:
            # Nom / Titre
            name_selectors = ['h2', 'h3', '.name', '.title', '[data-testid="entry-title"]', '[itemprop="name"]']
            for sel in name_selectors:
                name_el = await entry.query_selector(sel)
                if name_el:
                    text = await name_el.inner_text()
                    if text and text.strip():
                        parts = text.strip().split(' ', 1)
                        result['nom'] = parts[0]
                        result['prenom'] = parts[1] if len(parts) > 1 else ''
                        break
                        
            # Adresse
            addr_selectors = ['.address', '[itemprop="streetAddress"]', '.street', '[data-testid="entry-address"]']
            for sel in addr_selectors:
                addr_el = await entry.query_selector(sel)
                if addr_el:
                    result['adresse'] = (await addr_el.inner_text()).strip()
                    break
                    
            # Code postal et ville
            locality_selectors = ['.locality', '[itemprop="addressLocality"]', '.city']
            for sel in locality_selectors:
                loc_el = await entry.query_selector(sel)
                if loc_el:
                    loc_text = (await loc_el.inner_text()).strip()
                    # Format: "1200 Geneve" ou "Geneve"
                    match = re.match(r'(\d{4})?\s*(.+)', loc_text)
                    if match:
                        if match.group(1):
                            result['code_postal'] = match.group(1)
                        result['ville'] = match.group(2).strip()
                    break
                    
            # Telephone
            tel_selectors = ['.phone', '[itemprop="telephone"]', 'a[href^="tel:"]', '[data-testid="entry-phone"]']
            for sel in tel_selectors:
                tel_el = await entry.query_selector(sel)
                if tel_el:
                    href = await tel_el.get_attribute('href')
                    if href and href.startswith('tel:'):
                        result['telephone'] = href.replace('tel:', '').strip()
                    else:
                        result['telephone'] = (await tel_el.inner_text()).strip()
                    break
                    
            # Email
            email_el = await entry.query_selector('[itemprop="email"], a[href^="mailto:"]')
            if email_el:
                href = await email_el.get_attribute('href')
                if href and href.startswith('mailto:'):
                    result['email'] = href.replace('mailto:', '').strip()
            
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
            print(f"[Local.ch] Erreur extraction element: {e}")
            
        return result if result['nom'] else None


# =============================================================================
# FONCTION UTILITAIRE
# =============================================================================

async def scrape_localch(
    query: str,
    ville: str = "",
    limit: int = 50,
    type_recherche: str = "person"
) -> List[Dict]:
    """
    Fonction utilitaire pour scraper Local.ch
    
    Usage:
        results = await scrape_localch("restaurant", "Geneve", 50, "business")
    """
    async with LocalChScraper() as scraper:
        return await scraper.search(query, ville, limit, type_recherche=type_recherche)
