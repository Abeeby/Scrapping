# =============================================================================
# DEMO DATA - Script pour insérer des données de démonstration
# =============================================================================

import asyncio
from datetime import datetime, timedelta
import random
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.database import Prospect, EmailAccount, Bot, Campaign, Proxy

# =============================================================================
# DONNÉES DE DÉMONSTRATION
# =============================================================================

# Prénoms suisses courants
PRENOMS = [
    "Jean", "Pierre", "Marie", "Anne", "Marc", "Sophie", "Claude", "Isabelle",
    "François", "Catherine", "Nicolas", "Christine", "Thomas", "Martine",
    "Daniel", "Brigitte", "Patrick", "Nathalie", "Michel", "Valérie",
    "Philippe", "Elisabeth", "Laurent", "Véronique", "Eric", "Sandrine",
    "Didier", "Corinne", "Bernard", "Caroline", "Alain", "Stéphanie"
]

# Noms suisses courants
NOMS = [
    "Müller", "Meier", "Schmid", "Keller", "Weber", "Huber", "Schneider",
    "Meyer", "Steiner", "Fischer", "Gerber", "Brunner", "Baumann", "Frei",
    "Zimmermann", "Moser", "Widmer", "Wyss", "Graf", "Roth", "Baumgartner",
    "Kaufmann", "Sutter", "Kunz", "Hoffmann", "Lehmann", "Hofer", "Arnold",
    "Bernhard", "Favre", "Dupont", "Martin", "Rochat", "Bonvin", "Cretton"
]

# Rues genevoises
RUES_GENEVE = [
    "Rue du Rhône", "Rue de Lausanne", "Boulevard Carl-Vogt", "Rue de Carouge",
    "Avenue de Champel", "Chemin de Miremont", "Rue de l'Arquebuse",
    "Place du Molard", "Rue du Mont-Blanc", "Rue de Rive", "Rue de la Servette",
    "Avenue Pictet-de-Rochemont", "Rue des Eaux-Vives", "Boulevard des Philosophes",
    "Rue de Florissant", "Avenue de la Praille", "Chemin des Tulipiers",
    "Rue de Chêne-Bougeries", "Avenue de Frontenex", "Chemin de la Gradelle"
]

# Villes genevoises
VILLES_GE = [
    "Genève", "Carouge", "Vernier", "Lancy", "Meyrin", "Thônex", "Onex",
    "Versoix", "Grand-Saconnex", "Chêne-Bougeries", "Cologny", "Plan-les-Ouates",
    "Collonge-Bellerive", "Veyrier", "Bernex", "Bellevue", "Pregny-Chambésy"
]

# Types de biens
TYPES_BIENS = ["Villa", "Appartement", "Terrain", "Immeuble", "Local commercial", "Maison"]

# =============================================================================
# GÉNÉRATEURS
# =============================================================================

def generate_phone():
    """Génère un numéro de téléphone suisse"""
    prefixes = ["022", "079", "078", "077", "076"]
    return f"+41 {random.choice(prefixes)} {random.randint(100, 999)} {random.randint(10, 99)} {random.randint(10, 99)}"

def generate_email(prenom: str, nom: str):
    """Génère une adresse email"""
    domains = ["gmail.com", "outlook.com", "bluewin.ch", "sunrise.ch", "hotmail.com"]
    formats = [
        f"{prenom.lower()}.{nom.lower()}@{random.choice(domains)}",
        f"{prenom[0].lower()}.{nom.lower()}@{random.choice(domains)}",
        f"{prenom.lower()}{random.randint(1, 99)}@{random.choice(domains)}"
    ]
    return random.choice(formats).replace("é", "e").replace("è", "e").replace("ë", "e").replace("ü", "u")

def generate_prospect():
    """Génère un prospect aléatoire"""
    prenom = random.choice(PRENOMS)
    nom = random.choice(NOMS)
    ville = random.choice(VILLES_GE)
    
    return {
        "id": str(uuid.uuid4()),
        "nom": nom,
        "prenom": prenom,
        "telephone": generate_phone(),
        "email": generate_email(prenom, nom),
        "adresse": f"{random.randint(1, 150)} {random.choice(RUES_GENEVE)}",
        "code_postal": str(1200 + random.randint(0, 99)),
        "ville": ville,
        "canton": "GE",
        "type_bien": random.choice(TYPES_BIENS),
        "surface": random.randint(50, 500),
        "prix": random.randint(300000, 5000000),
        "score": random.randint(20, 95),
        "statut": random.choice(["nouveau", "contacte", "interesse", "negociation", "gagne", "perdu"]),
        "source": random.choice(["SITG Genève", "Search.ch", "Local.ch", "Comparis.ch", "ImmoScout24"]),
        "notes": "",
        "created_at": datetime.now() - timedelta(days=random.randint(0, 30))
    }

# =============================================================================
# INSERTION EN BASE
# =============================================================================

async def insert_demo_data():
    """Insère les données de démonstration dans la base"""
    
    # Connexion à la base
    engine = create_async_engine("sqlite+aiosqlite:///./prospection.db")
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        # =====================================================================
        # PROSPECTS (30)
        # =====================================================================
        print("📊 Insertion des prospects de démonstration...")
        
        for _ in range(30):
            prospect_data = generate_prospect()
            prospect = Prospect(**prospect_data)
            session.add(prospect)
        
        # =====================================================================
        # EMAILS (10)
        # =====================================================================
        print("📧 Insertion des comptes email de démonstration...")
        
        demo_emails = [
            {"email": "prospection.immo.ge1@gmail.com", "password": "demo123"},
            {"email": "prospection.immo.ge2@gmail.com", "password": "demo123"},
            {"email": "contact.immo.suisse@gmail.com", "password": "demo123"},
            {"email": "prospection.ventes1@outlook.com", "password": "demo123"},
            {"email": "prospection.ventes2@outlook.com", "password": "demo123"},
            {"email": "agent.immo.geneve@bluewin.ch", "password": "demo123"},
            {"email": "vente.immobilier.ge@sunrise.ch", "password": "demo123"},
            {"email": "prospects.suisse@hotmail.com", "password": "demo123"},
            {"email": "recherche.immo.ch@gmail.com", "password": "demo123"},
            {"email": "contact.vendeurs@outlook.com", "password": "demo123"},
        ]
        
        for email_data in demo_emails:
            email = EmailAccount(
                id=str(uuid.uuid4()),
                email=email_data["email"],
                password=email_data["password"],
                imap_server="imap.gmail.com" if "gmail" in email_data["email"] else "outlook.office365.com",
                smtp_server="smtp.gmail.com" if "gmail" in email_data["email"] else "smtp.office365.com",
                quota_daily=50,
                sent_today=random.randint(0, 30),
                is_active=random.random() > 0.2,
                last_used=datetime.now() - timedelta(hours=random.randint(0, 48))
            )
            session.add(email)
        
        # =====================================================================
        # PROXIES (8)
        # =====================================================================
        print("🌐 Insertion des proxies de démonstration...")
        
        demo_proxies = [
            {"host": "ch-proxy1.privatevpn.com", "port": 8080, "country": "CH"},
            {"host": "ch-proxy2.privatevpn.com", "port": 8080, "country": "CH"},
            {"host": "swiss-1.nordvpn.com", "port": 1080, "country": "CH"},
            {"host": "swiss-2.nordvpn.com", "port": 1080, "country": "CH"},
            {"host": "geneva.expressvpn.com", "port": 443, "country": "CH"},
            {"host": "zurich.expressvpn.com", "port": 443, "country": "CH"},
            {"host": "eu-west.surfshark.com", "port": 8080, "country": "FR"},
            {"host": "de-berlin.surfshark.com", "port": 8080, "country": "DE"},
        ]
        
        for proxy_data in demo_proxies:
            proxy = Proxy(
                id=str(uuid.uuid4()),
                host=proxy_data["host"],
                port=proxy_data["port"],
                protocol="http",
                country=proxy_data["country"],
                is_active=True,
                is_valid=random.random() > 0.3,
                latency_ms=random.randint(50, 500),
                success_rate=random.randint(70, 99)
            )
            session.add(proxy)
        
        # =====================================================================
        # BOTS (5)
        # =====================================================================
        print("🤖 Insertion des bots de démonstration...")
        
        demo_bots = [
            {"name": "Bot Comparis GE", "type": "comparis", "target_city": "Genève"},
            {"name": "Bot ImmoScout GE", "type": "immoscout", "target_city": "Genève"},
            {"name": "Bot Comparis VD", "type": "comparis", "target_city": "Lausanne"},
            {"name": "Bot Homegate", "type": "homegate", "target_city": "Genève"},
            {"name": "Bot Newhome", "type": "newhome", "target_city": "Genève"},
        ]
        
        for bot_data in demo_bots:
            bot = Bot(
                id=str(uuid.uuid4()),
                name=bot_data["name"],
                type=bot_data["type"],
                status=random.choice(["idle", "running", "paused"]),
                requests_count=random.randint(50, 500),
                success_count=random.randint(40, 450),
                error_count=random.randint(5, 50),
                last_run=datetime.now() - timedelta(hours=random.randint(0, 24)),
                config={
                    "target_city": bot_data["target_city"],
                    "target_radius": 20,
                    "max_requests": 100,
                    "delay_min": 3000,
                    "delay_max": 8000
                }
            )
            session.add(bot)
        
        # =====================================================================
        # CAMPAIGNS (3)
        # =====================================================================
        print("📢 Insertion des campagnes de démonstration...")
        
        demo_campaigns = [
            {
                "name": "Campagne Genève Décembre",
                "type": "brochure",
                "target_portal": "comparis",
                "target_city": "Genève",
                "target_radius": 15
            },
            {
                "name": "Campagne Vaud Nord",
                "type": "brochure",
                "target_portal": "immoscout",
                "target_city": "Nyon",
                "target_radius": 20
            },
            {
                "name": "Campagne Cologny Premium",
                "type": "brochure",
                "target_portal": "homegate",
                "target_city": "Cologny",
                "target_radius": 5
            },
        ]
        
        for camp_data in demo_campaigns:
            campaign = Campaign(
                id=str(uuid.uuid4()),
                name=camp_data["name"],
                type=camp_data["type"],
                target_portal=camp_data["target_portal"],
                target_city=camp_data["target_city"],
                target_radius=camp_data["target_radius"],
                status=random.choice(["pending", "active", "paused", "completed"]),
                total_targets=random.randint(50, 200),
                sent_count=random.randint(20, 150),
                response_count=random.randint(5, 30),
                created_at=datetime.now() - timedelta(days=random.randint(1, 14))
            )
            session.add(campaign)
        
        # Commit
        await session.commit()
        print("✅ Données de démonstration insérées avec succès!")
        print(f"   - 30 prospects")
        print(f"   - 10 comptes email")
        print(f"   - 8 proxies")
        print(f"   - 5 bots")
        print(f"   - 3 campagnes")

# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

if __name__ == "__main__":
    asyncio.run(insert_demo_data())



