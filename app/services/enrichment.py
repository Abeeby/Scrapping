# =============================================================================
# ENRICHMENT SERVICE - Enrichissement automatique des prospects
# =============================================================================

import asyncio
from sqlalchemy.future import select
from app.core.database import AsyncSessionLocal, Prospect
from app.core.logger import logger
from app.core.websocket import emit_activity
from app.scrapers.searchch import SearchChScraper

async def enrich_prospect_task(prospect_id: str):
    """
    Tâche de fond pour enrichir un prospect après saisie manuelle.
    Cherche le téléphone sur Search.ch.
    """
    logger.info(f"🔄 Début enrichissement auto pour prospect {prospect_id}")
    
    async with AsyncSessionLocal() as db:
        try:
            # Récupérer le prospect
            result = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
            prospect = result.scalar_one_or_none()
            
            if not prospect:
                logger.error(f"Prospect {prospect_id} introuvable pour enrichissement")
                return

            # Préparer la recherche
            query = f"{prospect.prenom} {prospect.nom}".strip()
            ville = prospect.ville
            
            await emit_activity("enrichment", f"Recherche auto pour {query} à {ville}...")
            
            # Lancer le scraper
            async with SearchChScraper() as scraper:
                results = await scraper.search_person(prospect.nom, prospect.prenom, ville)
                
                if results:
                    best_match = results[0] # Prendre le premier résultat
                    
                    # Mettre à jour si téléphone trouvé
                    if best_match.get('telephone'):
                        prospect.telephone = best_match['telephone']
                        logger.info(f"✅ Téléphone trouvé pour {query}: {prospect.telephone}")
                        await emit_activity("success", f"Téléphone trouvé pour {query} !")
                    
                    # Mettre à jour si email trouvé
                    if best_match.get('email'):
                        prospect.email = best_match['email']
                    
                    # Mettre à jour l'adresse si plus précise
                    if best_match.get('adresse') and len(best_match['adresse']) > len(prospect.adresse or ""):
                        prospect.adresse = best_match['adresse']
                        
                    await db.commit()
                else:
                    logger.info(f"⚠️ Aucun résultat pour {query}")
                    await emit_activity("info", f"Aucun numéro trouvé pour {query}")
                    
        except Exception as e:
            logger.error(f"❌ Erreur enrichissement {prospect_id}: {e}", exc_info=True)
            await emit_activity("error", f"Erreur enrichissement : {str(e)}")

