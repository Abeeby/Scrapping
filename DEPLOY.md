# Deploiement ProspectionPro sur Railway

## 1. Pre-requis

- Compte GitHub avec le code source
- Compte Railway.app (gratuit pour commencer)

## 2. Deploiement rapide

### Etape 1: Connecter GitHub a Railway

1. Allez sur [railway.app](https://railway.app)
2. Cliquez "Start a New Project"
3. Selectionnez "Deploy from GitHub repo"
4. Autorisez Railway a acceder a votre repo

### Etape 2: Ajouter PostgreSQL

1. Dans votre projet Railway, cliquez "+ New"
2. Selectionnez "Database" > "PostgreSQL"
3. Railway configure automatiquement la variable `DATABASE_URL`

### Etape 3: Configurer le deploiement

Le backend sera automatiquement detecte grace au `Procfile`.

Variables d'environnement a configurer (optionnel):
- `PORT` - Defini automatiquement par Railway

### Etape 4: Deployer

Railway deploie automatiquement a chaque push sur la branche main.

## 3. URL de l'application

Apres deploiement, Railway fournit une URL du type:
`https://prospectionpro-production.up.railway.app`

## 4. Variables d'environnement

| Variable | Description | Obligatoire |
|----------|-------------|-------------|
| DATABASE_URL | URL PostgreSQL (auto) | Oui |
| PORT | Port du serveur (auto) | Oui |

## 5. Structure des fichiers

```
backend/
  app/
    main.py           # Application FastAPI
    core/
      database.py     # Config DB (PostgreSQL/SQLite)
  static/             # Frontend React compile
  Procfile            # Commande de demarrage
  railway.json        # Config Railway
  requirements.txt    # Dependances Python
```





