# 🔍 Enrichisseur Dirigeants

Application web pour trouver les emails des dirigeants de sociétés françaises via **Pharow → Fullenrich → Claude AI** en cascade.

---

## 🚀 Déploiement sur Render (5 minutes)

### 1. Préparer le repo GitHub
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/TON_USER/enrichisseur-dirigeants.git
git push -u origin main
```

### 2. Créer le service sur Render
1. Aller sur [render.com](https://render.com) → **New → Web Service**
2. Connecter votre repo GitHub
3. Render détecte automatiquement le `render.yaml`
4. Cliquer **Create Web Service**

### 3. Configurer les variables d'environnement
Dans le dashboard Render → **Environment** :

| Variable | Valeur |
|---|---|
| `ANTHROPIC_API_KEY` | `sk-ant-api03-...` |
| `PHAROW_API_KEY` | `ph_...` |
| `FULLENRICH_API_KEY` | `fe_...` |

### 4. C'est en ligne ! 🎉
URL : `https://enrichisseur-dirigeants.onrender.com`

---

## 📋 Format CSV accepté

Colonnes (noms flexibles, ordre libre) :

```csv
nom;siren;domaine;prenom_dg;nom_dg
Acme SAS;123456789;acme.fr;Jean;Dupont
Société XYZ;987654321;xyz.com;;
```

Colonnes reconnues automatiquement :
- **nom** → nom, name, société, company, entreprise
- **siren** → siren, siret
- **domaine** → domaine, domain, website, site, url
- **prenom_dg** → prenom, prénom, firstname
- **nom_dg** → nom_dg, lastname, dirigeant, dg

---

## ⚡ Logique d'enrichissement (cascade)

```
1. Pharow (si SIREN dispo)     → dirigeant + email
        ↓ si pas d'email
2. Fullenrich (si nom + domaine) → email + téléphone
        ↓ si toujours rien
3. Claude AI (web search)       → fallback intelligent
```

---

## 🛠️ Lancer en local

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY="sk-ant-..."
export PHAROW_API_KEY="ph_..."
export FULLENRICH_API_KEY="fe_..."
uvicorn main:app --reload
```
Ouvrir http://localhost:8000

---

## 📁 Structure du projet

```
enrichisseur/
├── main.py           # Backend FastAPI
├── templates/
│   └── index.html    # Interface web
├── requirements.txt
├── render.yaml       # Config déploiement Render
└── README.md
```
