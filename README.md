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

---

## ✅ Validation des résultats (v3) — deux écrans

Les deux écrans lisent la base Notion « Enrichisseur v3 — Mémoire des sociétés ». **Rien n'est envoyé dans un CRM** depuis ces écrans :
l'envoi HubSpot viendra après la migration de fin octobre, avec un test à blanc sur 50 lignes avant tout envoi réel.

**`/v3/validation`, réservé à Marie et Michel Bastian (mot de passe admin)**
1. **Société** : adresses CRM / site / LinkedIn / Google, recommandation de l'outil, effectif LinkedIn, domiciliation.
   On coche l'adresse retenue (ou on la saisit), puis « Valider la société » ou « Écarter ».
2. **Contacts CRM** (une fois la société validée) : contacts Pipedrive « probablement parti » / « à vérifier » / « toujours là »,
   décision garder / marquer parti / à revoir. Les nouveaux contacts y sont visibles en lecture seule, avec l'avancement des négos.

**Saisie manuelle** : sur les nouveaux contacts (négos et admin) et sur les contacts CRM (admin), LinkedIn, fonction, rôle, e-mail
et téléphone sont modifiables. Les cases corrigées à la main sont surlignées et remplacent la valeur de l'enrichisseur ou de Pipedrive
(gardées dans Notion : `modifie`, `final_*`, `modifie_par`).

**`/v3/contacts`, pour les négos (mot de passe négos ; le mot de passe admin y donne aussi accès)**
- Nouveaux dirigeants, visibles dès la fin du lancement : profil LinkedIn affiché, téléphone à compléter,
  « Valider pour HubSpot » ou « Écarter ». Liste commune à tous les négos.
- Contacts déjà au CRM : lecture seule.
- Les sociétés écartées par l'admin disparaissent de cet écran.

Contrôle des contacts CRM (étape 4b du pipeline, choix « Sociétés + dirigeants » et « Tout ») :
- absent des dirigeants Sales Navigator → « à vérifier »
- profil LinkedIn lu par le phantom **LinkedIn Profile Scraper** : autre employeur → « probablement parti »

| Variable Render | Rôle |
|---|---|
| `VALIDATION_MDP_ADMIN` | mot de passe de l'écran admin (Marie + Michel) |
| `VALIDATION_MDP_NEGOS` | mot de passe de l'écran des négos |
| `VALIDATION_SECRET` | facultatif : clé de signature de la connexion (changer = déconnecter tout le monde) |
| `PB_PROFILE_SCRAPER_ID` | ID du phantom LinkedIn Profile Scraper (sans lui, les contacts restent « à vérifier ») |
| `PROFILS_MAX` | profils lus au plus par lancement (80 par défaut, quota LinkedIn) |

**Contacts présents dans plusieurs sociétés** (HubSpot associe un contact à plusieurs sociétés) :
- nouveau dirigeant déjà fiché dans Pipedrive chez une autre société (même nom, et même profil LinkedIn quand les deux sont connus) :
  le négo décide de l'ajouter (« Ajouter à … ») ou de l'écarter ; l'admin décide ensuite « changer de société » ou « deux sociétés »
  (décisions de type `ailleurs` dans Décisions contacts). Aucun doublon créé.
- même dirigeant trouvé pour plusieurs sociétés d'un lancement → « Aussi dirigeant de … » ; à l'envoi : un seul contact, associé à chacune
- contrôle du profil : 2ᵉ poste chez nous encore en cours → « toujours là » ; société absente des 2 derniers postes → « à vérifier » (on ne conclut pas)

**À prévoir pour l'envoi HubSpot (après la migration)** : créer dans HubSpot deux propriétés de type date,
`date_validation_societe` (Sociétés, alimentée par « Validée le ») et `date_validation_contact`
(Contacts, alimentée par la date de validation du négo), ainsi que le téléphone saisi par les négos.

**Rattrapage du contrôle des contacts CRM** (encadré retiré de l'écran admin le 16/10 ; routes et code conservés) : pour les sociétés déjà en mémoire
traitées avant ce contrôle. Rapproche les contacts Pipedrive des dirigeants déjà trouvés, puis lit les profils LinkedIn
(si `PB_PROFILE_SCRAPER_ID` est réglé, `PROFILS_MAX` profils au plus par passage). Ni Kaspr, ni Fullenrich, ni Sales Navigator.
Essai à blanc obligatoire sur 50 sociétés (rien n'est écrit, Excel du résultat) avant le passage réel.
Relancer un passage plus tard reprend les contacts « à vérifier » dont le profil n'a pas encore été lu.

**Import d'un Excel de lancement** (encadré retiré le 16/10 ; route `/v3/validation/api/import` conservée) : pour un lancement fait avant la mémoire des sociétés.
L'Excel de l'outil (onglets Résultats et Dirigeants) est retraduit en fiches mémoire, sans relancer de recherche ;
les sociétés déjà en mémoire (même SIREN, domaine ou nom) sont laissées telles quelles. Aperçu obligatoire avant l'import.
Lancer ensuite le rattrapage pour le contrôle des contacts CRM.

**Export HubSpot** (écran admin, en haut) : en attendant la migration, « Envoyer l'export par mail » (à `COPIE_FICHIER_COMPLET`)
ou « Télécharger l'Excel ». Fichier cumulatif de tout ce qui est validé : Sociétés (adresse retenue découpée, effectif, date
de validation société), Contacts à créer (validés par les négos, saisies manuelles comprises, date de validation contact),
Contacts existants (fichés ailleurs, décision admin), Contacts CRM (décisions admin), Sociétés écartées.
Clés d'import HubSpot : domaine pour les sociétés, e-mail pour les contacts (réimporter = mise à jour, pas de doublon).
