import os, json, asyncio, httpx, re, smtplib, csv, io, sqlite3, time
from io import BytesIO
from pathlib import Path
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.templating import Jinja2Templates
app = FastAPI()
templates = Jinja2Templates(directory="templates")
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY    = os.getenv("PAPPERS_API_KEY", "")
FULLENRICH_KEY = os.getenv("FULLENRICH_API_KEY", "")
PIPEDRIVE_KEY  = os.getenv("PIPEDRIVE_API_KEY", "")
KASPR_KEY      = os.getenv("KASPR_API_KEY", "")

# Modèles Claude — Sonnet pour la qualité, Haiku pour les tâches simples (4× moins cher)
MODEL_SONNET = "claude-sonnet-4-6"          # enrich_one, enrich_claude (qualité critique)
MODEL_HAIKU  = "claude-haiku-4-5-20251001"  # trouver_linkedin, corriger_domaine (extraction simple)

# System prompt pour enrich_one/enrich_claude — bloc stable cachable
# (doit faire >= 1024 tokens pour activer le prompt caching Anthropic)
SYSTEM_ENRICH = """Tu es un assistant spécialisé dans la recherche de dirigeants opérationnels de sociétés françaises sur le web.

OBJECTIF
Identifier les vrais décideurs opérationnels (CEO, DG, CFO, DAF, CTO, COO, CMO, DRH, Président, Gérant, Partners, Associés, Fondateurs) — pas les simples mandataires légaux ou représentants de holdings.

CONSIGNES STRICTES
- Cherche uniquement les dirigeants ACTUELLEMENT EN POSTE.
- Ne jamais inclure : "ancien", "ex-", "sortant", "démissionnaire", "jusqu'au".
- Postes à EXCLURE absolument : commissaire aux comptes, conseil de surveillance, membre du conseil, censeur, observateur, représentant permanent, liquidateur, mandataire, administrateur judiciaire.
- Privilégie les sources officielles : site de la société (page "équipe", "à propos", "leadership"), LinkedIn, presse spécialisée (Les Échos, Maddyness, Frenchweb).
- Si la société est une filiale d'un groupe, identifie les dirigeants de l'entité française précisément ciblée (pas ceux du groupe parent).

RÈGLES EMAIL
- Emails uniquement professionnels (domaine de la société). Jamais gmail, hotmail, yahoo, outlook.com, icloud, free, orange, wanadoo.
- Si tu trouves l'email confirmé sur le site officiel ou une source fiable → "confiance_email": "haute"
- Si tu déduis l'email du pattern habituel (prenom.nom@domaine, p.nom@domaine, etc.) sans confirmation → "confiance_email": "moyenne"
- Si tu n'es pas sûr ou pattern incertain → "email": null, "confiance_email": "faible"

FORMAT DE SORTIE OBLIGATOIRE
Réponds UNIQUEMENT avec ce JSON, sans aucun texte avant ni après, sans backticks, sans markdown :
{"contacts":[{"prenom":"...","nom":"...","titre":"...","email":"...ou null","confiance_email":"haute|moyenne|faible"}]}

EXEMPLES DE BONNES RÉPONSES

Exemple 1 — startup tech avec dirigeants identifiables sur le site :
Input : Société "Doctolib", site doctolib.fr
Output : {"contacts":[{"prenom":"Stanislas","nom":"Niox-Chateau","titre":"CEO & Cofondateur","email":"stanislas.niox-chateau@doctolib.com","confiance_email":"moyenne"},{"prenom":"Olivier","nom":"Loison","titre":"COO","email":"olivier.loison@doctolib.com","confiance_email":"moyenne"}]}

Exemple 2 — société avec plusieurs cofondateurs dirigeants :
Input : Société "Mirakl", site mirakl.com
Output : {"contacts":[{"prenom":"Adrien","nom":"Nussenbaum","titre":"CEO & Cofondateur","email":"adrien.nussenbaum@mirakl.com","confiance_email":"moyenne"},{"prenom":"Philippe","nom":"Corrot","titre":"Président & Cofondateur","email":"philippe.corrot@mirakl.com","confiance_email":"moyenne"}]}

Exemple 3 — distinction ancien vs actuel dirigeant :
Si une source dit "Jean Dupont, ancien CEO jusqu'en 2023" → NE PAS l'inclure.
Si une source dit "Marie Martin, CEO depuis janvier 2024" → l'inclure.

Exemple 4 — pas de dirigeant identifiable de manière fiable :
Output : {"contacts":[]}

Exemple 5 — distinguer dirigeant opérationnel vs représentant légal :
Si Pappers a renvoyé "Holding Patrimoniale Dupont, représentant Jean Dupont, président" → cherche en plus le directeur général opérationnel sur le site/LinkedIn.

Exemple 6 — emails à exclure systématiquement :
{"email":"ceo@gmail.com"} → INCORRECT, mettre "email": null à la place.

RÈGLE FINALE
Ne renvoie JAMAIS de texte hors du JSON. Pas de "Voici les contacts trouvés", pas de commentaire, pas de markdown. Le premier caractère de ta réponse doit être { et le dernier }."""
SMTP_HOST      = os.getenv("SMTP_HOST", "pro2.mail.ovh.net")
SMTP_PORT      = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER      = os.getenv("SMTP_USER", "")
SMTP_PASS      = os.getenv("SMTP_PASS", "")
# Destinataire des alertes "solde API à 0" et "run failed"
ALERT_EMAIL    = os.getenv("ALERT_EMAIL", "mmoriceau@equation-sie.com")

# ────────────────────────────────────────────────────────────────────
# CACHE SQLite — économise les appels Claude/Pappers répétés
# Utilise /var/data si Render Disk monté, sinon /tmp (perd au redéploy)
# - Cache SIREN → contacts Claude+web (TTL 60 jours)
# - Cache domaine SIREN → domaine officiel (TTL 90j si trouvé, 1j si vide)
# ────────────────────────────────────────────────────────────────────
CACHE_DIR = "/var/data" if os.path.isdir("/var/data") else "/tmp"
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
CACHE_DB = os.path.join(CACHE_DIR, "enrich_cache.db")
CONTACTS_TTL_DAYS = 60
DOMAINE_TTL_DAYS = 90
DOMAINE_EMPTY_TTL_DAYS = 1

def _init_cache():
    with sqlite3.connect(CACHE_DB) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS contacts_cache (
            siren TEXT PRIMARY KEY, contacts_json TEXT NOT NULL, cached_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS domaine_cache (
            siren TEXT PRIMARY KEY, domaine TEXT NOT NULL, cached_at INTEGER NOT NULL
        )""")
        conn.commit()
_init_cache()
print(f"[CACHE] Init OK → {CACHE_DB}")

def _normalize_siren(siren) -> str:
    return re.sub(r'\D', '', str(siren or ''))[:9]

def cache_contacts_get(siren: str):
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9: return None
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            row = conn.execute("SELECT contacts_json, cached_at FROM contacts_cache WHERE siren=?", (siren,)).fetchone()
            if row and (time.time() - row[1]) < CONTACTS_TTL_DAYS * 86400:
                return json.loads(row[0])
    except Exception as e:
        print(f"[CACHE GET ERROR] {siren}: {e}")
    return None

def cache_contacts_set(siren: str, contacts: list):
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9 or not contacts: return
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            conn.execute("INSERT OR REPLACE INTO contacts_cache (siren, contacts_json, cached_at) VALUES (?, ?, ?)",
                         (siren, json.dumps(contacts, ensure_ascii=False), int(time.time())))
            conn.commit()
    except Exception as e:
        print(f"[CACHE SET ERROR] {siren}: {e}")

def cache_domaine_get(siren: str):
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9: return None
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            row = conn.execute("SELECT domaine, cached_at FROM domaine_cache WHERE siren=?", (siren,)).fetchone()
            if row:
                domaine, ts = row
                ttl = DOMAINE_TTL_DAYS if domaine else DOMAINE_EMPTY_TTL_DAYS
                if (time.time() - ts) < ttl * 86400:
                    return domaine
    except Exception as e:
        print(f"[DOMAINE CACHE GET ERROR] {siren}: {e}")
    return None

def cache_domaine_set(siren: str, domaine: str):
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9: return
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            conn.execute("INSERT OR REPLACE INTO domaine_cache (siren, domaine, cached_at) VALUES (?, ?, ?)",
                         (siren, domaine or "", int(time.time())))
            conn.commit()
    except Exception as e:
        print(f"[DOMAINE CACHE SET ERROR] {siren}: {e}")

# ────────────────────────────────────────────────────────────────────
# SYSTÈME D'ALERTES EMAIL (solde API + run failed)
# 1 envoi max par heure par API (anti-spam : si 100 contacts plantent
# en Kaspr 402, tu reçois 1 seul email, pas 100)
# ────────────────────────────────────────────────────────────────────
import time as _t_alert
_alert_state = {}  # {api_name: last_sent_timestamp}
_ALERT_THROTTLE_SECONDS = 3600  # 1 heure

def _send_alert(subject: str, body: str, recipient: str = "") -> bool:
    """Envoie un email d'alerte simple (sans pièce jointe) via SMTP OVH."""
    if not SMTP_USER or not SMTP_PASS:
        print(f"[ALERT] SMTP non configuré, impossible d'envoyer : {subject}")
        return False
    dest = recipient or ALERT_EMAIL
    try:
        msg = MIMEMultipart()
        msg['From']    = SMTP_USER
        msg['To']      = dest
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [dest], msg.as_string())
        print(f"[ALERT] ✅ '{subject}' envoyé à {dest}")
        return True
    except Exception as e:
        print(f"[ALERT ERROR] {e}")
        return False


def _alert_api_down(api_name: str, status_code: int = 0, detail: str = "") -> None:
    """Alerte 'solde API à 0' avec throttling 1h par API."""
    now = _t_alert.time()
    last = _alert_state.get(api_name, 0)
    if now - last < _ALERT_THROTTLE_SECONDS:
        return  # déjà alerté il y a moins d'1h
    _alert_state[api_name] = now
    subject = f"🚨 [Enrichisseur] Solde {api_name} épuisé"
    body = f"""Bonjour Marie,

L'API {api_name} a renvoyé une erreur indiquant un problème de crédit / quota
(status HTTP {status_code}).

Tant que tu ne recharges pas, le pipeline d'enrichissement va se dégrader
sur cette source de données.

→ Va recharger ton solde sur le dashboard {api_name}.

Détail technique :
{detail[:300]}

Heure de détection : {datetime.now().isoformat(timespec='seconds')}

(Cette alerte est limitée à 1 envoi par heure et par API pour éviter le spam.)

— Enrichisseur Dirigeants
"""
    _send_alert(subject, body)
ANCIENS_KEYWORDS = [
    "ancien", "ancienne", "ex-", "ex ", "démissionnaire",
    "jusqu'au", "jusqu au", "sortant"
]
TITRES_EXCLUS = [
    "commissaire aux comptes", "commissaire", "conseil de surveillance",
    "membre du conseil", "membre du directoire observateur",
    "censeur", "observateur", "représentant permanent",
    "liquidateur", "mandataire", "administrateur judiciaire",
]
def nettoyer_prenom(prenom: str) -> str:
    """Supprime civilités et prénoms composés Pappers : 'M Denis' → 'Denis', 'Florian, Paul' → 'Florian'."""
    if not prenom:
        return ""
    prenom = prenom.split(",")[0].strip()
    import re
    prenom = re.sub(r'^(M\.?\s+|Mme\.?\s+|Mr\.?\s+|Dr\.?\s+|Me\.?\s+)', '', prenom, flags=re.IGNORECASE).strip()
    return prenom
def est_ancien_dirigeant(titre: str) -> bool:
    return any(kw in titre.lower() for kw in ANCIENS_KEYWORDS)
def est_titre_exclu(titre: str) -> bool:
    return any(kw in titre.lower() for kw in TITRES_EXCLUS)
def domaine_valide(d: str) -> bool:
    d = d.strip()
    return bool(d) and "." in d and " " not in d and len(d) > 3
def nettoyer_domaine(url: str) -> str:
    """Extrait le domaine depuis une URL complète."""
    if not url:
        return ""
    d = url.lower().strip()
    d = d.replace("https://", "").replace("http://", "").replace("www.", "")
    d = d.split("/")[0].strip()
    return d
def noms_similaires(nom_csv: str, nom_pappers: str) -> bool:
    import unicodedata
    def normaliser(s):
        s = s.lower().strip()
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
        s = re.sub(r"[.\-_/]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    a = normaliser(nom_csv)
    b = normaliser(nom_pappers)
    if a == b:
        return True
    mots_a = set(w for w in a.split() if len(w) > 2)
    mots_b = set(w for w in b.split() if len(w) > 2)
    if not mots_a:
        return True
    return len(mots_a & mots_b) > 0
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# -------------------------------------------------------
# ROUTE AIDE — Mode d'emploi de l'enrichisseur (HTML standalone)
# Visible sur https://enrichisseur-dirigeants.onrender.com/aide
# -------------------------------------------------------
AIDE_HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mode d'emploi — Enrichisseur Dirigeants</title>
<style>
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    background: #f8fafc; color: #1e293b;
    margin: 0; padding: 24px; line-height: 1.6;
  }
  .wrap { max-width: 860px; margin: 0 auto; }

  .header {
    background: linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%);
    color: white; padding: 28px 32px; border-radius: 12px;
    box-shadow: 0 4px 12px rgba(30,58,95,0.15); margin-bottom: 24px;
  }
  .header h1 { margin: 0 0 8px 0; font-size: 28px; font-weight: 700; }
  .header .pipe {
    font-size: 13px; opacity: 0.95; margin-top: 8px;
  }
  .header .pipe span {
    background: rgba(255,255,255,0.18); padding: 4px 10px;
    border-radius: 6px; margin: 0 3px; display: inline-block;
  }

  .card {
    background: white; border: 1px solid #e2e8f0; border-radius: 12px;
    padding: 24px 28px; margin-bottom: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
  }
  .card h2 {
    color: #1e3a5f; margin: 0 0 16px 0; font-size: 20px;
    display: flex; align-items: center; gap: 8px;
  }

  ol { padding-left: 24px; margin: 0; }
  ol li { margin-bottom: 14px; }
  ol li strong { color: #1e3a5f; }
  ol li ul { margin-top: 8px; padding-left: 20px; }
  ol li ul li { margin-bottom: 6px; font-size: 14.5px; }

  .pipeline {
    display: flex; gap: 8px; margin: 20px 0; flex-wrap: wrap;
    align-items: center; justify-content: center;
  }
  .step {
    padding: 10px 16px; border-radius: 8px; font-size: 13px;
    font-weight: 600; color: white; display: inline-flex;
    align-items: center; gap: 8px;
  }
  .step .num {
    background: rgba(255,255,255,0.3);
    width: 20px; height: 20px; border-radius: 50%;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 11px;
  }
  .arrow { color: #94a3b8; font-size: 16px; }

  .s1 { background: #2563eb; }  /* Pappers (bleu) */
  .s2 { background: #7c3aed; }  /* Claude (violet) */
  .s3 { background: #ea580c; }  /* Pipedrive (orange) */
  .s4 { background: #0891b2; }  /* Kaspr (cyan) */
  .s5 { background: #059669; }  /* FullEnrich (vert) */

  .warn {
    background: #fffbea; border: 1px solid #facc15; border-radius: 8px;
    padding: 14px 18px; margin: 16px 0; font-size: 14px;
  }
  .warn strong { color: #92400e; }

  .tip {
    background: #eff6ff; border: 1px solid #93c5fd; border-radius: 8px;
    padding: 14px 18px; margin: 16px 0; font-size: 14px;
  }
  .tip strong { color: #1e40af; }

  table { width: 100%; border-collapse: collapse; margin-top: 12px; }
  th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #e2e8f0;
           font-size: 14px; }
  th { background: #f1f5f9; color: #1e3a5f; font-weight: 700; }
  td code { background: #f1f5f9; padding: 2px 6px; border-radius: 4px;
            font-size: 12.5px; color: #be123c; }

  .back {
    display: inline-block; margin-top: 24px; padding: 10px 18px;
    background: #2563eb; color: white; text-decoration: none;
    border-radius: 8px; font-weight: 600; font-size: 14px;
  }
  .back:hover { background: #1e40af; }
</style>
</head>
<body>
<div class="wrap">

  <div class="header">
    <h1>📖 Mode d'emploi — Enrichisseur Dirigeants</h1>
    <div class="pipe">
      <span>CSV Sociétés</span> →
      <span>Pappers</span> →
      <span>Claude+web</span> →
      <span>Pipedrive</span> →
      <span>Kaspr</span> →
      <span>FullEnrich</span> →
      <span>Excel par email</span>
    </div>
  </div>

  <div class="card">
    <h2>🚀 Comment lancer un enrichissement</h2>
    <ol>
      <li><strong>Télécharge le template Excel</strong> en cliquant sur <code>📥 Template</code> en haut à droite. Tu auras un fichier prêt à remplir avec les bonnes colonnes.</li>
      <li><strong>Remplis 1 ligne par société</strong> dans le template :
        <ul>
          <li>🟢 <code>nom</code> = obligatoire (nom commercial de la société, ex : <em>Grenke Location</em>)</li>
          <li>🟣 <code>siren</code>, <code>domaine</code>, <code>org_id</code>, <code>fondateurs</code>, <code>contact_prenom</code>, <code>contact_nom</code>, <code>contact_titre</code>, <code>code_postal</code>, <code>ville</code>, <code>adresse</code> = optionnels (mais plus tu remplis, meilleurs sont les résultats)</li>
        </ul>
      </li>
      <li><strong>Glisse ton fichier</strong> dans la zone <code>📂 Glissez votre fichier ici</code> (ou clique pour parcourir).</li>
      <li><strong>Saisis ton email</strong> dans <code>📧 Recevoir l'Excel par email</code> (vérifie aussi tes spams). Tu peux mettre plusieurs adresses séparées par des virgules.</li>
      <li><strong>Clique sur l'un des 2 modes de lancement</strong> :
        <ul>
          <li><code>⚡ Tout lancer</code> = enchaîne automatiquement les 5 étapes du pipeline (recommandé)</li>
          <li><strong>Étape par étape</strong> = lance manuellement chaque étape (utile pour debug ou pour ne faire qu'une partie)</li>
        </ul>
      </li>
      <li><strong>Reçois l'Excel par email</strong> à la fin (ou télécharge-le directement avec <code>⬇️ Excel</code>).</li>
    </ol>
  </div>

  <div class="card">
    <h2>🔄 Le pipeline en détail</h2>
    <div class="pipeline">
      <div class="step s1"><span class="num">1</span>Pappers</div>
      <span class="arrow">→</span>
      <div class="step s2"><span class="num">2</span>Claude+web</div>
      <span class="arrow">→</span>
      <div class="step s3"><span class="num">3</span>Pipedrive</div>
      <span class="arrow">→</span>
      <div class="step s4"><span class="num">4</span>Kaspr</div>
      <span class="arrow">→</span>
      <div class="step s5"><span class="num">5</span>FullEnrich</div>
    </div>

    <table>
      <thead>
        <tr><th>Étape</th><th>Ce qu'elle fait</th><th>Ce qu'elle ramène</th></tr>
      </thead>
      <tbody>
        <tr>
          <td><strong>Pappers</strong></td>
          <td>Interroge la base légale française par SIREN ou nom + ville</td>
          <td>SIREN, domaine, représentants légaux actifs (président, gérant…)</td>
        </tr>
        <tr>
          <td><strong>Claude+web</strong></td>
          <td>Recherche en ligne les dirigeants opérationnels (CEO, CFO, CTO, COO, DG, DRH, Partners…)</td>
          <td>Personnes en poste avec emails pro probables</td>
        </tr>
        <tr>
          <td><strong>Pipedrive</strong></td>
          <td>Vérifie si la société et ses contacts existent déjà dans ton CRM</td>
          <td>Email + téléphone des contacts déjà connus</td>
        </tr>
        <tr>
          <td><strong>Kaspr</strong></td>
          <td>Pour chaque dirigeant sans email, cherche son LinkedIn puis l'email pro associé</td>
          <td>URL LinkedIn + email pro vérifié</td>
        </tr>
        <tr>
          <td><strong>FullEnrich</strong></td>
          <td>Batch final pour les contacts qui restent sans email/téléphone (par domaine)</td>
          <td>Email + téléphone (waterfall multi-providers)</td>
        </tr>
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>⚠️ Bonnes pratiques</h2>
    <div class="warn">
      <strong>⏱️ Patience</strong> : un fichier de 50 sociétés prend ~15-30 min, 200 sociétés ~1-2h. Le pipeline est volontairement séquentiel pour ne pas se faire bloquer par les API en aval. <strong>Tu peux fermer l'onglet</strong>, l'email arrivera à la fin.
    </div>
    <div class="tip">
      <strong>💡 Optimise ton input</strong> : plus tu fournis d'infos en entrée (SIREN, domaine, code postal, ville), moins l'enrichisseur tâtonne et plus le résultat est précis. Si tu connais déjà un contact (Prénom + Nom + Titre), remplis-le aussi : il sera enrichi en priorité.
    </div>
    <div class="tip">
      <strong>🎯 Filtres automatiques</strong> : les anciens dirigeants (ex-CEO, démissionnaires…) et les rôles non-opérationnels (commissaire aux comptes, censeur, liquidateur, mandataire…) sont automatiquement exclus du résultat.
    </div>
    <div class="warn">
      <strong>🚫 Cas qui peuvent rater</strong> : société radiée (signalée en rouge dans l'Excel), sociétés sans aucun dirigeant identifiable (renvoyée vide), domaine introuvable (FullEnrich sera skip). Ce n'est pas un bug, juste des limites des sources de données.
    </div>
  </div>

  <div class="card">
    <h2>📊 Le résultat</h2>
    <p>L'Excel final contient <strong>1 ligne par dirigeant</strong> trouvé, avec les colonnes :</p>
    <table>
      <thead><tr><th>Colonne</th><th>Contenu</th></tr></thead>
      <tbody>
        <tr><td><strong>Organisation</strong></td><td>Nom de la société</td></tr>
        <tr><td><strong>Prénom / Nom / Titre</strong></td><td>Identité du dirigeant</td></tr>
        <tr><td><strong>Email</strong></td><td>Email pro (en bleu si trouvé)</td></tr>
        <tr><td><strong>Téléphone</strong></td><td>Mobile en priorité, sinon fixe</td></tr>
        <tr><td><strong>LinkedIn</strong></td><td>URL profil quand trouvée</td></tr>
        <tr><td><strong>Domaine</strong></td><td>Site web officiel de la société</td></tr>
        <tr><td><strong>Confiance</strong></td><td>haute / moyenne / faible (fiabilité de l'email)</td></tr>
        <tr><td><strong>Source</strong></td><td>Pappers, Claude+web, Pipedrive, Kaspr, FullEnrich (ou combinaisons)</td></tr>
        <tr><td><strong>Dans Pipedrive</strong></td><td>"oui" si la société/contact est déjà dans ton CRM</td></tr>
      </tbody>
    </table>
    <p style="margin-top:14px;font-size:14px;color:#64748b;">Les lignes sont regroupées par société (alternance de couleur), avec un filtre Excel actif sur les en-têtes pour trier/filtrer rapidement.</p>
  </div>

  <a href="/" class="back">← Retour à l'enrichisseur</a>

</div>
</body>
</html>"""


@app.get("/aide", response_class=HTMLResponse)
async def aide():
    """Mode d'emploi visuel de l'enrichisseur, accessible directement
    sur /aide ou via un lien depuis l'index.html."""
    return HTMLResponse(content=AIDE_HTML)


# -------------------------------------------------------
# ROUTE ALERTE — Envoi d'un email d'alerte générique
# Appelée par le HF Space quand un run plante (mais utilisable
# pour n'importe quelle notification)
# -------------------------------------------------------
@app.post("/send_alert")
async def send_alert_route(request: Request):
    """POST {"subject": "...", "body": "...", "recipient": "..." (optionnel)}
    → envoi SMTP. Si recipient absent, utilise ALERT_EMAIL."""
    data = await request.json()
    subject = data.get("subject", "[Enrichisseur] Notification")
    body = data.get("body", "")
    recipient = (data.get("recipient") or "").strip()
    ok = _send_alert(subject, body, recipient or "")
    return {"ok": ok}


@app.get("/test_alert")
async def test_alert_route():
    """Test rapide : envoie un email d'alerte à ALERT_EMAIL.
    Visite https://enrichisseur-dirigeants.onrender.com/test_alert
    pour vérifier que les alertes arrivent bien dans ta boîte."""
    ok = _send_alert(
        subject="✅ [Enrichisseur] Test d'alerte — tout fonctionne",
        body=("Bonjour Marie,\n\n"
              "Si tu lis ceci, c'est que le système d'alerte SMTP fonctionne. "
              "Tu recevras un email de cette nature quand :\n\n"
              "• Un run de prospection plante (avec le détail de l'erreur)\n"
              "• Le solde d'une de tes APIs est épuisé (Pappers, Claude, "
              "Kaspr, FullEnrich) — 1 seul email par heure et par API pour "
              "éviter le spam.\n\n"
              f"Destinataire configuré : {ALERT_EMAIL}\n\n"
              "— Enrichisseur Dirigeants"),
    )
    return {"ok": ok, "recipient": ALERT_EMAIL,
            "info": "Si ok=true, l'email est parti. Vérifie ta boîte (et les spams)."}


@app.get("/template")
async def get_template():
    """Génère et sert le template Excel enrichisseur."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    wb = Workbook()
    ws = wb.active
    ws.title = "Sociétés à enrichir"
    colonnes = [
        ("nom",            True,  "Nom commercial de la société",        "Grenke Location"),
        ("siren",          False, "SIREN ou SIRET (9 ou 14 chiffres)",   "428616734"),
        ("domaine",        False, "Domaine du site web",                 "grenke.fr"),
        ("org_id",         False, "ID Pipedrive de l'organisation",      "17136"),
        ("fondateurs",     False, "Fondateurs connus (contexte Claude)", "Jean Dupont, Marie Martin"),
        ("contact_prenom", False, "Prénom du contact principal",         "Nathalie"),
        ("contact_nom",    False, "Nom de famille du contact",           "Seyller"),
        ("contact_titre",  False, "Poste / fonction du contact",         "CFO"),
        ("code_postal",    False, "Code postal du siège",                "75008"),
        ("ville",          False, "Ville du siège",                      "Paris"),
        ("adresse",        False, "Adresse complète du siège",           "9 Rue de Lisbonne 75008 Paris"),
    ]
    thin   = Side(style='thin', color="e2e8f0")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    ws.merge_cells(f'A1:{get_column_letter(len(colonnes))}1')
    c = ws['A1']
    c.value = "📋 Template Enrichisseur Dirigeants — 1 ligne par société"
    c.font  = Font(name='Arial', bold=True, size=13, color="FFFFFF")
    c.fill  = PatternFill('solid', start_color="1e3a5f")
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[1].height = 30
    ws.merge_cells(f'A2:{get_column_letter(len(colonnes))}2')
    c = ws['A2']
    c.value = "🟢 Colonne obligatoire    🟣 Colonne optionnelle — plus vous remplissez, meilleurs sont les résultats"
    c.font  = Font(name='Arial', size=10, color="FFFFFF")
    c.fill  = PatternFill('solid', start_color="2563eb")
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[2].height = 20
    for col_idx, (nom, obligatoire, desc, exemple) in enumerate(colonnes, 1):
        c = ws.cell(row=3, column=col_idx, value=nom)
        c.font  = Font(name='Arial', bold=True, size=10, color="FFFFFF")
        c.fill  = PatternFill('solid', start_color="059669" if obligatoire else "7c3aed")
        c.alignment = Alignment(horizontal='center', vertical='center')
        c.border = border
    ws.row_dimensions[3].height = 25
    for col_idx, (nom, obligatoire, desc, exemple) in enumerate(colonnes, 1):
        c = ws.cell(row=4, column=col_idx, value=desc)
        c.font  = Font(name='Arial', italic=True, size=9, color="475569")
        c.fill  = PatternFill('solid', start_color="f8fafc")
        c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        c.border = border
    ws.row_dimensions[4].height = 35
    for col_idx, (nom, obligatoire, desc, exemple) in enumerate(colonnes, 1):
        c = ws.cell(row=5, column=col_idx, value=exemple)
        c.font  = Font(name='Arial', size=9, color="94a3b8")
        c.fill  = PatternFill('solid', start_color="f1f5f9")
        c.alignment = Alignment(horizontal='center', vertical='center')
        c.border = border
    ws.row_dimensions[5].height = 20
    for row in range(6, 26):
        for col_idx in range(1, len(colonnes)+1):
            c = ws.cell(row=row, column=col_idx, value="")
            c.fill = PatternFill('solid', start_color="ffffff" if row % 2 == 0 else "f8fafc")
            c.border = border
            c.font = Font(name='Arial', size=10)
        ws.row_dimensions[row].height = 18
    largeurs = [22, 14, 20, 12, 28, 16, 18, 18, 12, 14, 32]
    for i, w in enumerate(largeurs, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = 'A6'
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=template_enrichisseur.xlsx"}
    )
async def health():
    return {
        "ok": True,
        "anthropic_key": bool(ANTHROPIC_KEY),
        "pappers_key": bool(PAPPERS_KEY),
        "fullenrich_key": bool(FULLENRICH_KEY),
        "pipedrive_key": bool(PIPEDRIVE_KEY),
        "kaspr_key": bool(KASPR_KEY),
    }
async def check_pipedrive(prenom: str, nom: str) -> dict:
    if not PIPEDRIVE_KEY or not prenom or not nom:
        return {}
    terme = f"{prenom} {nom}".strip()
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                "https://api.pipedrive.com/v1/persons/search",
                params={"term": terme, "fields": "name,email,phone", "exact_match": "false", "limit": 5, "api_token": PIPEDRIVE_KEY}
            )
            if r.status_code == 200:
                items = r.json().get("data", {}).get("items", [])
                for item in items:
                    person = item.get("item", {})
                    person_name = person.get("name", "").lower()
                    if nom.lower() in person_name or prenom.lower() in person_name:
                        email = ""
                        emails = person.get("emails", [])
                        if emails:
                            email = emails[0] if isinstance(emails[0], str) else emails[0].get("value", "")
                        phone = ""
                        phones = person.get("phones", [])
                        for ph in phones:
                            val   = ph if isinstance(ph, str) else ph.get("value", "")
                            label = (ph.get("label","") if isinstance(ph, dict) else "").lower()
                            if val and ("mobile" in label or "portable" in label or "cell" in label):
                                phone = val
                                break
                        if not phone and phones:
                            ph = phones[0]
                            phone = ph if isinstance(ph, str) else ph.get("value", "")
                        if email and "@" in email:
                            print(f"[PIPEDRIVE] ✅ {terme} → {email} | tél: {phone}")
                            return {"email": email, "phone": phone}
    except Exception as e:
        print(f"[PIPEDRIVE ERROR] {terme}: {e}")
    return {}
async def trouver_linkedin(prenom: str, nom: str, societe: str) -> str:
    """Cherche l'URL LinkedIn du dirigeant via Claude+web (max_uses:1)."""
    if not ANTHROPIC_KEY:
        return ""
    prenom = nettoyer_prenom(prenom)
    if not prenom or not nom:
        return ""
    try:
        prompt = f"""Trouve l'URL LinkedIn exacte de cette personne :
Prénom: {prenom}
Nom: {nom}
Société: {societe}
Réponds UNIQUEMENT avec l'URL complète (ex: https://www.linkedin.com/in/prenom-nom-xxxxx/)
Si tu n'es pas certain à 100%, réponds: NON"""
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                json={
                    "model": MODEL_HAIKU,  # extraction simple, Haiku suffit
                    "max_tokens": 200,
                    "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            print(f"[LINKEDIN] Status {r.status_code} pour {prenom} {nom}")
            if r.status_code == 200:
                all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text").strip()
                m = re.search(r'https?://(?:www\.)?linkedin\.com/in/[^\s\)\"\'\]]+', all_text)
                if m:
                    url = m.group().rstrip('/')
                    print(f"[LINKEDIN] ✅ {prenom} {nom} → {url}")
                    return url
    except Exception as e:
        print(f"[LINKEDIN ERROR] {prenom} {nom}: {e}")
    print(f"[LINKEDIN] ❌ Pas trouvé pour {prenom} {nom}")
    return ""
async def kaspr_email(prenom: str, nom: str, linkedin_url: str) -> str:
    """Récupère l'email via Kaspr avec une URL LinkedIn — B2B uniquement (illimité)."""
    if not KASPR_KEY or not linkedin_url:
        return ""
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            print(f"[KASPR] Appel pour {prenom} {nom} — {linkedin_url}")
            r = await c.post(
                "https://api.developers.kaspr.io/profile/linkedin",
                headers={
                    "Authorization": f"Bearer {KASPR_KEY}",
                    "Content-Type": "application/json",
                    "accept-version": "v2.0"
                },
                json={
                    "name": f"{prenom} {nom}",
                    "id": linkedin_url,
                    "dataToGet": ["workEmail"]
                }
            )
            print(f"[KASPR] Status {r.status_code} pour {prenom} {nom}")
            if r.status_code == 200:
                d = r.json()
                emails = d.get("emails", []) or d.get("workEmails", []) or d.get("work_emails", [])
                if isinstance(emails, list) and emails:
                    email = emails[0] if isinstance(emails[0], str) else emails[0].get("value","")
                    if email and "@" in email:
                        print(f"[KASPR] ✅ Email trouvé : {email}")
                        return email
                email = d.get("email","") or d.get("workEmail","") or d.get("work_email","")
                if email and "@" in email:
                    print(f"[KASPR] ✅ Email trouvé : {email}")
                    return email
            elif r.status_code == 402:
                print(f"[KASPR] Plus de crédits !")
                _alert_api_down("Kaspr", 402, r.text[:200] if hasattr(r, 'text') else "")
            else:
                print(f"[KASPR] Erreur {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"[KASPR ERROR] {prenom} {nom}: {e}")
    return ""
async def corriger_domaine(siren: str, societe: str) -> str:
    """Tente de trouver le vrai domaine via Pappers (SIREN ou nom) puis Claude.
    Met en cache 90j (1j si vide) pour éviter les appels répétés."""
    # CACHE : on a déjà cherché le domaine pour ce SIREN ?
    cached = cache_domaine_get(siren)
    if cached is not None:
        if cached:
            print(f"[DOMAINE CACHE HIT] {societe} → {cached}")
        return cached

    if PAPPERS_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                if siren:
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "siren": siren})
                    if r.status_code == 200:
                        d = r.json()
                        domaine = nettoyer_domaine(d.get("domaine_url","") or d.get("site_web",""))
                        if domaine_valide(domaine):
                            print(f"[DOMAINE FIX] Pappers SIREN → {domaine} pour {societe}")
                            cache_domaine_set(siren, domaine)
                            return domaine
                r2 = await c.get("https://api.pappers.fr/v2/recherche",
                    params={"api_token": PAPPERS_KEY, "q": societe, "par_page": 1})
                if r2.status_code == 200:
                    resultats = r2.json().get("resultats", [])
                    if resultats:
                        domaine = nettoyer_domaine(resultats[0].get("domaine_url","") or resultats[0].get("site_web",""))
                        if domaine_valide(domaine):
                            print(f"[DOMAINE FIX] Pappers nom → {domaine} pour {societe}")
                            cache_domaine_set(siren, domaine)
                            return domaine
        except Exception as e:
            print(f"[DOMAINE FIX ERROR Pappers] {e}")
    if ANTHROPIC_KEY:
        try:
            prompt = f"""Quel est le nom de domaine du site web officiel de cette société française : {societe} ?
Réponds UNIQUEMENT avec le domaine (ex: example.com), sans http ni www, sans aucun autre texte."""
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                    json={
                        "model": MODEL_HAIKU,  # extraction de domaine, Haiku suffit
                        "max_tokens": 50,
                        "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                        "messages": [{"role": "user", "content": prompt}]
                    }
                )
                if r.status_code == 200:
                    all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text").strip()
                    domaine = nettoyer_domaine(all_text.split()[0] if all_text else "")
                    if domaine_valide(domaine):
                        print(f"[DOMAINE FIX] Claude → {domaine} pour {societe}")
                        cache_domaine_set(siren, domaine)
                        return domaine
        except Exception as e:
            print(f"[DOMAINE FIX ERROR Claude] {e}")
    # Échec : on cache le vide (TTL 1j) pour éviter de re-tenter en boucle
    cache_domaine_set(siren, "")
    return ""
# -------------------------------------------------------
# ROUTE PASSE 1 : Pappers + Claude + Pipedrive
# -------------------------------------------------------
@app.post("/enrich_one")
async def enrich_one(request: Request):
    data = await request.json()
    pipedrive_org_contacts = []  # rempli plus bas si l'org est trouvée dans Pipedrive
    nom            = data.get("nom", "")
    siren          = re.sub(r'\D', '', data.get("siren", ""))[:9]
    domaine        = nettoyer_domaine(data.get("domaine", ""))
    org_id         = data.get("org_id", "")
    fondateurs     = data.get("fondateurs", "")
    contact_prenom = data.get("contact_prenom", "")
    contact_nom    = data.get("contact_nom", "")
    contact_titre  = data.get("contact_titre", "")
    contact_email  = data.get("contact_email", "")
    code_postal    = data.get("code_postal", "")
    ville          = data.get("ville", "")
    print(f"[START] {nom} | domaine={domaine} | siren={siren}")
    # ── CHECK PIPEDRIVE ORGANISATION ──
    if PIPEDRIVE_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(
                    "https://api.pipedrive.com/v1/organizations/search",
                    params={"term": nom, "exact_match": "false", "limit": 5, "api_token": PIPEDRIVE_KEY}
                )
                if r.status_code == 200:
                    items = r.json().get("data", {}).get("items", [])
                    for item in items:
                        org_name = item.get("item", {}).get("name", "")
                        org_id_pipe = item.get("item", {}).get("id", "")
                        if noms_similaires(nom, org_name):
                            print(f"[PIPEDRIVE ORG] ✅ '{nom}' dans Pipedrive → récupération contacts")
                            contacts_pipe = []
                            try:
                                r2 = await c.get(
                                    f"https://api.pipedrive.com/v1/organizations/{org_id_pipe}/persons",
                                    params={"api_token": PIPEDRIVE_KEY, "limit": 50}
                                )
                                if r2.status_code == 200:
                                    persons = r2.json().get("data") or []
                                    for p in persons:
                                        prenom_p = p.get("first_name", "") or ""
                                        nom_p    = p.get("last_name", "") or ""
                                        titre_p  = p.get("job_title", "") or ""
                                        emails_p = p.get("email", []) or []
                                        email_p  = ""
                                        for e in emails_p:
                                            val = e.get("value","") if isinstance(e, dict) else str(e)
                                            if val and "@" in val:
                                                email_p = val
                                                break
                                        phones_p = p.get("phone", []) or []
                                        phone_p  = ""
                                        for ph in phones_p:
                                            val   = ph.get("value","") if isinstance(ph, dict) else str(ph)
                                            label = (ph.get("label","") if isinstance(ph, dict) else "").lower()
                                            if val and ("mobile" in label or "portable" in label or "cell" in label):
                                                phone_p = val
                                                break
                                        if not phone_p:
                                            for ph in phones_p:
                                                val = ph.get("value","") if isinstance(ph, dict) else str(ph)
                                                if val:
                                                    phone_p = val
                                                    break
                                        contacts_pipe.append({
                                            "prenom": prenom_p, "nom": nom_p,
                                            "titre": titre_p, "email": email_p,
                                            "phone": phone_p,
                                            "confiance": "haute" if (email_p and phone_p) else "faible",
                                            "source": "Pipedrive",
                                            "dans_pipedrive": "oui",
                                            "siren": siren,
                                        })
                                    print(f"[PIPEDRIVE ORG] {len(contacts_pipe)} contacts récupérés")
                            except Exception as e2:
                                print(f"[PIPEDRIVE ORG CONTACTS ERROR] {e2}")
                            pipedrive_org_contacts = contacts_pipe
                            print(f"[PIPEDRIVE ORG] {len(contacts_pipe)} Pipedrive — on continue avec Pappers + Claude pour compléter")
                            break
        except Exception as e:
            print(f"[PIPEDRIVE ORG ERROR] {e}")
    pappers_contacts = []
    pappers_data = None
    if PAPPERS_KEY:
        if domaine_valide(domaine):
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "site_internet": domaine})
                    if r.status_code == 200:
                        pappers_data = r.json()
                        print(f"[PAPPERS] Trouvé par domaine")
                    elif r.status_code in (401, 402, 403, 429):
                        _alert_api_down("Pappers", r.status_code, r.text[:200])
            except Exception as e:
                print(f"[PAPPERS ERROR domaine] {e}")
        if not pappers_data and not siren:
            try:
                params = {"api_token": PAPPERS_KEY, "q": nom, "par_page": 1}
                if code_postal: params["code_postal"] = code_postal
                if ville:       params["ville"] = ville
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get("https://api.pappers.fr/v2/recherche", params=params)
                    if r.status_code == 200:
                        resultats = r.json().get("resultats", [])
                        if resultats:
                            siren = resultats[0].get("siren", "")
                            print(f"[PAPPERS] SIREN trouvé par nom : {siren}")
                    elif r.status_code in (401, 402, 403, 429):
                        _alert_api_down("Pappers", r.status_code, r.text[:200])
            except Exception as e:
                print(f"[PAPPERS ERROR nom] {e}")
        if not pappers_data and siren:
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "siren": siren})
                    if r.status_code == 200:
                        pappers_data = r.json()
                    elif r.status_code in (401, 402, 403, 429):
                        _alert_api_down("Pappers", r.status_code, r.text[:200])
            except Exception as e:
                print(f"[PAPPERS ERROR siren] {e}")
        if pappers_data:
            if not siren:
                siren = pappers_data.get("siren", "")
            if not domaine_valide(domaine):
                domaine_pappers = nettoyer_domaine(
                    pappers_data.get("domaine_url","") or pappers_data.get("site_web","")
                )
                if domaine_valide(domaine_pappers):
                    domaine = domaine_pappers
                    print(f"[PAPPERS] Domaine récupéré : {domaine}")
            nom_pappers = pappers_data.get("nom_entreprise","") or pappers_data.get("denomination","")
            if nom_pappers and not noms_similaires(nom, nom_pappers):
                print(f"[PAPPERS] ⚠️ Mauvaise société : '{nom_pappers}' pour '{nom}' — ignoré")
                pappers_data = None
                siren = ""
                domaine = nettoyer_domaine(data.get("domaine",""))
        if pappers_data:
            if pappers_data.get("entreprise_cessee") or pappers_data.get("statut_rcs","").lower() == "radié" or pappers_data.get("statut_consolide","").lower() == "radié":
                print(f"[RADIÉE] {nom} — arrêt")
                return {"results": [{"org_id":org_id,"societe":nom,"siren":siren,"domaine":domaine,"prenom":"","nom_dg":"","titre":"⚠️ Société radiée","email":"","confiance":"","source":"Pappers"}]}
            for rep in pappers_data.get("representants", []):
                if rep.get("personne_morale"):
                    continue
                titre = rep.get("qualite","Représentant légal")
                if est_ancien_dirigeant(titre) or est_titre_exclu(titre):
                    continue
                prenom_raw = rep.get("prenom","")
                prenom_clean = nettoyer_prenom(prenom_raw)
                pappers_contacts.append({
                    "prenom": prenom_clean,
                    "nom":    rep.get("nom",""),
                    "titre":  titre,
                    "email":  "",
                    "confiance": "",
                    "source": "Pappers"
                })
            print(f"[PAPPERS] {len(pappers_contacts)} représentants actifs | domaine={domaine}")
    if contact_prenom and contact_nom:
        contact_prenom_clean = nettoyer_prenom(contact_prenom)
        deja_present = any(
            noms_similaires(contact_prenom_clean, ct.get("prenom","")) and
            noms_similaires(contact_nom, ct.get("nom",""))
            for ct in pappers_contacts
        )
        if not deja_present:
            pappers_contacts.insert(0, {
                "prenom": contact_prenom_clean,
                "nom":    contact_nom,
                "titre":  contact_titre or "Dirigeant",
                "email":  contact_email,
                "confiance": "haute" if contact_email else "",
                "source": "Fichier source"
            })
            print(f"[SOURCE] Contact pré-rempli : {contact_prenom_clean} {contact_nom} ({contact_titre})")
        else:
            print(f"[SOURCE] Contact déjà dans Pappers : {contact_prenom_clean} {contact_nom} — skip")
    claude_contacts = []
    # CACHE : si on a déjà appelé Claude+web pour ce SIREN dans les 60j, on réutilise
    cached_claude = cache_contacts_get(siren) if siren else None
    if cached_claude is not None:
        claude_contacts = cached_claude
        print(f"[CACHE HIT] {nom} (SIREN {siren}) → {len(cached_claude)} contacts Claude réutilisés")
    elif ANTHROPIC_KEY:
        noms_deja = [f"{c['prenom']} {c['nom']}".strip() for c in pappers_contacts]
        exclusion = f"\nDirigeants déjà connus à ne PAS inclure : {', '.join(noms_deja)}" if noms_deja else ""
        contexte_fondateurs = f"\nFondateurs connus : {fondateurs}" if fondateurs else ""
        # Bloc dynamique uniquement (les instructions sont dans SYSTEM_ENRICH, cachées)
        prompt = f"""Société française à enrichir :
Nom : {nom}{chr(10)+"Site : "+domaine if domaine_valide(domaine) else ""}{chr(10)+"SIREN : "+siren if siren else ""}{contexte_fondateurs}{exclusion}"""
        delays = [10, 25, 45]
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=90) as c:
                    print(f"[CLAUDE] Tentative {attempt+1}/3 pour {nom}")
                    r = await c.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                        json={
                            "model": MODEL_SONNET,
                            "max_tokens": 1000,
                            "system": [
                                {"type": "text", "text": SYSTEM_ENRICH,
                                 "cache_control": {"type": "ephemeral"}}
                            ],
                            "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                            "messages": [{"role": "user", "content": prompt}]
                        }
                    )
                    print(f"[CLAUDE] Status {r.status_code} pour {nom}")
                    if r.status_code == 401:
                        _alert_api_down("Anthropic Claude", 401, r.text[:200])
                    if r.status_code in (429, 529):
                        await asyncio.sleep(delays[attempt])
                        continue
                    if r.status_code == 200:
                        all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
                        m = re.search(r'\{[\s\S]*"contacts"[\s\S]*\}', all_text)
                        if m:
                            parsed = json.loads(m.group())
                            for ct in parsed.get("contacts",[]):
                                ct["source"] = "Claude+web"
                                email = ct.get("email","") or ""
                                if any(x in email for x in ["gmail","hotmail","yahoo","outlook.com"]):
                                    ct["email"] = ""
                                    ct["confiance_email"] = "faible"
                                if est_ancien_dirigeant(ct.get("titre","")) or est_titre_exclu(ct.get("titre","")):
                                    ct["_skip"] = True
                            claude_contacts = [ct for ct in parsed.get("contacts",[]) if not ct.get("_skip")]
                            print(f"[CLAUDE OK] {len(claude_contacts)} contacts pour {nom}")
                            # Sauve en cache pour 60 jours (économise les futurs scans)
                            if siren and claude_contacts:
                                cache_contacts_set(siren, claude_contacts)
                                print(f"[CACHE SET] {nom} (SIREN {siren}) → {len(claude_contacts)} contacts")
                        break
                    else:
                        print(f"[CLAUDE ERROR DETAIL] {r.status_code}: {r.text[:300]}")
                        break
            except Exception as e:
                print(f"[CLAUDE EXCEPTION] {e}")
                if attempt < 2:
                    await asyncio.sleep(delays[attempt])
    if not domaine_valide(domaine) and siren:
        domaine = await corriger_domaine(siren, nom)
    for ct in pappers_contacts + claude_contacts + pipedrive_org_contacts:
        if not ct.get("domaine"):
            ct["domaine"] = domaine
    if PIPEDRIVE_KEY:
        for ct in pappers_contacts + claude_contacts:
            if ct.get("email"):
                continue
            pd_data = await check_pipedrive(ct.get("prenom",""), ct.get("nom",""))
            if pd_data.get("email"):
                ct["email"] = pd_data["email"]
                ct["confiance_email"] = "haute"
                ct["source"] = ct.get("source","") + "+Pipedrive"
                ct["dans_pipedrive"] = "oui"
            if pd_data.get("phone"):
                ct["phone"] = pd_data["phone"]
    existing_keys = {(c.get("prenom","").lower(), c.get("nom","").lower())
                     for c in pappers_contacts + claude_contacts
                     if c.get("prenom") or c.get("nom")}
    for ct in pipedrive_org_contacts:
        key = (ct.get("prenom","").lower(), ct.get("nom","").lower())
        if key not in existing_keys and (ct.get("prenom") or ct.get("nom")):
            pappers_contacts.insert(0, ct)
            existing_keys.add(key)
    tous_contacts = pappers_contacts + claude_contacts
    if not tous_contacts:
        tous_contacts = [{"prenom":"","nom":"","titre":"","email":"","confiance":"","source":""}]
    results = []
    for ct in tous_contacts:
        results.append({
            "org_id":         org_id,
            "societe":        nom,
            "siren":          siren,
            "domaine":        domaine,
            "prenom":         ct.get("prenom",""),
            "nom_dg":         ct.get("nom",""),
            "titre":          ct.get("titre",""),
            "email":          ct.get("email","") or "",
            "phone":          ct.get("phone","") or "",
            "linkedin":       ct.get("linkedin",""),
            "confiance":      ct.get("confiance_email", ct.get("confiance","")),
            "source":         ct.get("source",""),
            "dans_pipedrive": ct.get("dans_pipedrive",""),
        })
    print(f"[DONE] {nom} → {len(results)} contacts | domaine={domaine}")
    return {"results": results}
@app.post("/enrich_claude")
async def enrich_claude(request: Request):
    data = await request.json()
    nom        = data.get("nom", "")
    siren      = data.get("siren", "")
    domaine    = nettoyer_domaine(data.get("domaine", ""))
    fondateurs = data.get("fondateurs", "")
    max_contacts = int(data.get("max_contacts", 3))
    if not ANTHROPIC_KEY:
        return {"contacts": []}
    contexte_fondateurs = f"\nFondateurs connus : {fondateurs}" if fondateurs else ""
    prompt = f"""Société française à enrichir :
Nom : {nom}{chr(10)+"Site : "+domaine if domaine_valide(domaine) else ""}{chr(10)+"SIREN : "+siren if siren else ""}{contexte_fondateurs}
Retourne au maximum {max_contacts} contact(s)."""
    delays = [10, 25, 45]
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                print(f"[CLAUDE PHASE2] Tentative {attempt+1}/3 pour {nom}")
                r = await c.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                    json={
                        "model": MODEL_SONNET,
                        "max_tokens": 1000,
                        "system": [
                            {"type": "text", "text": SYSTEM_ENRICH,
                             "cache_control": {"type": "ephemeral"}}
                        ],
                        "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                        "messages": [{"role": "user", "content": prompt}]
                    }
                )
                print(f"[CLAUDE PHASE2] Status {r.status_code} pour {nom}")
                if r.status_code in (429, 529):
                    await asyncio.sleep(delays[attempt])
                    continue
                if r.status_code == 200:
                    all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
                    m = re.search(r'\{[\s\S]*"contacts"[\s\S]*\}', all_text)
                    if m:
                        parsed = json.loads(m.group())
                        contacts = []
                        for ct in parsed.get("contacts",[]):
                            email = ct.get("email","") or ""
                            if any(x in email for x in ["gmail","hotmail","yahoo","outlook.com"]):
                                ct["email"] = ""
                                ct["confiance_email"] = "faible"
                            if not est_ancien_dirigeant(ct.get("titre","")) and not est_titre_exclu(ct.get("titre","")):
                                contacts.append(ct)
                        print(f"[CLAUDE PHASE2 OK] {len(contacts)} contacts pour {nom}")
                        return {"contacts": contacts}
                else:
                    print(f"[CLAUDE PHASE2 ERROR] {r.status_code}: {r.text[:200]}")
                break
        except Exception as e:
            print(f"[CLAUDE PHASE2 EXCEPTION] {e}")
            if attempt < 2:
                await asyncio.sleep(delays[attempt])
    return {"contacts": []}
@app.post("/check_pipedrive")
async def check_pipedrive_route(request: Request):
    data = await request.json()
    prenom = data.get("prenom","")
    nom    = data.get("nom","")
    result = await check_pipedrive(prenom, nom)
    return {"email": result.get("email",""), "phone": result.get("phone","")}
@app.post("/enrich_emails")
async def enrich_emails(request: Request):
    data = await request.json()
    contacts = data.get("contacts", [])
    if not contacts:
        return {"emails": {}}
    emails_result = {}
    emails_par_nom = {}
    if KASPR_KEY:
        for ct in contacts:
            email = ct.get("email","")
            if email and "*" not in email:
                emails_par_nom[f"{ct.get('prenom','')} {ct.get('nom','')}".lower().strip()] = email
                continue
            prenom_clean = nettoyer_prenom(ct.get("prenom",""))
            cle = f"{prenom_clean} {ct.get('nom','')}".lower().strip()
            if cle in emails_par_nom:
                idx = str(ct.get("idx",0))
                emails_result[idx] = {"email": emails_par_nom[cle], "source": "+dedup"}
                print(f"[DEDUP] {cle} → email déjà trouvé, skip Kaspr")
                continue
            prenom = nettoyer_prenom(ct.get("prenom",""))
            nom_ct = ct.get("nom","")
            societe_ct = ct.get("societe","")
            idx = str(ct.get("idx",0))
            if not prenom or not nom_ct:
                continue
            print(f"[KASPR] Recherche LinkedIn pour {prenom} {nom_ct}")
            linkedin_url = await trouver_linkedin(prenom, nom_ct, societe_ct)
            if linkedin_url:
                if idx not in emails_result:
                    emails_result[idx] = {"email": "", "linkedin": linkedin_url, "source": ""}
                else:
                    emails_result[idx]["linkedin"] = linkedin_url
                email_kaspr = await kaspr_email(prenom, nom_ct, linkedin_url)
                if email_kaspr:
                    ct["email"] = email_kaspr
                    ct["linkedin"] = linkedin_url
                    ct["source_kaspr"] = True
                    emails_result[idx] = {"email": email_kaspr, "linkedin": linkedin_url, "source": "+Kaspr"}
                    print(f"[KASPR] ✅ {prenom} {nom_ct} → {email_kaspr}")
    domaines_corriges = {}
    for ct in contacts:
        if not domaine_valide(ct.get("domaine","")):
            societe = ct.get("societe","")
            siren = ct.get("siren","")
            if societe not in domaines_corriges:
                print(f"[DOMAINE FIX] Correction pour {societe}...")
                domaines_corriges[societe] = await corriger_domaine(siren, societe)
            if domaines_corriges[societe]:
                ct["domaine"] = domaines_corriges[societe]
    to_enrich = []
    phase = data.get("phase","fullenrich")
    for ct in contacts:
        email = ct.get("email","")
        confiance = ct.get("confiance","")
        phone = ct.get("phone","")
        if email and confiance not in ("faible","") and phone:
            continue
        if phase == "kaspr" and email and confiance not in ("faible",""):
            continue
        if not ct.get("prenom") or not ct.get("nom"):
            continue
        domaine_ct = ct.get("domaine","").strip()
        if not domaine_valide(domaine_ct):
            print(f"[FULLENRICH] Domaine toujours invalide pour {ct.get('prenom')} {ct.get('nom')} — ignoré")
            continue
        prenom_clean = nettoyer_prenom(ct["prenom"])
        to_enrich.append({
            "firstname":    prenom_clean,
            "lastname":     ct["nom"],
            "domain":       domaine_ct,
            "company_name": ct.get("societe",""),
            "enrich_fields": ["contact.emails", "contact.phones"],
            "custom": {"idx": str(ct.get("idx",0))}
        })
    if not to_enrich:
        return {"emails": emails_result}
    print(f"[FULLENRICH BATCH] {len(to_enrich)} contacts envoyés")
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://app.fullenrich.com/api/v1/contact/enrich/bulk",
                headers={"Authorization": f"Bearer {FULLENRICH_KEY}", "Content-Type": "application/json"},
                json={"name": "Enrichissement batch", "datas": to_enrich}
            )
            print(f"[FULLENRICH] Status lancement : {r.status_code}")
            if r.status_code not in (200, 201):
                print(f"[FULLENRICH ERROR] {r.text[:200]}")
                if r.status_code in (401, 402, 403):
                    _alert_api_down("FullEnrich", r.status_code, r.text[:200])
                return {"emails": emails_result}
            enrichment_id = r.json().get("enrichment_id") or r.json().get("id")
            if not enrichment_id:
                return {"emails": emails_result}
            print(f"[FULLENRICH] enrichment_id={enrichment_id}")
            for attempt in range(36):
                await asyncio.sleep(5)
                r2 = await c.get(
                    f"https://app.fullenrich.com/api/v1/contact/enrich/bulk/{enrichment_id}",
                    headers={"Authorization": f"Bearer {FULLENRICH_KEY}"}
                )
                if r2.status_code != 200:
                    continue
                result = r2.json()
                status = result.get("status","")
                print(f"[FULLENRICH] Polling {attempt+1}/36 — status={status}")
                if status == "FINISHED":
                    emails_par_idx = {}
                    for ct_result in result.get("datas",[]):
                        idx = ct_result.get("custom",{}).get("idx","-1")
                        contact_data = ct_result.get("contact",{})
                        email_val = ""
                        for e in contact_data.get("emails",[]):
                            val = e.get("value") or e.get("email") or ""
                            if val and "@" in val:
                                email_val = val
                                break
                        phone_val = ""
                        phones = contact_data.get("phones",[])
                        for p in phones:
                            val = p.get("number") or p.get("value") or p.get("phone") or ""
                            if val:
                                phone_val = val
                                break
                        if email_val or phone_val:
                            emails_par_idx[idx] = {"email": email_val, "phone": phone_val}
                    print(f"[FULLENRICH] {len(emails_par_idx)} contacts enrichis")
                    for k, v in emails_par_idx.items():
                        if k not in emails_result:
                            emails_result[k] = {"email": v["email"], "phone": v["phone"], "source": "+Fullenrich"}
                        else:
                            if v["email"] and not emails_result[k].get("email"):
                                emails_result[k]["email"] = v["email"]
                            if v["phone"] and not emails_result[k].get("phone"):
                                emails_result[k]["phone"] = v["phone"]
                    return {"emails": emails_result}
            print(f"[FULLENRICH] Timeout 180s")
            return {"emails": emails_result}
    except Exception as e:
        print(f"[FULLENRICH EXCEPTION] {e}")
        return {"emails": emails_result}
def generer_excel(rows: list) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    wb = Workbook()
    ws = wb.active
    ws.title = "Dirigeants enrichis"
    headers  = ['Organisation','Prénom','Nom','Titre','Email','Téléphone','LinkedIn','Domaine','Confiance','Source','Dans Pipedrive']
    col_map  = ['societe','prenom','nom_dg','titre','email','phone','linkedin','domaine','confiance','source','dans_pipedrive']
    thin     = Side(style='thin', color="e2e8f0")
    border   = Border(left=thin, right=thin, top=thin, bottom=thin)
    ws.merge_cells('A1:I1')
    c = ws['A1']
    c.value = "Enrichissement Dirigeants"
    c.font  = Font(name='Arial', bold=True, size=14, color="FFFFFF")
    c.fill  = PatternFill('solid', start_color="1e3a5f")
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[1].height = 32
    emails_count = len([r for r in rows if r.get('email')])
    ws.merge_cells('A2:I2')
    c = ws['A2']
    c.value = f"{len(rows)} contacts  |  {emails_count} emails trouvés  |  {len(rows)-emails_count} sans email"
    c.font  = Font(name='Arial', size=10, color="FFFFFF")
    c.fill  = PatternFill('solid', start_color="2563eb")
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[2].height = 22
    for col_idx, h in enumerate(headers, 1):
        c = ws.cell(row=3, column=col_idx, value=h)
        c.font = Font(name='Arial', bold=True, size=10, color="FFFFFF")
        c.fill = PatternFill('solid', start_color="2563eb")
        c.alignment = Alignment(horizontal='center', vertical='center')
        c.border = border
    ws.row_dimensions[3].height = 28
    src_colors = {'Pappers':'eff6ff','Claude':'f5f3ff','Pipedrive':'fef3c7','Kaspr':'e0f2fe','Fullenrich':'dcfce7'}
    rows = sorted(rows, key=lambda r: (r.get('societe','') or '').lower())
    org_list = []
    for r in rows:
        s = r.get('societe','')
        if s not in org_list:
            org_list.append(s)
    org_colors = {org: ("f0f7ff" if i % 2 == 0 else "FFFFFF") for i, org in enumerate(org_list)}
    for row_idx, row in enumerate(rows, 4):
        bg = org_colors.get(row.get('societe',''), "FFFFFF")
        ws.row_dimensions[row_idx].height = 18
        for col_idx, key in enumerate(col_map, 1):
            val = str(row.get(key, '') or '')
            c = ws.cell(row=row_idx, column=col_idx, value=val)
            c.font = Font(name='Arial', size=9)
            c.alignment = Alignment(vertical='center')
            c.border = border
            if key == 'email' and val:
                c.font = Font(name='Arial', size=9, color="2563eb", bold=True)
                c.fill = PatternFill('solid', start_color=bg)
            elif key == 'confiance':
                fills = {'haute':('dcfce7','166534'),'moyenne':('fef9c3','854d0e'),'faible':('fee2e2','991b1b')}
                if val in fills:
                    c.fill = PatternFill('solid', start_color=fills[val][0])
                    c.font = Font(name='Arial', size=9, color=fills[val][1], bold=True)
                else:
                    c.fill = PatternFill('solid', start_color=bg)
            elif key == 'source' and val:
                color = next((v for k,v in src_colors.items() if k in val), bg)
                c.fill = PatternFill('solid', start_color=color)
                c.font = Font(name='Arial', size=9, bold=True)
            elif key == 'dans_pipedrive' and val:
                c.fill = PatternFill('solid', start_color="fef3c7")
                c.font = Font(name='Arial', size=9, color="92400e", bold=True)
            else:
                c.fill = PatternFill('solid', start_color=bg)
    for i, w in enumerate([22,14,18,28,32,16,14,20,12,22,28], 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = 'A4'
    ws.auto_filter.ref = f"A3:I{len(rows)+3}"
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()
@app.post("/export_excel")
async def export_excel(request: Request):
    data = await request.json()
    rows = data.get("rows", [])
    if not rows:
        return {"ok": False}
    content = generer_excel(rows)
    return StreamingResponse(
        BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=enrichissement_dirigeants.xlsx"}
    )
@app.post("/send_csv")
async def send_csv(request: Request):
    data = await request.json()
    emails_raw  = data.get("emails", []) or ([data.get("email")] if data.get("email") else [])
    emails_dest = [e.strip() for e in emails_raw if e and "@" in e]
    rows        = data.get("rows", [])
    filename    = data.get("filename", "enrichissement_dirigeants.xlsx")
    if not emails_dest or not rows:
        return {"ok": False, "error": "Email(s) ou données manquants"}
    if not SMTP_USER or not SMTP_PASS:
        return {"ok": False, "error": "SMTP non configuré"}
    try:
        excel_content = generer_excel(rows)
        msg = MIMEMultipart()
        msg['From']    = SMTP_USER
        msg['To']      = ", ".join(emails_dest)
        msg['Subject'] = f"Enrichissement dirigeants — {len(rows)} contacts"
        emails_count = len([r for r in rows if r.get('email')])
        body = f"""Bonjour,
Votre enrichissement est terminé.
{len(rows)} contacts exportés dont {emails_count} emails trouvés.
Fichier Excel en pièce jointe.
Enrichisseur Dirigeants"""
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(excel_content)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(part)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, emails_dest, msg.as_string())
        print(f"[EMAIL] ✅ Excel envoyé à {', '.join(emails_dest)}")
        return {"ok": True}
    except Exception as e:
        print(f"[EMAIL ERROR] {e}")
        return {"ok": False, "error": str(e)}
