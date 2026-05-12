import os, json, asyncio, httpx, re, smtplib, csv, io, sqlite3, time
from io import BytesIO
from pathlib import Path
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
SMTP_HOST      = os.getenv("SMTP_HOST", "pro2.mail.ovh.net")
SMTP_PORT      = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER      = os.getenv("SMTP_USER", "")
SMTP_PASS      = os.getenv("SMTP_PASS", "")

# ────────────────────────────────────────────────────────────────────
# MODÈLES CLAUDE
# Sonnet : pour la recherche dirigeants (qualité critique, web search)
# Haiku  : pour les tâches d'extraction simple (URL LinkedIn, domaine)
# ────────────────────────────────────────────────────────────────────
MODEL_ENRICH = "claude-sonnet-4-6"          # qualité élevée requise
MODEL_EXTRACT = "claude-haiku-4-5-20251001" # extraction simple, ~4× moins cher

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

# ────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT MIS EN CACHE
# Bloc stable, identique pour toutes les sociétés → caché par Anthropic.
# Doit faire au moins 1024 tokens pour activer le cache : on a inclus
# des exemples few-shot qui améliorent en bonus la qualité des résultats.
# ────────────────────────────────────────────────────────────────────
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
Si une source dit "Paul Durand, CFO" sans précision temporelle et l'info est récente → l'inclure.

Exemple 4 — pas de dirigeant identifiable de manière fiable :
Output : {"contacts":[]}

Exemple 5 — distinguer dirigeant opérationnel vs représentant légal :
Si Pappers a renvoyé "Holding Patrimoniale Dupont, représentant Jean Dupont, président" → cherche en plus le directeur général opérationnel sur le site/LinkedIn.

Exemple 6 — emails à exclure systématiquement :
{"email":"ceo@gmail.com"} → INCORRECT, mettre "email": null à la place.

RÈGLE FINALE
Ne renvoie JAMAIS de texte hors du JSON. Pas de "Voici les contacts trouvés", pas de commentaire, pas de markdown. Le premier caractère de ta réponse doit être { et le dernier }."""


SYSTEM_LINKEDIN = """Tu es un assistant spécialisé dans la recherche d'URLs LinkedIn de dirigeants français.

CONSIGNES
- Cherche l'URL LinkedIn exacte de la personne demandée.
- Vérifie que la personne occupe bien un poste dans la société indiquée (pour éviter les homonymes).
- Format URL attendu : https://www.linkedin.com/in/prenom-nom-xxxxx/
- Si tu n'es pas certain à 100% que c'est la bonne personne → réponds exactement : NON
- Ne renvoie AUCUN autre texte que l'URL ou NON.

EXEMPLES
Input : Stanislas Niox-Chateau chez Doctolib
Output : https://www.linkedin.com/in/stanislas-niox-chateau-9728851a/

Input : Jean Dupont chez Société Inconnue (homonyme probable)
Output : NON"""


SYSTEM_DOMAINE = """Tu identifies le domaine officiel d'une société française.

CONSIGNES STRICTES
- Cherche sur le web le site officiel de la société.
- Renvoie UNIQUEMENT le domaine — un seul mot, sans aucun autre texte, sans phrase, sans explication.
- Format : nomdomaine.tld (exemple : exemple.com)
- INTERDIT : http://, https://, www., slash final, chemin, commentaire, "Je n'ai pas trouvé", etc.
- Si tu identifies un site qui semble être le site officiel → renvoie son domaine, même si tu n'es pas sûr à 100%.
- UNIQUEMENT si vraiment aucun site web n'existe → renvoie une chaîne vide (rien du tout).

EXEMPLES
Input : Doctolib → Output : doctolib.fr
Input : Mirakl → Output : mirakl.com
Input : Particeep → Output : particeep.com
Input : Novatim → Output : novatim.com
Input : Lightspeed (capital-investissement français) → Output : lightspeed.com
Input : Société totalement inexistante → Output : """


# ────────────────────────────────────────────────────────────────────
# CACHE DISQUE PAR SIREN — sqlite, zéro dépendance externe
# Persiste les résultats Claude+web 60 jours pour éviter de re-payer
# une recherche déjà faite sur la même société.
# Sur Render : attacher un Render Disk monté sur /var/data (1 $/mois)
# Sinon : fallback /tmp (perdu au redéploiement, OK pour test local)
# ────────────────────────────────────────────────────────────────────
CACHE_DIR = "/var/data" if os.path.isdir("/var/data") else "/tmp"
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
CACHE_DB = os.path.join(CACHE_DIR, "enrich_cache.db")
CACHE_TTL_DAYS = 60       # contacts dirigeants : 60 jours
DOMAINE_TTL_DAYS = 90     # domaines : 90 jours (changent quasi jamais)

def _init_cache():
    with sqlite3.connect(CACHE_DB) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS claude_cache (
            siren TEXT PRIMARY KEY,
            contacts_json TEXT NOT NULL,
            cached_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS domaine_cache (
            siren TEXT PRIMARY KEY,
            domaine TEXT NOT NULL,
            cached_at INTEGER NOT NULL
        )""")
        conn.commit()
_init_cache()
print(f"[CACHE] Init OK → {CACHE_DB}")


def _normalize_siren(siren) -> str:
    return re.sub(r'\D', '', str(siren or ''))[:9]


def cache_get(siren: str):
    """Renvoie la liste de contacts en cache si fraîche (< TTL), sinon None."""
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9:
        return None
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            row = conn.execute(
                "SELECT contacts_json, cached_at FROM claude_cache WHERE siren = ?",
                (siren,)
            ).fetchone()
            if row and (time.time() - row[1]) < CACHE_TTL_DAYS * 86400:
                return json.loads(row[0])
    except Exception as e:
        print(f"[CACHE GET ERROR] {siren}: {e}")
    return None


def cache_set(siren: str, contacts: list) -> None:
    """Sauve les contacts en cache pour cette société."""
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9 or not contacts:
        return
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO claude_cache (siren, contacts_json, cached_at) VALUES (?, ?, ?)",
                (siren, json.dumps(contacts, ensure_ascii=False), int(time.time()))
            )
            conn.commit()
    except Exception as e:
        print(f"[CACHE SET ERROR] {siren}: {e}")


def cache_stats() -> dict:
    """Stats utiles pour suivi."""
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            total = conn.execute("SELECT COUNT(*) FROM claude_cache").fetchone()[0]
            cutoff = int(time.time()) - CACHE_TTL_DAYS * 86400
            valid = conn.execute("SELECT COUNT(*) FROM claude_cache WHERE cached_at >= ?", (cutoff,)).fetchone()[0]
            dom_total = conn.execute("SELECT COUNT(*) FROM domaine_cache").fetchone()[0]
            dom_cutoff = int(time.time()) - DOMAINE_TTL_DAYS * 86400
            dom_valid = conn.execute("SELECT COUNT(*) FROM domaine_cache WHERE cached_at >= ?", (dom_cutoff,)).fetchone()[0]
        return {
            "contacts": {"total_societes": total, "valides_dans_ttl": valid, "ttl_jours": CACHE_TTL_DAYS},
            "domaines": {"total_societes": dom_total, "valides_dans_ttl": dom_valid, "ttl_jours": DOMAINE_TTL_DAYS},
        }
    except Exception as e:
        return {"error": str(e)}


def domaine_cache_get(siren: str):
    """Renvoie le domaine en cache (peut être vide si on a déjà tenté sans succès)."""
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9:
        return None
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            row = conn.execute(
                "SELECT domaine, cached_at FROM domaine_cache WHERE siren = ?",
                (siren,)
            ).fetchone()
            if row and (time.time() - row[1]) < DOMAINE_TTL_DAYS * 86400:
                return row[0]  # peut être chaîne vide
    except Exception as e:
        print(f"[DOMAINE CACHE GET ERROR] {siren}: {e}")
    return None


def domaine_cache_set(siren: str, domaine: str) -> None:
    """Sauve le domaine (même vide pour éviter de re-tenter inutilement)."""
    siren = _normalize_siren(siren)
    if not siren or len(siren) != 9:
        return
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO domaine_cache (siren, domaine, cached_at) VALUES (?, ?, ?)",
                (siren, domaine or "", int(time.time()))
            )
            conn.commit()
    except Exception as e:
        print(f"[DOMAINE CACHE SET ERROR] {siren}: {e}")


def _domaine_candidates(nom: str) -> list:
    """Génère des candidats de domaine depuis le nom de société."""
    if not nom:
        return []
    import unicodedata
    base = nom.lower().strip()
    base = unicodedata.normalize("NFD", base)
    base = "".join(c for c in base if unicodedata.category(c) != "Mn")
    base = re.sub(r"[^\w\s\-]", "", base)
    # Enlever suffixes juridiques courants
    for suffix in [" sas", " sa", " sarl", " eurl", " sci", " snc", " selarl",
                   " france", " group", " groupe", " holding", " holdings", " et associes",
                   " et fils", " international"]:
        if base.endswith(suffix):
            base = base[:-len(suffix)].strip()
    base = re.sub(r"\s+", " ", base).strip()
    if not base or len(base) < 2:
        return []
    base_clean = base.replace(" ", "").replace("-", "")
    base_dash  = base.replace(" ", "-")
    base_first = base.split()[0] if " " in base else base
    candidates = []
    seen = set()
    # Essayer dans cet ordre : .com → .fr → .io → .co → .eu
    for variant in [base_clean, base_dash, base_first]:
        if variant and len(variant) >= 3:
            for tld in [".com", ".fr", ".io", ".co", ".eu"]:
                cand = f"{variant}{tld}"
                if cand not in seen:
                    candidates.append(cand)
                    seen.add(cand)
    return candidates


async def _verifier_domaine_async(domaine: str, timeout: float = 4.0) -> bool:
    """Vérifie que le domaine répond à du HTTPS (HEAD ou GET)."""
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; EnrichBot/1.0)"}
        ) as c:
            try:
                r = await c.head(f"https://{domaine}")
                if r.status_code < 400:
                    return True
            except Exception:
                pass
            r = await c.get(f"https://{domaine}")
            return r.status_code < 400
    except Exception:
        return False


def _extraire_domaine_du_texte(texte: str) -> str:
    """Extrait un domaine valide du texte (pattern xxx.tld), même si Claude a ajouté du blabla."""
    if not texte:
        return ""
    # Chercher un pattern domaine
    matches = re.findall(r'\b([a-z0-9](?:[a-z0-9\-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?)+)\b', texte.lower())
    for m in matches:
        # Filtrer les faux positifs (ex: "1.0", "v2.0", chemins)
        if "." in m and len(m) > 4 and not m.split(".")[0].isdigit():
            tld = m.split(".")[-1]
            if len(tld) >= 2 and tld.isalpha():
                return nettoyer_domaine(m)
    return ""


def log_usage(label: str, societe: str, response_json: dict) -> None:
    """Log la consommation tokens d'un appel Claude pour suivi des coûts."""
    usage = response_json.get("usage", {}) or {}
    print(f"[COST] {label} | {societe} | "
          f"in={usage.get('input_tokens', 0)} | "
          f"cache_read={usage.get('cache_read_input_tokens', 0)} | "
          f"cache_write={usage.get('cache_creation_input_tokens', 0)} | "
          f"out={usage.get('output_tokens', 0)}")


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


@app.get("/health")
async def health():
    return {
        "ok": True,
        "anthropic_key": bool(ANTHROPIC_KEY),
        "pappers_key": bool(PAPPERS_KEY),
        "fullenrich_key": bool(FULLENRICH_KEY),
        "pipedrive_key": bool(PIPEDRIVE_KEY),
        "kaspr_key": bool(KASPR_KEY),
        "cache": cache_stats(),
    }


@app.get("/cache/stats")
async def cache_stats_route():
    """Affiche le nombre de sociétés en cache (utile pour suivre l'effet de l'optim)."""
    return cache_stats()


@app.post("/cache/clear")
async def cache_clear_route(request: Request):
    """Vide le cache. À utiliser uniquement si tu veux forcer un re-scan complet.
    Pour éviter les accidents : exige un body JSON {\"confirm\": \"oui\"}."""
    data = await request.json()
    if data.get("confirm") != "oui":
        return {"ok": False, "error": "Ajoute {\"confirm\": \"oui\"} dans le body pour confirmer"}
    try:
        with sqlite3.connect(CACHE_DB) as conn:
            conn.execute("DELETE FROM claude_cache")
            conn.commit()
        return {"ok": True, "message": "Cache vidé"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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
    """Cherche l'URL LinkedIn du dirigeant via Claude+web (Haiku, max_uses:1)."""
    if not ANTHROPIC_KEY:
        return ""
    prenom = nettoyer_prenom(prenom)
    if not prenom or not nom:
        return ""
    try:
        user_prompt = f"""Trouve l'URL LinkedIn de :
Prénom : {prenom}
Nom : {nom}
Société : {societe}"""
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                json={
                    "model": MODEL_EXTRACT,  # Haiku — extraction simple
                    "max_tokens": 200,
                    "system": [
                        {
                            "type": "text",
                            "text": SYSTEM_LINKEDIN,
                            "cache_control": {"type": "ephemeral"}
                        }
                    ],
                    "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                    "messages": [{"role": "user", "content": user_prompt}]
                }
            )
            print(f"[LINKEDIN] Status {r.status_code} pour {prenom} {nom}")
            if r.status_code == 200:
                response_json = r.json()
                log_usage("LINKEDIN", f"{prenom} {nom}", response_json)
                all_text = " ".join(b.get("text","") for b in response_json.get("content",[]) if b.get("type")=="text").strip()
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
            else:
                print(f"[KASPR] Erreur {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"[KASPR ERROR] {prenom} {nom}: {e}")
    return ""


async def corriger_domaine(siren: str, societe: str) -> str:
    """Trouve le domaine officiel via : cache → Pappers → heuristique HTTP → Claude+web.
    Met en cache le résultat (90 jours) pour ne plus re-tenter."""

    # ── 1. CACHE ──────────────────────────────────────────────
    cached = domaine_cache_get(siren)
    if cached is not None:
        if cached:
            print(f"[DOMAINE CACHE HIT] {societe} → {cached}")
        else:
            print(f"[DOMAINE CACHE HIT] {societe} → vide (déjà tenté)")
        return cached

    # ── 2. PAPPERS (SIREN puis recherche par nom) ─────────────
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
                            domaine_cache_set(siren, domaine)
                            return domaine
                r2 = await c.get("https://api.pappers.fr/v2/recherche",
                    params={"api_token": PAPPERS_KEY, "q": societe, "par_page": 1})
                if r2.status_code == 200:
                    resultats = r2.json().get("resultats", [])
                    if resultats:
                        domaine = nettoyer_domaine(resultats[0].get("domaine_url","") or resultats[0].get("site_web",""))
                        if domaine_valide(domaine):
                            print(f"[DOMAINE FIX] Pappers nom → {domaine} pour {societe}")
                            domaine_cache_set(siren, domaine)
                            return domaine
        except Exception as e:
            print(f"[DOMAINE FIX ERROR Pappers] {e}")

    # ── 3. HEURISTIQUE : générer candidats + vérifier HTTP ────
    # Gratuit, rapide, et ça résout la majorité des cas
    candidats = _domaine_candidates(societe)
    if candidats:
        print(f"[DOMAINE HEURISTIQUE] {societe} → essais : {', '.join(candidats[:5])}...")
        # Tester en parallèle pour aller vite (5 candidats à la fois)
        for chunk_start in range(0, len(candidats), 5):
            chunk = candidats[chunk_start:chunk_start+5]
            results = await asyncio.gather(
                *(_verifier_domaine_async(c) for c in chunk),
                return_exceptions=True
            )
            for cand, ok in zip(chunk, results):
                if ok is True:
                    print(f"[DOMAINE FIX] Heuristique HTTP → {cand} pour {societe}")
                    domaine_cache_set(siren, cand)
                    return cand

    # ── 4. CLAUDE + WEB (filet final, Sonnet pour fiabilité) ──
    if ANTHROPIC_KEY:
        try:
            user_prompt = f"Société française : {societe}\nDonne-moi le domaine de son site officiel."
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                    json={
                        "model": MODEL_ENRICH,  # Sonnet : domaine est critique pour Marie
                        "max_tokens": 80,
                        "system": [
                            {
                                "type": "text",
                                "text": SYSTEM_DOMAINE,
                                "cache_control": {"type": "ephemeral"}
                            }
                        ],
                        "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 2}],
                        "messages": [{"role": "user", "content": user_prompt}]
                    }
                )
                if r.status_code == 200:
                    response_json = r.json()
                    log_usage("DOMAINE", societe, response_json)
                    all_text = " ".join(b.get("text","") for b in response_json.get("content",[]) if b.get("type")=="text").strip()
                    # Parser robuste : extraire un domaine du texte même si Claude a ajouté du blabla
                    domaine = _extraire_domaine_du_texte(all_text)
                    if domaine_valide(domaine):
                        # Vérifier que le domaine répond vraiment (évite les hallucinations)
                        if await _verifier_domaine_async(domaine):
                            print(f"[DOMAINE FIX] Claude+web → {domaine} pour {societe}")
                            domaine_cache_set(siren, domaine)
                            return domaine
                        else:
                            print(f"[DOMAINE FIX] Claude a proposé {domaine} mais le site ne répond pas")
        except Exception as e:
            print(f"[DOMAINE FIX ERROR Claude] {e}")

    # ── ÉCHEC : on cache le vide pour ne pas re-tenter ────────
    print(f"[DOMAINE FIX] ❌ Aucun domaine trouvé pour {societe}")
    domaine_cache_set(siren, "")
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

    # ÉTAPE 1 : Pappers
    if PAPPERS_KEY:
        if domaine_valide(domaine):
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "site_internet": domaine})
                    if r.status_code == 200:
                        pappers_data = r.json()
                        print(f"[PAPPERS] Trouvé par domaine")
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
            except Exception as e:
                print(f"[PAPPERS ERROR nom] {e}")
        if not pappers_data and siren:
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "siren": siren})
                    if r.status_code == 200:
                        pappers_data = r.json()
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

    # Pré-remplir le contact connu depuis le fichier source
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

    # ÉTAPE 2 : Claude + web_search (recherche CEO/CFO opérationnels)
    # Règle métier : on saute Claude+web si :
    #   1) la société est déjà dans Pipedrive (contacts récupérés ci-dessus)
    #   2) on a déjà un résultat en cache pour cette société (SIREN, TTL 60j)
    claude_contacts = []
    skip_claude_web = False
    skip_reason = ""

    if pipedrive_org_contacts:
        skip_claude_web = True
        skip_reason = f"déjà dans Pipedrive ({len(pipedrive_org_contacts)} contacts)"
        print(f"[SKIP CLAUDE+WEB] {nom} — {skip_reason}")

    if not skip_claude_web:
        cached = cache_get(siren)
        if cached is not None:
            claude_contacts = cached
            skip_claude_web = True
            skip_reason = f"cache hit SIREN {siren}"
            print(f"[CACHE HIT] {nom} → {len(cached)} contacts (économie d'1 appel Claude+web)")

    if not skip_claude_web and ANTHROPIC_KEY:
        noms_deja = [f"{c['prenom']} {c['nom']}".strip() for c in pappers_contacts]
        exclusion = f"\nDirigeants déjà connus à ne PAS inclure : {', '.join(noms_deja)}" if noms_deja else ""
        contexte_fondateurs = f"\nFondateurs connus : {fondateurs}" if fondateurs else ""
        user_prompt = f"""Société française à enrichir :
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
                            "model": MODEL_ENRICH,  # Sonnet — qualité requise
                            "max_tokens": 1000,
                            "system": [
                                {
                                    "type": "text",
                                    "text": SYSTEM_ENRICH,
                                    "cache_control": {"type": "ephemeral"}
                                }
                            ],
                            "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                            "messages": [{"role": "user", "content": user_prompt}]
                        }
                    )
                    print(f"[CLAUDE] Status {r.status_code} pour {nom}")
                    if r.status_code in (429, 529):
                        await asyncio.sleep(delays[attempt])
                        continue
                    if r.status_code == 200:
                        response_json = r.json()
                        log_usage("ENRICH", nom, response_json)
                        all_text = " ".join(b.get("text","") for b in response_json.get("content",[]) if b.get("type")=="text")
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
                            # Mettre en cache pour 60 jours (évite de re-payer si scan futur sur la même société)
                            if siren and claude_contacts:
                                cache_set(siren, claude_contacts)
                                print(f"[CACHE SET] {nom} (SIREN {siren}) → {len(claude_contacts)} contacts mis en cache")
                        break
                    else:
                        print(f"[CLAUDE ERROR DETAIL] {r.status_code}: {r.text[:300]}")
                        break
            except Exception as e:
                print(f"[CLAUDE EXCEPTION] {e}")
                if attempt < 2:
                    await asyncio.sleep(delays[attempt])

    # Domaine fallback si toujours manquant
    if not domaine_valide(domaine) and siren:
        domaine = await corriger_domaine(siren, nom)

    # Propager le domaine sur tous les contacts
    for ct in pappers_contacts + claude_contacts + pipedrive_org_contacts:
        if not ct.get("domaine"):
            ct["domaine"] = domaine

    # ÉTAPE 3 : Pipedrive check par personne (pour les contacts qui n'ont pas d'email)
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

    # ─── FUSION ──────────────────────────────────────────────────
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


# -------------------------------------------------------
# ROUTE CLAUDE ONLY (Phase 2 standalone)
# -------------------------------------------------------
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

    # Vérifier le cache avant de payer un appel
    cached = cache_get(siren)
    if cached is not None:
        print(f"[CACHE HIT P2] {nom} (SIREN {siren}) → {len(cached)} contacts depuis cache")
        return {"contacts": cached[:max_contacts]}

    contexte_fondateurs = f"\nFondateurs connus : {fondateurs}" if fondateurs else ""
    user_prompt = f"""Société française à enrichir :
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
                        "model": MODEL_ENRICH,  # Sonnet — qualité requise
                        "max_tokens": 1000,
                        "system": [
                            {
                                "type": "text",
                                "text": SYSTEM_ENRICH,
                                "cache_control": {"type": "ephemeral"}
                            }
                        ],
                        "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                        "messages": [{"role": "user", "content": user_prompt}]
                    }
                )
                print(f"[CLAUDE PHASE2] Status {r.status_code} pour {nom}")
                if r.status_code in (429, 529):
                    await asyncio.sleep(delays[attempt])
                    continue
                if r.status_code == 200:
                    response_json = r.json()
                    log_usage("ENRICH-P2", nom, response_json)
                    all_text = " ".join(b.get("text","") for b in response_json.get("content",[]) if b.get("type")=="text")
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
                        # Mettre en cache pour 60 jours
                        if siren and contacts:
                            cache_set(siren, contacts)
                            print(f"[CACHE SET P2] {nom} (SIREN {siren}) → {len(contacts)} contacts mis en cache")
                        return {"contacts": contacts}
                else:
                    print(f"[CLAUDE PHASE2 ERROR] {r.status_code}: {r.text[:200]}")
                break
        except Exception as e:
            print(f"[CLAUDE PHASE2 EXCEPTION] {e}")
            if attempt < 2:
                await asyncio.sleep(delays[attempt])
    return {"contacts": []}


# -------------------------------------------------------
# ROUTE PIPEDRIVE CHECK standalone
# -------------------------------------------------------
@app.post("/check_pipedrive")
async def check_pipedrive_route(request: Request):
    data = await request.json()
    prenom = data.get("prenom","")
    nom    = data.get("nom","")
    result = await check_pipedrive(prenom, nom)
    return {"email": result.get("email",""), "phone": result.get("phone","")}


# -------------------------------------------------------
# ROUTE PASSE 2 : Kaspr + LinkedIn + Fullenrich batch
# -------------------------------------------------------
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


# -------------------------------------------------------
# HELPER : Génère un Excel mis en forme en mémoire
# -------------------------------------------------------
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


# -------------------------------------------------------
# ROUTE EXPORT EXCEL
# -------------------------------------------------------
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


# -------------------------------------------------------
# ROUTE ENVOI EMAIL CSV
# -------------------------------------------------------
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
        # Pièce jointe Excel — MIME type officiel pour qu'Apple Mail
        # reconnaisse le .xlsx (icône Excel + double-clic direct)
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
