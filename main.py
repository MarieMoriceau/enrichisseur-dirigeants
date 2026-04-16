import os, json, asyncio, httpx, re, smtplib, csv, io
from io import BytesIO
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
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
        # Enlever accents, tirets, points, espaces multiples
        s = s.lower().strip()
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
        s = re.sub(r"[.\-_/]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    a = normaliser(nom_csv)
    b = normaliser(nom_pappers)
    # Correspondance exacte après normalisation
    if a == b:
        return True
    # Au moins 1 mot significatif en commun
    mots_a = set(w for w in a.split() if len(w) > 2)
    mots_b = set(w for w in b.split() if len(w) > 2)
    if not mots_a:
        return True
    return len(mots_a & mots_b) > 0

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health():
    return {
        "ok": True,
        "anthropic_key": bool(ANTHROPIC_KEY),
        "pappers_key": bool(PAPPERS_KEY),
        "fullenrich_key": bool(FULLENRICH_KEY),
        "pipedrive_key": bool(PIPEDRIVE_KEY),
        "kaspr_key": bool(KASPR_KEY),
    }

async def check_pipedrive(prenom: str, nom: str) -> str:
    if not PIPEDRIVE_KEY or not prenom or not nom:
        return ""
    terme = f"{prenom} {nom}".strip()
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                "https://api.pipedrive.com/v1/persons/search",
                params={"term": terme, "fields": "name,email", "exact_match": "false", "limit": 5, "api_token": PIPEDRIVE_KEY}
            )
            if r.status_code == 200:
                items = r.json().get("data", {}).get("items", [])
                for item in items:
                    person = item.get("item", {})
                    person_name = person.get("name", "").lower()
                    if nom.lower() in person_name or prenom.lower() in person_name:
                        emails = person.get("emails", [])
                        if emails:
                            email = emails[0] if isinstance(emails[0], str) else emails[0].get("value", "")
                            if email and "@" in email:
                                print(f"[PIPEDRIVE] ✅ {terme} → {email}")
                                return email
    except Exception as e:
        print(f"[PIPEDRIVE ERROR] {terme}: {e}")
    return ""

async def trouver_linkedin(prenom: str, nom: str, societe: str) -> str:
    """Cherche l'URL LinkedIn du dirigeant via Claude+web (max_uses:1)."""
    if not ANTHROPIC_KEY:
        return ""
    # Nettoyer les prénoms composés Pappers ex: "Emmanuel, Roger" → "Emmanuel"
    prenom = prenom.split(",")[0].strip()
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
                    "model": "claude-sonnet-4-6",
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
                    "dataToGet": ["workEmail"]  # B2B uniquement = crédits illimités
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
    """Tente de trouver le vrai domaine via Pappers (SIREN ou nom) puis Claude."""
    if PAPPERS_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                # Tentative 1 : par SIREN
                if siren:
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "siren": siren})
                    if r.status_code == 200:
                        d = r.json()
                        domaine = nettoyer_domaine(d.get("domaine_url","") or d.get("site_web",""))
                        if domaine_valide(domaine):
                            print(f"[DOMAINE FIX] Pappers SIREN → {domaine} pour {societe}")
                            return domaine
                # Tentative 2 : par nom si SIREN ne donne pas de domaine
                r2 = await c.get("https://api.pappers.fr/v2/recherche",
                    params={"api_token": PAPPERS_KEY, "q": societe, "par_page": 1})
                if r2.status_code == 200:
                    resultats = r2.json().get("resultats", [])
                    if resultats:
                        domaine = nettoyer_domaine(resultats[0].get("domaine_url","") or resultats[0].get("site_web",""))
                        if domaine_valide(domaine):
                            print(f"[DOMAINE FIX] Pappers nom → {domaine} pour {societe}")
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
                        "model": "claude-sonnet-4-6",
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
                        return domaine
        except Exception as e:
            print(f"[DOMAINE FIX ERROR Claude] {e}")
    return ""

# -------------------------------------------------------
# ROUTE PASSE 1 : Pappers + Claude + Pipedrive
# -------------------------------------------------------
@app.post("/enrich_one")
async def enrich_one(request: Request):
    data = await request.json()
    nom            = data.get("nom", "")
    siren          = re.sub(r'\D', '', data.get("siren", ""))[:9]  # nettoie "331191825-00099" → "331191825"
    domaine        = nettoyer_domaine(data.get("domaine", ""))
    org_id         = data.get("org_id", "")
    fondateurs     = data.get("fondateurs", "")
    contact_prenom = data.get("contact_prenom", "")
    contact_nom    = data.get("contact_nom", "")
    contact_titre  = data.get("contact_titre", "")
    code_postal    = data.get("code_postal", "")
    ville          = data.get("ville", "")

    print(f"[START] {nom} | domaine={domaine} | siren={siren}")

    # ── CHECK PIPEDRIVE ORGANISATION (avant tout le process) ──────
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
                            # Récupérer tous les contacts de cette organisation
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
                                        contacts_pipe.append({
                                            "prenom": prenom_p, "nom": nom_p,
                                            "titre": titre_p, "email": email_p,
                                            "confiance": "haute" if email_p else "",
                                            "source": "Pipedrive",
                                            "dans_pipedrive": "oui"
                                        })
                                    print(f"[PIPEDRIVE ORG] {len(contacts_pipe)} contacts récupérés")
                            except Exception as e2:
                                print(f"[PIPEDRIVE ORG CONTACTS ERROR] {e2}")

                            if not contacts_pipe:
                                contacts_pipe = [{"prenom":"","nom":"","titre":"","email":"",
                                                  "confiance":"","source":"Pipedrive","dans_pipedrive":"oui"}]
                            results = []
                            for ct in contacts_pipe:
                                results.append({
                                    "org_id": org_id, "societe": nom, "siren": siren,
                                    "domaine": domaine,
                                    "prenom": ct["prenom"], "nom_dg": ct["nom"],
                                    "titre": ct["titre"], "email": ct["email"],
                                    "linkedin": "", "confiance": ct["confiance"],
                                    "source": ct["source"],
                                    "dans_pipedrive": ct["dans_pipedrive"]
                                })
                            return {"results": results}
        except Exception as e:
            print(f"[PIPEDRIVE ORG ERROR] {e}")

    pappers_contacts = []
    pappers_data = None

    # ÉTAPE 1 : Pappers
    if PAPPERS_KEY:
        # Tentative 1 : par domaine
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

        # Tentative 2 : par nom
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

        # Tentative 3 : par SIREN
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

            # Récupérer le domaine depuis Pappers si manquant
            if not domaine_valide(domaine):
                domaine_pappers = nettoyer_domaine(
                    pappers_data.get("domaine_url","") or pappers_data.get("site_web","")
                )
                if domaine_valide(domaine_pappers):
                    domaine = domaine_pappers
                    print(f"[PAPPERS] Domaine récupéré : {domaine}")

            # Vérifier que c'est bien la bonne société
            nom_pappers = pappers_data.get("nom_entreprise","") or pappers_data.get("denomination","")
            if nom_pappers and not noms_similaires(nom, nom_pappers):
                print(f"[PAPPERS] ⚠️ Mauvaise société : '{nom_pappers}' pour '{nom}' — ignoré")
                pappers_data = None
                siren = ""
                domaine = nettoyer_domaine(data.get("domaine",""))

        if pappers_data:
            # Société radiée ?
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
                prenom_clean = prenom_raw.split(",")[0].strip()  # "Florian, Paul, Robert" → "Florian"
                pappers_contacts.append({
                    "prenom": prenom_clean,
                    "nom":    rep.get("nom",""),
                    "titre":  titre,
                    "email":  "",
                    "confiance": "",
                    "source": "Pappers"
                })
            print(f"[PAPPERS] {len(pappers_contacts)} représentants actifs | domaine={domaine}")

    # Pré-remplir le contact connu depuis le fichier source (ex: fichier occupants)
    # Dédup : on ne l'ajoute que s'il n'est pas déjà dans pappers_contacts
    if contact_prenom and contact_nom:
        contact_prenom_clean = contact_prenom.split(",")[0].strip()
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
                "email":  "",
                "confiance": "",
                "source": "Fichier source"
            })
            print(f"[SOURCE] Contact pré-rempli : {contact_prenom_clean} {contact_nom} ({contact_titre})")
        else:
            print(f"[SOURCE] Contact déjà dans Pappers : {contact_prenom_clean} {contact_nom} — skip")

    # ÉTAPE 2 : Claude + web_search
    claude_contacts = []
    if ANTHROPIC_KEY:
        noms_deja = [f"{c['prenom']} {c['nom']}".strip() for c in pappers_contacts]
        exclusion = f"\nNe pas inclure : {', '.join(noms_deja)}" if noms_deja else ""
        # Ajouter les fondateurs connus du CSV comme contexte
        contexte_fondateurs = f"\nFondateurs connus : {fondateurs}" if fondateurs else ""

        prompt = f"""Recherche sur le web les dirigeants ACTUELS et leurs emails pour cette société française :
Nom: {nom}{chr(10)+"Site: "+domaine if domaine_valide(domaine) else ""}{chr(10)+"SIREN: "+siren if siren else ""}{contexte_fondateurs}{exclusion}

Cherche : CEO, DG, CFO, DAF, CTO, COO, CMO, DRH, Président, Gérant, Partners, Associés, Fondateurs.
- Dirigeants en poste UNIQUEMENT (pas "ancien", "ex-")
- Emails professionnels uniquement (pas gmail/hotmail/yahoo)

Réponds UNIQUEMENT avec ce JSON :
{{"contacts":[{{"prenom":"...","nom":"...","titre":"...","email":"...ou null","confiance_email":"haute|moyenne|faible"}}]}}"""

        delays = [10, 25, 45]
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=90) as c:
                    print(f"[CLAUDE] Tentative {attempt+1}/3 pour {nom}")
                    r = await c.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                        json={
                            "model": "claude-sonnet-4-6",
                            "max_tokens": 1000,
                            "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
                            "messages": [{"role": "user", "content": prompt}]
                        }
                    )
                    print(f"[CLAUDE] Status {r.status_code} pour {nom}")
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
                        break
                    else:
                        print(f"[CLAUDE ERROR DETAIL] {r.status_code}: {r.text[:300]}")
                        break
            except Exception as e:
                print(f"[CLAUDE EXCEPTION] {e}")
                if attempt < 2:
                    await asyncio.sleep(delays[attempt])

    tous_contacts = pappers_contacts + claude_contacts

    # Si domaine toujours manquant, on tente de le corriger maintenant
    if not domaine_valide(domaine) and siren:
        domaine = await corriger_domaine(siren, nom)

    # Mettre à jour le domaine sur tous les contacts
    for ct in tous_contacts:
        if not ct.get("domaine"):
            ct["domaine"] = domaine

    # ÉTAPE 3 : Pipedrive check
    if PIPEDRIVE_KEY:
        for ct in tous_contacts:
            if ct.get("email"):
                continue
            email_pd = await check_pipedrive(ct.get("prenom",""), ct.get("nom",""))
            if email_pd:
                ct["email"] = email_pd
                ct["confiance_email"] = "haute"
                ct["source"] = ct.get("source","") + "+Pipedrive"
                ct["dans_pipedrive"] = "oui"

    # ÉTAPE 4 : Kaspr + LinkedIn → géré en batch dans /enrich_emails après la Passe 1
    # (évite la surcharge Claude pendant la Passe 1)

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

    contexte_fondateurs = f"\nFondateurs connus : {fondateurs}" if fondateurs else ""
    prompt = f"""Recherche sur le web les dirigeants ACTUELS et leurs emails pour cette société française :
Nom: {nom}{chr(10)+"Site: "+domaine if domaine_valide(domaine) else ""}{chr(10)+"SIREN: "+siren if siren else ""}{contexte_fondateurs}

Cherche : CEO, DG, CFO, DAF, CTO, COO, CMO, DRH, Président, Gérant, Partners, Associés, Fondateurs.
- Dirigeants en poste UNIQUEMENT (pas "ancien", "ex-")
- Emails professionnels uniquement (pas gmail/hotmail/yahoo)
- Retourne AU MAXIMUM {max_contacts} contact(s)

Réponds UNIQUEMENT avec ce JSON :
{{"contacts":[{{"prenom":"...","nom":"...","titre":"...","email":"...ou null","confiance_email":"haute|moyenne|faible"}}]}}"""

    delays = [10, 25, 45]
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                print(f"[CLAUDE PHASE2] Tentative {attempt+1}/3 pour {nom}")
                r = await c.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05", "content-type": "application/json"},
                    json={
                        "model": "claude-sonnet-4-6",
                        "max_tokens": 1000,
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

# -------------------------------------------------------
# ROUTE PIPEDRIVE CHECK standalone
# -------------------------------------------------------
@app.post("/check_pipedrive")
async def check_pipedrive_route(request: Request):
    data = await request.json()
    prenom = data.get("prenom","")
    nom    = data.get("nom","")
    email  = await check_pipedrive(prenom, nom)
    return {"email": email}

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

    # -------------------------------------------------------
    # KASPR : cherche LinkedIn puis email pour chaque contact
    # Déduplication : on skip si un contact avec même nom a déjà un email
    # -------------------------------------------------------
    emails_par_nom = {}  # cache "prenom nom" → email déjà trouvé
    if KASPR_KEY:
        for ct in contacts:
            email = ct.get("email","")
            if email and "*" not in email:
                emails_par_nom[f"{ct.get('prenom','')} {ct.get('nom','')}".lower().strip()] = email
                continue
            # Vérifier doublon
            prenom_clean = ct.get("prenom","").split(",")[0].strip()
            cle = f"{prenom_clean} {ct.get('nom','')}".lower().strip()
            if cle in emails_par_nom:
                idx = str(ct.get("idx",0))
                emails_result[idx] = {"email": emails_par_nom[cle], "source": "+dedup"}
                print(f"[DEDUP] {cle} → email déjà trouvé, skip Kaspr")
                continue
            # Nettoyer prénom composé Pappers ex: "Emmanuel, Roger" → "Emmanuel"
            prenom = ct.get("prenom","").split(",")[0].strip()
            nom_ct = ct.get("nom","")
            societe_ct = ct.get("societe","")
            idx = str(ct.get("idx",0))
            if not prenom or not nom_ct:
                continue
            print(f"[KASPR] Recherche LinkedIn pour {prenom} {nom_ct}")
            linkedin_url = await trouver_linkedin(prenom, nom_ct, societe_ct)
            if linkedin_url:
                email_kaspr = await kaspr_email(prenom, nom_ct, linkedin_url)
                if email_kaspr:
                    ct["email"] = email_kaspr
                    ct["linkedin"] = linkedin_url
                    ct["source_kaspr"] = True
                    emails_result[idx] = {"email": email_kaspr, "linkedin": linkedin_url, "source": "+Kaspr"}
                    print(f"[KASPR] ✅ {prenom} {nom_ct} → {email_kaspr}")

    # Corriger les domaines invalides avant Fullenrich (1 seul appel par société)
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
    for ct in contacts:
        email = ct.get("email","")
        confiance = ct.get("confiance","")
        if email and confiance not in ("faible",""):
            continue
        if not ct.get("prenom") or not ct.get("nom"):
            continue
        domaine_ct = ct.get("domaine","").strip()
        if not domaine_valide(domaine_ct):
            print(f"[FULLENRICH] Domaine toujours invalide pour {ct.get('prenom')} {ct.get('nom')} — ignoré")
            continue
        prenom_clean = ct["prenom"].split(",")[0].strip()  # "Emmanuel, Roger" → "Emmanuel"
        to_enrich.append({
            "firstname":    prenom_clean,
            "lastname":     ct["nom"],
            "domain":       domaine_ct,
            "company_name": ct.get("societe",""),
            "enrich_fields": ["contact.emails"],
            "custom": {"idx": str(ct.get("idx",0))}
        })

    if not to_enrich:
        return {"emails": {}}

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
                return {"emails": {}}

            enrichment_id = r.json().get("enrichment_id") or r.json().get("id")
            if not enrichment_id:
                return {"emails": {}}

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
                        emails = ct_result.get("contact",{}).get("emails",[])
                        if emails:
                            for e in emails:
                                val = e.get("value") or e.get("email") or ""
                                if val and "@" in val:
                                    emails_par_idx[idx] = val
                                    break
                    print(f"[FULLENRICH] {len(emails_par_idx)} emails trouvés")
                    # Fusionner Kaspr + Fullenrich
                    for k, v in emails_par_idx.items():
                        if k not in emails_result:
                            emails_result[k] = {"email": v, "source": "+Fullenrich"}
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

    headers  = ['Organisation','Prénom','Nom','Titre','Email','LinkedIn','Confiance','Source','Dans Pipedrive']
    col_map  = ['societe','prenom','nom_dg','titre','email','linkedin','confiance','source','dans_pipedrive']
    thin     = Side(style='thin', color="e2e8f0")
    border   = Border(left=thin, right=thin, top=thin, bottom=thin)

    # Titre
    ws.merge_cells('A1:I1')
    c = ws['A1']
    c.value = "Enrichissement Dirigeants"
    c.font  = Font(name='Arial', bold=True, size=14, color="FFFFFF")
    c.fill  = PatternFill('solid', start_color="1e3a5f")
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[1].height = 32

    # Stats
    emails_count = len([r for r in rows if r.get('email')])
    ws.merge_cells('A2:I2')
    c = ws['A2']
    c.value = f"{len(rows)} contacts  |  {emails_count} emails trouvés  |  {len(rows)-emails_count} sans email"
    c.font  = Font(name='Arial', size=10, color="FFFFFF")
    c.fill  = PatternFill('solid', start_color="2563eb")
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[2].height = 22

    # Headers
    for col_idx, h in enumerate(headers, 1):
        c = ws.cell(row=3, column=col_idx, value=h)
        c.font = Font(name='Arial', bold=True, size=10, color="FFFFFF")
        c.fill = PatternFill('solid', start_color="2563eb")
        c.alignment = Alignment(horizontal='center', vertical='center')
        c.border = border
    ws.row_dimensions[3].height = 28

    src_colors = {'Pappers':'eff6ff','Claude':'f5f3ff','Pipedrive':'fef3c7','Kaspr':'e0f2fe','Fullenrich':'dcfce7'}

    # Tri par organisation + couleurs alternées par orga
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

    for i, w in enumerate([22,14,18,28,32,14,12,22,28], 1):
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

    if not emails_dest or not rows:
        return {"ok": False, "error": "Email(s) ou données manquants"}
    if not SMTP_USER or not SMTP_PASS:
        return {"ok": False, "error": "SMTP non configuré"}

    try:
        # Générer l'Excel en mémoire
        excel_content = generer_excel(rows)

        # Construire le mail
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

        # Pièce jointe Excel
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(excel_content)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="enrichissement_dirigeants.xlsx"')
        msg.attach(part)

        # Envoi SMTP
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, emails_dest, msg.as_string())

        print(f"[EMAIL] ✅ Excel envoyé à {', '.join(emails_dest)}")
        return {"ok": True}

    except Exception as e:
        print(f"[EMAIL ERROR] {e}")
        return {"ok": False, "error": str(e)}
