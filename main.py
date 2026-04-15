import os, json, asyncio, httpx, re
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY    = os.getenv("PAPPERS_API_KEY", "")
FULLENRICH_KEY = os.getenv("FULLENRICH_API_KEY", "")
PIPEDRIVE_KEY  = os.getenv("PIPEDRIVE_API_KEY", "")

ANCIENS_KEYWORDS = [
    "ancien", "ancienne", "ex-", "ex ", "démissionnaire",
    "jusqu'au", "jusqu au", "sortant"
]

# Titres à exclure car pas des dirigeants opérationnels
TITRES_EXCLUS = [
    "commissaire aux comptes", "commissaire", "conseil de surveillance",
    "membre du conseil", "membre du directoire observateur",
    "censeur", "observateur", "représentant permanent",
    "liquidateur", "mandataire", "administrateur judiciaire",
]

def est_ancien_dirigeant(titre: str) -> bool:
    titre_lower = titre.lower()
    return any(kw in titre_lower for kw in ANCIENS_KEYWORDS)

def est_titre_exclu(titre: str) -> bool:
    titre_lower = titre.lower()
    return any(kw in titre_lower for kw in TITRES_EXCLUS)

def noms_similaires(nom_csv: str, nom_pappers: str) -> bool:
    """Vérifie que le nom trouvé par Pappers correspond bien à la société du CSV."""
    a = nom_csv.lower().strip()
    b = nom_pappers.lower().strip()
    # Extraire les mots significatifs (> 3 chars)
    mots_a = set(w for w in a.split() if len(w) > 3)
    mots_b = set(w for w in b.split() if len(w) > 3)
    if not mots_a:
        return True  # nom trop court, on accepte
    # Au moins 1 mot en commun
    communs = mots_a & mots_b
    return len(communs) > 0

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
    }

# -------------------------------------------------------
# PASSE 1.5 : Vérification Pipedrive par nom
# Retourne l'email si le contact existe déjà
# -------------------------------------------------------
async def check_pipedrive(prenom: str, nom: str) -> str:
    if not PIPEDRIVE_KEY or not prenom or not nom:
        return ""
    terme = f"{prenom} {nom}".strip()
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                "https://api.pipedrive.com/v1/persons/search",
                params={
                    "term": terme,
                    "fields": "name,email",
                    "exact_match": "false",
                    "limit": 5,
                    "api_token": PIPEDRIVE_KEY
                }
            )
            if r.status_code == 200:
                items = r.json().get("data", {}).get("items", [])
                for item in items:
                    person = item.get("item", {})
                    # Vérifier que le nom correspond approximativement
                    person_name = person.get("name", "").lower()
                    if nom.lower() in person_name or prenom.lower() in person_name:
                        emails = person.get("emails", [])
                        if emails:
                            email = emails[0] if isinstance(emails[0], str) else emails[0].get("value", "")
                            if email and "@" in email:
                                print(f"[PIPEDRIVE] ✅ {terme} → {email}")
                                return email
            print(f"[PIPEDRIVE] ❌ {terme} non trouvé")
    except Exception as e:
        print(f"[PIPEDRIVE ERROR] {terme}: {e}")
    return ""

# -------------------------------------------------------
# ROUTE PASSE 1 : Pappers + Claude + Pipedrive check
# -------------------------------------------------------
@app.post("/enrich_one")
async def enrich_one(request: Request):
    data = await request.json()
    nom     = data.get("nom", "")
    siren   = data.get("siren", "")
    domaine = data.get("domaine", "")
    org_id  = data.get("org_id", "")

    print(f"[START] {nom} | domaine={domaine} | siren={siren}")

    pappers_contacts = []
    pappers_data = None

    # ÉTAPE 1 : Pappers
    if PAPPERS_KEY:
        if domaine:
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
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get("https://api.pappers.fr/v2/recherche",
                        params={"api_token": PAPPERS_KEY, "q": nom, "par_page": 1})
                    if r.status_code == 200:
                        resultats = r.json().get("resultats", [])
                        if resultats:
                            siren = resultats[0].get("siren", "")
                            vrai_domaine = resultats[0].get("domaine_url", "") or resultats[0].get("site_web", "")
                            if vrai_domaine and not domaine:
                                domaine = vrai_domaine
                            print(f"[PAPPERS] SIREN={siren} domaine={domaine}")
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
            if not domaine:
                domaine = pappers_data.get("domaine_url", "") or pappers_data.get("site_web", "")

            # Vérifier que Pappers a bien trouvé la bonne société
            nom_pappers = pappers_data.get("nom_entreprise", "") or pappers_data.get("denomination", "")
            if nom_pappers and not noms_similaires(nom, nom_pappers):
                print(f"[PAPPERS] ⚠️ Mauvaise société trouvée : '{nom_pappers}' pour '{nom}' — ignoré")
                pappers_data = None
                siren = ""
                domaine = data.get("domaine", "")  # remettre le domaine original

        if pappers_data:
            # Vérifier si la société est radiée
            entreprise_cessee = pappers_data.get("entreprise_cessee", False)
            statut_rcs = pappers_data.get("statut_rcs", "").lower()
            statut_consolide = pappers_data.get("statut_consolide", "").lower()
            if entreprise_cessee or statut_rcs == "radié" or statut_consolide == "radié":
                print(f"[RADIÉE] {nom} est radiée — enrichissement arrêté")
                return {"results": [{
                    "org_id": org_id, "societe": nom, "siren": siren, "domaine": domaine,
                    "prenom": "", "nom_dg": "", "titre": "⚠️ Société radiée",
                    "email": "", "confiance": "", "source": "Pappers"
                }]}

            for rep in pappers_data.get("representants", []):
                if rep.get("personne_morale"):
                    continue
                titre = rep.get("qualite", "Représentant légal")
                # Filtrer anciens dirigeants et titres non opérationnels
                if est_ancien_dirigeant(titre) or est_titre_exclu(titre):
                    print(f"[FILTRE] Ignoré : {rep.get('prenom','')} {rep.get('nom','')} ({titre})")
                    continue
                pappers_contacts.append({
                    "prenom": rep.get("prenom", ""),
                    "nom":    rep.get("nom", ""),
                    "titre":  titre,
                    "email":  "",
                    "confiance": "",
                    "source": "Pappers"
                })
            print(f"[PAPPERS] {len(pappers_contacts)} représentants actifs")

    # ÉTAPE 2 : Claude + web_search
    claude_contacts = []
    if ANTHROPIC_KEY:
        noms_deja = [f"{c['prenom']} {c['nom']}".strip() for c in pappers_contacts]
        exclusion = f"\nNe pas inclure : {', '.join(noms_deja)}" if noms_deja else ""

        prompt = f"""Recherche sur le web les dirigeants ACTUELS et leurs emails pour cette société française :
Nom: {nom}{chr(10)+"SIREN: "+siren if siren else ""}{chr(10)+"Site: "+domaine if domaine else ""}{exclusion}

Cherche : CEO, DG, CFO, DAF, CTO, COO, CMO, DRH, Président, Gérant, Partners, Associés, Fondateurs.
- Dirigeants encore en poste UNIQUEMENT
- Exclure "ancien", "ex-", "démissionnaire"
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
                        headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                        json={
                            "model": "claude-sonnet-4-20250514",
                            "max_tokens": 1000,
                            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
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
                            for ct in parsed.get("contacts", []):
                                ct["source"] = "Claude+web"
                                email = ct.get("email", "") or ""
                                if any(x in email for x in ["gmail","hotmail","yahoo","outlook.com"]):
                                    ct["email"] = ""
                                    ct["confiance_email"] = "faible"
                                if est_ancien_dirigeant(ct.get("titre", "")):
                                    ct["_skip"] = True
                            claude_contacts = [ct for ct in parsed.get("contacts", []) if not ct.get("_skip")]
                            print(f"[CLAUDE OK] {len(claude_contacts)} contacts pour {nom}")
                        break
                    else:
                        break
            except Exception as e:
                print(f"[CLAUDE EXCEPTION] {e}")
                if attempt < 2:
                    await asyncio.sleep(delays[attempt])

    tous_contacts = pappers_contacts + claude_contacts

    # ÉTAPE 3 : Pipedrive check pour chaque contact sans email
    if PIPEDRIVE_KEY:
        print(f"[PIPEDRIVE] Vérification de {len(tous_contacts)} contacts")
        for ct in tous_contacts:
            if ct.get("email"):
                continue  # déjà un email, on skip
            email_pipedrive = await check_pipedrive(ct.get("prenom",""), ct.get("nom",""))
            if email_pipedrive:
                ct["email"] = email_pipedrive
                ct["confiance_email"] = "haute"
                ct["source"] = ct.get("source","") + "+Pipedrive"

    if not tous_contacts:
        tous_contacts = [{"prenom":"","nom":"","titre":"","email":"","confiance":"","source":""}]

    results = []
    for ct in tous_contacts:
        results.append({
            "org_id":    org_id,
            "societe":   nom,
            "siren":     siren,
            "domaine":   domaine,
            "prenom":    ct.get("prenom",""),
            "nom_dg":    ct.get("nom",""),
            "titre":     ct.get("titre",""),
            "email":     ct.get("email","") or "",
            "confiance": ct.get("confiance_email", ct.get("confiance","")),
            "source":    ct.get("source",""),
        })

    print(f"[DONE] {nom} → {len(results)} contacts")
    return {"results": results}


# -------------------------------------------------------
# HELPERS : validation et correction de domaine
# -------------------------------------------------------
def domaine_valide(d: str) -> bool:
    d = d.strip()
    return bool(d) and "." in d and " " not in d and len(d) > 3

async def corriger_domaine(siren: str, societe: str) -> str:
    """Tente de trouver le vrai domaine via Pappers puis Claude."""
    # Tentative 1 : Pappers via SIREN
    if PAPPERS_KEY and siren:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get("https://api.pappers.fr/v2/entreprise",
                    params={"api_token": PAPPERS_KEY, "siren": siren})
                if r.status_code == 200:
                    d = r.json()
                    domaine = d.get("domaine_url","") or d.get("site_web","")
                    if domaine and domaine_valide(domaine):
                        print(f"[DOMAINE FIX] Pappers → {domaine} pour {societe}")
                        return domaine.strip()
        except Exception as e:
            print(f"[DOMAINE FIX ERROR Pappers] {e}")

    # Tentative 2 : Claude web search
    if ANTHROPIC_KEY:
        try:
            prompt = f"""Quel est le nom de domaine du site web officiel de cette société française : {societe} ?
Réponds UNIQUEMENT avec le domaine (ex: example.com), sans http ni www, sans aucun autre texte."""
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 50,
                        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                        "messages": [{"role": "user", "content": prompt}]
                    }
                )
                if r.status_code == 200:
                    all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text").strip()
                    # Nettoyer la réponse
                    domaine = all_text.lower().replace("http://","").replace("https://","").replace("www.","").split("/")[0].strip()
                    if domaine_valide(domaine):
                        print(f"[DOMAINE FIX] Claude → {domaine} pour {societe}")
                        return domaine
        except Exception as e:
            print(f"[DOMAINE FIX ERROR Claude] {e}")

    return ""

# -------------------------------------------------------
# ROUTE PASSE 2 : Fullenrich batch
# -------------------------------------------------------
@app.post("/enrich_emails")
async def enrich_emails(request: Request):
    data = await request.json()
    contacts = data.get("contacts", [])

    if not FULLENRICH_KEY:
        return {"error": "Clé Fullenrich manquante"}
    if not contacts:
        return {"emails": {}}

    # Corriger les domaines invalides avant envoi
    for ct in contacts:
        domaine_ct = ct.get("domaine", "").strip()
        if not domaine_valide(domaine_ct):
            siren = ct.get("siren", "")
            societe = ct.get("societe", "")
            print(f"[DOMAINE FIX] Domaine invalide '{domaine_ct}' pour {societe}, tentative correction...")
            nouveau = await corriger_domaine(siren, societe)
            if nouveau:
                ct["domaine"] = nouveau

    to_enrich = []
    for ct in contacts:
        email = ct.get("email", "")
        confiance = ct.get("confiance", "")
        if email and confiance not in ("faible", ""):
            continue
        if not ct.get("prenom") or not ct.get("nom"):
            continue
        domaine_ct = ct.get("domaine", "").strip()
        if not domaine_valide(domaine_ct):
            print(f"[FULLENRICH] Domaine toujours invalide après correction, ignoré : '{domaine_ct}'")
            continue
        to_enrich.append({
            "firstname":    ct["prenom"],
            "lastname":     ct["nom"],
            "domain":       domaine_ct,
            "company_name": ct.get("societe", ""),
            "enrich_fields": ["contact.emails"],
            "custom": {"idx": str(ct.get("idx", 0))}
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
                status = result.get("status", "")
                print(f"[FULLENRICH] Polling {attempt+1}/36 — status={status}")

                if status == "FINISHED":
                    emails_par_idx = {}
                    for ct_result in result.get("datas", []):
                        idx = ct_result.get("custom", {}).get("idx", "-1")
                        emails = ct_result.get("contact", {}).get("emails", [])
                        if emails:
                            for e in emails:
                                val = e.get("value") or e.get("email") or ""
                                if val and "@" in val:
                                    emails_par_idx[idx] = val
                                    break
                    print(f"[FULLENRICH] {len(emails_par_idx)} emails trouvés")
                    return {"emails": emails_par_idx}

            print(f"[FULLENRICH] Timeout 180s")
            return {"emails": {}}

    except Exception as e:
        print(f"[FULLENRICH EXCEPTION] {e}")
        return {"emails": {}}
