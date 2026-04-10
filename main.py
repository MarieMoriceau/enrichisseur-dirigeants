import os, json, asyncio, httpx, re
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY    = os.getenv("PAPPERS_API_KEY", "")
FULLENRICH_KEY = os.getenv("FULLENRICH_API_KEY", "")

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
    }

@app.post("/enrich_one")
async def enrich_one(request: Request):
    data = await request.json()
    nom     = data.get("nom", "")
    siren   = data.get("siren", "")
    domaine = data.get("domaine", "")
    org_id  = data.get("org_id", "")

    print(f"[START] {nom} | domaine={domaine} | siren={siren}")

    pappers_contacts = []

    # -------------------------------------------------------
    # ÉTAPE 1 : Pappers → SIREN + représentants légaux
    # -------------------------------------------------------
    if PAPPERS_KEY:
        pappers_data = None

        if domaine:
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    print(f"[PAPPERS] Recherche par domaine : {domaine}")
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "site_internet": domaine})
                    print(f"[PAPPERS] Status {r.status_code} pour {nom}")
                    if r.status_code == 200:
                        pappers_data = r.json()
            except Exception as e:
                print(f"[PAPPERS ERROR domaine] {e}")

        if not pappers_data and not siren:
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    print(f"[PAPPERS] Recherche par nom : {nom}")
                    r = await c.get("https://api.pappers.fr/v2/recherche",
                        params={"api_token": PAPPERS_KEY, "q": nom, "par_page": 1})
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
                    print(f"[PAPPERS] Recherche par SIREN : {siren}")
                    r = await c.get("https://api.pappers.fr/v2/entreprise",
                        params={"api_token": PAPPERS_KEY, "siren": siren})
                    if r.status_code == 200:
                        pappers_data = r.json()
            except Exception as e:
                print(f"[PAPPERS ERROR siren] {e}")

        if pappers_data:
            if not siren:
                siren = pappers_data.get("siren", "")
            for rep in pappers_data.get("representants", []):
                if rep.get("personne_morale"):
                    continue
                pappers_contacts.append({
                    "prenom": rep.get("prenom", ""),
                    "nom":    rep.get("nom", ""),
                    "titre":  rep.get("qualite", "Représentant légal"),
                    "email":  "",
                    "confiance": "",
                    "source": "Pappers"
                })
            print(f"[PAPPERS] {len(pappers_contacts)} représentants | SIREN={siren}")

    # -------------------------------------------------------
    # ÉTAPE 2 : Claude + web_search → dirigeants + emails
    # -------------------------------------------------------
    claude_contacts = []

    if not ANTHROPIC_KEY:
        print("[ERROR] Pas de clé Anthropic !")
    else:
        noms_deja_trouves = [f"{c['prenom']} {c['nom']}".strip() for c in pappers_contacts]
        exclusion = f"\nNe pas inclure (déjà connus) : {', '.join(noms_deja_trouves)}" if noms_deja_trouves else ""

        prompt = f"""Recherche sur le web les dirigeants et leurs emails professionnels pour cette société française :
Nom: {nom}{chr(10)+"SIREN: "+siren if siren else ""}{chr(10)+"Site: "+domaine if domaine else ""}{exclusion}

Cherche sur LinkedIn, le site officiel, Societe.com :
CEO, DG, CFO, DAF, CTO, COO, CMO, DRH, Président, Gérant, Partners, Associés, Fondateurs.
Et leur email professionnel si trouvé.

Réponds UNIQUEMENT avec ce JSON :
{{"contacts":[{{"prenom":"...","nom":"...","titre":"...","email":"...ou null","confiance_email":"haute|moyenne|faible"}}]}}"""

        delays = [10, 25, 45]
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=90) as c:
                    print(f"[CLAUDE] Tentative {attempt+1}/3 pour {nom}")
                    r = await c.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={
                            "x-api-key": ANTHROPIC_KEY,
                            "anthropic-version": "2023-06-01",
                            "content-type": "application/json"
                        },
                        json={
                            "model": "claude-sonnet-4-20250514",
                            "max_tokens": 1000,
                            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                            "messages": [{"role": "user", "content": prompt}]
                        }
                    )
                    print(f"[CLAUDE] Status {r.status_code} pour {nom}")

                    if r.status_code in (429, 529):
                        wait = delays[attempt]
                        print(f"[WAIT] {r.status_code} — attente {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    if r.status_code == 200:
                        all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
                        print(f"[CLAUDE] Réponse {len(all_text)} chars pour {nom}")
                        m = re.search(r'\{[\s\S]*"contacts"[\s\S]*\}', all_text)
                        if m:
                            parsed = json.loads(m.group())
                            for ct in parsed.get("contacts", []):
                                ct["source"] = "Claude+web"
                            claude_contacts = parsed.get("contacts", [])
                            print(f"[CLAUDE OK] {len(claude_contacts)} contacts pour {nom}")
                        break
                    else:
                        print(f"[CLAUDE ERROR] {r.status_code}: {r.text[:200]}")
                        break

            except Exception as e:
                print(f"[CLAUDE EXCEPTION] {nom} tentative {attempt+1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(delays[attempt])

    # -------------------------------------------------------
    # ÉTAPE 3 : Fullenrich → email pour contacts sans email
    # ou avec confiance faible uniquement (économie de quota)
    # -------------------------------------------------------
    tous_contacts = pappers_contacts + claude_contacts

    if FULLENRICH_KEY and domaine:
        for ct in tous_contacts:
            email_actuel = ct.get("email", "")
            confiance = ct.get("confiance_email", ct.get("confiance", ""))
            # On appelle Fullenrich seulement si pas d'email ou confiance faible
            if email_actuel and confiance != "faible":
                continue
            prenom = ct.get("prenom", "")
            nom_ct = ct.get("nom", "")
            if not prenom or not nom_ct:
                continue
            try:
                async with httpx.AsyncClient(timeout=15) as c:
                    print(f"[FULLENRICH] Recherche email pour {prenom} {nom_ct} @ {domaine}")
                    r = await c.post(
                        "https://api.fullenrich.com/v1/enrich/email",
                        headers={
                            "Authorization": f"Bearer {FULLENRICH_KEY}",
                            "Content-Type": "application/json"
                        },
                        json={
                            "first_name": prenom,
                            "last_name": nom_ct,
                            "domain": domaine
                        }
                    )
                    print(f"[FULLENRICH] Status {r.status_code} pour {prenom} {nom_ct}")
                    if r.status_code == 200:
                        fe_data = r.json()
                        email_trouve = fe_data.get("email") or fe_data.get("data", {}).get("email", "")
                        if email_trouve:
                            print(f"[FULLENRICH OK] {email_trouve}")
                            ct["email"] = email_trouve
                            ct["confiance_email"] = "haute"
                            ct["source"] = ct.get("source","") + "+Fullenrich"
            except Exception as e:
                print(f"[FULLENRICH ERROR] {prenom} {nom_ct}: {e}")

    if not tous_contacts:
        tous_contacts = [{"prenom":"","nom":"","titre":"","email":"","confiance_email":"","source":""}]

    results = []
    for ct in tous_contacts:
        results.append({
            "org_id":   org_id,
            "societe":  nom,
            "siren":    siren,
            "domaine":  domaine,
            "prenom":   ct.get("prenom",""),
            "nom_dg":   ct.get("nom",""),
            "titre":    ct.get("titre",""),
            "email":    ct.get("email","") or "",
            "confiance": ct.get("confiance_email", ct.get("confiance","")),
            "source":   ct.get("source",""),
        })

    print(f"[DONE] {nom} → {len(results)} contacts au total")
    return {"results": results}
