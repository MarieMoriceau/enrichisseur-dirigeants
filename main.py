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

async def fullenrich_emails(contacts, domaine, nom_societe):
    """
    Envoie une liste de contacts à Fullenrich et attend le résultat.
    Retourne un dict {index: email} pour les emails trouvés.
    """
    if not FULLENRICH_KEY or not contacts:
        return {}

    # On prépare uniquement les contacts sans email ou confiance faible
    to_enrich = []
    indices = []
    for i, ct in enumerate(contacts):
        email_actuel = ct.get("email", "")
        confiance = ct.get("confiance_email", ct.get("confiance", ""))
        prenom = ct.get("prenom", "")
        nom = ct.get("nom", "")
        if not prenom or not nom:
            continue
        if email_actuel and confiance not in ("faible", ""):
            continue
        to_enrich.append({
            "firstname": prenom,
            "lastname": nom,
            "domain": domaine,
            "company_name": nom_societe,
            "enrich_fields": ["contact.emails"],
            "custom": {"idx": str(i)}
        })
        indices.append(i)

    if not to_enrich:
        return {}

    print(f"[FULLENRICH] Envoi de {len(to_enrich)} contacts pour {nom_societe}")

    try:
        async with httpx.AsyncClient(timeout=30) as c:
            # Étape 1 : lancer l'enrichissement
            r = await c.post(
                "https://app.fullenrich.com/api/v1/contact/enrich/bulk",
                headers={
                    "Authorization": f"Bearer {FULLENRICH_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "name": f"Enrichissement {nom_societe}",
                    "datas": to_enrich
                }
            )
            print(f"[FULLENRICH] Status lancement : {r.status_code}")
            if r.status_code not in (200, 201):
                print(f"[FULLENRICH ERROR] {r.text[:200]}")
                return {}

            enrichment_id = r.json().get("enrichment_id") or r.json().get("id")
            if not enrichment_id:
                print(f"[FULLENRICH] Pas d'enrichment_id dans : {r.text[:200]}")
                return {}

            print(f"[FULLENRICH] enrichment_id={enrichment_id}")

            # Étape 2 : polling jusqu'à FINISHED (max 60s)
            for attempt in range(12):
                await asyncio.sleep(5)
                r2 = await c.get(
                    f"https://app.fullenrich.com/api/v1/contact/enrich/bulk/{enrichment_id}",
                    headers={"Authorization": f"Bearer {FULLENRICH_KEY}"}
                )
                if r2.status_code != 200:
                    continue
                result = r2.json()
                status = result.get("status", "")
                print(f"[FULLENRICH] Polling {attempt+1}/12 — status={status}")

                if status == "FINISHED":
                    # Extraire les emails trouvés
                    emails_par_idx = {}
                    for contact_result in result.get("datas", []):
                        idx = int(contact_result.get("custom", {}).get("idx", -1))
                        emails = contact_result.get("contact", {}).get("emails", [])
                        if idx >= 0 and emails:
                            # Prendre le premier email valide
                            for e in emails:
                                val = e.get("value") or e.get("email") or ""
                                if val and "@" in val:
                                    emails_par_idx[idx] = val
                                    break
                    print(f"[FULLENRICH] {len(emails_par_idx)} emails trouvés")
                    return emails_par_idx

            print(f"[FULLENRICH] Timeout — pas de résultat après 60s")
            return {}

    except Exception as e:
        print(f"[FULLENRICH EXCEPTION] {e}")
        return {}


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
                    if r.status_code == 200:
                        pappers_data = r.json()
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

    if ANTHROPIC_KEY:
        noms_deja_trouves = [f"{c['prenom']} {c['nom']}".strip() for c in pappers_contacts]
        exclusion = f"\nNe pas inclure (déjà connus) : {', '.join(noms_deja_trouves)}" if noms_deja_trouves else ""

        prompt = f"""Recherche sur le web les dirigeants et leurs emails professionnels pour cette société française :
Nom: {nom}{chr(10)+"SIREN: "+siren if siren else ""}{chr(10)+"Site: "+domaine if domaine else ""}{exclusion}

Cherche sur LinkedIn, le site officiel, Societe.com :
CEO, DG, CFO, DAF, CTO, COO, CMO, DRH, Président, Gérant, Partners, Associés, Fondateurs.
Inclure uniquement des emails professionnels (pas gmail/hotmail/yahoo).
Si tu n'es pas certain d'un nom, ne l'inclus pas.

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
                        await asyncio.sleep(delays[attempt])
                        continue

                    if r.status_code == 200:
                        all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
                        m = re.search(r'\{[\s\S]*"contacts"[\s\S]*\}', all_text)
                        if m:
                            parsed = json.loads(m.group())
                            for ct in parsed.get("contacts", []):
                                ct["source"] = "Claude+web"
                                # Filtrer les emails perso
                                email = ct.get("email", "") or ""
                                if any(x in email for x in ["gmail", "hotmail", "yahoo", "outlook.com"]):
                                    ct["email"] = ""
                                    ct["confiance_email"] = "faible"
                            claude_contacts = parsed.get("contacts", [])
                            print(f"[CLAUDE OK] {len(claude_contacts)} contacts pour {nom}")
                        break
                    else:
                        break

            except Exception as e:
                print(f"[CLAUDE EXCEPTION] {e}")
                if attempt < 2:
                    await asyncio.sleep(delays[attempt])

    # -------------------------------------------------------
    # ÉTAPE 3 : Fullenrich → compléter les emails manquants
    # -------------------------------------------------------
    tous_contacts = pappers_contacts + claude_contacts

    if FULLENRICH_KEY and domaine and tous_contacts:
        emails_trouves = await fullenrich_emails(tous_contacts, domaine, nom)
        for idx, email in emails_trouves.items():
            if idx < len(tous_contacts):
                tous_contacts[idx]["email"] = email
                tous_contacts[idx]["confiance_email"] = "haute"
                source = tous_contacts[idx].get("source", "")
                if "Fullenrich" not in source:
                    tous_contacts[idx]["source"] = source + "+Fullenrich"

    if not tous_contacts:
        tous_contacts = [{"prenom":"","nom":"","titre":"","email":"","confiance_email":"","source":""}]

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

    print(f"[DONE] {nom} → {len(results)} contacts au total")
    return {"results": results}
