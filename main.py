import os, json, asyncio, httpx, re
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY   = os.getenv("PAPPERS_API_KEY", "")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health():
    return {"ok": True, "anthropic_key": bool(ANTHROPIC_KEY), "pappers_key": bool(PAPPERS_KEY)}

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
    if PAPPERS_KEY and (domaine or siren):
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                # On cherche par site_internet si on a le domaine, sinon par siren
                if domaine:
                    params = {"api_token": PAPPERS_KEY, "site_internet": domaine}
                else:
                    params = {"api_token": PAPPERS_KEY, "siren": siren}

                print(f"[PAPPERS] Appel pour {nom} avec params={list(params.keys())}")
                r = await c.get("https://api.pappers.fr/v2/entreprise", params=params)
                print(f"[PAPPERS] Status {r.status_code} pour {nom}")

                if r.status_code == 200:
                    d = r.json()
                    # Récupérer le SIREN si on ne l'avait pas
                    if not siren:
                        siren = d.get("siren", "")
                    # Récupérer les représentants légaux
                    for rep in d.get("representants", []):
                        if rep.get("personne_morale"):
                            continue  # on ignore les représentants qui sont des sociétés
                        pappers_contacts.append({
                            "prenom": rep.get("prenom", ""),
                            "nom":    rep.get("nom", ""),
                            "titre":  rep.get("qualite", "Représentant légal"),
                            "source": "Pappers"
                        })
                    print(f"[PAPPERS] {len(pappers_contacts)} représentants pour {nom}")
                else:
                    print(f"[PAPPERS] Erreur {r.status_code}: {r.text[:100]}")
        except Exception as e:
            print(f"[PAPPERS ERROR] {nom}: {e}")

    # -------------------------------------------------------
    # ÉTAPE 2 : Claude → dirigeants non légaux (CEO, CFO, DAF...)
    # -------------------------------------------------------
    claude_contacts = []

    if not ANTHROPIC_KEY:
        print("[ERROR] Pas de clé Anthropic !")
    else:
        # On liste les noms déjà trouvés via Pappers pour éviter les doublons
        noms_deja_trouves = [f"{c['prenom']} {c['nom']}" for c in pappers_contacts]
        exclusion = f"\nExclure ces personnes déjà connues : {', '.join(noms_deja_trouves)}" if noms_deja_trouves else ""

        prompt = f"""Tu es un expert en dirigeants d'entreprises françaises.
Trouve les dirigeants opérationnels (pas les représentants légaux) de cette société :
Nom: {nom}{chr(10)+"SIREN: "+siren if siren else ""}{chr(10)+"Site: "+domaine if domaine else ""}{exclusion}

Cherche : CEO, Directeur Général, CFO, DAF, CTO, COO, CMO, DRH, Partners, Associés.
Ne cherche PAS les gérants ou présidents légaux (déjà récupérés).
Si tu ne trouves personne de certain, retourne une liste vide.

Réponds UNIQUEMENT avec ce JSON, sans texte avant ni après :
{{"contacts":[{{"prenom":"...","nom":"...","titre":"..."}}]}}"""

        delays = [5, 15, 30]
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=60) as c:
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
                            "max_tokens": 600,
                            # PHASE 2 : remplacer [] par [{"type":"web_search_20250305","name":"web_search"}]
                            "tools": [],
                            "messages": [{"role": "user", "content": prompt}]
                        }
                    )
                    print(f"[CLAUDE] Status {r.status_code} pour {nom}")

                    if r.status_code in (429, 529):
                        wait = delays[attempt]
                        print(f"[WAIT] Status {r.status_code} — attente {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    if r.status_code == 200:
                        all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
                        print(f"[CLAUDE] Réponse {len(all_text)} chars pour {nom}")
                        m = re.search(r'\{[\s\S]*"contacts"[\s\S]*\}', all_text)
                        if m:
                            parsed = json.loads(m.group())
                            for ct in parsed.get("contacts", []):
                                ct["source"] = "Claude"
                            claude_contacts = parsed.get("contacts", [])
                            print(f"[CLAUDE OK] {len(claude_contacts)} contacts pour {nom}")
                        else:
                            print(f"[CLAUDE WARN] Pas de JSON pour {nom}: {all_text[:100]}")
                        break
                    else:
                        print(f"[CLAUDE ERROR] Status {r.status_code}: {r.text[:200]}")
                        break

            except Exception as e:
                print(f"[CLAUDE EXCEPTION] {nom} tentative {attempt+1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(delays[attempt])

    # -------------------------------------------------------
    # ÉTAPE 3 : Fusionner Pappers + Claude
    # -------------------------------------------------------
    tous_contacts = pappers_contacts + claude_contacts

    if not tous_contacts:
        tous_contacts = [{"prenom":"","nom":"","titre":"","source":""}]

    results = []
    for ct in tous_contacts:
        results.append({
            "org_id":  org_id,
            "societe": nom,
            "siren":   siren,
            "domaine": domaine,
            "prenom":  ct.get("prenom",""),
            "nom_dg":  ct.get("nom",""),
            "titre":   ct.get("titre",""),
            "email":   "",
            "confiance": "",
            "source":  ct.get("source",""),
        })

    print(f"[DONE] {nom} → {len(results)} contacts au total")
    return {"results": results}
