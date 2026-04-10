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

    print(f"[START] {nom} | key={'OK' if ANTHROPIC_KEY else 'MISSING'}")

    # Pappers → domaine si manquant
    if not domaine and siren and PAPPERS_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get("https://api.pappers.fr/v2/entreprise",
                    params={"api_token": PAPPERS_KEY, "siren": siren})
                if r.status_code == 200:
                    d = r.json()
                    domaine = d.get("domaine_url","") or d.get("site_web","")
        except Exception as e:
            print(f"[PAPPERS ERROR] {e}")

    contacts = []
    if not ANTHROPIC_KEY:
        print("[ERROR] Pas de clé Anthropic !")
        return {"results": [{"org_id":org_id,"societe":nom,"siren":siren,"domaine":domaine,
            "prenom":"","nom_dg":"","titre":"","email":"","confiance":"faible","source":"Clé API manquante"}]}

    # -------------------------------------------------------
    # PHASE 1 : noms et titres uniquement (pas d'email pour l'instant)
    # PHASE 2 (à réactiver plus tard) : ajouter web_search + recherche email
    # -------------------------------------------------------
    prompt = f"""Tu es un expert en annuaires d'entreprises françaises.
Donne-moi les dirigeants de cette société :
Nom: {nom}{chr(10)+"SIREN: "+siren if siren else ""}{chr(10)+"Site: "+domaine if domaine else ""}

Cherche dans ta connaissance : CEO, DG, Président, Gérant, CFO, DAF, CTO, COO, CMO, Partners, Associés.

Réponds UNIQUEMENT avec ce JSON, sans texte avant ni après :
{{"domaine":"...ou null","contacts":[{{"prenom":"...","nom":"...","titre":"..."}}]}}"""

    # -------------------------------------------------------
    # PHASE 2 (désactivé) — réactiver quand on voudra les emails :
    #
    # prompt += " Et leur email professionnel."
    # tools = [{"type": "web_search_20250305", "name": "web_search"}]
    # (remplacer tools=[] par tools=tools dans l'appel API ci-dessous)
    # -------------------------------------------------------

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
                    print(f"[WAIT] Status {r.status_code} — attente {wait}s avant retry")
                    await asyncio.sleep(wait)
                    continue

                if r.status_code == 200:
                    all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
                    print(f"[CLAUDE] Réponse {len(all_text)} chars pour {nom}")
                    m = re.search(r'\{[\s\S]*"contacts"[\s\S]*\}', all_text)
                    if m:
                        parsed = json.loads(m.group())
                        if not domaine and parsed.get("domaine"):
                            domaine = parsed["domaine"]
                        contacts = parsed.get("contacts", [])
                        print(f"[OK] {len(contacts)} contacts pour {nom}")
                    else:
                        print(f"[WARN] Pas de JSON pour {nom}: {all_text[:200]}")
                    break
                else:
                    print(f"[ERROR] Status {r.status_code}: {r.text[:200]}")
                    break

        except Exception as e:
            print(f"[EXCEPTION] {nom} tentative {attempt+1}: {e}")
            if attempt < 2:
                await asyncio.sleep(delays[attempt])

    if not contacts:
        contacts = [{"prenom":"","nom":"","titre":""}]

    results = []
    for ct in contacts:
        results.append({
            "org_id":  org_id,
            "societe": nom,
            "siren":   siren,
            "domaine": domaine,
            "prenom":  ct.get("prenom",""),
            "nom_dg":  ct.get("nom",""),
            "titre":   ct.get("titre",""),
            # PHASE 2 : ces champs seront remplis quand on réactivera la recherche email
            "email":    "",
            "confiance": "",
            "source":   "Claude (mémoire)",
        })
    return {"results": results}
