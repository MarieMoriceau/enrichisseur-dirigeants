import os, io, csv, json, asyncio, httpx, re, uuid
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY   = os.getenv("PAPPERS_API_KEY", "")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/enrich_one")
async def enrich_one(request: Request):
    """Enrichit UNE seule société - appelé depuis le frontend société par société"""
    data = await request.json()
    nom = data.get("nom", "")
    siren = data.get("siren", "")
    domaine = data.get("domaine", "")
    org_id = data.get("org_id", "")

    # 1. Pappers pour le domaine si manquant
    if not domaine and siren and PAPPERS_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(
                    "https://api.pappers.fr/v2/entreprise",
                    params={"api_token": PAPPERS_KEY, "siren": siren}
                )
                if r.status_code == 200:
                    d = r.json()
                    domaine = d.get("domaine_url","") or d.get("site_web","")
        except:
            pass

    # 2. Claude web search
    contacts = []
    if ANTHROPIC_KEY:
        try:
            prompt = f"""Société: {nom}{" SIREN:"+siren if siren else ""}{" "+domaine if domaine else ""}
Trouve dirigeants (CEO/CFO/DG/Partners/Associés) et emails. JSON uniquement:
{{"domaine":"...","contacts":[{{"prenom":"...","nom":"...","titre":"...","email":"...ou null","confiance_email":"haute|moyenne|faible","source":"..."}}]}}"""

            async with httpx.AsyncClient(timeout=55) as client:
                r = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 500,
                        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                        "messages": [{"role": "user", "content": prompt}]
                    }
                )
                if r.status_code == 200:
                    all_text = " ".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")
                    m = re.search(r'\{[\s\S]*"contacts"[\s\S]*\}', all_text)
                    if m:
                        parsed = json.loads(m.group())
                        if not domaine and parsed.get("domaine"):
                            domaine = parsed["domaine"]
                        contacts = parsed.get("contacts", [])
        except Exception as e:
            print(f"[ERROR] {nom}: {e}")

    if not contacts:
        contacts = [{"prenom":"","nom":"","titre":"","email":"","confiance_email":"faible","source":""}]

    results = []
    for c in contacts:
        results.append({
            "org_id": org_id,
            "societe": nom,
            "siren": siren,
            "domaine": domaine,
            "prenom": c.get("prenom",""),
            "nom_dg": c.get("nom",""),
            "titre": c.get("titre",""),
            "email": c.get("email","") or "",
            "confiance": c.get("confiance_email",""),
            "source": f"Claude ({c.get('source','web')})" if c.get("source") else "",
        })
    return {"results": results}
