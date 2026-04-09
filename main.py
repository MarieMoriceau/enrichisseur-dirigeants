import os, io, csv, json, asyncio, httpx
from fastapi import FastAPI, UploadFile, File, Form, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Optional
import uuid

app = FastAPI(title="Enrichisseur Dirigeants")
templates = Jinja2Templates(directory="templates")

# ─── CONFIG APIS ────────────────────────────────────────────────────
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
PHAROW_KEY     = os.getenv("PHAROW_API_KEY", "")
FULLENRICH_KEY = os.getenv("FULLENRICH_API_KEY", "")

# ─── STOCKAGE EN MÉMOIRE (jobs) ─────────────────────────────────────
jobs: dict[str, dict] = {}   # job_id → {rows, results, status, progress}


# ════════════════════════════════════════════════════════════════════
#  ROUTES UI
# ════════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ════════════════════════════════════════════════════════════════════
#  PARSING INPUT
# ════════════════════════════════════════════════════════════════════
def parse_csv_bytes(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for r in reader:
        row = {k.strip().lower(): v.strip() for k, v in r.items()}
        rows.append(normalize_row(row))
    return rows

def parse_paste(text: str) -> list[dict]:
    """Accepte CSV ou lignes simples nom;siren;domaine"""
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if not lines:
        return []
    # Détecte séparateur
    sep = ";" if ";" in lines[0] else ","
    reader = csv.DictReader(lines, delimiter=sep)
    rows = []
    for r in reader:
        row = {k.strip().lower(): v.strip() for k, v in r.items()}
        rows.append(normalize_row(row))
    return rows

def normalize_row(row: dict) -> dict:
    """Normalise les noms de colonnes flexibles"""
    aliases = {
        "nom": ["nom","name","société","societe","company","entreprise"],
        "siren": ["siren","siret"],
        "domaine": ["domaine","domain","website","site","url"],
        "prenom_dg": ["prenom","prénom","firstname","first_name","prenom_dg"],
        "nom_dg": ["nom_dg","lastname","last_name","dirigeant","manager","dg"],
    }
    result = {"id": str(uuid.uuid4())}
    for target, keys in aliases.items():
        for k in keys:
            if k in row and row[k]:
                result[target] = row[k]
                break
        if target not in result:
            result[target] = ""
    return result


# ════════════════════════════════════════════════════════════════════
#  ENRICHISSEMENT — SOURCES EN CASCADE
# ════════════════════════════════════════════════════════════════════
async def enrich_one(row: dict) -> dict:
    """Tente Pharow → Fullenrich → Claude en fallback"""
    result = {
        "nom": row.get("nom",""),
        "siren": row.get("siren",""),
        "domaine": row.get("domaine",""),
        "prenom_dg": row.get("prenom_dg",""),
        "nom_dg": row.get("nom_dg",""),
        "email": "",
        "telephone": "",
        "titre": "",
        "confiance": "",
        "source": "",
        "notes": "",
    }

    # 1. PHAROW
    if PHAROW_KEY and row.get("siren"):
        pharow = await try_pharow(row["siren"])
        if pharow:
            result.update(pharow)
            result["source"] = "Pharow"
            if result.get("email"):
                return result

    # 2. FULLENRICH
    if FULLENRICH_KEY and (result.get("prenom_dg") or row.get("prenom_dg")) and result.get("domaine"):
        prenom = result.get("prenom_dg") or row.get("prenom_dg","")
        nom    = result.get("nom_dg") or row.get("nom_dg","")
        fe = await try_fullenrich(prenom, nom, result["domaine"])
        if fe:
            result.update(fe)
            result["source"] = (result.get("source","") + " + Fullenrich").lstrip(" + ")
            if result.get("email"):
                return result

    # 3. CLAUDE FALLBACK
    claude = await try_claude(row)
    if claude:
        result.update(claude)
        result["source"] = (result.get("source","") + " + Claude").lstrip(" + ")

    return result


# ── PHAROW ──────────────────────────────────────────────────────────
async def try_pharow(siren: str) -> Optional[dict]:
    """Doc: https://docs.pharow.com"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.pharow.com/v1/companies/search",
                params={"siren": siren},
                headers={"Authorization": f"Bearer {PHAROW_KEY}"}
            )
            if r.status_code != 200:
                return None
            data = r.json()
            company = data.get("data", {})
            contacts = company.get("contacts", [])
            if not contacts:
                return None
            top = contacts[0]
            return {
                "prenom_dg": top.get("first_name",""),
                "nom_dg": top.get("last_name",""),
                "titre": top.get("job_title",""),
                "email": top.get("email",""),
                "telephone": top.get("phone",""),
                "domaine": company.get("domain",""),
                "confiance": "haute",
            }
    except Exception as e:
        return None


# ── FULLENRICH ───────────────────────────────────────────────────────
async def try_fullenrich(prenom: str, nom: str, domaine: str) -> Optional[dict]:
    """Doc: https://docs.fullenrich.com"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                "https://api.fullenrich.com/v1/enrich",
                headers={
                    "Authorization": f"Bearer {FULLENRICH_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "first_name": prenom,
                    "last_name": nom,
                    "company_domain": domaine
                }
            )
            if r.status_code != 200:
                return None
            data = r.json()
            person = data.get("person", {})
            emails = person.get("emails", [])
            phones = person.get("phones", [])
            return {
                "email": emails[0].get("value","") if emails else "",
                "telephone": phones[0].get("value","") if phones else "",
                "titre": person.get("title",""),
                "confiance": "haute" if emails else "faible",
            }
    except Exception:
        return None


# ── CLAUDE FALLBACK ──────────────────────────────────────────────────
async def try_claude(row: dict) -> Optional[dict]:
    if not ANTHROPIC_KEY:
        return None
    prompt = f"""Tu es un assistant B2B spécialisé en recherche de contacts de dirigeants de sociétés françaises.

Société: {row.get('nom','')}
{"SIREN: " + row['siren'] if row.get('siren') else ""}
{"Domaine web: " + row['domaine'] if row.get('domaine') else ""}
{"DG connu: " + row.get('prenom_dg','') + " " + row.get('nom_dg','') if row.get('prenom_dg') else ""}

Trouve le dirigeant principal (PDG/DG/Président/Gérant) et son email professionnel.

Réponds UNIQUEMENT avec ce JSON (pas de markdown, pas de texte autour):
{{"prenom_dg":"...","nom_dg":"...","titre":"...","domaine":"...","email":"...","confiance":"haute|moyenne|faible","notes":"..."}}"""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 500,
                    "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            data = r.json()
            text_block = next((b for b in data.get("content",[]) if b.get("type")=="text"), None)
            if not text_block:
                return None
            import re
            m = re.search(r'\{.*?\}', text_block["text"], re.DOTALL)
            if not m:
                return None
            return json.loads(m.group())
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════
#  ENDPOINTS API
# ════════════════════════════════════════════════════════════════════
@app.post("/upload")
async def upload(background_tasks: BackgroundTasks, file: UploadFile = File(None), paste: str = Form("")):
    rows = []
    if file and file.filename:
        content = await file.read()
        rows = parse_csv_bytes(content)
    elif paste:
        rows = parse_paste(paste)

    if not rows:
        return JSONResponse({"error": "Aucune donnée valide"}, status_code=400)

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"rows": rows, "results": [], "status": "pending", "progress": 0, "total": len(rows)}
    background_tasks.add_task(run_job, job_id)
    return {"job_id": job_id, "total": len(rows)}


async def run_job(job_id: str):
    job = jobs[job_id]
    job["status"] = "running"
    rows = job["rows"]
    for i, row in enumerate(rows):
        result = await enrich_one(row)
        job["results"].append(result)
        job["progress"] = i + 1
        await asyncio.sleep(0.3)   # rate limiting
    job["status"] = "done"


@app.get("/status/{job_id}")
async def status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "Job introuvable"}, status_code=404)
    return {
        "status": job["status"],
        "progress": job["progress"],
        "total": job["total"],
        "results": job["results"]
    }


@app.get("/export/{job_id}")
async def export(job_id: str):
    job = jobs.get(job_id)
    if not job or not job["results"]:
        return JSONResponse({"error": "Aucun résultat"}, status_code=404)

    output = io.StringIO()
    fields = ["nom","siren","domaine","prenom_dg","nom_dg","titre","email","telephone","confiance","source","notes"]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(job["results"])

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=enrichissement_{job_id[:8]}.csv"}
    )
