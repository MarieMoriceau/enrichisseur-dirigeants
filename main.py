import os, io, csv, json, asyncio, httpx, re, uuid
from fastapi import FastAPI, UploadFile, File, Form, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates

app = FastAPI(title="Enrichisseur Dirigeants v3")
templates = Jinja2Templates(directory="templates")

ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY    = os.getenv("PAPPERS_API_KEY", "")
FULLENRICH_KEY = os.getenv("FULLENRICH_API_KEY", "")
APOLLO_KEY     = os.getenv("APOLLO_API_KEY", "")

# Titres ciblés pour Apollo
TITRES_APOLLO = [
    "CEO", "Chief Executive Officer",
    "CFO", "Chief Financial Officer", "Directeur Financier", "DAF",
    "COO", "Chief Operating Officer",
    "CMO", "Chief Marketing Officer",
    "CTO", "Chief Technology Officer",
    "President", "Président",
    "Directeur Général", "General Manager",
    "Managing Director", "Gérant",
    "Founder", "Co-Founder", "Associé"
]

# ─── STOCKAGE DISQUE ────────────────────────────────────────────────
JOBS_DIR = "/tmp/jobs"
os.makedirs(JOBS_DIR, exist_ok=True)

def job_path(jid): return f"{JOBS_DIR}/{jid}.json"
def save_job(jid, job):
    with open(job_path(jid), "w") as f: json.dump(job, f)
def load_job(jid):
    p = job_path(jid)
    if not os.path.exists(p): return None
    with open(p) as f: return json.load(f)


# ════════════════════════════════════════════════════════════════════
#  ROUTES UI
# ════════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ════════════════════════════════════════════════════════════════════
#  PARSING
# ════════════════════════════════════════════════════════════════════
def parse_csv_bytes(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig", errors="replace")
    first_line = text.splitlines()[0] if text.splitlines() else ""
    sep = ";" if first_line.count(";") > first_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=sep)
    return [normalize_row({k.strip().lower(): v.strip() for k, v in r.items()}) for r in reader]

def parse_paste(text: str) -> list[dict]:
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if not lines: return []
    sep = ";" if ";" in lines[0] else ","
    reader = csv.DictReader(lines, delimiter=sep)
    return [normalize_row({k.strip().lower(): v.strip() for k, v in r.items()}) for r in reader]

def normalize_row(row: dict) -> dict:
    aliases = {
        "nom":     ["nom","name","société","societe","company","entreprise","organisation"],
        "siren":   ["siren","siret"],
        "domaine": ["domaine","domain","website","site","url"],
    }
    result = {"id": str(uuid.uuid4())}
    for target, keys in aliases.items():
        for k in keys:
            if k in row and row[k] and row[k] != "—":
                result[target] = row[k]; break
        if target not in result: result[target] = ""
    return result


# ════════════════════════════════════════════════════════════════════
#  APOLLO — CHERCHE LES DIRIGEANTS PAR TITRE + DOMAINE
# ════════════════════════════════════════════════════════════════════
async def get_dirigeants_apollo(nom_societe: str, domaine: str, siren: str) -> list[dict]:
    if not APOLLO_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Cherche par domaine si dispo, sinon par nom société
            payload = {
                "api_key": APOLLO_KEY,
                "person_titles": TITRES_APOLLO,
                "page": 1,
                "per_page": 10,
            }
            if domaine:
                payload["q_organization_domains"] = [domaine]
            else:
                payload["q_organization_name"] = nom_societe

            r = await client.post(
                "https://api.apollo.io/api/v1/mixed_people/search",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            if r.status_code != 200:
                return []

            data = r.json()
            people = data.get("people") or data.get("contacts") or []
            dirigeants = []
            for p in people:
                prenom = p.get("first_name", "")
                nom = p.get("last_name", "")
                titre = p.get("title", "")
                email = p.get("email", "") or ""
                # Ignore les emails génériques
                if email and ("@" not in email or "catch" in email.lower()):
                    email = ""
                dirigeants.append({
                    "prenom": prenom,
                    "nom": nom,
                    "titre": titre,
                    "email_apollo": email,
                    "source_dirigeant": "Apollo"
                })
            return dirigeants
    except Exception:
        return []


# ════════════════════════════════════════════════════════════════════
#  PAPPERS — SIREN → DOMAINE (uniquement)
# ════════════════════════════════════════════════════════════════════
async def get_domaine_pappers(siren: str) -> str:
    if not PAPPERS_KEY or not siren:
        return ""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.pappers.fr/v2/entreprise",
                params={"api_token": PAPPERS_KEY, "siren": siren}
            )
            if r.status_code != 200: return ""
            data = r.json()
            return data.get("domaine_url", "") or data.get("site_web", "")
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════════
#  FULLENRICH — EMAIL PAR PERSONNE
# ════════════════════════════════════════════════════════════════════
async def get_email_fullenrich(prenom: str, nom: str, domaine: str, societe: str) -> dict:
    if not FULLENRICH_KEY or not prenom or not nom:
        return {}
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                "https://app.fullenrich.com/api/v1/contact/enrich/bulk",
                headers={"Authorization": f"Bearer {FULLENRICH_KEY}", "Content-Type": "application/json"},
                json={
                    "name": f"{prenom} {nom} - {societe}",
                    "datas": [{
                        "firstname": prenom,
                        "lastname": nom,
                        "company_name": societe,
                        "domain": domaine or "",
                        "enrich_fields": ["contact.emails", "contact.phones"]
                    }]
                }
            )
            if r.status_code not in (200, 201): return {}
            data = r.json()
            enrichment_id = data.get("enrichment_id") or data.get("id")
            if not enrichment_id: return {}

            for _ in range(15):
                await asyncio.sleep(3)
                r2 = await client.get(
                    f"https://app.fullenrich.com/api/v1/contact/enrich/bulk/{enrichment_id}",
                    headers={"Authorization": f"Bearer {FULLENRICH_KEY}"}
                )
                result = r2.json()
                status = result.get("status", "")
                if status in ("finished", "completed", "done"):
                    contacts = result.get("datas") or result.get("data") or []
                    if contacts:
                        c = contacts[0]
                        emails = c.get("emails") or []
                        phones = c.get("phones") or []
                        email_val = emails[0].get("value", "") if emails else ""
                        phone_val = phones[0].get("value", "") if phones else ""
                        return {"email": email_val, "telephone": phone_val,
                                "confiance": "haute" if email_val else "faible",
                                "source_email": "Fullenrich"}
                    break
                elif status in ("failed", "error", "cancelled"):
                    break
            return {}
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════════
#  CLAUDE FALLBACK
# ════════════════════════════════════════════════════════════════════
async def get_email_claude(prenom: str, nom: str, societe: str, domaine: str) -> dict:
    if not ANTHROPIC_KEY: return {}
    prompt = f"""Société: {societe}
{"Dirigeant: " + prenom + " " + nom if prenom else ""}
{"Domaine: " + domaine if domaine else ""}

Trouve l'email professionnel de cette personne.
Réponds UNIQUEMENT avec ce JSON (pas de markdown):
{{"email":"...ou null","domaine":"...ou null","confiance":"haute|moyenne|faible","notes":"..."}}"""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 300,
                    "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            data = r.json()
            text_block = next((b for b in data.get("content", []) if b.get("type") == "text"), None)
            if not text_block: return {}
            m = re.search(r'\{.*?\}', text_block["text"], re.DOTALL)
            if not m: return {}
            parsed = json.loads(m.group())
            parsed["source_email"] = "Claude"
            return parsed
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════════
#  ENRICHISSEMENT PRINCIPAL
# ════════════════════════════════════════════════════════════════════
async def enrich_societe(row: dict) -> list[dict]:
    nom_societe = row.get("nom", "")
    siren = row.get("siren", "")
    domaine = row.get("domaine", "")

    # Pappers = domaine uniquement si manquant
    if not domaine and siren:
        domaine = await get_domaine_pappers(siren)

    # Apollo = tous les dirigeants par titre
    dirigeants = await get_dirigeants_apollo(nom_societe, domaine, siren)

    if not dirigeants:
        dirigeants = [{"prenom": "", "nom": "", "titre": "", "email_apollo": "", "source_dirigeant": ""}]

    results = []
    for dg in dirigeants:
        prenom = dg.get("prenom", "")
        nom_dg = dg.get("nom", "")
        titre = dg.get("titre", "")
        email_apollo = dg.get("email_apollo", "")

        result = {
            "societe": nom_societe,
            "siren": siren,
            "domaine": domaine,
            "prenom": prenom,
            "nom_dg": nom_dg,
            "titre": titre,
            "email": "",
            "telephone": "",
            "confiance": "",
            "source": dg.get("source_dirigeant", ""),
            "notes": ""
        }

        # Email déjà dans Apollo ?
        if email_apollo:
            result["email"] = email_apollo
            result["confiance"] = "haute"
            result["source"] += " + email Apollo"
        elif prenom and nom_dg:
            # Fullenrich
            fe = await get_email_fullenrich(prenom, nom_dg, domaine, nom_societe)
            if fe:
                result["email"] = fe.get("email", "")
                result["telephone"] = fe.get("telephone", "")
                result["confiance"] = fe.get("confiance", "")
                result["source"] += (" + " if result["source"] else "") + fe.get("source_email", "")
            # Claude fallback
            if not result["email"]:
                claude = await get_email_claude(prenom, nom_dg, nom_societe, domaine)
                if claude:
                    result["email"] = claude.get("email", "")
                    result["confiance"] = claude.get("confiance", "")
                    result["notes"] = claude.get("notes", "")
                    result["source"] += (" + " if result["source"] else "") + "Claude"
                    if not domaine and claude.get("domaine"):
                        result["domaine"] = claude["domaine"]
        else:
            claude = await get_email_claude("", "", nom_societe, domaine)
            if claude:
                result["email"] = claude.get("email", "")
                result["notes"] = claude.get("notes", "")
                result["source"] = "Claude"

        results.append(result)
        await asyncio.sleep(0.2)

    return results


# ════════════════════════════════════════════════════════════════════
#  JOBS
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
    save_job(job_id, {"rows": rows, "results": [], "status": "pending", "progress": 0, "total": len(rows)})
    background_tasks.add_task(run_job, job_id)
    return {"job_id": job_id, "total": len(rows)}

async def run_job(job_id: str):
    job = load_job(job_id)
    if not job: return
    job["status"] = "running"
    save_job(job_id, job)
    for i, row in enumerate(job["rows"]):
        results = await enrich_societe(row)
        job = load_job(job_id)
        job["results"].extend(results)
        job["progress"] = i + 1
        save_job(job_id, job)
        await asyncio.sleep(0.3)
    job = load_job(job_id)
    job["status"] = "done"
    save_job(job_id, job)

@app.get("/status/{job_id}")
async def status(job_id: str):
    job = load_job(job_id)
    if not job: return JSONResponse({"error": "Job introuvable"}, status_code=404)
    return {"status": job["status"], "progress": job["progress"], "total": job["total"], "results": job["results"]}

@app.get("/export/{job_id}")
async def export(job_id: str):
    job = load_job(job_id)
    if not job or not job["results"]:
        return JSONResponse({"error": "Aucun résultat"}, status_code=404)
    output = io.StringIO()
    fields = ["societe","siren","domaine","prenom","nom_dg","titre","email","telephone","confiance","source","notes"]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(job["results"])
    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=enrichissement_{job_id[:8]}.csv"}
    )
