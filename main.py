import os, io, csv, json, asyncio, httpx, re, uuid
from fastapi import FastAPI, UploadFile, File, Form, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import Optional

app = FastAPI(title="Enrichisseur Dirigeants v2")
templates = Jinja2Templates(directory="templates")

ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY    = os.getenv("PAPPERS_API_KEY", "")
FULLENRICH_KEY = os.getenv("FULLENRICH_API_KEY", "")

# Titres qui indiquent un dirigeant ou associé
TITRES_CIBLES = [
    "président", "directeur général", "gérant", "associé", "administrateur",
    "directeur financier", "daf", "dg", "pdg", "ceo", "cfo", "coo",
    "vice-président", "directeur général délégué"
]

jobs: dict[str, dict] = {}


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
    # Détecte le séparateur
    first_line = text.splitlines()[0] if text.splitlines() else ""
    sep = ";" if first_line.count(";") > first_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=sep)
    return [normalize_row({k.strip().lower(): v.strip() for k, v in r.items()}) for r in reader]

def parse_paste(text: str) -> list[dict]:
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if not lines:
        return []
    sep = ";" if ";" in lines[0] else ","
    reader = csv.DictReader(lines, delimiter=sep)
    return [normalize_row({k.strip().lower(): v.strip() for k, v in r.items()}) for r in reader]

def normalize_row(row: dict) -> dict:
    aliases = {
        "nom":      ["nom","name","société","societe","company","entreprise","organisation"],
        "siren":    ["siren","siret"],
        "domaine":  ["domaine","domain","website","site","url"],
    }
    result = {"id": str(uuid.uuid4())}
    for target, keys in aliases.items():
        for k in keys:
            if k in row and row[k] and row[k] != "—":
                result[target] = row[k]
                break
        if target not in result:
            result[target] = ""
    return result


# ════════════════════════════════════════════════════════════════════
#  PAPPERS — TOUS LES MANDATAIRES
# ════════════════════════════════════════════════════════════════════
async def get_dirigeants_pappers(siren: str) -> list[dict]:
    """Retourne tous les mandataires sociaux via Pappers API"""
    if not PAPPERS_KEY or not siren:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.pappers.fr/v2/entreprise",
                params={"api_token": PAPPERS_KEY, "siren": siren}
            )
            if r.status_code != 200:
                return []
            data = r.json()

            dirigeants = []
            # Représentants légaux
            for rep in data.get("representants", []):
                qualite = rep.get("qualite", "").lower()
                # Filtre : on prend DG, Gérant, Président, Associés
                if any(t in qualite for t in TITRES_CIBLES) or "associé" in qualite:
                    dirigeants.append({
                        "prenom": rep.get("prenom", ""),
                        "nom": rep.get("nom", "") or rep.get("denomination", ""),
                        "titre": rep.get("qualite", ""),
                        "source_dirigeant": "Pappers"
                    })

            # Bénéficiaires effectifs (associés)
            for ben in data.get("beneficiaires_effectifs", []):
                prenom = ben.get("prenom", "")
                nom = ben.get("nom", "")
                # Eviter les doublons
                if not any(d["prenom"] == prenom and d["nom"] == nom for d in dirigeants):
                    dirigeants.append({
                        "prenom": prenom,
                        "nom": nom,
                        "titre": f"Associé ({ben.get('pourcentage_parts', '?')}%)",
                        "source_dirigeant": "Pappers"
                    })

            # Récupère aussi le domaine si pas connu
            domaine = data.get("domaine_url", "") or data.get("site_web", "")

            return dirigeants, domaine

    except Exception as e:
        return [], ""


# ════════════════════════════════════════════════════════════════════
#  FULLENRICH — EMAIL PAR PERSONNE
# ════════════════════════════════════════════════════════════════════
async def get_email_fullenrich(prenom: str, nom: str, domaine: str, societe: str) -> dict:
    if not FULLENRICH_KEY or not prenom or not nom:
        return {}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.fullenrich.com/v1/enrichments",
                headers={
                    "Authorization": f"Bearer {FULLENRICH_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "name": f"{prenom} {nom} - {societe}",
                    "datas": [{
                        "first_name": prenom,
                        "last_name": nom,
                        "company_name": societe,
                        "domain": domaine or ""
                    }]
                }
            )
            if r.status_code not in (200, 201):
                return {}

            data = r.json()
            enrichment_id = data.get("id")
            if not enrichment_id:
                return {}

            # Poll le résultat (max 30s)
            for _ in range(10):
                await asyncio.sleep(3)
                r2 = await client.get(
                    f"https://api.fullenrich.com/v1/enrichments/{enrichment_id}",
                    headers={"Authorization": f"Bearer {FULLENRICH_KEY}"}
                )
                result = r2.json()
                status = result.get("status")
                if status == "completed":
                    contacts = result.get("data", [])
                    if contacts:
                        c = contacts[0]
                        emails = c.get("emails", [])
                        phones = c.get("phones", [])
                        return {
                            "email": emails[0].get("value", "") if emails else "",
                            "telephone": phones[0].get("value", "") if phones else "",
                            "confiance": "haute" if emails else "faible",
                            "source_email": "Fullenrich"
                        }
                    break
                elif status == "failed":
                    break
            return {}
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════════
#  CLAUDE FALLBACK — EMAIL + DAF
# ════════════════════════════════════════════════════════════════════
async def get_email_claude(prenom: str, nom: str, societe: str, domaine: str) -> dict:
    if not ANTHROPIC_KEY:
        return {}
    prompt = f"""Société: {societe}
Dirigeant: {prenom} {nom}
{"Domaine: " + domaine if domaine else ""}

Trouve l'email professionnel de cette personne.
Réponds UNIQUEMENT avec ce JSON (pas de markdown):
{{"email":"...ou null","domaine":"...ou null","confiance":"haute|moyenne|faible","notes":"..."}}"""

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
                    "max_tokens": 300,
                    "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            data = r.json()
            text_block = next((b for b in data.get("content", []) if b.get("type") == "text"), None)
            if not text_block:
                return {}
            m = re.search(r'\{.*?\}', text_block["text"], re.DOTALL)
            if not m:
                return {}
            parsed = json.loads(m.group())
            parsed["source_email"] = "Claude"
            return parsed
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════════
#  ENRICHISSEMENT PRINCIPAL
# ════════════════════════════════════════════════════════════════════
async def enrich_societe(row: dict) -> list[dict]:
    """
    Pour une société, retourne une liste de résultats (un par dirigeant)
    """
    nom_societe = row.get("nom", "")
    siren = row.get("siren", "")
    domaine = row.get("domaine", "")

    results = []

    # 1. Récupère tous les dirigeants via Pappers
    dirigeants = []
    pappers_domaine = ""
    if siren:
        dirigeants, pappers_domaine = await get_dirigeants_pappers(siren)
        if pappers_domaine and not domaine:
            domaine = pappers_domaine

    # Si Pappers n'a rien → on met au moins une ligne avec Claude fallback
    if not dirigeants:
        dirigeants = [{"prenom": "", "nom": "", "titre": "", "source_dirigeant": ""}]

    # 2. Pour chaque dirigeant, cherche l'email
    for dg in dirigeants:
        prenom = dg.get("prenom", "")
        nom_dg = dg.get("nom", "")
        titre = dg.get("titre", "")

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

        if prenom and nom_dg:
            # Fullenrich
            fe = await get_email_fullenrich(prenom, nom_dg, domaine, nom_societe)
            if fe:
                result["email"] = fe.get("email", "")
                result["telephone"] = fe.get("telephone", "")
                result["confiance"] = fe.get("confiance", "")
                result["source"] += (" + " if result["source"] else "") + fe.get("source_email", "")

            # Claude fallback si pas d'email
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
            # Pas de dirigeant connu → Claude cherche tout
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
    jobs[job_id] = {"rows": rows, "results": [], "status": "pending", "progress": 0, "total": len(rows)}
    background_tasks.add_task(run_job, job_id)
    return {"job_id": job_id, "total": len(rows)}


async def run_job(job_id: str):
    job = jobs[job_id]
    job["status"] = "running"
    for i, row in enumerate(job["rows"]):
        results = await enrich_societe(row)
        job["results"].extend(results)
        job["progress"] = i + 1
        await asyncio.sleep(0.3)
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
    fields = ["societe", "siren", "domaine", "prenom", "nom_dg", "titre", "email", "telephone", "confiance", "source", "notes"]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(job["results"])

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=enrichissement_{job_id[:8]}.csv"}
    )
