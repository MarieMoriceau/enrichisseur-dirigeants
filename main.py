import os, io, csv, json, asyncio, httpx, re, uuid
from fastapi import FastAPI, UploadFile, File, Form, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates

app = FastAPI(title="Enrichisseur Dirigeants v4")
templates = Jinja2Templates(directory="templates")

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PAPPERS_KEY   = os.getenv("PAPPERS_API_KEY", "")

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
#  PAPPERS — MANDATAIRES LÉGAUX + DOMAINE
# ════════════════════════════════════════════════════════════════════
async def get_pappers(siren: str) -> tuple[list[dict], str]:
    if not PAPPERS_KEY or not siren:
        return [], ""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.pappers.fr/v2/entreprise",
                params={"api_token": PAPPERS_KEY, "siren": siren}
            )
            if r.status_code != 200: return [], ""
            data = r.json()
            dirigeants = []
            for rep in data.get("representants", []):
                prenom = rep.get("prenom", "")
                nom = rep.get("nom", "") or rep.get("denomination", "")
                if prenom or nom:
                    dirigeants.append({
                        "prenom": prenom,
                        "nom": nom,
                        "titre": rep.get("qualite", ""),
                        "source": "Pappers"
                    })
            for ben in data.get("beneficiaires_effectifs", []):
                prenom = ben.get("prenom", "")
                nom = ben.get("nom", "")
                if prenom and nom and not any(d["prenom"]==prenom and d["nom"]==nom for d in dirigeants):
                    dirigeants.append({
                        "prenom": prenom,
                        "nom": nom,
                        "titre": f"Associé ({ben.get('pourcentage_parts','?')}%)",
                        "source": "Pappers"
                    })
            domaine = data.get("domaine_url","") or data.get("site_web","")
            return dirigeants, domaine
    except Exception:
        return [], ""


# ════════════════════════════════════════════════════════════════════
#  CLAUDE — TROUVE TOUS LES DIRIGEANTS + EMAILS VIA WEB SEARCH
# ════════════════════════════════════════════════════════════════════
async def claude_find_all(nom_societe: str, domaine: str, siren: str, dirigeants_pappers: list[dict]) -> list[dict]:
    """
    Claude cherche sur le web TOUS les dirigeants opérationnels
    (CEO, CFO, COO, Partners, Associés...) + leurs emails
    """
    if not ANTHROPIC_KEY:
        return []

    pappers_info = ""
    if dirigeants_pappers:
        pappers_info = "Dirigeants légaux déjà connus (Pappers) :\n" + \
            "\n".join([f"- {d['prenom']} {d['nom']} ({d['titre']})" for d in dirigeants_pappers])

    prompt = f"""Tu es un assistant B2B expert en recherche de contacts de dirigeants de sociétés françaises.

Société : {nom_societe}
{"SIREN : " + siren if siren else ""}
{"Domaine web : " + domaine if domaine else ""}
{pappers_info}

MISSION : Trouve TOUS les dirigeants opérationnels de cette société :
- CEO / Directeur Général / PDG
- CFO / DAF / Directeur Financier  
- COO / Directeur des Opérations
- CMO / Directeur Marketing
- CTO / Directeur Technique
- Partners / Associés (cabinets)
- Managing Director
- Tout autre C-level ou associé visible

Pour chaque personne trouvée, cherche aussi son email professionnel.

Utilise la recherche web pour trouver ces informations sur :
- Le site officiel de la société
- LinkedIn
- Societe.com, Pappers, Infogreffe
- Articles de presse, communiqués

Réponds UNIQUEMENT avec ce JSON (aucun texte avant ou après) :
{{
  "domaine": "domaine.com ou null",
  "contacts": [
    {{
      "prenom": "...",
      "nom": "...",
      "titre": "CEO/CFO/Partner/etc",
      "email": "...ou null",
      "confiance_email": "haute|moyenne|faible",
      "source": "site officiel|LinkedIn|presse|etc"
    }}
  ]
}}"""

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 2000,
                    "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            data = r.json()
            text_block = next((b for b in data.get("content", []) if b.get("type") == "text"), None)
            if not text_block: return []

            m = re.search(r'\{[\s\S]*\}', text_block["text"])
            if not m: return []

            parsed = json.loads(m.group())
            contacts = parsed.get("contacts", [])
            domaine_trouve = parsed.get("domaine", "")

            results = []
            for c in contacts:
                results.append({
                    "prenom": c.get("prenom", ""),
                    "nom": c.get("nom", ""),
                    "titre": c.get("titre", ""),
                    "email": c.get("email", "") or "",
                    "confiance": c.get("confiance_email", ""),
                    "source": "Claude (" + c.get("source", "web") + ")",
                    "domaine_trouve": domaine_trouve
                })
            return results
    except Exception:
        return []


# ════════════════════════════════════════════════════════════════════
#  ENRICHISSEMENT PRINCIPAL
# ════════════════════════════════════════════════════════════════════
async def enrich_societe(row: dict) -> list[dict]:
    nom_societe = row.get("nom", "")
    siren = row.get("siren", "")
    domaine = row.get("domaine", "")

    # 1. Pappers → mandataires légaux + domaine
    dirigeants_pappers, pappers_domaine = await get_pappers(siren)
    if pappers_domaine and not domaine:
        domaine = pappers_domaine

    # 2. Claude → tous les dirigeants opérationnels + emails
    claude_contacts = await claude_find_all(nom_societe, domaine, siren, dirigeants_pappers)

    # Si Claude a trouvé un domaine qu'on n'avait pas
    if not domaine and claude_contacts:
        domaine = claude_contacts[0].get("domaine_trouve", "") or domaine

    # 3. Fusion : Claude en priorité, Pappers en complément
    results = []

    # Contacts Claude
    for c in claude_contacts:
        results.append({
            "societe": nom_societe,
            "siren": siren,
            "domaine": domaine,
            "prenom": c.get("prenom", ""),
            "nom_dg": c.get("nom", ""),
            "titre": c.get("titre", ""),
            "email": c.get("email", ""),
            "telephone": "",
            "confiance": c.get("confiance", ""),
            "source": c.get("source", "Claude"),
            "notes": ""
        })

    # Ajoute les mandataires Pappers non déjà présents dans Claude
    noms_claude = {(r["prenom"].lower(), r["nom_dg"].lower()) for r in results}
    for dg in dirigeants_pappers:
        key = (dg["prenom"].lower(), dg["nom"].lower())
        if key not in noms_claude:
            results.append({
                "societe": nom_societe,
                "siren": siren,
                "domaine": domaine,
                "prenom": dg["prenom"],
                "nom_dg": dg["nom"],
                "titre": dg["titre"],
                "email": "",
                "telephone": "",
                "confiance": "",
                "source": "Pappers",
                "notes": "Email non trouvé"
            })

    # Si rien du tout
    if not results:
        results.append({
            "societe": nom_societe,
            "siren": siren,
            "domaine": domaine,
            "prenom": "", "nom_dg": "", "titre": "",
            "email": "", "telephone": "",
            "confiance": "faible",
            "source": "",
            "notes": "Aucun contact trouvé"
        })

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
        await asyncio.sleep(0.5)
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
