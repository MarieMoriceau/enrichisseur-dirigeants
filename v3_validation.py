"""
Enrichisseur v3 — onglet Validation (réservé à Marie, les négociateurs n'y ont pas accès)
  Écran Sociétés : adresses CRM / site / LinkedIn / Google, recommandation de l'outil, effectif LinkedIn, domiciliation.
                   On coche l'adresse retenue (ou on en saisit une), puis on valide ou on écarte la société.
  Écran Contacts : ouvert une fois la société validée. Nouveaux dirigeants à créer, contacts du CRM
                   « probablement parti » / « à vérifier » / « toujours là ».
Les décisions sont gardées dans la mémoire Notion (« Enrichisseur v3 — Mémoire des sociétés »).
Rien n'est envoyé dans un CRM : l'envoi HubSpot viendra après la migration de fin octobre,
avec un test à blanc sur 50 lignes avant tout envoi réel.
"""
import json, re
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

import v3_memoire as MEM

router = APIRouter()
STATUTS = ("À valider", "Société validée", "Contacts validés", "Écartée")
SOURCES = ("CRM", "Site", "LinkedIn", "Google", "Saisie")
ACTIONS = {"nouveau": ("créer", "ignorer"), "crm": ("garder", "marquer parti", "à revoir")}
RE_PAGE = re.compile(r"[0-9a-f\-]{32,36}")


def _props(pg: dict) -> dict:
    p = pg.get("properties") or {}
    nb = lambda n: (p.get(n) or {}).get("number")
    return {"page": pg["id"], "nom": MEM._val(p, "Société"), "siren": MEM._val(p, "SIREN"), "domaine": MEM._val(p, "Domaine"),
            "perimetre": MEM._val(p, "Périmètre"), "traitee": MEM._val(p, "Traitée le")[:10], "lancement": MEM._val(p, "Lancement"),
            "effectif": nb("Effectif LinkedIn"), "nb_dirigeants": nb("Nb dirigeants"),
            "validation": MEM._val(p, "Validation") or "À valider", "adresse_retenue": MEM._val(p, "Adresse retenue"),
            "source_retenue": MEM._val(p, "Source adresse retenue"), "validee": MEM._val(p, "Validée le")[:10],
            "decisions": MEM._val(p, "Décisions contacts"), "hubspot": MEM._val(p, "Envoyée à HubSpot le")[:10]}


@router.get("/v3/validation", response_class=HTMLResponse)
async def page():
    with open("templates/validation.html", encoding="utf-8") as f:
        return f.read()


@router.get("/v3/validation/api/societes")
async def societes():
    if not MEM.NOTION_KEY:
        return JSONResponse({"erreur": "NOTION_API_KEY absente"}, 500)
    out, curseur = [], None
    async with httpx.AsyncClient(timeout=30) as c:
        while True:
            corps = {"sorts": [{"property": "Traitée le", "direction": "descending"}], "page_size": 100}
            if curseur:
                corps["start_cursor"] = curseur
            r = await MEM._req(c, "POST", f"https://api.notion.com/v1/databases/{MEM.MEMOIRE_DB}/query", json=corps)
            if r.status_code != 200:
                return JSONResponse({"erreur": f"mémoire Notion inaccessible ({r.status_code})"}, 502)
            j = r.json()
            out += [_props(pg) for pg in j.get("results", [])]
            if not j.get("has_more"):
                break
            curseur = j.get("next_cursor")
    for s in out:
        s.pop("decisions", None)                     # inutile dans la liste
    return {"societes": out}


@router.get("/v3/validation/api/societe/{page_id}")
async def societe(page_id: str):
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "GET", f"https://api.notion.com/v1/pages/{page_id}")
    if r.status_code != 200:
        return JSONResponse({"erreur": "société introuvable (retraitée depuis ? rechargez la liste)"}, 404)
    info = _props(r.json())
    try:
        info["decisions"] = json.loads(info["decisions"]) if info["decisions"] else []
    except ValueError:
        info["decisions"] = []
    info["data"] = await MEM.lire(page_id)
    return info


async def _maj(page_id: str, props: dict):
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "PATCH", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    if r.status_code != 200:
        return JSONResponse({"erreur": f"Notion a refusé la mise à jour ({r.status_code}) : {r.text[:200]}"}, 502)
    return _props(r.json())


@router.post("/v3/validation/api/societe/{page_id}")
async def valider_societe(page_id: str, req: Request):
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    statut = b.get("statut", "")
    if statut not in ("Société validée", "Écartée", "À valider"):
        return JSONResponse({"erreur": "statut inconnu"}, 400)
    adresse = re.sub(r"\s+", " ", str(b.get("adresse") or "")).strip()
    source = b.get("source", "")
    if statut == "Société validée" and not adresse:
        return JSONResponse({"erreur": "cochez ou saisissez l'adresse retenue"}, 400)
    if source and source not in SOURCES:
        return JSONResponse({"erreur": "source inconnue"}, 400)
    props = {"Validation": {"select": {"name": statut}},
             "Validée le": {"date": {"start": datetime.now(timezone.utc).isoformat(timespec="seconds")} if statut != "À valider" else None}}
    if statut == "Société validée":
        props["Adresse retenue"] = MEM._txt(adresse)
        props["Source adresse retenue"] = {"select": {"name": source}} if source else {"select": None}
    return await _maj(page_id, props)


@router.post("/v3/validation/api/contacts/{page_id}")
async def valider_contacts(page_id: str, req: Request):
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "GET", f"https://api.notion.com/v1/pages/{page_id}")
    if r.status_code != 200:
        return JSONResponse({"erreur": "société introuvable"}, 404)
    if _props(r.json())["validation"] not in ("Société validée", "Contacts validés"):
        return JSONResponse({"erreur": "validez d'abord la société"}, 409)
    b = await req.json()
    decisions = []
    for d in b.get("decisions") or []:
        t, a = d.get("type"), d.get("action")
        if t not in ACTIONS or a not in ACTIONS[t]:
            return JSONResponse({"erreur": f"décision invalide : {t} / {a}"}, 400)
        decisions.append({k: str(d.get(k) or "")[:300] for k in
                          ("type", "action", "id", "prenom", "nom", "nom_complet", "fonction", "email", "linkedin", "statut")})
    return await _maj(page_id, {"Validation": {"select": {"name": "Contacts validés"}},
                                "Décisions contacts": MEM._txts(json.dumps(decisions, ensure_ascii=False))})
