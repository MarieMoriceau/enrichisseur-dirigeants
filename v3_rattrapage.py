"""
Enrichisseur v3 — rattrapage du contrôle des contacts CRM pour les sociétés déjà en mémoire

Pour les sociétés traitées en « Sociétés + dirigeants » ou « Tout » avant l'ajout de l'étape 4b
(ou dont des contacts « à vérifier » n'ont pas encore eu leur profil LinkedIn lu) :
  1. relit les dirigeants Sales Navigator gardés dans la mémoire Notion
  2. récupère les contacts Pipedrive de la société et les rapproche → « toujours là » / « à vérifier »,
     et cherche les nouveaux dirigeants dans tout Pipedrive (déjà fichés chez une autre société → à rattacher)
  3. lit les profils LinkedIn des « à vérifier » (phantom Profile Scraper, PROFILS_MAX au plus) → « probablement parti »
Aucun crédit Kaspr / Fullenrich, pas de Sales Navigator.

Essai à blanc d'abord (50 sociétés) : rien n'est écrit dans Notion, un Excel montre le résultat.
Le passage réel n'est possible qu'après un essai à blanc terminé.
"""
import asyncio, io, json, time
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from openpyxl import Workbook

import v3_memoire as MEM
import v3_linkedin as LI
import v3_validation as VALID

router = APIRouter()
ESSAI_TAILLE = 50
ETAT: dict = {"en_cours": False, "essai_ok": False, "mode": "", "phase": "", "journal": [], "lignes": [], "fini": "",
              "stop": False}


def _journal(msg: str):
    ETAT["journal"].append(f"{time.strftime('%H:%M:%S')} {msg}")
    ETAT["journal"] = ETAT["journal"][-40:]
    print(f"[V3 RATTRAPAGE] {msg}")


def a_rattraper(data: dict) -> bool:
    """Contrôle jamais fait, ou des « à vérifier » dont le profil LinkedIn n'a pas encore été lu (si le phantom est branché)."""
    if not str(data.get("id") or "").isdigit():
        return False                                   # pas d'ID Pipedrive : rien à contrôler
    if "contacts_crm" not in data:
        return True
    if any(not d.get("dans_pipedrive") and "ailleurs_crm" not in d for d in data.get("dirigeants", [])):
        return True                                    # nouveaux dirigeants pas encore cherchés dans tout Pipedrive
    return bool(LI.PB_PROFILS) and any(p.get("statut") == "à vérifier" and p.get("linkedin") and "profil" not in p.get("detail", "")
                                       for p in data["contacts_crm"])


async def _reecrire(c: httpx.AsyncClient, page: str, data: dict):
    """Remplace le JSON de la page (blocs code) sans changer de page : validation et décisions restent en place."""
    curseur, anciens = None, []
    while True:
        r = await MEM._req(c, "GET", f"https://api.notion.com/v1/blocks/{page}/children",
                           params={"page_size": 100, **({"start_cursor": curseur} if curseur else {})})
        j = r.json()
        anciens += [b["id"] for b in j.get("results", []) if b.get("type") == "code"]
        if not j.get("has_more"):
            break
        curseur = j.get("next_cursor")
    txt = json.dumps({k: v for k, v in data.items() if k not in MEM.A_JETER}, ensure_ascii=False, default=str)
    blocs = [{"object": "block", "type": "code", "code": {"language": "json",
              "rich_text": [{"type": "text", "text": {"content": txt[i:i + 1900]}}]}} for i in range(0, len(txt), 1900)]
    r = await MEM._req(c, "PATCH", f"https://api.notion.com/v1/blocks/{page}/children", json={"children": blocs[:100]})
    if r.status_code != 200:
        raise RuntimeError(f"écriture refusée ({r.status_code})")
    for b in anciens:                                  # l'ancien JSON n'est supprimé qu'une fois le nouveau écrit
        await MEM._req(c, "DELETE", f"https://api.notion.com/v1/blocks/{b}")
    await MEM._req(c, "PATCH", f"https://api.notion.com/v1/pages/{page}", json={"properties": {
        "Nb nouveaux contacts": {"number": sum(1 for d in data.get("dirigeants", []) if not d.get("dans_pipedrive"))}}})


async def _tourner(essai: bool):
    from v3 import ressemblance                        # import tardif (v3 importe ce module)
    ETAT.update(en_cours=True, mode="essai à blanc" if essai else "réel", journal=[], lignes=[], fini="", stop=False)
    try:
        ETAT["phase"] = "Lecture de la mémoire Notion…"
        pages = [s for s in await VALID._toutes() if s["perimetre"] in ("dirigeants", "tout")]
        cibles = []
        for s in pages:
            if ETAT["stop"] or (essai and len(cibles) >= ESSAI_TAILLE):
                break
            data = await MEM.lire(s["page"])
            if data and a_rattraper(data):
                data["_page"] = data.get("_page") or s["page"]
                cibles.append(data)
        _journal(f"{len(cibles)} sociétés à rattraper ({len(pages)} sociétés avec dirigeants en mémoire)"
                 + (f", essai limité à {ESSAI_TAILLE}" if essai else ""))
        if not cibles:
            ETAT["fini"] = "rien à rattraper"
            return
        if not LI.PB_PROFILS:
            _journal("PB_PROFILE_SCRAPER_ID absent : les contacts absents de Sales Navigator restent « à vérifier »")
        job = {"id": f"rattrapage_{int(time.time())}", "phase": ""}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            pb = LI.PhantomBuster(_journal)
            tache = asyncio.create_task(LI.controler_contacts_crm(job, cibles, pb, c, ressemblance, _journal, lambda: ETAT["stop"]))
            while not tache.done():
                ETAT["phase"] = job.get("phase", "")
                await asyncio.sleep(1)
            await tache
            if job.get("profils_lus"):
                ETAT["profils_lus"] = job["profils_lus"]
            for r in cibles:
                for p in r.get("contacts_crm", []):
                    ETAT["lignes"].append([r.get("id", ""), r.get("nom", ""), p["id"], p["nom_complet"], p["fonction"],
                                           p["email"], p["linkedin"], p["statut"], p["detail"]])
            if not essai and not ETAT["stop"]:
                ok = 0
                for i, r in enumerate(cibles, 1):
                    ETAT["phase"] = f"Écriture dans Notion {i}/{len(cibles)}…"
                    page = r.pop("_page")
                    try:
                        await _reecrire(c, page, r)
                        ok += 1
                    except Exception as e:
                        _journal(f"{r.get('nom')} : {e}")
                _journal(f"{ok} sociétés mises à jour dans Notion")
        n = lambda s: sum(1 for l in ETAT["lignes"] if l[7] == s)
        ETAT["fini"] = (f"{len(cibles)} sociétés, {len(ETAT['lignes'])} contacts CRM : {n('probablement parti')} probablement partis, "
                        f"{n('à vérifier')} à vérifier, {n('toujours là')} toujours là")
        if essai and not ETAT["stop"]:
            ETAT["essai_ok"] = True
    except Exception as e:
        _journal(f"erreur : {e}")
        ETAT["fini"] = f"erreur : {e}"
    finally:
        ETAT.update(en_cours=False, phase="")


@router.post("/v3/validation/api/rattrapage")
async def lancer(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    b = await req.json()
    essai = b.get("mode") != "réel"
    if ETAT["en_cours"]:
        return JSONResponse({"erreur": "un rattrapage tourne déjà"}, 409)
    if not essai and not ETAT["essai_ok"]:
        return JSONResponse({"erreur": "faites d'abord l'essai à blanc sur 50 sociétés et vérifiez l'Excel"}, 409)
    asyncio.create_task(_tourner(essai))
    return {"ok": True}


@router.post("/v3/validation/api/rattrapage/stop")
async def arreter(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    ETAT["stop"] = True
    return {"ok": True}


@router.get("/v3/validation/api/rattrapage")
async def etat(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    return {k: ETAT.get(k) for k in ("en_cours", "essai_ok", "mode", "phase", "fini")} | {
        "journal": ETAT["journal"][-8:], "nb_lignes": len(ETAT["lignes"]), "profil_branche": bool(LI.PB_PROFILS)}


@router.get("/v3/validation/api/rattrapage/excel")
async def excel(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    from v3 import feuille
    wb = Workbook()
    ws = wb.active
    ws.title = "Lisez-moi"
    for l in [f"Rattrapage du contrôle des contacts CRM ({ETAT['mode']})", ETAT["fini"],
              "Essai à blanc : rien n'a été écrit dans Notion." if ETAT["mode"] == "essai à blanc" else "Passage réel : la mémoire Notion a été mise à jour.",
              "probablement parti : le profil LinkedIn montre un autre employeur",
              "à vérifier : absent des dirigeants Sales Navigator (profil non lu, ou pas d'URL LinkedIn dans Pipedrive)",
              "toujours là : présent dans Sales Navigator, ou profil LinkedIn chez la société", "", "Journal :"] + ETAT["journal"]:
        ws.append([l])
    ordre = {"probablement parti": 0, "à vérifier": 1, "toujours là": 2}
    feuille(wb, "Contacts CRM", ["ID Pipedrive société", "Société", "ID personne", "Nom", "Fonction CRM", "E-mail CRM", "LinkedIn",
                                 "Statut", "Détail"], sorted(ETAT["lignes"], key=lambda l: (ordre.get(l[7], 3), l[1])))
    b = io.BytesIO()
    wb.save(b)
    nom = f"rattrapage_contacts_crm_{'essai' if ETAT['mode'] == 'essai à blanc' else 'reel'}_{datetime.now(timezone.utc):%Y%m%d_%H%M}.xlsx"
    return StreamingResponse(io.BytesIO(b.getvalue()), headers={"Content-Disposition": f'attachment; filename="{nom}"'},
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
