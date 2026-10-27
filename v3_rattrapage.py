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


def corrections_li(decisions_txt: str) -> dict:
    """URL LinkedIn saisies par l'admin sur les contacts CRM : {id personne Pipedrive: URL}."""
    try:
        dec = json.loads(decisions_txt) if decisions_txt else []
    except ValueError:
        return {}
    return {str(x.get("id")): x["linkedin"] for x in dec if (x.get("type") or "crm") == "crm" and "linkedin.com/in/" in str(x.get("linkedin") or "")}


def a_rattraper(data: dict) -> bool:
    """Contrôle jamais fait, ou des « à vérifier » dont le profil LinkedIn n'a pas encore été lu (si le phantom est branché)."""
    if not str(data.get("id") or "").isdigit():
        return False                                   # pas d'ID Pipedrive : rien à contrôler
    if "contacts_crm" not in data:
        return True
    corr = data.get("_corrections_li") or {}
    if LI.PB_PROFILS and any(p.get("statut") == "à vérifier" and not p.get("linkedin") and corr.get(str(p.get("id")))
                             for p in data["contacts_crm"]):
        return True                                    # URL ajoutée à la main depuis le dernier passage
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
            if data:
                data["_corrections_li"] = corrections_li(s.get("decisions", ""))
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
                    r.pop("_corrections_li", None)
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


# ───────── contacts Pipedrive jamais lus (société sans page LinkedIn, avant le 26/10) ─────────
# Tout se fait en tâche de fond (analyse de ~600 fiches Notion : trop long pour une seule requête web) ;
# rappeler l'adresse pour voir où on en est.
MANQUANTS = {"actif": False, "phase": "", "analysees": 0, "total": 0, "a_lire": None, "exemples": [], "fait": 0, "ok": 0,
             "contacts": 0, "echecs": [], "illisibles": 0, "_cibles": [], "_quand": 0.0}


async def _lire_page(s):
    for essai in range(3):
        try:
            return await MEM.lire(s["page"])
        except Exception:
            await asyncio.sleep(2 + essai * 3)
    return None


async def _analyser():
    socs = [s for s in await VALID._toutes() if s["validation"] != "Écartée" and s["perimetre"] in ("dirigeants", "tout")]
    MANQUANTS.update(total=len(socs), analysees=0, illisibles=0)
    sem, cibles = asyncio.Semaphore(3), []
    async def voir(s):
        async with sem:
            d = await _lire_page(s)
        MANQUANTS["analysees"] += 1
        if d is None:
            MANQUANTS["illisibles"] += 1
        elif d and str(d.get("id") or "").isdigit() and "contacts_crm" not in d:
            cibles.append((s, d))
    await asyncio.gather(*(voir(s) for s in socs))
    return cibles


async def _tache_manquants(confirmer: bool):
    MANQUANTS.update(actif=True, phase="analyse des fiches", fait=0, ok=0, contacts=0, echecs=[])
    try:
        cibles = await _analyser()
        MANQUANTS.update(a_lire=len(cibles), exemples=[s["nom"] for s, _ in cibles[:40]], _cibles=cibles, _quand=time.time())
        if not confirmer:
            MANQUANTS["phase"] = "essai à blanc terminé : rien n'a été écrit"
            return
        MANQUANTS["phase"] = "lecture Pipedrive et enregistrement"
        async with httpx.AsyncClient(timeout=30) as c:
            for s, data in cibles:
                try:
                    data["contacts_crm"] = await LI.personnes_fiches(c, data)
                    for p in data["contacts_crm"]:
                        p["statut"], p["detail"] = "", "non contrôlé : page LinkedIn de la société introuvable"
                    await _reecrire(c, s["page"], data)
                    MANQUANTS["ok"] += 1
                    MANQUANTS["contacts"] += len(data["contacts_crm"])
                except Exception as e:
                    MANQUANTS["echecs"].append(f"{s['nom']} : {e}")
                MANQUANTS["fait"] += 1
        MANQUANTS["phase"] = "terminé"
    except Exception as e:
        MANQUANTS["phase"] = f"arrêt sur erreur : {e}"
    finally:
        MANQUANTS["actif"] = False


@router.get("/v3/validation/api/contacts-crm-manquants")
async def contacts_crm_manquants(req: Request, confirmer: int = 0):
    """Admin. Sociétés avec une fiche Pipedrive dont les contacts n'ont jamais été lus (pas de page LinkedIn trouvée).
    Sans &confirmer=1 : essai à blanc (analyse seulement). Avec : lit leurs contacts dans Pipedrive (doublons compris)
    et les enregistre. Pipedrive seulement : aucun phantom, aucun crédit LinkedIn. En tâche de fond : rappeler l'adresse."""
    if (x := VALID._refus(req, "admin")):
        return x
    etat = {k: v for k, v in MANQUANTS.items() if not k.startswith("_")}
    if MANQUANTS["actif"]:
        return etat | {"conseil": "en cours : rouvrez cette adresse dans une minute"}
    if not confirmer and MANQUANTS["phase"] and time.time() - MANQUANTS["_quand"] < 900:
        return etat | {"conseil": "résultat du dernier passage (moins de 15 min) ; ajoutez &confirmer=1 pour écrire"}
    asyncio.create_task(_tache_manquants(bool(confirmer)))
    return {"lance": "écriture" if confirmer else "essai à blanc", "conseil": "rouvrez cette adresse (sans &confirmer) dans 2 minutes",
            "dernier_passage": etat if etat["phase"] else None}


# ───────── e-mails Fullenrich manqués (envoi refusé « Too many data » avant le 27/10) ─────────
# Mêmes contacts que le run aurait envoyés : rôles 1 à 5, 4 premiers par société, absents de Pipedrive, sans e-mail,
# société avec un domaine. Un contact déjà tenté par ce rattrapage n'est jamais renvoyé (crédits).
FULLENRICH = {"actif": False, "phase": "", "lancement": "", "mode": "", "societes": 0, "contacts": 0, "trouves": 0,
              "ecrites": 0, "echecs": [], "exemples": [], "_cibles": [], "_quand": 0.0, "_lignes": []}


def contacts_fullenrich(data: dict) -> list:
    """Dirigeants de la fiche à envoyer à Fullenrich (même sélection que l'étape 5 du run)."""
    if not data.get("domaine"):
        return []
    ds = sorted(data.get("dirigeants", []), key=lambda d: str(d.get("role", "")))
    utiles = [d for d in ds if str(d.get("role", ""))[:1] in "12345"][:4]
    return [d for d in utiles if not d.get("dans_pipedrive") and not d.get("email") and not d.get("fullenrich_tente")
            and d.get("prenom") and d.get("nom")]


def _du_lancement(s: dict, lancements: list) -> bool:
    l = (s.get("lancement") or "").lower()
    return any(x in l for x in lancements)


async def _tache_fullenrich(lancements: list, confirmer: bool):
    FULLENRICH.update(actif=True, phase="lecture des fiches", mode="écriture" if confirmer else "essai à blanc",
                      societes=0, contacts=0, trouves=0, ecrites=0, echecs=[], exemples=[], _lignes=[])
    try:
        socs = [s for s in await VALID._toutes() if s["validation"] != "Écartée" and _du_lancement(s, lancements)]
        sem, cibles = asyncio.Semaphore(3), []
        async def voir(s):
            async with sem:
                d = await _lire_page(s)
            if d and (cs := contacts_fullenrich(d)):
                cibles.append((s, d, cs))
        await asyncio.gather(*(voir(s) for s in socs))
        n = sum(len(cs) for _, _, cs in cibles)
        FULLENRICH.update(societes=len(cibles), contacts=n, _cibles=cibles, _quand=time.time(),
                          exemples=[f"{s['nom']} : {', '.join(d.get('nom_complet') or d['prenom'] + ' ' + d['nom'] for d in cs)}"
                                    for s, _, cs in cibles[:15]])
        FULLENRICH["_lignes"] = [[d0.get("id", ""), s["nom"], d0.get("domaine", ""), d.get("prenom", ""), d.get("nom", ""),
                                  d.get("fonction", ""), d.get("role", ""), d.get("linkedin", ""), ""] for s, d0, cs in cibles for d in cs]
        if not confirmer:
            FULLENRICH["phase"] = f"essai à blanc terminé : rien n'a été envoyé ({len(socs)} sociétés de ces lancements lues)"
            return
        if not LI.FULLENRICH_KEY:
            FULLENRICH["phase"] = "FULLENRICH_API_KEY absente dans Render"
            return
        FULLENRICH["phase"] = f"Fullenrich : {n} contacts par lots de {LI.FULLENRICH_LOT}"
        paires = [(d, d0, s) for s, d0, cs in cibles for d in cs]
        journal = lambda m: FULLENRICH["echecs"].append(m) if ("refusé" in m or "échec" in m or "pas de réponse" in m) else None
        async with httpx.AsyncClient(timeout=60) as c:
            trouves = await LI.fullenrich_emails(c, [(d, d0) for d, d0, _ in paires], journal)
            FULLENRICH["trouves"] = len(trouves)
            quand = datetime.now(timezone.utc).date().isoformat()
            for i, (d, _, _) in enumerate(paires):
                d["fullenrich_tente"] = quand
                if trouves.get(str(i)):
                    d["email"], d["source_email"] = trouves[str(i)], "Fullenrich"
                    FULLENRICH["_lignes"][i][8] = d["email"]
            FULLENRICH["phase"] = "écriture dans Notion"
            for s, d0, _ in cibles:
                try:
                    await _reecrire(c, s["page"], d0)
                    await MEM._req(c, "PATCH", f"https://api.notion.com/v1/pages/{s['page']}", json={"properties": {
                        "Nb e-mails": {"number": sum(1 for d in d0.get("dirigeants", []) if d.get("email"))}}})
                    FULLENRICH["ecrites"] += 1
                except Exception as e:
                    FULLENRICH["echecs"].append(f"{s['nom']} : {e}")
        FULLENRICH["phase"] = "terminé"
    except Exception as e:
        FULLENRICH["phase"] = f"arrêt sur erreur : {e}"
    finally:
        FULLENRICH["actif"] = False


@router.get("/v3/validation/api/fullenrich-rattrapage")
async def fullenrich_rattrapage(req: Request, lancement: str = "", confirmer: int = 0):
    """Admin. E-mails Fullenrich des dirigeants d'un ou plusieurs lancements (séparés par des virgules, morceau du nom
    suffit : « 4097 (suite 1/9),locataires_flex_paris »). Sans &confirmer=1 : essai à blanc (nombre de contacts = crédits
    au plus, rien n'est envoyé). Avec : envoi par lots de 100, e-mails écrits dans la mémoire. L'écriture n'est possible
    qu'après un essai à blanc des mêmes lancements de moins d'une heure. Détail : /v3/validation/api/fullenrich-rattrapage/excel"""
    if (x := VALID._refus(req, "admin")):
        return x
    lancements = [x.strip().lower() for x in lancement.split(",") if x.strip()]
    etat = {k: v for k, v in FULLENRICH.items() if not k.startswith("_")}
    if FULLENRICH["actif"]:
        return etat | {"conseil": "en cours : rouvrez cette adresse (sans &confirmer) dans une minute"}
    if not lancements:
        return etat | {"conseil": "ajoutez ?lancement=… (morceau du nom, plusieurs séparés par des virgules)"}
    meme = FULLENRICH["lancement"] == ",".join(lancements)
    if not confirmer and meme and FULLENRICH["phase"] and time.time() - FULLENRICH["_quand"] < 900:
        return etat | {"conseil": "résultat du dernier passage ; &confirmer=1 pour envoyer à Fullenrich (crédits)"}
    if confirmer and not (meme and FULLENRICH["mode"] == "essai à blanc" and FULLENRICH["phase"].startswith("essai à blanc terminé")
                          and time.time() - FULLENRICH["_quand"] < 3600):
        return JSONResponse({"erreur": "faites d'abord l'essai à blanc de ces mêmes lancements (sans &confirmer), il y a moins d'une heure"}, 409)
    FULLENRICH["lancement"] = ",".join(lancements)
    asyncio.create_task(_tache_fullenrich(lancements, bool(confirmer)))
    return {"lance": "envoi Fullenrich" if confirmer else "essai à blanc", "conseil": "rouvrez cette adresse (sans &confirmer) dans 1 à 2 minutes"}


@router.get("/v3/validation/api/fullenrich-rattrapage/excel")
async def fullenrich_rattrapage_excel(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    from v3 import feuille
    wb = Workbook(); wb.remove(wb.active)
    feuille(wb, "Contacts Fullenrich", ["ID Pipedrive", "Société", "Domaine", "Prénom", "Nom", "Fonction", "Rôle", "LinkedIn",
                                        "E-mail trouvé"], FULLENRICH["_lignes"])
    b = io.BytesIO(); wb.save(b)
    nom = f"rattrapage_fullenrich_{'essai' if FULLENRICH['mode'] == 'essai à blanc' else 'reel'}_{datetime.now(timezone.utc):%Y%m%d_%H%M}.xlsx"
    return StreamingResponse(io.BytesIO(b.getvalue()), headers={"Content-Disposition": f'attachment; filename="{nom}"'},
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
