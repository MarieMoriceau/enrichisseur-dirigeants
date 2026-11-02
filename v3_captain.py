"""
Enrichisseur v3 — vérification des e-mails par Captain Verify, avec un référentiel pour ne jamais payer deux fois

Référentiel : base Notion « Référentiel Captain Verify » (une ligne par adresse déjà vérifiée), créée au premier usage
à côté de la base mémoire. Le référentiel Excel de Marie (onglet « Tous_emails », ~10 000 adresses déjà payées) y est
importé une fois. Avant tout appel à Captain Verify, l'adresse est cherchée dans le référentiel ; chaque nouvelle
vérification y est ajoutée. Le référentiel se télécharge au même format que l'Excel de Marie.

Règle de Marie (02/11) : on ne garde que les adresses « ok ». Une adresse invalid / risky / unknown est retirée du
contact (gardée dans « email_ecarte » avec son statut) ; dans un lancement, Fullenrich est alors tenté en secours.

Page d'administration : /v3/validation/captain (import du référentiel, vérification des e-mails déjà trouvés,
téléchargement du référentiel). Essai à blanc avant chaque traitement de masse.
"""
import asyncio, hashlib, io, os, time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill

import v3_memoire as MEM
import v3_validation as VALID

router = APIRouter()
CV_KEY = os.getenv("CAPTAINVERIFY_API_KEY", "")
CV_URL = "https://api.captainverify.com/v2/verify"
CV_INTERVALLE = float(os.getenv("CAPTAINVERIFY_INTERVALLE", "1.3"))     # API : 50 vérifications par minute au plus
TITRE_DB = "Référentiel Captain Verify"
GARDER = {"ok"}                                                          # règle de Marie : seulement « ok »

REF = {"db": os.getenv("CAPTAINVERIFY_DB", "40e5dcdf1e3a4430a42eb7c5eedf78b1"),   # créée le 02/11 sous « Services Render »
       "emails": {}, "charge": False, "verrou": None, "erreur": ""}
_API = {"dernier": 0.0, "verrou": None, "credits": None, "a_sec": False}


def actif() -> bool:
    return bool(CV_KEY)


def _norm(e: str) -> str:
    return (e or "").strip().lower()


def _statut(r: str) -> str:
    """Statut au format du référentiel de Marie : valid → ok."""
    r = (r or "").strip().lower()
    return "ok" if r in ("valid", "ok") else r


# ───────────────────────── référentiel (base Notion) ─────────────────────────
PROPS = {"Email": {"title": {}}, "Statut": {"select": {}}, "Domaine": {"rich_text": {}}, "Détails": {"rich_text": {}},
         "Free": {"checkbox": {}}, "Rôle": {"checkbox": {}}, "Ok for all": {"checkbox": {}}, "Protected": {"checkbox": {}},
         "Listes": {"rich_text": {}}, "Vérifié le": {"date": {}}, "Source": {"select": {}}}


async def _base(c) -> str:
    """Identifiant de la base référentiel : variable Render, sinon recherche par titre, sinon création à côté de la mémoire."""
    if REF["db"]:
        return REF["db"]
    r = await MEM._req(c, "POST", "https://api.notion.com/v1/search",
                       json={"query": TITRE_DB, "filter": {"property": "object", "value": "database"}})
    for x in (r.json().get("results", []) if r.status_code == 200 else []):
        if "".join(t.get("plain_text", "") for t in x.get("title", [])).strip() == TITRE_DB and not x.get("archived"):
            REF["db"] = x["id"]
            return REF["db"]
    r = await MEM._req(c, "GET", f"https://api.notion.com/v1/databases/{MEM.MEMOIRE_DB}")
    parent = (r.json().get("parent") or {}) if r.status_code == 200 else {}
    if parent.get("type") != "page_id":
        raise RuntimeError("impossible de créer la base « Référentiel Captain Verify » : la base mémoire n'est pas dans une page "
                           "Notion. Créez-la à la main et mettez son identifiant dans Render (CAPTAINVERIFY_DB).")
    r = await MEM._req(c, "POST", "https://api.notion.com/v1/databases", json={
        "parent": {"type": "page_id", "page_id": parent["page_id"]},
        "title": [{"type": "text", "text": {"content": TITRE_DB}}], "properties": PROPS})
    if r.status_code != 200:
        raise RuntimeError(f"Notion refuse de créer la base référentiel ({r.status_code}) : {r.text[:200]}")
    REF["db"] = r.json()["id"]
    return REF["db"]


def _depuis_notion(pg: dict) -> dict:
    p = pg.get("properties") or {}
    coche = lambda n: bool((p.get(n) or {}).get("checkbox"))
    return {"email": _norm(MEM._val(p, "Email")), "status": MEM._val(p, "Statut"), "domaine": MEM._val(p, "Domaine"),
            "details": MEM._val(p, "Détails"), "free": coche("Free"), "role": coche("Rôle"), "ok_for_all": coche("Ok for all"),
            "protected": coche("Protected"), "listes": MEM._val(p, "Listes"), "le": MEM._val(p, "Vérifié le"),
            "source": MEM._val(p, "Source")}


async def charger(force: bool = False) -> dict:
    """Référentiel complet en mémoire (une seule lecture par démarrage du serveur : ~100 requêtes pour 10 000 adresses)."""
    REF["verrou"] = REF["verrou"] or asyncio.Lock()
    async with REF["verrou"]:
        if REF["charge"] and not force:
            return REF["emails"]
        out, curseur = {}, None
        async with httpx.AsyncClient(timeout=30) as c:
            db = await _base(c)
            while True:
                corps = {"page_size": 100, **({"start_cursor": curseur} if curseur else {})}
                r = await MEM._req(c, "POST", f"https://api.notion.com/v1/databases/{db}/query", json=corps)
                if r.status_code != 200:
                    raise RuntimeError(f"référentiel Captain Verify illisible ({r.status_code})")
                j = r.json()
                for pg in j.get("results", []):
                    x = _depuis_notion(pg)
                    if x["email"]:
                        out[x["email"]] = x
                if not j.get("has_more"):
                    break
                curseur = j.get("next_cursor")
        REF.update(emails=out, charge=True, erreur="")
        return out


async def _ajouter(c, x: dict):
    """Une adresse de plus dans le référentiel (Notion + mémoire du serveur)."""
    db = await _base(c)
    props = {"Email": {"title": [{"text": {"content": x["email"]}}]}, "Statut": {"select": {"name": x["status"] or "unknown"}},
             "Domaine": MEM._txt(x.get("domaine", "")), "Détails": MEM._txt(x.get("details", "")),
             "Free": {"checkbox": bool(x.get("free"))}, "Rôle": {"checkbox": bool(x.get("role"))},
             "Ok for all": {"checkbox": bool(x.get("ok_for_all"))}, "Protected": {"checkbox": bool(x.get("protected"))},
             "Listes": MEM._txt(x.get("listes", "")), "Source": {"select": {"name": x.get("source") or "enrichisseur"}},
             **({"Vérifié le": {"date": {"start": x["le"]}}} if x.get("le") else {})}
    r = await MEM._req(c, "POST", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db}, "properties": props})
    if r.status_code != 200:
        raise RuntimeError(f"écriture refusée ({r.status_code})")
    REF["emails"][x["email"]] = x


# ───────────────────────── appel à Captain Verify ─────────────────────────
class CreditsEpuises(RuntimeError):
    pass


async def _appeler(c, email: str) -> dict:
    """Une vérification payante. Crédits épuisés : Captain Verify répond « unknown » ; on s'arrête sans rien enregistrer."""
    _API["verrou"] = _API["verrou"] or asyncio.Lock()
    async with _API["verrou"]:
        attente = _API["dernier"] + CV_INTERVALLE - time.monotonic()
        if attente > 0:
            await asyncio.sleep(attente)
        _API["dernier"] = time.monotonic()
        r = await c.get(CV_URL, params={"apikey": CV_KEY, "email": email}, timeout=25)
    j = r.json() if r.status_code == 200 else {}
    if r.status_code != 200 or not j.get("success", True):
        raise RuntimeError(f"Captain Verify : réponse {r.status_code} {str(j.get('message') or '')[:120]}")
    _API["credits"] = j.get("credits", _API["credits"])
    if _statut(j.get("result")) == "unknown" and j.get("credits") is not None and int(j.get("credits") or 0) <= 0:
        _API["a_sec"] = True
        raise CreditsEpuises("crédits Captain Verify épuisés : rechargez le compte")
    return {"email": email, "status": _statut(j.get("result")), "domaine": email.split("@")[-1], "details": j.get("details") or "",
            "free": bool(j.get("free")), "role": bool(j.get("role")), "ok_for_all": bool(j.get("ok4all")),
            "protected": bool(j.get("protected")), "listes": "", "source": "enrichisseur",
            "le": datetime.now(timezone.utc).date().isoformat()}


async def statut(c, email: str) -> tuple:
    """(statut, payé ?) : référentiel d'abord, Captain Verify seulement si l'adresse n'y est pas."""
    e = _norm(email)
    if not e:
        return "", False
    ref = await charger()
    if e in ref:
        return ref[e]["status"], False
    x = await _appeler(c, e)
    if x["status"] != "unknown":                    # « unknown » est recrédité par Captain Verify : on pourra réessayer
        await _ajouter(c, x)
    return x["status"], True


async def filtrer(c, contacts: list, journal) -> dict:
    """contacts = dirigeants avec un e-mail. Garde les « ok », retire les autres (gardés dans email_ecarte).
    Sans clé Captain Verify : rien n'est touché. Renvoie le décompte."""
    n = Counter()
    if not actif():
        return n
    for d in contacts:
        if not d.get("email") or d.get("cv_statut"):
            continue
        try:
            s, paye = await statut(c, d["email"])
        except CreditsEpuises as e:
            journal(str(e))
            n["arret"] += 1
            break
        except Exception as e:
            journal(f"Captain Verify : {d['email']} non vérifiée ({e})")
            n["erreur"] += 1
            continue
        d["cv_statut"], d["cv_le"] = s, datetime.now(timezone.utc).date().isoformat()
        n["payees" if paye else "referentiel"] += 1
        n[s] += 1
        if s not in GARDER:
            d["email_ecarte"], d["source_email_ecarte"] = d["email"], d.get("source_email", "")
            d["email"], d["source_email"] = "", ""
    if n:
        journal(f"Captain Verify : {n['ok']} ok gardées, {sum(n[k] for k in ('invalid', 'risky', 'unknown'))} écartées "
                f"({n['payees']} vérifications payées, {n['referentiel']} déjà dans le référentiel)")
    return n


def passe(data: dict) -> bool:
    """Case « Captain Verify » de la société : au moins un e-mail trouvé, et tous passés par Captain Verify."""
    ds = [d for d in data.get("dirigeants", []) if d.get("email") or d.get("email_ecarte")]
    return bool(ds) and all(d.get("cv_statut") for d in ds)


def prop_passe(data: dict) -> dict:
    return {"Captain Verify": {"checkbox": passe(data)}}


# ───────────────────────── import du référentiel Excel de Marie ─────────────────────────
IMPORT = {"actif": False, "phase": "", "cle": "", "lignes": [], "nouvelles": 0, "deja": 0, "fait": 0, "erreurs": [], "_quand": 0.0}


def lire_referentiel(data: bytes) -> list:
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb["Tous_emails"] if "Tous_emails" in wb.sheetnames else wb.worksheets[0]
    it = ws.iter_rows(values_only=True)
    h = [str(x or "").strip().lower() for x in next(it)]
    if "email" not in h:
        raise ValueError("colonne « email » introuvable (onglet Tous_emails attendu)")
    out, vus = [], set()
    for r in it:
        x = dict(zip(h, r))
        e = _norm(x.get("email"))
        if "@" not in e or e in vus:
            continue
        vus.add(e)
        out.append({"email": e, "status": _statut(str(x.get("status") or "")), "domaine": str(x.get("domaine") or e.split("@")[-1]),
                    "details": str(x.get("details") or ""), "free": bool(x.get("free")), "role": bool(x.get("role")),
                    "ok_for_all": bool(x.get("ok_for_all")), "protected": bool(x.get("protected")),
                    "listes": str(x.get("listes") or ""), "source": "import référentiel", "le": ""})
    return out


async def _importer():
    IMPORT.update(actif=True, fait=0, erreurs=[], phase="écriture dans Notion")
    try:
        ref = await charger()
        async with httpx.AsyncClient(timeout=30) as c:
            for x in IMPORT["lignes"]:
                if x["email"] in ref:
                    continue
                try:
                    await _ajouter(c, x)
                except Exception as e:
                    IMPORT["erreurs"] = (IMPORT["erreurs"] + [f"{x['email']} : {e}"])[-20:]
                IMPORT["fait"] += 1
        IMPORT["phase"] = f"terminé : {IMPORT['fait']} adresses ajoutées au référentiel"
    except Exception as e:
        IMPORT["phase"] = f"arrêt sur erreur : {e}"
    finally:
        IMPORT.update(actif=False, lignes=[], cle="")


# ───────────────────────── vérification des e-mails déjà trouvés (contacts validés) ─────────────────────────
DEJA = {"actif": False, "phase": "", "mode": "", "contacts": 0, "dans_ref": 0, "a_payer": 0, "compte": {}, "ecrites": 0,
        "erreurs": [], "_quand": 0.0, "_fin": 0.0, "_lignes": []}


def a_verifier(s: dict, data: dict) -> list:
    """Dirigeants validés (« prêt pour HubSpot ») dont l'e-mail trouvé par l'outil n'est pas encore passé par Captain Verify.
    Une adresse saisie à la main par un négo n'est pas touchée."""
    nouveaux = VALID._json(s.get("nouveaux"), {}) if isinstance(s.get("nouveaux"), str) else (s.get("nouveaux") or {})
    out = []
    for d in data.get("dirigeants", []):
        e = nouveaux.get(VALID.cle_contact(d)) or {}
        if e.get("etat") == "prêt pour HubSpot" and d.get("email") and not d.get("cv_statut") and "email" not in e.get("modifie", []):
            out.append(d)
    return out


async def _deja(confirmer: bool):
    from v3_rattrapage import _lire_page, _reecrire
    DEJA.update(actif=True, phase="lecture du référentiel et des fiches", mode="écriture" if confirmer else "essai à blanc",
                contacts=0, dans_ref=0, a_payer=0, compte={}, ecrites=0, erreurs=[], _lignes=[])
    try:
        ref = await charger()
        socs = [s for s in await VALID._toutes() if s["validation"] != "Écartée" and s.get("nouveaux")]
        sem, cibles = asyncio.Semaphore(3), []
        async def voir(s):
            async with sem:
                d0 = await _lire_page(s)
            if d0 and (ds := a_verifier(s, d0)):
                cibles.append((s, d0, ds))
        await asyncio.gather(*(voir(s) for s in socs))
        cibles.sort(key=lambda x: x[0]["nom"].lower())
        tous = [d for _, _, ds in cibles for d in ds]
        DEJA.update(contacts=len(tous), dans_ref=sum(1 for d in tous if _norm(d["email"]) in ref), _quand=time.time())
        DEJA["a_payer"] = len({_norm(d["email"]) for d in tous if _norm(d["email"]) not in ref})
        DEJA["compte"] = dict(Counter(ref[_norm(d["email"])]["status"] for d in tous if _norm(d["email"]) in ref))
        if not confirmer:
            DEJA["phase"] = f"essai à blanc terminé : rien n'a été envoyé ni modifié ({len(socs)} sociétés lues)"
            return
        if not actif():
            DEJA["phase"] = "CAPTAINVERIFY_API_KEY absente dans Render"
            return
        journal = lambda m: DEJA["erreurs"].append(m) if ("non vérifiée" in m or "épuisés" in m) else None
        async with httpx.AsyncClient(timeout=30) as c:
            for i, (s, d0, ds) in enumerate(cibles, 1):
                DEJA["phase"] = f"vérification {i}/{len(cibles)} sociétés"
                avant = {id(d): d["email"] for d in ds}
                n = await filtrer(c, ds, journal)
                for d in ds:
                    DEJA["_lignes"].append([s["nom"], d.get("prenom", ""), d.get("nom", ""), avant[id(d)], d.get("cv_statut", ""),
                                            "gardée" if d.get("email") else "écartée"])
                DEJA["compte"] = dict(Counter(DEJA["compte"]) + Counter({k: v for k, v in n.items() if k in ("ok", "invalid", "risky", "unknown")}))
                if any(d.get("cv_statut") for d in ds):
                    try:
                        await _reecrire(c, s["page"], d0)
                        await MEM._req(c, "PATCH", f"https://api.notion.com/v1/pages/{s['page']}", json={"properties": {
                            "Nb e-mails": {"number": sum(1 for d in d0.get("dirigeants", []) if d.get("email"))},
                            **prop_passe(d0)}})
                        DEJA["ecrites"] += 1
                    except Exception as e:
                        DEJA["erreurs"].append(f"{s['nom']} : {e}")
                if n.get("arret"):
                    DEJA["phase"] = "arrêté : crédits Captain Verify épuisés (ce qui est fait est enregistré)"
                    return
        DEJA["phase"] = "terminé"
    except Exception as e:
        DEJA["phase"] = f"arrêt sur erreur : {e}"
    finally:
        DEJA["actif"], DEJA["_fin"] = False, time.time()


# ───────────────────────── routes ─────────────────────────
@router.get("/v3/validation/api/captain")
async def captain_etat(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    return {"cle": actif(), "credits": _API["credits"], "referentiel": len(REF["emails"]) if REF["charge"] else None,
            "base": REF["db"], "erreur": REF["erreur"],
            "import": {k: v for k, v in IMPORT.items() if k not in ("lignes", "cle") and not k.startswith("_")},
            "deja": {k: v for k, v in DEJA.items() if not k.startswith("_")},
            "coche": {k: v for k, v in COCHE.items() if not k.startswith("_")}}


@router.post("/v3/validation/api/captain/charger")
async def captain_charger(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    try:
        ref = await charger(force=True)
    except Exception as e:
        REF["erreur"] = str(e)
        return JSONResponse({"erreur": str(e)}, 502)
    return {"referentiel": len(ref), "base": REF["db"], "statuts": dict(Counter(x["status"] for x in ref.values()))}


@router.post("/v3/validation/api/captain/import")
async def captain_import(req: Request, fichier: UploadFile = File(...), confirmer: int = 0):
    """Import du référentiel Excel. Sans confirmer : essai à blanc (combien d'adresses nouvelles). Avec : écriture,
    seulement pour le même fichier qu'un essai de moins d'une heure (renvoyé avec confirmer=1)."""
    if (x := VALID._refus(req, "admin")):
        return x
    if IMPORT["actif"]:
        return JSONResponse({"erreur": "un import tourne déjà"}, 409)
    data = await fichier.read()
    cle = hashlib.sha1(data).hexdigest()
    try:
        lignes = lire_referentiel(data)
        ref = await charger()
    except Exception as e:
        return JSONResponse({"erreur": str(e)}, 400)
    nouvelles = [x for x in lignes if x["email"] not in ref]
    if not confirmer:
        IMPORT.update(cle=cle, _quand=time.time(), nouvelles=len(nouvelles), deja=len(lignes) - len(nouvelles),
                      phase="essai à blanc terminé : rien n'a été écrit")
        return {"essai": True, "adresses": len(lignes), "nouvelles": len(nouvelles), "deja": len(lignes) - len(nouvelles),
                "statuts": dict(Counter(x["status"] for x in nouvelles)),
                "duree_min": round(len(nouvelles) * 0.45 / 60)}
    if IMPORT["cle"] != cle or time.time() - IMPORT["_quand"] > 3600:
        return JSONResponse({"erreur": "faites d'abord l'essai à blanc avec ce même fichier (il y a moins d'une heure)"}, 409)
    IMPORT["lignes"] = nouvelles
    asyncio.create_task(_importer())
    return {"lance": True, "nouvelles": len(nouvelles)}


@router.post("/v3/validation/api/captain/deja")
async def captain_deja(req: Request):
    """Vérification des e-mails déjà trouvés. Corps {"confirmer": true} après un essai à blanc de moins d'une heure."""
    if (x := VALID._refus(req, "admin")):
        return x
    b = await req.json()
    if DEJA["actif"]:
        return JSONResponse({"erreur": "déjà en cours"}, 409)
    confirmer = bool(b.get("confirmer"))
    if confirmer and not (DEJA["mode"] == "essai à blanc" and DEJA["phase"].startswith("essai à blanc terminé")
                          and time.time() - DEJA["_quand"] < 3600):
        return JSONResponse({"erreur": "faites d'abord l'essai à blanc (il y a moins d'une heure)"}, 409)
    if confirmer and IMPORT["actif"]:
        return JSONResponse({"erreur": "attendez la fin de l'import du référentiel : sinon on paierait des adresses déjà connues"}, 409)
    asyncio.create_task(_deja(confirmer))
    return {"lance": "vérification" if confirmer else "essai à blanc"}


COCHE = {"actif": False, "phase": "", "a_cocher": 0, "fait": 0, "_quand": 0.0, "_pages": []}


async def _cocher(confirmer: bool):
    from v3_rattrapage import _lire_page
    COCHE.update(actif=True, phase="lecture des fiches", fait=0)
    try:
        if not confirmer:
            socs = [s for s in await VALID._toutes() if s["validation"] != "Écartée"]
            sem, pages = asyncio.Semaphore(3), []
            async def voir(s):
                async with sem:
                    d0 = await _lire_page(s)
                if d0 and passe(d0):
                    pages.append(s["page"])
            await asyncio.gather(*(voir(s) for s in socs))
            COCHE.update(_pages=pages, a_cocher=len(pages), _quand=time.time(),
                         phase=f"essai à blanc terminé : {len(pages)} sociétés à cocher sur {len(socs)} lues, rien n'est écrit")
            return
        async with httpx.AsyncClient(timeout=30) as c:
            for p in COCHE["_pages"]:
                await MEM._req(c, "PATCH", f"https://api.notion.com/v1/pages/{p}", json={"properties": {"Captain Verify": {"checkbox": True}}})
                COCHE["fait"] += 1
                COCHE["phase"] = f"écriture {COCHE['fait']}/{len(COCHE['_pages'])}"
        COCHE["phase"] = f"terminé : {COCHE['fait']} sociétés cochées"
    except Exception as e:
        COCHE["phase"] = f"arrêt sur erreur : {e}"
    finally:
        COCHE["actif"] = False


@router.post("/v3/validation/api/captain/cocher")
async def captain_cocher(req: Request):
    """Coche « Captain Verify » dans Notion pour les sociétés déjà vérifiées. Corps {"confirmer": true} après l'essai."""
    if (x := VALID._refus(req, "admin")):
        return x
    b = await req.json()
    if COCHE["actif"]:
        return JSONResponse({"erreur": "déjà en cours"}, 409)
    confirmer = bool(b.get("confirmer"))
    if confirmer and not (COCHE["phase"].startswith("essai à blanc terminé") and time.time() - COCHE["_quand"] < 3600):
        return JSONResponse({"erreur": "faites d'abord l'essai à blanc (il y a moins d'une heure)"}, 409)
    asyncio.create_task(_cocher(confirmer))
    return {"lance": True}


@router.get("/v3/validation/api/captain/deja/excel")
async def captain_deja_excel(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    from v3 import feuille
    wb = Workbook(); wb.remove(wb.active)
    feuille(wb, "Vérifications", ["Société", "Prénom", "Nom", "E-mail", "Captain Verify", "Résultat"], DEJA["_lignes"])
    b = io.BytesIO(); wb.save(b)
    return StreamingResponse(io.BytesIO(b.getvalue()), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="captainverify_verifications_{datetime.now():%Y-%m-%d}.xlsx"'})


def excel_referentiel(ref: dict) -> bytes:
    """Le référentiel au format de l'Excel de Marie : Lisez-moi, Synthèse, Tous_emails, Par_domaine."""
    lignes = sorted(ref.values(), key=lambda x: (x["status"] != "invalid", x["email"]))
    wb = Workbook()
    ws = wb.active; ws.title = "Lisez-moi"
    for t in [f"RÉFÉRENTIEL MAÎTRE CAPTAINVERIFY — à jour au {datetime.now():%d/%m/%Y}", "",
              f"Total : {len(ref)} emails uniques, tous déjà payés.", "",
              "À CROISER avant tout nouvel upload : un email présent ici ne doit pas être revérifié.",
              "L'enrichisseur le consulte avant chaque appel à Captain Verify et y ajoute chaque nouvelle vérification.",
              "Onglet Par_domaine : statut dominant de chaque domaine déjà sondé."]:
        ws.append([t])
    ws["A1"].font = Font(bold=True, size=13, color="4A1E5C"); ws.column_dimensions["A"].width = 110
    viol = PatternFill("solid", fgColor="4A1E5C")
    def onglet(titre, entetes, rows):
        s = wb.create_sheet(titre); s.append(entetes)
        for r in rows:
            s.append(r)
        for c in s[1]:
            c.font, c.fill = Font(bold=True, color="FFFFFF"), viol
        s.freeze_panes = "A2"; s.auto_filter.ref = s.dimensions
    n = Counter(x["status"] for x in lignes)
    onglet("Synthèse", ["statut", "emails uniques", "%"],
           [[k, v, round(100 * v / max(len(lignes), 1), 1)] for k, v in n.most_common()])
    onglet("Tous_emails", ["email", "status", "domaine", "free", "disposable", "role", "ok_for_all", "protected", "did_you_mean",
                           "details", "listes", "vérifié le", "source"],
           [[x["email"], x["status"], x["domaine"], int(x["free"]), 0, int(x["role"]), int(x["ok_for_all"]), int(x["protected"]), "",
             x["details"], x["listes"], x["le"], x["source"]] for x in lignes])
    dom = defaultdict(list)
    for x in lignes:
        dom[x["domaine"]].append(x["status"])
    onglet("Par_domaine", ["domaine", "emails", "statuts", "homogene"],
           sorted([[d, len(v), ", ".join(sorted(set(v))), "oui" if len(set(v)) == 1 else "non"] for d, v in dom.items()],
                  key=lambda r: (-r[1], r[0])))
    for s, l in (("Tous_emails", (40, 10, 28)), ("Par_domaine", (32, 8, 24, 10))):
        for i, w in enumerate(l):
            wb[s].column_dimensions[chr(65 + i)].width = w
    b = io.BytesIO(); wb.save(b)
    return b.getvalue()


@router.get("/v3/validation/api/captain/referentiel.xlsx")
async def captain_referentiel_excel(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    try:
        ref = await charger()
    except Exception as e:
        return JSONResponse({"erreur": str(e)}, 502)
    return StreamingResponse(io.BytesIO(excel_referentiel(ref)),
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="captainverify_referentiel_maitre_{datetime.now():%Y-%m-%d}.xlsx"'})


@router.get("/v3/validation/captain")
async def captain_page(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    return HTMLResponse(PAGE)


PAGE = """<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Captain Verify</title>
<style>
:root{--ink:#2b2233;--muted:#776b80;--line:#e6e0ea;--violet:#4A1E5C;--bg:#faf8f5;--card:#fff;--ok:#1f7a4d;--err:#b0302a}
body{font:15px/1.5 system-ui,sans-serif;margin:0;background:var(--bg);color:var(--ink)}
main{max-width:860px;margin:0 auto;padding:24px 16px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:16px;margin:0 0 6px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin:14px 0}
.msg{color:var(--muted);font-size:14px;white-space:pre-line}.err{color:var(--err)}.ok{color:var(--ok)}
button,.btn{font:600 14px system-ui;background:var(--violet);color:#fff;border:0;border-radius:8px;padding:8px 14px;cursor:pointer;text-decoration:none;display:inline-block}
button.ghost,.btn.ghost{background:transparent;color:var(--violet);border:1px solid var(--violet)}
button:disabled{opacity:.5;cursor:default}[hidden]{display:none!important}.ligne{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-top:8px}
</style>
<main>
<p><a href="/v3/validation">← Validation</a></p>
<h1>Captain Verify</h1>
<p class="msg" id="etat">Chargement…</p>

<div class="card"><h2>1. Référentiel des adresses déjà vérifiées</h2>
<p class="msg">Avant chaque vérification, l'outil regarde ici : une adresse déjà connue n'est jamais repayée. Chaque nouvelle vérification s'y ajoute.</p>
<div class="ligne"><button class="ghost" id="charger">Lire le référentiel</button>
<a class="btn ghost" href="/v3/validation/api/captain/referentiel.xlsx">Télécharger le référentiel (Excel)</a></div>
<p class="msg" id="refMsg"></p></div>

<div class="card"><h2>2. Importer ton Excel de référentiel</h2>
<p class="msg">Onglet « Tous_emails ». Essai à blanc d'abord : il compte les adresses nouvelles, rien n'est écrit.</p>
<div class="ligne"><input type="file" id="fichier" accept=".xlsx"><button id="essaiImport">Essai à blanc</button>
<button id="okImport" hidden>Importer</button></div>
<p class="msg" id="impMsg"></p></div>

<div class="card"><h2>3. Vérifier les e-mails déjà trouvés</h2>
<p class="msg">Contacts validés dont l'e-mail n'est pas encore passé par Captain Verify. On ne garde que les « ok ».</p>
<div class="ligne"><button id="essaiDeja">Essai à blanc</button><button id="okDeja" hidden>Lancer la vérification</button>
<a class="btn ghost" id="xlDeja" href="/v3/validation/api/captain/deja/excel" hidden>Détail (Excel)</a></div>
<p class="msg" id="dejaMsg"></p></div>

<div class="card"><h2>4. Cocher « Captain Verify » dans Notion</h2>
<p class="msg">Pour les sociétés déjà vérifiées avant l'ajout de la case. Ensuite, la case se coche toute seule.</p>
<div class="ligne"><button id="essaiCoche">Essai à blanc</button><button id="okCoche" hidden>Cocher</button></div>
<p class="msg" id="cocheMsg"></p></div>
</main>
<script>
const $ = s => document.querySelector(s);
async function api(url, corps, brut) {
  const o = corps === undefined ? {} : brut ? {method: "POST", body: corps} : {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(corps)};
  const r = await fetch(url, o);
  if (r.status === 401 || r.status === 403) { location.href = "/v3/connexion?suite=/v3/validation/captain"; throw new Error("reconnexion"); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(j.erreur || ("erreur " + r.status));
  return j;
}
const fmt = o => Object.entries(o || {}).map(([k, v]) => `${k} ${v}`).join(" · ");
let timer = null;
async function etat() {
  clearTimeout(timer);
  let e; try { e = await api("/v3/validation/api/captain"); } catch (er) { $("#etat").textContent = er.message; return; }
  $("#etat").innerHTML = (e.cle ? "Clé Captain Verify : <span class='ok'>branchée</span>" : "Clé Captain Verify : <span class='err'>absente de Render (CAPTAINVERIFY_API_KEY)</span>")
    + (e.credits != null ? ` · ${e.credits} crédits restants` : "") + (e.referentiel != null ? ` · référentiel : ${e.referentiel} adresses` : "");
  const i = e.import, d = e.deja;
  if (i.actif || i.phase.startsWith("terminé") || i.phase.startsWith("arrêt")) $("#impMsg").textContent = i.actif ? `Import en cours : ${i.fait} adresses écrites…` : i.phase;
  if (d.phase) {
    $("#dejaMsg").textContent = `${d.phase}\\n${d.contacts} contacts à vérifier · ${d.dans_ref} déjà dans le référentiel (gratuit) · ${d.a_payer} à payer`
      + (Object.keys(d.compte || {}).length ? `\\nStatuts : ${fmt(d.compte)}` : "") + (d.ecrites ? ` · ${d.ecrites} sociétés mises à jour` : "")
      + ((d.erreurs || []).length ? `\\nIncidents : ${d.erreurs.slice(-3).join(" ; ")}` : "");
    $("#okDeja").hidden = d.actif || !(d.mode === "essai à blanc" && d.phase.startsWith("essai à blanc terminé") && d.contacts);
    $("#xlDeja").hidden = !(d.mode === "écriture");
  }
  $("#essaiDeja").disabled = d.actif;
  const k = e.coche || {};
  if (k.phase) $("#cocheMsg").textContent = k.phase;
  $("#okCoche").hidden = k.actif || !(k.phase || "").startsWith("essai à blanc terminé") || !k.a_cocher;
  $("#essaiCoche").disabled = k.actif;
  if (i.actif || d.actif || k.actif) timer = setTimeout(etat, 5000);
}
$("#charger").onclick = async () => {
  $("#refMsg").textContent = "Lecture (une minute pour 10 000 adresses)…";
  try { const r = await api("/v3/validation/api/captain/charger", {}); $("#refMsg").textContent = `${r.referentiel} adresses · ${fmt(r.statuts)}`; etat(); }
  catch (e) { $("#refMsg").textContent = e.message; }
};
async function envoyer(confirmer) {
  const f = $("#fichier").files[0]; if (!f) { $("#impMsg").textContent = "Choisissez le fichier."; return; }
  const fd = new FormData(); fd.append("fichier", f);
  $("#impMsg").textContent = confirmer ? "Lancement…" : "Lecture du fichier et du référentiel (une minute)…";
  try {
    const r = await api("/v3/validation/api/captain/import" + (confirmer ? "?confirmer=1" : ""), fd, true);
    if (r.essai) {
      $("#impMsg").textContent = `${r.adresses} adresses dans le fichier · ${r.deja} déjà dans le référentiel · ${r.nouvelles} à ajouter (${fmt(r.statuts)}).`
        + (r.nouvelles ? `\\nDurée estimée : ${r.duree_min} min. Évitez de le lancer pendant un run.` : "");
      $("#okImport").hidden = !r.nouvelles;
    } else { $("#okImport").hidden = true; etat(); }
  } catch (e) { $("#impMsg").textContent = e.message; }
}
$("#essaiImport").onclick = () => envoyer(false);
$("#okImport").onclick = () => envoyer(true);
$("#essaiDeja").onclick = async () => { $("#dejaMsg").textContent = "Essai à blanc lancé (2 à 3 minutes)…"; try { await api("/v3/validation/api/captain/deja", {}); setTimeout(etat, 3000); } catch (e) { $("#dejaMsg").textContent = e.message; } };
$("#okDeja").onclick = async () => {
  if (!confirm("Lancer la vérification ? Les adresses non « ok » seront retirées des contacts.")) return;
  try { await api("/v3/validation/api/captain/deja", {confirmer: true}); setTimeout(etat, 3000); } catch (e) { $("#dejaMsg").textContent = e.message; }
};
$("#essaiCoche").onclick = async () => { $("#cocheMsg").textContent = "Essai à blanc lancé (2 à 3 minutes)…"; try { await api("/v3/validation/api/captain/cocher", {}); setTimeout(etat, 3000); } catch (e) { $("#cocheMsg").textContent = e.message; } };
$("#okCoche").onclick = async () => { try { await api("/v3/validation/api/captain/cocher", {confirmer: true}); setTimeout(etat, 2000); } catch (e) { $("#cocheMsg").textContent = e.message; } };
etat();
</script></html>"""
