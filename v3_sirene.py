"""SIRENE : date d'installation à l'adresse des bureaux (≈ début de bail) et bureaux chez un opérateur de flex.

- Date : l'établissement de la société (siège ou secondaire) situé à l'adresse des bureaux retenue ; sa date de création
  à l'INSEE = date d'installation à cette adresse. Pas d'établissement à cette adresse (sous-location, flex non déclaré) :
  pas de date. Un siège ailleurs que les bureaux n'est jamais utilisé (ce serait la date d'une domiciliation).
- Flex : (a) l'adresse SIRENE de l'établissement cite un opérateur (« IMMEUBLE WEWORK 20 RUE LA FAYETTE ») ;
  (b) un opérateur de la liste a un établissement (centre) à la même adresse que les bureaux.
  Liste des opérateurs : page Notion « Opérateurs de flex et co work » d'Equation (OPERATEURS_FLEX), complétable par la
  variable Render FLEX_OPERATEURS (noms séparés par des virgules).

Premier usage : essai à blanc sur un échantillon de sociétés validées (/v3/validation/api/sirene-essai), rien n'est écrit.
"""
import asyncio, io, os, re, time
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from openpyxl import Workbook

import v3_linkedin as LI
import v3_memoire as MEM
import v3_validation as VALID

router = APIRouter()

# noms tels qu'on les cherche dans SIRENE (nom ou enseigne de l'opérateur)
OPERATEURS_FLEX = ["WeWork", "Morning", "Deskeo", "Patchwork", "Spaces", "Regus", "Come and Work", "Les Nouveaux Bureaux",
                   "Sora", "Klam", "Hellow", "Joro", "Bope", "Stell", "Snapdesk", "All Day", "Hiptown", "Startway", "Wojo",
                   "Reflex", "Deskapad", "Hybrid for work", "Kwerk", "Volum", "The Officers", "Imagin Office", "MyFlexOffice"]
OPERATEURS_FLEX += [x.strip() for x in os.getenv("FLEX_OPERATEURS", "").split(",") if x.strip()]


def _norm(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", LI._sans_accents(t or "")).strip()


def operateur_cite(adresse: str) -> str:
    """Opérateur nommé dans une adresse SIRENE (« IMMEUBLE WEWORK … », « C/O MORNING … »)."""
    a = f" {_norm(adresse)} "
    return next((o for o in OPERATEURS_FLEX if f" {_norm(o)} " in a), "")


def _numeros(a: str) -> set:
    """Numéros de rue d'une adresse, plages comprises : « 2-4 », « 17-21 », « 26 AU 28 », « 43- 45 » (SIRENE écrit
    souvent « 2-4 2 RUE JULES LEFEBVRE » quand les bureaux sont au « 4 rue Jules Lefebvre »)."""
    t = LI._sans_accents(a or "")
    t = t[:m.start()] if (m := re.search(r"\b\d{5}\b", t)) else t
    out = set()
    for d, f in re.findall(r"\b(\d{1,4})\s*(?:-|a|au|et)\s*(\d{1,4})\b", t):
        if 0 < int(f) - int(d) <= 12:
            out |= set(range(int(d), int(f) + 1))
    out |= {int(n) for n in re.findall(r"\b(\d{1,4})(?:bis|ter|b)?\b", t)}
    return out


def meme_lieu(a: str, b: str) -> bool:
    """Même adresse, en tolérant les plages de numéros (« 2-4 » contient « 4 »)."""
    if LI.meme_adresse(a, b):
        return True
    ka, kb = LI.cle_adresse(a), LI.cle_adresse(b)
    if not (ka and kb) or ka[2] != kb[2] or not (ka[1] and kb[1]):
        return False
    x, y = ka[1], kb[1]
    if not (x in y or y in x or x.split()[0] == y.split()[0]):
        return False
    return bool(_numeros(a) & _numeros(b))


def _cp(a: str) -> str:
    m = re.search(r"\b(75\d{3}|77\d{3}|78\d{3}|91\d{3}|92\d{3}|93\d{3}|94\d{3}|95\d{3}|\d{5})\b", a or "")
    return m.group(1) if m else ""


async def _chercher(api, params: dict) -> list:
    import v3
    r = await v3._insee_get(api, params | {"per_page": 5, "limite_matching_etablissements": 100})
    try:
        return r.json().get("results", []) if r is not None and r.status_code == 200 else []
    except ValueError:
        return []


async def etablissements(api, siren: str, nom: str, cp: str) -> list:
    """Établissements de la société dans ce code postal (+ son siège) : [{adresse, date_creation, est_siege, ouvert}]."""
    out = []
    for q in ([nom, siren] if nom else [siren]):
        for x in await _chercher(api, {"q": q, "code_postal": cp} if cp else {"q": q}):
            if x.get("siren") != siren:
                continue
            for e in [x.get("siege") or {}] + (x.get("matching_etablissements") or []):
                if e.get("adresse") and not any(o["siret"] == e.get("siret") for o in out):
                    out.append({"siret": e.get("siret", ""), "adresse": e["adresse"], "date_creation": e.get("date_creation") or "",
                                "est_siege": bool(e.get("est_siege")), "ouvert": e.get("etat_administratif") == "A"})
        if out:
            break
    return out


_CENTRES: dict = {}            # (opérateur, cp) -> [(adresse, nom)] ; mémoire du serveur, vidée au redémarrage


async def centres_flex(api, cp: str) -> list:
    """Centres des opérateurs de flex dans ce code postal : établissements SIRENE ouverts dont le nom ou l'enseigne
    contient le nom de l'opérateur."""
    out = []
    for o in OPERATEURS_FLEX:
        if (o, cp) not in _CENTRES:
            trouves = []
            for x in await _chercher(api, {"q": o, "code_postal": cp}):
                noms = _norm(" ".join([x.get("nom_complet") or ""] + [" ".join(e.get("liste_enseignes") or [])
                                                                       for e in x.get("matching_etablissements") or []]))
                if f" {_norm(o)} " not in f" {noms} ":
                    continue
                for e in x.get("matching_etablissements") or []:
                    if e.get("etat_administratif") == "A" and e.get("adresse"):
                        trouves.append((e["adresse"], x.get("nom_complet", "")))
            _CENTRES[(o, cp)] = trouves
        out += [(o, a, n) for a, n in _CENTRES[(o, cp)]]
    return out


async def analyser(api, siren: str, nom: str, bureaux: str) -> dict:
    """{date, etab, siege, operateur, indice} pour l'adresse des bureaux."""
    rep = {"date": "", "etab": "", "siege": "", "operateur": "", "indice": ""}
    cp = _cp(bureaux)
    if not (siren and LI.cle_adresse(bureaux)):
        rep["indice"] = "adresse des bureaux incomplète (numéro, rue, code postal)" if siren else "pas de SIREN"
        return rep
    etabs = await etablissements(api, siren, nom, cp)
    ici = sorted([e for e in etabs if meme_lieu(e["adresse"], bureaux)], key=lambda e: (not e["ouvert"], e["date_creation"]))
    if ici:
        e = ici[0]
        rep.update(date=e["date_creation"], etab=e["adresse"], siege="oui" if e["est_siege"] else "non (établissement secondaire)")
        if (o := operateur_cite(e["adresse"])):
            rep.update(operateur=o, indice=f"adresse SIRENE de la société : « {e['adresse']} »")
    else:
        rep["indice"] = "aucun établissement de la société à l'adresse des bureaux"
    if not rep["operateur"] and cp:
        for o, a, n in await centres_flex(api, cp):
            if meme_lieu(a, bureaux):
                rep.update(operateur=o, indice=f"centre {o} à la même adresse ({n} : {a})")
                break
    return rep


# ───────────────────────── essai à blanc ─────────────────────────
ESSAI = {"actif": False, "phase": "", "fait": 0, "total": 0, "lignes": [], "fini": ""}


async def _essai(limite: int):
    ESSAI.update(actif=True, phase="choix des sociétés", fait=0, total=0, lignes=[], fini="")
    try:
        socs = [s for s in await VALID._toutes() if s["validation"] in ("Société validée", "Contacts validés")
                and LI.cle_adresse(s.get("adresse_retenue") or "") and re.fullmatch(r"\d{9}", s.get("siren") or "")][:limite]
        ESSAI.update(total=len(socs), phase="lecture SIRENE")
        async with httpx.AsyncClient(timeout=20) as api:
            for s in socs:
                try:
                    r = await analyser(api, s["siren"], s["nom"], s["adresse_retenue"])
                except Exception as e:
                    r = {"date": "", "etab": "", "siege": "", "operateur": "", "indice": f"erreur : {e}"}
                ESSAI["lignes"].append([s["nom"], s["siren"], s["adresse_retenue"], r["etab"], r["date"], r["siege"],
                                        r["operateur"], r["indice"], s.get("debut_bail", ""), "oui" if s.get("flexoffice") else ""])
                ESSAI["fait"] += 1
        ESSAI["phase"] = "terminé"
    except Exception as e:
        ESSAI["phase"] = f"arrêt sur erreur : {e}"
    finally:
        ESSAI.update(actif=False, fini=datetime.now(timezone.utc).isoformat(timespec="seconds"))


@router.get("/v3/validation/api/sirene-essai")
async def sirene_essai(req: Request, lancer: int = 0, limite: int = 50):
    """Admin. ?lancer=1 : essai à blanc sur `limite` sociétés validées (rien n'est écrit). Sans : état + résumé.
    Un essai terminé depuis moins d'une heure n'est pas écrasé par lancer=1 (lancer=2 pour forcer).
    Le détail : /v3/validation/api/sirene-essai/excel"""
    if (x := VALID._refus(req, "admin")):
        return x
    recent = ESSAI["fini"] and (datetime.now(timezone.utc) - datetime.fromisoformat(ESSAI["fini"])).total_seconds() < 3600
    if lancer and not ESSAI["actif"] and not (recent and lancer != 2):
        asyncio.create_task(_essai(max(1, min(limite, 300))))
        return {"lance": True, "conseil": "rouvrez cette adresse (sans lancer=1) dans 2 à 3 minutes"}
    l = ESSAI["lignes"]
    return {k: ESSAI[k] for k in ("actif", "phase", "fait", "total", "fini")} | {
        "avec_date": sum(1 for x in l if x[4]), "flex_detectes": sum(1 for x in l if x[6]),
        "exemples": [{"societe": x[0], "date": x[4], "siege": x[5], "flex": x[6]} for x in l if x[4] or x[6]][:15],
        "excel": "/v3/validation/api/sirene-essai/excel"}


@router.get("/v3/validation/api/sirene-essai/excel")
async def sirene_essai_excel(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    from v3 import feuille
    wb = Workbook(); wb.remove(wb.active)
    feuille(wb, "Essai SIRENE", ["Société", "SIREN", "Adresse des bureaux (retenue)", "Établissement SIRENE à cette adresse",
                                 "Date de création de l'établissement", "Siège ?", "Opérateur flex détecté", "Indice / raison",
                                 "Début de bail actuel", "Flexoffice actuel"], ESSAI["lignes"])
    buf = io.BytesIO(); wb.save(buf)
    return StreamingResponse(io.BytesIO(buf.getvalue()), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="essai_sirene_bail_flex_{datetime.now():%Y-%m-%d}.xlsx"'})
