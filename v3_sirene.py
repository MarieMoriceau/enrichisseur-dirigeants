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


# champs SIRENE autres que l'adresse où le nom d'un opérateur peut figurer (domiciliataire, complément, enseigne…)
def _texte_etab(e: dict) -> str:
    """Adresse + tous les textes de l'établissement dont le nom évoque une domiciliation, un complément d'adresse,
    une enseigne ou un nom commercial (le nom exact du champ « domiciliation » varie selon les sources)."""
    morceaux = [e.get("adresse") or ""]
    for k, v in e.items():
        if not re.search(r"domicil|complement|enseigne|commercial|denomination_usuelle", k):
            continue
        if isinstance(v, str):
            morceaux.append(v)
        elif isinstance(v, list):
            morceaux += [x for x in v if isinstance(x, str)]
        elif isinstance(v, dict):
            morceaux += [x for x in v.values() if isinstance(x, str)]
    return " ".join(morceaux)


def meme_rue(a: str, b: str) -> bool:
    """Même rue et même code postal, numéros différents tolérés (siège au 12, bureaux au 8 de la même rue)."""
    ka, kb = LI.cle_adresse(a), LI.cle_adresse(b)
    if not (ka and kb) or ka[2] != kb[2] or not (ka[1] and kb[1]):
        return False
    x, y = ka[1], kb[1]
    return x in y or y in x or x.split()[0] == y.split()[0]


def _numeros(a: str) -> set:
    """Numéros de rue d'une adresse, plages comprises : « 2-4 », « 17-21 », « 26 AU 28 », « 43- 45 » (SIRENE écrit
    souvent « 2-4 2 RUE JULES LEFEBVRE » quand les bureaux sont au « 4 rue Jules Lefebvre »)."""
    t = LI._sans_accents(a or "")
    t = t[:m.start()] if (m := re.search(r"\b\d{5}\b", t)) else t
    out = set()
    for d, f in re.findall(r"\b(\d{1,4})\s*(?:-|a|au|et)\s*(\d{1,4})\b", t):
        if 0 < int(f) - int(d) <= 12:                 # « 17-21 » = 17, 19, 21 (même côté de la rue)
            out |= set(range(int(d), int(f) + 1, 2 if (int(f) - int(d)) % 2 == 0 else 1))
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


def source_operateur(indice: str) -> str:
    """Texte de la colonne « Source opérateur flex » à partir de l'indice SIRENE."""
    return f"SIRENE : {indice}"[:300] if indice else "SIRENE"


async def props_operateur(valeur: str, source: str) -> dict:
    """Propriétés Notion de l'opérateur et de sa source (colonne créée au besoin)."""
    props = {"Opérateur flex": MEM._txt(valeur)}
    if await VALID.assurer_colonne("Source opérateur flex"):
        props["Source opérateur flex"] = MEM._txt(source)
    return props


def completer_cp(adresse: str, autres: list) -> str:
    """Adresse sans code postal (« 1 bis avenue de la République », fiche Pipedrive) : on reprend le code postal d'une
    autre source qui donne le même numéro dans la même rue. Plusieurs codes postaux possibles : on ne devine pas."""
    if _cp(adresse) or not (k := LI.cle_adresse(f"{adresse} 00000")) or not k[1]:
        return adresse
    cps = set()
    for a in autres:
        ka = LI.cle_adresse(a or "")
        if not (ka and ka[1]):
            continue
        x, y = k[1], ka[1]
        if (x in y or y in x or x.split()[0] == y.split()[0]) and (_numeros(adresse) & _numeros(a) or k[0] == ka[0]):
            cps.add(ka[2])
    return f"{adresse} {cps.pop()}" if len(cps) == 1 else adresse


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


def _noms_insee(x: dict) -> list:
    """Noms sous lesquels chercher la société : « YIELD (YIELD STUDIO) » → YIELD, YIELD STUDIO."""
    out = []
    for t in (x.get("nom_raison_sociale"), x.get("nom_complet"), x.get("sigle")):
        for m in re.findall(r"[^()]+", t or ""):
            if (m := m.strip()) and m.lower() not in [o.lower() for o in out]:
                out.append(m)
    return out


async def etablissements(api, siren: str, nom: str, cp: str) -> list:
    """Établissements de la société dans ce code postal (+ son siège) : [{adresse, date_creation, est_siege, ouvert}].
    L'API ne liste les établissements que quand le texte cherché correspond à leur nom : une recherche par SIREN ne
    renvoie que le siège. On cherche donc sous chacun de ses noms (nom de la fiche, raison sociale, nom complet, sigle),
    filtré par code postal, et on garde tout ce qui porte ce SIREN."""
    out = []

    def garder(x):
        for e in [x.get("siege") or {}] + (x.get("matching_etablissements") or []):
            if e.get("adresse") and not any(o["siret"] == e.get("siret") for o in out):
                out.append({"siret": e.get("siret", ""), "adresse": e["adresse"], "date_creation": e.get("date_creation") or "",
                            "est_siege": bool(e.get("est_siege")), "ouvert": e.get("etat_administratif") == "A",
                            "texte": _texte_etab(e)})

    noms = [nom] if nom else []
    for x in await _chercher(api, {"q": siren}):
        if x.get("siren") == siren:
            garder(x)
            noms += [n for n in _noms_insee(x) if n.lower() not in [o.lower() for o in noms]]
    for n in noms:
        if cp and any(_cp(e["adresse"]) == cp and not e["est_siege"] for e in out):
            break                                     # un établissement dans ce code postal : trouvé
        for x in await _chercher(api, {"q": n, "code_postal": cp} if cp else {"q": n}):
            if x.get("siren") == siren:
                garder(x)
    return out


# ── Liste Equation des adresses de coworkings et bureaux opérés (outil Dispos opérateurs, mise à jour chaque mois)
ADRESSES_URL = os.getenv("ADRESSES_OPERATEURS_URL", "https://dispos-operateurs.onrender.com/adresses.json")
ADRESSES_CLE = os.getenv("ADRESSES_OPERATEURS_CLE", "")          # même valeur que ADRESSES_CLE dans le service Dispos
ADRESSES_DUREE = 6 * 3600                                         # relue au plus toutes les 6 h
_ADRESSES = {"quand": 0.0, "releve": "", "liste": [], "erreur": ""}


async def adresses_operateurs() -> list:
    """[{operateur, nom, adresse, code_postal}] du dernier relevé ; liste précédente gardée si Dispos ne répond pas."""
    if not ADRESSES_CLE:
        _ADRESSES["erreur"] = "ADRESSES_OPERATEURS_CLE absente dans Render"
        return []
    if time.time() - _ADRESSES["quand"] < ADRESSES_DUREE and _ADRESSES["liste"]:
        return _ADRESSES["liste"]
    try:
        async with httpx.AsyncClient(timeout=90) as c:            # le service Dispos peut être endormi (~1 min)
            r = await c.get(ADRESSES_URL, headers={"X-Cle": ADRESSES_CLE})
        if r.status_code == 200:
            j = r.json()
            _ADRESSES.update(quand=time.time(), releve=j.get("releve", ""), liste=j.get("adresses") or [], erreur="")
        else:
            _ADRESSES.update(erreur=f"Dispos opérateurs : {r.status_code}", quand=time.time() - ADRESSES_DUREE + 600)
    except Exception as e:
        _ADRESSES.update(erreur=f"Dispos opérateurs injoignable : {e}", quand=time.time() - ADRESSES_DUREE + 600)
    return _ADRESSES["liste"]


def operateur_liste(bureaux: str, liste: list) -> tuple:
    """(opérateur, indice) si l'adresse des bureaux figure dans la liste Equation des coworkings / bureaux opérés."""
    for l in liste:
        adr = l.get("adresse_ban") or f"{l.get('adresse', '')} {l.get('code_postal', '')}"
        if meme_lieu(adr, bureaux):
            op = (l.get("operateur") or "").strip()
            if op.lower().startswith("indépendant"):
                op = f"{l['nom']} (indépendant)" if l.get("nom") else op
            centre = f"{l.get('nom')} : " if l.get("nom") else ""
            return op, (f"liste Equation des coworkings et bureaux opérés (relevé {_ADRESSES['releve'] or '?'}) : "
                        f"{centre}{l.get('adresse', '')} {l.get('code_postal', '')}").strip()
    return "", ""


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
    """{date, etab, siege, operateur, indice, sur} pour l'adresse des bureaux. `sur` : l'opérateur est écrit dans la
    fiche SIRENE de la société elle-même (pas seulement un centre voisin), Flexoffice peut être coché."""
    rep = {"date": "", "etab": "", "siege": "", "operateur": "", "indice": "", "sur": False}
    cp = _cp(bureaux)
    if not (siren and LI.cle_adresse(bureaux)):
        rep["indice"] = "adresse des bureaux incomplète (numéro, rue, code postal)" if siren else "pas de SIREN"
        return rep
    etabs = await etablissements(api, siren, nom, cp)
    ici = sorted([e for e in etabs if meme_lieu(e["adresse"], bureaux)], key=lambda e: (not e["ouvert"], e["date_creation"]))
    if ici:
        e = ici[0]
        rep.update(date=e["date_creation"], etab=e["adresse"], siege="oui" if e["est_siege"] else "non (établissement secondaire)")
        if (o := operateur_cite(e.get("texte") or e["adresse"])):
            rep.update(operateur=o, sur=True, indice=f"fiche SIRENE de la société : « {e.get('texte') or e['adresse']} »")
    else:
        rep["indice"] = "aucun établissement de la société à l'adresse des bureaux"
    if not rep["operateur"]:
        # siège ouvert domicilié chez un opérateur (« Domiciliation : PATCHWORK ») : dans la même rue que les bureaux,
        # c'est le centre où ils travaillent ; ailleurs, simple domiciliation possible → probable.
        # Pas de date de bail tirée de ce siège : seulement un établissement déclaré à l'adresse exacte des bureaux.
        for e in sorted([e for e in etabs if e["est_siege"] and e["ouvert"]], key=lambda e: e["date_creation"], reverse=True):
            if (o := operateur_cite(e.get("texte") or "")):
                proche = meme_rue(e["adresse"], bureaux)
                rep.update(operateur=o, sur=proche, indice=f"siège domicilié chez {o} (SIRENE : {e['adresse']})"
                                                            + ("" if proche else ", rue différente des bureaux"))
                break
    if not rep["operateur"] and cp:
        for o, a, n in await centres_flex(api, cp):
            if meme_lieu(a, bureaux):
                rep.update(operateur=o, indice=f"centre {o} à la même adresse ({n} : {a})")
                break
    if not rep["operateur"]:                          # SIRENE muet : liste Equation des coworkings / bureaux opérés
        o, indice = operateur_liste(bureaux, await adresses_operateurs())
        if o:
            rep.update(operateur=o, indice=indice)
    return rep


# ───────────────────────── écriture dans la fiche ─────────────────────────
async def appliquer(api, page_id: str, info: dict, adresse: str) -> dict:
    """Remplit ce qui est vide : « Début de bail » (+ sa source) et « Opérateur flex » ; coche « Flexoffice » quand
    l'opérateur figure dans la fiche SIRENE de la société (adresse, domiciliation, enseigne : sûr). Centre à la même adresse : « … (probable) », à confirmer.
    Ne remplace jamais une saisie à la main ni une valeur venue du fichier CRM."""
    r = await analyser(api, info.get("siren", ""), info.get("nom", ""), adresse)
    props = {}
    if r["date"] and not info.get("debut_bail") and await VALID.assurer_colonne("Début de bail", "date"):
        props["Début de bail"] = {"date": {"start": r["date"]}}
        if await VALID.assurer_colonne("Source début de bail"):
            props["Source début de bail"] = MEM._txt(f"SIRENE : établissement ({'siège' if r['siege'].startswith('oui') else 'secondaire'}) "
                                                     f"créé à cette adresse le {r['date'][8:10]}/{r['date'][5:7]}/{r['date'][:4]}")
    if r["operateur"] and not (info.get("operateur_flex") or "").replace("(probable)", "").strip() \
            and await VALID.assurer_colonne("Opérateur flex"):
        sur = bool(r.get("sur")) or bool(info.get("flexoffice"))   # déjà coché : l'opérateur n'est plus « probable »
        props |= await props_operateur(r["operateur"] if sur else f"{r['operateur']} (probable)", source_operateur(r["indice"]))
        if sur and not info.get("flexoffice") and await VALID.assurer_colonne("Flexoffice", "checkbox"):
            props["Flexoffice"] = {"checkbox": True}
    if props:
        await VALID._maj(page_id, props)
    return r | {"ecrit": sorted(props)}


ADRESSES_FICHE = ("adresse_google", "adresse_linkedin", "adresse_site", "adresse_crm", "adresse_proposee", "adresse_notion")


async def appliquer_en_fond(page_id: str):
    """Appelé à la validation d'une société (adresse retenue connue)."""
    try:
        info = await VALID._une(page_id)
        if info and info.get("adresse_retenue"):
            adresse = info["adresse_retenue"]
            if not _cp(adresse):                       # adresse du CRM sans code postal : repris d'une autre source
                d = await MEM.lire(page_id) or {}
                adresse = completer_cp(adresse, [d.get(k, "") for k in ADRESSES_FICHE])
            async with httpx.AsyncClient(timeout=20) as api:
                await appliquer(api, page_id, info, adresse)
    except Exception as e:
        print(f"[V3 SIRENE] {page_id} : {e}")


# ───────────────────────── rattrapage des sociétés déjà validées ─────────────────────────
RATTRAPAGE = {"actif": False, "phase": "", "fait": 0, "total": 0, "dates": 0, "flex_surs": 0, "flex_probables": 0, "echecs": []}


async def _rattraper():
    RATTRAPAGE.update(actif=True, phase="lecture SIRENE et écriture", fait=0, dates=0, flex_surs=0, flex_probables=0, echecs=[])
    try:
        socs = [s for s in await VALID._toutes() if s["validation"] in ("Société validée", "Contacts validés")
                and s.get("adresse_retenue") and re.fullmatch(r"\d{9}", s.get("siren") or "")
                and (not s.get("debut_bail") or not s.get("operateur_flex"))]
        RATTRAPAGE["total"] = len(socs)
        async with httpx.AsyncClient(timeout=20) as api:
            for s in socs:
                try:
                    r = await appliquer(api, s["page"], s, s["adresse_retenue"])
                    RATTRAPAGE["dates"] += "Début de bail" in r["ecrit"]
                    if "Opérateur flex" in r["ecrit"]:
                        RATTRAPAGE["flex_surs" if "Flexoffice" in r["ecrit"] else "flex_probables"] += 1
                except Exception as e:
                    RATTRAPAGE["echecs"].append(f"{s['nom']} : {e}")
                RATTRAPAGE["fait"] += 1
        RATTRAPAGE["phase"] = "terminé"
    except Exception as e:
        RATTRAPAGE["phase"] = f"arrêt sur erreur : {e}"
    finally:
        RATTRAPAGE["actif"] = False


@router.get("/v3/validation/api/sirene-rattrapage")
async def sirene_rattrapage(req: Request, confirmer: int = 0):
    """Admin. Sociétés validées : remplit « Début de bail » et « Opérateur flex » vides d'après SIRENE.
    Sans &confirmer=1 : nombre de sociétés concernées. Avec : lance en tâche de fond ; rappeler l'adresse pour suivre."""
    if (x := VALID._refus(req, "admin")):
        return x
    if RATTRAPAGE["actif"] or (RATTRAPAGE["phase"] and not confirmer):
        return dict(RATTRAPAGE)
    if confirmer:
        asyncio.create_task(_rattraper())
        return {"lance": True, "conseil": "rouvrez cette adresse (sans confirmer) pour suivre"}
    socs = [s for s in await VALID._toutes() if s["validation"] in ("Société validée", "Contacts validés")
            and s.get("adresse_retenue") and re.fullmatch(r"\d{9}", s.get("siren") or "")]
    return {"essai_a_blanc": True, "societes_validees": len(socs),
            "sans_date_de_bail": sum(1 for s in socs if not s.get("debut_bail")),
            "sans_operateur_flex": sum(1 for s in socs if not s.get("operateur_flex")),
            "conseil": "ajoutez ?confirmer=1 pour remplir (cases vides seulement)"}


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


@router.get("/v3/validation/api/sirene-une")
async def sirene_une(req: Request, siren: str, adresse: str, nom: str = ""):
    """Admin. Essai à blanc pour une société : ce que SIRENE donne pour ces bureaux, rien n'est écrit.
    Montre aussi les champs bruts du siège pour vérifier où figure la domiciliation."""
    if (x := VALID._refus(req, "admin")):
        return x
    async with httpx.AsyncClient(timeout=20) as api:
        r = await analyser(api, siren.strip(), nom, adresse)
        brut = [x.get("siege") for x in await _chercher(api, {"q": siren.strip()}) if x.get("siren") == siren.strip()]
    return {"essai_a_blanc": True, "resultat": r, "siege_brut": brut[:1]}


@router.get("/v3/validation/api/sirene-flex/{page_id}")
async def sirene_flex(page_id: str, req: Request, adresse: str = "", autres: str = "", relire: int = 0):
    """Fiche de validation : ce que SIRENE dit pour l'adresse des bureaux affichée.
    - opérateur de flex (centre déclaré à cette adresse) ; enregistré si Flexoffice est déjà coché ;
    - début de bail = date de création de l'établissement de la société à cette adresse exacte (siège ou secondaire) ;
      enregistré tout de suite si la case est vide (admin).
    `autres` : les autres adresses de la fiche (séparées par « | »), pour retrouver un code postal manquant.
    `relire=1` : l'adresse cochée a changé. Ce que SIRENE avait rempli pour l'ancienne adresse (source « SIRENE… »)
    est recalculé pour la nouvelle, ou effacé si SIRENE n'a rien ; les saisies à la main et le « (aucun) » restent.
    La case Flexoffice n'est jamais touchée. Réponse : `etat` = valeurs enregistrées après passage."""
    if (x := VALID._refus(req, "negos")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    info = await VALID._une(page_id) or {}
    admin = VALID.role(req) == "admin"
    if relire:
        return await _relire_adresse(page_id, info, adresse, autres, admin)
    op = (info.get("operateur_flex") or "").strip()
    rep = {"operateur": "", "indice": "", "enregistre": False, "retire": op == "(aucun)", "date": "", "source_date": "",
           "date_ecrite": False, "adresse_lue": ""}
    if op and op != "(aucun)":
        rep.update(operateur=op.replace("(probable)", "").strip(), enregistre=True)
    adresse = completer_cp(adresse.strip() or info.get("adresse_retenue", ""), autres.split("|"))
    rep["adresse_lue"] = adresse
    if not (re.fullmatch(r"\d{9}", info.get("siren") or "") and adresse):
        return rep | {"indice": rep["indice"] or "pas de SIREN ou pas d'adresse"}
    if not _cp(adresse):
        return rep | {"indice": rep["indice"] or "adresse sans code postal : SIRENE ne peut pas la comparer",
                      "sans_cp": True}
    if op and info.get("debut_bail") and (info.get("source_flex") or op == "(aucun)"):
        return rep                                    # rien à chercher
    async with httpx.AsyncClient(timeout=20) as api:
        r = await analyser(api, info["siren"], info.get("nom", ""), adresse)
    if r["date"] and not info.get("debut_bail"):
        j = r["date"]
        source = (f"SIRENE : {'siège' if r['siege'].startswith('oui') else 'établissement secondaire'} "
                  f"créé à cette adresse le {j[8:10]}/{j[5:7]}/{j[:4]}")
        rep.update(date=j, source_date=source)
        if VALID.role(req) == "admin" and await VALID.assurer_colonne("Début de bail", "date"):
            props = {"Début de bail": {"date": {"start": j}}}
            if await VALID.assurer_colonne("Source début de bail"):
                props["Source début de bail"] = MEM._txt(source)
            await VALID._maj(page_id, props)
            rep["date_ecrite"] = True
    if not op:
        rep.update(operateur=r["operateur"], indice=r["indice"], source=source_operateur(r["indice"]) if r["operateur"] else "")
        if r["operateur"] and info.get("flexoffice") and VALID.role(req) == "admin" \
                and await VALID.assurer_colonne("Opérateur flex"):
            await VALID._maj(page_id, await props_operateur(r["operateur"], rep["source"]))
            rep["enregistre"] = True
    elif op != "(aucun)" and not info.get("source_flex") and r["operateur"] \
            and r["operateur"].lower() == rep["operateur"].lower():
        rep["source"] = source_operateur(r["indice"])     # opérateur enregistré avant qu'on garde sa source
        if VALID.role(req) == "admin" and await VALID.assurer_colonne("Source opérateur flex"):
            await VALID._maj(page_id, {"Source opérateur flex": MEM._txt(rep["source"])})
    return rep


def _auto(source: str) -> bool:
    """Valeur remplie par l'outil à partir de SIRENE (et non saisie ou retirée à la main)."""
    return (source or "").strip().upper().startswith("SIRENE")


async def _relire_adresse(page_id: str, info: dict, adresse: str, autres: str, admin: bool) -> dict:
    """Changement d'adresse cochée : début de bail et opérateur tirés de SIRENE recalculés pour la nouvelle adresse."""
    op = (info.get("operateur_flex") or "").strip()
    bail, src_bail, src_flex = info.get("debut_bail") or "", info.get("source_bail") or "", info.get("source_flex") or ""
    bail_auto, op_auto = bool(bail) and _auto(src_bail), bool(op) and op != "(aucun)" and _auto(src_flex)
    adresse = completer_cp(adresse.strip(), autres.split("|"))
    rep = {"relu": True, "adresse_lue": adresse, "operateur": "", "indice": "", "source": "", "date": "", "source_date": "",
           "bail_garde": bool(bail) and not bail_auto, "op_garde": bool(op) and not op_auto}
    r = {"date": "", "siege": "", "operateur": "", "indice": ""}
    if re.fullmatch(r"\d{9}", info.get("siren") or "") and adresse and _cp(adresse):
        async with httpx.AsyncClient(timeout=20) as api:
            r = await analyser(api, info["siren"], info.get("nom", ""), adresse)
    elif adresse and not _cp(adresse):
        rep["sans_cp"] = True
    props = {}
    if not rep["bail_garde"]:
        if r["date"]:
            j = r["date"]
            src = (f"SIRENE : {'siège' if r['siege'].startswith('oui') else 'établissement secondaire'} "
                   f"créé à cette adresse le {j[8:10]}/{j[5:7]}/{j[:4]}")
            rep.update(date=j, source_date=src)
            if j != bail:
                props |= {"Début de bail": {"date": {"start": j}}, "Source début de bail": MEM._txt(src)}
                bail, src_bail = j, src
        elif bail_auto:                                   # la date venait de l'ancienne adresse
            props |= {"Début de bail": {"date": None}, "Source début de bail": MEM._txt("")}
            bail, src_bail = "", ""
    if not rep["op_garde"] and op != "(aucun)":
        rep.update(operateur=r["operateur"], indice=r["indice"], source=source_operateur(r["indice"]) if r["operateur"] else "")
        if r["operateur"] and (info.get("flexoffice") or op_auto):
            if r["operateur"] != op or rep["source"] != src_flex:
                props |= {"Opérateur flex": MEM._txt(r["operateur"]), "Source opérateur flex": MEM._txt(rep["source"])}
                op, src_flex = r["operateur"], rep["source"]
        elif op_auto:                                     # l'opérateur venait de l'ancienne adresse
            props |= {"Opérateur flex": MEM._txt(""), "Source opérateur flex": MEM._txt("")}
            op, src_flex = "", ""
    if props and admin:
        for col, typ in (("Début de bail", "date"), ("Source début de bail", "rich_text"),
                         ("Opérateur flex", "rich_text"), ("Source opérateur flex", "rich_text")):
            if col in props and not await VALID.assurer_colonne(col, typ):
                props.pop(col)
        await VALID._maj(page_id, props)
    elif props:                                           # négo : on montre sans enregistrer
        bail, src_bail, op, src_flex = info.get("debut_bail") or "", info.get("source_bail") or "", \
            (info.get("operateur_flex") or "").strip(), info.get("source_flex") or ""
    rep["etat"] = {"debut_bail": bail, "source_bail": src_bail, "operateur_flex": op, "source_flex": src_flex}
    return rep


@router.get("/v3/validation/api/operateurs-flex")
async def operateurs_flex(req: Request):
    """Liste des opérateurs de flex connus (saisie de l'opérateur sur la fiche)."""
    if (x := VALID._refus(req, "negos")):
        return x
    noms = list(OPERATEURS_FLEX)
    for l in await adresses_operateurs():
        o = (l.get("operateur") or "").strip()
        if o and not o.lower().startswith("indépendant") and o.lower() not in [n.lower() for n in noms]:
            noms.append(o)
    return {"operateurs": sorted(noms, key=str.lower), "releve": _ADRESSES["releve"], "erreur": _ADRESSES["erreur"]}
