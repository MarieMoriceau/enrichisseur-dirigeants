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
                                "est_siege": bool(e.get("est_siege")), "ouvert": e.get("etat_administratif") == "A",
                                "texte": _texte_etab(e)})
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
        props["Opérateur flex"] = MEM._txt(r["operateur"] if sur else f"{r['operateur']} (probable)")
        if sur and not info.get("flexoffice") and await VALID.assurer_colonne("Flexoffice", "checkbox"):
            props["Flexoffice"] = {"checkbox": True}
    if props:
        await VALID._maj(page_id, props)
    return r | {"ecrit": sorted(props)}


async def appliquer_en_fond(page_id: str):
    """Appelé à la validation d'une société (adresse retenue connue)."""
    try:
        info = await VALID._une(page_id)
        if info and info.get("adresse_retenue"):
            async with httpx.AsyncClient(timeout=20) as api:
                await appliquer(api, page_id, info, info["adresse_retenue"])
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
async def sirene_flex(page_id: str, req: Request, adresse: str = ""):
    """Fiche de validation : ce que SIRENE dit pour l'adresse des bureaux affichée.
    - opérateur de flex (centre déclaré à cette adresse) ; enregistré seulement si Flexoffice est déjà coché ;
    - début de bail = date de création de l'établissement de la société à cette adresse exacte (siège ou
      secondaire) ; proposé, pas écrit : il l'est à la validation (même règle)."""
    if (x := VALID._refus(req, "negos")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    info = await VALID._une(page_id) or {}
    op = (info.get("operateur_flex") or "").strip()
    rep = {"operateur": "", "indice": "", "enregistre": False, "retire": op == "(aucun)", "date": "", "source_date": ""}
    if op and op != "(aucun)":
        rep.update(operateur=op.replace("(probable)", "").strip(), enregistre=True)
    adresse = adresse.strip() or info.get("adresse_retenue", "")
    if not (re.fullmatch(r"\d{9}", info.get("siren") or "") and adresse):
        return rep | {"indice": rep["indice"] or "pas de SIREN ou pas d'adresse"}
    if op and info.get("debut_bail"):
        return rep                                    # rien à chercher
    async with httpx.AsyncClient(timeout=20) as api:
        r = await analyser(api, info["siren"], info.get("nom", ""), adresse)
    if r["date"] and not info.get("debut_bail"):
        j = r["date"]
        rep.update(date=j, source_date=f"SIRENE : {'siège' if r['siege'].startswith('oui') else 'établissement secondaire'} "
                                       f"créé à cette adresse le {j[8:10]}/{j[5:7]}/{j[:4]}")
    if not op:
        rep.update(operateur=r["operateur"], indice=r["indice"])
        if r["operateur"] and info.get("flexoffice") and VALID.role(req) == "admin" \
                and await VALID.assurer_colonne("Opérateur flex"):
            await VALID._maj(page_id, {"Opérateur flex": MEM._txt(r["operateur"])})
            rep["enregistre"] = True
    return rep


@router.get("/v3/validation/api/operateurs-flex")
async def operateurs_flex(req: Request):
    """Liste des opérateurs de flex connus (saisie de l'opérateur sur la fiche)."""
    if (x := VALID._refus(req, "negos")):
        return x
    return {"operateurs": OPERATEURS_FLEX}
