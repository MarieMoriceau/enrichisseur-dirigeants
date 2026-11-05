"""Adresse physique (prospection terrain) reportée dans la mémoire après coup.

Les négos lancent parfois l'enrichisseur sur des sociétés vues sur le terrain sans mettre l'adresse dans le fichier.
Cette route va la chercher dans la base Notion « 🚶🏻‍♂️ Prospection terrain » et la reporte dans le JSON de la mémoire
(champ adresse_notion = « Prospection physique » dans l'écran de validation), puis recalcule l'adresse proposée.
Elle reporte aussi le bureau flexible vu sur place : colonne « Type de société » = flex, ou opérateur nommé dans
« Remarques » (« Come and work », « Klam », « occupant morning », « Lnb »…) → case Flexoffice et Opérateur flex.
Une saisie à la main dans la validation n'est jamais remplacée ; un opérateur SIRENE « (probable) » l'est.

  /v3/validation/api/adresses-terrain?lancement=Prosp terrain,Office national   → essai à blanc (rien n'est écrit)
  … &confirmer=1                                                                  → écriture
  … &etat=1                                                                       → avancement / rapport
  ?lancement=*                                                                    → tous les lancements
  … &par=Sébastien                                                                → seulement les sociétés de ce négo
                                                                                    (son prénom reçu ou ses propres lancements)
Adresse : colonne « Adresse normalisée » de la base (« 11 Rue du Faubourg Poissonnière, 75009 Paris, France ») quand elle a
un code postal ; sinon l'ancienne lecture de « Adresse » + « CP » (BAN). Une adresse terrain déjà présente est remplacée par
la normalisée seulement s'il s'agit du même numéro dans la même rue (ou si elle n'avait pas de code postal).
Au lancement (traiter), les sociétés trouvées dans la base reçoivent aussi cette adresse (completer_lancement).
La base terrain doit être partagée avec l'intégration Notion de l'outil (comme la mémoire).
"""
import asyncio, os, re, time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import v3_linkedin as LI
import v3_memoire as MEM
import v3_sirene as SIRENE
import v3_validation as VALID

router = APIRouter()
TERRAIN_DB = os.getenv("TERRAIN_DB", "c70e0416a7d64dd990011f222bf5a4e7")
ETAT: dict = {"en_cours": False, "mode": "", "phase": "", "fait": 0, "total": 0, "lignes": [], "fini": "", "erreur": ""}


def _v(p: dict, nom: str) -> str:
    return MEM._val(p, nom) if p.get(nom) else ""


async def lire_terrain(c: httpx.AsyncClient) -> list:
    out, curseur = [], None
    while True:
        corps = {"page_size": 100, **({"start_cursor": curseur} if curseur else {})}
        r = await MEM._req(c, "POST", f"https://api.notion.com/v1/databases/{TERRAIN_DB}/query", json=corps)
        if r.status_code != 200:
            raise RuntimeError(f"base Prospection terrain inaccessible ({r.status_code}) : partagez-la avec l'intégration de l'outil")
        j = r.json()
        for pg in j.get("results", []):
            p = pg.get("properties") or {}
            out.append({"nom": _v(p, "Société"), "nom_pd": _v(p, "Nom Pipedrive"), "id": re.sub(r"\D", "", _v(p, "ID Pipedrive")),
                        "adresse": _v(p, "Adresse"), "cp": _v(p, "CP"), "nego": _v(p, "Négo"),
                        "date": _v(p, "Date de visite")[:10], "remarques": _v(p, "Remarques"),
                        "type": _v(p, "Type de société"), "adresse_norm": _v(p, "Adresse normalisée"),
                        "page": (pg.get("id") or "").replace("-", "")})
        if not j.get("has_more"):
            return out
        curseur = j.get("next_cursor")


def rue_et_cp(t: dict) -> tuple:
    """(« 8 rue d'Athènes », code postal écrit par le négo ou déduit de « 8e » / « 2ème arrondissement »)."""
    a = re.sub(r"\s+", " ", t.get("adresse") or "").strip(" ,")
    cp = (t.get("cp") or "").strip()
    m = re.search(r"\b(\d{5})\b", a)
    if m:
        cp, a = cp or m.group(1), (a[:m.start()] + a[m.end():]).strip(" ,")
    arr = re.search(r",?\s*\b(\d{1,2})\s*(?:e|er|ème|eme|è)\b(?:\s*arrondissement)?\s*$", a, re.I)
    if arr and 1 <= int(arr.group(1)) <= 20:
        cp, a = cp or f"750{int(arr.group(1)):02d}", a[:arr.start()].strip(" ,")
    a = re.sub(r"[\s,]+paris$", "", a, flags=re.I).strip(" ,")
    return a, cp


def adresse_complete(t: dict) -> str:
    """Adresse avec le code postal écrit (ou l'arrondissement) ; sans code postal, la rue seule."""
    brut = re.sub(r"\s+", " ", t.get("adresse") or "").strip(" ,")
    if re.search(r"\b\d{5}\b", brut):                  # adresse déjà complète : telle quelle
        return brut
    rue, cp = rue_et_cp(t)
    if not rue:
        return ""
    return f"{rue} {cp}" + (" Paris" if cp.startswith("75") else "") if cp else rue


_BAN: dict = {}


async def ban_paris(rue: str) -> str:
    """Adresse officielle (BAN) d'un numéro + rue à Paris : corrige le code postal (« 8 rue d'Athènes, 8e » → 75009)
    et les fautes (« rue d'Ambroise » → Rue d'Amboise). "" si la BAN hésite ou ne répond pas."""
    from difflib import SequenceMatcher
    if rue in _BAN:
        return _BAN[rue]
    num = re.match(r"\s*(\d+)", rue)
    if not num:
        return ""
    voie = LI._sans_accents(rue[num.end():]).strip(" ,")
    feats = None
    async with httpx.AsyncClient(timeout=10) as c:
        for essai in range(3):                         # la BAN refuse parfois une rafale : on réessaie
            for url in SIRENE.BAN_URLS:
                try:
                    r = await c.get(url, params={"q": f"{rue} Paris", "limit": 10, "type": "housenumber"})
                    if r.status_code == 200:
                        feats = r.json().get("features", [])
                        break
                except (httpx.HTTPError, ValueError):
                    continue
            if feats is not None:
                break
            await asyncio.sleep(1 + essai)
    if feats is None:
        return ""                                      # BAN muette : pas mis en cache, on retentera
    bons = {}
    for f in feats:
        p = f.get("properties", {})
        cp = str(p.get("postcode") or "")
        if not cp.startswith("75") or str(p.get("housenumber", "")).split()[0:1] != [num.group(1)]:
            continue
        if SequenceMatcher(None, voie, LI._sans_accents(p.get("street", ""))).ratio() >= 0.75:
            bons.setdefault(cp, f"{p.get('housenumber')} {p.get('street')} {cp} Paris")
    _BAN[rue] = next(iter(bons.values())) if len(bons) == 1 else ""
    return _BAN[rue]


async def adresse_paris(t: dict, autres: list) -> str:
    """Adresse terrain complète : BAN d'abord (code postal juste, fautes corrigées), sinon le code postal écrit par le négo,
    sinon le code postal des autres adresses connues de la même rue."""
    rue, cp = rue_et_cp(t)
    if not rue:
        return ""
    if (officielle := await ban_paris(rue)):
        return officielle
    if cp:
        return adresse_complete(t)
    a, _ = await SIRENE.completer_adresse(rue, autres + ["1 rue de la Paix 75002 Paris"])
    return a


def adresse_normalisee(t: dict) -> str:
    """« 40 Rue du Faubourg Poissonnière, 75010 Paris, France » → « 40 bis Rue du Faubourg Poissonnière 75010 Paris »
    (le « bis » relevé par le négo est remis). "" sans code postal (« 102 Rue Rayon, Paris, France »)."""
    a = re.sub(r"\s+", " ", t.get("adresse_norm") or "").strip(" ,")
    a = re.sub(r",?\s*France$", "", a, flags=re.I).strip(" ,")
    if not re.search(r"\b\d{5}\b", a):
        return ""
    a = re.sub(r"\s*,\s*", " ", a)
    brut = re.match(r"\s*(\d+)\s*(bis|ter)\b", t.get("adresse") or "", re.I)
    if brut and re.match(rf"{brut.group(1)}\b(?!\s*(bis|ter))", a, re.I):
        a = f"{brut.group(1)} {brut.group(2).lower()}{a[len(brut.group(1)):]}"
    return a


async def adresse_terrain(t: dict, autres: list) -> str:
    """Adresse normalisée de la base d'abord, sinon l'adresse relevée complétée (BAN, code postal du négo…)."""
    return adresse_normalisee(t) or await adresse_paris(t, autres)


def meme_lieu(a: str, b: str) -> bool:
    """Même numéro dans la même rue (code postal ignoré : c'est lui qu'on corrige souvent)."""
    ka, kb = LI.cle_adresse(_avec_cp_fictif(a)), LI.cle_adresse(_avec_cp_fictif(b))
    return bool(ka and kb and ka[0] == kb[0] and ka[1] and kb[1] and (ka[1] in kb[1] or kb[1] in ka[1]))


def _avec_cp_fictif(a: str) -> str:
    return a if re.search(r"\b\d{5}\b", a or "") else f"{a} 00000"


def remplacer(avant: str, nouvelle: str) -> str:
    """Que faire de l'adresse terrain déjà présente : "garder" (identique ou autre lieu) ou "remplacer"."""
    if not avant:
        return "remplacer"
    if avant.strip().lower() == nouvelle.strip().lower():
        return "garder"
    cp_avant, cp_nouv = re.search(r"\b\d{5}\b", avant), re.search(r"\b\d{5}\b", nouvelle)
    if not cp_avant:
        return "remplacer" if meme_lieu(avant, nouvelle) or not LI.cle_adresse(_avec_cp_fictif(avant)) else "garder"
    if meme_lieu(avant, nouvelle) and cp_nouv and cp_avant.group(0) != cp_nouv.group(0):
        return "remplacer"                             # même lieu, code postal corrigé
    return "garder"                                    # même lieu écrit autrement (« , France »), ou autre lieu


async def completer_lancement(orgs: list, log=lambda m: None) -> int:
    """Au lancement : les sociétés passées en prospection terrain reçoivent l'adresse normalisée de la base
    (adresse_notion = « Prospection physique »). Une adresse terrain venue du fichier n'est remplacée que si c'est
    le même lieu mieux écrit. Ne bloque jamais le lancement."""
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            terrain = await lire_terrain(c)
    except Exception as e:
        log(f"Prospection terrain illisible ({e}) : adresses du fichier seulement")
        return 0
    n = 0
    for o in orgs:
        ref = re.sub(r"[^0-9a-f]", "", str((o.get("infos_source") or {}).get("ref_notion") or "").lower())
        t = next((x for x in terrain if ref and x["page"] == ref and x["adresse"]), None)
        if not t:
            t, _ = apparier(o.get("nom", ""), re.sub(r"\D", "", str(o.get("id") or "")), terrain, None)
        if not t or not (a := adresse_normalisee(t)):
            continue
        if remplacer(o.get("adresse_notion", ""), a) == "remplacer":
            o["adresse_notion"] = a
            o["cp"] = o.get("cp") or re.search(r"\b(\d{5})\b", a).group(1)
            n += 1
    if n:
        log(f"Prospection terrain : adresse normalisée reprise pour {n} société(s)")
    return n


def meme_nom(a: str, b: str) -> bool:
    """Même société : noms identiques (sans accents, espaces ni mots de métier) ou presque (fautes de frappe).
    Pas d'inclusion de mots : « Gibert avocats » ne doit pas prendre la fiche « Avocats »."""
    from v3 import norm_nom, nom_court
    from difflib import SequenceMatcher
    if not a or not b:
        return False
    ca, cb = norm_nom(a).replace(" ", ""), norm_nom(b).replace(" ", "")
    ka, kb = nom_court(a).replace(" ", "") or ca, nom_court(b).replace(" ", "") or cb
    if ca == cb or (ka and ka == kb):
        return bool(ca)
    return min(len(ca), len(cb)) >= 8 and SequenceMatcher(None, ca, cb).ratio() >= 0.9   # « ERMES » ≠ « Hermès »


def apparier(nom: str, id_pd: str, terrain: list, ressemblance) -> tuple:
    """(fiche terrain, comment) ; (None, raison) si rien ou plusieurs adresses différentes."""
    if id_pd:
        par_id = [t for t in terrain if t["id"] == id_pd and t["adresse"]]
        if par_id:
            return max(par_id, key=lambda t: t["date"]), "ID Pipedrive"
    proches = [t for t in terrain if t["adresse"] and (meme_nom(nom, t["nom"]) or meme_nom(nom, t["nom_pd"]))]
    if not proches:
        return None, "absente de Prospection terrain"
    lieu = lambda t: (k[:2] if (k := LI.cle_adresse(_avec_cp_fictif(adresse_complete(t)))) else adresse_complete(t).lower())
    adresses = {lieu(t) for t in proches}            # « 10 rue de la pépinière » = « 10 rue de la Pépinière 75008 Paris »
    if len(adresses) > 1:
        return None, "plusieurs adresses terrain pour ce nom : " + " | ".join(sorted({adresse_complete(t) for t in proches}))[:200]
    return max(proches, key=lambda t: (bool(adresse_normalisee(t)), t["date"])), "nom"


# Abréviations des négos → nom de la liste des opérateurs (v3_sirene.OPERATEURS_FLEX)
ALIAS_FLEX = {"lnb": "Les Nouveaux Bureaux", "come work": "Come and Work", "comeandwork": "Come and Work",
              "come and work": "Come and Work", "wework": "WeWork", "we work": "WeWork", "reflexe": "Reflex"}
# Doute du négo (« je ne sais pas s'ils ont un plateau de Deskeo ») : sans « flex » dans Type de société, on ne retient pas l'opérateur
DOUTES = ("sais pas", "sait pas", "peut etre", "pas sur", "pas sure")
# Classée flex mais ce n'est peut-être pas un opérateur (sous-location, « soit c'est un opérateur soit… ») : on ne coche rien
PAS_FLEX_SUR = ("sous loc", "sous location", "soit c est", "soit cest")


def operateur_remarque(texte: str) -> str:
    """Opérateur de flex nommé dans une remarque de négo ; "" sinon."""
    a = f" {SIRENE._norm(texte)} "
    for k, v in ALIAS_FLEX.items():
        if f" {k} " in a:
            return v
    return SIRENE.operateur_cite(texte)


def flex_terrain(t: dict, terrain: list) -> tuple:
    """(bureau flexible vu sur place, opérateur, fiche qui le dit) à partir de toutes les visites de la même société
    (plusieurs passages : « Lokki — Come and work » puis « Lokki — viennent d'arriver »). La visite la plus récente d'abord."""
    memes = [x for x in terrain if (t["id"] and x["id"] == t["id"]) or meme_nom(t["nom"], x["nom"])
             or (t["nom_pd"] and meme_nom(t["nom_pd"], x["nom_pd"] or x["nom"]))]
    memes = sorted(memes or [t], key=lambda x: x["date"] or "", reverse=True)
    for x in memes:
        rem = x.get("remarques", "")
        if not (op := operateur_remarque(rem)):
            continue
        n = f" {SIRENE._norm(rem)} "
        if (x.get("type") or "").strip().lower() != "flex" and any(f" {d} " in n for d in DOUTES):
            continue
        return True, op, x
    x = next((x for x in memes if (x.get("type") or "").strip().lower() == "flex"), None)
    if x and any(f" {d} " in f" {SIRENE._norm(x.get('remarques', ''))} " for d in PAS_FLEX_SUR):
        return False, "", None
    return (True, "", x) if x else (False, "", None)


AUCUN = "(aucun)"      # même valeur que « retiré à la main » : SIRENE ne repropose plus d'opérateur


def _canon(op: str) -> str:
    """Nom comparable d'un opérateur : « LNB » = « Les Nouveaux Bureaux »."""
    return SIRENE._norm(operateur_remarque(op) or op)


def decision_flex(s: dict, flex: bool, op: str, source: str) -> tuple:
    """(propriétés Notion à écrire, action pour le rapport) d'après la fiche de validation s."""
    if not flex:
        return {}, ""
    props, faits = {}, []
    if not s.get("flexoffice"):
        props["Flexoffice"] = {"checkbox": True}
        faits.append("Flexoffice cochée")
    actuel = (s.get("operateur_flex") or "").strip()
    if op:
        if actuel == "(aucun)":
            faits.append("opérateur retiré à la main : non touché")
        elif not actuel or actuel.endswith("(probable)"):
            props["Opérateur flex"] = op
            props["_source"] = source
            faits.append(f"opérateur {op}" + (f" (remplace {actuel})" if actuel else ""))
        elif _canon(actuel) != _canon(op):           # la fiche et le terrain se contredisent : on laisse vide (décision Marie)
            props["Opérateur flex"] = AUCUN
            props["_source"] = f"vidé : la fiche disait {actuel}, le terrain dit {op} ({source})"
            faits.append(f"conflit : opérateur vidé (fiche {actuel}, terrain {op})")
    return props, " ; ".join(faits) or "déjà à jour"


async def _flex(s: dict, t: dict, terrain: list, ligne: dict, confirmer: bool):
    flex, op, x = flex_terrain(t, terrain)
    if not flex:
        return
    ligne["remarque"] = f"{x.get('type') or ''} | {x.get('remarques') or ''}".strip(" |")[:200]
    source = f"Prospection terrain ({x.get('nego') or '?'}, visite du {x.get('date') or '?'})"
    props, action = decision_flex(s, flex, op, source)
    ligne["flex"] = action
    if not confirmer or not props:
        return
    ecrire = {}
    if "Flexoffice" in props and await VALID.assurer_colonne("Flexoffice", "checkbox"):
        ecrire["Flexoffice"] = props["Flexoffice"]
    if "Opérateur flex" in props and await VALID.assurer_colonne("Opérateur flex"):
        ecrire |= await SIRENE.props_operateur(props["Opérateur flex"], props["_source"])
    r = await VALID._maj(s["page"], ecrire) if ecrire else None
    if isinstance(r, JSONResponse):
        ligne["flex"] = "écriture refusée par Notion : " + action


async def _tourner(lancements: list, confirmer: bool, par: str = ""):
    from v3 import ressemblance                        # import tardif (v3 importe ce module)
    ETAT.update(en_cours=True, mode="réel" if confirmer else "essai à blanc", phase="Lecture de Prospection terrain…",
                fait=0, total=0, lignes=[], fini="", erreur="")
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            terrain = await lire_terrain(c)
            ETAT["phase"] = "Lecture de la mémoire…"
            pages = [s for s in await VALID._toutes()
                     if lancements == ["*"] or any(l.lower() in (s.get("lancement") or "").lower() for l in lancements)]
            if par:
                import v3_equipe as EQ
                liste = await EQ.membres()
                pages = [s for s in pages if EQ.norm(par) in {EQ.norm(p) for p in EQ.attribution(liste, s.get("negos", ""),
                                                                                                   s.get("lance_par", ""))["a"]}]
            ETAT["total"] = len(pages)
            for s in pages:
                ETAT["phase"] = f"{s['nom']}…"
                data = await MEM.lire(s["page"])
                ligne = {"societe": s["nom"], "lancement": s.get("lancement", "")[:80], "adresse_terrain": "", "trouvee_par": "",
                         "avant": (data or {}).get("adresse_notion", ""), "action": "", "flex": "", "remarque": ""}
                if not data:
                    ligne["action"] = "page illisible"
                else:
                    t, comment = apparier(data.get("nom") or s["nom"], re.sub(r"\D", "", str(data.get("id") or "")), terrain, ressemblance)
                    if not t:
                        ligne["action"] = comment
                    else:
                        autres = [x for x in (data.get("adresse_crm"), data.get("adresse_site"), data.get("adresse_linkedin"),
                                              data.get("adresse_google"), data.get("siege_insee")) if x]
                        a = await adresse_terrain(t, autres)
                        ligne.update(adresse_terrain=a, trouvee_par=f"{comment} ({t['nego']}, visite du {t['date'] or '?'})"
                                     + (" · adresse normalisée" if adresse_normalisee(t) else ""))
                        avant = data.get("adresse_notion") or ""
                        if not re.search(r"\b\d{5}\b", a):
                            ligne["action"] = "code postal introuvable : non reportée" if not avant else "déjà renseignée"
                        elif avant and remplacer(avant, a) == "garder":
                            ligne["action"] = "déjà renseignée" if meme_lieu(avant, a) \
                                else "autre adresse dans la fiche : non touchée"
                        else:
                            ligne["action"] = ("à remplacer" if avant else "à reporter") if not confirmer else ("remplacée" if avant else "reportée")
                            if s.get("adresse_retenue"):
                                ligne["note"] = f"adresse retenue à la validation inchangée : {s['adresse_retenue']}"
                            if confirmer:
                                data["adresse_notion"] = a
                                data.update(LI.decider_adresse(data))
                                from v3_rattrapage import _reecrire
                                await _reecrire(c, s["page"], data)
                        await _flex(s, t, terrain, ligne, confirmer)
                ETAT["lignes"].append(ligne)
                ETAT["fait"] += 1
    except Exception as e:
        ETAT["erreur"] = str(e)
    finally:
        n = sum(1 for l in ETAT["lignes"] if l["action"] in ("à reporter", "reportée"))
        r = sum(1 for l in ETAT["lignes"] if l["action"].startswith(("à remplacer", "remplacée")))
        f = sum(1 for l in ETAT["lignes"] if l.get("flex") and l["flex"] != "déjà à jour" and "non touché" not in l["flex"])
        f_conflits = sum(1 for l in ETAT["lignes"] if "conflit" in (l.get("flex") or ""))
        ETAT.update(en_cours=False, phase="", fini=time.strftime("%H:%M"),
                    resume=f"{len(ETAT['lignes'])} sociétés lues, {n} adresses {'reportées' if confirmer else 'à reporter'}, "
                           f"{r} {'remplacées' if confirmer else 'à remplacer'} par l'adresse normalisée, "
                           f"{f} fiches flex {'mises à jour' if confirmer else 'à mettre à jour'} (dont {f_conflits} opérateurs vidés pour conflit)")


def _rapport() -> dict:
    lignes = ETAT["lignes"]
    compte = {}
    for l in lignes:
        k = l["action"].split(" :")[0]
        compte[k] = compte.get(k, 0) + 1
    return {**{k: v for k, v in ETAT.items() if k != "lignes"}, "compte": compte, "lignes": lignes}


@router.get("/v3/validation/api/adresses-terrain")
async def adresses_terrain(req: Request, lancement: str = "", confirmer: int = 0, etat: int = 0, par: str = ""):
    if (x := VALID._refus(req, "admin")):
        return x
    if etat or ETAT["en_cours"]:
        return _rapport()
    lancements = [l.strip() for l in lancement.split(",") if l.strip()]
    if not lancements:
        return JSONResponse({"erreur": "indiquez ?lancement=… (un ou plusieurs morceaux de nom de lancement, séparés par des virgules)"}, 400)
    if confirmer and ETAT.get("mode") != "essai à blanc":
        return JSONResponse({"erreur": "faites d'abord l'essai à blanc (sans &confirmer=1)"}, 400)
    if confirmer and (ETAT.get("lancements"), ETAT.get("par")) != (lancements, par):
        return JSONResponse({"erreur": "l'essai à blanc portait sur d'autres lancements ou un autre négo : refaites-le"}, 400)
    ETAT.update(lancements=lancements, par=par)
    asyncio.create_task(_tourner(lancements, bool(confirmer), par))
    await asyncio.sleep(0.2)
    return {"lance": True, "mode": "réel" if confirmer else "essai à blanc",
            "suivi": "rechargez la page avec &etat=1 pour voir l'avancement et le rapport"}
