"""Adresse physique (prospection terrain) reportée dans la mémoire après coup.

Les négos lancent parfois l'enrichisseur sur des sociétés vues sur le terrain sans mettre l'adresse dans le fichier.
Cette route va la chercher dans la base Notion « 🚶🏻‍♂️ Prospection terrain » et la reporte dans le JSON de la mémoire
(champ adresse_notion = « Prospection physique » dans l'écran de validation), puis recalcule l'adresse proposée.

  /v3/validation/api/adresses-terrain?lancement=Prosp terrain,Office national   → essai à blanc (rien n'est écrit)
  … &confirmer=1                                                                  → écriture
  … &etat=1                                                                       → avancement / rapport
La base terrain doit être partagée avec l'intégration Notion de l'outil (comme la mémoire).
"""
import asyncio, os, re, time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import v3_linkedin as LI
import v3_memoire as MEM
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
                        "date": _v(p, "Date de visite")[:10]})
        if not j.get("has_more"):
            return out
        curseur = j.get("next_cursor")


def adresse_complete(t: dict) -> str:
    a = re.sub(r"\s+", " ", t.get("adresse") or "").strip(" ,")
    cp = (t.get("cp") or "").strip()
    if not a:
        return ""
    if cp and not re.search(r"\b\d{5}\b", a):
        a += f" {cp}"
    m = re.search(r"\b(75|92|93|94)\d{3}\b", a)
    if m and m.group(1) == "75" and "paris" not in a.lower():
        a += " Paris"
    return a


def apparier(nom: str, id_pd: str, terrain: list, ressemblance) -> tuple:
    """(fiche terrain, comment) ; (None, raison) si rien ou plusieurs adresses différentes."""
    if id_pd:
        par_id = [t for t in terrain if t["id"] == id_pd and t["adresse"]]
        if par_id:
            return max(par_id, key=lambda t: t["date"]), "ID Pipedrive"
    proches = [t for t in terrain if t["adresse"] and max(ressemblance(nom, t["nom"]), ressemblance(nom, t["nom_pd"] or "")) >= 0.92]
    if not proches:
        return None, "absente de Prospection terrain"
    adresses = {LI.cle_adresse(adresse_complete(t)) or adresse_complete(t).lower() for t in proches}
    if len(adresses) > 1:
        return None, "plusieurs adresses terrain pour ce nom : " + " | ".join(sorted({adresse_complete(t) for t in proches}))[:200]
    return max(proches, key=lambda t: t["date"]), "nom"


async def _tourner(lancements: list, confirmer: bool):
    from v3 import ressemblance                        # import tardif (v3 importe ce module)
    ETAT.update(en_cours=True, mode="réel" if confirmer else "essai à blanc", phase="Lecture de Prospection terrain…",
                fait=0, total=0, lignes=[], fini="", erreur="")
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            terrain = await lire_terrain(c)
            ETAT["phase"] = "Lecture de la mémoire…"
            pages = [s for s in await VALID._toutes() if any(l.lower() in (s.get("lancement") or "").lower() for l in lancements)]
            ETAT["total"] = len(pages)
            for s in pages:
                ETAT["phase"] = f"{s['nom']}…"
                data = await MEM.lire(s["page"])
                ligne = {"societe": s["nom"], "lancement": s.get("lancement", "")[:80], "adresse_terrain": "", "trouvee_par": "",
                         "avant": (data or {}).get("adresse_notion", ""), "action": ""}
                if not data:
                    ligne["action"] = "page illisible"
                else:
                    t, comment = apparier(data.get("nom") or s["nom"], re.sub(r"\D", "", str(data.get("id") or "")), terrain, ressemblance)
                    if not t:
                        ligne["action"] = comment
                    else:
                        a = adresse_complete(t)
                        ligne.update(adresse_terrain=a, trouvee_par=f"{comment} ({t['nego']}, visite du {t['date'] or '?'})")
                        if LI.meme_adresse(a, data.get("adresse_notion") or "") or a == data.get("adresse_notion"):
                            ligne["action"] = "déjà renseignée"
                        else:
                            ligne["action"] = "à reporter" if not confirmer else "reportée"
                            if confirmer:
                                data["adresse_notion"] = a
                                data.update(LI.decider_adresse(data))
                                from v3_rattrapage import _reecrire
                                await _reecrire(c, s["page"], data)
                ETAT["lignes"].append(ligne)
                ETAT["fait"] += 1
    except Exception as e:
        ETAT["erreur"] = str(e)
    finally:
        n = sum(1 for l in ETAT["lignes"] if l["action"] in ("à reporter", "reportée"))
        ETAT.update(en_cours=False, phase="", fini=time.strftime("%H:%M"),
                    resume=f"{len(ETAT['lignes'])} sociétés lues, {n} adresses {'reportées' if confirmer else 'à reporter'}")


def _rapport() -> dict:
    lignes = ETAT["lignes"]
    compte = {}
    for l in lignes:
        k = l["action"].split(" :")[0]
        compte[k] = compte.get(k, 0) + 1
    return {**{k: v for k, v in ETAT.items() if k != "lignes"}, "compte": compte, "lignes": lignes}


@router.get("/v3/validation/api/adresses-terrain")
async def adresses_terrain(req: Request, lancement: str = "", confirmer: int = 0, etat: int = 0):
    if (x := VALID._refus(req, "admin")):
        return x
    if etat or ETAT["en_cours"]:
        return _rapport()
    lancements = [l.strip() for l in lancement.split(",") if l.strip()]
    if not lancements:
        return JSONResponse({"erreur": "indiquez ?lancement=… (un ou plusieurs morceaux de nom de lancement, séparés par des virgules)"}, 400)
    if confirmer and ETAT.get("mode") != "essai à blanc":
        return JSONResponse({"erreur": "faites d'abord l'essai à blanc (sans &confirmer=1)"}, 400)
    asyncio.create_task(_tourner(lancements, bool(confirmer)))
    await asyncio.sleep(0.2)
    return {"lance": True, "mode": "réel" if confirmer else "essai à blanc",
            "suivi": "rechargez la page avec &etat=1 pour voir l'avancement et le rapport"}
