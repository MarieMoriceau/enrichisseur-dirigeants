"""
Enrichisseur v3 — lecture des profils LinkedIn restés en attente, pour un fichier lancé

Les contacts du CRM absents des dirigeants Sales Navigator sont « à vérifier » ; leur profil LinkedIn tranche ensuite
entre « probablement parti » et « toujours là ». Un lancement ne lit que PROFILS_MAX profils (80) : au-delà, et quand le
Profile Scraper était absent ou en échec, les contacts restent « à vérifier » sans que le profil ait été lu.

Ici, par fichier (écran de validation, à côté de la liste de phoning) :
  1. aperçu : combien de profils restent à lire (rien n'est lancé, aucun quota consommé) ;
  2. lecture : PROFILS_MAX profils au plus par passage, sur le compte LinkedIn « automatisations », dans la file
     d'attente du compte et dans son quota du jour ; statuts et détails réécrits dans la mémoire Notion.
Pas de Pipedrive, pas de Sales Navigator, aucun crédit Kaspr / Fullenrich. Le lancement n'est possible qu'après
un aperçu du même fichier de moins d'une heure.
"""
import time, uuid
from datetime import datetime

import httpx

import v3_linkedin as LI
import v3_planning as PLAN
import v3_suivi as SUIVI
import v3_validation as VALID
from v3_rattrapage import corrections_li, _du_lancement, _lire_page, _reecrire

ETAT = {"actif": False, "lancement": "", "phase": "", "journal": [], "apercu": None, "_apercu_le": 0.0}


def _log(m: str):
    ETAT["journal"] = (ETAT["journal"] + [f"{datetime.now(SUIVI.PARIS):%H:%M:%S} {m}"])[-40:]
    print(f"[V3 PROFILS] {m}")


def a_lire(data: dict, corrections: dict) -> list:
    """Contacts CRM de la société dont le profil reste à lire ; l'URL saisie à la main dans l'écran compte aussi."""
    out = []
    for p in data.get("contacts_crm", []):
        if p.get("statut") == "à vérifier" and not p.get("linkedin") and corrections.get(str(p.get("id"))):
            p["linkedin"] = corrections[str(p["id"])]
        if LI.profil_a_lire(p):
            out.append(p)
    return out


async def _societes(lancement: str) -> list:
    """[(fiche mémoire, données, contacts à lire)] pour les sociétés du fichier (écartées comprises : on ne trie pas)."""
    import asyncio
    socs = [s for s in await VALID._toutes() if _du_lancement(s, [lancement.lower()])
            and s.get("perimetre") in ("dirigeants", "tout")]
    sem, out = asyncio.Semaphore(3), []
    async def voir(s):
        async with sem:
            data = await _lire_page(s)
        if data:
            l = a_lire(data, corrections_li(s.get("decisions", "")))
            if l:
                out.append((s, data, l))
    await asyncio.gather(*(voir(s) for s in socs))
    return sorted(out, key=lambda x: x[0].get("nom", ""))


async def _reste_quota() -> int:
    return max(0, SUIVI.QUOTA_JOUR - await SUIVI.utilise_compte(PLAN.maintenant(), PLAN.compte(PLAN.OUTILS)))


async def apercu(lancement: str) -> dict:
    """Essai à blanc : rien n'est lancé."""
    socs = await _societes(lancement)
    n = sum(len(l) for _, _, l in socs)
    reste = await _reste_quota()
    lot = min(n, LI.PROFILS_MAX, reste)
    res = {"lancement": lancement, "societes": len(socs), "profils": n, "ce_passage": lot, "quota_restant": reste,
           "exemples": [f"{p.get('nom_complet', '')} ({s.get('nom', '')})" for s, _, l in socs for p in l][:8]}
    ETAT.update(apercu=res, lancement=lancement.lower(), _apercu_le=time.time())
    return res


async def lancer(lancement: str, entrer_file, sortir_file, jobs: dict) -> dict:
    import asyncio
    if ETAT["actif"]:
        return {"lance": False, "motif": "une lecture de profils est déjà en cours"}
    if not (ETAT["lancement"] == lancement.lower() and time.time() - ETAT["_apercu_le"] < 3600):
        return {"lance": False, "motif": "faites d'abord l'aperçu de ce fichier (il y a moins d'une heure)"}
    if not LI.pb_configure() or not LI.phantoms_du_job({"origine": PLAN.OUTILS})[2]:
        return {"lance": False, "motif": "Profile Scraper du compte automatisations absent de Render (PB_PROFILE_SCRAPER_ID)"}
    reste = await _reste_quota()
    if not reste:
        return {"lance": False, "motif": "quota LinkedIn du jour du compte automatisations atteint : relancez demain"}
    socs = await _societes(lancement)
    lot, pris = [], []
    for s, data, l in socs:                                   # sociétés entières, jusqu'au plafond du passage
        if lot and len(lot) + len(l) > min(LI.PROFILS_MAX, reste):
            break
        pris.append((s, data))
        lot += [(data, p) for p in l]
    if not lot:
        return {"lance": False, "motif": "plus aucun profil à lire dans ce fichier"}
    ETAT.update(actif=True, phase="Démarrage…", journal=[])
    n_total = sum(len(l) for _, _, l in socs)
    asyncio.create_task(_travailler(lancement, pris, lot, n_total, entrer_file, sortir_file, jobs))
    return {"lance": True, "profils": len(lot), "motif": f"{len(lot)} profils en lecture"
            + (f", {n_total - len(lot)} restent pour un prochain passage" if n_total > len(lot) else "")}


async def _travailler(lancement: str, pris: list, lot: list, n_total: int, entrer_file, sortir_file, jobs: dict):
    from v3 import ressemblance                               # import tardif (v3 importe ce module)
    jid = "profils" + uuid.uuid4().hex[:6]
    job = {"id": jid, "nom": f"Profils : {lancement} ({len(lot)})", "origine": PLAN.OUTILS, "stop": False, "journal": [],
           "perimetre": "tout", "total": len(lot), "mails": [], "phase": ""}
    jobs[jid] = {**job, "etat": "en cours", "linkedin": True, "resultats": [], "fait": 0, "debut": time.time(), "fin": None}
    job = jobs[jid]
    notion_id = ""
    try:
        notion_id = await SUIVI.notion_creer(job)
        await SUIVI.maj(notion_id, {"Nb LinkedIn": len(lot)})   # compté sur le quota du compte automatisations
        _log(f"« {lancement} » : {len(lot)} profils à lire sur {n_total}")
        ETAT["phase"] = "En attente du compte LinkedIn…"
        if not await entrer_file(job):
            _log("arrêté avant le départ")
            return
        _, _, profils = LI.phantoms_du_job(job)
        pb = LI.PhantomBuster(_log)
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            ETAT["phase"] = f"LinkedIn : lecture de {len(lot)} profils…"
            lignes = await pb.executer(c, profils, f"v3_{jid}_profils", lambda: job["stop"],
                                       valeurs=[p["linkedin"] for _, p in lot], colonne="profileUrl")
            partis = LI.classer_profils(lot, lignes, ressemblance)
            ETAT["phase"] = "Enregistrement dans la mémoire Notion…"
            ok = 0
            for s, data in pris:
                data.pop("_corrections_li", None)
                try:
                    await _reecrire(c, s["page"], data)
                    ok += 1
                except Exception as e:
                    _log(f"{s.get('nom')} : {e}")
        _log(f"terminé : {partis} probablement partis sur {len(lot)} profils, {ok} sociétés mises à jour"
             + (f" ; {n_total - len(lot)} profils restent à lire" if n_total > len(lot) else ""))
        job["etat"] = "terminé"
    except Exception as e:
        job["etat"] = "erreur"
        _log(f"erreur : {type(e).__name__} : {e}")
    finally:
        sortir_file(job)
        job["fin"] = time.time()
        job["envoi"] = "fait"
        if notion_id:
            await SUIVI.maj(notion_id, {"Statut": "TERMINE" if job["etat"] == "terminé" else "ERREUR"})
        ETAT.update(actif=False, phase="", _apercu_le=0.0)
