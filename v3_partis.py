"""
Enrichisseur v3 — suivi des contacts « partis »

Quand l'admin marque un contact du CRM « parti » (écran de validation, contacts déjà au CRM), on peut le retrouver :
  1. LinkedIn Profile Scraper sur son profil : sa société actuelle et son nouveau poste ;
  2. Company Scraper sur la page LinkedIn de la nouvelle société : site, adresse, effectif ;
  3. Kaspr : son e-mail professionnel actuel.
Tout passe par le compte LinkedIn « automatisations » (jamais celui des négos), dans la limite du quota du jour,
et dans la file d'attente de ce compte. Le résultat est rangé dans la page mémoire de l'ANCIENNE société,
colonne Notion « Partis suivis » (JSON par identifiant Pipedrive), état « à valider ». L'admin valide ou écarte ;
les validés partent dans l'export HubSpot (onglet « Contacts partis ») pour mettre à jour la fiche existante.
"""
import json, re, time, uuid
from datetime import datetime, timezone

import httpx

import v3_linkedin as LI
import v3_memoire as MEM
import v3_planning as PLAN
import v3_suivi as SUIVI
import v3_validation as VALID

ETATS = ("à valider", "validé", "écarté", "introuvable")
IDF = ("75", "77", "78", "91", "92", "93", "94", "95")
SEUIL_EFFECTIF = 10
ETAT = {"actif": False, "debut": "", "phase": "", "journal": [], "total": 0, "fait": 0, "stop": False}
_COLONNE = {"ok": False}


def _log(m: str):
    ETAT["journal"] = (ETAT["journal"] + [f"{datetime.now(SUIVI.PARIS):%H:%M:%S} {m}"])[-40:]
    print(f"[V3 PARTIS] {m}")


async def assurer_colonne():
    if _COLONNE["ok"]:
        return
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "PATCH", f"https://api.notion.com/v1/databases/{MEM.MEMOIRE_DB}",
                           json={"properties": {"Partis suivis": {"rich_text": {}}}})
    _COLONNE["ok"] = r.status_code == 200
    if r.status_code != 200:
        print(f"[V3 PARTIS] colonne : Notion {r.status_code} {r.text[:200]}")


def suivis_de(s: dict) -> dict:
    return VALID._json(s.get("partis"), {})


def a_rechercher(socs: list) -> list:
    """[(société, décision)] : contacts marqués « parti », avec un profil LinkedIn, pas encore recherchés."""
    out = []
    for s in socs:
        deja = suivis_de(s)
        for d in VALID._json(s.get("decisions"), []):
            if (d.get("type") or "crm") == "crm" and d.get("action") == "marquer parti" and d.get("linkedin") \
                    and str(d.get("id")) not in deja:
                out.append((s, d))
    return out


def alertes(x: dict) -> list:
    a = []
    try:
        if x.get("effectif") not in (None, "") and int(x["effectif"]) < SEUIL_EFFECTIF:
            a.append(f"effectif {x['effectif']} (moins de {SEUIL_EFFECTIF})")
    except (TypeError, ValueError):
        pass
    cp = (re.search(r"\b(\d{5})\b", x.get("adresse") or "") or [None, ""])[1]
    if cp and cp[:2] not in IDF:
        a.append(f"hors Île-de-France ({cp})")
    return a


def liste(socs: list) -> list:
    """Tous les partis déjà recherchés, à plat, pour l'écran admin et l'export."""
    out = []
    for s in socs:
        for pid, x in suivis_de(s).items():
            out.append({**x, "id": pid, "page": s["page"], "ancienne_societe": s["nom"], "alertes": alertes(x)})
    ordre = {"à valider": 0, "introuvable": 1, "validé": 2, "écarté": 3}
    return sorted(out, key=lambda x: (ordre.get(x.get("etat"), 4), x.get("ancienne_societe", "")))


def _domaine(url: str) -> str:
    d = re.sub(r"^https?://", "", (url or "").strip().lower()).split("/")[0]
    return d[4:] if d.startswith("www.") else d


def _cle(u: str) -> str:
    return re.sub(r"[?#].*$", "", (u or "").lower()).rstrip("/").split("linkedin.com", 1)[-1]


async def enregistrer(page: str, maj: dict) -> dict:
    """Fusionne {id: fiche} dans la colonne « Partis suivis » de la page (relue juste avant d'écrire)."""
    await assurer_colonne()
    s = await VALID._une(page)
    suivis = suivis_de(s) if s else {}
    for pid, x in maj.items():
        suivis[pid] = {**suivis.get(pid, {}), **x}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "PATCH", f"https://api.notion.com/v1/pages/{page}",
                           json={"properties": {"Partis suivis": MEM._txts(json.dumps(suivis, ensure_ascii=False))}})
    if r.status_code != 200:
        raise RuntimeError(f"Notion a refusé l'enregistrement ({r.status_code})")
    return suivis


async def lancer(entrer_file, sortir_file, jobs: dict) -> dict:
    """Démarre la recherche (une seule à la fois). Renvoie tout de suite ; le travail continue en tâche de fond."""
    import asyncio
    if ETAT["actif"]:
        return {"lance": False, "motif": "une recherche des partis est déjà en cours"}
    if not LI.pb_configure() or not LI.phantoms_du_job({"origine": PLAN.OUTILS})[2]:
        return {"lance": False, "motif": "Profile Scraper du compte automatisations absent de Render (PB_PROFILE_SCRAPER_ID)"}
    socs = await VALID._toutes()
    cand = a_rechercher(socs)
    if not cand:
        return {"lance": False, "motif": "aucun contact « parti » à rechercher"}
    c_compte = PLAN.compte(PLAN.OUTILS)
    reste = max(0, SUIVI.QUOTA_JOUR - await SUIVI.utilise_compte(PLAN.maintenant(), c_compte))
    if not reste:
        return {"lance": False, "motif": "quota LinkedIn du jour du compte automatisations atteint : relancez demain"}
    lot = cand[:reste]
    ETAT.update(actif=True, debut=datetime.now(SUIVI.PARIS).isoformat(timespec="minutes"), phase="Démarrage…",
                journal=[], total=len(lot), fait=0, stop=False)
    asyncio.create_task(_travailler(lot, len(cand), entrer_file, sortir_file, jobs))
    return {"lance": True, "contacts": len(lot), "en_attente": len(cand) - len(lot),
            "motif": f"{len(lot)} contacts recherchés" + (f", {len(cand) - len(lot)} restent pour demain (quota)" if len(cand) > len(lot) else "")}


async def _travailler(lot: list, n_total: int, entrer_file, sortir_file, jobs: dict):
    jid = "partis" + uuid.uuid4().hex[:6]
    job = {"id": jid, "nom": f"Partis : {len(lot)} contacts", "origine": PLAN.OUTILS, "stop": False, "journal": [],
           "perimetre": "tout", "total": len(lot), "mails": [], "phase": ""}
    jobs[jid] = {**job, "etat": "en cours", "linkedin": True, "resultats": [], "fait": 0, "debut": time.time(), "fin": None}
    job = jobs[jid]
    notion_id = ""
    try:
        notion_id = await SUIVI.notion_creer(job)
        await SUIVI.maj(notion_id, {"Nb LinkedIn": len(lot)})          # compté sur le quota du compte automatisations
        _log(f"{len(lot)} contacts partis à rechercher (compte LinkedIn « {PLAN.NOM_COMPTE[PLAN.compte(PLAN.OUTILS)]} »)")
        ETAT["phase"] = "En attente du compte LinkedIn…"
        if not await entrer_file(job):
            _log("arrêté avant le départ")
            return
        scraper, _, profils = LI.phantoms_du_job(job)
        pb = LI.PhantomBuster(_log)
        stop = lambda: ETAT["stop"]
        trouves = {}                                          # (page, id) -> fiche
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            # 1. profils : société et poste actuels
            ETAT["phase"] = f"LinkedIn : lecture de {len(lot)} profils…"
            lignes = await pb.executer(c, profils, f"v3_{jid}_profils", stop,
                                       valeurs=[d["linkedin"] for _, d in lot], colonne="profileUrl")
            par_url = {}
            for l in lignes:
                for k in ("query", "profileUrl", "linkedinProfileUrl", "linkedInProfileUrl"):
                    if l.get(k):
                        par_url.setdefault(_cle(l[k]), l)
            for s, d in lot:
                l = par_url.get(_cle(d["linkedin"]))
                base = {"nom_complet": d.get("nom_complet", ""), "linkedin": d["linkedin"], "ancien_email": d.get("email", ""),
                        "ancienne_fonction": d.get("fonction", ""), "le": datetime.now(timezone.utc).isoformat(timespec="seconds")}
                if not l:
                    trouves[(s["page"], str(d["id"]))] = {**base, "etat": "introuvable", "detail": "profil non lu par le phantom"}
                    continue
                nom_ent, cid, poste, en_cours, periode = LI._entreprise_actuelle(l)
                if not (nom_ent and en_cours):
                    trouves[(s["page"], str(d["id"]))] = {**base, "etat": "introuvable",
                                                          "detail": f"aucun poste en cours (dernier : {nom_ent or '—'} {periode})".strip()}
                    continue
                trouves[(s["page"], str(d["id"]))] = {**base, "etat": "à valider", "nouvelle_societe": nom_ent,
                    "linkedin_societe": f"https://www.linkedin.com/company/{cid}/" if cid else "", "linkedin_id": cid,
                    "fonction": poste, "role": LI.role(poste) or "8 Autre", "lieu": l.get("location", ""), "periode": periode}
            ETAT["fait"] = len(lot)
            # 2. nouvelles sociétés : site, adresse, effectif
            urls = sorted({x["linkedin_societe"] for x in trouves.values() if x.get("linkedin_societe")})
            if urls and scraper and not stop():
                ETAT["phase"] = f"LinkedIn : {len(urls)} nouvelles sociétés…"
                try:
                    ls = await pb.executer(c, scraper, f"v3_{jid}_societes", stop, valeurs=urls, colonne="linkedinUrl")
                except Exception as e:
                    _log(f"Company Scraper en échec : {e}")
                    ls = []
                par_id = {}
                for l in ls:
                    i = re.sub(r"\D", "", str(l.get("linkedinID") or l.get("mainCompanyID") or ""))
                    if i:
                        par_id[i] = l
                for x in trouves.values():
                    l = par_id.get(x.get("linkedin_id") or "")
                    if not l:
                        continue
                    x["domaine"] = _domaine(l.get("website") or "")
                    x["adresse"] = l.get("companyAddress", "")
                    try:
                        x["effectif"] = int(float(l.get("employeesOnLinkedIn") or ""))
                    except ValueError:
                        x["effectif"] = ""
            # 3. e-mail actuel (Kaspr, à partir du profil LinkedIn)
            a_mailer = [x for x in trouves.values() if x["etat"] == "à valider"]
            for i, x in enumerate(a_mailer, 1):
                if stop():
                    break
                ETAT["phase"] = f"Kaspr : e-mails {i}/{len(a_mailer)}…"
                x["email"] = await LI.kaspr_email(c, x["nom_complet"], x["linkedin"])
                x["source_email"] = "Kaspr" if x["email"] else ""
        # 4. rangement dans les pages mémoire des anciennes sociétés
        ETAT["phase"] = "Enregistrement…"
        par_page = {}
        for (page, pid), x in trouves.items():
            par_page.setdefault(page, {})[pid] = x
        for page, maj in par_page.items():
            await enregistrer(page, maj)
        ok = sum(1 for x in trouves.values() if x["etat"] == "à valider")
        _log(f"terminé : {ok} retrouvés dans une nouvelle société, {len(trouves) - ok} introuvables"
             + (f" ; {n_total - len(lot)} restent pour demain (quota)" if n_total > len(lot) else ""))
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
        ETAT.update(actif=False, phase="")
