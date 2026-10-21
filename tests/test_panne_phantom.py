"""Phantom des dirigeants en panne (20/10 : « Sales Navigator account not set up ») et mémoire à zéro dirigeant.
Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest

import v3, v3_linkedin as LI, v3_memoire as MEM


def test_memoire_zero_dirigeant_pas_reprise():
    idx = [{"page": "p2", "nom": "alan", "siren": "443061841", "domaine": "", "perim": "tout", "nb_dir": 0},
           {"page": "p1", "nom": "alan", "siren": "443061841", "domaine": "", "perim": "tout", "nb_dir": 3}]
    o = {"nom": "Alan", "siren_crm": "443061841", "site_crm": ""}
    assert MEM.chercher(idx, o, "tout", v3.norm_nom, v3.domaines)["page"] == "p1"          # l'ancienne, avec dirigeants
    assert MEM.chercher(idx[:1], o, "tout", v3.norm_nom, v3.domaines) == {}                # à refaire
    assert MEM.chercher(idx[:1], o, "societes", v3.norm_nom, v3.domaines)["page"] == "p2"  # adresse seule : reprise OK


class FauxPB:
    """Simule le phantom Sales Navigator : en erreur à chaque lot, ou en état de marche."""
    def __init__(self, panne):
        self.panne, self.appels = panne, 0
    async def executer(self, c, agent, nom, stop, **kw):
        self.appels += 1
        self.dernier_statut = "error" if self.panne else "finished"
        self.derniere_sortie = "Sales Navigator account not set up" if self.panne else ""
        return []


def _job(n):
    return {"id": "j", "stop": False, "perimetre": "tout", "origine": "outils", "resultats": [
        {"nom": f"S{i}", "linkedin_id": str(1000 + i), "linkedin": f"https://www.linkedin.com/company/s{i}",
         "source_linkedin": "site web", "effectif_linkedin": 20} for i in range(n)]}


def _faux_pb(panne, appels):
    class Faux:
        def __init__(self, journal):
            pass
        async def executer(self, c, agent, nom, stop, **kw):
            appels.append(nom)
            sn = "salesnav" in nom
            self.dernier_statut = "error" if (sn and panne) else "finished"
            self.derniere_sortie = "Sales Navigator account not set up" if (sn and panne) else ""
            return []
    return Faux


def _phantoms(monkeypatch):
    for k in ("PB_SCRAPER", "PB_SCRAPER_NEGOS"):
        monkeypatch.setattr(LI, k, "sc", raising=False)
    for k in ("PB_SALESNAV", "PB_SALESNAV_NEGOS"):
        monkeypatch.setattr(LI, k, "sn", raising=False)
    monkeypatch.setattr(LI, "PIPEDRIVE_KEY", "", raising=False)


def test_panne_arrete_le_lancement(monkeypatch):
    _phantoms(monkeypatch)
    appels = []
    monkeypatch.setattr(LI, "PhantomBuster", _faux_pb(True, appels))
    job = _job(60)                                                    # 5 lots de 12 sociétés
    with pytest.raises(LI.PhantomEnPanne) as e:
        asyncio.run(LI.etapes_linkedin(job, v3.ressemblance, v3.domaine))
    assert sum("salesnav" in a for a in appels) == LI.SN_ECHECS_MAX and "not set up" in str(e.value)


def test_lots_vides_sans_panne_continuent(monkeypatch):
    _phantoms(monkeypatch)
    appels = []
    monkeypatch.setattr(LI, "PhantomBuster", _faux_pb(False, appels))
    job = _job(30)
    job["perimetre"] = "dirigeants"                                   # pas d'e-mails : on s'arrête après les dirigeants
    asyncio.run(LI.etapes_linkedin(job, v3.ressemblance, v3.domaine))
    assert sum("salesnav" in a for a in appels) == 3                  # les 3 lots passent


def test_motif_et_erreur():
    job = {"erreur": "le phantom Sales Navigator (dirigeants) échoue à chaque lot : x", "stop": False}
    assert v3.motif_arret(job).startswith("erreur : le phantom Sales Navigator")


def test_relancer_vides(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import v3_validation as VALID
    async def index(j, tout=False):
        return [{"page": "p1", "nom": "cleerly", "lancement": "Prospection hebdo 20/10", "nb_dir": 0, "perim": "tout"},
                {"page": "p2", "nom": "alan", "lancement": "Prospection hebdo 20/10 ; S42", "nb_dir": 3, "perim": "tout"},
                {"page": "p3", "nom": "ecartee", "lancement": "Prospection hebdo 20/10", "nb_dir": 0, "perim": "tout"},
                {"page": "p4", "nom": "autre", "lancement": "Prospection hebdo 20/10 (suite 1/3)", "nb_dir": 0, "perim": "tout"}]
    async def toutes():
        return [{"page": "p3", "validation": "Écartée"}]
    async def lire(p):
        return {"nom": "Cleerly", "siren_crm": "", "site_crm": "cleerly.fr", "adresse_notion": "1 rue X 75002 Paris",
                "infos_source": {"nego": "Richard"}, "dirigeants": [], "mode": "complet"}
    lances = []
    monkeypatch.setattr(MEM, "index", index); monkeypatch.setattr(MEM, "lire", lire)
    monkeypatch.setattr(VALID, "_toutes", toutes); monkeypatch.setattr(VALID, "_refus", lambda r, b: None)
    async def prog(): return []
    monkeypatch.setattr(v3.SUIVI, "orgs_programmees", prog)
    monkeypatch.setattr(v3, "nouveau_job", lambda nom, mails, perim, orgs, **kw: lances.append((nom, orgs, kw)) or "jid")
    monkeypatch.setattr(v3, "jlog", lambda j, m: None); monkeypatch.setitem(v3.JOBS, "jid", {})
    app = FastAPI(); app.include_router(v3.router); c = TestClient(app)
    r = c.get("/v3/validation/api/relancer-vides", params={"lancement": "Prospection hebdo 20/10"}).json()
    assert r["essai_a_blanc"] and r["a_relancer"] == 1 and r["exemples"] == ["cleerly"] and not lances
    r = c.get("/v3/validation/api/relancer-vides", params={"lancement": "Prospection hebdo 20/10", "confirmer": 1}).json()
    nom, orgs, kw = lances[0]
    assert r["relancees"] == 1 and kw["origine"] == "outils"
    assert orgs[0] == {"id": "", "nom": "Cleerly", "site_crm": "cleerly.fr", "siren_crm": "", "adresse_crm": "", "cp": "", "ville": "",
                       "adresse_notion": "1 rue X 75002 Paris", "infos_source": {"nego": "Richard"}}


def test_relance_garde_le_travail_manuel(monkeypatch):
    """Société déjà validée, SIREN corrigé, Base quali saisie : la relance garde tout, avec les nouveaux dirigeants."""
    corr = '{"avant": "947938924", "insee": {"raison_sociale": "CREME DE LA CREME"}}'
    idx = [{"page": "old", "nom": "creme de la creme", "siren": "443061841", "domaine": "cremedelacreme.io", "perim": "tout",
            "lancement": "Prospection hebdo 20/10", "lance_par": "", "negos": "Lionel", "nb_dir": 0, "hist": "",
            "validation": "Société validée", "validee": "2026-10-20T14:24:00.000Z", "siren_corrige": corr,
            "base_quali": "YES", "adresse_retenue": "46 Rue Notre-Dame des Victoires, 75002 Paris", "source_retenue": "Notion"}]
    envoyes = []
    class FauxClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
    async def faux_req(c, methode, url, **kw):
        envoyes.append((methode, url, kw.get("json")))
        class R:
            status_code = 200
            def json(self): return {"id": "new"}
        return R()
    monkeypatch.setattr(MEM, "NOTION_KEY", "x"); monkeypatch.setattr(MEM, "_COLONNE_NEGOS", {"ok": True})
    monkeypatch.setattr(MEM.httpx, "AsyncClient", lambda **k: FauxClient()); monkeypatch.setattr(MEM, "_req", faux_req)
    r = {"nom": "Crème de la crème", "siren_crm": "443061841", "domaine": "cremedelacreme.io", "infos_source": {"nego": "Lionel"},
         "dirigeants": [{"nom_complet": "A B", "linkedin": "x", "email": ""}]}
    job = {"etat": "terminé", "perimetre": "tout", "nom": "Prospection hebdo 20/10 (relance sans dirigeants)", "mails": [],
           "resultats": [r], "_idx_tout": idx}
    asyncio.run(MEM.enregistrer(job, v3.norm_nom, print))
    props = next(j for m, u, j in envoyes if m == "POST")["properties"]
    assert props["Validation"]["select"]["name"] == "Société validée" and props["Validée le"]["date"]["start"].startswith("2026-10-20")
    assert props["SIREN"]["rich_text"][0]["text"]["content"] == "443061841"
    assert "CREME DE LA CREME" in props["SIREN corrigé"]["rich_text"][0]["text"]["content"]
    assert props["Base quali"]["rich_text"][0]["text"]["content"] == "YES"
    assert props["Adresse retenue"]["rich_text"][0]["text"]["content"].startswith("46 Rue")
    assert props["Nb dirigeants"]["number"] == 1
    assert ("PATCH", "https://api.notion.com/v1/pages/old", {"archived": True}) in envoyes      # l'ancienne fiche vide est archivée


def _job_sites(n):
    j = _job(n)
    for r in j["resultats"]:
        r["linkedin_site"] = r["linkedin"]
        r.pop("linkedin_id")
    return j


def _faux_scraper(reponses, appels):
    """Faux PhantomBuster : le Company Scraper renvoie, lot après lot, les lignes de `reponses` (None = trop long)."""
    class Faux:
        def __init__(self, journal):
            pass
        async def executer(self, c, agent, nom, stop, valeurs=(), limite_s=3600, **kw):
            appels.append((nom, len(valeurs), limite_s))
            if "scraper" not in nom:
                self.dernier_statut = "finished"
                return []
            rep = reponses.pop(0) if reponses else []
            self.dernier_statut = "trop long" if rep is None else "finished"
            return [{"query": u, "companyUrl": u, "name": "x", "linkedinID": "42", "employeesOnLinkedIn": "30",
                     "companyAddress": "1 rue X, Paris, 75002, FR"} for u in (valeurs if rep == "tout" else [])]
    return Faux


def test_scraper_par_lots(monkeypatch):
    _phantoms(monkeypatch)
    appels = []
    monkeypatch.setattr(LI, "PhantomBuster", _faux_scraper(["tout", "tout", None, "tout", "tout", "tout", "tout"], appels))
    job = _job_sites(125)                                             # le lancement du 21/10
    job["perimetre"] = "societes"
    asyncio.run(LI.etapes_linkedin(job, v3.ressemblance, v3.domaine))
    lots = [a for a in appels if "scraper" in a[0]]
    assert len(lots) == 7 and all(n <= LI.SCRAPER_LOT for _, n, _ in lots) and all(l >= 1800 for _, _, l in lots)
    lus = sum(1 for r in job["resultats"] if r.get("effectif_linkedin") == 30)
    assert lus == 125 - 20 and job["li_utilise"] == 125             # un lot trop long perdu, pas les 6 autres


def test_scraper_en_panne(monkeypatch):
    _phantoms(monkeypatch)
    appels = []
    monkeypatch.setattr(LI, "PhantomBuster", _faux_scraper([[], []], appels))
    with pytest.raises(LI.PhantomEnPanne) as e:
        asyncio.run(LI.etapes_linkedin(_job_sites(125), v3.ressemblance, v3.domaine))
    assert "Company Scraper" in str(e.value) and sum("scraper" in a[0] for a in appels) == LI.SCRAPER_ECHECS_MAX


def test_trop_long_recupere_le_partiel(monkeypatch):
    pb = LI.PhantomBuster(lambda m: None)
    faits = []
    async def lancer(c, agent, nom, **kw): return "cid"
    async def attendre(c, cid, stop, limite_s=3600, agent=""): faits.append(("attendre", limite_s)); return "trop long"
    async def arreter(c, agent): faits.append(("arreter", agent))
    async def resultats(c, cid, nom=""): faits.append(("resultats", cid)); return [{"a": 1}]
    async def dodo(s): pass
    monkeypatch.setattr(pb, "lancer", lancer); monkeypatch.setattr(pb, "attendre", attendre)
    monkeypatch.setattr(pb, "arreter", arreter); monkeypatch.setattr(pb, "resultats", resultats)
    monkeypatch.setattr(LI.asyncio, "sleep", dodo)
    assert asyncio.run(pb.executer(None, "ag", "n", lambda: False, limite_s=2400)) == [{"a": 1}]
    assert faits == [("attendre", 2400), ("arreter", "ag"), ("resultats", "cid")]


def test_phantom_occupe_on_attend(monkeypatch):
    """21/10 : 429 « maxParallelismReached » (phantom encore occupé) → on attend au lieu d'échouer."""
    class Rep:
        def __init__(self, code, txt=""):
            self.status_code, self.text = code, txt
        def json(self): return {"containerId": "cid"}
    reps = [Rep(429, '{"detailedErrorSlug":"maxParallelismReached"}')] * 3 + [Rep(200)]
    class C:
        async def post(self, *a, **k): return reps.pop(0)
    pb = LI.PhantomBuster(lambda m: None)
    async def arg(c, agent): return {"spreadsheetUrl": ""}
    async def heb(c, col, vals): return "https://x/y.csv"
    async def dodo(s): pass
    monkeypatch.setattr(pb, "_argument", arg); monkeypatch.setattr(pb, "_heberger_csv", heb)
    monkeypatch.setattr(LI.asyncio, "sleep", dodo)
    assert asyncio.run(pb.lancer(C(), "ag", "n", valeurs=["a"], colonne="linkedinUrl")) == "cid"
    reps[:] = [Rep(429, "maxParallelismReached")] * 5
    monkeypatch.setattr(LI, "ATTENTE_PHANTOM_OCCUPE", 0)
    with pytest.raises(RuntimeError) as e:
        asyncio.run(pb.lancer(C(), "ag", "n", valeurs=["a"], colonne="linkedinUrl"))
    assert "occupé" in str(e.value)


def test_texte_alerte(monkeypatch):
    import v3_suivi as SUIVI
    mails = []
    monkeypatch.setattr(SUIVI, "SMTP_USER", "x"); monkeypatch.setattr(SUIVI, "SMTP_PASS", "y")
    monkeypatch.setattr(SUIVI, "COPIE_COMPLETE", ["m@x.fr"])
    monkeypatch.setattr(SUIVI, "_envoyer", lambda dest, sujet, corps, *a: mails.append(corps))
    asyncio.run(SUIVI.alerte_phantom({"nom": "L", "origine": "outils", "phantom_en_panne": "phantom occupé depuis plus de 45 min"}, 5))
    asyncio.run(SUIVI.alerte_phantom({"nom": "L", "origine": "outils", "phantom_en_panne": "Sales Navigator account not set up"}, 0))
    assert "Stop" in mails[0] and "cookie" not in mails[0] and "cookie" in mails[1] and "5 liste" in mails[0]


def test_relancer_vides_ecarte_les_programmees(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import v3_validation as VALID
    async def index(j, tout=False):
        return [{"page": f"p{i}", "nom": n, "siren": s, "lancement": "Prospection hebdo 20/10 (suite 1/3)", "nb_dir": 0, "perim": "tout"}
                for i, (n, s) in enumerate([("kraaft", ""), ("joivy", "443061841"), ("klox", ""), ("lazeo", "")])]
    async def toutes(): return []
    async def prog(): return [{"nom": "Kraaft"}, {"nom": "Autre nom", "siren_crm": "443061841"}]   # même SIREN = même société
    monkeypatch.setattr(MEM, "index", index); monkeypatch.setattr(VALID, "_toutes", toutes)
    monkeypatch.setattr(VALID, "_refus", lambda r, b: None); monkeypatch.setattr(v3.SUIVI, "orgs_programmees", prog)
    for k in ("PB_COMPANY_SCRAPER_ID_NEGOS", "PB_SALESNAV_ID_NEGOS"):     # deux comptes LinkedIn, comme sur Render
        monkeypatch.setenv(k, "1")
    app = FastAPI(); app.include_router(v3.router); c = TestClient(app)
    r = c.get("/v3/validation/api/relancer-vides", params={"lancement": "Prospection hebdo 20/10 (suite 1/3)", "origine": "negos"}).json()
    assert r["a_relancer"] == 2 and r["deja_programmees"] == 2 and sorted(r["exemples"]) == ["klox", "lazeo"]
    assert r["compte_linkedin"] == "négos"
