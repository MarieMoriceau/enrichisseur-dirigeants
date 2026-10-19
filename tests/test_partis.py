"""Contacts partis : sélection, recherche (phantoms simulés), décision admin, export."""
import asyncio, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_partis as P

DEC = [{"type": "crm", "action": "marquer parti", "id": 66896, "nom_complet": "Jean Dupont", "linkedin": "https://www.linkedin.com/in/jdupont/",
        "email": "jean@ancienne.fr", "fonction": "DAF"},
       {"type": "crm", "action": "garder", "id": 1, "linkedin": "https://www.linkedin.com/in/x/"},
       {"type": "crm", "action": "marquer parti", "id": 2, "nom_complet": "Sans Profil", "linkedin": ""}]
SOC = {"page": "p1", "nom": "Ancienne SA", "decisions": json.dumps(DEC), "partis": ""}


def test_selection_et_alertes():
    assert [d["id"] for _, d in P.a_rechercher([SOC])] == [66896]
    deja = {**SOC, "partis": json.dumps({"66896": {"etat": "à valider"}})}
    assert P.a_rechercher([deja]) == []
    assert P.alertes({"effectif": 4, "adresse": "1 rue X, 69002 Lyon"}) == ["effectif 4 (moins de 10)", "hors Île-de-France (69002)"]
    assert P.alertes({"effectif": 40, "adresse": "1 rue X, 92100 Boulogne"}) == []


def test_recherche(monkeypatch):
    enregistre = {}
    async def toutes():
        return [SOC]
    async def quota(d, c):
        return 0
    class FauxPB:
        def __init__(self, log): pass
        async def executer(self, c, agent, nom, stop, valeurs=None, colonne=""):
            if "profils" in nom:
                return [{"query": valeurs[0], "companyName": "Nouvelle SAS", "linkedinCompanyId": "123",
                         "linkedinJobTitle": "Directeur financier", "linkedinJobDateRange": "janv. 2026 - Present", "location": "Paris"}]
            return [{"linkedinID": "123", "website": "https://www.nouvelle.fr/", "companyAddress": "5 rue Y, 75008 Paris",
                     "employeesOnLinkedIn": "42"}]
    async def kaspr(c, nom, url):
        return "jean.dupont@nouvelle.fr"
    async def faux_enregistrer(page, maj):
        enregistre.setdefault(page, {}).update(maj)
    async def rien(*a, **k):
        return "notion1"
    async def entrer(job):
        return True
    monkeypatch.setattr(P.VALID, "_toutes", toutes)
    monkeypatch.setattr(P.SUIVI, "utilise_compte", quota)
    monkeypatch.setattr(P.SUIVI, "notion_creer", rien)
    monkeypatch.setattr(P.SUIVI, "maj", rien)
    monkeypatch.setattr(P.LI, "PhantomBuster", FauxPB)
    monkeypatch.setattr(P.LI, "kaspr_email", kaspr)
    monkeypatch.setattr(P.LI, "pb_configure", lambda: True)
    monkeypatch.setattr(P.LI, "phantoms_du_job", lambda job: ("scraper", "salesnav", "profils"))
    monkeypatch.setattr(P, "enregistrer", faux_enregistrer)

    async def scenario():
        jobs = {}
        r = await P.lancer(entrer, lambda job: None, jobs)
        assert r["lance"] and r["contacts"] == 1
        while P.ETAT["actif"]:
            await asyncio.sleep(0.01)
    asyncio.run(scenario())
    x = enregistre["p1"]["66896"]
    assert x["etat"] == "à valider" and x["nouvelle_societe"] == "Nouvelle SAS" and x["fonction"] == "Directeur financier"
    assert x["role"] == "4 CFO / DAF" and x["domaine"] == "nouvelle.fr" and x["effectif"] == 42
    assert x["email"] == "jean.dupont@nouvelle.fr" and x["ancien_email"] == "jean@ancienne.fr"
