import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_rattrapage as R, v3_linkedin as LI, v3_validation as VALID


def test_selection():
    d = {"domaine": "a.fr", "dirigeants": [
        {"prenom": "A", "nom": "1", "role": "1 CEO"}, {"prenom": "B", "nom": "2", "role": "2 Fondateur", "dans_pipedrive": "oui"},
        {"prenom": "C", "nom": "3", "role": "3 DAF", "email": "c@a.fr"}, {"prenom": "D", "nom": "4", "role": "5 Associé"},
        {"prenom": "E", "nom": "5", "role": "5 Associé"}, {"prenom": "F", "nom": "6", "role": "6 Immobilier"},
        {"prenom": "G", "nom": "7", "role": "1 CEO", "fullenrich_tente": "2026-10-27"}]}
    assert [x["prenom"] for x in R.contacts_fullenrich(d)] == ["A"]      # 4 premiers rôles 1-5 : A, G, B, C → seul A reste à chercher
    assert R.contacts_fullenrich({"dirigeants": d["dirigeants"]}) == []       # pas de domaine


def test_tache(monkeypatch):
    pages = {"p1": {"id": "1", "nom": "Soc", "domaine": "a.fr", "dirigeants": [{"prenom": "A", "nom": "1", "role": "1 CEO"}]}}
    async def toutes():
        return [{"page": "p1", "nom": "Soc", "validation": "À valider", "lancement": "organizations-1702184-4097 (suite 1/9)"},
                {"page": "p2", "nom": "Autre", "validation": "À valider", "lancement": "autre"}]
    async def lire(s): return pages.get(s["page"])
    ecrits = {}
    async def reecrire(c, page, data): ecrits[page] = data
    async def req(*a, **k): return None
    async def fe(c, contacts, journal): return {"0": "a1@a.fr"}
    monkeypatch.setattr(VALID, "_toutes", toutes); monkeypatch.setattr(R, "_lire_page", lire)
    monkeypatch.setattr(R, "_reecrire", reecrire); monkeypatch.setattr(R.MEM, "_req", req)
    monkeypatch.setattr(LI, "fullenrich_emails", fe); monkeypatch.setattr(LI, "FULLENRICH_KEY", "x")
    asyncio.run(R._tache_fullenrich(["4097 (suite 1/9)"], False))
    assert R.FULLENRICH["contacts"] == 1 and not ecrits
    asyncio.run(R._tache_fullenrich(["4097 (suite 1/9)"], True))
    d = ecrits["p1"]["dirigeants"][0]
    assert d["email"] == "a1@a.fr" and d["source_email"] == "Fullenrich" and d["fullenrich_tente"]


def test_excel_relu(monkeypatch):
    async def toutes():
        return [{"page": "p1", "nom": "Soc", "validation": "À valider", "lancement": "locataires_flex_paris (suite 1/2)"}]
    async def lire(s):
        return {"id": "1", "domaine": "a.fr", "dirigeants": [
            {"prenom": "A", "nom": "1", "role": "1 CEO", "fullenrich_tente": "2026-10-27", "email": "a@a.fr", "source_email": "Fullenrich"},
            {"prenom": "B", "nom": "2", "role": "5 Associé", "fullenrich_tente": "2026-10-27"},
            {"prenom": "C", "nom": "3", "role": "1 CEO", "email": "c@a.fr", "source_email": "Kaspr"}]}
    monkeypatch.setattr(VALID, "_toutes", toutes); monkeypatch.setattr(R, "_lire_page", lire)
    l = asyncio.run(R._lignes_depuis_notion(["locataires_flex_paris"]))
    assert [(x[3], x[8]) for x in l] == [("A", "a@a.fr"), ("B", "")]
