"""SIRENE : date d'installation et opérateur de flex. Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_sirene as S


def test_operateur_cite():
    assert S.operateur_cite("IMMEUBLE WEWORK 20 RUE LA FAYETTE 75009 PARIS") == "WeWork"
    assert S.operateur_cite("C/O MORNING 10 RUE X 75002 PARIS") == "Morning"
    assert S.operateur_cite("12 RUE DU MORNING STAR 75002") == "Morning"   # faux positif possible : signalé dans l'Excel
    assert S.operateur_cite("4 RUE JULES LEFEBVRE 75009 PARIS") == ""


def test_analyser(monkeypatch):
    S._CENTRES.clear()
    async def chercher(api, p):
        if p["q"] in ("AVA", "322869637"):
            return [{"siren": "322869637", "siege": {"siret": "1", "adresse": "25 RUE DE MAUBEUGE 75009 PARIS", "date_creation": "2007-01-27",
                                                     "est_siege": True, "etat_administratif": "A"},
                     "matching_etablissements": [{"siret": "2", "adresse": "IMMEUBLE WEWORK 20 RUE LA FAYETTE 75009 PARIS",
                                                  "date_creation": "2026-03-01", "est_siege": False, "etat_administratif": "A"}]}]
        if p["q"] == "Morning":
            return [{"siren": "9", "nom_complet": "MORNING", "matching_etablissements": [
                {"adresse": "7 RUE DE MADRID 75008 PARIS", "etat_administratif": "A", "liste_enseignes": ["MORNING"]}]}]
        return []
    monkeypatch.setattr(S, "_chercher", chercher)
    r = asyncio.run(S.analyser(None, "322869637", "AVA", "20 rue La Fayette, 75009 Paris"))
    assert r["date"] == "2026-03-01" and r["siege"].startswith("non") and r["operateur"] == "WeWork"
    r = asyncio.run(S.analyser(None, "322869637", "AVA", "25 rue de Maubeuge 75009 Paris"))
    assert r["date"] == "2007-01-27" and r["siege"] == "oui" and r["operateur"] == ""
    r = asyncio.run(S.analyser(None, "111111111", "Autre", "7 rue de Madrid, 75008 Paris"))
    assert r["date"] == "" and r["operateur"] == "Morning" and "centre Morning" in r["indice"]


def test_plages_de_numeros():
    assert S.meme_lieu("2-4 2 RUE JULES LEFEBVRE 75009 PARIS", "4 rue Jules Lefebvre 75009")
    assert S.meme_lieu("26 AU 28 26 RUE DE LONDRES 75009 PARIS", "28 rue de Londres, 75009 Paris")
    assert S.meme_lieu("43-45 RUE DE SAINT-PETERSBOURG 75008 PARIS", "43- 45 Rue de Saint-Pétersbourg, 75008 Paris")
    assert not S.meme_lieu("2-4 2 RUE JULES LEFEBVRE 75009 PARIS", "8 rue Jules Lefebvre 75009")
    assert not S.meme_lieu("4 RUE DE LONDRES 75009 PARIS", "4 rue Jules Lefebvre 75009")


def test_appliquer(monkeypatch):
    import v3_validation as VALID
    ecrit = {}
    async def maj(p, props):
        ecrit.update(props); return {}
    async def col(nom, genre="rich_text"):
        return True
    async def analyse(api, siren, nom, adr):
        return {"date": "2023-09-01", "etab": "x", "siege": "oui", "operateur": "WeWork", "indice": "centre WeWork à la même adresse"}
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", col); monkeypatch.setattr(S, "analyser", analyse)
    r = asyncio.run(S.appliquer(None, "p", {"siren": "1", "nom": "A", "debut_bail": "", "operateur_flex": ""}, "x"))
    assert ecrit["Début de bail"] == {"date": {"start": "2023-09-01"}} and "Flexoffice" not in ecrit
    assert ecrit["Opérateur flex"]["rich_text"][0]["text"]["content"] == "WeWork (probable)"
    assert "01/09/2023" in ecrit["Source début de bail"]["rich_text"][0]["text"]["content"]
    ecrit.clear()
    asyncio.run(S.appliquer(None, "p", {"siren": "1", "nom": "A", "debut_bail": "2020-01-01", "operateur_flex": "Morning"}, "x"))
    assert ecrit == {}                                  # rien d'écrasé
