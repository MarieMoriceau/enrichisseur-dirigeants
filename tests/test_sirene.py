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


def test_plage_meme_cote():
    assert S.meme_lieu("17-21 17 RUE SAINT-FIACRE 75002 PARIS", "19 rue Saint-Fiacre, 75002 Paris")
    assert not S.meme_lieu("17-21 17 RUE SAINT-FIACRE 75002 PARIS", "20 Rue Saint-Fiacre, 75002 Paris")
    assert not S.meme_lieu("17-21 17 RUE SAINT-FIACRE 75002 PARIS", "18 Rue Saint-Fiacre, 75002 Paris")


def test_siege_domicilie_chez_operateur(monkeypatch):
    """H Company : siège au 12 rue Sainte-Cécile « Domiciliation : PATCHWORK », bureaux au 8 de la même rue."""
    S._CENTRES.clear()
    async def chercher(api, p):
        if p["q"] in ("H Company", "927528471"):
            return [{"siren": "927528471", "siege": {"siret": "92752847100026", "adresse": "12 RUE SAINTE-CECILE 75009 PARIS",
                                                     "date_creation": "2025-11-25", "est_siege": True, "etat_administratif": "A",
                                                     "domiciliation": {"nom": "PATCHWORK"}},
                     "matching_etablissements": [{"siret": "92752847100018", "adresse": "22 RUE DROUOT 75009 PARIS",
                                                  "date_creation": "2024-04-09", "est_siege": False, "etat_administratif": "F"}]}]
        return []
    monkeypatch.setattr(S, "_chercher", chercher)
    r = asyncio.run(S.analyser(None, "927528471", "H Company", "8 rue Sainte-Cécile 75009"))
    assert r["operateur"] == "Patchwork" and r["sur"] and r["date"] == ""      # date vide : pas d'établissement au 8
    r = asyncio.run(S.analyser(None, "927528471", "H Company", "5 rue de Provence 75009"))
    assert r["operateur"] == "Patchwork" and not r["sur"] and r["date"] == ""


def test_texte_etab():
    assert S.operateur_cite(S._texte_etab({"adresse": "12 RUE X 75009", "complement_adresse": "C/O WOJO"})) == "Wojo"
    assert S.operateur_cite(S._texte_etab({"adresse": "12 RUE X 75009", "liste_enseignes": ["KWERK"]})) == "Kwerk"


def test_route_sirene_flex(monkeypatch):
    import v3_validation as VALID
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI(); app.include_router(S.router)
    ecrit = {}
    async def une(p):
        return {"siren": "927528471", "nom": "H Company", "flexoffice": True, "operateur_flex": "", "adresse_retenue": "8 rue Sainte-Cécile 75009"}
    async def analyse(api, siren, nom, adr):
        return {"date": "", "etab": "", "siege": "", "operateur": "Patchwork", "indice": "centre Patchwork à la même adresse", "sur": False}
    async def maj(p, props):
        ecrit.update(props); return {}
    async def col(nom, genre="rich_text"):
        return True
    monkeypatch.setattr(VALID, "_refus", lambda req, b: None); monkeypatch.setattr(VALID, "role", lambda req: "admin")
    monkeypatch.setattr(VALID, "_une", une); monkeypatch.setattr(S, "analyser", analyse)
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", col)
    page = "0" * 32
    j = TestClient(app).get(f"/v3/validation/api/sirene-flex/{page}").json()
    assert j["operateur"] == "Patchwork" and j["enregistre"]
    assert ecrit["Opérateur flex"]["rich_text"][0]["text"]["content"] == "Patchwork"


def test_operateur_retire(monkeypatch):
    import v3_validation as VALID
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI(); app.include_router(S.router)
    async def une(p):
        return {"siren": "927528471", "flexoffice": True, "operateur_flex": "(aucun)", "adresse_retenue": "8 rue Sainte-Cécile 75009"}
    monkeypatch.setattr(VALID, "_refus", lambda req, b: None); monkeypatch.setattr(VALID, "_une", une)
    j = TestClient(app).get("/v3/validation/api/sirene-flex/" + "0" * 32).json()
    assert j["operateur"] == "" and j["retire"]
    # la validation (appliquer) ne remplace pas un opérateur retiré
    ecrit = {}
    async def maj(p, props): ecrit.update(props); return {}
    async def col(nom, genre="rich_text"): return True
    async def analyse(api, siren, nom, adr):
        return {"date": "", "etab": "", "siege": "", "operateur": "Patchwork", "indice": "centre", "sur": False}
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", col); monkeypatch.setattr(S, "analyser", analyse)
    asyncio.run(S.appliquer(None, "p", {"siren": "1", "debut_bail": "x", "operateur_flex": "(aucun)", "flexoffice": True}, "x"))
    assert "Opérateur flex" not in ecrit


def test_route_sirene_flex_date(monkeypatch):
    """AB Tasty : siège au 8 rue Sainte-Cécile créé le 01/01/2026, opérateur déjà enregistré : la date est proposée."""
    import v3_validation as VALID
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI(); app.include_router(S.router)
    async def une(p):
        return {"siren": "518685540", "nom": "AB Tasty", "flexoffice": True, "operateur_flex": "Patchwork", "debut_bail": ""}
    async def analyse(api, siren, nom, adr):
        return {"date": "2026-01-01", "etab": "8 RUE SAINTE-CECILE", "siege": "oui", "operateur": "Patchwork", "indice": "", "sur": False}
    monkeypatch.setattr(VALID, "_refus", lambda req, b: None); monkeypatch.setattr(VALID, "role", lambda req: "admin")
    monkeypatch.setattr(VALID, "_une", une); monkeypatch.setattr(S, "analyser", analyse)
    async def maj(p, props): return {}
    async def col(nom, genre="rich_text"): return True
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", col)
    j = TestClient(app).get("/v3/validation/api/sirene-flex/" + "0" * 32, params={"adresse": "8 rue Sainte-Cécile 75009"}).json()
    assert j["date"] == "2026-01-01" and "siège créé à cette adresse le 01/01/2026" in j["source_date"] and j["operateur"] == "Patchwork"


def test_saisie_operateur(monkeypatch):
    import v3, v3_validation as VALID
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI(); app.include_router(v3.router)
    ecrit = {}
    async def maj(p, props): ecrit.update(props); return {}
    async def col(nom, genre="rich_text"): return True
    monkeypatch.setattr(VALID, "_refus", lambda req, b: None)
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", col)
    j = TestClient(app).post("/v3/validation/api/operateur-flex/" + "0" * 32, json={"operateur": "morning"}).json()
    assert j["operateur_flex"] == "Morning" and ecrit["Flexoffice"] == {"checkbox": True}
    assert ecrit["Opérateur flex"]["rich_text"][0]["text"]["content"] == "Morning"
    ecrit.clear()
    j = TestClient(app).post("/v3/validation/api/operateur-flex/" + "0" * 32, json={"operateur": "Blue Office"}).json()
    assert j["operateur_flex"] == "Blue Office"
    ecrit.clear()
    j = TestClient(app).post("/v3/validation/api/operateur-flex/" + "0" * 32, json={"retirer": True}).json()
    assert j["operateur_flex"] == "(aucun)" and "Flexoffice" not in ecrit


def test_completer_cp():
    autres = ["18 rue de la Fontaine au Roi, Paris, 75011, FR", "1 bis Av. de la République, 75011 Paris, France"]
    assert S.completer_cp("1 bis avenue de la République", autres) == "1 bis avenue de la République 75011"
    assert S.completer_cp("1 bis avenue de la République", ["1 avenue de la République 13001 Marseille",
                                                            "1 avenue de la République 75011 Paris"]) == "1 bis avenue de la République"
    assert S.completer_cp("8 rue de Sofia 75018", autres) == "8 rue de Sofia 75018"
    assert S.meme_lieu("1 B AVENUE DE LA REPUBLIQUE 75011 PARIS", S.completer_cp("1 bis avenue de la République", autres))


def test_route_date_ecrite(monkeypatch):
    import v3_validation as VALID
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI(); app.include_router(S.router)
    ecrit, vu = {}, {}
    async def une(p):
        return {"siren": "840592455", "nom": "Ask for the moon", "flexoffice": True, "operateur_flex": "", "debut_bail": ""}
    async def analyse(api, siren, nom, adr):
        vu["adr"] = adr
        return {"date": "2025-10-01", "etab": "1 B AVENUE DE LA REPUBLIQUE 75011 PARIS", "siege": "non (établissement secondaire)",
                "operateur": "", "indice": "", "sur": False}
    async def maj(p, props): ecrit.update(props); return {}
    async def col(nom, genre="rich_text"): return True
    monkeypatch.setattr(VALID, "_refus", lambda req, b: None); monkeypatch.setattr(VALID, "role", lambda req: "admin")
    monkeypatch.setattr(VALID, "_une", une); monkeypatch.setattr(S, "analyser", analyse)
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", col)
    j = TestClient(app).get("/v3/validation/api/sirene-flex/" + "0" * 32, params={
        "adresse": "1 bis avenue de la République", "autres": "1 bis Av. de la République, 75011 Paris, France"}).json()
    assert vu["adr"].endswith("75011") and j["date"] == "2025-10-01" and j["date_ecrite"]
    assert ecrit["Début de bail"] == {"date": {"start": "2025-10-01"}}
    assert "établissement secondaire créé à cette adresse le 01/10/2025" in ecrit["Source début de bail"]["rich_text"][0]["text"]["content"]


def test_source_operateur(monkeypatch):
    """Yield Studio : Morning déjà enregistré sans source → la source SIRENE est retrouvée et notée."""
    import v3_validation as VALID
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI(); app.include_router(S.router)
    ecrit = {}
    async def une(p):
        return {"siren": "853327518", "nom": "Yield Studio", "flexoffice": True, "operateur_flex": "Morning",
                "debut_bail": "", "source_flex": ""}
    async def analyse(api, siren, nom, adr):
        return {"date": "", "etab": "", "siege": "", "operateur": "Morning", "sur": False,
                "indice": "centre Morning à la même adresse (LESPACE (MORNING LAFFITE) : 24 RUE DE MOGADOR 75009 PARIS)"}
    async def maj(p, props): ecrit.update(props); return {}
    async def col(nom, genre="rich_text"): return True
    monkeypatch.setattr(VALID, "_refus", lambda req, b: None); monkeypatch.setattr(VALID, "role", lambda req: "admin")
    monkeypatch.setattr(VALID, "_une", une); monkeypatch.setattr(S, "analyser", analyse)
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", col)
    j = TestClient(app).get("/v3/validation/api/sirene-flex/" + "0" * 32, params={"adresse": "24 rue de Mogador 75009"}).json()
    assert j["source"].startswith("SIRENE : centre Morning") and "24 RUE DE MOGADOR" in j["source"]
    assert ecrit["Source opérateur flex"]["rich_text"][0]["text"]["content"] == j["source"]


def test_etablissements_par_raison_sociale(monkeypatch):
    """Yield Studio : la recherche par SIREN ne donne que le siège (Tours) ; l'établissement du 24 rue de Mogador
    n'apparaît qu'en cherchant « YIELD » dans le 75009."""
    S._CENTRES.clear()
    async def chercher(api, p):
        if p["q"] == "853327518":
            return [{"siren": "853327518", "nom_complet": "YIELD (YIELD STUDIO)", "nom_raison_sociale": "YIELD",
                     "siege": {"siret": "1", "adresse": "1 IMPASSE DU PALAIS 37000 TOURS", "date_creation": "2021-06-18",
                               "est_siege": True, "etat_administratif": "A"}, "matching_etablissements": []}]
        if p["q"] == "YIELD" and p.get("code_postal") == "75009":
            return [{"siren": "853327518", "siege": {"siret": "1", "adresse": "1 IMPASSE DU PALAIS 37000 TOURS", "est_siege": True},
                     "matching_etablissements": [{"siret": "2", "adresse": "24 RUE DE MOGADOR 75009 PARIS", "date_creation": "2025-01-30",
                                                  "est_siege": False, "etat_administratif": "A"}]}]
        return []
    monkeypatch.setattr(S, "_chercher", chercher)
    r = asyncio.run(S.analyser(None, "853327518", "Yield Studio", "24 rue de Mogador 75009"))
    assert r["date"] == "2025-01-30" and r["siege"].startswith("non")
