"""Adresse terrain reprise de la base Notion « Prospection terrain » pour les lancements sans adresse physique."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3, v3_terrain as T

TERRAIN = [{"nom": "Kearney", "nom_pd": "", "id": "", "adresse": "8 rue Lamennais", "cp": "75008", "nego": "Richard", "date": "2026-11-03"},
           {"nom": "Sweet", "nom_pd": "", "id": "", "adresse": "10 rue de Lisbonne", "cp": "75008", "nego": "Sébastien", "date": "2026-10-30"},
           {"nom": "Sweet", "nom_pd": "", "id": "", "adresse": "3 rue Taitbout", "cp": "75009", "nego": "Richard", "date": "2026-10-01"},
           {"nom": "Hermès International", "nom_pd": "Hermès", "id": "4242", "adresse": "24 rue du Faubourg Saint-Honoré", "cp": "75008",
            "nego": "Sébastien", "date": "2026-10-30"}]


def test_adresse_complete():
    assert T.adresse_complete({"adresse": "8 rue Lamennais", "cp": "75008"}) == "8 rue Lamennais 75008 Paris"
    assert T.adresse_complete({"adresse": "1 rue X 92100 Boulogne", "cp": "92100"}) == "1 rue X 92100 Boulogne"
    assert T.adresse_complete({"adresse": "2 rue des Italiens, 2ème arrondissement", "cp": ""}) == "2 rue des Italiens 75002 Paris"
    assert T.adresse_complete({"adresse": "8 rue d'Athènes, 8e", "cp": ""}) == "8 rue d'Athènes 75008 Paris"
    assert T.adresse_complete({"adresse": "8 rue de Choiseul Paris", "cp": ""}) == "8 rue de Choiseul"
    assert T.rue_et_cp({"adresse": "14 rue Favart 75002", "cp": ""}) == ("14 rue Favart", "75002")
    assert T.adresse_complete({"adresse": "27 boulevard des Italiens", "cp": ""}) == "27 boulevard des Italiens"


def test_appariement():
    t, c = T.apparier("Kearney", "", TERRAIN, v3.ressemblance)
    assert t["adresse"] == "8 rue Lamennais" and c == "nom"
    t, c = T.apparier("Hermès", "4242", TERRAIN, v3.ressemblance)
    assert c == "ID Pipedrive"
    t, c = T.apparier("Sweet", "", TERRAIN, v3.ressemblance)
    assert t is None and "plusieurs adresses" in c
    assert T.apparier("Splio", "", TERRAIN, v3.ressemblance) == (None, "absente de Prospection terrain")


def test_pas_d_inclusion_de_mots():
    assert not T.meme_nom("Gibert avocats", "Avocats") and not T.meme_nom("Briand avocats", "Avocats")
    assert T.meme_nom("Rubix consulting", "Rubix") and T.meme_nom("Hermès", "HERMES") and T.meme_nom("Mgtech", "MG Tech")
    assert not T.meme_nom("ERMES", "Hermès")


def test_ban_corrige_cp_et_faute(monkeypatch):
    import asyncio
    class R:
        status_code = 200
        def __init__(self, f): self.f = f
        def json(self): return {"features": self.f}
    def f(num, rue, cp):
        return {"properties": {"housenumber": num, "street": rue, "postcode": cp}}
    rep = {"8 rue d'Athènes Paris": [f("8", "Rue d'Athènes", "75009")],
           "3 rue d'Ambroise Paris": [f("3", "Rue d'Amboise", "75002"), f("3", "Rue de la Paix", "75002")]}
    class C:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get(self, url, params=None): return R(rep.get(params["q"], []))
    monkeypatch.setattr(T.httpx, "AsyncClient", C)
    T._BAN.clear()
    assert asyncio.run(T.adresse_paris({"adresse": "8 rue d'Athènes, 8e", "cp": ""}, [])) == "8 Rue d'Athènes 75009 Paris"
    assert asyncio.run(T.adresse_paris({"adresse": "3 rue d'Ambroise", "cp": ""}, [])) == "3 Rue d'Amboise 75002 Paris"
