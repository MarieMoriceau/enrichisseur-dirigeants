"""Recherche Pipedrive : un mot en commun ne fait pas une même société (cas « Oliver », 06/11)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3


def test_un_seul_mot_ne_suffit_pas():
    for b in ("Oliver Wyman", "Bernd Oliver Bühler", "Oliverlist SAS", "Oliver Marketing"):
        assert not v3.meme_societe_crm("Oliver", b), b


def test_vrais_doublons():
    assert v3.meme_societe_crm("Oliver Wyman", "OLIVER WYMAN")
    assert v3.meme_societe_crm("Oliver Wyman", "Oliver-Wyman")
    assert v3.meme_societe_crm("Oliver Wyman", "Oliver Wyman SAS")
    assert v3.meme_societe_crm("Oliver Wyman", "Olivr Wyman")
    assert v3.meme_societe_crm("Grand Large Yachting", "Grand Large Yachting Paris")
    assert not v3.meme_societe_crm("Oliver Marketing", "Oliver Wyman")


def test_relance_lache_la_mauvaise_fiche(monkeypatch):
    import asyncio
    async def une(p):
        return {"nom": "Oliver Marketing", "siren": "809609340", "siren_corrige": '{"avant": "814935243"}'}
    async def lire(p):
        return {"id": "26989", "nom": "Oliver", "siren_crm": "814935243", "adresse_notion": "7 Rue d'Athènes 75009 Paris"}
    async def nom_pd(i):
        return "Bernd Oliver Bühler"
    monkeypatch.setattr(v3.VALID, "_une", une)
    monkeypatch.setattr(v3.MEM, "lire", lire)
    monkeypatch.setattr(v3, "nom_pipedrive", nom_pd)
    o = asyncio.run(v3._orgs_a_relancer(["p"]))[0]
    assert o["id"] == "" and o["siren_crm"] == "809609340" and o["nom"] == "Oliver Marketing"
    async def nom_pd2(i):
        return "OLIVER MARKETING"
    monkeypatch.setattr(v3, "nom_pipedrive", nom_pd2)
    assert asyncio.run(v3._orgs_a_relancer(["p"]))[0]["id"] == "26989"
