"""Contacts Pipedrive : doublons et sociétés sans page LinkedIn. Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_linkedin as LI


def test_fiches_doublons():
    assert LI.fiches_pipedrive({"id": "82964", "dans_crm": "Pipedrive (3 fiches, doublons : 15395, 68743)"}) == ["82964", "15395", "68743"]
    assert LI.fiches_pipedrive({"id": "82964", "dans_crm": "fichier"}) == ["82964"]
    assert LI.fiches_pipedrive({"id": "", "dans_crm": ""}) == []


def test_personnes_de_toutes_les_fiches(monkeypatch):
    async def pers(c, oid):
        return {"82964": [], "15395": [{"id": "1", "nom_complet": "Anna Naccache"}],
                "68743": [{"id": "1", "nom_complet": "Anna Naccache"}, {"id": "2", "nom_complet": "Frédéric Remeur"}]}[oid]
    monkeypatch.setattr(LI, "personnes_societe", pers)
    out = asyncio.run(LI.personnes_fiches(None, {"id": "82964", "dans_crm": "Pipedrive (3 fiches, doublons : 15395, 68743)"}))
    assert [p["nom_complet"] for p in out] == ["Anna Naccache", "Frédéric Remeur"] and out[0]["fiche_doublon"] == "15395"


def test_tache_manquants_essai(monkeypatch):
    import v3_rattrapage as R, v3_validation as VALID, v3_memoire as MEM
    async def toutes():
        return [{"page": "a", "nom": "Homeland", "validation": "Société validée", "perimetre": "tout"},
                {"page": "b", "nom": "Lue", "validation": "À valider", "perimetre": "tout"},
                {"page": "c", "nom": "Plante", "validation": "À valider", "perimetre": "tout"}]
    async def lire(p):
        if p == "c":
            raise RuntimeError("timeout")
        return {"id": "82964"} if p == "a" else {"id": "1", "contacts_crm": []}
    async def dodo(_):
        pass
    monkeypatch.setattr(VALID, "_toutes", toutes); monkeypatch.setattr(MEM, "lire", lire); monkeypatch.setattr(R.asyncio, "sleep", dodo)
    asyncio.run(R._tache_manquants(False))
    assert R.MANQUANTS["a_lire"] == 1 and R.MANQUANTS["exemples"] == ["Homeland"] and R.MANQUANTS["illisibles"] == 1
    assert not R.MANQUANTS["actif"] and "essai" in R.MANQUANTS["phase"]
