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


def test_appariement():
    t, c = T.apparier("Kearney", "", TERRAIN, v3.ressemblance)
    assert t["adresse"] == "8 rue Lamennais" and c == "nom"
    t, c = T.apparier("Hermès", "4242", TERRAIN, v3.ressemblance)
    assert c == "ID Pipedrive"
    t, c = T.apparier("Sweet", "", TERRAIN, v3.ressemblance)
    assert t is None and "plusieurs adresses" in c
    assert T.apparier("Splio", "", TERRAIN, v3.ressemblance) == (None, "absente de Prospection terrain")
