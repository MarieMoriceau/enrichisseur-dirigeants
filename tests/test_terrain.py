"""Fichier de prospection terrain : l'adresse physique sert de source sûre et départage les homonymes SIRENE."""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3, v3_linkedin as LI

CSV = ("Société;Adresse physique (terrain);Code postal;Ville\n"
       "Sombrero films;1 place Boieldieu;75002;Paris\nGeorgen;3 rue d'Amboise;75002;Paris\n").encode()


def test_adresse_terrain_lue_avec_code_postal():
    o = v3.lire_fichier("relance.csv", CSV)
    assert o[0]["adresse_notion"] == "1 place Boieldieu 75002 Paris" and o[0]["adresse_crm"] == "" and o[0]["cp"] == "75002"


def test_terrain_confirme_le_siege():
    r = {"nom": "Georgen", "siren_crm": "", "siren_site": "", "siren_nom": "1", "etat_nom": "active", "mode": "complet",
         "site_joignable": "non", "controle_nom": "", "raison_sociale": "GEORGEN", "siege_insee": "3 RUE D'AMBOISE 75002 PARIS",
         "adresse_notion": "3 rue d'Amboise 75002 Paris"}
    st, act, conf = v3.statut(r)
    assert conf == "haute" and "terrain" in act


def test_homonyme_au_bon_endroit_choisi(monkeypatch):
    class R:
        status_code = 200
        def json(self):
            return {"results": [
                {"siren": "880722848", "nom_raison_sociale": "GEORGEN", "etat_administratif": "A",
                 "siege": {"adresse": "RTE NATIONALE 54800 HANNONVILLE-SUZEMONT"}},
                {"siren": "123456789", "nom_raison_sociale": "GEORGEN", "etat_administratif": "A",
                 "siege": {"adresse": "3 RUE D'AMBOISE 75002 PARIS"}}]}
    async def get(api, p):
        return R()
    monkeypatch.setattr(v3, "_insee_get", get)
    r = asyncio.run(v3.insee_par_nom(None, "Georgen", "75002", "3 rue d'Amboise 75002 Paris"))
    assert r["siren"] == "123456789"


def test_page_linkedin_du_fichier():
    f = ("Société;Adresse physique (terrain);Code postal;Ville;LinkedIn\n"
         "M141 production;1 place Boieldieu;75002;Paris;https://www.linkedin.com/company/m141/home/\n"
         "Pce;6 rue d'Amboise;75002;Paris;https://www.linkedin.com/in/quelquun\n").encode()
    o = v3.lire_fichier("relance.csv", f)
    assert o[0]["linkedin_fichier"].endswith("/company/m141/home/") and "linkedin_fichier" not in o[1]
