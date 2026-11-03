"""SIREN trouvé par le nom : confiance haute quand la raison sociale colle et que le siège = adresse trouvée. Lancer : python3 -m pytest tests/"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3, v3_linkedin as LI


def _row(**k):
    base = {"siren_crm": "", "siren_site": "", "mode": "complet", "site_joignable": "oui", "etat_nom": "active",
            "controle_nom": "➖ pas de SIREN sur le site : site non vérifié", "nb_homonymes": 1}
    return {**base, **k}


def test_sombrero_site_et_linkedin():
    r = _row(nom="Sombrero films", siren_nom="494928492", raison_sociale="SOMBRERO FILMS", siege_insee="1 PLACE BOIELDIEU 75002 PARIS",
             adresses_site_liste=["1 place Boieldieu, 75002 Paris"], adresse_linkedin="1, Place Boieldieu, Paris, 75002, FR")
    st, act, conf = v3.statut(r)
    assert (st, conf) == ("Active", "haute") and "site + LinkedIn" in act


def test_perazzone_par_google_apres_coup():
    r = _row(nom="Cabinet Perazzone Brun", siren_nom="517864575", raison_sociale="CABINET PERAZZONE-BRUN",
             siege_insee="14 RUE FAVART 75002 PARIS", nb_homonymes=3)
    assert v3.statut(r)[2] == "moyenne"                       # rien pour confirmer encore
    r["adresse_google"] = "14 Rue Favart, 75002 Paris, France"
    d = LI.decider_adresse(r)                                 # Google arrive : statut recalculé
    assert d["confiance"] == "haute" and "Google" in d["action"]


def test_siege_ailleurs_reste_a_verifier():
    r = _row(nom="Pce", siren_nom="521429811", raison_sociale="PCE", siege_insee="23 RUE HECTOR BERLIOZ 95360 MONTMAGNY",
             adresse_google="6 Rue d'Amboise, 75002 Paris, France", nb_homonymes=5)
    assert v3.statut(r)[2] == "moyenne"


def test_raison_sociale_differente_reste_a_verifier():
    r = _row(nom="Georgen", siren_nom="880722848", raison_sociale="LUDOVIC GEORGEN", siege_insee="14 RUE FAVART 75002 PARIS",
             adresse_google="14 Rue Favart, 75002 Paris")
    assert v3.statut(r)[2] == "moyenne"


def test_societe_fermee_jamais_haute():
    r = _row(nom="Gibert avocats", siren_nom="788559300", raison_sociale="GIBERT AVOCATS", etat_nom="fermée",
             siege_insee="39 RUE DE LA BOURSE 69002 LYON", adresse_google="39 rue de la Bourse 69002 Lyon")
    assert v3.statut(r)[2] == "moyenne"
