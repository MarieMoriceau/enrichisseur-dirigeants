"""Libellé exact du code NAF, et domiciliation recalculée après une correction de SIREN. Lancer : python3 -m pytest tests/"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import v3_naf as NAF, v3_validation as VALID


def test_libelle_sous_classe():
    assert NAF.libelle("69.10Z") == "Activités juridiques"
    assert NAF.libelle("6910z") == "Activités juridiques"          # sans point, en minuscule
    assert NAF.libelle("62.01Z") == "Programmation informatique"
    assert NAF.libelle("69.10Y") == "" and NAF.libelle("") == ""     # NAF 2025 ou vide : on retombe sur la section
    assert len(NAF._TABLE) == 732


def _entropy(retenue=""):
    corr = {"avant": "843944497", "insee": {"raison_sociale": "ENTROPY", "siege_insee": "13 RUE DU QUATRE SEPTEMBRE 75002 PARIS",
                                             "naf": "69.10Z", "secteur": "Activités spécialisées, scientifiques et techniques"}}
    return {"siren_corrige": json.dumps(corr), "adresse_retenue": retenue, "source_retenue": "LinkedIn" if retenue else "",
            "data": {"siege_insee": "52 AVENUE DES CHAMPS ELYSEES 75008 PARIS", "naf": "70.22Z",
                     "adresse_proposee": "13 rue du Quatre-Septembre, 75002 Paris", "confiance_adresse": "haute",
                     "sources_adresse": "LinkedIn + Site", "verdict_domiciliation": "📍 Siège ≠ bureaux"}}


def test_correction_siren_recalcule_domiciliation_et_activite():
    info = _entropy()
    VALID.appliquer_correction_siren(info)
    assert info["data"]["verdict_domiciliation"] == "✅ Cohérent"
    assert info["data"]["activite"] == "Activités juridiques"


def test_adresse_validee_prime():
    info = _entropy(retenue="8 rue de la Paix, 75002 Paris")
    VALID.appliquer_correction_siren(info)
    assert info["data"]["verdict_domiciliation"] == "📍 Siège ≠ bureaux"


def test_sans_correction_rien_ne_change_sauf_le_libelle():
    info = {"siren_corrige": "", "data": {"naf": "47.11B", "verdict_domiciliation": "🔍 À vérifier"}}
    VALID.appliquer_correction_siren(info)
    assert info["data"] == {"naf": "47.11B", "verdict_domiciliation": "🔍 À vérifier", "activite": "Commerce d'alimentation générale"}
