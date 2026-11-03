"""Sombrero films (03/11) : 0 contact. Directeurs/producteurs gardés, et représentants légaux SIRENE en secours."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_linkedin as LI


def test_roles_elargis():
    assert LI.role("Productrice") == "8 Autre" and LI.role("Directeur de production") == "8 Autre"
    assert LI.role("Owner") == "5 Associé / Gérant"
    assert LI.role("Assistante de production") == "" and LI.role("Directeur adjoint") == ""
    assert LI.role("CEO") == "1 CEO / Président / DG"


def test_representants_legaux_en_secours():
    r = {"nom": "M141 production", "statut": "Active", "confiance": "haute", "domaine": "m141.fr",
         "dirigeants_insee": [{"nom": "DUPONT", "prenom": "JEAN PIERRE", "qualite": "Président de SAS"},
                              {"nom": "MARTIN", "prenom": "ANNE", "qualite": "Directeur général délégué"},
                              {"nom": "X", "prenom": "Y", "qualite": "Commissaire aux comptes titulaire"}]}
    autre = {"nom": "Georgen", "statut": "Radiée, à vérifier", "confiance": "moyenne",
             "dirigeants_insee": [{"nom": "G", "prenom": "L", "qualite": "Gérant"}]}
    assert LI.dirigeants_sirene([r, autre]) == [r]
    assert [d["nom_complet"] for d in r["dirigeants"]] == ["Jean Dupont", "Anne Martin"]
    assert r["dirigeants"][0]["role"] == "1 CEO / Président / DG" and r["dirigeants"][0]["linkedin"] == ""


def test_pas_de_secours_si_linkedin_a_trouve():
    r = {"statut": "Active", "confiance": "haute", "dirigeants": [{"nom": "a"}],
         "dirigeants_insee": [{"nom": "B", "prenom": "C", "qualite": "Président"}]}
    assert LI.dirigeants_sirene([r]) == [] and len(r["dirigeants"]) == 1
