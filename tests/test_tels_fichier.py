"""Téléphones des personnes dans le fichier déposé (export Pipedrive). Lancer : python3 -m pytest tests/"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3, v3_linkedin as LI

CSV = ("Organisation - Nom;Organisation - ID;Personne - Nom;Personne - ID;Personne - Téléphone\n"
       "Botify;4581;Adrien Menard;101;06 12 34 56 78\n"
       "Botify;4581;Alexia Ceva;18703;\n"
       "Botify;4581;Julie Martin;102;+33 1 44 55 66 77\n"
       "Yacast France;67548;Paul Durand;201;01 23 45 67 89\n").encode("utf-8")


def test_une_societe_par_organisation_avec_ses_numeros():
    orgs = v3.lire_fichier("personnes.csv", CSV)
    assert [o["nom"] for o in orgs] == ["Botify", "Yacast France"]
    assert orgs[0]["tels_crm"] == [{"id": "101", "nom": "Adrien Menard", "telephone": "+33612345678"},
                                   {"id": "102", "nom": "Julie Martin", "telephone": "+33144556677"}]


def test_rattachement_aux_contacts_crm():
    r = {"tels_crm": [{"id": "101", "nom": "Adrien Menard", "telephone": "+33612345678"},
                      {"id": "", "nom": "Julie Martin", "telephone": "+33144556677"}],
         "contacts_crm": [{"id": "101", "nom_complet": "Menard", "telephone": ""},
                          {"id": "999", "nom_complet": "Julie  Martin", "telephone": ""},
                          {"id": "102", "nom_complet": "Autre", "telephone": "+33600000000"}]}
    assert LI.tels_du_fichier(r) == 2
    assert r["contacts_crm"][0]["telephone"] == "+33612345678" and r["contacts_crm"][0]["source_tel"] == "fichier déposé"
    assert r["contacts_crm"][1]["telephone"] == "+33144556677"
    assert r["contacts_crm"][2]["telephone"] == "+33600000000"                      # numéro Pipedrive gardé


def test_fichier_organisations_inchange():
    f = "Organisation - Nom;Organisation - ID;Adresse\nBotify;4581;22 rue Royale 75008 Paris\n".encode("utf-8")
    o = v3.lire_fichier("orgs.csv", f)
    assert len(o) == 1 and "tels_crm" not in o[0] and o[0]["adresse_crm"].startswith("22 rue Royale")
