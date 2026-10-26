"""Fichier CRM : date de début de bail et opérateur flex lus. Lancer : python3 -m pytest tests/"""
import io, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from openpyxl import Workbook
import v3


def test_date_bail():
    assert v3.date_bail("2010-07-27") == "2010-07-27"
    assert v3.date_bail("2010-07-27 00:00:00") == "2010-07-27"
    assert v3.date_bail("27/7/2010") == "2010-07-27"
    assert v3.date_bail("2019") == "" and v3.date_bail("") == "" and v3.date_bail("2010-02-31") == ""


def test_export_pipedrive():
    wb = Workbook(); ws = wb.active
    ws.append(["Organisation - ID", "Organisation - Nom", "Organisation - OPERATEUR FLEX", "Organisation - Adresse",
               "Organisation - ADRESSE VERIFIEE", "Organisation - ANNEE DEBUT DE BAIL", "Organisation - DATE DEBUT DE BAIL"])
    ws.append([450, "Hbc Avocats", "", "4 Av. Hoche, 75008 Paris", "", "2009", "2010-07-27"])
    ws.append([451, "Flexy", "Morning", "10 rue X, 75002 Paris", "", "", ""])
    b = io.BytesIO(); wb.save(b)
    o = v3.lire_fichier("export.xlsx", b.getvalue())
    assert o[0]["debut_bail_crm"] == "2010-07-27" and "flex_crm" not in o[0] and o[0]["adresse_crm"].startswith("4 Av")
    assert o[1]["flex_crm"] == "Morning" and "debut_bail_crm" not in o[1]
