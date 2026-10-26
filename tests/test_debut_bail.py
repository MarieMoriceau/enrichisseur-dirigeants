"""« Début de bail » : date saisie à la main dans l'écran société. Lancer : python3 -m pytest tests/"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient

import v3, v3_validation as VALID

PAGE = "d" * 32


def _client(monkeypatch):
    ecrit, colonnes = {}, []
    async def maj(p, props):
        ecrit.update(props); return {"ok": True}
    async def colonne(nom, genre="rich_text"):
        colonnes.append((nom, genre)); return True
    async def une(p):
        return {"page": p, "operateur_flex": "WeWork (probable)"}
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", colonne); monkeypatch.setattr(VALID, "_une", une)
    monkeypatch.setattr(VALID, "_refus", lambda req, besoin: None)
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app), ecrit, colonnes


def test_enregistrer_et_effacer(monkeypatch):
    c, ecrit, colonnes = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": "2024-03-01"})
    assert r.json()["debut_bail"] == "2024-03-01" and ecrit["Début de bail"] == {"date": {"start": "2024-03-01"}}
    assert ("Début de bail", "date") in colonnes and ecrit["Source début de bail"]["rich_text"][0]["text"]["content"] == "saisie à la main"
    c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": ""})
    assert ecrit["Début de bail"] == {"date": None}


def test_refus(monkeypatch):
    c, ecrit, _ = _client(monkeypatch)
    assert c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": "01/03/2024"}).status_code == 400
    assert c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": "1024-03-01"}).status_code == 400
    assert not ecrit


def test_flexoffice(monkeypatch):
    c, ecrit, colonnes = _client(monkeypatch)
    assert c.post(f"/v3/validation/api/flexoffice/{PAGE}", json={"coche": True}).json()["flexoffice"] is True
    assert ecrit["Flexoffice"] == {"checkbox": True} and ("Flexoffice", "checkbox") in colonnes
    assert ecrit["Opérateur flex"]["rich_text"][0]["text"]["content"] == "WeWork"      # probable → confirmé
    c.post(f"/v3/validation/api/flexoffice/{PAGE}", json={"coche": False})
    assert ecrit["Flexoffice"] == {"checkbox": False}


def test_lecture_case():
    import v3_memoire as MEM
    assert MEM._val({"F": {"type": "checkbox", "checkbox": True}}, "F") == "oui"
    assert MEM._val({"F": {"type": "checkbox", "checkbox": False}}, "F") == ""


def test_flexoffice_fichier(monkeypatch):
    c, ecrit, _ = _client(monkeypatch)
    pages = []
    async def toutes():
        return [{"page": "a", "nom": "A", "lancement": "locataires_flex_paris (suite 1/2)", "flexoffice": False},
                {"page": "b", "nom": "B", "lancement": "autre ; locataires_flex_paris (suite 2/2)", "flexoffice": True},
                {"page": "c", "nom": "C", "lancement": "locataires_flex_paris_bis", "flexoffice": False}]
    async def maj(p, props):
        pages.append(p); return {"ok": True}
    monkeypatch.setattr(VALID, "_toutes", toutes); monkeypatch.setattr(VALID, "_maj", maj)
    j = c.get("/v3/validation/api/flexoffice-fichier", params={"lancement": "locataires_flex_paris"}).json()
    assert j["societes"] == 2 and j["a_cocher"] == 1 and not pages
    j = c.get("/v3/validation/api/flexoffice-fichier", params={"lancement": "locataires_flex_paris (suite 1/2)", "confirmer": 1}).json()
    assert j["cochees"] == 1 and pages == ["a"]
