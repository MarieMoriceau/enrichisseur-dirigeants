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
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", colonne)
    monkeypatch.setattr(VALID, "_refus", lambda req, besoin: None)
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app), ecrit, colonnes


def test_enregistrer_et_effacer(monkeypatch):
    c, ecrit, colonnes = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": "2024-03-01"})
    assert r.json()["debut_bail"] == "2024-03-01" and ecrit["Début de bail"] == {"date": {"start": "2024-03-01"}}
    assert colonnes == [("Début de bail", "date")]
    c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": ""})
    assert ecrit["Début de bail"] == {"date": None}


def test_refus(monkeypatch):
    c, ecrit, _ = _client(monkeypatch)
    assert c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": "01/03/2024"}).status_code == 400
    assert c.post(f"/v3/validation/api/debut-bail/{PAGE}", json={"date": "1024-03-01"}).status_code == 400
    assert not ecrit
