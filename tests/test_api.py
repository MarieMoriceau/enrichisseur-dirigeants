"""API /v3/api/lancer : clé, format, compte automatisations. Lancer : python3 -m pytest tests/"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient
import v3

DEUX = [{"nom": "Poiscaille", "siren": "443 061 841", "code_postal": "75011", "ville": "Paris", "adresse": "12 rue X",
         "statut_pipedrive": "Prospect", "id_pipedrive": "4521", "nego": "LBA", "campagne": "S43", "ref_notion": "abc"},
        {"nom": "Ulule", "code_postal": "75010"}]


def _client(monkeypatch, cle="secret"):
    monkeypatch.setattr(v3, "API_CLE", cle)
    vus = {}
    async def faux_traiter(jid, orgs):
        vus[jid] = orgs
        j = v3.JOBS[jid]
        j.update(reparti=True, nb_memoire=1, noms_memoire=["Ulule"], total=2, lots_reportes=[])
    monkeypatch.setattr(v3, "traiter", faux_traiter)
    monkeypatch.setattr(v3.LI, "pb_configure", lambda: True)
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app), vus


def test_cle(monkeypatch):
    c, _ = _client(monkeypatch)
    assert c.post("/v3/api/lancer", json={"societes": DEUX}).status_code == 401
    assert c.post("/v3/api/lancer", json={"societes": DEUX}, headers={"X-Enrichisseur-Cle": "faux"}).status_code == 401
    c2, _ = _client(monkeypatch, cle="")
    assert c2.post("/v3/api/lancer", json={"societes": DEUX}, headers={"X-Enrichisseur-Cle": ""}).status_code == 503


def test_lancer_et_suivi(monkeypatch):
    c, vus = _client(monkeypatch)
    h = {"X-Enrichisseur-Cle": "secret"}
    r = c.post("/v3/api/lancer", json={"nom": "Prospection hebdo S43", "source": "prospection-hebdo", "societes": DEUX + [{"siren": "1"}]}, headers=h)
    assert r.status_code == 202, r.text
    j = r.json()
    assert j["ok"] and j["recues"] == 2 and j["ignorees_sans_nom"] == [2] and j["perimetre"] == "tout"
    jid = j["lancement"]
    assert v3.JOBS[jid]["origine"] == "outils"                     # jamais le compte des négos
    o = vus[jid][0]
    assert o["siren_crm"] == "443061841" and o["cp"] == "75011" and o["id"] == "4521" and o["infos_source"]["nego"] == "LBA"
    s = c.get(f"/v3/api/lancement/{jid}", headers=h).json()
    assert s["deja_enrichies"] == {"nombre": 1, "societes": ["Ulule"]} and s["aujourdhui"] == 1 and s["recues"] == 2
    assert c.get("/v3/api/lancement/inconnu", headers=h).status_code == 404


def test_erreurs(monkeypatch):
    c, _ = _client(monkeypatch)
    h = {"X-Enrichisseur-Cle": "secret"}
    assert c.post("/v3/api/lancer", content="pas du json", headers=h).status_code == 400
    assert c.post("/v3/api/lancer", json={"societes": []}, headers=h).status_code == 400
    assert c.post("/v3/api/lancer", json={"societes": DEUX, "perimetre": "bidon"}, headers=h).status_code == 400
