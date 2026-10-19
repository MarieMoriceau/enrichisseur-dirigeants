"""Écarter une société : décision (hors HubSpot / à supprimer) et motif enregistrés dans Notion."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient
import v3, v3_validation as VAL

PAGE = "0123456789abcdef0123456789abcdef"


class R:
    def __init__(self, code, j=None): self.status_code, self._j, self.text = code, j or {}, ""
    def json(self): return self._j


def _client(monkeypatch):
    envois = []
    async def faux_req(c, methode, url, json=None, **k):
        envois.append((methode, url, json))
        if "/databases/" in url:
            return R(200)
        props = json["properties"]
        sel = lambda n: {"type": "select", "select": props[n]["select"] if n in props else None}
        return R(200, {"id": PAGE, "properties": {"Validation": sel("Validation"), "Écartée pour": sel("Écartée pour"),
                       "Motif écart": props.get("Motif écart", {"rich_text": []})}})
    monkeypatch.setattr(VAL.MEM, "_req", faux_req)
    monkeypatch.setattr(VAL, "_refus", lambda req, besoin: None)
    VAL._COLONNES_ECART["ok"] = False
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app), envois


def test_ecarter_a_supprimer(monkeypatch):
    c, envois = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/societe/{PAGE}", json={"statut": "Écartée", "decision": "À supprimer de la base",
                                                         "motifs": ["Hors Île-de-France", "bidon"], "motif_libre": "  siège à Lyon "})
    assert r.status_code == 200, r.text
    assert envois[0][0] == "PATCH" and "/databases/" in envois[0][1]          # colonnes créées une fois
    props = envois[1][2]["properties"]
    assert props["Écartée pour"] == {"select": {"name": "À supprimer de la base"}}
    assert props["Motif écart"]["rich_text"][0]["text"]["content"] == "Hors Île-de-France · siège à Lyon"
    assert r.json()["decision_ecart"] == "À supprimer de la base"


def test_remettre_a_valider_efface(monkeypatch):
    c, envois = _client(monkeypatch)
    c.post(f"/v3/validation/api/societe/{PAGE}", json={"statut": "À valider"})
    props = envois[-1][2]["properties"]
    assert props["Écartée pour"] == {"select": None}


def test_decision_inconnue(monkeypatch):
    c, _ = _client(monkeypatch)
    assert c.post(f"/v3/validation/api/societe/{PAGE}", json={"statut": "Écartée", "decision": "n'importe"}).status_code == 400
