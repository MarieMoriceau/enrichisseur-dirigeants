"""Validation des contacts CRM : un e-mail repris tel quel de Pipedrive, même incomplet, ne bloque pas la validation."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient
import v3_validation as V

PAGE = "0" * 32


def _client(monkeypatch):
    ecrit = {}
    async def une(p):
        return {"validation": "Société validée"}
    async def maj(p, props):
        ecrit.update(props)
        return {"validation": "Contacts validés"}
    monkeypatch.setattr(V, "_refus", lambda req, role: None)
    monkeypatch.setattr(V, "_une", une)
    monkeypatch.setattr(V, "_maj", maj)
    app = FastAPI(); app.include_router(V.router)
    return TestClient(app), ecrit


def _dec(email, email_crm, t="crm"):
    return {"type": t, "action": "garder" if t == "crm" else "à décider", "id": "101583",
            "nom_complet": "JEAN FRANCOIS FELLMANN", "email": email, "email_crm": email_crm}


def test_email_crm_incomplet_tel_quel(monkeypatch):
    c, ecrit = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/contacts/{PAGE}", json={"decisions": [_dec("jffellmann@oceaneconsulting", "jffellmann@oceaneconsulting")]})
    assert r.status_code == 200, r.text
    assert "jffellmann@oceaneconsulting" in str(ecrit["Décisions contacts"])


def test_contact_fiche_ailleurs_tel_quel(monkeypatch):
    c, _ = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/contacts/{PAGE}", json={"decisions": [_dec("x@societe", "", "ailleurs")]})
    assert r.status_code == 200, r.text


def test_saisie_a_la_main_toujours_controlee(monkeypatch):
    c, _ = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/contacts/{PAGE}", json={"decisions": [_dec("jffellmann@oceane", "jffellmann@oceaneconsulting")]})
    assert r.status_code == 400 and "e-mail illisible" in r.json()["erreur"]
    r = c.post(f"/v3/validation/api/contacts/{PAGE}", json={"decisions": [_dec("jffellmann@oceaneconsulting.com", "jffellmann@oceaneconsulting")]})
    assert r.status_code == 200
