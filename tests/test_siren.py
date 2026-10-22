"""Correction manuelle du SIREN par l'admin. Lancer : python3 -m pytest tests/"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient

import v3, v3_validation as VALID

PAGE = "a" * 32


def _client(monkeypatch, siren_actuel="507807949", corrige=""):
    ecrit = {}
    async def une(page):
        return {"page": page, "siren": siren_actuel, "siren_corrige": corrige}
    async def maj(page, props):
        ecrit.update(props)
        return {"ok": True}
    async def insee(c, siren):
        if siren == "443061841":
            return {"raison_sociale": "POISCAILLE", "etat": "active", "date_fermeture": "", "siege": "12 RUE X 75011 PARIS",
                    "tranche_insee": "21", "naf": "47.11B", "secteur": "Commerce"}
        return {"raison_sociale": "", "etat": "introuvable", "date_fermeture": "", "siege": "", "tranche_insee": "", "naf": "", "secteur": ""}
    async def colonne(nom):
        return True
    monkeypatch.setattr(VALID, "_une", une); monkeypatch.setattr(VALID, "_maj", maj)
    monkeypatch.setattr(VALID, "assurer_colonne", colonne); monkeypatch.setattr(v3, "insee", insee)
    monkeypatch.setattr(VALID, "_refus", lambda req, besoin: None)
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app), ecrit


def test_siren_corrige(monkeypatch):
    c, ecrit = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/siren/{PAGE}", json={"siren": "443 061 841"})
    assert r.status_code == 200, r.text
    assert ecrit["SIREN"]["rich_text"][0]["text"]["content"] == "443061841"
    corr = json.loads(ecrit["SIREN corrigé"]["rich_text"][0]["text"]["content"])
    assert corr["avant"] == "507807949" and corr["insee"]["raison_sociale"] == "POISCAILLE"
    info = {"siren": "443061841", "siren_corrige": ecrit["SIREN corrigé"]["rich_text"][0]["text"]["content"],
            "data": {"raison_sociale": "AGENCE 79", "siege_insee": "PUTEAUX", "linkedin": "x"}}
    VALID.appliquer_correction_siren(info)
    assert info["data"]["raison_sociale"] == "POISCAILLE" and info["data"]["linkedin"] == "x" and info["siren_avant"] == "507807949"


def test_siren_refuse(monkeypatch):
    c, ecrit = _client(monkeypatch)
    assert c.post(f"/v3/validation/api/siren/{PAGE}", json={"siren": "443061842"}).status_code == 400   # clé fausse
    assert c.post(f"/v3/validation/api/siren/{PAGE}", json={"siren": "12345"}).status_code == 400
    assert c.post(f"/v3/validation/api/siren/{PAGE}", json={"siren": "507807949"}).status_code == 400   # déjà celui-là
    assert c.post(f"/v3/validation/api/siren/{PAGE}", json={"siren": "732829320"}).status_code == 400   # introuvable
    assert not ecrit


def test_deuxieme_correction_garde_le_premier(monkeypatch):
    c, ecrit = _client(monkeypatch, siren_actuel="732829320", corrige=json.dumps({"avant": "507807949"}))
    assert c.post(f"/v3/validation/api/siren/{PAGE}", json={"siren": "443061841"}).status_code == 200
    assert json.loads(ecrit["SIREN corrigé"]["rich_text"][0]["text"]["content"])["avant"] == "507807949"


def test_relire_fiche_insee_meme_siren(monkeypatch):
    c, ecrit = _client(monkeypatch, siren_actuel="443061841")
    r = c.post(f"/v3/validation/api/siren/{PAGE}", json={"siren": "443061841", "relire": True})
    assert r.status_code == 200, r.text
    corr = json.loads(ecrit["SIREN corrigé"]["rich_text"][0]["text"]["content"])
    assert corr["avant"] == "443061841" and corr["insee"]["raison_sociale"] == "POISCAILLE"
    assert corr["insee"]["action"].startswith("🔄 Fiche INSEE relue")


def test_insee_sature_puis_repond(monkeypatch):
    import asyncio
    monkeypatch.setattr(v3, "INSEE_INTERVALLE", 0)
    async def dodo(_):
        pass
    monkeypatch.setattr(v3.asyncio, "sleep", dodo)
    class R:
        def __init__(s, code, j=None): s.status_code, s._j = code, j
        def json(s): return s._j
    reponses = [R(429), R(429), R(200, {"results": [{"siren": "823634910", "nom_complet": "QARE", "etat_administratif": "A",
                                                       "siege": {"adresse": "10 RUE DE PENTHIEVRE 75008 PARIS"}}]})]
    class C:
        async def get(s, url, params=None): return reponses.pop(0)
    i = asyncio.run(v3.insee(C(), "823634910"))
    assert i["etat"] == "active" and i["raison_sociale"] == "QARE"


def test_message_insee_injoignable():
    st, act, conf = v3._statut({"siren_crm": "823634910", "siren_site": "", "mode": "SIREN déjà connu", "etat_crm": "erreur API"})
    assert "injoignable" in act
