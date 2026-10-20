"""« Base quali » : lue dans Pipedrive (ou le fichier), modifiable dans l'écran société, exportée. Lancer : python3 -m pytest tests/"""
import asyncio, io, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient

import v3, v3_validation as VALID, v3_memoire as MEM

PAGE = "c" * 32
CHAMPS = {"ok": True, "base_quali": "abc123", "base_quali_options": {"12": "Qualifiée", "13": "À qualifier", "14": "Hors cible"}}


def test_valeur_liste_deroulante_et_texte():
    assert v3.base_quali_de({"abc123": "12"}, CHAMPS) == "Qualifiée"
    assert v3.base_quali_de({"abc123": "12,14"}, CHAMPS) == "Qualifiée, Hors cible"     # choix multiple
    assert v3.base_quali_de({"abc123": None}, CHAMPS) == ""
    assert v3.base_quali_de({"abc123": " bonne cible "}, {"base_quali": "abc123"}) == "bonne cible"   # texte libre
    assert v3.base_quali_de({"x": "1"}, {}) == ""                                          # champ absent de Pipedrive


def test_colonne_fichier_et_api():
    csv = "Organisation - Nom;Organisation - SIREN;Organisation - Base quali\nAlan;443061841;Qualifiée\n"
    orgs = v3.lire_fichier("export.csv", csv.encode())
    assert orgs[0]["base_quali_crm"] == "Qualifiée"
    assert v3._org_api({"nom": "Alan", "base_quali": "À qualifier"})["base_quali_crm"] == "À qualifier"


def _client(monkeypatch, retenue=""):
    ecrit = {}
    async def une(p):
        return {"page": p, "base_quali": retenue}
    async def lire(p):
        return {"id": "4521", "base_quali_crm": "À qualifier"}
    async def maj(p, props):
        ecrit.update(props); return {"ok": True}
    async def champs(api):
        return CHAMPS
    async def en_direct(api, org_id):
        return "Qualifiée"
    async def colonne(nom):
        return True
    monkeypatch.setattr(VALID, "_une", une); monkeypatch.setattr(MEM, "lire", lire); monkeypatch.setattr(VALID, "_maj", maj)
    monkeypatch.setattr(v3, "_champs_pipedrive", champs); monkeypatch.setattr(v3, "base_quali_pipedrive", en_direct)
    monkeypatch.setattr(v3, "PIPEDRIVE_KEY", "x"); monkeypatch.setattr(VALID, "assurer_colonne", colonne)
    monkeypatch.setattr(VALID, "_refus", lambda req, besoin: None)
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app), ecrit


def test_lire_et_modifier(monkeypatch):
    c, ecrit = _client(monkeypatch)
    j = c.get(f"/v3/validation/api/base-quali/{PAGE}").json()
    assert j["crm"] == "Qualifiée" and j["retenue"] == "" and j["options"] == ["Qualifiée", "À qualifier", "Hors cible"]
    r = c.post(f"/v3/validation/api/base-quali/{PAGE}", json={"valeur": "  Hors   cible "})
    assert r.json()["retenue"] == "Hors cible" and ecrit["Base quali"]["rich_text"][0]["text"]["content"] == "Hors cible"
