"""Corriger le nom d'une société, la relancer, fiche remplacée. Lancer : python3 -m pytest tests/"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient

import v3, v3_validation as VALID, v3_memoire as MEM

PAGE = "e" * 32


def _client(monkeypatch):
    ecrit, lances = {}, []
    async def maj(p, props):
        ecrit.update(props); return {"ok": True}
    async def une(p):
        return {"page": p, "nom": "Godin Associés", "siren": "431560531", "siren_corrige": json.dumps({"avant": "838232098"})}
    async def lire(p):
        return {"id": "", "nom": "Godim", "siren_crm": "", "cp": "75002", "ville": "Paris",
                "adresse_notion": "12 Rue du 4 Septembre, 75002 Paris", "infos_source": {"nego": "Richard"}, "siren_site": "838232098"}
    def job(nom, mails, perim, orgs, forcer=False, origine=""):
        lances.append((nom, perim, orgs, forcer, origine)); v3.JOBS["x1"] = {"id": "x1", "nom": nom, "journal": [], "etat": "en cours"}; return "x1"
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "_une", une); monkeypatch.setattr(MEM, "lire", lire)
    monkeypatch.setattr(v3, "nouveau_job", job); monkeypatch.setattr(v3, "JOBS", {})
    monkeypatch.setattr(VALID, "_refus", lambda req, besoin: None); monkeypatch.setattr(VALID, "utilisateur", lambda req: "m@x.fr")
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app), ecrit, lances


def test_corriger_nom(monkeypatch):
    c, ecrit, _ = _client(monkeypatch)
    r = c.post(f"/v3/validation/api/nom/{PAGE}", json={"nom": "  Godin   Associés "})
    assert r.json()["nom"] == "Godin Associés"
    assert ecrit["Société"]["title"][0]["text"]["content"] == "Godin Associés"
    assert ecrit["Nom normalisé"]["rich_text"][0]["text"]["content"] == v3.norm_nom("Godin Associés")
    assert c.post(f"/v3/validation/api/nom/{PAGE}", json={"nom": "x"}).status_code == 400


def test_relancer_avec_nom_et_siren_corriges(monkeypatch):
    c, _, lances = _client(monkeypatch)
    j = c.get(f"/v3/validation/api/relancer-societes?pages={PAGE}").json()
    assert j["essai_a_blanc"] and j["societes"] == [{"nom": "Godin Associés", "siren": "431560531"}] and not lances
    r = c.post(f"/v3/validation/api/relancer/{PAGE}", json={}).json()
    nom, perim, orgs, forcer, origine = lances[0]
    assert r["lance"] == "x1" and perim == "tout" and forcer and origine == "outils"
    assert orgs[0]["nom"] == "Godin Associés" and orgs[0]["siren_crm"] == "431560531"
    assert orgs[0]["adresse_notion"].startswith("12 Rue") and "siren_site" not in orgs[0]     # l'ancien faux SIREN ne repart pas
    assert c.post(f"/v3/validation/api/relancer/{PAGE}", json={}).status_code == 409          # un run tourne déjà


def test_fiche_remplacee(monkeypatch):
    import asyncio
    class R:
        status_code = 400
        text = '{"message":"Can not edit block that is archived."}'
    async def req(c, m, url, **kw):
        return R()
    monkeypatch.setattr(MEM, "_req", req)
    r = asyncio.run(VALID._maj(PAGE, {}))
    assert r.status_code == 409 and "remplacée" in json.loads(r.body)["erreur"]
