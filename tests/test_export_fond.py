"""Export HubSpot préparé en tâche de fond (des centaines de fiches Notion : plusieurs minutes). Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient
import v3, v3_export as EX, v3_validation as VALID


def _monter(monkeypatch):
    monkeypatch.setattr(VALID, "_refus", lambda req, besoin: None)
    async def donnees(suivi=None):
        if suivi is not None:
            suivi.update(total=2, fait=2)
        return []
    monkeypatch.setattr(EX, "_donnees", donnees)
    monkeypatch.setattr(EX, "construire", lambda socs: (b"XLSX", {"societes": 0, "contacts": 0}))
    monkeypatch.setattr(EX, "_lancer", lambda mail: None)          # la tâche de fond est testée à part
    EX.EXPORT.update(actif=False, xl=None, pret_le=0.0, erreur="", mail=False)
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app)


def test_page_d_attente_puis_fichier(monkeypatch):
    c = _monter(monkeypatch)
    r = c.get("/v3/validation/api/export")
    assert r.headers["content-type"].startswith("text/html") and "Préparation" in r.text
    asyncio.run(EX._preparer())
    assert EX.EXPORT["xl"] == b"XLSX" and not EX.EXPORT["actif"]
    r = c.get("/v3/validation/api/export")
    assert r.content == b"XLSX" and "attachment" in r.headers["content-disposition"]
    assert c.get("/v3/validation/api/export/etat").json()["pret"]


def test_erreur_affichee(monkeypatch):
    c = _monter(monkeypatch)
    async def panne(suivi=None):
        raise RuntimeError("mémoire Notion inaccessible (502)")
    monkeypatch.setattr(EX, "_donnees", panne)
    asyncio.run(EX._preparer())
    assert "502" in c.get("/v3/validation/api/export").text


def test_fiche_bloquee_sautee(monkeypatch):
    import v3_memoire as MEM
    monkeypatch.setattr(EX, "LECTURE_MAX", 0.2)
    async def toutes():
        return [{"page": p, "nom": n, "validation": "Société validée", "nouveaux": "{}", "decisions": "[]"} for p, n in (("a", "Botify"), ("b", "Bloquée"))]
    async def lire(p):
        if p == "b":
            await asyncio.sleep(5)
        return {"id": "1"}
    monkeypatch.setattr(VALID, "_toutes", toutes); monkeypatch.setattr(MEM, "lire", lire)
    monkeypatch.setattr(VALID, "appliquer_correction_siren", lambda s: None)
    suivi = {}
    socs = asyncio.run(EX._donnees(suivi))
    assert suivi["fait"] == 2 and suivi["sautees"] == ["Bloquée (TimeoutError)"] and socs[1]["data"] == {}
