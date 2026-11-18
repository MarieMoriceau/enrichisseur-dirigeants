"""Anthropic ou Kaspr en panne de paiement (17/11 : « recherche web en erreur (400) », aucun e-mail) :
le lancement s'arrête net au lieu de rendre un Excel vide. Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import httpx
import pytest

import v3, v3_linkedin as LI


def _rep(code, corps):
    return httpx.Response(code, json=corps, request=httpx.Request("POST", "https://x"))


CREDIT = {"type": "error", "error": {"type": "invalid_request_error",
                                     "message": "Your credit balance is too low to access the Anthropic API."}}


def test_detection_anthropic():
    e = LI.panne_anthropic(_rep(400, CREDIT))
    assert isinstance(e, LI.FournisseurEnPanne) and "credit balance" in str(e) and "400" in str(e)
    assert LI.panne_anthropic(_rep(401, {"error": {"message": "invalid x-api-key"}}))
    assert LI.panne_anthropic(_rep(429, {"error": {"message": "rate limit"}})) is None      # passager
    assert LI.panne_anthropic(_rep(529, {"error": {"message": "overloaded"}})) is None
    assert LI.panne_anthropic(_rep(200, {})) is None


def test_detection_kaspr():
    assert LI.panne_kaspr(_rep(404, {"message": "profile not found"})) is None                 # normal
    assert LI.panne_kaspr(_rep(402, {"message": "payment required"}))
    assert LI.panne_kaspr(_rep(403, {"message": "forbidden"}))
    assert LI.panne_kaspr(_rep(400, {"message": "Insufficient credits"}))
    assert LI.panne_kaspr(_rep(400, {"message": "invalid linkedin id"})) is None
    assert LI.panne_kaspr(_rep(429, {"message": "too many requests"})) is None


def _client(code, corps):
    return httpx.AsyncClient(transport=httpx.MockTransport(lambda req: httpx.Response(code, json=corps)))


def test_recherche_site_leve_la_panne(monkeypatch):
    monkeypatch.setattr(v3, "ANTHROPIC_KEY", "k", raising=False)
    async def go():
        async with _client(400, CREDIT) as c:
            await v3.chercher_domaine(c, "Fideliz", "75009", "Paris")
    with pytest.raises(LI.FournisseurEnPanne):
        asyncio.run(go())
    async def go_429():
        async with _client(429, {"error": {"message": "rate"}}) as c:
            return await v3.chercher_domaine(c, "Fideliz", "75009", "Paris")
    assert asyncio.run(go_429()) == ("", "recherche web en erreur (429)")


def test_kaspr_et_profil_levent_la_panne(monkeypatch):
    monkeypatch.setattr(LI, "KASPR_KEY", "k")
    monkeypatch.setattr(LI, "ANTHROPIC_KEY", "k", raising=False)
    async def kaspr(code, corps):
        async with _client(code, corps) as c:
            return await LI.kaspr_email(c, "Jens Korner", "https://www.linkedin.com/in/jens-korner")
    with pytest.raises(LI.FournisseurEnPanne):
        asyncio.run(kaspr(402, {"message": "Payment required"}))
    assert asyncio.run(kaspr(404, {"message": "not found"})) == ""
    async def profil():
        async with _client(400, CREDIT) as c:
            return await LI.profil_representant(c, {"prenom": "Jens", "nom": "Korner"}, {"nom": "Webgains"})
    with pytest.raises(LI.FournisseurEnPanne):
        asyncio.run(profil())


def test_le_lancement_s_arrete_net(monkeypatch):
    appels = []
    async def crm(api, nom, cp):
        return {}
    async def sirene(api, nom, cp, adresse=""):
        return {}
    async def domaine(api, nom, *a, **k):
        appels.append(nom)
        raise LI.FournisseurEnPanne("Anthropic (recherche web Claude)", 400, "Your credit balance is too low")
    monkeypatch.setattr(v3, "chercher_crm", crm)
    monkeypatch.setattr(v3, "insee_par_nom", sirene)
    monkeypatch.setattr(v3, "chercher_domaine", domaine)
    orgs = [{"nom": f"S{i}", "id": "", "siren_crm": "", "site_crm": "", "adresse_crm": "", "cp": "", "ville": ""} for i in range(30)]
    v3.JOBS["p"] = {"id": "p", "nom": "Prosp terrain 17/11/26", "linkedin": False, "stop": False, "resultats": [], "fait": 0,
                    "journal": [], "perimetre": "tout", "origine": "outils"}
    asyncio.run(v3._traiter("p", orgs))
    job = v3.JOBS.pop("p")
    assert job["stop"] and job["fournisseur_en_panne"].startswith("Anthropic")
    assert job["erreur"] == job["fournisseur_en_panne"]
    assert len(appels) < 30                               # les sociétés suivantes ne partent plus
    assert sum("⛔" in l for l in map(str, job["journal"])) == 1


def test_fiches_du_lancement():
    idx = [{"page": "a", "nom": "fideliz", "lancement": "Prosp terrain 17:11:26"},
           {"page": "b", "nom": "webgains", "lancement": "Base quali ; Prosp terrain 17/11/26 (suite 1/2)"},
           {"page": "c", "nom": "lyreco", "lancement": "Prosp terrain 04:11:26"}]
    assert [e["page"] for e in v3.fiches_du_lancement(idx, "Prosp_terrain_17_11_26")] == ["a", "b"]
    assert v3.fiches_du_lancement(idx, "Prosp terrain 18/11/26") == []
