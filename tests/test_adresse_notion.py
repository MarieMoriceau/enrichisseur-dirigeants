"""Adresse Notion (prospection hebdo) et rattrapage « Lancé par »."""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_linkedin as LI
import v3 as V
import v3_memoire as MEM


def test_notion_prioritaire():
    r = {"adresse_crm": "1 rue de la Paix 75002 Paris", "adresse_notion": "12 avenue de l'Opéra 75001 Paris",
         "adresse_google": "12 av. de l'Opéra, 75001 Paris"}
    d = LI._decider_adresse(r)
    assert d["adresse_proposee"] == r["adresse_notion"] and d["confiance_adresse"] == "haute"
    assert d["sources_adresse"].startswith("Notion") and "corriger le CRM" in d["action_adresse"]
    assert "Notion" not in LI._decider_adresse({"adresse_crm": "1 rue de la Paix 75002 Paris"})["sources_adresse"]


def test_api_adresse_notion():
    o = V._org_api({"nom": "Acme", "adresse": "1 rue X 75002 Paris", "adresse_notion": "5 rue Y 92100 Boulogne"})
    assert o["adresse_notion"] == "5 rue Y 92100 Boulogne" and o["adresse_crm"] == "1 rue X 75002 Paris" and o["cp"] == "92100"
    assert "adresse_notion" not in V._org_api({"nom": "Acme"})


def test_attribuer_anciens(monkeypatch):
    idx = [{"page": "p1", "nom": "a", "date": "2026-10-10", "lance_par": ""},
           {"page": "p2", "nom": "b", "date": "2026-10-12", "lance_par": "x@y.fr"},
           {"page": "p3", "nom": "c", "date": "2026-10-17", "lance_par": ""},
           {"page": "p4", "nom": "d", "date": "2026-09-01", "lance_par": ""}]
    ecrits = []
    async def index(j, tout=False): return idx
    class R: status_code = 200
    async def req(c, m, url, **kw): ecrits.append(url.rsplit("/", 1)[-1]); return R()
    monkeypatch.setattr(MEM, "index", index); monkeypatch.setattr(MEM, "_req", req)
    r = asyncio.run(MEM.attribuer_anciens("m@e.fr", "2026-10-16", False, 50))
    assert r["sans_lance_par"] == 2 and r["modifiees"] == 0 and not ecrits          # essai à blanc
    r = asyncio.run(MEM.attribuer_anciens("m@e.fr", "2026-10-16", True, 1))
    assert r["modifiees"] == 1 and r["reste_apres"] == 1 and ecrits == ["p1"]
