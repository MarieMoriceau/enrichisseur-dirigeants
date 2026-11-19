"""19/11 : les suites suspendues de 4103 bloquaient le quota de chaque jour, la relance du 17/11 était repoussée au 6/12.
Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import v3_suivi as SUIVI
import v3_planning as PLAN


def _listes():
    return ([{"page": "rel", "nom": "Prosp terrain 17/11/26 (relance) (suite 1/1)", "statut": "EN_ATTENTE",
              "depart": "2026-12-06T08:00", "nb": 21, "erreur": ""}] +
            [{"page": f"s{i}", "nom": f"organizations-1702184-4103 (suite {i}/27)", "statut": "SUSPENDU",
              "depart": f"2026-11-{16 + i}T08:00", "nb": 150 if i < 27 else 71, "erreur": "Suspendu"} for i in range(8, 28)])


def test_replanifier_relance_aujourd_hui_puis_une_suite_par_jour(monkeypatch):
    vus = []
    async def utilise(d, c, exclure=None):
        vus.append(exclure)
        return 0
    monkeypatch.setattr(SUIVI, "utilise_compte", utilise)
    monkeypatch.setattr(PLAN, "maintenant", lambda: datetime(2026, 11, 19, 12, 30, tzinfo=SUIVI.PARIS))
    plan = asyncio.run(SUIVI.replanifier(_listes(), PLAN.OUTILS))
    assert plan[0]["nouveau_depart"] == "2026-11-19T12:30+01:00"          # relance : tout de suite
    assert plan[1]["nouveau_depart"] == "2026-11-20T08:00+01:00"          # 150 + 21 ne tient pas le 19
    assert plan[2]["nouveau_depart"] == "2026-11-21T08:00+01:00"
    assert plan[-1]["nouveau_depart"] == "2026-12-09T08:00+01:00"         # suite 27/27 (71) : 20 jours plus tard
    assert all(e == {x["page"] for x in _listes()} for e in vus)          # les listes déplacées ne se comptent pas


def test_un_jour_deja_entame(monkeypatch):
    async def utilise(d, c, exclure=None):
        return 140 if d.day == 19 else 0
    monkeypatch.setattr(SUIVI, "utilise_compte", utilise)
    monkeypatch.setattr(PLAN, "maintenant", lambda: datetime(2026, 11, 19, 12, 30, tzinfo=SUIVI.PARIS))
    plan = asyncio.run(SUIVI.replanifier(_listes()[:2], PLAN.OUTILS))
    assert plan[0]["nouveau_depart"].startswith("2026-11-20T08:00")
    assert plan[1]["nouveau_depart"].startswith("2026-11-21T08:00")


def test_quota_ne_compte_pas_les_suspendues(monkeypatch):
    corps = []
    class R:
        status_code = 200
        def json(self):
            return {"results": [{"id": "aa-bb", "properties": {"Nb LinkedIn": {"type": "number", "number": 21}}},
                                {"id": "cc", "properties": {"Nb LinkedIn": {"type": "number", "number": 30}}}], "has_more": False}
    async def n(c, meth, url, **k):
        corps.append(k["json"])
        return R()
    monkeypatch.setattr(SUIVI, "NOTION_KEY", "k")
    monkeypatch.setattr(SUIVI, "_n", n)
    total = asyncio.run(SUIVI.utilise_le(datetime(2026, 11, 19, tzinfo=SUIVI.PARIS), "outils", {"aabb"}))
    assert total == 30
    filtres = corps[0]["filter"]["and"]
    assert {"property": "Statut", "select": {"does_not_equal": "SUSPENDU"}} in filtres


def test_sans_memoire_suit_les_lots_reportes(monkeypatch):
    import v3
    programmes = []
    async def planifier(reste, origine):
        return [(datetime(2026, 11, 20, 8, tzinfo=SUIVI.PARIS), reste)]
    async def programmer(nom, perimetre, mails, lot, depart, origine):
        programmes.append(lot)
    monkeypatch.setattr(SUIVI, "planifier_suite", planifier)
    monkeypatch.setattr(SUIVI, "programmer", programmer)
    job = {"id": "x", "nom": "Prosp terrain 17/11/26 (relance)", "perimetre": "tout", "forcer": True, "origine": "outils"}
    asyncio.run(v3._programmer_reste(job, [{"nom": "Fideliz"}, {"nom": "Webgains"}]))
    assert all(o["sans_memoire"] for o in programmes[0])
