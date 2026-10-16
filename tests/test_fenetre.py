"""Tests du créneau des négociateurs (heure de Paris), sans attendre 9 h.
Lancer : python3 -m pytest tests/  (ou : python3 tests/test_fenetre.py)"""
import os, sys
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_planning as PLAN

PARIS = ZoneInfo("Europe/Paris")
UTC = timezone.utc


def p(*a):
    return datetime(*a, tzinfo=PARIS)


def test_semaine():
    lundi = (2026, 10, 19)
    assert not PLAN.dans_fenetre(p(*lundi, 8, 59, 59))
    assert PLAN.dans_fenetre(p(*lundi, 9, 0))
    assert PLAN.dans_fenetre(p(*lundi, 18, 59))
    assert not PLAN.dans_fenetre(p(*lundi, 19, 0))
    assert not PLAN.dans_fenetre(p(*lundi, 7, 0))


def test_week_end_libre():
    assert not PLAN.dans_fenetre(p(2026, 10, 17, 11, 0))   # samedi
    assert not PLAN.dans_fenetre(p(2026, 10, 18, 15, 0))   # dimanche


def test_heure_utc_de_render_ete_et_hiver():
    # Render donne l'heure en UTC : 9 h à Paris = 7 h UTC l'été, 8 h UTC l'hiver
    assert PLAN.dans_fenetre(datetime(2026, 10, 16, 7, 0, tzinfo=UTC))       # vendredi d'été (UTC+2) → 9 h Paris
    assert not PLAN.dans_fenetre(datetime(2026, 10, 16, 6, 59, tzinfo=UTC))
    assert not PLAN.dans_fenetre(datetime(2026, 11, 3, 7, 30, tzinfo=UTC))   # mardi d'hiver (UTC+1) → 8 h 30 Paris
    assert PLAN.dans_fenetre(datetime(2026, 11, 3, 8, 0, tzinfo=UTC))        # → 9 h Paris


def test_changements_d_heure():
    # passage à l'heure d'hiver : dimanche 25/10/2026 ; le lundi 26, 9 h Paris = 8 h UTC
    assert PLAN.dans_fenetre(datetime(2026, 10, 26, 8, 0, tzinfo=UTC))
    assert not PLAN.dans_fenetre(datetime(2026, 10, 26, 7, 30, tzinfo=UTC))
    # passage à l'heure d'été : dimanche 29/03/2026 ; le lundi 30, 9 h Paris = 7 h UTC
    assert PLAN.dans_fenetre(datetime(2026, 3, 30, 7, 0, tzinfo=UTC))
    assert not PLAN.dans_fenetre(datetime(2026, 3, 30, 6, 30, tzinfo=UTC))


def test_simulation():
    PLAN.simuler("2026-10-19T08:58")
    try:
        assert not PLAN.dans_fenetre()
        assert PLAN.maintenant().strftime("%H:%M") == "08:58"
        PLAN._decalage += timedelta(minutes=2)              # deux minutes plus tard
        assert PLAN.dans_fenetre()
    finally:
        PLAN.simuler("off")
    assert abs(PLAN.maintenant() - datetime.now(PARIS)) < timedelta(seconds=2)


def test_prochaine_bascule():
    assert PLAN.prochaine_bascule(p(2026, 10, 16, 20, 0)) == p(2026, 10, 19, 9, 0)   # vendredi soir → lundi 9 h
    assert PLAN.prochaine_bascule(p(2026, 10, 19, 6, 0)) == p(2026, 10, 19, 9, 0)


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f()
            print("ok ", n)
