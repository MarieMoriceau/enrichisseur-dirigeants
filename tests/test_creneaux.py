"""Tests des créneaux LinkedIn (heure de Paris), sans attendre 7 h ni 9 h.
Lancer : python3 -m pytest tests/"""
import os, sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_planning as PLAN

PARIS, UTC = ZoneInfo("Europe/Paris"), timezone.utc


def p(*a):
    return datetime(*a, tzinfo=PARIS)


def test_creneaux():
    j = (2026, 10, 19)
    assert not PLAN.dans_creneau("outils", p(*j, 6, 59))
    assert PLAN.dans_creneau("outils", p(*j, 7, 0)) and PLAN.dans_creneau("outils", p(*j, 8, 59, 59))
    assert not PLAN.dans_creneau("outils", p(*j, 9, 0))
    assert not PLAN.dans_creneau("negos", p(*j, 8, 59)) and PLAN.dans_creneau("negos", p(*j, 9, 0))
    assert PLAN.dans_creneau("negos", p(*j, 18, 59)) and not PLAN.dans_creneau("negos", p(*j, 19, 0))
    assert PLAN.dans_creneau("negos", p(2026, 10, 17, 11, 0))          # samedi : mêmes règles tous les jours


def test_render_en_utc():
    # 5 h UTC = 7 h à Paris en été, 6 h à Paris en hiver
    assert PLAN.dans_creneau("outils", datetime(2026, 7, 1, 5, 0, tzinfo=UTC))
    assert not PLAN.dans_creneau("outils", datetime(2026, 12, 1, 5, 0, tzinfo=UTC))
    assert PLAN.dans_creneau("outils", datetime(2026, 12, 1, 6, 0, tzinfo=UTC))
    assert PLAN.dans_creneau("negos", datetime(2026, 12, 1, 8, 0, tzinfo=UTC))   # 9 h Paris en hiver


def test_changement_d_heure():
    # nuit du 24 au 25/10/2026 : passage à l'heure d'hiver. 7 h reste 7 h à Paris.
    d = PLAN.prochain_debut("outils", p(2026, 10, 24, 20, 0))
    assert (d.day, d.hour, d.minute) == (25, 7, 0) and d.astimezone(UTC).hour == 6
    d = PLAN.prochain_debut("negos", p(2026, 3, 28, 20, 0))             # passage à l'heure d'été
    assert (d.day, d.hour) == (29, 9) and d.astimezone(UTC).hour == 7


def test_prochain_debut():
    assert PLAN.prochain_debut("negos", p(2026, 10, 19, 8, 0)) == p(2026, 10, 19, 9, 0)    # aujourd'hui
    assert PLAN.prochain_debut("negos", p(2026, 10, 19, 9, 30)) == p(2026, 10, 20, 9, 0)   # demain
    assert PLAN.prochain_debut("outils", p(2026, 10, 19, 6, 0)) == p(2026, 10, 19, 7, 0)
    assert PLAN.prochain_debut("outils", p(2026, 10, 19, 8, 0)) == p(2026, 10, 20, 7, 0)


def test_simulation():
    try:
        PLAN.simuler("08:58")
        assert PLAN.etat()["creneau_ouvert"] == "outils"
        PLAN.simuler("09:00")
        assert PLAN.etat()["creneau_ouvert"] == "negos"
        PLAN.simuler("2026-10-19T19:30")
        assert PLAN.etat()["creneau_ouvert"] == "" and PLAN.etat()["simulee"]
    finally:
        PLAN.simuler("off")
    assert not PLAN.etat()["simulee"]


def test_detection():
    assert PLAN.detecter_origine("prospection_20261015_1012_500m.xlsx",
                                 ["Synthèse", "Contacts (détail)", "Récap sociétés AVEC contacts", "Sociétés SANS contact"],
                                 ["Indicateur", "Valeur"]) == ("outils", "géolocalisation")
    assert PLAN.detecter_origine("2_effectifs_a_chercher_lundi.xlsx", ["À envoyer"],
                                 ["Société", "Requête envoyée", "CP"]) == ("outils", "prospection hebdo")
    assert PLAN.detecter_origine("liste.xlsx", ["À envoyer"], ["Société", "Requête envoyée"])[0] == "outils"
    assert PLAN.detecter_origine("export_pipedrive.xlsx", ["Feuille1"], ["Organisation - Nom"]) == ("negos", "")
    assert PLAN.detecter_origine("liste.csv", [], [])[0] == "negos"
