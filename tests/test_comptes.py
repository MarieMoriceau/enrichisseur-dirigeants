"""Tests des 2 comptes LinkedIn (automatisations / négos), du quota par compte et de l'heure de reprise.
Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_planning as PLAN

PARIS, UTC = ZoneInfo("Europe/Paris"), timezone.utc


def p(*a):
    return datetime(*a, tzinfo=PARIS)


def _deux(monkeypatch, oui=True):
    for k in ("PB_COMPANY_SCRAPER_ID_NEGOS", "PB_SALESNAV_ID_NEGOS"):
        if oui:
            monkeypatch.setenv(k, "123")
        else:
            monkeypatch.delenv(k, raising=False)


def test_comptes(monkeypatch):
    _deux(monkeypatch, False)
    assert not PLAN.deux_comptes() and PLAN.comptes() == ["unique"]
    assert PLAN.compte("outils") == PLAN.compte("negos") == "unique"
    _deux(monkeypatch)
    assert PLAN.deux_comptes() and PLAN.comptes() == ["outils", "negos"]
    assert PLAN.compte("outils") == "outils" and PLAN.compte("negos") == "negos" and PLAN.compte("") == "negos"
    monkeypatch.delenv("PB_SALESNAV_ID_NEGOS")                   # un seul des deux phantoms : pas encore 2 comptes
    assert not PLAN.deux_comptes()


def test_phantoms(monkeypatch):
    import v3_linkedin as LI
    monkeypatch.setattr(LI, "PB_SCRAPER", "auto_s"); monkeypatch.setattr(LI, "PB_SALESNAV", "auto_sn")
    monkeypatch.setattr(LI, "PB_PROFILS", "auto_p")
    monkeypatch.setattr(LI, "PB_SCRAPER_NEGOS", "negos_s"); monkeypatch.setattr(LI, "PB_SALESNAV_NEGOS", "negos_sn")
    monkeypatch.setattr(LI, "PB_PROFILS_NEGOS", "")
    _deux(monkeypatch)
    assert LI.phantoms_du_job({"origine": "negos"}) == ("negos_s", "negos_sn", "")   # jamais le Profile Scraper de l'autre compte
    assert LI.phantoms_du_job({"origine": "outils"}) == ("auto_s", "auto_sn", "auto_p")
    _deux(monkeypatch, False)
    assert LI.phantoms_du_job({"origine": "negos"}) == ("auto_s", "auto_sn", "auto_p")   # compte unique


def test_reprise():
    assert PLAN.prochaine_reprise(p(2026, 10, 19, 6, 0)) == p(2026, 10, 19, 8, 0)
    assert PLAN.prochaine_reprise(p(2026, 10, 19, 8, 0)) == p(2026, 10, 20, 8, 0)
    d = PLAN.prochaine_reprise(p(2026, 10, 24, 20, 0))            # passage à l'heure d'hiver : 8 h reste 8 h à Paris
    assert (d.day, d.hour) == (25, 8) and d.astimezone(UTC).hour == 7


def test_simulation():
    try:
        PLAN.simuler("07:58")
        assert PLAN.etat()["simulee"] and PLAN.maintenant().hour == 7
    finally:
        PLAN.simuler("off")
    assert not PLAN.etat()["simulee"]


def _faux_quota(monkeypatch, deja: dict):
    """deja : {(date iso, filtre) : sociétés déjà comptées}."""
    import v3_suivi as SUIVI
    async def utilise_le(d, origine=""):
        return deja.get((d.date().isoformat(), origine), 0)
    monkeypatch.setattr(SUIVI, "utilise_le", utilise_le)
    monkeypatch.setattr(SUIVI, "QUOTA_JOUR", 150)
    return SUIVI


def test_quota_par_compte(monkeypatch):
    _deux(monkeypatch)
    SUIVI = _faux_quota(monkeypatch, {("2026-10-20", "outils"): 150, ("2026-10-20", "negos"): 40})
    try:
        PLAN.simuler("2026-10-19T20:00")
        lots = asyncio.run(SUIVI.planifier_suite(list(range(200)), "negos"))
        assert [(d.day, d.hour, len(l)) for d, l in lots] == [(20, 8, 110), (21, 8, 90)]   # 150 - 40, puis le reste
        lots = asyncio.run(SUIVI.planifier_suite(list(range(10)), "outils"))
        assert [(d.day, len(l)) for d, l in lots] == [(21, 10)]                              # compte automatisations plein le 20
    finally:
        PLAN.simuler("off")


def test_quota_compte_unique(monkeypatch):
    _deux(monkeypatch, False)
    SUIVI = _faux_quota(monkeypatch, {("2026-10-20", ""): 100})
    try:
        PLAN.simuler("2026-10-19T20:00")
        lots = asyncio.run(SUIVI.planifier_suite(list(range(80)), "negos"))
        assert [(d.day, len(l)) for d, l in lots] == [(20, 50), (21, 30)]                     # 150 partagés
    finally:
        PLAN.simuler("off")


def test_detection():
    assert PLAN.detecter_origine("prospection_20261015_1012_500m.xlsx",
                                 ["Synthèse", "Contacts (détail)", "Récap sociétés AVEC contacts", "Sociétés SANS contact"],
                                 ["Indicateur", "Valeur"]) == ("outils", "géolocalisation")
    assert PLAN.detecter_origine("2_effectifs_a_chercher_lundi.xlsx", ["À envoyer"],
                                 ["Société", "Requête envoyée", "CP"]) == ("outils", "prospection hebdo")
    assert PLAN.detecter_origine("export_pipedrive.xlsx", ["Feuille1"], ["Organisation - Nom"]) == ("negos", "")


def test_file_d_attente(monkeypatch):
    """Deux runs sur le même compte : le 2e attend la fin des étapes LinkedIn du 1er. Comptes différents : en parallèle."""
    _deux(monkeypatch)
    import v3 as V
    async def scenario():
        V.FILES.clear()
        jobs = {i: {"id": i, "nom": i, "origine": o, "stop": False, "journal": []}
                for i, o in (("a", "negos"), ("b", "negos"), ("c", "outils"), ("d", "negos"))}
        V.JOBS.update(jobs)
        ordre = []
        async def run(i, duree):
            try:
                if await V.entrer_file(jobs[i]):
                    ordre.append(f"{i}+")
                    await asyncio.sleep(duree)
                    ordre.append(f"{i}-")
                else:
                    ordre.append(f"{i} arrêté")
            finally:
                V.sortir_file(jobs[i])
        monkeypatch.setattr(V.asyncio, "sleep", _rapide)
        t = [asyncio.create_task(run("a", 60)), asyncio.create_task(run("b", 1)), asyncio.create_task(run("c", 1)),
             asyncio.create_task(run("d", 1))]
        await _vrai_sleep(0.01)
        assert "En file d'attente" in jobs["b"]["phase"] and "« a »" in jobs["b"]["phase"]
        jobs["d"]["stop"] = True                           # arrêté pendant l'attente : ne part jamais sur LinkedIn
        await asyncio.gather(*t)
        return ordre
    ordre = asyncio.run(scenario())
    assert ordre.index("c+") < ordre.index("a-")           # compte automatisations : n'attend pas les négos
    assert ordre.index("a-") < ordre.index("b+")           # même compte : l'un après l'autre
    assert "d arrêté" in ordre and "d+" not in ordre


_vrai_sleep = asyncio.sleep


async def _rapide(s):
    await _vrai_sleep(s / 1000)


def test_plafond_avocats():
    import v3_linkedin as LI
    d = lambda nom, role, idf=True: {"nom_complet": nom, "role": role, "idf": idf}
    associes = [d(f"Associé {i}", "5 Associé / Gérant", i % 2 == 0) for i in range(9)]
    r = {"naf": "69.10Z", "dirigeants": associes[:3] + [d("Managing", "1 CEO / Président / DG")] + associes[3:]}
    assert LI.plafonner_dirigeants(r) == 5 and len(r["dirigeants"]) == 5
    noms = [x["nom_complet"] for x in r["dirigeants"]]
    assert "Managing" in noms                                         # le rôle passe avant
    assert all(x["idf"] for x in r["dirigeants"] if x["nom_complet"] != "Managing")   # puis l'Île-de-France
    autre = {"naf": "62.01Z", "dirigeants": associes[:]}
    assert LI.plafonner_dirigeants(autre) == 0 and len(autre["dirigeants"]) == 9      # autres secteurs : inchangé
    assert LI.plafonner_dirigeants({"naf": "6910Z", "dirigeants": associes[:4]}) == 0  # 4 ≤ 5 : rien à faire
    assert LI._plafonds_naf("69.10Z:5, 70.22Z:8,bidon") == {"6910Z": 5, "7022Z": 8}


def test_filet_nom_avocats():
    import v3_linkedin as LI
    neuf = lambda: [{"nom_complet": f"A{i}", "role": "5 Associé / Gérant", "idf": True} for i in range(9)]
    for nom in ("Dupont Avocats", "AARPI Martin & Co", "Gide Loyrette Nouel Law", "Smith Lawyers", "cabinet d'avocat X"):
        r = {"nom": nom, "naf": "", "dirigeants": neuf()}
        assert LI.plafonner_dirigeants(r) == 4, nom
    for nom in ("Lawson Software", "Avocado Tech", "Legal Place"):
        assert LI.plafonner_dirigeants({"nom": nom, "naf": "", "dirigeants": neuf()}) == 0, nom
    # NAF connu et différent : le nom ne compte pas
    assert LI.plafonner_dirigeants({"nom": "Law Consulting", "naf": "70.22Z", "dirigeants": neuf()}) == 0


def test_url_finder_et_google_maps_par_compte(monkeypatch):
    import v3_linkedin as LI
    monkeypatch.setattr(LI, "PB_URL_FINDER", "uf"); monkeypatch.setattr(LI, "PB_GMAPS", "gm")
    monkeypatch.setattr(LI, "PB_URL_FINDER_NEGOS", ""); monkeypatch.setattr(LI, "PB_GMAPS_NEGOS", "")
    _deux(monkeypatch)
    assert LI.phantoms_sans_cookie_du_job({"origine": "negos"}) == ("uf", "gm")        # pas de copie : phantoms communs
    monkeypatch.setattr(LI, "PB_URL_FINDER_NEGOS", "uf_n"); monkeypatch.setattr(LI, "PB_GMAPS_NEGOS", "gm_n")
    assert LI.phantoms_sans_cookie_du_job({"origine": "negos"}) == ("uf_n", "gm_n")
    assert LI.phantoms_sans_cookie_du_job({"origine": "outils"}) == ("uf", "gm")
