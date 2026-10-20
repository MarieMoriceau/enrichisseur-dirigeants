"""Phantom des dirigeants en panne (20/10 : « Sales Navigator account not set up ») et mémoire à zéro dirigeant.
Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest

import v3, v3_linkedin as LI, v3_memoire as MEM


def test_memoire_zero_dirigeant_pas_reprise():
    idx = [{"page": "p2", "nom": "alan", "siren": "443061841", "domaine": "", "perim": "tout", "nb_dir": 0},
           {"page": "p1", "nom": "alan", "siren": "443061841", "domaine": "", "perim": "tout", "nb_dir": 3}]
    o = {"nom": "Alan", "siren_crm": "443061841", "site_crm": ""}
    assert MEM.chercher(idx, o, "tout", v3.norm_nom, v3.domaines)["page"] == "p1"          # l'ancienne, avec dirigeants
    assert MEM.chercher(idx[:1], o, "tout", v3.norm_nom, v3.domaines) == {}                # à refaire
    assert MEM.chercher(idx[:1], o, "societes", v3.norm_nom, v3.domaines)["page"] == "p2"  # adresse seule : reprise OK


class FauxPB:
    """Simule le phantom Sales Navigator : en erreur à chaque lot, ou en état de marche."""
    def __init__(self, panne):
        self.panne, self.appels = panne, 0
    async def executer(self, c, agent, nom, stop, **kw):
        self.appels += 1
        self.dernier_statut = "error" if self.panne else "finished"
        self.derniere_sortie = "Sales Navigator account not set up" if self.panne else ""
        return []


def _job(n):
    return {"id": "j", "stop": False, "perimetre": "tout", "origine": "outils", "resultats": [
        {"nom": f"S{i}", "linkedin_id": str(1000 + i), "linkedin": f"https://www.linkedin.com/company/s{i}",
         "source_linkedin": "site web", "effectif_linkedin": 20} for i in range(n)]}


def _faux_pb(panne, appels):
    class Faux:
        def __init__(self, journal):
            pass
        async def executer(self, c, agent, nom, stop, **kw):
            appels.append(nom)
            sn = "salesnav" in nom
            self.dernier_statut = "error" if (sn and panne) else "finished"
            self.derniere_sortie = "Sales Navigator account not set up" if (sn and panne) else ""
            return []
    return Faux


def _phantoms(monkeypatch):
    for k in ("PB_SCRAPER", "PB_SCRAPER_NEGOS"):
        monkeypatch.setattr(LI, k, "sc", raising=False)
    for k in ("PB_SALESNAV", "PB_SALESNAV_NEGOS"):
        monkeypatch.setattr(LI, k, "sn", raising=False)
    monkeypatch.setattr(LI, "PIPEDRIVE_KEY", "", raising=False)


def test_panne_arrete_le_lancement(monkeypatch):
    _phantoms(monkeypatch)
    appels = []
    monkeypatch.setattr(LI, "PhantomBuster", _faux_pb(True, appels))
    job = _job(60)                                                    # 5 lots de 12 sociétés
    with pytest.raises(LI.PhantomEnPanne) as e:
        asyncio.run(LI.etapes_linkedin(job, v3.ressemblance, v3.domaine))
    assert sum("salesnav" in a for a in appels) == LI.SN_ECHECS_MAX and "not set up" in str(e.value)


def test_lots_vides_sans_panne_continuent(monkeypatch):
    _phantoms(monkeypatch)
    appels = []
    monkeypatch.setattr(LI, "PhantomBuster", _faux_pb(False, appels))
    job = _job(30)
    job["perimetre"] = "dirigeants"                                   # pas d'e-mails : on s'arrête après les dirigeants
    asyncio.run(LI.etapes_linkedin(job, v3.ressemblance, v3.domaine))
    assert sum("salesnav" in a for a in appels) == 3                  # les 3 lots passent


def test_motif_et_erreur():
    job = {"erreur": "le phantom Sales Navigator (dirigeants) échoue à chaque lot : x", "stop": False}
    assert v3.motif_arret(job).startswith("erreur : le phantom Sales Navigator")
