"""Représentants SIRENE : profil LinkedIn par recherche web ; page LinkedIn corrigée dans la validation (05/11)."""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient
import v3, v3_linkedin as LI, v3_rattrapage as RAT

CAA = {"nom": "CAA EXPERTISE", "domaine": "caaexpertise.com", "raison_sociale": "CAA EXPERTISE", "siren_site": "822831095"}


def _rep(prenom="Yulai", nom="Ao"):
    return {"prenom": prenom, "nom": nom, "nom_complet": f"{prenom} {nom}", "fonction": "Président de SAS (SIRENE)",
            "role": "1 CEO / Président / DG", "linkedin": "", "email": "", "source": "SIRENE"}


class _R:
    def __init__(self, txt, code=200):
        self.status_code, self._t = code, txt
    def json(self):
        return {"content": [{"type": "text", "text": self._t}]}


class _C:
    def __init__(self, txt, code=200):
        self.txt, self.code, self.vus = txt, code, []
    async def post(self, url, **k):
        self.vus.append(k["json"]["messages"][0]["content"])
        return _R(self.txt, self.code)


def test_page_societe():
    assert LI.page_societe("https://fr.linkedin.com/company/ca-expertises/about/?x=1") == "https://www.linkedin.com/company/ca-expertises/"
    assert LI.page_societe("linkedin.com/company/caa-expertise") == "https://www.linkedin.com/company/caa-expertise/"
    assert LI.page_societe("https://www.linkedin.com/in/yulai-ao") == ""


def test_profil_trouve(monkeypatch):
    monkeypatch.setattr(LI, "ANTHROPIC_KEY", "k")
    c = _C("Voici :\nhttps://fr.linkedin.com/in/yulai-ao-12345/ | Expert-comptable, Président")
    url, titre = asyncio.run(LI.profil_representant(c, _rep(), CAA))
    assert url == "https://www.linkedin.com/in/yulai-ao-12345" and titre == "Expert-comptable, Président"
    assert "822831095" in c.vus[0] and "caaexpertise.com" in c.vus[0] and "Président de SAS" in c.vus[0]


def test_profil_non(monkeypatch):
    monkeypatch.setattr(LI, "ANTHROPIC_KEY", "k")
    assert asyncio.run(LI.profil_representant(_C("NON"), _rep(), CAA)) == ("", "")
    assert asyncio.run(LI.profil_representant(_C("", 500), _rep(), CAA)) == ("", "")


def test_profils_representants_met_a_jour(monkeypatch):
    async def faux(c, d, r):
        return ("https://www.linkedin.com/in/yulai-ao", "Directeur général") if d["nom"] == "Ao" else ("", "")
    monkeypatch.setattr(LI, "profil_representant", faux)
    a, b = _rep(), _rep("Jean", "Inconnu")
    n = asyncio.run(LI.profils_representants(None, [(CAA, a), (CAA, b)]))
    assert n == 1
    assert a["linkedin"].endswith("yulai-ao") and a["fonction"] == "Directeur général" and a["fonction_sirene"].startswith("Président")
    assert a["role"] == "1 CEO / Président / DG" and a["source"].startswith("SIRENE + profil")
    assert not b["linkedin"] and b["profil_cherche"] and b["fonction"] == "Président de SAS (SIRENE)"
    assert LI.representants_sans_profil({"dirigeants": [a, b, _rep("X", "Y")]}) == [_rep("X", "Y")]


def test_representants_deja_decides_pas_touches():
    d1, d2 = _rep(), _rep("Jean", "Dupont")
    s = {"nouveaux": '{"yulai ao": {"etat": "écarté"}}'}
    assert RAT.representants_a_chercher(s, {"dirigeants": [d1, d2]}) == [d2]


def test_route_page_linkedin(monkeypatch):
    ecrit = {}
    async def lire(p):
        return {"nom": "CAA EXPERTISE", "linkedin": "https://linkedin.com/company/ca-expertises"}
    async def reecrire(c, p, d):
        ecrit.update(d)
    monkeypatch.setattr(v3.VALID, "_refus", lambda req, role: None)
    monkeypatch.setattr(v3.MEM, "lire", lire)
    monkeypatch.setattr(RAT, "_reecrire", reecrire)
    app = FastAPI(); app.include_router(v3.router)
    c = TestClient(app)
    p = "0" * 32
    r = c.post(f"/v3/validation/api/page-linkedin/{p}", json={"url": "https://www.linkedin.com/company/caa-expertise/about/"})
    assert r.status_code == 200 and ecrit["linkedin_corrige"] == "https://www.linkedin.com/company/caa-expertise/"
    assert c.post(f"/v3/validation/api/page-linkedin/{p}", json={"url": "https://www.linkedin.com/in/yulai"}).status_code == 400
    ecrit.clear()
    assert c.post(f"/v3/validation/api/page-linkedin/{p}", json={"url": ""}).status_code == 200 and "linkedin_corrige" not in ecrit
    assert "linkedin_corrige" in v3.CHAMPS_ORG[7:]                 # repris par « Relancer cette société »


def test_reponses_inventees_ou_sans_poste(monkeypatch):
    monkeypatch.setattr(LI, "ANTHROPIC_KEY", "k")
    for txt in ("https://www.linkedin.com/in/maxime-peronnin-XXXX` | Directeur général",
                "https://www.linkedin.com/in/...` | Gérant",
                "https://www.linkedin.com/in/gregory-bellaiche-42b780106",                 # pas de poste : pas sûr
                "https://www.linkedin.com/in/f-shoukry-11365810 | Président de SAS (SIRENE)"):
        assert asyncio.run(LI.profil_representant(_C(txt), _rep(), CAA)) == ("", ""), txt
    url, t = asyncio.run(LI.profil_representant(_C("https://www.linkedin.com/in/st%C3%A9phan-constance-3b0658 | Président-fondateur"), _rep(), CAA))
    assert url.endswith("st%C3%A9phan-constance-3b0658") and t == "Président-fondateur"


def test_essai_rouvert_pas_relance(monkeypatch):
    import time
    monkeypatch.setattr(RAT.VALID, "_refus", lambda req, role: None)
    lances = []
    monkeypatch.setattr(RAT, "_tache_profils_sirene", lambda l, c: lances.append(c) or __import__("asyncio").sleep(0))
    RAT.SIRENE_P.update(actif=False, lancement="dossier enrichisseur sl", mode="essai à blanc",
                        phase="essai à blanc terminé : 20 profils trouvés sur 33", _quand=time.time(), trouves=20)
    app = FastAPI(); app.include_router(RAT.router)
    r = TestClient(app).get("/v3/validation/api/profils-sirene?lancement=dossier enrichisseur SL")
    assert r.status_code == 200 and r.json()["trouves"] == 20 and not lances
