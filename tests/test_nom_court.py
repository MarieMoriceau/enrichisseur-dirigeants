"""Nom sans mots de métier pour SIRENE et le URL Finder (« M141 production » → « m141 »). Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3


def test_nom_court():
    assert v3.nom_court("M141 production") == "m141"
    assert v3.nom_court("Sombrero films") == "sombrero"
    assert v3.nom_court("Gibert avocats") == "gibert"
    assert v3.nom_court("Cabinet Perazzone Brun") == "perazzone brun"
    assert v3.nom_court("Botify") == ""                     # rien à retirer : pas de second essai
    assert v3.nom_court("Production") == ""                 # plus rien de distinctif


class _R:
    def __init__(self, res): self.status_code, self._r = 200, res
    def json(self): return {"results": self._r}


def test_insee_par_nom_m141(monkeypatch):
    vus = []
    async def get(api, p):
        vus.append(p["q"])
        if p["q"] == "m141":
            return _R([{"siren": "807572458", "nom_raison_sociale": "M141 DIGITAL LAB", "etat_administratif": "A"},
                       {"siren": "439405218", "nom_raison_sociale": "M141", "etat_administratif": "A"}])
        return _R([])
    monkeypatch.setattr(v3, "_insee_get", get)
    r = asyncio.run(v3.insee_par_nom(None, "M141 production", "75002"))
    assert r["siren"] == "439405218" and r["nb"] == 2 and vus[:2] == ["M141 production", "M141 production"]


def test_mot_de_metier_au_milieu_garde():
    assert v3.nom_court("Les films du losange") == ""
    assert v3.nom_court("Agence Bold avocats") == "bold"
