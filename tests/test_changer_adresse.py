"""Adresse cochée changée : début de bail et opérateur tirés de SIRENE recalculés. Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_sirene as S, v3_validation as VALID

PAGE = "e" * 32
PLATONIA = {"page": PAGE, "siren": "909395147", "nom": "Platonia Games", "flexoffice": True,
            "debut_bail": "2022-01-12", "source_bail": "SIRENE : siège créé à cette adresse le 12/01/2022",
            "operateur_flex": "Reflex", "source_flex": "SIRENE : centre Reflex à la même adresse (91 RUE DU FAUBOURG SAINT-HONORE 75008 PARIS)"}


def _monter(monkeypatch, sirene):
    ecrit = {}
    async def maj(p, props):
        ecrit.update(props); return {"ok": True}
    async def colonne(nom, genre="rich_text"):
        return True
    async def analyser(api, siren, nom, adresse):
        return sirene.get(adresse, {"date": "", "siege": "", "operateur": "", "indice": ""})
    monkeypatch.setattr(VALID, "_maj", maj); monkeypatch.setattr(VALID, "assurer_colonne", colonne)
    monkeypatch.setattr(S, "analyser", analyser)
    return ecrit


def _txt(v):
    return v["rich_text"][0]["text"]["content"] if v.get("rich_text") else ""


def test_platonia_crm_sans_rien_dans_sirene(monkeypatch):
    ecrit = _monter(monkeypatch, {})
    r = asyncio.run(S._relire_adresse(PAGE, dict(PLATONIA), "25 rue du Petit Musc 75004", "", True))
    assert ecrit["Début de bail"] == {"date": None} and _txt(ecrit["Opérateur flex"]) == ""
    assert r["etat"] == {"debut_bail": "", "source_bail": "", "operateur_flex": "", "source_flex": ""}
    assert "Flexoffice" not in ecrit                                   # la case n'est jamais touchée


def test_nouvelle_adresse_avec_un_autre_centre(monkeypatch):
    ecrit = _monter(monkeypatch, {"25 rue du Petit Musc 75004": {"date": "2024-05-02", "siege": "non",
                                                                 "operateur": "Morning", "indice": "centre Morning à la même adresse"}})
    r = asyncio.run(S._relire_adresse(PAGE, dict(PLATONIA), "25 rue du Petit Musc 75004", "", True))
    assert r["etat"]["debut_bail"] == "2024-05-02" and r["etat"]["operateur_flex"] == "Morning"
    assert "secondaire" in _txt(ecrit["Source début de bail"])


def test_saisies_a_la_main_gardees(monkeypatch):
    ecrit = _monter(monkeypatch, {})
    info = {**PLATONIA, "source_bail": "saisie à la main", "operateur_flex": "(aucun)", "source_flex": "retiré à la main"}
    r = asyncio.run(S._relire_adresse(PAGE, info, "25 rue du Petit Musc 75004", "", True))
    assert not ecrit and r["bail_garde"] and r["etat"]["operateur_flex"] == "(aucun)"


def test_nego_voit_sans_enregistrer(monkeypatch):
    ecrit = _monter(monkeypatch, {})
    r = asyncio.run(S._relire_adresse(PAGE, dict(PLATONIA), "25 rue du Petit Musc 75004", "", False))
    assert not ecrit and r["etat"]["operateur_flex"] == "Reflex"
