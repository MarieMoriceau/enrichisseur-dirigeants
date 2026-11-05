"""Adresse normalisée de la base Prospection terrain : lancements et reprise de l'historique."""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_terrain as T


def _t(nom, adresse, norm, id_="", page="p1"):
    return {"nom": nom, "nom_pd": "", "id": id_, "adresse": adresse, "cp": "", "nego": "Sébastien", "date": "2026-10-30",
            "remarques": "", "type": "", "adresse_norm": norm, "page": page}


def test_adresse_normalisee():
    assert T.adresse_normalisee(_t("A", "11, rue du faubourg Poissonnière", "11 Rue du Faubourg Poissonnière, 75009 Paris, France")) \
        == "11 Rue du Faubourg Poissonnière 75009 Paris"
    assert T.adresse_normalisee(_t("B", "40 bis rue du faubourg Poissonnière", "40 Rue du Faubourg Poissonnière, 75010 Paris, France")) \
        == "40 bis Rue du Faubourg Poissonnière 75010 Paris"
    assert T.adresse_normalisee(_t("C", "102 rue Rayon", "102 Rue Rayon, Paris, France")) == ""
    assert T.adresse_normalisee(_t("D", "1 rue X", "")) == ""


def test_remplacement():
    n = "21 Rue d'Uzès 75002 Paris"
    assert T.remplacer("", n) == "remplacer"
    assert T.remplacer("21, rue d uzes", n) == "remplacer"                       # sans code postal
    assert T.remplacer("21 rue d'Uzès 75009 Paris", n) == "remplacer"           # même lieu, code postal faux
    assert T.remplacer("21 Rue d'Uzès 75002 Paris", n) == "garder"
    assert T.remplacer("8 rue de Choiseul 75002 Paris", n) == "garder"          # autre lieu : on n'y touche pas


def test_completer_lancement(monkeypatch):
    base = [_t("Elaia partners", "21, rue d uzes", "21 Rue d'Uzès, 75002 Paris, France", page="aaa"),
            _t("The second life", "102 rue Rayon", "102 Rue Rayon, Paris, France"),
            _t("Kearney", "8 rue Lamennais", "8 Rue Lamennais, 75008 Paris, France", id_="77")]
    async def lire(c):
        return base
    monkeypatch.setattr(T, "lire_terrain", lire)
    orgs = [{"nom": "Elaia Partners", "id": ""},
            {"nom": "The Second Life", "id": ""},
            {"nom": "Cabinet K", "id": "77", "adresse_notion": "8 rue Lamennais"},
            {"nom": "Autre", "id": "", "infos_source": {"ref_notion": "aaa"}},
            {"nom": "Splio", "id": ""}]
    n = asyncio.run(T.completer_lancement(orgs))
    assert n == 3
    assert orgs[0]["adresse_notion"] == "21 Rue d'Uzès 75002 Paris" and orgs[0]["cp"] == "75002"
    assert "adresse_notion" not in orgs[1]                                      # pas de code postal : rien
    assert orgs[2]["adresse_notion"] == "8 Rue Lamennais 75008 Paris"           # fichier sans code postal : remplacée
    assert orgs[3]["adresse_notion"] == "21 Rue d'Uzès 75002 Paris"             # retrouvée par la page Notion
    assert "adresse_notion" not in orgs[4]


def test_lancement_jamais_bloque(monkeypatch):
    async def panne(c):
        raise RuntimeError("Notion 502")
    monkeypatch.setattr(T, "lire_terrain", panne)
    vus = []
    assert asyncio.run(T.completer_lancement([{"nom": "X"}], vus.append)) == 0 and "illisible" in vus[0]
