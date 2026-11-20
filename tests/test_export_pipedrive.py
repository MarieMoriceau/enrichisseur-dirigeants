"""Export Pipedrive : mêmes données que l'export HubSpot, au format d'import Pipedrive. Lancer : python3 -m pytest tests/"""
import asyncio, io, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastapi import FastAPI
from fastapi.testclient import TestClient
from openpyxl import load_workbook
import v3, v3_export as EX, v3_validation as VALID
import export_fixture as F


def _feuilles(xl: bytes) -> dict:
    wb = load_workbook(io.BytesIO(xl))
    out = {}
    for ws in wb.worksheets:
        rows = list(ws.iter_rows(values_only=True))
        out[ws.title] = [dict(zip(rows[0], r)) for r in rows[1:]] if ws.title != "Lisez-moi" else rows
    return out


def test_onglets_dans_l_ordre_d_import():
    xl, r = EX.construire_pipedrive(F.socs())
    assert list(_feuilles(xl)) == ["Lisez-moi"] + EX.ONGLETS_PD
    assert r["org_maj"] == 1 and r["org_neuves"] == 1 and r["contacts"] == 1


def test_memes_donnees_que_hubspot():
    """Chaque valeur de l'export HubSpot se retrouve dans l'export Pipedrive."""
    hs, pd = _feuilles(EX.construire(F.socs())[0]), _feuilles(EX.construire_pipedrive(F.socs())[0])
    for titre_hs, titres_pd in (("Sociétés", ["Organisations à mettre à jour", "Organisations à créer"]),
                                ("Contacts à créer", ["Personnes à créer"]), ("Contacts existants", ["Personnes à rattacher"]),
                                ("Contacts CRM", ["Personnes CRM"]), ("Contacts partis", ["Personnes parties"]),
                                ("Sociétés des partis", ["Organisations des partis"]), ("Sociétés écartées", ["Organisations écartées"]),
                                ("À supprimer", ["À supprimer"]), ("Repreneurs", ["Repreneurs"])):
        valeurs_pd = {str(v) for t in titres_pd for l in pd[t] for v in l.values() if v not in (None, "")}
        lignes_pd = sum(len(pd[t]) for t in titres_pd)
        assert lignes_pd == len(hs[titre_hs]), titre_hs
        for l in hs[titre_hs]:
            for h, v in l.items():
                if v in (None, "") or h in ("Street Address", "Postal Code", "City", "Décision") and titre_hs == "Sociétés écartées":
                    continue
                if h in ("Street Address", "Postal Code", "City"):        # regroupés dans « Adresse »
                    assert any(str(v) in x for x in valeurs_pd), (titre_hs, h, v)
                else:
                    assert str(v) in valeurs_pd, (titre_hs, h, v)


def test_cles_pipedrive():
    pd = _feuilles(EX.construire_pipedrive(F.socs())[0])
    org = pd["Organisations à mettre à jour"][0]
    assert org["ID Pipedrive (organisation)"] == "4101" and org["Adresse"] == "22 Rue Royale, 75008 Paris" and org["Site web"] == "botify.com"
    assert pd["Organisations à créer"][0]["Nom"] == "Atelier Sans Site"          # pas de site : gardée, contrairement à HubSpot
    assert "ID Pipedrive (organisation)" not in pd["Organisations à créer"][0]
    p = pd["Personnes à créer"][0]
    assert p["Nom"] == "Adrien Ménard" and p["Organisation"] == "Botify" and p["ID Pipedrive (organisation)"] == "4101"
    assert pd["Personnes à rattacher"][0]["ID Pipedrive (personne)"] == 555
    parti = pd["Personnes parties"][0]
    assert parti["E-mail"] == "paul@neo.fr" and parti["Ancien e-mail"] == "paul@botify.com" and parti["Organisation"] == "Neo"
    assert [l["Décision"] for l in pd["Organisations écartées"]] == ["À supprimer de la base", "Hors cible"]


def test_un_onglet():
    xl, _ = EX.construire_pipedrive(F.socs())
    wb = load_workbook(io.BytesIO(EX.un_onglet(xl, "Personnes à créer")))
    assert [w.title for w in wb.worksheets] == ["Personnes à créer"]


def _client(monkeypatch):
    monkeypatch.setattr(VALID, "_refus", lambda req, besoin: None)
    async def donnees(suivi=None):
        return F.socs()
    monkeypatch.setattr(EX, "_donnees", donnees)
    monkeypatch.setattr(EX, "_lancer", lambda mail: None)
    EX.EXPORT.update(actif=False, xl=None, xl_pd=None, pret_le=0.0, erreur="", mail=False, mails=[])
    app = FastAPI(); app.include_router(v3.router)
    return TestClient(app)


def test_route_pipedrive(monkeypatch):
    c = _client(monkeypatch)
    assert "Pipedrive" in c.get("/v3/validation/api/export-pipedrive").text          # page d'attente
    asyncio.run(EX._preparer())
    r = c.get("/v3/validation/api/export-pipedrive")
    assert "pipedrive" in r.headers["content-disposition"] and "Organisations à créer" in _feuilles(r.content)
    r = c.get("/v3/validation/api/export-pipedrive", params={"onglet": "Personnes CRM"})
    assert list(_feuilles(r.content)) == ["Personnes CRM"] and "personnes_crm" in r.headers["content-disposition"]
    assert c.get("/v3/validation/api/export-pipedrive", params={"onglet": "Inconnu"}).status_code == 404
    e = c.get("/v3/validation/api/export/etat").json()
    assert e["pret_pipedrive"] and e["resume_pipedrive"]["org_neuves"] == 1
    assert "Sociétés" in _feuilles(c.get("/v3/validation/api/export").content)    # l'export HubSpot reste servi


def test_pipedrive_en_panne_ne_bloque_pas_hubspot(monkeypatch):
    c = _client(monkeypatch)
    monkeypatch.setattr(EX, "construire_pipedrive", lambda socs: 1 / 0)
    asyncio.run(EX._preparer())
    assert EX.EXPORT["xl"] and not EX.EXPORT["erreur"] and EX.EXPORT["xl_pd"] is None
    relances = []
    monkeypatch.setattr(EX, "_lancer", lambda mail: relances.append(mail))
    r = c.get("/v3/validation/api/export-pipedrive")             # erreur affichée, plus de page « Préparation » sans fin
    assert "échoué" in r.text and "ZeroDivisionError" in r.text and "refresh" not in r.text and relances == []
    assert c.get("/v3/validation/api/export/etat").json()["erreur_pipedrive"]
    assert "Sociétés" in _feuilles(c.get("/v3/validation/api/export").content)    # HubSpot toujours servi


def test_erreur_hubspot_sans_relance(monkeypatch):
    c = _client(monkeypatch)
    relances = []
    monkeypatch.setattr(EX, "_lancer", lambda mail: relances.append(mail))
    EX.EXPORT.update(erreur="RuntimeError : Notion 502")
    r = c.get("/v3/validation/api/export")
    assert "502" in r.text and "refresh" not in r.text and relances == []
    c.get("/v3/validation/api/export", params={"refaire": 1})       # « Réessayer » relance bien
    assert relances == [False] and EX.EXPORT["erreur"] == ""


def test_valeurs_liste_ou_dictionnaire():
    socs = F.socs()
    socs[1]["adresse_retenue"] = "6 Rue d'Amboise, 75002 Paris"
    socs[0]["data"]["contacts_crm"][0]["detail"] = ["plus sur LinkedIn", "e-mail en erreur"]
    socs[0]["data"]["id"] = {"value": 4101}
    xl, _ = EX.construire_pipedrive(socs)
    assert _feuilles(xl)["Personnes CRM"][0]["Détail du contrôle"] == "plus sur LinkedIn ; e-mail en erreur"


def test_mails_demandes(monkeypatch):
    envoyes = []
    async def envoyer(fmt):
        envoyes.append(fmt)
    monkeypatch.setattr(EX, "_envoyer_mail", envoyer)
    _client(monkeypatch)
    EX.EXPORT["mails"] = ["pipedrive", "hubspot"]
    asyncio.run(EX._preparer())
    assert envoyes == ["pipedrive", "hubspot"] and EX.EXPORT["mails"] == []
    assert "Pipedrive et HubSpot" in EX.EXPORT["envoi"]
