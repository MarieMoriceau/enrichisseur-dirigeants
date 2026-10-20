"""Espace de chaque négo : champ « nego » de prospection-hebdo, table Équipe négos, connexion par lien e-mail.
Lancer : python3 -m pytest tests/"""
import asyncio, os, re, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from fastapi.testclient import TestClient

import v3, v3_equipe as EQ, v3_memoire as MEM, v3_validation as VALID, v3_suivi as SUIVI

EQUIPE = [
    {"prenom": "Marine", "mail": "mbureau@equation-sie.com", "actif": True, "admin": False, "nego": True},
    {"prenom": "Richard", "mail": "rabou-khalil@equation-sie.com", "actif": True, "admin": False, "nego": True},
    {"prenom": "Lionel", "mail": "lbastian@equation-sie.com", "actif": True, "admin": True, "nego": True},
    {"prenom": "Sébastien", "mail": "", "actif": True, "admin": False, "nego": True},
    {"prenom": "Marie", "mail": "mmoriceau@equation-sie.com", "actif": True, "admin": True, "nego": False},
    {"prenom": "Nicolas", "mail": "nvial@equation-sie.com", "actif": False, "admin": False, "nego": True},
]


def soc(nom, negos="", lance_par="", **kw):
    return {"page": "a" * 32, "nom": nom, "negos": negos, "lance_par": lance_par, "domaine": "", "traitee": "2026-10-20",
            "lancement": "S43", "effectif": 20, "nb_nouveaux": 2, "nb_prets": 0, "validation": "À valider",
            "perimetre": "tout", "nouveaux": "", "decisions": "", **kw}


# ───────────── règles ─────────────
def test_norm_et_fusion():
    assert EQ.norm(" SÉBASTIEN ") == EQ.norm("sebastien") == "sebastien"
    assert EQ.fusion("Marine", ["Sébastien", "sebastien", ""]) == "Marine ; Sébastien"
    assert EQ.fusion("", ["Marine ; Richard"]) == "Marine ; Richard"


def test_attribution():
    a = EQ.attribution(EQUIPE, "sebastien", "")
    assert a == {"a": ["Sébastien"], "inconnus": [], "non_attribue": False}
    a = EQ.attribution(EQUIPE, "Seb", "")                         # faute de frappe : non attribué, prénom affiché
    assert a["non_attribue"] and a["inconnus"] == ["Seb"]
    assert EQ.attribution(EQUIPE, "Nicolas", "")["non_attribue"]  # parti : décoché « Actif »
    assert EQ.attribution(EQUIPE, "", "mmoriceau@equation-sie.com")["non_attribue"]   # lancée par Marie (admin, pas négo)
    assert EQ.attribution(EQUIPE, "", "rabou-khalil@equation-sie.com")["a"] == ["Richard"]  # lancée par Richard lui-même
    assert EQ.attribution(EQUIPE, "Marine ; Richard", "")["a"] == ["Marine", "Richard"]


def test_visible_pour():
    marine, richard = EQ.par_mail(EQUIPE, "MBureau@equation-sie.com"), EQ.par_mail(EQUIPE, "rabou-khalil@equation-sie.com")
    s = soc("Alan", negos="Marine ; Richard")
    assert EQ.visible_pour(marine, s, EQUIPE) and EQ.visible_pour(richard, s, EQUIPE)
    assert not EQ.visible_pour(marine, soc("Ulule", negos="Richard"), EQUIPE)
    assert EQ.visible_pour(marine, soc("Lancée", lance_par="mbureau@equation-sie.com"), EQUIPE)
    assert not EQ.par_mail(EQUIPE, "nvial@equation-sie.com")                       # inactif : inconnu
    assert not EQ.visible_pour(EQ.par_mail(EQUIPE, "mmoriceau@equation-sie.com"), soc("X", negos="Marie"), EQUIPE)


# ───────────── l'API garde le négo et fusionne les doublons ─────────────
def test_api_doublons_fusionnes():
    orgs = [v3._org_api(x) for x in (
        {"nom": "Alan", "siren": "443 061 841", "nego": "Marine"},
        {"nom": "ALAN", "siren": "443061841", "nego": "Richard", "adresse_notion": "117 quai de Valmy, 75010 Paris"},
        {"nom": "Ulule", "code_postal": "75010", "nego": "Sébastien"},
        {"nom": "ulule", "code_postal": "75010", "nego": "sebastien"})]
    garde, n = v3._fusion_doublons_api(orgs)
    assert n == 2 and len(garde) == 2
    assert garde[0]["infos_source"]["nego"] == "Marine ; Richard" and garde[0]["adresse_notion"].startswith("117")
    assert garde[1]["infos_source"]["nego"] == "Sébastien"
    assert MEM.nego_de(garde[0]) == "Marine ; Richard"


def test_reprise_memoire_garde_le_nouveau_nego(monkeypatch):
    """Société déjà en mémoire (relevée par Marine) qui revient avec Richard : Richard est ajouté, Marine reste."""
    async def faux_index(*a, **k):
        return [{"page": "p1", "nom": "alan", "siren": "443061841", "domaine": "", "perim": "tout", "date": "2026-10-19",
                 "lance_par": "", "lancement": "S42", "negos": "Marine"}]
    async def faux_lire(page):
        return {"nom": "Alan", "mode": "complet", "infos_source": {"nego": "Marine"}}
    monkeypatch.setattr(MEM, "index", faux_index)
    monkeypatch.setattr(MEM, "lire", faux_lire)
    o = v3._org_api({"nom": "Alan", "siren": "443061841", "nego": "Richard"})
    job = {"perimetre": "tout", "mails": [], "nom": "S43"}
    reprises, reste = asyncio.run(MEM.reprendre(job, [o], v3.norm_nom, v3.domaines, print))
    assert not reste and reprises[0]["_negos"] == "Marine" and reprises[0]["_nego_nouveau"] == "Richard"

    envoyes = []
    class FauxClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
    async def faux_req(c, methode, url, **kw):
        envoyes.append(kw.get("json"))
        class R: status_code = 200
        return R()
    monkeypatch.setattr(MEM, "NOTION_KEY", "x")
    monkeypatch.setattr(MEM, "_COLONNE_NEGOS", {"ok": True})
    monkeypatch.setattr(MEM.httpx, "AsyncClient", lambda **k: FauxClient())
    monkeypatch.setattr(MEM, "_req", faux_req)
    asyncio.run(MEM.ajouter_lanceurs({**job, "resultats": reprises}, print))
    assert envoyes and envoyes[0]["properties"]["Négos"]["rich_text"][0]["text"]["content"] == "Marine ; Richard"


# ───────────── écrans et connexion ─────────────
def _app(monkeypatch, societes):
    async def faux_membres(forcer=False):
        return EQUIPE
    async def fausses_toutes():
        return [dict(s) for s in societes]
    monkeypatch.setattr(EQ, "membres", faux_membres)
    monkeypatch.setattr(VALID, "_toutes", fausses_toutes)
    app = FastAPI(); app.include_router(VALID.router)
    return TestClient(app, base_url="https://testserver")


def _connecter(c, monkeypatch, mail):
    """Demande un lien, l'attrape dans le « mail », l'ouvre : renvoie la réponse de l'ouverture."""
    partis = []
    monkeypatch.setattr(SUIVI, "SMTP_USER", "x"); monkeypatch.setattr(SUIVI, "SMTP_PASS", "y")
    monkeypatch.setattr(SUIVI, "_envoyer", lambda dest, sujet, corps, *a: partis.append((dest, corps)))
    VALID._DERNIER_ENVOI.clear()
    r = c.post("/v3/connexion/lien", data={"mail": mail, "suite": "/v3/contacts"}, follow_redirects=False)
    if not partis:
        return r
    lien = re.search(r"https?://\S+", partis[0][1]).group(0)
    return c.get(lien.split("testserver", 1)[-1], follow_redirects=False)


SOCIETES = [soc("Alan", negos="Marine ; Richard"), soc("Ulule", negos="Richard"), soc("Sans négo"),
            soc("Faute", negos="Seb"), soc("Lancée par Marine", lance_par="mbureau@equation-sie.com"),
            soc("Écartée", negos="Marine", validation="Écartée")]


def test_negos_ne_voient_que_les_leurs(monkeypatch):
    c = _app(monkeypatch, SOCIETES)
    r = _connecter(c, monkeypatch, "MBureau@equation-sie.com")
    assert r.status_code == 303 and r.headers["location"] == "/v3/contacts"
    noms = {s["nom"]: s for s in c.get("/v3/contacts/api/societes").json()["societes"]}
    assert set(noms) == {"Alan", "Lancée par Marine"}
    assert noms["Alan"]["attribue_a"] == ["Marine", "Richard"]          # « aussi suivie par Richard » à l'écran
    assert c.get("/v3/validation/api/societes").status_code == 403      # pas admin


def test_admin_voit_tout_avec_non_attribue(monkeypatch):
    c = _app(monkeypatch, SOCIETES)
    assert _connecter(c, monkeypatch, "mmoriceau@equation-sie.com").status_code == 303
    j = c.get("/v3/validation/api/societes").json()
    par = {s["nom"]: s for s in j["societes"]}
    assert len(par) == len(SOCIETES)
    assert par["Sans négo"]["non_attribue"] and par["Faute"]["non_attribue"] and par["Faute"]["prenoms_inconnus"] == ["Seb"]
    assert not par["Alan"]["non_attribue"]
    assert j["negos"] == ["Marine", "Richard", "Lionel", "Sébastien"]


def test_lionel_admin_et_nego(monkeypatch):
    c = _app(monkeypatch, SOCIETES)
    assert _connecter(c, monkeypatch, "lbastian@equation-sie.com").status_code == 303
    assert c.get("/v3/validation/api/societes").status_code == 200                # admin
    socs = c.get("/v3/contacts/api/societes").json()["societes"]
    assert len(socs) == 5 and not any(s["a_moi"] for s in socs)                 # tout voir, rien à son prénom ici
    assert "Lionel" in c.get("/v3/contacts").text                               # « Mes sociétés (Lionel) » proposé


def test_lien_refuse(monkeypatch):
    c = _app(monkeypatch, SOCIETES)
    r = _connecter(c, monkeypatch, "inconnu@equation-sie.com")
    assert "info=inconnu" in r.headers["location"]
    assert "info=inconnu" in _connecter(c, monkeypatch, "nvial@equation-sie.com").headers["location"]  # inactif
    # lien expiré ou trafiqué
    vieux = VALID._jeton_lien("mbureau@equation-sie.com", int(time.time()) - 1)
    assert "info=expire" in c.get(f"/v3/connexion/lien?t={vieux}", follow_redirects=False).headers["location"]
    faux = VALID._jeton_lien("mbureau@equation-sie.com", int(time.time()) + 600)[:-2] + "00"
    assert "info=expire" in c.get(f"/v3/connexion/lien?t={faux}", follow_redirects=False).headers["location"]
    # un lien par minute
    _connecter(c, monkeypatch, "mbureau@equation-sie.com")
    VALID._DERNIER_ENVOI["mbureau@equation-sie.com"] = time.time()
    r = c.post("/v3/connexion/lien", data={"mail": "mbureau@equation-sie.com"}, follow_redirects=False)
    assert "info=attendre" in r.headers["location"]


def test_ancien_mot_de_passe_negos_et_retrait(monkeypatch):
    c = _app(monkeypatch, SOCIETES)
    monkeypatch.setattr(VALID, "MDP_NEGOS", "negos"); monkeypatch.setattr(VALID, "MDP_ADMIN", "admin")
    r = c.post("/v3/connexion", data={"mdp": "negos", "mail": "mbureau@equation-sie.com"}, follow_redirects=False)
    assert "erreur=1" in r.headers["location"]                                  # le mot de passe partagé n'ouvre plus rien
    c.cookies.set(VALID.COOKIE, VALID._jeton("negos", "mbureau@equation-sie.com"))  # ancien cookie : refusé
    assert c.get("/v3/contacts/api/societes").status_code == 401
    assert _connecter(c, monkeypatch, "mbureau@equation-sie.com").status_code == 303
    assert c.get("/v3/contacts/api/societes").status_code == 200
    EQUIPE_SANS = [dict(m, actif=False) if m["prenom"] == "Marine" else m for m in EQUIPE]
    async def sans_marine(forcer=False):
        return EQUIPE_SANS
    monkeypatch.setattr(EQ, "membres", sans_marine)
    assert c.get("/v3/contacts/api/societes").status_code == 401               # retirée de la table : coupée tout de suite
    r = c.post("/v3/connexion", data={"mdp": "admin"}, follow_redirects=False)  # secours admin
    assert r.status_code == 303 and c.get("/v3/validation/api/societes").status_code == 200
