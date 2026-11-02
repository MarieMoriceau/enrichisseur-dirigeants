"""Captain Verify : référentiel consulté avant chaque appel, seules les « ok » gardées. Lancer : python3 -m pytest tests/"""
import asyncio, io, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from openpyxl import Workbook, load_workbook
import v3_captain as CV


def _monter(monkeypatch, ref, reponses):
    appels, ajouts = [], []
    monkeypatch.setattr(CV, "CV_KEY", "cle")
    CV.REF.update(emails=dict(ref), charge=True)
    async def appeler(c, e):
        appels.append(e)
        return {"email": e, "status": reponses[e], "domaine": e.split("@")[1], "details": "", "free": False, "role": False,
                "ok_for_all": False, "protected": False, "listes": "", "source": "enrichisseur", "le": "2026-11-02"}
    async def ajouter(c, x):
        ajouts.append(x["email"]); CV.REF["emails"][x["email"]] = x
    monkeypatch.setattr(CV, "_appeler", appeler); monkeypatch.setattr(CV, "_ajouter", ajouter)
    return appels, ajouts


def test_referentiel_d_abord_et_seulement_ok(monkeypatch):
    ref = {"connu@ok.fr": {"email": "connu@ok.fr", "status": "ok"}, "mort@x.fr": {"email": "mort@x.fr", "status": "invalid"}}
    appels, ajouts = _monter(monkeypatch, ref, {"neuf@y.fr": "ok", "douteux@z.fr": "risky", "inconnu@w.fr": "unknown"})
    ds = [{"email": e, "source_email": "Kaspr"} for e in ("Connu@ok.fr", "mort@x.fr", "neuf@y.fr", "douteux@z.fr", "inconnu@w.fr")]
    n = asyncio.run(CV.filtrer(None, ds, lambda m: None))
    assert appels == ["neuf@y.fr", "douteux@z.fr", "inconnu@w.fr"]            # les deux connues ne sont pas repayées
    assert ajouts == ["neuf@y.fr", "douteux@z.fr"]                            # « unknown » est recrédité : pas gardé
    assert [d["email"] for d in ds] == ["Connu@ok.fr", "", "neuf@y.fr", "", ""]
    assert ds[1]["email_ecarte"] == "mort@x.fr" and ds[1]["cv_statut"] == "invalid" and ds[1]["source_email_ecarte"] == "Kaspr"
    assert n["payees"] == 3 and n["referentiel"] == 2 and n["ok"] == 2


def test_credits_epuises_arrete(monkeypatch):
    monkeypatch.setattr(CV, "CV_KEY", "cle")
    CV.REF.update(emails={}, charge=True)
    async def appeler(c, e):
        raise CV.CreditsEpuises("crédits Captain Verify épuisés")
    monkeypatch.setattr(CV, "_appeler", appeler)
    ds = [{"email": "a@b.fr"}, {"email": "c@d.fr"}]
    log = []
    n = asyncio.run(CV.filtrer(None, ds, log.append))
    assert n["arret"] == 1 and ds[0]["email"] == "a@b.fr" and "épuisés" in log[0]


def test_sans_cle_rien_ne_change(monkeypatch):
    monkeypatch.setattr(CV, "CV_KEY", "")
    ds = [{"email": "a@b.fr"}]
    asyncio.run(CV.filtrer(None, ds, lambda m: None))
    assert ds == [{"email": "a@b.fr"}]


def test_import_et_export_au_format_de_marie():
    wb = Workbook(); ws = wb.active; ws.title = "Tous_emails"
    ws.append(["email", "status", "domaine", "free", "disposable", "role", "ok_for_all", "protected", "did_you_mean", "details", "listes"])
    ws.append(["A.Blanchet@clementblanchet.com", "invalid", "clementblanchet.com", 0, 0, 0, 0, 0, None, "email error", "78157"])
    ws.append(["b@x.fr", "ok", "x.fr", 0, 0, 1, 1, 0, None, None, "77309"])
    ws.append(["b@x.fr", "ok", "x.fr", 0, 0, 1, 1, 0, None, None, "77309"])
    b = io.BytesIO(); wb.save(b)
    l = CV.lire_referentiel(b.getvalue())
    assert [x["email"] for x in l] == ["a.blanchet@clementblanchet.com", "b@x.fr"] and l[1]["ok_for_all"] and l[1]["role"]
    xl = load_workbook(io.BytesIO(CV.excel_referentiel({x["email"]: x for x in l})))
    assert xl.sheetnames == ["Lisez-moi", "Synthèse", "Tous_emails", "Par_domaine"]
    assert [c.value for c in xl["Tous_emails"][2]][:2] == ["a.blanchet@clementblanchet.com", "invalid"]


def test_a_verifier_ignore_saisie_main_et_deja_verifies():
    ds = [{"linkedin": "l/a", "email": "a@x.fr"}, {"linkedin": "l/b", "email": "b@x.fr", "cv_statut": "ok"},
          {"linkedin": "l/c", "email": "c@x.fr"}, {"linkedin": "l/d", "email": ""}]
    nouveaux = {"l/a": {"etat": "prêt pour HubSpot"}, "l/b": {"etat": "prêt pour HubSpot"},
                "l/c": {"etat": "prêt pour HubSpot", "modifie": ["email"], "email": "c@x.fr"}, "l/d": {"etat": "prêt pour HubSpot"}}
    assert [d["email"] for d in CV.a_verifier({"nouveaux": json.dumps(nouveaux)}, {"dirigeants": ds})] == ["a@x.fr"]
