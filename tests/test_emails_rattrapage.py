"""E-mails des contacts validés restés sans adresse (tous rôles, avec ou sans domaine). Lancer : python3 -m pytest tests/"""
import asyncio, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_rattrapage as R, v3_linkedin as LI


def _d(n, role, **k):
    return {"prenom": n, "nom": "Durand", "nom_complet": f"{n} Durand", "role": role,
            "linkedin": f"https://www.linkedin.com/in/{n.lower()}", "email": "", **k}


def test_selection():
    ds = [_d("Ana", "7 Autre C-level / VP"), _d("Bob", "1 CEO / Président / DG", email="bob@x.fr"),
          _d("Cid", "6 Immobilier / Services généraux"), _d("Dan", "2 Fondateur"),
          _d("Eve", "4 CFO / DAF", kaspr_tente="2026-11-02", fullenrich_tente="2026-11-02")]
    nouveaux = {f"https://www.linkedin.com/in/{n}": {"etat": "prêt pour HubSpot"} for n in ("ana", "bob", "cid", "eve")}
    nouveaux["https://www.linkedin.com/in/dan"] = {"etat": "écarté"}
    s = {"nouveaux": json.dumps(nouveaux)}
    assert [d["prenom"] for d in R.contacts_valides_sans_email(s, {"dirigeants": ds})] == ["Ana", "Cid"]


def test_fullenrich_sans_domaine(monkeypatch):
    envoye = {}
    async def lot(c, h, datas, journal, n, total):
        envoye["datas"] = datas
        return {}
    monkeypatch.setattr(LI, "_fullenrich_lot", lot)
    asyncio.run(LI.fullenrich_emails(None, [(_d("Ana", "7"), {"nom": "Piste", "domaine": ""})], lambda m: None))
    x = envoye["datas"][0]
    assert "domain" not in x and x["company_name"] == "Piste" and x["linkedin_url"].endswith("/ana")
