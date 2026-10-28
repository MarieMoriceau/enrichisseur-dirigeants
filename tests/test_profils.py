"""Profils LinkedIn des contacts CRM « à vérifier » jamais lus (plafond de 80). Lancer : python3 -m pytest tests/"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_linkedin as LI


def _r(a, b):
    a, b = a.lower(), b.lower()
    return 1.0 if a == b else 0.0


def test_profil_a_lire():
    base = {"statut": "à vérifier", "linkedin": "https://www.linkedin.com/in/alexiaceva"}
    assert LI.profil_a_lire({**base, "detail": "absent des dirigeants Sales Navigator"})          # contrôle ancien
    assert LI.profil_a_lire({**base, "detail": "absent ; profil pas encore lu (plafond de 80 profils par lancement)"})
    assert not LI.profil_a_lire({**base, "detail": "absent ; profil non lu"})                     # phantom muet : pas de boucle
    assert not LI.profil_a_lire({**base, "detail": "absent ; profil lu, aucun poste affiché"})
    assert not LI.profil_a_lire({**base, "linkedin": "", "detail": "absent"})
    assert not LI.profil_a_lire({**base, "statut": "toujours là", "detail": "x"})


def test_alexia_ceva_partie_de_botify():
    """Profil réel du 27/10 : présidente de Gaignard Millon, chez Botify jusqu'en avril 2022."""
    soc = {"nom": "Botify", "linkedin_id": "2717398"}
    p = {"statut": "à vérifier", "linkedin": "https://www.linkedin.com/in/alexiaceva/",
         "detail": "absent des dirigeants Sales Navigator ; profil pas encore lu (plafond de 80 profils par lancement)"}
    lignes = [{"profileUrl": "https://www.linkedin.com/in/alexiaceva", "companyName": "Gaignard Millon",
               "linkedinJobTitle": "Présidente", "linkedinJobDateRange": "oct. 2024 - Present",
               "previousCompanyName": "Botify", "linkedinPreviousJobDateRange": "mars 2017 - avr. 2022"}]
    assert LI.classer_profils([(soc, p)], lignes, _r) == 1
    assert p["statut"] == "probablement parti" and "Gaignard Millon" in p["detail"] and "pas encore lu" not in p["detail"]


def test_plafond_marque_les_profils_non_lus(monkeypatch):
    monkeypatch.setattr(LI, "PROFILS_MAX", 1)
    async def fiches(c, r):
        return [{"id": str(i), "nom_complet": f"Contact {i}", "linkedin": f"https://www.linkedin.com/in/c{i}", "email": ""}
                for i in (1, 2)]
    monkeypatch.setattr(LI, "personnes_fiches", fiches)
    async def ailleurs(*a):
        return {}
    monkeypatch.setattr(LI, "personne_ailleurs", ailleurs)
    monkeypatch.setattr(LI, "phantoms_du_job", lambda job: ("", "", ""))         # pas de Profile Scraper
    soc = {"id": "4581", "nom": "Botify", "linkedin_id": "1", "dirigeants": []}
    log = []
    asyncio.run(LI.controler_contacts_crm({}, [soc], None, None, _r, log.append, lambda: False))
    d = [p["detail"] for p in soc["contacts_crm"]]
    assert "plafond de 1 profils" in d[1] and "Profile Scraper absent" in d[0]
    assert all(LI.profil_a_lire(p) for p in soc["contacts_crm"])


def test_a_lire_prend_l_url_saisie_a_la_main():
    import v3_profils as P
    data = {"contacts_crm": [{"id": "18703", "statut": "à vérifier", "linkedin": "", "detail": "absent des dirigeants Sales Navigator"},
                             {"id": "1", "statut": "toujours là", "linkedin": "https://www.linkedin.com/in/x", "detail": ""}]}
    l = P.a_lire(data, {"18703": "https://www.linkedin.com/in/alexiaceva"})
    assert [p["id"] for p in l] == ["18703"] and l[0]["linkedin"].endswith("alexiaceva")


def test_lancer_refuse_sans_apercu():
    import v3_profils as P
    P.ETAT.update(actif=False, lancement="", _apercu_le=0.0)
    r = asyncio.run(P.lancer("4097 (suite 1/9)", None, None, {}))
    assert not r["lance"] and "aperçu" in r["motif"]


def test_fonction_reprise_de_linkedin_si_vide():
    soc = {"nom": "Botify", "linkedin_id": "1"}
    vide = {"statut": "à vérifier", "linkedin": "https://www.linkedin.com/in/a", "fonction": "", "role": "", "detail": "x"}
    pleine = {"statut": "à vérifier", "linkedin": "https://www.linkedin.com/in/b", "fonction": "DAF", "role": "", "detail": "x"}
    lignes = [{"profileUrl": f"https://www.linkedin.com/in/{k}", "companyName": "Botify", "linkedinCompanyId": "1",
               "linkedinJobTitle": "Head of Workplace", "linkedinJobDateRange": "2021 - Present"} for k in "ab"]
    LI.classer_profils([(soc, vide), (soc, pleine)], lignes, _r)
    assert vide["fonction"] == "Head of Workplace" and vide["source_fonction"] == "profil LinkedIn"
    assert pleine["fonction"] == "DAF" and "source_fonction" not in pleine          # Pipedrive prime
