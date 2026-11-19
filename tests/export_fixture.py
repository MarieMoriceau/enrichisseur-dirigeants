"""Jeu de sociétés commun aux tests des exports HubSpot et Pipedrive (toutes les sortes de lignes)."""
import json


def socs():
    """Comme après _donnees : « nouveaux » et « decisions » déjà décodés."""
    out = _brut()
    for s in out:
        s["nouveaux"], s["decisions"] = json.loads(s["nouveaux"]), json.loads(s["decisions"])
    return out


def _brut():
    return [
        {"page": "p1", "nom": "Botify", "validation": "Contacts validés", "siren": "123456789", "domaine": "botify.com",
         "adresse_retenue": "22 Rue Royale, 75008 Paris", "source_retenue": "site", "traitee": "2026-11-02T10:00", "validee": "2026-11-03T09:00",
         "base_quali": "YES", "debut_bail": "2021", "flexoffice": False, "operateur_flex": "",
         "data": {"id": "4101", "linkedin": "https://linkedin.com/company/botify", "effectif_linkedin": 210, "naf": "62.01Z",
                  "statut_activite": "Active", "raison_sociale": "BOTIFY SAS", "secteur": "Logiciel",
                  "verdict_domiciliation": "✅ Bureaux réels",
                  "dirigeants": [{"nom_complet": "Adrien Ménard", "prenom": "Adrien", "nom": "Ménard", "email": "adrien@botify.com",
                                  "fonction": "CEO", "role": "1 CEO", "linkedin": "https://linkedin.com/in/amenard", "cv_statut": "ok"},
                                 {"nom_complet": "Julie Roy", "email": "julie@botify.com", "fonction": "CFO", "role": "3 CFO",
                                  "linkedin": "https://linkedin.com/in/jroy", "ailleurs_crm": {"org_nom": "Ancienne SA"}}],
                  "contacts_crm": [{"id": 77, "nom_complet": "Paul Durand", "email": "paul@botify.com", "statut": "parti", "detail": "plus sur LinkedIn"}]},
         "nouveaux": json.dumps({"https://linkedin.com/in/amenard": {"etat": "prêt pour HubSpot", "le": "2026-11-03T09:10", "modifie_par": "Michel"},
                                 "https://linkedin.com/in/jroy": {"etat": "prêt pour HubSpot", "action": "existant", "fiche_existante": 555,
                                                                  "le": "2026-11-03T09:12", "modifie": ["telephone"], "telephone": "0601020304"}}),
         "decisions": json.dumps([{"type": "crm", "id": 77, "action": "marquer parti", "le": "2026-11-03"},
                                  {"type": "ailleurs", "id": 555, "action": "changer de société"}]),
         "partis": json.dumps({"77": {"etat": "validé", "ancien_email": "paul@botify.com", "email": "paul@neo.fr", "nom_complet": "Paul Durand",
                                      "fonction": "COO", "role": "2 COO", "domaine": "neo.fr", "nouvelle_societe": "Neo",
                                      "adresse": "5 Rue du Mail, 75002 Paris", "effectif": 40, "decide_le": "2026-11-04"}})},
        {"page": "p2", "nom": "Atelier Sans Site", "validation": "Société validée", "siren": "", "domaine": "",
         "adresse_retenue": "6 Rue d'Amboise, 75002 Paris", "source_retenue": "terrain", "traitee": "", "validee": "2026-11-05",
         "flexoffice": True, "operateur_flex": "Morning (probable)", "data": {}, "nouveaux": "{}", "decisions": "[]"},
        {"page": "p3", "nom": "Wojo", "validation": "Écartée", "siren": "987654321", "domaine": "wojo.com", "decision_ecart": "À supprimer de la base",
         "motif_ecart": "Rachetée / renommée", "repreneur": "IWG", "adresse_retenue": "", "validee": "2026-11-09",
         "data": {"id": "9001"}, "nouveaux": "{}", "decisions": "[]"},
        {"page": "p4", "nom": "Hors Paris", "validation": "Écartée", "siren": "", "domaine": "", "motif_ecart": "Lyon", "validee": "2026-11-09",
         "data": {}, "nouveaux": "{}", "decisions": "[]"},
    ]
