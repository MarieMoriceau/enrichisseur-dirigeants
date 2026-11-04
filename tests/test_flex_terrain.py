"""Bureau flexible vu sur place (base Prospection terrain : « Type de société » = flex, opérateur dans « Remarques »)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_terrain as T


def test_operateur_dans_les_remarques():
    assert T.operateur_remarque("Come and work , Viennent d’emménager") == "Come and Work"
    assert T.operateur_remarque("Opérateur Come and work; envoyer mail") == "Come and Work"
    assert T.operateur_remarque("Karos : Come And Work") == "Come and Work"
    assert T.operateur_remarque("mail Klam, cote droite") == "Klam"
    assert T.operateur_remarque("mail flex, occupant morning") == "Morning"
    assert T.operateur_remarque("mail;  Deskeo") == "Deskeo"
    assert T.operateur_remarque("Snapdesk") == "Snapdesk"
    assert T.operateur_remarque("Arrivée récente (il y a deux jours) Lnb") == "Les Nouveaux Bureaux"


def test_remarques_sans_operateur():
    for r in ("mail", "Envoyer mail", "fond de cour, vente", "Cherche 100 m\nPiste produit", "mail ; 1000m2",
              "Sous-location mentionnée", "Locaux vides", "Bâtiment cours. Prospect pour vente.",
              "J’ai laissé mes coordonnées à l’ananas, elle va voir avec sa direction", "Tout l'immeuble.",
              "Groupe locaux trop grand pour leurs besoins", "Viennent d’arriver"):
        assert T.operateur_remarque(r) == "", r


V = lambda nom, rem="", typ="", date="2026-10-01", id_="": {"nom": nom, "nom_pd": "", "id": id_, "adresse": "1 rue X", "cp": "75002",
                                                           "nego": "Richard", "date": date, "remarques": rem, "type": typ}


def test_plusieurs_visites_operateur_retrouve():
    terrain = [V("Lokki", "Come and work", "flex", "2026-09-01"), V("Lokki", "Viennent d’arriver", "flex", "2026-10-15")]
    flex, op, x = T.flex_terrain(terrain[1], terrain)
    assert flex and op == "Come and Work" and x["date"] == "2026-09-01"


def test_flex_sans_operateur_coche_quand_meme():
    terrain = [V("OneHive", "mail", "flex")]
    assert T.flex_terrain(terrain[0], terrain)[:2] == (True, "")
    props, action = T.decision_flex({"flexoffice": False, "operateur_flex": ""}, True, "", "terrain")
    assert props == {"Flexoffice": {"checkbox": True}} and action == "Flexoffice cochée"


def test_pas_flex():
    terrain = [V("Prose", "mail", "Yes")]
    assert T.flex_terrain(terrain[0], terrain) == (False, "", None)
    assert T.decision_flex({}, False, "", "") == ({}, "")


def test_hierarchie_des_sources():
    # SIRENE « probable » : le terrain l'emporte
    props, action = T.decision_flex({"flexoffice": False, "operateur_flex": "Morning (probable)"}, True, "Klam", "src")
    assert props["Opérateur flex"] == "Klam" and "remplace" in action
    # saisie à la main ou opérateur confirmé : jamais remplacé
    props, action = T.decision_flex({"flexoffice": True, "operateur_flex": "Deskeo"}, True, "Klam", "src")
    assert props == {} and "non touché" in action
    props, action = T.decision_flex({"flexoffice": True, "operateur_flex": "(aucun)"}, True, "Klam", "src")
    assert props == {} and "retiré à la main" in action
    assert T.decision_flex({"flexoffice": True, "operateur_flex": "Come and Work"}, True, "Come and Work", "s") == ({}, "déjà à jour")


def test_cas_du_premier_essai_a_blanc():
    # « réflexe » = Reflex
    assert T.operateur_remarque("Ils sont chez réflexe, ils n’ont pas de projet") == "Reflex"
    # LNB déjà noté = Les Nouveaux Bureaux : rien à faire
    assert T.decision_flex({"flexoffice": True, "operateur_flex": "LNB"}, True, "Les Nouveaux Bureaux", "s") == ({}, "déjà à jour")
    # doute du négo sur une société non classée flex : pas d'opérateur, pas de case cochée
    aeg = V("Aeg", "Sont la depuis 5/6 ans, en revanche je ne sais pas s’ils ont un plateau de deskeo comme extension ou pas!", "Yes")
    assert T.flex_terrain(aeg, [aeg]) == (False, "", None)
    # société classée « Yes » mais chez un opérateur affirmé : retenue
    monks = V("Monks", "Fin de contrat chez LNB en juillet 2027", "Yes")
    assert T.flex_terrain(monks, [monks])[:2] == (True, "Les Nouveaux Bureaux")
    # doute mais classée flex par le négo : retenue
    sek = V("Sekoia", "Bah ici on sait pas, immeuble morning", "flex")
    assert T.flex_terrain(sek, [sek])[:2] == (True, "Morning")
