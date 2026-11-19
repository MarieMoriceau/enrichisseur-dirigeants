"""
Enrichisseur v3 — export des validations au format d'import HubSpot, et le même au format d'import Pipedrive (19/11/2026)

Un seul Excel, cumulatif (tout ce qui est validé à ce jour) : HubSpot reconnaît les sociétés par leur domaine
et les contacts par leur e-mail, donc réimporter le même fichier met à jour au lieu de créer des doublons.
  · Sociétés          : sociétés validées (adresse retenue, effectif LinkedIn, date de validation société)
  · Contacts à créer   : nouveaux dirigeants validés par les négos (saisies manuelles comprises, date de validation contact)
  · Contacts existants : dirigeants déjà dans Pipedrive chez une autre société (décision admin : changer de société / deux sociétés)
  · Contacts CRM       : décisions de l'admin sur les contacts déjà au CRM (garder / marquer parti / à supprimer / à revoir, corrections)
  · Sociétés écartées
Envoyé par mail à COPIE_FICHIER_COMPLET (Marie) ou téléchargé depuis l'écran admin.
"""
import asyncio, time, io, re
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from openpyxl import Workbook
from openpyxl.styles import Font

import v3_memoire as MEM
import v3_suivi as SUIVI
import v3_validation as VALID

router = APIRouter()
_SEM = asyncio.Semaphore(4)
LECTURE_MAX = 120          # secondes au plus par fiche Notion (pauses Notion comprises) avant de la sauter


def _date(x: str) -> str:
    return (x or "")[:10]


def _prenom_nom(nom_complet: str, prenom: str = "", nom: str = "") -> tuple:
    if prenom or nom:
        return prenom, nom
    m = (nom_complet or "").strip().split(" ", 1)
    return (m[0], m[1]) if len(m) == 2 else ("", nom_complet or "")


async def _donnees(suivi: dict = None) -> list:
    """Sociétés de la mémoire avec leur JSON complet (seulement celles qui ont une validation ou des contacts traités)."""
    toutes = await VALID._toutes()
    utiles = [s for s in toutes if s["validation"] != "À valider" or s["nouveaux"] or s["decisions"]]
    if suivi is not None:
        suivi.update(total=len(utiles), fait=0)

    sem = asyncio.Semaphore(4)                  # propre à cet export : une lecture abandonnée ne garde pas de place

    async def lire(s):
        s["data"] = {}
        async with sem:
            if suivi is not None:
                suivi.setdefault("en_lecture", {})[s["page"]] = s["nom"]
            try:
                s["data"] = await MEM.lire(s["page"]) or {}
            except Exception as e:
                if suivi is not None:
                    suivi.setdefault("sautees", []).append(f"{s['nom']} ({type(e).__name__})")
            if suivi is not None:
                suivi["en_lecture"].pop(s["page"], None)
        if suivi is not None:
            suivi["fait"] = suivi.get("fait", 0) + 1

    # Garde-fou : si plus aucune fiche n'avance pendant LECTURE_MAX secondes, on abandonne celles qui restent
    # (sans attendre qu'elles se terminent : une requête coincée ne doit plus bloquer l'export, 02/11).
    taches = {asyncio.create_task(lire(s)): s for s in utiles}
    reste, compte, depuis = set(taches), -1, time.monotonic()
    while reste:
        _, reste = await asyncio.wait(reste, timeout=min(5, LECTURE_MAX))
        fait = (suivi or {}).get("fait", len(taches) - len(reste))
        if fait != compte:
            compte, depuis = fait, time.monotonic()
        elif time.monotonic() - depuis > LECTURE_MAX:
            for t in reste:
                t.cancel()
                s = taches[t]
                s["data"] = s.get("data") or {}
                if suivi is not None:
                    suivi.setdefault("sautees", []).append(f"{s['nom']} (ne répond pas)")
                    (suivi.get("en_lecture") or {}).pop(s["page"], None)
            print(f"[V3 EXPORT] fiches abandonnées : {[taches[t]['nom'] for t in reste]}")
            break
    for s in utiles:
        VALID.appliquer_correction_siren(s)                 # SIREN corrigé à la main : la bonne fiche INSEE
        s["decisions"] = VALID._json(s["decisions"], [])
        s["nouveaux"] = VALID._json(s["nouveaux"], {})
    return utiles


# En-têtes de l'export HubSpot. Les lignes sont des dictionnaires indexés par ces en-têtes : l'export Pipedrive
# reprend exactement les mêmes valeurs, sous d'autres noms de colonnes (19/11/2026).
H_SOC = ["Company Domain Name", "Name", "SIREN", "LinkedIn Company Page", "Street Address", "Postal Code", "City",
         "Source de l'adresse", "Effectif LinkedIn", "Date relevé effectif", "Croissance effectif (%)", "STATUT ACTIVITÉ",
         "Raison sociale", "CODE NAF", "ACTIVITÉ", "SECTEUR", "DOMICILIATION", "Base quali", "Début de bail", "Flexoffice",
         "Opérateur flex", "ID Pipedrive", "Date validation société"]
H_NEW = ["First Name", "Last Name", "Email", "Phone Number", "Job Title", "Rôle", "LinkedIn URL",
         "Company Domain Name", "Société", "ID Pipedrive société", "Date validation contact", "Société validée ?",
         "Champs saisis à la main", "Saisi par", "Captain Verify"]
H_EXIST = ["First Name", "Last Name", "Email", "Phone Number", "Job Title", "Rôle", "LinkedIn URL",
           "Company Domain Name", "Nouvelle société", "ID personne Pipedrive", "Fiché aujourd'hui chez",
           "Décision admin", "Date validation contact", "Société validée ?", "Champs saisis à la main", "Captain Verify"]
H_CRM = ["Décision", "First Name", "Last Name", "Email", "Phone Number", "Job Title", "Rôle", "LinkedIn URL",
         "Company Domain Name", "Société", "ID personne Pipedrive", "Contrôle", "Détail du contrôle", "Date validation",
         "Captain Verify"]
H_PARTIS = ["Email actuel (clé HubSpot)", "Nouvel e-mail", "First Name", "Last Name", "Job Title", "Rôle",
            "LinkedIn URL", "Company Domain Name", "Nouvelle société", "Ancienne société",
            "ID personne Pipedrive", "Alertes", "Date validation"]
H_SOC_PARTIS = ["Company Domain Name", "Name", "LinkedIn Company Page", "Street Address", "Postal Code", "City", "Effectif LinkedIn"]
H_ECART = ["Société", "Domaine", "SIREN", "ID Pipedrive", "Décision", "Motif", "Repreneur", "Écartée le"]
H_SUPPR = ["Société", "ID Pipedrive", "SIREN", "Domaine", "Motif", "Repreneur", "Décidé le"]
H_REPRIS = ["Repreneur ou nouveau nom", "Ancienne société", "SIREN ancienne société", "Adresse retenue", "Écartée le"]


def _ligne(entetes: list, valeurs: list, **extra) -> dict:
    return dict(zip(entetes, valeurs)) | extra


def _tables(socs: list) -> dict:
    """Toutes les lignes des deux exports (HubSpot et Pipedrive), calculées une seule fois."""
    from v3 import decouper_adresse
    validees = [s for s in socs if s["validation"] in ("Société validée", "Contacts validés")]
    ecartees = [s for s in socs if s["validation"] == "Écartée"]
    domaine_de = {s["page"]: s["domaine"] or (s["data"] or {}).get("domaine", "") for s in socs}
    t = {"ecartees_n": len(ecartees)}

    # ── sociétés
    t["soc"] = []
    for s in validees:
        d = s["data"] or {}
        rue, cp, ville = decouper_adresse(s["adresse_retenue"])
        eff = d.get("effectif_linkedin", "")
        t["soc"].append(_ligne(H_SOC, [
            domaine_de[s["page"]], s["nom"], s["siren"], d.get("linkedin") or d.get("linkedin_site", ""),
            rue, cp, ville, s["source_retenue"], eff, _date(s["traitee"]) if eff != "" else "", d.get("croissance_effectif", ""),
            d.get("statut_activite") or d.get("statut", ""), d.get("raison_sociale", ""), d.get("naf", ""), VALID.activite_de(d), d.get("secteur", ""),
            re.sub(r"^\W+", "", d.get("verdict_domiciliation", "")).strip(), s.get("base_quali") or d.get("base_quali_crm", ""),
            s.get("debut_bail", ""), "oui" if s.get("flexoffice") else "",
            (s.get("operateur_flex") or "").replace("(aucun)", "").replace(" (probable)" if s.get("flexoffice") else "(probable)",
                                                    "" if s.get("flexoffice") else "(à confirmer)").strip(),
            d.get("id", ""), _date(s["validee"])], _adresse=s["adresse_retenue"]))

    # ── contacts (nouveaux validés par les négos)
    t["new"], t["exist"] = [], []
    for s in socs:
        if s["validation"] == "Écartée":
            continue
        dirs = {VALID.cle_contact(x): x for x in VALID.nouveaux_dirigeants(s["data"] or {})}
        dec_a = {str(x.get("id")): x for x in s["decisions"] if x.get("type") == "ailleurs"}
        for cle, e in s["nouveaux"].items():
            if e.get("etat") != "prêt pour HubSpot":
                continue
            d = dirs.get(cle, {})
            f = VALID.fiche_nouveau(d, e) if d else {k: e.get("final_" + k, e.get(k, "")) for k in VALID.CHAMPS_MODIFIABLES}
            nom_complet = e.get("nom_complet") or d.get("nom_complet", "")
            prenom, nom = _prenom_nom(nom_complet, e.get("prenom") or d.get("prenom", ""), e.get("nom") or d.get("nom", ""))
            # adresse passée par Captain Verify (et jugée ok) ; une adresse saisie à la main ne l'est pas
            cv = "oui" if f.get("email") and d.get("cv_statut") == "ok" and "email" not in e.get("modifie", []) else "non"
            commun = [prenom, nom, f.get("email", ""), f.get("telephone", ""), f.get("fonction", ""), f.get("role", ""),
                      f.get("linkedin", ""), domaine_de[s["page"]], s["nom"]]
            societe_ok = "oui" if s["validation"] in ("Société validée", "Contacts validés") else "non (adresse pas encore validée)"
            saisies = ", ".join(e.get("modifie", []))
            extra = {"_nom_complet": nom_complet or f"{prenom} {nom}".strip(), "_org_id": (s["data"] or {}).get("id", "")}
            if e.get("action") == "existant":
                a = dec_a.get(str(e.get("fiche_existante")), {})
                t["exist"].append(_ligne(H_EXIST, commun + [
                    e.get("fiche_existante", ""), (d.get("ailleurs_crm") or {}).get("org_nom", "") or a.get("societe_actuelle", ""),
                    a.get("action", "à décider"), _date(e.get("le")), societe_ok, saisies, cv], **extra))
            else:
                t["new"].append(_ligne(H_NEW, commun + [d.get("id") or (s["data"] or {}).get("id", ""), _date(e.get("le")), societe_ok, saisies,
                                                        e.get("modifie_par", ""), cv], **extra))

    # ── contacts déjà au CRM (décisions admin)
    t["crm"] = []
    for s in socs:
        crm = {str(p.get("id")): p for p in (s["data"] or {}).get("contacts_crm", [])}
        for x in s["decisions"]:
            if (x.get("type") or "crm") != "crm":
                continue
            p = crm.get(str(x.get("id")), {})
            nom_complet = x.get("nom_complet") or p.get("nom_complet", "")
            prenom, nom = _prenom_nom(nom_complet)
            t["crm"].append(_ligne(H_CRM, [
                x.get("action", ""), prenom, nom, x.get("email") or p.get("email", ""), x.get("telephone") or p.get("telephone", ""),
                x.get("fonction") or p.get("fonction", ""), x.get("role") or p.get("role", ""), x.get("linkedin") or p.get("linkedin", ""),
                domaine_de[s["page"]], s["nom"], x.get("id", ""), x.get("statut") or p.get("statut", ""), p.get("detail", ""),
                _date(x.get("le") or s["validee"]), "non"], _nom_complet=nom_complet, _org_id=(s["data"] or {}).get("id", "")))
    ordre = {"à supprimer": 0, "marquer parti": 1, "à revoir": 2, "garder": 3}
    t["crm"].sort(key=lambda l: (ordre.get(l["Décision"], 3), l["Société"]))

    # ── contacts partis retrouvés et validés : mise à jour de la fiche existante (nouvelle société, poste, e-mail)
    import v3_partis as PARTIS
    t["partis"], t["soc_partis"], vus = [], [], set()
    for x in [x for x in PARTIS.liste(socs) if x.get("etat") == "validé"]:
        prenom, nom = _prenom_nom(x.get("nom_complet", ""))
        t["partis"].append(_ligne(H_PARTIS, [
            x.get("ancien_email", ""), x.get("email", ""), prenom, nom, x.get("fonction", ""), x.get("role", ""),
            x.get("linkedin", ""), x.get("domaine", ""), x.get("nouvelle_societe", ""), x.get("ancienne_societe", ""),
            x.get("id", ""), " ; ".join(x.get("alertes", [])), _date(x.get("decide_le") or x.get("le"))], _nom_complet=x.get("nom_complet", "")))
        cle = x.get("domaine") or x.get("nouvelle_societe")
        if cle and cle not in vus:
            vus.add(cle)
            rue, cp, ville = decouper_adresse(x.get("adresse", ""))
            t["soc_partis"].append(_ligne(H_SOC_PARTIS, [x.get("domaine", ""), x.get("nouvelle_societe", ""), x.get("linkedin_societe", ""),
                                                         rue, cp, ville, x.get("effectif", "")], _adresse=x.get("adresse", "")))

    # ── sociétés écartées, à supprimer, repreneurs
    t["ecart"] = [_ligne(H_ECART, [s["nom"], domaine_de[s["page"]], s["siren"], (s["data"] or {}).get("id", ""),
                                   s.get("decision_ecart") or "Hors cible HubSpot", s.get("motif_ecart", ""), s.get("repreneur", ""),
                                   _date(s["validee"])], _decision=s.get("decision_ecart") or "") for s in ecartees]
    t["suppr"] = [_ligne(H_SUPPR, [s["nom"], (s["data"] or {}).get("id", ""), s["siren"], domaine_de[s["page"]], s.get("motif_ecart", ""),
                                   s.get("repreneur", ""), _date(s["validee"])])
                  for s in ecartees if s.get("decision_ecart") == "À supprimer de la base"]
    t["repris"] = [_ligne(H_REPRIS, [s["repreneur"], s["nom"], s["siren"], s.get("adresse_retenue", ""), _date(s["validee"])])
                   for s in ecartees if s.get("repreneur")]          # repreneurs et nouveaux noms : à lancer dans l'enrichisseur
    return t


def _resume(t: dict) -> dict:
    return {"societes": len(t["soc"]), "contacts": len(t["new"]), "existants": len(t["exist"]), "crm": len(t["crm"]),
            "ecartees": t["ecartees_n"], "a_supprimer": len(t["suppr"]), "partis": len(t["partis"]), "repreneurs": len(t["repris"])}


def _lisez_moi(wb, lignes: list):
    ws = wb.active
    ws.title = "Lisez-moi"
    for l in lignes:
        ws.append([l])
    ws["A1"].font = Font(name="Arial", bold=True, size=13, color="4A1E5C")
    ws.column_dimensions["A"].width = 120


def _onglet(wb, titre: str, entetes: list, lignes: list):
    from v3 import feuille
    feuille(wb, titre, entetes, [[l.get(h, "") for h in entetes] for l in lignes])


def construire(socs: list) -> tuple:
    """Export au format d'import HubSpot."""
    auj = datetime.now(timezone.utc).date().isoformat()
    t = _tables(socs)
    l_crm = t["crm"]
    wb = Workbook()
    _lisez_moi(wb, [
        f"ENRICHISSEUR V3 · export des validations pour HubSpot · {auj}",
        "Fichier cumulatif : tout ce qui est validé à ce jour. HubSpot reconnaît les sociétés par le domaine et les contacts par l'e-mail :",
        "réimporter le même fichier met à jour les fiches au lieu de créer des doublons.", "",
        f"Sociétés validées : {len(t['soc'])}",
        f"Contacts à créer (validés par les négos) : {len(t['new'])}",
        f"Contacts déjà dans Pipedrive ailleurs (à rattacher ou déplacer) : {len(t['exist'])}",
        f"Décisions sur les contacts CRM : {len(l_crm)} (dont {sum(1 for l in l_crm if l['Décision'] == 'marquer parti')} à marquer partis, {sum(1 for l in l_crm if l['Décision'] == 'à supprimer')} à supprimer)",
        f"Sociétés écartées : {t['ecartees_n']}, dont {len(t['suppr'])} à supprimer de Pipedrive (onglet dédié)", "",
        "Contacts partis : contacts retrouvés dans une nouvelle société et validés. Mise à jour de la fiche existante :",
        "  importer avec « Email actuel » comme clé ; le nouvel e-mail va dans une propriété (« nouvel_email ») ou se corrige à la main.",
        "  Les nouvelles sociétés sont dans « Sociétés des partis » (à importer avant, pour l'association par domaine).", "",
        "Ordre d'import conseillé dans HubSpot : 1. Sociétés  2. Contacts à créer (associés par Company Domain Name)",
        "  3. Contacts existants et Contacts CRM : à traiter à la main ou en import de mise à jour (clé = e-mail).",
        "Propriétés HubSpot à créer avant l'import : date_validation_societe, date_validation_contact, effectif_linkedin,",
        "  statut_activite, domiciliation, role_dirigeant, captain_verify (case à cocher). Les colonnes « Date validation » et",
        "  « Captain Verify » (oui = e-mail vérifié et jugé ok) sont prévues pour elles.",
        "Les contacts dont la société n'est pas encore validée sont signalés (colonne « Société validée ? »)."])
    for titre, h, cle in (("Sociétés", H_SOC, "soc"), ("Contacts à créer", H_NEW, "new"), ("Contacts existants", H_EXIST, "exist"),
                          ("Contacts CRM", H_CRM, "crm"), ("Contacts partis", H_PARTIS, "partis"),
                          ("Sociétés des partis", H_SOC_PARTIS, "soc_partis"), ("Sociétés écartées", H_ECART, "ecart"),
                          ("À supprimer", H_SUPPR, "suppr"), ("Repreneurs", H_REPRIS, "repris")):
        _onglet(wb, titre, h, t[cle])
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue(), _resume(t)


# ───────────────────────── export Pipedrive (19/11/2026 : HubSpot prend du retard) ─────────────────────────
# Mêmes lignes que l'export HubSpot. Différences : Pipedrive reconnaît une fiche existante par son identifiant
# (« Pipedrive System ID ») et non par le domaine ou l'e-mail, l'adresse tient dans un seul champ, et une personne
# est rattachée à une organisation par l'identifiant ou le nom de celle-ci. Les identifiants sont ceux du compte
# Pipedrive de la clé PIPEDRIVE_API_KEY de l'outil.
# Chaque colonne : (titre dans le fichier Pipedrive, clé dans la ligne). Une clé absente de la ligne donne une case vide.
_ID_ORG, _ID_PERS = "ID Pipedrive (organisation)", "ID Pipedrive (personne)"
_ORG_FIN = [("SIREN", "SIREN"), ("Raison sociale", "Raison sociale"), ("LinkedIn", "LinkedIn Company Page"),
            ("Source de l'adresse", "Source de l'adresse"), ("Effectif LinkedIn", "Effectif LinkedIn"),
            ("Date relevé effectif", "Date relevé effectif"), ("Croissance effectif (%)", "Croissance effectif (%)"),
            ("STATUT ACTIVITÉ", "STATUT ACTIVITÉ"), ("CODE NAF", "CODE NAF"), ("ACTIVITÉ", "ACTIVITÉ"), ("SECTEUR", "SECTEUR"),
            ("DOMICILIATION", "DOMICILIATION"), ("BASE QUALI", "Base quali"), ("Début de bail", "Début de bail"),
            ("Flexoffice", "Flexoffice"), ("OPERATEUR FLEX", "Opérateur flex"), ("Date validation société", "Date validation société")]
P_ORG_MAJ = [(_ID_ORG, "ID Pipedrive"), ("Nom", "Name"), ("Adresse", "_adresse"), ("Site web", "Company Domain Name")] + _ORG_FIN
P_ORG_NEW = [("Nom", "Name"), ("Adresse", "_adresse"), ("Site web", "Company Domain Name")] + _ORG_FIN
_PERS = [("Nom", "_nom_complet"), ("Prénom", "First Name"), ("Nom de famille", "Last Name"), ("E-mail", "Email"),
         ("Téléphone", "Phone Number"), ("Fonction", "Job Title"), ("Rôle", "Rôle"), ("LinkedIn", "LinkedIn URL")]
P_NEW = _PERS + [("Organisation", "Société"), (_ID_ORG, "_org_id"), ("Site web société", "Company Domain Name"),
                 ("Date validation contact", "Date validation contact"), ("Société validée ?", "Société validée ?"),
                 ("Champs saisis à la main", "Champs saisis à la main"), ("Saisi par", "Saisi par"), ("Captain Verify", "Captain Verify")]
P_EXIST = [(_ID_PERS, "ID personne Pipedrive")] + _PERS + [
    ("Organisation", "Nouvelle société"), (_ID_ORG, "_org_id"), ("Site web société", "Company Domain Name"),
    ("Fiché aujourd'hui chez", "Fiché aujourd'hui chez"), ("Décision admin", "Décision admin"),
    ("Date validation contact", "Date validation contact"), ("Société validée ?", "Société validée ?"),
    ("Champs saisis à la main", "Champs saisis à la main"), ("Captain Verify", "Captain Verify")]
P_CRM = [(_ID_PERS, "ID personne Pipedrive"), ("Décision", "Décision")] + _PERS + [
    ("Organisation", "Société"), (_ID_ORG, "_org_id"), ("Site web société", "Company Domain Name"), ("Contrôle", "Contrôle"),
    ("Détail du contrôle", "Détail du contrôle"), ("Date validation", "Date validation"), ("Captain Verify", "Captain Verify")]
P_PARTIS = [(_ID_PERS, "ID personne Pipedrive"), ("Nom", "_nom_complet"), ("Prénom", "First Name"), ("Nom de famille", "Last Name"),
            ("E-mail", "Nouvel e-mail"), ("Ancien e-mail", "Email actuel (clé HubSpot)"), ("Fonction", "Job Title"), ("Rôle", "Rôle"),
            ("LinkedIn", "LinkedIn URL"), ("Organisation", "Nouvelle société"), ("Site web société", "Company Domain Name"),
            ("Ancienne société", "Ancienne société"), ("Alertes", "Alertes"), ("Date validation", "Date validation")]
P_SOC_PARTIS = [("Nom", "Name"), ("Adresse", "_adresse"), ("Site web", "Company Domain Name"), ("LinkedIn", "LinkedIn Company Page"),
                ("Effectif LinkedIn", "Effectif LinkedIn")]
P_ECART = [(_ID_ORG, "ID Pipedrive"), ("Organisation", "Société"), ("Site web", "Domaine"), ("SIREN", "SIREN"), ("Décision", "_decision"),
           ("Motif", "Motif"), ("Repreneur", "Repreneur"), ("Écartée le", "Écartée le")]
P_SUPPR = [(_ID_ORG, "ID Pipedrive"), ("Organisation", "Société"), ("SIREN", "SIREN"), ("Site web", "Domaine"), ("Motif", "Motif"),
           ("Repreneur", "Repreneur"), ("Décidé le", "Décidé le")]
P_REPRIS = [("Repreneur ou nouveau nom", "Repreneur ou nouveau nom"), ("Ancienne société", "Ancienne société"),
            ("SIREN ancienne société", "SIREN ancienne société"), ("Adresse retenue", "Adresse retenue"), ("Écartée le", "Écartée le")]


def _onglet_pd(wb, titre: str, colonnes: list, lignes: list):
    from v3 import feuille
    def case(l, k):
        v = l.get(k)
        if k == "_decision":
            return v or "Hors cible"
        return "" if v is None else v
    feuille(wb, titre, [c for c, _ in colonnes], [[case(l, k) for _, k in colonnes] for l in lignes])


# Onglets du fichier Pipedrive, dans l'ordre d'import
ONGLETS_PD = ["Organisations à mettre à jour", "Organisations à créer", "Organisations des partis", "Personnes à créer",
              "Personnes à rattacher", "Personnes CRM", "Personnes parties", "Organisations écartées", "À supprimer", "Repreneurs"]


def construire_pipedrive(socs: list) -> tuple:
    """Export au format d'import Pipedrive : mêmes lignes que l'export HubSpot."""
    auj = datetime.now(timezone.utc).date().isoformat()
    t = _tables(socs)
    maj = [l for l in t["soc"] if str(l.get("ID Pipedrive") or "").strip()]
    neuves = [l for l in t["soc"] if not str(l.get("ID Pipedrive") or "").strip()]
    l_crm = t["crm"]
    wb = Workbook()
    _lisez_moi(wb, [
        f"ENRICHISSEUR V3 · export des validations pour Pipedrive · {auj}",
        "Mêmes données que l'export HubSpot, au format d'import Pipedrive. Fichier cumulatif : tout ce qui est validé à ce jour.",
        "Pipedrive reconnaît une fiche existante par son identifiant : dans l'écran d'import, associer chaque colonne",
        f"« {_ID_ORG} » au champ « Pipedrive System ID » de l'organisation et « {_ID_PERS} » à celui de la personne.",
        "Avec l'identifiant, l'import met la fiche à jour au lieu d'en créer une autre. Sans identifiant, il crée la fiche.", "",
        f"Organisations à mettre à jour (déjà dans Pipedrive) : {len(maj)}",
        f"Organisations à créer (pas encore dans Pipedrive) : {len(neuves)}",
        f"Personnes à créer (validées par les négos) : {len(t['new'])}",
        f"Personnes déjà dans Pipedrive ailleurs (à rattacher ou déplacer) : {len(t['exist'])}",
        f"Décisions sur les personnes CRM : {len(l_crm)} (dont {sum(1 for l in l_crm if l['Décision'] == 'marquer parti')} à marquer parties, {sum(1 for l in l_crm if l['Décision'] == 'à supprimer')} à supprimer)",
        f"Personnes parties retrouvées : {len(t['partis'])} · Organisations écartées : {t['ecartees_n']}, dont {len(t['suppr'])} à supprimer", "",
        "IMPORTER UN ONGLET À LA FOIS. Le bouton « Un onglet » de l'écran de validation télécharge chaque onglet seul.",
        "Ordre conseillé :",
        "  1. Organisations à mettre à jour   2. Organisations à créer   3. Organisations des partis",
        "  4. Personnes à créer (rattachées par l'identifiant de l'organisation, sinon par son nom)",
        "  5. Personnes à rattacher, Personnes CRM, Personnes parties : mise à jour par l'identifiant de la personne.",
        "     « Personnes parties » : la colonne E-mail porte le nouvel e-mail ; l'organisation est la nouvelle société.",
        "  6. Organisations écartées, À supprimer, Repreneurs : pour information. L'import ne supprime rien :",
        "     filtrer la liste Organisations sur les identifiants de l'onglet « À supprimer » et supprimer en masse.",
        "     Les décisions « à supprimer » de l'onglet Personnes CRM se traitent de la même façon.", "",
        "Avant d'importer : chaque colonne doit correspondre à un champ Pipedrive (Préférences > Champs de données).",
        "  Champs déjà connus : SIREN, BASE QUALI, OPERATEUR FLEX. Pour les autres (Effectif LinkedIn, STATUT ACTIVITÉ,",
        "  DOMICILIATION, Captain Verify, Rôle, Date validation…), créer le champ ou choisir « Ne pas importer ».",
        "  Valeurs des listes déroulantes (BASE QUALI, OPERATEUR FLEX) : elles doivent être écrites comme dans Pipedrive.",
        "Commencer par un essai sur une cinquantaine de lignes avant l'import complet.",
        "Les personnes dont l'organisation n'est pas encore validée sont signalées (colonne « Société validée ? »)."])
    for titre, cols, lignes in (("Organisations à mettre à jour", P_ORG_MAJ, maj), ("Organisations à créer", P_ORG_NEW, neuves),
                                ("Organisations des partis", P_SOC_PARTIS, t["soc_partis"]), ("Personnes à créer", P_NEW, t["new"]),
                                ("Personnes à rattacher", P_EXIST, t["exist"]), ("Personnes CRM", P_CRM, l_crm),
                                ("Personnes parties", P_PARTIS, t["partis"]), ("Organisations écartées", P_ECART, t["ecart"]),
                                ("À supprimer", P_SUPPR, t["suppr"]), ("Repreneurs", P_REPRIS, t["repris"])):
        _onglet_pd(wb, titre, cols, lignes)
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue(), _resume(t) | {"org_maj": len(maj), "org_neuves": len(neuves)}


def un_onglet(xl: bytes, titre: str) -> bytes:
    """Le fichier réduit à un seul onglet (Pipedrive importe une feuille à la fois)."""
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(xl))
    for ws in list(wb.worksheets):
        if ws.title != titre:
            wb.remove(ws)
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue()


def nom_fichier(fmt: str = "hubspot", onglet: str = "") -> str:
    suite = "_" + re.sub(r"\W+", "_", onglet.lower()).strip("_") if onglet else ""
    return f"enrichisseur_v3_export_{fmt}{suite}_{datetime.now(timezone.utc):%Y-%m-%d}.xlsx"


# Export en tâche de fond : lire des centaines de fiches Notion prend plusieurs minutes (une requête Notion toutes les
# 0,4 s pour tout le serveur), bien plus que ce qu'un navigateur attend. Le fichier prêt reste disponible une heure.
# Une seule lecture de la mémoire sert aux deux fichiers : HubSpot (« xl ») et Pipedrive (« xl_pd »).
EXPORT = {"actif": False, "fait": 0, "total": 0, "phase": "", "erreur": "", "xl": None, "resume": {}, "pret_le": 0.0,
          "mail": False, "envoi": "", "xl_pd": None, "resume_pd": {}, "mails": []}
EXPORT_GARDE = 3600
_FORMATS = {"hubspot": ("xl", "resume", "HubSpot"), "pipedrive": ("xl_pd", "resume_pd", "Pipedrive")}


def _pret(fmt: str = "hubspot") -> bool:
    return bool(EXPORT.get(_FORMATS[fmt][0])) and time.time() - EXPORT["pret_le"] < EXPORT_GARDE


def _corps_mail(r: dict, fmt: str = "hubspot") -> str:
    lignes = ["Bonjour,", "", f"Voici l'export des validations de l'enrichisseur v3, prêt pour l'import {_FORMATS[fmt][2]}.", ""]
    if fmt == "pipedrive":
        lignes += [f"Organisations à mettre à jour : {r.get('org_maj', 0)}", f"Organisations à créer : {r.get('org_neuves', 0)}",
                   f"Personnes à créer : {r['contacts']}", f"Personnes déjà dans Pipedrive ailleurs : {r['existants']}",
                   f"Décisions sur les personnes CRM : {r['crm']}"]
    else:
        lignes += [f"Sociétés validées : {r['societes']}", f"Contacts à créer : {r['contacts']}",
                   f"Contacts déjà dans Pipedrive ailleurs : {r['existants']}", f"Décisions sur les contacts CRM : {r['crm']}"]
    return "\n".join(lignes + [
        f"Sociétés écartées : {r['ecartees']} (dont {r.get('a_supprimer', 0)} à supprimer de Pipedrive)",
        f"Contacts partis retrouvés (mise à jour) : {r.get('partis', 0)}", "",
        *([f"Fiches Notion illisibles, absentes de ce fichier : {', '.join(r['sautees'])}", ""] if r.get("sautees") else []),
        "Le fichier est cumulatif : tout ce qui est validé à ce jour. Le mode d'emploi est dans l'onglet Lisez-moi.", "",
        "Enrichisseur v3"])


def _sujet(fmt: str) -> str:
    return f"Enrichisseur v3 · export {_FORMATS[fmt][2]} du {datetime.now():%d/%m/%Y}"


async def _envoyer_mail(fmt: str):
    cle, res, _ = _FORMATS[fmt]
    await asyncio.to_thread(SUIVI._envoyer, SUIVI.COPIE_COMPLETE, _sujet(fmt), _corps_mail(EXPORT[res], fmt), EXPORT[cle], nom_fichier(fmt))


async def _preparer():
    EXPORT.update(actif=True, phase="lecture de la mémoire Notion", erreur="", fait=0, total=0, envoi="", sautees=[], en_lecture={})
    try:
        socs = await _donnees(EXPORT)
        xl, r = construire(socs)
        sautees = {"sautees": list(EXPORT.get("sautees", []))}
        try:
            xl_pd, r_pd = construire_pipedrive(socs)
        except Exception as e:                       # l'export Pipedrive ne doit jamais bloquer l'export HubSpot
            print(f"[V3 EXPORT PIPEDRIVE] {e}")
            xl_pd, r_pd = None, {}
        EXPORT.update(xl=xl, resume=r | sautees, xl_pd=xl_pd, resume_pd=r_pd | sautees, pret_le=time.time(), phase="prêt")
        if EXPORT["mail"] and "hubspot" not in EXPORT["mails"]:      # ancien appel _lancer(True)
            EXPORT["mails"].append("hubspot")
        envois = []
        for fmt in list(EXPORT["mails"]):
            if not EXPORT.get(_FORMATS[fmt][0]):
                continue
            EXPORT["phase"] = "envoi du mail"
            await _envoyer_mail(fmt)
            envois.append(_FORMATS[fmt][2])
        if envois:
            EXPORT["envoi"] = f"{' et '.join(envois)} envoyé à {', '.join(SUIVI.COPIE_COMPLETE)}"
        EXPORT["phase"] = "prêt"
    except Exception as e:
        EXPORT.update(erreur=f"{type(e).__name__} : {e}", phase="erreur")
        print(f"[V3 EXPORT] {e}")
    finally:
        EXPORT.update(actif=False, mail=False, mails=[])


def _lancer(mail):
    """mail : False, True (= HubSpot, ancien appel) ou le format à envoyer (« hubspot » / « pipedrive »)."""
    if mail:
        fmt = "hubspot" if mail is True else mail
        if fmt not in EXPORT["mails"]:
            EXPORT["mails"].append(fmt)
    if not EXPORT["actif"]:
        asyncio.create_task(_preparer())


def _notion() -> str:
    import time as _t, notion_debit as D
    pause = D._ETAT["pause_jusqua"] - _t.monotonic()
    return (f"Notion demande de patienter ({int(pause)} s)" if pause > 0 else "") + \
           (f" · {D._ETAT['refus']} refus Notion depuis le démarrage du serveur" if D._ETAT["refus"] else "")


def _etat() -> dict:
    return {k: EXPORT.get(k) for k in ("actif", "fait", "total", "phase", "erreur", "envoi")} | {
        "sautees": EXPORT.get("sautees", []), "en_lecture": list((EXPORT.get("en_lecture") or {}).values()), "notion": _notion(),
        "pret": _pret(), "resume": EXPORT["resume"] if _pret() else {},
        "pret_pipedrive": _pret("pipedrive"), "resume_pipedrive": EXPORT["resume_pd"] if _pret("pipedrive") else {},
        "onglets_pipedrive": ONGLETS_PD,
        "pret_depuis_min": int((time.time() - EXPORT["pret_le"]) // 60) if _pret() else None}


_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _servir(fmt: str, refaire: int, onglet: str = "") -> HTMLResponse | StreamingResponse:
    cle = _FORMATS[fmt][0]
    url = "/v3/validation/api/export" + ("-pipedrive" if fmt == "pipedrive" else "")
    if _pret(fmt) and not refaire and not EXPORT["actif"]:
        xl = EXPORT[cle]
        if onglet:
            if onglet not in ONGLETS_PD:
                return HTMLResponse(f"Onglet inconnu : {onglet}", 404)
            xl = un_onglet(xl, onglet)
        return StreamingResponse(io.BytesIO(xl), headers={"Content-Disposition": f'attachment; filename="{nom_fichier(fmt, onglet)}"'},
                                 media_type=_XLSX)
    if refaire and not EXPORT["actif"]:
        EXPORT.update(xl=None, xl_pd=None, pret_le=0.0)
    _lancer(False)
    if EXPORT["erreur"] and not EXPORT["actif"]:
        msg = f"La préparation a échoué : {EXPORT['erreur']}. Rechargez la page pour réessayer."
    else:
        avance = f"{EXPORT['fait']} / {EXPORT['total']} sociétés lues" if EXPORT["total"] else "démarrage"
        msg = f"Préparation de l'export {_FORMATS[fmt][2]} en cours ({avance}). Le téléchargement démarre tout seul quand le fichier est prêt."
        enc = list((EXPORT.get("en_lecture") or {}).values())
        if enc:
            msg += f"<br><small>En lecture : {', '.join(enc)}. Une fiche qui ne répond pas est sautée au bout de {LECTURE_MAX // 60} min.</small>"
        if (n := _notion()):
            msg += f"<br><small>{n}</small>"
    from urllib.parse import quote
    retour = url + (f"?onglet={quote(onglet)}" if onglet else "")
    return HTMLResponse(f"""<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="8;url={retour}">
<title>Export en préparation</title><body style="font:16px system-ui;margin:40px;color:#2b2233;background:#faf8f5">
<p>{msg}</p><p style="color:#776">Cette page se recharge toutes les 8 secondes. Comptez quelques minutes.</p>
<p><a href="/v3/validation">Retour à la validation</a></p></body>""")


@router.get("/v3/validation/api/export")
async def telecharger(req: Request, refaire: int = 0):
    """Fichier prêt depuis moins d'une heure : téléchargé tout de suite (&refaire=1 pour en refaire un).
    Sinon la préparation part en tâche de fond et une page d'attente se recharge toute seule."""
    if (x := VALID._refus(req, "admin")):
        return x
    return _servir("hubspot", refaire)


@router.get("/v3/validation/api/export-pipedrive")
async def telecharger_pipedrive(req: Request, refaire: int = 0, onglet: str = ""):
    """Même chose au format Pipedrive ; &onglet=… pour un seul onglet (Pipedrive importe une feuille à la fois)."""
    if (x := VALID._refus(req, "admin")):
        return x
    return _servir("pipedrive", refaire, onglet)


@router.get("/v3/validation/api/export/etat")
async def export_etat(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    return _etat()


async def _mail(req: Request, fmt: str):
    if (x := VALID._refus(req, "admin")):
        return x
    if not (SUIVI.SMTP_USER and SUIVI.SMTP_PASS and SUIVI.COPIE_COMPLETE):
        return JSONResponse({"erreur": "envoi de mail non configuré (SMTP_USER / SMTP_PASS / COPIE_FICHIER_COMPLET)"}, 500)
    if _pret(fmt) and not EXPORT["actif"]:                # fichier tout frais : envoyé tel quel
        try:
            await _envoyer_mail(fmt)
        except Exception as e:
            return JSONResponse({"erreur": f"envoi impossible : {e}"}, 502)
        return _etat() | {"envoye_a": SUIVI.COPIE_COMPLETE}
    _lancer(fmt)
    return _etat() | {"en_preparation": True, "envoye_a": SUIVI.COPIE_COMPLETE}


@router.post("/v3/validation/api/export/mail")
async def envoyer(req: Request):
    """Prépare l'export en tâche de fond et l'envoie par mail ; répond tout de suite."""
    return await _mail(req, "hubspot")


@router.post("/v3/validation/api/export-pipedrive/mail")
async def envoyer_pipedrive(req: Request):
    return await _mail(req, "pipedrive")


# ───────────────────────── liste de phoning d'un fichier lancé ─────────────────────────
_SUITE = re.compile(r"( \((suite \d+/\d+|relance[^)]*)\))+$")


@router.get("/v3/contacts/api/phoning")
async def phoning(req: Request, lancement: str = ""):
    """Excel de phoning d'un fichier lancé (toutes ses suites) : nouveaux contacts validés (« prêt pour HubSpot »)
    + contacts déjà dans Pipedrive (sauf ceux marqués partis / à supprimer).
    Admin : tout ; négo : ses sociétés seulement (même règle que son écran)."""
    if (x := VALID._refus(req, "negos")):
        return x
    base = _SUITE.sub("", lancement.strip())
    if len(base) < 3:
        return JSONResponse({"erreur": "précisez le fichier (?lancement=…)"}, 400)
    qui = await VALID.moi(req)
    if not qui.get("role"):
        return JSONResponse({"erreur": "accès refusé : reconnectez-vous"}, 403)
    socs = [s for s in await VALID._toutes()
            if any(_SUITE.sub("", l.strip()) == base for l in (s.get("lancement") or "").split(";"))
            and s["validation"] != "Écartée" and VALID.visible_par(qui, s)]

    async def lire(s):
        for essai in range(3):
            try:
                async with _SEM:
                    s["data"] = await MEM.lire(s["page"])
                break
            except Exception:
                s["data"] = {}
                await asyncio.sleep(2 + 2 * essai)
        s["nouveaux"] = VALID._json(s["nouveaux"], {})
        s["decisions"] = VALID._json(s["decisions"], [])
        return s
    socs = await asyncio.gather(*(lire(s) for s in socs))
    lignes = []
    for s in sorted(socs, key=lambda s: (s["nom"] or "").lower()):
        d0 = s["data"] or {}
        adresse = s.get("adresse_retenue") or d0.get("adresse_proposee") or d0.get("adresse_crm") or ""
        soc = [s["nom"], s.get("domaine") or d0.get("domaine", ""), adresse, d0.get("effectif_linkedin", "")]
        # nouveaux contacts validés
        dirs = {VALID.cle_contact(x): x for x in VALID.nouveaux_dirigeants(d0)}
        for cle, e in s["nouveaux"].items():
            if e.get("etat") != "prêt pour HubSpot":
                continue
            d = dirs.get(cle, {})
            f = VALID.fiche_nouveau(d, e) if d else {k: e.get("final_" + k, e.get(k, "")) for k in VALID.CHAMPS_MODIFIABLES}
            prenom, nom = _prenom_nom(e.get("nom_complet") or d.get("nom_complet", ""), e.get("prenom") or d.get("prenom", ""),
                                      e.get("nom") or d.get("nom", ""))
            autre = (d.get("ailleurs_crm") or {}).get("org_nom", "")
            lignes.append(soc + ["Nouveau contact", prenom, nom, f.get("fonction", ""), f.get("role", ""), f.get("email", ""),
                                 f.get("telephone", ""), f.get("linkedin", ""),
                                 f"validé le {_date(e.get('le'))}" + (f" · déjà dans Pipedrive chez {autre}" if autre else ""),
                                 s.get("negos", ""), "", ""])
        # contacts déjà dans Pipedrive (corrections de l'écran admin comprises)
        dec = {str(x.get("id")): x for x in s["decisions"] if (x.get("type") or "crm") == "crm"}
        for p in d0.get("contacts_crm", []):
            x = dec.get(str(p.get("id")), {})
            if x.get("action") in ("marquer parti", "à supprimer") or p.get("statut") == "probablement parti" and not x:
                continue
            v = lambda k: (x.get(k) or "").strip() or p.get(k, "")
            prenom, nom = _prenom_nom(p.get("nom_complet", ""))
            etat = x.get("action") or p.get("statut") or "non contrôlé"
            lignes.append(soc + ["Pipedrive", prenom, nom, v("fonction"), v("role"), v("email"), v("telephone"), v("linkedin"),
                                 f"{etat} · fiche Pipedrive {p.get('id', '')}" + (f" (doublon {p['fiche_doublon']})" if p.get("fiche_doublon") else ""),
                                 s.get("negos", ""), "", ""])
    from v3 import feuille
    wb = Workbook()
    wb.remove(wb.active)
    feuille(wb, "Phoning", ["Société", "Site", "Adresse des bureaux", "Effectif LinkedIn", "Source", "Prénom", "Nom", "Fonction",
                            "Rôle", "E-mail", "Téléphone", "LinkedIn", "Statut", "Négo", "Date d'appel", "Résultat / notes"], lignes)
    buf = io.BytesIO(); wb.save(buf)
    nom = re.sub(r"[^A-Za-z0-9_-]+", "_", base)[:50].strip("_") or "fichier"
    return StreamingResponse(io.BytesIO(buf.getvalue()), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="phoning_{nom}_{datetime.now():%Y-%m-%d}.xlsx"'})
