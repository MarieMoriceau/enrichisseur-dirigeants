"""
Enrichisseur v3 — quel compte LinkedIn (Sales Navigator) sert à quoi

  · Deux comptes Sales Navigator, chacun avec ses propres phantoms PhantomBuster (le cookie LinkedIn est
    enregistré dans le phantom, pas dans le code) :
      - compte « automatisations » = les phantoms actuels (PB_COMPANY_SCRAPER_ID, PB_SALESNAV_ID, PB_PROFILE_SCRAPER_ID).
        Il sert aux listes outils (géolocalisation, prospection hebdo), et en dehors de l'enrichisseur à la veille
        LinkedIn et à la prospection par adresse pivot ;
      - compte « négos » = les phantoms dupliqués avec le cookie du 2e compte (mêmes variables suffixées _NEGOS).
        Il ne sert qu'aux demandes des négociateurs.
  · Plus aucun créneau horaire : chaque compte tourne quand il veut, seul son quota compte (150 sociétés par jour
    et par compte, QUOTA_LINKEDIN_JOUR).
  · Quota du jour atteint : le reliquat repart le lendemain à l'heure de reprise (REPRISE_HEURE, 8 h par défaut,
    heure de Paris), sur le même compte.
  · Tant que les phantoms _NEGOS ne sont pas dans Render, tout tourne sur le compte unique, avec un quota de 150
    partagé entre outils et négos (comme avant).

Heures : Render tourne en UTC. Tout est calculé en Europe/Paris avec zoneinfo (paquet tzdata dans requirements.txt).

Test sans attendre : GET /v3/fenetre?cle=<CRON_SECRET>&simuler=07:58 fait croire à l'outil qu'il est 7 h 58 à Paris
(l'horloge simulée avance ensuite normalement) ; &simuler=off revient à l'heure réelle. La simulation ne concerne que
l'heure de reprise des listes reportées, jamais le quota ni les dates enregistrées.
"""
import os, re
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")
OUTILS, NEGOS = "outils", "negos"
UNIQUE = "unique"
LIBELLE = {OUTILS: "liste outils (géolocalisation / prospection hebdo)", NEGOS: "demande de négo"}
NOM_COMPTE = {OUTILS: "automatisations", NEGOS: "négos", UNIQUE: "unique"}


def _heure(txt: str, defaut: str) -> dtime:
    m = re.fullmatch(r"\s*(\d{1,2})[:h](\d{2})\s*", txt or "") or re.fullmatch(r"(\d{1,2})[:h](\d{2})", defaut)
    return dtime(int(m.group(1)), int(m.group(2)))


REPRISE = _heure(os.getenv("REPRISE_HEURE", ""), "08:00")   # heure de départ des listes reportées (modifiable dans Render)

_decalage = timedelta(0)                                      # simulation d'heure (tests), remise à zéro au redémarrage


def origine_valide(o: str) -> str:
    return OUTILS if o == OUTILS else NEGOS


def deux_comptes() -> bool:
    """Le compte négos est branché quand ses deux phantoms indispensables sont dans Render."""
    return bool(os.getenv("PB_COMPANY_SCRAPER_ID_NEGOS", "") and os.getenv("PB_SALESNAV_ID_NEGOS", ""))


def compte(origine: str) -> str:
    """Compte LinkedIn utilisé par une origine : « outils », « negos », ou « unique » tant qu'il n'y a qu'un compte."""
    return origine_valide(origine) if deux_comptes() else UNIQUE


def comptes() -> list:
    return [OUTILS, NEGOS] if deux_comptes() else [UNIQUE]


def origines_du_compte(c: str) -> list:
    return [OUTILS, NEGOS] if c == UNIQUE else [c]


def maintenant() -> datetime:
    """Heure de Paris (simulée si une simulation est en cours)."""
    return datetime.now(PARIS) + _decalage


def _a(jour: datetime, h: dtime) -> datetime:
    return datetime(jour.year, jour.month, jour.day, h.hour, h.minute, tzinfo=PARIS)


def reprise_du(jour: datetime) -> datetime:
    return _a(jour.astimezone(PARIS), REPRISE)


def prochaine_reprise(t: datetime | None = None) -> datetime:
    """Prochaine heure de reprise strictement après t (aujourd'hui si elle n'est pas encore passée, sinon demain)."""
    t = (t or maintenant()).astimezone(PARIS)
    d = reprise_du(t)
    return d if d > t else reprise_du(t + timedelta(days=1))


def simuler(valeur: str) -> str:
    """« 07:58 » (aujourd'hui à Paris), « 2026-10-19T07:58 » (date précise, heure de Paris) ou « off »."""
    global _decalage
    v = (valeur or "").strip()
    if v.lower() in ("", "off", "non", "0"):
        _decalage = timedelta(0)
        return "heure réelle"
    reel = datetime.now(PARIS)
    if re.fullmatch(r"\d{1,2}[:h]\d{2}", v):
        cible = _a(reel, _heure(v, "00:00"))
    else:
        cible = datetime.fromisoformat(v)
        cible = cible.replace(tzinfo=PARIS) if cible.tzinfo is None else cible.astimezone(PARIS)
    _decalage = cible - reel
    t = maintenant()
    return f"heure simulée : {JOURS_FR[t.weekday()]} {t:%d/%m %H:%M} (Paris)"


JOURS_FR = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]


def libelle_jour(d: datetime) -> str:
    """« aujourd'hui à 8 h », « demain à 8 h », « lundi 19/10 à 8 h »…"""
    d = d.astimezone(PARIS)
    auj = maintenant().date()
    h = f"{d.hour} h" + (f" {d.minute:02d}" if d.minute else "")
    if d.date() == auj:
        return f"aujourd'hui à {h}"
    if d.date() == auj + timedelta(days=1):
        return f"demain à {h}"
    return f"{JOURS_FR[d.weekday()]} {d:%d/%m} à {h}"


def _h(x: dtime) -> str:
    return f"{x.hour} h" + (f" {x.minute:02d}" if x.minute else "")


def compte_txt(origine: str) -> str:
    """Phrase courte pour l'écran : sur quel compte part une origine."""
    c = compte(origine)
    return "compte LinkedIn unique (quota partagé)" if c == UNIQUE else f"compte LinkedIn « {NOM_COMPTE[c]} »"


def etat() -> dict:
    t = maintenant()
    if deux_comptes():
        msg = "Deux comptes LinkedIn : automatisations (listes outils) et négos. Aucun créneau, 150 sociétés par jour et par compte."
    else:
        msg = "Un seul compte LinkedIn branché : listes outils et négos partagent le quota du jour."
    msg += f" Listes reportées : {libelle_jour(prochaine_reprise(t))}."
    return {"heure_paris": t.isoformat(timespec="seconds"), "simulee": _decalage != timedelta(0),
            "deux_comptes": deux_comptes(), "comptes": [NOM_COMPTE[c] for c in comptes()],
            "reprise": _h(REPRISE), "message": msg}


# ───────────────────────── origine d'un fichier ─────────────────────────
def detecter_origine(nom_fichier: str, onglets: list, entetes: list) -> tuple:
    """(origine, libellé) d'après la forme du fichier déposé, sans rien changer aux outils qui le produisent."""
    o = [str(x or "").strip().lower() for x in onglets]
    e = [str(x or "").strip().lower() for x in entetes]
    n = (nom_fichier or "").lower()
    if "sociétés sans contact" in o or ("synthèse" in o and any("sans contact" in x for x in o)):
        return OUTILS, "géolocalisation"
    if ("à envoyer" in o and "requête envoyée" in e) or re.match(r"\d_effectifs_a_chercher_", n):
        return OUTILS, "prospection hebdo"
    return NEGOS, ""
