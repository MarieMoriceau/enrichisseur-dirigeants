"""
Enrichisseur v3 — qui utilise le compte LinkedIn, et quand (heure de Paris)

  · Listes « outils » (géolocalisation, prospection hebdo) : de 7 h à 9 h, 50 sociétés au plus par jour.
  · Demandes des négociateurs : de 9 h à 19 h, avec ce qui reste du quota du jour (150 au total, inchangé).
  · Lancé hors de son créneau : attend le prochain début de créneau (7 h ou 9 h).
  · Quota dépassé : le reliquat repart le lendemain, au début de son créneau (outils 7 h, puis négos 9 h).
  · Encore en cours à la fin du créneau (9 h pour les outils, 19 h pour les négos) : arrêt propre, reliquat reporté.

Heures : Render tourne en UTC. Tout est calculé en Europe/Paris avec zoneinfo (paquet tzdata dans requirements.txt) :
7 h, 9 h et 19 h restent 7 h, 9 h et 19 h à Paris, été comme hiver.

Test sans attendre : GET /v3/fenetre?cle=<CRON_SECRET>&simuler=08:58 fait croire à l'outil qu'il est 8 h 58 à Paris
(l'horloge simulée avance ensuite normalement) ; &simuler=off revient à l'heure réelle. La simulation ne concerne que
les créneaux, jamais le quota ni les dates enregistrées.
"""
import os, re
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")
OUTILS, NEGOS = "outils", "negos"
LIBELLE = {OUTILS: "liste outils (géolocalisation / prospection hebdo)", NEGOS: "demande de négo"}


def _heure(txt: str, defaut: str) -> dtime:
    m = re.fullmatch(r"\s*(\d{1,2})[:h](\d{2})\s*", txt or "") or re.fullmatch(r"(\d{1,2})[:h](\d{2})", defaut)
    return dtime(int(m.group(1)), int(m.group(2)))


# Créneaux (modifiables dans Render, sans toucher au code)
CRENEAUX = {
    OUTILS: (_heure(os.getenv("CRENEAU_OUTILS_DEBUT", ""), "07:00"), _heure(os.getenv("CRENEAU_OUTILS_FIN", ""), "09:00")),
    NEGOS: (_heure(os.getenv("CRENEAU_NEGOS_DEBUT", ""), "09:00"), _heure(os.getenv("CRENEAU_NEGOS_FIN", ""), "19:00")),
}
QUOTA_OUTILS = int(os.getenv("QUOTA_OUTILS_JOUR", "50"))     # part du quota du jour réservée aux listes outils, le matin
INTERVALLE = int(os.getenv("FENETRE_VERIF_S", "15"))          # le garde de fin de créneau vérifie l'heure toutes les 15 s

_decalage = timedelta(0)                                      # simulation d'heure (tests), remise à zéro au redémarrage


def origine_valide(o: str) -> str:
    return OUTILS if o == OUTILS else NEGOS


def maintenant() -> datetime:
    """Heure de Paris (simulée si une simulation est en cours)."""
    return datetime.now(PARIS) + _decalage


def _a(jour: datetime, h: dtime) -> datetime:
    return datetime(jour.year, jour.month, jour.day, h.hour, h.minute, tzinfo=PARIS)


def dans_creneau(origine: str, t: datetime | None = None) -> bool:
    t = (t or maintenant()).astimezone(PARIS)
    debut, fin = CRENEAUX[origine_valide(origine)]
    return debut <= t.time() < fin


def debut_creneau(origine: str, jour: datetime) -> datetime:
    return _a(jour.astimezone(PARIS), CRENEAUX[origine_valide(origine)][0])


def fin_creneau(origine: str, t: datetime | None = None) -> datetime:
    t = (t or maintenant()).astimezone(PARIS)
    return _a(t, CRENEAUX[origine_valide(origine)][1])


def prochain_debut(origine: str, t: datetime | None = None) -> datetime:
    """Prochain début de créneau strictement après t (aujourd'hui s'il n'est pas encore passé, sinon demain)."""
    t = (t or maintenant()).astimezone(PARIS)
    d = debut_creneau(origine, t)
    return d if d > t else debut_creneau(origine, t + timedelta(days=1))


def simuler(valeur: str) -> str:
    """« 08:58 » (aujourd'hui à Paris), « 2026-10-19T08:58 » (date précise, heure de Paris) ou « off »."""
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
    """« aujourd'hui à 9 h », « demain à 7 h », « lundi 19/10 à 9 h »…"""
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


def creneau_txt(origine: str) -> str:
    d, f = CRENEAUX[origine_valide(origine)]
    return f"{_h(d)} – {_h(f)}"


def etat() -> dict:
    t = maintenant()
    if dans_creneau(OUTILS, t):
        msg = f"Créneau des listes outils ({creneau_txt(OUTILS)}) : les demandes des négos partent à {_h(CRENEAUX[NEGOS][0])}."
    elif dans_creneau(NEGOS, t):
        msg = f"Créneau des négos ({creneau_txt(NEGOS)}) : les listes outils partent {libelle_jour(prochain_debut(OUTILS, t))}."
    else:
        msg = (f"Hors créneau : listes outils {libelle_jour(prochain_debut(OUTILS, t))}, "
               f"demandes des négos {libelle_jour(prochain_debut(NEGOS, t))}.")
    return {"heure_paris": t.isoformat(timespec="seconds"), "simulee": _decalage != timedelta(0),
            "creneau_ouvert": OUTILS if dans_creneau(OUTILS, t) else NEGOS if dans_creneau(NEGOS, t) else "",
            "message": msg, "creneaux": {o: creneau_txt(o) for o in (OUTILS, NEGOS)}, "quota_outils": QUOTA_OUTILS}


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
