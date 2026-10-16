"""
Enrichisseur v3 — règles d'occupation du compte LinkedIn (partagé avec la veille, la prospection et les négociateurs)

  1. Fenêtre des négociateurs : du lundi au vendredi, de 9 h à 19 h (heure de Paris), aucun robot ne touche au compte.
     Un run qui utilise LinkedIn ne démarre pas dans ce créneau, et un run en cours à 9 h s'interrompt proprement :
     son reliquat est reporté au prochain matin 7 h, comme quand le quota du jour est atteint.
  2. L'enrichisseur passe en dernier : ses listes reportées démarrent quand la veille LinkedIn appelle /cron en fin
     de parcours, jamais à heure fixe. Un seul run à la fois : un second appel est refusé, pas mis en attente.

Heures : Render tourne en UTC. Toutes les heures sont calculées en Europe/Paris avec zoneinfo (paquet tzdata dans
requirements.txt), donc 7 h et 9 h restent 7 h et 9 h à Paris, été comme hiver.

Test sans attendre 9 h : GET /v3/fenetre?cle=<CRON_SECRET>&simuler=08:58 fait croire à l'outil qu'il est 8 h 58
(à Paris, aujourd'hui) ; l'horloge simulée avance ensuite normalement. &simuler=off revient à l'heure réelle.
La simulation ne touche que ces règles horaires, jamais le quota ni ses dates.
"""
import os, re
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")


def _heure(txt: str, defaut: str) -> dtime:
    m = re.fullmatch(r"\s*(\d{1,2})[:h](\d{2})\s*", txt or defaut) or re.fullmatch(r"(\d{1,2})[:h](\d{2})", defaut)
    return dtime(int(m.group(1)), int(m.group(2)))


# Créneau réservé aux négociateurs (modifiable dans Render, sans toucher au code)
DEBUT = _heure(os.getenv("FENETRE_NEGOS_DEBUT", ""), "09:00")
FIN = _heure(os.getenv("FENETRE_NEGOS_FIN", ""), "19:00")
JOURS = (0, 1, 2, 3, 4)                    # lundi … vendredi ; le week-end, pas de restriction
INTERVALLE = int(os.getenv("FENETRE_VERIF_S", "15"))   # le garde de 9 h vérifie l'heure toutes les 15 s

_decalage = timedelta(0)                   # simulation d'heure (tests), remis à zéro au redémarrage


def maintenant() -> datetime:
    """Heure de Paris (simulée si une simulation est en cours)."""
    return datetime.now(PARIS) + _decalage


def dans_fenetre(t: datetime | None = None) -> bool:
    """Vrai si le compte LinkedIn appartient aux négociateurs à cet instant."""
    t = (t or maintenant()).astimezone(PARIS)
    return t.weekday() in JOURS and DEBUT <= t.time() < FIN


def fin_fenetre(t: datetime | None = None) -> datetime:
    """Heure à laquelle le créneau en cours se termine (19 h le même jour)."""
    t = (t or maintenant()).astimezone(PARIS)
    return datetime(t.year, t.month, t.day, FIN.hour, FIN.minute, tzinfo=PARIS)


def prochaine_bascule(t: datetime | None = None) -> datetime:
    """Prochain début de créneau des négociateurs (9 h d'un jour ouvré), pour l'affichage."""
    t = (t or maintenant()).astimezone(PARIS)
    for i in range(8):
        j = t + timedelta(days=i)
        d = datetime(j.year, j.month, j.day, DEBUT.hour, DEBUT.minute, tzinfo=PARIS)
        if j.weekday() in JOURS and d > t:
            return d
    return t


def simuler(valeur: str) -> str:
    """« 08:58 » (aujourd'hui à Paris), « 2026-10-19T08:58 » (date précise, heure de Paris) ou « off »."""
    global _decalage
    v = (valeur or "").strip()
    if v.lower() in ("", "off", "non", "0"):
        _decalage = timedelta(0)
        return "heure réelle"
    reel = datetime.now(PARIS)
    if re.fullmatch(r"\d{1,2}[:h]\d{2}", v):
        h = _heure(v, "00:00")
        cible = datetime(reel.year, reel.month, reel.day, h.hour, h.minute, tzinfo=PARIS)
    else:
        cible = datetime.fromisoformat(v)
        cible = cible.replace(tzinfo=PARIS) if cible.tzinfo is None else cible.astimezone(PARIS)
    _decalage = cible - reel
    t = maintenant()
    return f"heure simulée : {JOURS_FR[t.weekday()]} {t:%d/%m %H:%M} (Paris)"


def etat() -> dict:
    t = maintenant()
    ouvert = not dans_fenetre(t)
    return {"heure_paris": t.isoformat(timespec="seconds"), "simulee": _decalage != timedelta(0),
            "robots_autorises": ouvert,
            "message": (f"Créneau automatique ouvert : les robots peuvent utiliser LinkedIn jusqu'à {libelle_jour(prochaine_bascule(t))}."
                        if ouvert else
                        f"Compte LinkedIn réservé aux négociateurs jusqu'à {fin_fenetre(t):%H:%M} : aucun run LinkedIn ne démarre."),
            "creneau_negos": f"{DEBUT:%H:%M}–{FIN:%H:%M}, du lundi au vendredi (heure de Paris)"}


JOURS_FR = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]


def libelle_jour(d: datetime) -> str:
    """« demain 7 h », « lundi 19/10 à 7 h »… pour les messages."""
    d = d.astimezone(PARIS)
    auj = maintenant().date()
    h = f"{d.hour} h" + (f" {d.minute:02d}" if d.minute else "")
    if d.date() == auj:
        return f"aujourd'hui à {h}"
    if d.date() == auj + timedelta(days=1):
        return f"demain à {h}"
    return f"{JOURS_FR[d.weekday()]} {d:%d/%m} à {h}"
