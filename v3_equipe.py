"""
Enrichisseur v3 — équipe des négos
  La table Notion « Enrichisseur v3 — Équipe négos » dit qui peut se connecter aux écrans de validation :
  Prénom (écrit comme dans la liste déroulante « nego » de prospection-hebdo), E-mail, Actif,
  Négo (reçoit les sociétés à son prénom) et Admin (voit tout). On peut cocher les deux (Lionel).
  Elle sert à deux choses :
    · la connexion par lien e-mail (seuls les e-mails « Actif » reçoivent un lien) ;
    · le tri des sociétés : le champ « nego » envoyé par prospection-hebdo est un prénom, on le relie ici à un e-mail.
  Modifier la table suffit (pas de redémarrage) : elle est relue toutes les 5 minutes.
"""
import os, re, time, unicodedata

import httpx

import v3_memoire as MEM

EQUIPE_DB = os.getenv("NOTION_EQUIPE_DB", "3e5a5d3c5b4749deb050d08de1659be4")
DUREE_CACHE = 300
_CACHE = {"membres": None, "le": 0.0, "erreur": ""}


def norm(prenom: str) -> str:
    """« Sébastien », « sebastien », « SEBASTIEN » → « sebastien » (accents, majuscules et espaces ignorés)."""
    t = unicodedata.normalize("NFKD", str(prenom or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", t).strip()


def separer(txt: str) -> list:
    """« Marine ; Sébastien » → ["Marine", "Sébastien"] (accepte aussi , et /)."""
    return [x.strip() for x in re.split(r"[;,/]", str(txt or "")) if x.strip()]


def fusion(existant: str, nouveaux) -> str:
    """Ajoute des prénoms à une liste « A ; B » sans doublon (même prénom avec ou sans accent = doublon)."""
    vus, out = set(), []
    for p in separer(existant) + [y for x in (nouveaux or []) for y in separer(x)]:
        if p and norm(p) not in vus:
            vus.add(norm(p))
            out.append(p)
    return " ; ".join(out)[:2000]


def _membre(pg: dict) -> dict:
    p = pg.get("properties") or {}
    coche = lambda n: bool((p.get(n) or {}).get("checkbox"))
    return {"prenom": MEM._val(p, "Prénom").strip(), "mail": str((p.get("E-mail") or {}).get("email") or "").strip().lower(),
            "actif": coche("Actif"), "admin": coche("Admin"), "nego": coche("Négo")}


async def membres(forcer: bool = False) -> list:
    """Membres de la table (cache 5 min). En cas d'erreur Notion : la dernière liste lue, et l'erreur est gardée."""
    if not forcer and _CACHE["membres"] is not None and time.time() - _CACHE["le"] < DUREE_CACHE:
        return _CACHE["membres"]
    out, curseur = [], None
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            while True:
                corps = {"page_size": 100, **({"start_cursor": curseur} if curseur else {})}
                r = await MEM._req(c, "POST", f"https://api.notion.com/v1/databases/{EQUIPE_DB}/query", json=corps)
                if r.status_code != 200:
                    raise RuntimeError(f"table « Équipe négos » inaccessible ({r.status_code}) : connectez l'intégration "
                                       f"Notion de l'enrichisseur à cette table")
                j = r.json()
                out += [m for m in (_membre(pg) for pg in j.get("results", [])) if m["prenom"] or m["mail"]]
                if not j.get("has_more"):
                    break
                curseur = j.get("next_cursor")
    except (RuntimeError, httpx.HTTPError) as e:
        _CACHE["erreur"] = str(e)
        print(f"[V3 ÉQUIPE] {e}")
        return _CACHE["membres"] or []
    _CACHE.update(membres=out, le=time.time(), erreur="")
    return out


def erreur() -> str:
    return _CACHE["erreur"]


def actifs(liste: list) -> list:
    return [m for m in liste if m["actif"]]


def par_mail(liste: list, mail: str) -> dict:
    mail = (mail or "").strip().lower()
    return next((m for m in actifs(liste) if mail and m["mail"] == mail), {})


def attribution(liste: list, negos: str, lance_par: str) -> dict:
    """À qui revient une société.
      · « negos » : les prénoms reçus (prospection-hebdo), reconnus dans la table, sans accent ni majuscule près ;
      · « lance_par » : les négos de la table qui l'ont lancée eux-mêmes (leur e-mail est dans « Lancé par »).
    Renvoie {"a": [prénoms de la table], "inconnus": [prénoms reçus absents de la table], "non_attribue": bool}."""
    act = [m for m in actifs(liste) if m["nego"]]
    par_prenom = {norm(m["prenom"]): m for m in act if m["prenom"]}
    mails = {x.strip().lower() for x in re.split(r"[;,\s]+", lance_par or "") if "@" in x}
    a, inconnus = [], []
    for p in separer(negos):
        m = par_prenom.get(norm(p))
        (a if m else inconnus).append(m["prenom"] if m else p)
    for m in act:                                       # lancée par un négo lui-même : elle est à lui
        if m["mail"] and m["mail"] in mails and m["prenom"] not in a:
            a.append(m["prenom"])
    return {"a": a, "inconnus": inconnus, "non_attribue": not a}


def visible_pour(membre: dict, s: dict, liste: list) -> bool:
    """Un négo voit une société si son prénom est dans « Négos » ou s'il l'a lancée lui-même."""
    if not membre or not membre.get("nego"):
        return False
    mails = {x.strip().lower() for x in re.split(r"[;,\s]+", s.get("lance_par") or "") if "@" in x}
    if membre["mail"] and membre["mail"] in mails:
        return True
    return bool(membre["prenom"]) and norm(membre["prenom"]) in {norm(p) for p in separer(s.get("negos", ""))}
