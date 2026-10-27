"""Débit Notion partagé par tout le serveur.

Notion accepte en moyenne ~3 requêtes par seconde par intégration. Un simple sémaphore limitait le nombre de requêtes
EN MÊME TEMPS, pas leur nombre PAR SECONDE : trois requêtes rapides en parallèle dépassaient largement la limite,
et chaque tâche réessayait de son côté pendant que Notion bloquait déjà (1 396 refus en 24 h le 27/10).

Ici : (1) au plus une requête toutes les INTERVALLE secondes pour tout le serveur ; (2) après un 429, TOUT le serveur
attend le délai Retry-After annoncé par Notion avant d'envoyer quoi que ce soit."""
import asyncio, os, time

INTERVALLE = float(os.getenv("NOTION_INTERVALLE", "0.4"))     # 2,5 requêtes/s : sous la limite, avec de la marge
_VERROU = asyncio.Lock()
_ETAT = {"dernier": 0.0, "pause_jusqua": 0.0, "refus": 0}


async def attendre_mon_tour():
    async with _VERROU:
        maintenant = time.monotonic()
        attente = max(_ETAT["dernier"] + INTERVALLE, _ETAT["pause_jusqua"]) - maintenant
        if attente > 0:
            await asyncio.sleep(attente)
        _ETAT["dernier"] = time.monotonic()


def refus(r, essai: int) -> float:
    """Notion a répondu 429 : pause générale. Renvoie la durée de la pause."""
    try:
        delai = float(r.headers.get("retry-after") or 0)
    except ValueError:
        delai = 0
    delai = min(max(delai, 1 + essai * 2), 30)
    _ETAT["pause_jusqua"] = max(_ETAT["pause_jusqua"], time.monotonic() + delai)
    _ETAT["refus"] += 1
    return delai


async def requete(c, methode: str, url: str, essais: int = 7, **kw):
    """Requête Notion cadencée ; réessaie après un 429 (pause générale), jusqu'à `essais` fois."""
    r = None
    for essai in range(essais):
        await attendre_mon_tour()
        r = await c.request(methode, url, **kw)
        if r.status_code != 429:
            return r
        refus(r, essai)
    return r
