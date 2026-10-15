"""
Enrichisseur v3 — mémoire des sociétés
  Chaque société enrichie est gardée dans la base Notion « Enrichisseur v3 — Mémoire des sociétés ».
  Si elle revient dans un lancement moins de 6 mois après, l'outil reprend ces résultats au lieu de relancer
  la recherche : pas de quota LinkedIn, pas de crédits Kaspr ou Fullenrich.
  Une ligne est reprise seulement si elle a été traitée avec un choix au moins aussi complet
  (une société passée en « Sociétés » ne sert pas pour une demande « Tout »).
"""
import asyncio, json, os, re
from datetime import datetime, timedelta, timezone

import httpx

NOTION_KEY = os.getenv("NOTION_API_KEY", "")
MEMOIRE_DB = os.getenv("NOTION_MEMOIRE_DB", "0447dcf54a344a3883be1d4170aaa692")
DUREE_JOURS = int(os.getenv("MEMOIRE_JOURS", "183"))          # 6 mois
RANG = {"siren": 0, "societes": 1, "dirigeants": 2, "tout": 3}
A_JETER = {"adresses_site", "pages", "candidats", "memoire"}   # brut du scan du site, inutile à garder
_SEM = asyncio.Semaphore(3)                                    # Notion : environ 3 requêtes par seconde


def _h(json_=True):
    h = {"Authorization": f"Bearer {NOTION_KEY}", "Notion-Version": "2022-06-28"}
    if json_:
        h["Content-Type"] = "application/json"
    return h


def _txt(v):
    return {"rich_text": [{"text": {"content": str(v or "")[:2000]}}]}


def _txts(v):
    """Texte long (plus de 2 000 caractères) découpé en morceaux pour une propriété Notion."""
    v = str(v or "")
    return {"rich_text": [{"text": {"content": v[i:i + 2000]}} for i in range(0, len(v), 2000)][:100]}


def _val(p, nom):
    x = p.get(nom) or {}
    t = x.get("type")
    if t in ("title", "rich_text"):
        return "".join(b.get("plain_text", "") for b in x[t])
    if t == "select":
        return (x["select"] or {}).get("name", "")
    if t == "date":
        return (x["date"] or {}).get("start", "")
    return ""


async def _req(c, methode, url, **kw):
    async with _SEM:
        for essai in range(4):
            r = await c.request(methode, url, headers=_h("json" in kw), **kw)
            if r.status_code != 429:
                return r
            await asyncio.sleep(1 + essai)
        return r


async def index(journal, tout: bool = False) -> list:
    """Sociétés traitées depuis moins de 6 mois (ou toutes, pour l'historique des effectifs). Propriétés seulement."""
    if not NOTION_KEY:
        return []
    depuis = (datetime.now(timezone.utc) - timedelta(days=DUREE_JOURS)).date().isoformat()
    out, curseur = [], None
    async with httpx.AsyncClient(timeout=30) as c:
        while True:
            corps = {"sorts": [{"property": "Traitée le", "direction": "descending"}], "page_size": 100}
            if not tout:
                corps["filter"] = {"property": "Traitée le", "date": {"on_or_after": depuis}}
            if curseur:
                corps["start_cursor"] = curseur
            r = await _req(c, "POST", f"https://api.notion.com/v1/databases/{MEMOIRE_DB}/query", json=corps)
            if r.status_code != 200:
                journal(f"mémoire inaccessible ({r.status_code}) : l'intégration Notion doit être connectée à la base "
                        f"« Enrichisseur v3 — Mémoire des sociétés »")
                return []
            j = r.json()
            for pg in j.get("results", []):
                p = pg["properties"]
                out.append({"page": pg["id"], "nom": _val(p, "Nom normalisé"), "siren": _val(p, "SIREN"),
                            "domaine": _val(p, "Domaine"), "perim": _val(p, "Périmètre"), "date": _val(p, "Traitée le"),
                            "hist": _val(p, "Historique effectifs"), "validation": _val(p, "Validation"),
                            "adresse_retenue": _val(p, "Adresse retenue"), "source_retenue": _val(p, "Source adresse retenue")})
            if not j.get("has_more"):
                return out
            curseur = j.get("next_cursor")


def _meme(e: dict, siren: str, domaine: str, nom: str) -> bool:
    return bool((siren and e["siren"] == siren) or (domaine and e["domaine"] == domaine) or (nom and e["nom"] == nom))


def historique(e: dict) -> list:
    """« 2026-10-15:48; 2026-04-02:41 » -> [(date, effectif)], du plus récent au plus ancien."""
    out = []
    for morceau in (e.get("hist") or "").split(";"):
        d, _, n = morceau.strip().partition(":")
        if d and n.strip().isdigit():
            out.append((d[:10], int(n)))
    return out


MOIS = ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."]


def evolution(idx_tout: list, r: dict, norm_nom) -> dict:
    """Croissance de l'effectif LinkedIn depuis la dernière mesure d'au moins 3 mois (gardée dans la mémoire)."""
    eff = r.get("effectif_linkedin")
    if eff in (None, ""):
        return {}
    siren = r.get("siren_site") or r.get("siren_crm") or r.get("siren_nom") or ""
    nom = norm_nom(r.get("nom") or "")
    e = next((e for e in idx_tout if _meme(e, siren, r.get("domaine", ""), nom) and e.get("hist")), None)
    if not e:
        return {"evolution_effectif": "première mesure", "croissance_effectif": ""}
    limite = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
    ref = next(((d, n) for d, n in historique(e) if d <= limite), None)
    if not ref or not ref[1]:
        return {"evolution_effectif": "mesure précédente trop récente (moins de 3 mois)", "croissance_effectif": ""}
    d, n = ref
    pct = round(100 * (int(eff) - n) / n)
    quand = f"{MOIS[int(d[5:7]) - 1]} {d[:4]}"
    return {"evolution_effectif": f"{int(eff) - n:+d} salariés ({pct:+d} %) depuis {quand} ({n})", "croissance_effectif": pct}


def chercher(idx: list, o: dict, perim: str, norm_nom, domaines) -> dict:
    """La ligne la plus récente de la mémoire qui correspond à cette société (SIREN, site ou nom) et couvre le choix demandé."""
    siren = re.sub(r"\D", "", o.get("siren_crm") or "")[:9]
    doms = set(domaines(o.get("site_crm") or ""))
    nom = norm_nom(o.get("nom") or "")
    for e in idx:                                   # déjà triées de la plus récente à la plus ancienne
        if RANG.get(e["perim"], -1) < RANG[perim]:
            continue
        if (siren and e["siren"] == siren) or (e["domaine"] and e["domaine"] in doms) or (nom and e["nom"] == nom):
            return e
    return {}


async def lire(page: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as c:
        texte, curseur = "", None
        while True:
            r = await _req(c, "GET", f"https://api.notion.com/v1/blocks/{page}/children",
                           params={"page_size": 100, **({"start_cursor": curseur} if curseur else {})})
            if r.status_code != 200:
                return {}
            j = r.json()
            for b in j.get("results", []):
                if b.get("type") == "code":
                    texte += "".join(t.get("plain_text", "") for t in b["code"]["rich_text"])
            if not j.get("has_more"):
                break
            curseur = j.get("next_cursor")
    try:
        return json.loads(texte)
    except ValueError:
        return {}


async def reprendre(job: dict, orgs: list, norm_nom, domaines, journal) -> tuple:
    """Sépare la liste : (lignes reprises de la mémoire, sociétés à traiter vraiment)."""
    idx = await index(journal)
    if not idx:
        return [], orgs
    trouves, reste = [], []
    for o in orgs:
        e = chercher(idx, o, job["perimetre"], norm_nom, domaines)
        (trouves if e else reste).append((o, e) if e else o)
    lignes = await asyncio.gather(*(lire(e["page"]) for _, e in trouves))
    reprises = []
    for (o, e), row in zip(trouves, lignes):
        if not row:
            reste.append(o)                          # page illisible : on relance la recherche
            continue
        if str(row.get("mode", "")).startswith("adresse seule") and job["perimetre"] in ("dirigeants", "tout"):
            reste.append(o)                          # traitée avant la recherche de dirigeants des SIREN connus : on relance
            continue
        row["memoire"] = f"oui, traitée le {e['date'][:10]}"
        row["id"] = o.get("id") or row.get("id", "")  # l'ID Pipedrive du fichier du jour prime
        reprises.append(row)
    if reprises:
        journal(f"mémoire : {len(reprises)} sociétés déjà traitées depuis moins de 6 mois, reprises sans nouvelle recherche")
    return reprises, reste


async def enregistrer(job: dict, norm_nom, journal):
    """Garde dans la mémoire les sociétés traitées par ce lancement (les anciennes lignes de la même société sont archivées)."""
    if not NOTION_KEY or job["etat"] == "erreur":
        return
    nouvelles = [r for r in job["resultats"] if not r.get("memoire") and (r.get("siren_site") or r.get("siren_crm")
                 or r.get("siren_nom") or r.get("domaine"))]
    if not nouvelles:
        return
    idx = job.get("_idx_tout") or await index(lambda *_: None, tout=True)
    maintenant = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ok = 0
    async with httpx.AsyncClient(timeout=30) as c:
        async def une(r):
            nonlocal ok
            siren = r.get("siren_site") or r.get("siren_crm") or r.get("siren_nom") or ""
            nom = norm_nom(r.get("nom") or "")
            meme = [e for e in idx if _meme(e, siren, r.get("domaine", ""), nom)]
            anciennes = [e["page"] for e in meme]
            hist = historique(meme[0]) if meme else []                  # les mesures passées suivent la société
            if r.get("effectif_linkedin") not in (None, ""):
                hist = [(maintenant[:10], int(r["effectif_linkedin"]))] + [h for h in hist if h[0] != maintenant[:10]]
            hist_txt = "; ".join(f"{d}:{n}" for d, n in hist[:24])
            data = json.dumps({k: v for k, v in r.items() if k not in A_JETER}, ensure_ascii=False, default=str)
            blocs = [{"object": "block", "type": "code", "code": {"language": "json",
                      "rich_text": [{"type": "text", "text": {"content": data[i:i + 1900]}}]}} for i in range(0, len(data), 1900)]
            props = {"Société": {"title": [{"text": {"content": (r.get("nom") or "")[:100]}}]},
                     "Nom normalisé": _txt(nom), "SIREN": _txt(siren), "Domaine": _txt(r.get("domaine", "")),
                     "Périmètre": {"select": {"name": job["perimetre"]}}, "Traitée le": {"date": {"start": maintenant}},
                     "Lancement": _txt(job.get("nom", "")), "Nb dirigeants": {"number": len(r.get("dirigeants", []))},
                     "Nb e-mails": {"number": sum(1 for d in r.get("dirigeants", []) if d.get("email"))},
                     "Nb nouveaux contacts": {"number": sum(1 for d in r.get("dirigeants", []) if not d.get("dans_pipedrive"))},
                     "Effectif LinkedIn": {"number": int(r["effectif_linkedin"]) if r.get("effectif_linkedin") not in (None, "") else None},
                     "Historique effectifs": _txt(hist_txt),
                     "Validation": {"select": {"name": "À valider"}}}       # nouvelles données : à revoir dans l'onglet Validation
            ancienne = next((e for e in meme if e.get("adresse_retenue")), None)
            if ancienne:                                                     # l'adresse déjà retenue suit la société
                props["Adresse retenue"] = _txt(ancienne["adresse_retenue"])
                if ancienne.get("source_retenue"):
                    props["Source adresse retenue"] = {"select": {"name": ancienne["source_retenue"]}}
            rr = await _req(c, "POST", "https://api.notion.com/v1/pages",
                            json={"parent": {"database_id": MEMOIRE_DB}, "properties": props, "children": blocs[:100]})
            if rr.status_code != 200:
                return
            ok += 1
            for p in anciennes:
                await _req(c, "PATCH", f"https://api.notion.com/v1/pages/{p}", json={"archived": True})
        await asyncio.gather(*(une(r) for r in nouvelles))
    journal(f"mémoire : {ok} sociétés enregistrées pour 6 mois")
