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

import notion_debit as DEBIT

NOTION_KEY = os.getenv("NOTION_API_KEY", "")
MEMOIRE_DB = os.getenv("NOTION_MEMOIRE_DB", "0447dcf54a344a3883be1d4170aaa692")
DUREE_JOURS = int(os.getenv("MEMOIRE_JOURS", "183"))          # 6 mois
RANG = {"siren": 0, "societes": 1, "dirigeants": 2, "tout": 3}
A_JETER = {"adresses_site", "pages", "candidats", "memoire", "_page_memoire", "_lance_par", "_lancement", "_negos", "_nego_nouveau",
           "_bail_memoire", "_flex_memoire"}   # brut du scan du site, inutile à garder


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
    if t == "checkbox":
        return "oui" if x["checkbox"] else ""
    return ""


async def _req(c, methode, url, **kw):
    """Toutes les requêtes Notion de l'enrichisseur passent ici : débit partagé par tout le serveur (notion_debit)."""
    return await DEBIT.requete(c, methode, url, headers=_h("json" in kw), **kw)


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
                            "adresse_retenue": _val(p, "Adresse retenue"), "source_retenue": _val(p, "Source adresse retenue"),
                            "lance_par": _val(p, "Lancé par"), "lancement": _val(p, "Lancement"),
                            "negos": _val(p, "Négos"), "nb_dir": (p.get("Nb dirigeants") or {}).get("number"),
                            "validee": _val(p, "Validée le"), "siren_corrige": _val(p, "SIREN corrigé"),
                            "base_quali": _val(p, "Base quali"), "debut_bail": _val(p, "Début de bail"),
                            "flexoffice": _val(p, "Flexoffice") == "oui", "operateur_flex": _val(p, "Opérateur flex"),
                            "source_bail": _val(p, "Source début de bail")})
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
        if RANG[perim] >= RANG["dirigeants"] and e.get("nb_dir") == 0:
            continue                                # zéro dirigeant (phantom en panne ?) : on recherche à nouveau
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
        row["_page_memoire"], row["_lance_par"], row["_lancement"] = e["page"], e.get("lance_par", ""), e.get("lancement", "")
        row["_negos"], row["_nego_nouveau"] = e.get("negos", ""), nego_de(o)   # le négo de CET envoi, pas celui de l'ancien
        row["_bail_memoire"], row["_flex_memoire"] = e.get("debut_bail", ""), bool(e.get("flexoffice"))
        for k in ("debut_bail_crm", "flex_crm"):        # infos du fichier du jour (date de bail, opérateur flex)
            if o.get(k):
                row[k] = o[k]
        row["id"] = o.get("id") or row.get("id", "")  # l'ID Pipedrive du fichier du jour prime
        reprises.append(row)
    if reprises:
        journal(f"mémoire : {len(reprises)} sociétés déjà traitées depuis moins de 6 mois, reprises sans nouvelle recherche")
    return reprises, reste


def nego_de(o: dict) -> str:
    """Prénom du négo envoyé par prospection-hebdo (champ « nego » du JSON), tel que reçu."""
    return str((o.get("infos_source") or {}).get("nego") or o.get("nego") or "").strip()


_COLONNE_NEGOS = {"ok": None}


async def assurer_colonne_negos(c=None) -> bool:
    """Crée une fois la colonne texte « Négos » dans la base mémoire si elle manque. False si Notion refuse."""
    if _COLONNE_NEGOS["ok"] is not None:
        return _COLONNE_NEGOS["ok"]
    if not NOTION_KEY:
        return False
    async def go(c):
        r = await _req(c, "PATCH", f"https://api.notion.com/v1/databases/{MEMOIRE_DB}", json={"properties": {"Négos": {"rich_text": {}}}})
        _COLONNE_NEGOS["ok"] = r.status_code == 200
        if r.status_code != 200:
            print(f"[V3 MÉMOIRE] colonne Négos : Notion {r.status_code} {r.text[:200]}")
    if c is None:
        async with httpx.AsyncClient(timeout=30) as c2:
            await go(c2)
    else:
        await go(c)
    return _COLONNE_NEGOS["ok"]


def fusion_mails(existant: str, nouveaux) -> str:
    vus = []
    for m in re.split(r"[;,\s]+", existant or "") + list(nouveaux or []):
        m = (m or "").strip().lower()
        if "@" in m and m not in vus:
            vus.append(m)
    return "; ".join(vus)


def fusion_lancements(existant: str, nom: str) -> str:
    """« Liste A ; Liste B » : une société reprise de la mémoire garde les noms de tous les fichiers qui l'ont contenue."""
    vus = []
    for x in (existant or "").split(";") + [nom or ""]:
        x = x.strip()
        if x and x not in vus:
            vus.append(x)
    return " ; ".join(vus)[:2000]


async def attribuer_anciens(mail: str, avant: str, confirmer: bool, limite: int, journal=lambda m: None) -> dict:
    """Sociétés de la mémoire sans « Lancé par » traitées avant une date : on y met un mail (rattrapage du 16/10).
    Sans confirmer : essai à blanc, rien n'est écrit. Avec confirmer : au plus « limite » sociétés modifiées."""
    idx = await index(journal, tout=True)
    cibles = [e for e in idx if not (e.get("lance_par") or "").strip() and (e.get("date") or "")[:10] < avant]
    a_faire = cibles[:max(0, limite)] if confirmer else []
    erreurs = 0
    if a_faire:
        async with httpx.AsyncClient(timeout=30) as c:
            for i in range(0, len(a_faire), 10):
                rs = await asyncio.gather(*(_req(c, "PATCH", f"https://api.notion.com/v1/pages/{e['page']}",
                                                 json={"properties": {"Lancé par": _txt(mail)}}) for e in a_faire[i:i + 10]))
                erreurs += sum(1 for r in rs if r.status_code != 200)
    return {"essai_a_blanc": not confirmer, "mail": mail, "avant": avant, "sans_lance_par": len(cibles),
            "modifiees": len(a_faire) - erreurs, "erreurs": erreurs,
            "reste_apres": len(cibles) - (len(a_faire) - erreurs),
            "exemples": [f"{e['nom']} ({(e.get('date') or '')[:10]})" for e in (a_faire or cibles)[:50]]}


async def ajouter_lanceurs(job: dict, journal):
    """Société reprise de la mémoire : le négo qui l'a relancée est ajouté à « Lancé par » pour qu'il la voie dans son écran,
    et le nom de son fichier est ajouté à « Lancement » pour qu'il la retrouve en filtrant par fichier."""
    if not NOTION_KEY:
        return
    import v3_equipe as EQ, v3_validation as VALID
    negos_ok = await assurer_colonne_negos()
    a_faire = []
    for r in job["resultats"]:
        if not r.get("_page_memoire"):
            continue
        props = {}
        n = EQ.fusion(r.get("_negos", ""), [r.get("_nego_nouveau", "")])
        if negos_ok and n != EQ.fusion(r.get("_negos", ""), []):
            props["Négos"] = _txt(n)
        m = fusion_mails(r.get("_lance_par", ""), job.get("mails", []))
        if m != fusion_mails(r.get("_lance_par", ""), []):
            props["Lancé par"] = _txt(m)
        l = fusion_lancements(r.get("_lancement", ""), job.get("nom", ""))
        if l != (r.get("_lancement") or "").strip():
            props["Lancement"] = _txt(l)
        if r.get("debut_bail_crm") and not r.get("_bail_memoire") and await VALID.assurer_colonne("Début de bail", "date"):
            props["Début de bail"] = {"date": {"start": r["debut_bail_crm"]}}      # une date saisie à la main n'est jamais écrasée
        if r.get("flex_crm") and not r.get("_flex_memoire") and await VALID.assurer_colonne("Flexoffice", "checkbox"):
            props["Flexoffice"] = {"checkbox": True}
        if props:
            a_faire.append((r["_page_memoire"], props))
    if not a_faire:
        return
    async with httpx.AsyncClient(timeout=30) as c:
        await asyncio.gather(*(_req(c, "PATCH", f"https://api.notion.com/v1/pages/{p}", json={"properties": props})
                               for p, props in a_faire))
    journal(f"mémoire : {len(a_faire)} sociétés déjà connues rattachées à ce lancement (négo, fichier, lancé par)"
            + (f" ({', '.join(job['mails'])})" if job.get("mails") else ""))


async def enregistrer(job: dict, norm_nom, journal):
    """Garde dans la mémoire les sociétés traitées par ce lancement (les anciennes lignes de la même société sont archivées)."""
    if not NOTION_KEY or job["etat"] == "erreur":
        return
    nouvelles = [r for r in job["resultats"] if not r.get("memoire") and (r.get("siren_site") or r.get("siren_crm")
                 or r.get("siren_nom") or r.get("domaine"))]
    if not nouvelles:
        return
    import v3_equipe as EQ
    idx = job.get("_idx_tout") or await index(lambda *_: None, tout=True)
    negos_ok = await assurer_colonne_negos()
    import v3_validation as VALID                     # colonnes créées au premier usage (écran de validation ou fichier CRM)
    rs = job.get("resultats", [])
    colonnes = {"bail": any(e.get("debut_bail") for e in idx) or (any(r.get("debut_bail_crm") for r in rs)
                                                                  and await VALID.assurer_colonne("Début de bail", "date")),
                "flex": any(e.get("flexoffice") for e in idx) or (any(r.get("flex_crm") for r in rs)
                                                                  and await VALID.assurer_colonne("Flexoffice", "checkbox"))}
    maintenant = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ok = 0
    async with httpx.AsyncClient(timeout=30) as c:
        async def une(r):
            nonlocal ok
            siren = r.get("siren_site") or r.get("siren_crm") or r.get("siren_nom") or ""
            nom = norm_nom(r.get("nom") or "")
            meme = [e for e in idx if _meme(e, siren, r.get("domaine", ""), nom)]
            anciennes = [e["page"] for e in meme]
            ancienne_l = "; ".join(e.get("lance_par", "") for e in meme)
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
                     "Lancement": _txt(fusion_lancements(" ; ".join(e.get("lancement", "") for e in meme), job.get("nom", ""))),
                     "Nb dirigeants": {"number": len(r.get("dirigeants", []))},
                     "Nb e-mails": {"number": sum(1 for d in r.get("dirigeants", []) if d.get("email"))},
                     "Nb nouveaux contacts": {"number": sum(1 for d in r.get("dirigeants", []) if not d.get("dans_pipedrive"))},
                     "Effectif LinkedIn": {"number": int(r["effectif_linkedin"]) if r.get("effectif_linkedin") not in (None, "") else None},
                     "Historique effectifs": _txt(hist_txt),
                     "Validation": {"select": {"name": "À valider"}},       # nouvelles données : à revoir dans l'onglet Validation
                     "Lancé par": _txt(fusion_mails(ancienne_l, job.get("mails", [])))}
            if negos_ok:                                    # les prénoms des relevés précédents suivent la société
                props["Négos"] = _txt(EQ.fusion(" ; ".join(e.get("negos", "") for e in meme), [nego_de(r)]))
            # le travail déjà fait à la main suit la société quand elle est retraitée (relance après une panne…)
            validee = next((e for e in meme if e.get("validation") in ("Société validée", "Contacts validés")), None)
            if validee:                                     # adresse déjà validée ; les nouveaux contacts restent à valider
                props["Validation"] = {"select": {"name": "Société validée"}}
                if validee.get("validee"):
                    props["Validée le"] = {"date": {"start": validee["validee"]}}
            for cle, col in (("siren_corrige", "SIREN corrigé"), ("base_quali", "Base quali"), ("operateur_flex", "Opérateur flex"),
                             ("source_bail", "Source début de bail")):
                v = next((e[cle] for e in meme if e.get(cle)), "")
                if v:                                       # la colonne existe forcément : une ancienne ligne l'a remplie
                    props[col] = _txts(v)
            bail = next((e["debut_bail"] for e in meme if e.get("debut_bail")), "") or r.get("debut_bail_crm", "")
            if bail and colonnes.get("bail"):               # saisie à la main d'abord, sinon celle du fichier CRM
                props["Début de bail"] = {"date": {"start": bail}}
            if (any(e.get("flexoffice") for e in meme) or r.get("flex_crm")) and colonnes.get("flex"):
                props["Flexoffice"] = {"checkbox": True}     # cochée à la main, ou opérateur flex dans le fichier CRM
            if next((e for e in meme if e.get("siren_corrige")), None):
                props["SIREN"] = _txt(next(e for e in meme if e.get("siren_corrige"))["siren"])   # le SIREN corrigé à la main prime
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
