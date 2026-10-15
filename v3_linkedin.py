"""
Enrichisseur v3 — étapes 3 à 5 (LinkedIn)
  3. Page LinkedIn de la société (lien du site, sinon Company URL Finder) → Company Scraper :
     adresse des bureaux + effectif réel (salariés sur LinkedIn)
  4. Sales Navigator Search Export, filtré par identifiant LinkedIn de la société → dirigeants
  5. Kaspr → e-mail professionnel des dirigeants

Les phantoms sont pilotés par l'API PhantomBuster : on reprend leurs réglages enregistrés
(cookie LinkedIn, délais…) et on ne remplace que la liste à traiter.
"""
import asyncio, csv, io, json, os, re, time, unicodedata, uuid
from urllib.parse import quote

import httpx

PB_KEY = os.getenv("PHANTOMBUSTER_API_KEY", "")
PB_URL_FINDER = os.getenv("PB_URL_FINDER_ID", "")
PB_SCRAPER = os.getenv("PB_COMPANY_SCRAPER_ID", "")
PB_SALESNAV = os.getenv("PB_SALESNAV_ID", "")
PB_GMAPS = os.getenv("PB_GMAPS_ID", "")
URL_PUBLIQUE = os.getenv("RENDER_EXTERNAL_URL", "")   # adresse publique du service, fournie par Render
CSV_HEBERGES: dict = {}                              # jeton -> (contenu CSV, heure) servis aux phantoms
KASPR_KEY = os.getenv("KASPR_API_KEY", "")
FULLENRICH_KEY = os.getenv("FULLENRICH_API_KEY", "")
PIPEDRIVE_KEY = os.getenv("PIPEDRIVE_API_KEY", "")
PB_API = "https://api.phantombuster.com/api/v2"
MAX_LINKEDIN = 150          # quota prudent par lancement (recommandation PhantomBuster avec Sales Navigator)

IDF = ("75", "77", "78", "91", "92", "93", "94", "95")
RE_IDF = re.compile(r"paris|[iî]le-de-france|hauts-de-seine|seine-saint-denis|val-de-marne|yvelines|essonne|"
                    r"val-d'oise|seine-et-marne|boulogne|levallois|neuilly|courbevoie|puteaux|issy|saint-cloud|"
                    r"saint-mand|montreuil|clichy|suresnes|nanterre|la d[ée]fense", re.I)
# Lieu LinkedIn en France : « …, France », une région française, ou une grande agglomération française (« Lyon et périphérie »)
RE_FRANCE = re.compile(
    r"\bfrance\b|[iî]le-de-france|auvergne|rh[ôo]ne-alpes|bourgogne|franche-comt|bretagne|centre-val|corse|grand est|"
    r"hauts-de-france|normandie|nouvelle-aquitaine|occitanie|pays de la loire|provence|c[ôo]te d.azur|"
    r"\b(paris|lyon|lille|marseille|toulouse|bordeaux|nantes|nice|strasbourg|montpellier|rennes|grenoble|rouen|toulon|"
    r"saint-[ée]tienne|clermont-ferrand|tours|orl[ée]ans|angers|dijon|reims|le havre|metz|nancy|besan[çc]on|brest|caen|"
    r"limoges|avignon|chamb[ée]ry|annecy|mulhouse|perpignan|pau|bayonne|la rochelle|poitiers|amiens|valence|le mans|"
    r"n[îi]mes|aix-en-provence|lorient|vannes|quimper|troyes|la roche-sur-yon|cannes|antibes)\b(?! *,? *(texas|ontario|kentucky|tennessee))",
    re.I)
PAYS_ETRANGERS = re.compile(r"[ée]tats-unis|united states|royaume-uni|united kingdom|canada|qu[ée]bec|montr[ée]al|belgique|suisse|"
                            r"allemagne|espagne|italie|pays-bas|luxembourg|maroc|tunisie|alg[ée]rie|s[ée]n[ée]gal|"
                            r"[ée]mirats|arabie|chine|inde|singapour|japon|australie|br[ée]sil|argentine|mexique|isra[ëe]l|portugal|irlande|laos", re.I)


def en_france(lieu: str) -> bool:
    """Le dirigeant est-il basé en France ? Lieu vide = on ne sait pas = écarté (consigne : France uniquement)."""
    l = (lieu or "").strip()
    return bool(l) and bool(RE_FRANCE.search(l)) and not PAYS_ETRANGERS.search(l)


def pb_configure() -> bool:
    return bool(PB_KEY and PB_SCRAPER)


# ───────────────────────── petits utilitaires ─────────────────────────
def _sans_accents(x: str) -> str:
    return unicodedata.normalize("NFKD", x or "").encode("ascii", "ignore").decode().lower()


def cle_linkedin(url: str) -> str:
    """Forme normalisée d'une URL de page société LinkedIn, pour rapprocher entrée et sortie des phantoms."""
    u = (url or "").strip().lower()
    m = re.search(r"linkedin\.com/company/([^/?#\s]+)", u)
    return m.group(1).strip("/") if m else u


def cle_adresse(a: str):
    """(numéro, mots clés de la rue, département) pour comparer deux adresses."""
    t = re.sub(r"[^a-z0-9 ]", " ", _sans_accents(a))
    m_cp = re.search(r"\b(\d{5})\b", t)
    if not m_cp:
        return None
    avant = t[:m_cp.start()]
    m_num = re.search(r"\b(\d{1,4})(?:bis|ter)?\b", avant)
    if not m_num:
        return None
    vides = {"rue", "avenue", "av", "boulevard", "bd", "place", "pl", "cite", "de", "du", "la", "le", "des", "l", "d",
             "quai", "allee", "impasse", "passage", "paris", "cedex", "bis", "ter", "tour", "bat", "batiment", "etage"}
    mots = [w for w in re.sub(r"\d+", " ", avant[m_num.end():]).split() if w not in vides and len(w) > 1]
    return m_num.group(1), " ".join(mots)[:30], m_cp.group(1)


def meme_adresse(a: str, b: str) -> bool:
    ka, kb = cle_adresse(a), cle_adresse(b)
    if not (ka and kb) or ka[0] != kb[0]:
        return False
    if ka[2] != kb[2]:
        return False
    x, y = ka[1], kb[1]
    return bool(x and y) and (x in y or y in x or x.split()[0] == y.split()[0])


def _hors_idf(a: str) -> str:
    c = cle_adresse(a)
    return "" if (c and c[2][:2] in IDF) else " · hors Île-de-France"


# Grandes adresses de domiciliation parisiennes (des milliers de sociétés y ont leur siège). Liste à compléter.
ADRESSES_DOMICILIATION = [
    "60 rue François 1er 75008 Paris", "66 avenue des Champs-Élysées 75008 Paris", "229 rue Saint-Honoré 75001 Paris",
    "10 rue de Penthièvre 75008 Paris", "128 rue La Boétie 75008 Paris",
]
RE_DOMICIL_INSEE = re.compile(r"\bc/o\b|\bchez\b|domicil|business cent|centre d.affaires", re.I)


def verdict_domiciliation(r: dict, dec: dict) -> dict:
    """Le siège légal (INSEE) est-il une simple domiciliation, ailleurs que les bureaux ?
    C'est le signal des cibles coworking : une société domiciliée qui travaille ailleurs, ou nulle part."""
    siege = (r.get("siege_insee") or "").strip()
    bureaux = dec.get("adresse_proposee") or ""
    sur = dec.get("confiance_adresse") == "haute"
    if r.get("mention_domiciliation") and not str(r.get("controle_nom", "")).startswith("⚠️"):
        return {"verdict_domiciliation": "🎯 Domiciliation probable",
                "raison_domiciliation": f"le site l'indique : « {r['mention_domiciliation']} »"}
    if not siege or not cle_adresse(siege):
        return {"verdict_domiciliation": "", "raison_domiciliation": "siège INSEE inconnu"}
    if RE_DOMICIL_INSEE.search(siege):
        return {"verdict_domiciliation": "🎯 Domiciliation probable", "raison_domiciliation": "siège INSEE « chez » un tiers"}
    if any(meme_adresse(siege, a) for a in ADRESSES_DOMICILIATION):
        return {"verdict_domiciliation": "🎯 Domiciliation probable",
                "raison_domiciliation": "siège à une adresse connue de domiciliation" + ("" if bureaux else ", aucun bureau trouvé ailleurs")}
    connue = bureaux or (r.get("adresse_crm") or "")
    if cle_adresse(siege)[2][:2] not in IDF and connue and cle_adresse(connue) and not meme_adresse(siege, connue):
        return {"verdict_domiciliation": "🏢 Siège hors Île-de-France",
                "raison_domiciliation": "siège en région, bureaux en Île-de-France" if not _hors_idf(connue) else "siège et bureaux hors Île-de-France"}
    if not bureaux:
        return {"verdict_domiciliation": "", "raison_domiciliation": "adresse des bureaux inconnue"}
    if meme_adresse(siege, bureaux):
        return {"verdict_domiciliation": "✅ Cohérent", "raison_domiciliation": "siège = bureaux"}
    if sur:
        return {"verdict_domiciliation": "📍 Siège ≠ bureaux",
                "raison_domiciliation": f"siège ailleurs que les bureaux confirmés ({dec.get('sources_adresse', '')}) : domiciliation ou ancien siège"}
    return {"verdict_domiciliation": "🔍 À vérifier", "raison_domiciliation": "siège ≠ adresse trouvée, mais celle-ci n'est pas confirmée"}


def decider_adresse(r: dict) -> dict:
    d = _decider_adresse(r)
    d.update(verdict_domiciliation(r, d))
    return d


def _decider_adresse(r: dict) -> dict:
    """Adresse des bureaux à partir de 4 sources : CRM, site internet, LinkedIn, Google Maps.
    Règle : une adresse est sûre quand 2 sources au moins la donnent.
    Le site peut afficher plusieurs adresses (plusieurs bureaux) : on garde celle qui concorde avec une autre source."""
    crm = (r.get("adresse_crm") or "").strip()
    li = (r.get("adresse_linkedin") or "").strip()
    li_etranger = bool(li) and li.rsplit(",", 1)[-1].strip().upper() not in ("FR", "FRANCE", "")
    sources = []                                             # (nom, adresse), du plus fiable au moins fiable
    site_liste = [a for a in (r.get("adresses_site_liste") or []) if cle_adresse(a)]
    if site_liste:
        autres = [x for x in (crm, li if not li_etranger else "", r.get("adresse_google", "")) if x and cle_adresse(x)]
        site = next((a for a in site_liste if any(meme_adresse(a, x) for x in autres)), site_liste[0])
        sources.append(("site", site))
        r["adresse_site"] = site
        r["page_adresse_site"] = (r.get("pages_adresse_site") or {}).get(site, "")
    if li and not li_etranger and cle_adresse(li):
        sources.append(("LinkedIn", li))
    if r.get("adresse_google") and cle_adresse(r["adresse_google"]):
        sources.append(("Google", r["adresse_google"]))
    if crm and cle_adresse(crm):
        sources.append(("CRM", crm))

    # groupes de sources qui donnent la même adresse
    groupes = []
    for nom, a in sources:
        g = next((g for g in groupes if meme_adresse(g[0][1], a)), None)
        (g.append((nom, a)) if g else groupes.append([(nom, a)]))
    groupes.sort(key=lambda g: (-len(g), not any(n == "site" for n, _ in g), not any(n == "CRM" for n, _ in g)))
    noms = lambda g: " + ".join(n for n, _ in g)
    externes = [(n, a) for n, a in sources if n != "CRM"]

    if groupes and len(groupes[0]) >= 2:
        g = groupes[0]
        hors_g = [n for n, _ in sources if n != "CRM" and all(n != m for m, _ in g)]
        reste = f" · autre adresse sur {', '.join(hors_g)}" if hors_g else ""
        if any(n == "CRM" for n, _ in g):
            return {"adresse_proposee": crm, "confiance_adresse": "haute", "sources_adresse": noms(g),
                    "action_adresse": f"✅ adresse du CRM confirmée par {noms([x for x in g if x[0] != 'CRM'])}{reste}"}
        a = g[0][1]
        if crm:
            return {"adresse_proposee": a, "confiance_adresse": "haute", "sources_adresse": noms(g),
                    "action_adresse": f"🔄 nouvelle adresse confirmée par {noms(g)} : corriger le CRM{reste}{_hors_idf(a)}"}
        return {"adresse_proposee": a, "confiance_adresse": "haute", "sources_adresse": noms(g),
                "action_adresse": f"➕ adresse confirmée par {noms(g)} : à ajouter au CRM{reste}{_hors_idf(a)}"}

    if len(externes) == 1:
        n, a = externes[0]
        quoi = f"🆕 adresse différente sur {'le site' if n == 'site' else n}" if crm else f"🆕 adresse trouvée seulement sur {'le site' if n == 'site' else n}"
        return {"adresse_proposee": a, "confiance_adresse": "moyenne", "sources_adresse": n,
                "action_adresse": f"{quoi}{_hors_idf(a)} : à valider"}
    if len(externes) > 1:
        a = externes[0][1]
        return {"adresse_proposee": a, "confiance_adresse": "moyenne", "sources_adresse": externes[0][0],
                "action_adresse": f"⚖️ adresses différentes ({', '.join(n for n, _ in sources)}) : à arbitrer"}
    if li_etranger:
        return {"adresse_proposee": "", "confiance_adresse": "basse", "sources_adresse": "",
                "action_adresse": "⚠️ LinkedIn affiche le siège à l'étranger : garder l'adresse du CRM"}
    return {"adresse_proposee": "", "confiance_adresse": "", "sources_adresse": "",
            "action_adresse": "⚪ aucune autre source que le CRM" if crm else "⚪ aucune adresse trouvée"}


def role(titre: str) -> str:
    x = _sans_accents(titre)
    if "partnership" in x or "assistant" in x:
        return ""
    if re.search(r"\b(ceo|pdg|president|presidente|directeur general|directrice generale|managing director|general manager|dg)\b", x) \
            and not re.search(r"adjoint|deputy|regional|region", x):
        return "1 CEO / Président / DG"
    if re.search(r"fondat|founder", x):
        return "2 Fondateur"
    if re.search(r"\b(coo|chief operating|directeur des operations|directrice des operations)\b", x):
        return "3 COO / Opérations"
    if re.search(r"\b(cfo|daf|chief financial|directeur financier|directrice financiere|directeur administratif)\b", x):
        return "4 CFO / DAF"
    if re.search(r"\b(associe|associee|partner|gerant|gerante)\b", x):
        return "5 Associé / Gérant"
    if re.search(r"\b(cto|cmo|chro|drh|chief|vp|vice[- ]president)\b", x):
        return "6 Autre C-level / VP"
    return ""


def url_salesnav(entreprises: list) -> str:
    """Recherche Sales Navigator : entreprises actuelles (par identifiant LinkedIn) × niveaux CXO, Associé, VP."""
    vals = ",".join(
        f"(id:urn%3Ali%3Aorganization%3A{int(i)},text:{quote(re.sub(r'[,()]', ' ', n), safe='')},selectionType:INCLUDED)"
        for i, n in entreprises)
    q = (f"(filters:List((type:CURRENT_COMPANY,values:List({vals})),(type:SENIORITY_LEVEL,values:List("
         f"(id:310,selectionType:INCLUDED),(id:320,selectionType:INCLUDED),(id:300,selectionType:INCLUDED)))))")
    return "https://www.linkedin.com/sales/search/people?query=" + quote(q, safe="():,")


# ───────────────────────── client PhantomBuster ─────────────────────────
class PhantomBuster:
    def __init__(self, journal):
        self.h = {"X-Phantombuster-Key": PB_KEY}
        self.log = journal

    async def _argument(self, c: httpx.AsyncClient, agent: str) -> dict:
        r = await c.get(f"{PB_API}/agents/fetch", headers=self.h, params={"id": agent})
        r.raise_for_status()
        a = r.json().get("argument") or "{}"
        return json.loads(a) if isinstance(a, str) else (a or {})

    @staticmethod
    async def _heberger_csv(c: httpx.AsyncClient, colonne: str, valeurs: list) -> str:
        """Le phantom attend une URL de tableur : l'outil sert lui-même le CSV, à son adresse publique
        (/v3/csv/<jeton>.csv). paste.rs, utilisé avant, refuse désormais les dépôts."""
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([colonne])
        for v in valeurs:
            w.writerow([v])
        contenu = buf.getvalue()
        if URL_PUBLIQUE:
            jeton = uuid.uuid4().hex
            CSV_HEBERGES[jeton] = (contenu, time.time())
            for k in [k for k, (_, t) in CSV_HEBERGES.items() if time.time() - t > 86400]:
                CSV_HEBERGES.pop(k, None)                    # ménage : rien ne reste plus de 24 h
            return f"{URL_PUBLIQUE.rstrip('/')}/v3/csv/{jeton}.csv"
        r = await c.post("https://paste.rs/", content=contenu.encode("utf-8"))   # secours, hors Render
        r.raise_for_status()
        return r.text.strip() + ".csv"

    async def lancer(self, c: httpx.AsyncClient, agent: str, nom_csv: str, valeurs: list = None,
                     colonne: str = "", url_directe: str = "", resultats_par_recherche: int = 0) -> str:
        arg = await self._argument(c, agent)
        cle_url = next((k for k, v in arg.items() if isinstance(v, str) and "linkedin.com/sales/" in v), None)
        if url_directe and cle_url:                         # Sales Navigator : URL de recherche directe
            arg[cle_url] = url_directe
            n = 1
            self.log(f"{nom_csv} : recherche Sales Navigator envoyée via « {cle_url} »")
        else:
            if url_directe:
                valeurs, colonne = [url_directe], colonne or "searchUrl"
            lien = await self._heberger_csv(c, colonne, valeurs)
            cle = next((k for k in ("spreadsheetUrl", "spreadsheetURL", "sheetUrl", "csvUrl", "searches", "queries")
                        if k in arg), "spreadsheetUrl")
            arg[cle] = lien
            arg["columnName"] = colonne
            n = len(valeurs)
            self.log(f"{nom_csv} : {n} lignes envoyées via « {cle} » (réglages du phantom : {', '.join(sorted(arg))[:200]})")
        for k in list(arg):                                  # traiter toute la liste en un seul lancement
            kl = k.lower()
            if kl.endswith("perlaunch"):
                resultats = any(m in kl for m in ("result", "profile", "lead"))
                arg[k] = resultats_par_recherche * n if (resultats and resultats_par_recherche) else n
            elif resultats_par_recherche and ("persearch" in kl or kl == "numberofprofiles"):
                arg[k] = resultats_par_recherche
        arg["csvName"] = nom_csv
        for essai in range(4):
            r = await c.post(f"{PB_API}/agents/launch", headers={**self.h, "Content-Type": "application/json"},
                             json={"id": agent, "argument": json.dumps(arg), "saveArgument": False})
            if r.status_code == 200:
                return r.json()["containerId"]
            if r.status_code in (429, 500, 502, 503, 504) and essai < 3:
                await asyncio.sleep(30 * (essai + 1))
                continue
            raise RuntimeError(f"lancement refusé ({r.status_code}) : {r.text[:200]}")

    async def attendre(self, c: httpx.AsyncClient, conteneur: str, stop, limite_s: int = 3600):
        debut = time.time()
        while time.time() - debut < limite_s:
            if stop():
                return "arrêté"
            r = await c.get(f"{PB_API}/containers/fetch", headers=self.h, params={"id": conteneur})
            st = (r.json() or {}).get("status", "") if r.status_code == 200 else ""
            if st in ("finished", "error", "launch error"):
                return st
            await asyncio.sleep(10)
        return "trop long"

    @staticmethod
    def _lignes(obj) -> list:
        """resultObject : soit directement la liste des résultats (JSON), soit un dict avec l'URL du CSV."""
        if isinstance(obj, list):
            return [{k: ("" if v is None else (v if isinstance(v, str) else json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)))
                     for k, v in x.items()} for x in obj if isinstance(x, dict)]
        return []

    async def resultats(self, c: httpx.AsyncClient, conteneur: str, nom: str = "") -> list:
        """Récupère les résultats du lancement : JSON du resultObject, sinon CSV/JSON indiqué dans la sortie."""
        sortie = ""
        for essai in range(6):
            if essai:
                await asyncio.sleep(5 * essai)
            try:
                r = await c.get(f"{PB_API}/containers/fetch-result-object", headers=self.h, params={"id": conteneur})
                ro = r.json().get("resultObject") if r.status_code == 200 else None
                if isinstance(ro, str):
                    try:
                        ro = json.loads(ro)
                    except ValueError:
                        ro = None
                lignes = self._lignes(ro)
                if lignes:
                    self.log(f"{nom} : {len(lignes)} lignes (resultObject)")
                    return lignes
                url = None
                if isinstance(ro, dict):
                    url = next((ro[k] for k in ("csvURL", "csvUrl", "resultsCsvUrl", "jsonUrl", "jsonURL") if isinstance(ro.get(k), str)), None)
                if not url:
                    r = await c.get(f"{PB_API}/containers/fetch-output", headers=self.h, params={"id": conteneur})
                    out = (r.json().get("output") or "") if r.status_code == 200 else ""
                    sortie = "\n".join(map(str, out)) if isinstance(out, list) else str(out)
                    urls = [u for u in re.findall(r'https?://[^\s\'"<>)\]]+\.(?:csv|json)\b', sortie) if "paste.rs" not in u]
                    url = ([u for u in urls if "phantombuster" in u or "amazonaws" in u] or urls or [None])[-1]
                if url:
                    r = await c.get(url, timeout=60)
                    r.raise_for_status()
                    if url.endswith(".json"):
                        lignes = self._lignes(r.json())
                    else:
                        lignes = list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig", errors="replace"))))
                    self.log(f"{nom} : {len(lignes)} lignes ({url.rsplit('/', 1)[-1]})")
                    return lignes
            except Exception as e:
                self.log(f"{nom} : lecture du résultat impossible ({e})")
        fin = re.sub(r"\s+", " ", sortie[-400:]) if sortie else "aucune sortie"
        self.log(f"{nom} : aucun résultat récupéré. Fin du journal du phantom : {fin}")
        return []

    async def executer(self, c, agent, nom_csv, stop, **kw) -> list:
        cid = await self.lancer(c, agent, nom_csv, **kw)
        self.log(f"phantom lancé ({nom_csv})")
        st = await self.attendre(c, cid, stop)
        if st != "finished":
            self.log(f"phantom {nom_csv} : {st}")
            if st != "error":
                return []
        return await self.resultats(c, cid, nom_csv)


# ───────────────────────── Kaspr ─────────────────────────
async def kaspr_email(c: httpx.AsyncClient, nom: str, url_profil: str) -> str:
    if not KASPR_KEY or not url_profil:
        return ""
    try:
        r = await c.post("https://api.developers.kaspr.io/profile/linkedin", timeout=20, headers={
            "Authorization": f"Bearer {KASPR_KEY}", "Content-Type": "application/json", "accept-version": "v2.0"},
            json={"name": nom, "id": url_profil, "dataToGet": ["workEmail"]})
        if r.status_code != 200:
            return ""
        d = r.json()
        for cle in ("emails", "workEmails", "work_emails"):
            e = d.get(cle)
            if isinstance(e, list) and e:
                v = e[0] if isinstance(e[0], str) else (e[0].get("value") or e[0].get("email") or "")
                if "@" in v:
                    return v
        for cle in ("email", "workEmail", "work_email"):
            if "@" in str(d.get(cle) or ""):
                return d[cle]
        prof = d.get("profile") or {}
        for e in prof.get("professionalEmails") or prof.get("workEmails") or []:
            v = e if isinstance(e, str) else (e.get("email") or e.get("value") or "")
            if "@" in v:
                return v
    except Exception:
        pass
    return ""


# ───────────────────────── Pipedrive (personnes) & Fullenrich ─────────────────────────
async def personne_pipedrive(c: httpx.AsyncClient, nom_complet: str, r: dict, ressemblance) -> dict:
    """La personne existe-t-elle déjà dans Pipedrive, rattachée à cette société ?"""
    if not nom_complet:
        return {}
    try:
        res = await c.get("https://api.pipedrive.com/v1/persons/search",
                          params={"term": nom_complet, "fields": "name", "limit": 10, "api_token": PIPEDRIVE_KEY})
        items = [i.get("item", {}) for i in (res.json().get("data") or {}).get("items", [])]
        for p in items:
            if ressemblance(nom_complet, p.get("name", "")) < 0.9:
                continue
            org = p.get("organization") or {}
            meme_org = (r.get("id") and str(org.get("id")) == str(r["id"])) or \
                       ressemblance(r.get("nom", ""), org.get("name", "") or "") >= 0.8
            if not meme_org:
                continue
            emails = [e for e in (p.get("emails") or []) if isinstance(e, str) and "@" in e]
            return {"id": p.get("id"), "email": emails[0] if emails else ""}
    except Exception as e:
        print(f"[V3 PIPEDRIVE PERSONNE] {nom_complet}: {e}")
    return {}


async def fullenrich_emails(c: httpx.AsyncClient, contacts: list, journal) -> dict:
    """contacts = [(dirigeant, société)] → {index: e-mail}. E-mails seulement (pas de téléphone) pour limiter les crédits."""
    datas = [{"firstname": d["prenom"], "lastname": d["nom"], "domain": r["domaine"], "company_name": r["nom"],
              "linkedin_url": d["linkedin"], "enrich_fields": ["contact.emails"], "custom": {"idx": str(i)}}
             for i, (d, r) in enumerate(contacts)]
    h = {"Authorization": f"Bearer {FULLENRICH_KEY}", "Content-Type": "application/json"}
    try:
        r = await c.post("https://app.fullenrich.com/api/v1/contact/enrich/bulk", headers=h,
                         json={"name": "Enrichisseur v3", "datas": datas})
        if r.status_code not in (200, 201):
            journal(f"Fullenrich refusé ({r.status_code}) : {r.text[:150]}")
            return {}
        eid = r.json().get("enrichment_id") or r.json().get("id")
        for _ in range(60):                                   # jusqu'à 5 minutes
            await asyncio.sleep(5)
            r2 = await c.get(f"https://app.fullenrich.com/api/v1/contact/enrich/bulk/{eid}", headers=h)
            if r2.status_code != 200:
                continue
            res = r2.json()
            if res.get("status") == "FINISHED":
                out = {}
                for x in res.get("datas", []):
                    idx = (x.get("custom") or {}).get("idx", "")
                    for e in (x.get("contact") or {}).get("emails", []):
                        v = e.get("email") or e.get("value") or ""
                        if "@" in v:
                            out[idx] = v
                            break
                return out
        journal("Fullenrich : pas de réponse après 5 minutes")
    except Exception as e:
        journal(f"Fullenrich en échec : {e}")
    return {}


# ───────────────────────── orchestration des étapes 3 à 5 ─────────────────────────
async def etapes_linkedin(job: dict, ressemblance, domaine):
    res = [r for r in job["resultats"] if r.get("statut") not in ("Radiée",)
           and not r.get("controle_nom", "").startswith("⚠️ le site appartient")][:MAX_LINKEDIN]
    stop = lambda: job["stop"]
    jid = job["id"]

    def journal(msg):
        job.setdefault("journal", []).append(f"{time.strftime('%H:%M:%S')} {msg}")
        print(f"[V3 LINKEDIN {jid}] {msg}")

    pb = PhantomBuster(journal)
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
        # 3a. page LinkedIn : celle du site d'abord, sinon Company URL Finder
        for r in res:
            r["linkedin"], r["source_linkedin"] = (r.get("linkedin_site") or ""), ("site web" if r.get("linkedin_site") else "")
        manquants = [r for r in res if not r["linkedin"]]
        if manquants and PB_URL_FINDER:
            job["phase"] = f"Recherche des pages LinkedIn manquantes ({len(manquants)})…"
            try:
                lignes = await pb.executer(c, PB_URL_FINDER, f"v3_{jid}_urlfinder", stop,
                                           valeurs=[r["nom"] for r in manquants], colonne="Société")
                par_nom = {}
                for l in lignes:
                    if l.get("linkedinUrl"):
                        par_nom.setdefault((l.get("query") or "").strip().lower(), l["linkedinUrl"])
                for r in manquants:
                    u = par_nom.get(r["nom"].strip().lower())
                    if u:
                        r["linkedin"], r["source_linkedin"] = u, "URL Finder (à contrôler)"
            except Exception as e:
                journal(f"URL Finder en échec : {e}")
        if stop():
            return

        # 3b. Company Scraper : adresse + effectif
        a_scraper = [r for r in res if r["linkedin"]]
        if a_scraper:
            job["phase"] = f"LinkedIn : adresse et effectif de {len(a_scraper)} sociétés…"
            try:
                lignes = await pb.executer(c, PB_SCRAPER, f"v3_{jid}_scraper", stop,
                                           valeurs=[r["linkedin"] for r in a_scraper], colonne="linkedinUrl")
            except Exception as e:
                journal(f"Company Scraper en échec : {e}")
                lignes = []
            par_url = {cle_linkedin(l.get("query") or ""): l for l in lignes}
            for l in lignes:
                par_url.setdefault(cle_linkedin(l.get("companyUrl") or ""), l)
            for r in a_scraper:
                l = par_url.get(cle_linkedin(r["linkedin"]))
                if not l:                                   # secours : rapprochement par le nom
                    l = next((x for x in lignes if ressemblance(r["nom"], x.get("name", "")) >= 0.9
                              or (r.get("domaine") and domaine(x.get("website") or "") == r["domaine"])), None)
                if not l:
                    r["controle_linkedin"] = "page non lue par le phantom"
                    continue
                # page trouvée par URL Finder : on vérifie que c'est la bonne société
                if r["source_linkedin"].startswith("URL Finder"):
                    site_li = domaine(l.get("website") or "")
                    ok = (r.get("domaine") and site_li == r["domaine"]) or ressemblance(r["nom"], l.get("name", "")) >= 0.85
                    if not ok:
                        r["controle_linkedin"] = f"❌ page écartée : « {l.get('name', '')} » ({site_li or 'sans site'})"
                        r["linkedin"] = ""
                        continue
                    r["controle_linkedin"] = "✅ page vérifiée (site ou nom)"
                else:
                    r["controle_linkedin"] = "✅ lien du site officiel"
                r["linkedin_id"] = re.sub(r"\D", "", str(l.get("linkedinID") or l.get("mainCompanyID") or ""))
                r["nom_linkedin"] = l.get("name", "")
                r["adresse_linkedin"] = l.get("companyAddress", "")
                r["tranche_linkedin"] = l.get("size", "")
                try:
                    r["effectif_linkedin"] = int(float(l.get("employeesOnLinkedIn") or ""))
                except ValueError:
                    r["effectif_linkedin"] = ""
                r.update(decider_adresse(r))
                eff = r["effectif_linkedin"]
                r["statut_activite"] = ("Quasi inactive (≤ 7 salariés LinkedIn)" if eff != "" and eff <= 7 else "Active") \
                    if r.get("statut") != "Radiée, à vérifier" else "Radiée, à vérifier"
        if stop():
            return

        # 3c. Google Maps : arbitre quand LinkedIn et le CRM ne s'accordent pas (ou quand LinkedIn est muet)
        a_arbitrer = [r for r in res if r.get("confiance_adresse") != "haute" and not stop()]
        if PB_GMAPS and a_arbitrer:
            job["phase"] = f"Google Maps : arbitrage de {len(a_arbitrer)} adresses…"
            def requete(r):
                cle = next((cle_adresse(x) for x in (r.get("adresse_crm"), r.get("adresse_site"), r.get("adresse_linkedin"))
                            if x and cle_adresse(x)), None)
                return f"{r['nom']}, {cle[2] if cle else 'Paris'}"
            for r in a_arbitrer:
                r["requete_google"] = requete(r)
            try:
                lignes = await pb.executer(c, PB_GMAPS, f"v3_{jid}_gmaps", stop, resultats_par_recherche=3,
                                           valeurs=[r["requete_google"] for r in a_arbitrer], colonne="Recherche")
            except Exception as e:
                journal(f"Google Maps en échec : {e}")
                lignes = []
            par_req = {}
            for l in lignes:
                par_req.setdefault((l.get("query") or "").strip().lower(), []).append(l)
            for r in a_arbitrer:
                cands = par_req.get(r["requete_google"].lower(), [])
                # une fiche Google n'est retenue que si son site = celui de la société (ou, sans site connu, nom quasi identique)
                bon = next((l for l in cands if r.get("domaine") and domaine(l.get("website") or "") == r["domaine"]), None)
                if not bon and not r.get("domaine"):
                    bon = next((l for l in cands if ressemblance(r["nom"], l.get("title", "")) >= 0.9), None)
                if bon and bon.get("address"):
                    r["adresse_google"] = bon["address"]
                    r["fiche_google"] = bon.get("title", "")
                    r.update(decider_adresse(r))
                elif cands:
                    r["fiche_google"] = "❌ fiches Google écartées (homonymes) : " + ", ".join(l.get("title", "") for l in cands[:3])
        if stop():
            return
        perim = job.get("perimetre", "tout")
        if perim == "societes":
            journal("Choix « Sociétés » : pas de recherche de dirigeants ni d'e-mails")
            job["phase"] = ""
            return

        # 4. Sales Navigator : dirigeants (seulement les sociétés « complètes », pas le mode adresse seule)
        cibles = [r for r in res if r.get("linkedin_id") and not r.get("mode", "").startswith("adresse")]
        par_id = {r["linkedin_id"]: r for r in cibles}
        if cibles and PB_SALESNAV:
            lots = [cibles[i:i + 18] for i in range(0, len(cibles), 18)]
            for n_lot, lot in enumerate(lots, 1):
                if stop():
                    return
                job["phase"] = f"Sales Navigator : dirigeants, lot {n_lot}/{len(lots)}…"
                try:
                    lignes = await pb.executer(c, PB_SALESNAV, f"v3_{jid}_salesnav_{n_lot}", stop,
                                               url_directe=url_salesnav([(r["linkedin_id"], r.get("nom_linkedin") or r["nom"]) for r in lot]),
                                               resultats_par_recherche=150)
                except Exception as e:
                    journal(f"Sales Navigator lot {n_lot} en échec : {e}")
                    continue
                for l in lignes:
                    cid = re.sub(r"\D", "", str(l.get("companyId") or ""))[:12]
                    r = par_id.get(cid)                     # les homonymes (même nom, autre société) sont écartés ici
                    rl = role(l.get("title", ""))
                    if not r or not rl:
                        continue
                    if not en_france(l.get("location", "")):      # consigne : dirigeants basés en France uniquement
                        job["hors_france"] = job.get("hors_france", 0) + 1
                        continue
                    r.setdefault("dirigeants", []).append({
                        "prenom": (l.get("firstName") or "").strip(), "nom": (l.get("lastName") or "").strip(),
                        "nom_complet": (l.get("fullName") or "").strip(),
                        "fonction": l.get("title", ""), "role": rl, "lieu": l.get("location", ""),
                        "idf": bool(RE_IDF.search(l.get("location", "") or "")),
                        "linkedin": (l.get("defaultProfileUrl") or l.get("linkedInProfileUrl") or "").replace("https://linkedin.com", "https://www.linkedin.com"),
                        "email": "", "source_email": "", "dans_pipedrive": ""})

        if job.get("hors_france"):
            journal(f"Sales Navigator : {job['hors_france']} dirigeants basés hors de France écartés")
        if perim == "dirigeants":
            journal("Choix « Sociétés + dirigeants » : pas de recherche d'e-mails (ni Kaspr ni Fullenrich)")
            job["phase"] = ""
            return

        # 5. e-mails des dirigeants utiles (rôles 1 à 5, basés en France, 4 max par société)
        a_faire = []
        for r in cibles:
            ds = sorted(r.get("dirigeants", []), key=lambda d: d["role"])
            for d in [d for d in ds if d["role"][0] in "12345"][:4]:
                a_faire.append((r, d))

        # 5a. déjà dans Pipedrive ? alors ni Kaspr ni Fullenrich (on reprend l'e-mail de la fiche s'il existe)
        if PIPEDRIVE_KEY:
            for i, (r, d) in enumerate(a_faire, 1):
                if stop():
                    return
                job["phase"] = f"Pipedrive : dirigeants déjà connus ? {i}/{len(a_faire)}…"
                p = await personne_pipedrive(c, d["nom_complet"], r, ressemblance)
                if p:
                    d["dans_pipedrive"] = f"oui (fiche {p['id']})"
                    d["email"], d["source_email"] = p["email"], ("Pipedrive" if p["email"] else "")
        a_enrichir = [(r, d) for r, d in a_faire if not d["dans_pipedrive"]]

        # 5b. Kaspr (e-mails pro illimités) à partir du profil LinkedIn
        if KASPR_KEY:
            for i, (r, d) in enumerate(a_enrichir, 1):
                if stop():
                    return
                job["phase"] = f"Kaspr : e-mails {i}/{len(a_enrichir)}…"
                d["email"] = await kaspr_email(c, d["nom_complet"], d["linkedin"])
                d["source_email"] = "Kaspr" if d["email"] else ""

        # 5c. Fullenrich en secours (crédits) pour ceux que Kaspr n'a pas trouvés
        reste = [(r, d) for r, d in a_enrichir if not d["email"] and r.get("domaine")]
        if FULLENRICH_KEY and reste and not stop():
            job["phase"] = f"Fullenrich : {len(reste)} e-mails en secours…"
            trouves = await fullenrich_emails(c, [(d, r) for r, d in reste], journal)
            for n_, (r, d) in enumerate(reste):
                if trouves.get(str(n_)):
                    d["email"], d["source_email"] = trouves[str(n_)], "Fullenrich"
        job["stats_email"] = {
            "deja_pipedrive": sum(1 for _, d in a_faire if d["dans_pipedrive"]),
            "kaspr": sum(1 for _, d in a_faire if d["source_email"] == "Kaspr"),
            "fullenrich": sum(1 for _, d in a_faire if d["source_email"] == "Fullenrich")}
        journal(f"e-mails : {job['stats_email']['deja_pipedrive']} dirigeants déjà dans Pipedrive (non enrichis), "
                f"{job['stats_email']['kaspr']} trouvés par Kaspr, {job['stats_email']['fullenrich']} par Fullenrich")
    job["phase"] = ""
