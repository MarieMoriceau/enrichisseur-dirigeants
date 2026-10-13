"""
Enrichisseur v3 — étapes 3 à 5 (LinkedIn)
  3. Page LinkedIn de la société (lien du site, sinon Company URL Finder) → Company Scraper :
     adresse des bureaux + effectif réel (salariés sur LinkedIn)
  4. Sales Navigator Search Export, filtré par identifiant LinkedIn de la société → dirigeants
  5. Kaspr → e-mail professionnel des dirigeants

Les phantoms sont pilotés par l'API PhantomBuster : on reprend leurs réglages enregistrés
(cookie LinkedIn, délais…) et on ne remplace que la liste à traiter.
"""
import asyncio, csv, io, json, os, re, time, unicodedata
from urllib.parse import quote

import httpx

PB_KEY = os.getenv("PHANTOMBUSTER_API_KEY", "")
PB_URL_FINDER = os.getenv("PB_URL_FINDER_ID", "")
PB_SCRAPER = os.getenv("PB_COMPANY_SCRAPER_ID", "")
PB_SALESNAV = os.getenv("PB_SALESNAV_ID", "")
KASPR_KEY = os.getenv("KASPR_API_KEY", "")
PB_API = "https://api.phantombuster.com/api/v2"
MAX_LINKEDIN = 150          # quota prudent par lancement (recommandation PhantomBuster avec Sales Navigator)

IDF = ("75", "77", "78", "91", "92", "93", "94", "95")
RE_IDF = re.compile(r"paris|[iî]le-de-france|hauts-de-seine|seine-saint-denis|val-de-marne|yvelines|essonne|"
                    r"val-d'oise|seine-et-marne|boulogne|levallois|neuilly|courbevoie|puteaux|issy|saint-cloud|"
                    r"saint-mand|montreuil|clichy|suresnes|nanterre|la d[ée]fense", re.I)


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


def decider_adresse(crm: str, linkedin: str) -> dict:
    """Compare l'adresse du CRM et celle déclarée sur LinkedIn."""
    li = (linkedin or "").strip()
    if not li:
        return {"adresse_proposee": "", "action_adresse": "⚪ pas d'adresse sur LinkedIn", "confiance_adresse": ""}
    pays = li.rsplit(",", 1)[-1].strip().upper()
    if pays and pays not in ("FR", "FRANCE"):
        return {"adresse_proposee": "", "action_adresse": "⚠️ LinkedIn affiche le siège à l'étranger : garder l'adresse du CRM",
                "confiance_adresse": "basse"}
    if not cle_adresse(li):
        return {"adresse_proposee": "", "action_adresse": "⚪ LinkedIn n'indique que la ville", "confiance_adresse": ""}
    if crm and meme_adresse(crm, li):
        return {"adresse_proposee": crm, "action_adresse": "✅ adresse du CRM confirmée par LinkedIn", "confiance_adresse": "haute"}
    dep = cle_adresse(li)[2][:2]
    hors = "" if dep in IDF else " · hors Île-de-France"
    return {"adresse_proposee": li, "action_adresse": f"🆕 adresse différente sur LinkedIn (déménagement ?){hors} : à valider",
            "confiance_adresse": "moyenne"}


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
        """Le phantom attend une URL de tableur : on dépose un CSV temporaire sur paste.rs (comme prospection-bureaux)."""
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([colonne])
        for v in valeurs:
            w.writerow([v])
        r = await c.post("https://paste.rs/", content=buf.getvalue().encode("utf-8"))
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
                arg[k] = max(n, resultats_par_recherche * n) if resultats_par_recherche else n
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
                r.update(decider_adresse(r.get("adresse_crm", ""), r["adresse_linkedin"]))
                eff = r["effectif_linkedin"]
                r["statut_activite"] = ("Quasi inactive (≤ 7 salariés LinkedIn)" if eff != "" and eff <= 7 else "Active") \
                    if r.get("statut") != "Radiée, à vérifier" else "Radiée, à vérifier"
        if stop():
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
                    r.setdefault("dirigeants", []).append({
                        "prenom": (l.get("firstName") or "").strip(), "nom": (l.get("lastName") or "").strip(),
                        "nom_complet": (l.get("fullName") or "").strip(),
                        "fonction": l.get("title", ""), "role": rl, "lieu": l.get("location", ""),
                        "idf": bool(RE_IDF.search(l.get("location", "") or "")),
                        "linkedin": (l.get("defaultProfileUrl") or l.get("linkedInProfileUrl") or "").replace("https://linkedin.com", "https://www.linkedin.com"),
                        "email": ""})

        # 5. Kaspr : e-mails des dirigeants (rôles 1 à 5, en Île-de-France ou lieu inconnu, 4 max par société)
        if KASPR_KEY:
            a_faire = []
            for r in cibles:
                ds = sorted(r.get("dirigeants", []), key=lambda d: d["role"])
                a_faire += [d for d in ds if d["role"][0] in "12345" and (d["idf"] or not d["lieu"])][:4]
            for i, d in enumerate(a_faire, 1):
                if stop():
                    return
                job["phase"] = f"Kaspr : e-mails {i}/{len(a_faire)}…"
                d["email"] = await kaspr_email(c, d["nom_complet"], d["linkedin"])
    job["phase"] = ""
