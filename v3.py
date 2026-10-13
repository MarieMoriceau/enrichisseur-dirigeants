"""
Enrichisseur v3 — étapes 1 et 2
  1. Site web de la société : SIREN (mentions légales) + page LinkedIn officielle (pied de page)
  2. INSEE (API recherche-entreprises, gratuite) : société active ou fermée, raison sociale officielle

Entrée : export Pipedrive des Organisations (xlsx ou csv).
Sortie : Excel prêt pour HubSpot (rien n'est écrit dans un CRM).
"""
import asyncio, csv, io, os, re, time, unicodedata, uuid
from difflib import SequenceMatcher
from datetime import date
from html import unescape
from urllib.parse import urljoin, urlparse

import httpx
from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment

router = APIRouter()
JOBS: dict = {}

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
INSEE_API = "https://recherche-entreprises.api.gouv.fr/search"
PAPPERS_KEY = os.getenv("PAPPERS_API_KEY", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
FORMES = r"\b(sas|sasu|sarl|eurl|sa|sca|snc|sci|selarl|selas|scp|groupe|group|holding|france|paris|international|ltd|limited|gmbh|inc|llp|llc|et associes|associes|and partners|partners)\b"
CHEMINS_LEGAUX = ["/mentions-legales", "/mentions-legales/", "/fr/mentions-legales", "/legal",
                  "/mentions", "/legal-notice", "/informations-legales", "/cgu", "/cgv"]
MOTS_LEGAUX = re.compile(r"mentions?[\s\-_]*l[ée]gales?|legal|imprint|cgu|cgv|conditions|informations[\s\-_]*l[ée]gales", re.I)
RE_LINKEDIN = re.compile(r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/company/([A-Za-z0-9\-_%.&']+)", re.I)
RE_SIRET = re.compile(r"(?<!\d)(\d{3}[\s. ]?\d{3}[\s. ]?\d{3}[\s. ]?\d{5})(?!\d)")
RE_SIREN_CTX = re.compile(
    r"(?:siren|rcs|r\.c\.s\.?|registre du commerce|immatricul\w+|n°\s*tva|tva intracom\w*|siret)"
    r"[^0-9]{0,60}?((?:FR\s?\d{2}\s?)?\d{3}[\s. ]?\d{3}[\s. ]?\d{3})(?![\d])", re.I)


# ───────────────────────── utilitaires ─────────────────────────
def luhn_ok(n: str) -> bool:
    if len(n) != 9 or not n.isdigit():
        return False
    s = 0
    for i, ch in enumerate(reversed(n)):
        d = int(ch)
        if i % 2:
            d *= 2
            if d > 9:
                d -= 9
        s += d
    return s % 10 == 0


def domaine(url) -> str:
    if not url or not isinstance(url, str):
        return ""
    u = url.strip().lower()
    if u in ("v", "-", "n/a"):
        return ""
    if not u.startswith("http"):
        u = "http://" + u
    h = urlparse(u).netloc.split("@")[-1].split(":")[0]
    h = h[4:] if h.startswith("www.") else h
    if "." not in h or any(x in h for x in ("linkedin.", "facebook.", "linktr.ee", "pappers.", "societe.com")):
        return ""
    return h


def texte(html: str) -> str:
    html = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", unescape(html))


def sirens_dans(t: str) -> list:
    """SIREN candidats trouvés dans un texte (contrôle de Luhn)."""
    c = []
    for m in RE_SIREN_CTX.finditer(t):
        n = re.sub(r"\D", "", m.group(1))
        if len(n) == 11 and m.group(1).upper().startswith("FR"):   # n° TVA FRxx + SIREN
            n = n[2:]
        n = n[-9:]
        if luhn_ok(n):
            c.append(n)
    for m in RE_SIRET.finditer(t):
        n = re.sub(r"\D", "", m.group(1))[:9]
        if luhn_ok(n):
            c.append(n)
    return c


def linkedins_dans(html: str) -> list:
    out = []
    for m in RE_LINKEDIN.finditer(html):
        slug = m.group(1).strip("/.").split("?")[0]
        if slug and slug.lower() not in ("linkedin", "company"):
            out.append("https://www.linkedin.com/company/" + slug + "/")
    return out


def liens_legaux(html: str, base: str) -> list:
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', html):
        href, lib = m.group(1), texte(m.group(2))
        if MOTS_LEGAUX.search(href) or MOTS_LEGAUX.search(lib):
            u = urljoin(base, href)
            if urlparse(u).netloc.replace("www.", "") == urlparse(base).netloc.replace("www.", ""):
                out.append(u)
    return list(dict.fromkeys(out))[:4]


# ───────────────────────── étape 1 : site web ─────────────────────────
async def scanner_site(client: httpx.AsyncClient, dom: str) -> dict:
    r = {"site_joignable": "non", "siren_site": "", "page_siren": "", "linkedin_site": "", "note_site": ""}
    if not dom:
        r["note_site"] = "pas de site dans le CRM"
        return r
    html, base = None, None
    for prefixe in (f"https://{dom}", f"https://www.{dom}", f"http://{dom}"):
        try:
            resp = await client.get(prefixe)
            if resp.status_code < 400 and "html" in resp.headers.get("content-type", "html"):
                html, base = resp.text, str(resp.url)
                break
        except Exception:
            continue
    if html is None:
        r["note_site"] = "site injoignable"
        return r
    r["site_joignable"] = "oui"
    li = linkedins_dans(html)
    sirens = [(s, base) for s in sirens_dans(texte(html))]

    pages = liens_legaux(html, base)
    if not sirens:
        pages += [urljoin(base, c) for c in CHEMINS_LEGAUX]
    vues = set()
    for p in pages:
        if sirens or len(vues) >= 6:
            break
        if p in vues:
            continue
        vues.add(p)
        try:
            resp = await client.get(p)
            if resp.status_code >= 400:
                continue
            sirens += [(s, str(resp.url)) for s in sirens_dans(texte(resp.text))]
            li += linkedins_dans(resp.text)
        except Exception:
            continue

    if sirens:
        compte = {}
        for s, _ in sirens:
            compte[s] = compte.get(s, 0) + 1
        best = max(compte, key=compte.get)
        r["siren_site"] = best
        r["page_siren"] = next(p for s, p in sirens if s == best)
        if len(compte) > 1:
            r["note_site"] = "plusieurs SIREN sur le site : " + ", ".join(compte)
    else:
        r["note_site"] = "aucun SIREN affiché"
    if li:
        r["linkedin_site"] = max(set(li), key=li.count)
    return r


# ───────────────────────── étape 2 : INSEE ─────────────────────────
async def insee(client: httpx.AsyncClient, siren: str) -> dict:
    vide = {"raison_sociale": "", "etat": "", "date_fermeture": "", "siege": "", "tranche_insee": ""}
    if not siren:
        return vide
    for essai in range(3):
        try:
            resp = await client.get(INSEE_API, params={"q": siren, "per_page": 1})
            if resp.status_code == 429:
                await asyncio.sleep(2 + essai * 3)
                continue
            res = resp.json().get("results", [])
            res = [x for x in res if x.get("siren") == siren]
            if not res:
                return {**vide, "etat": "introuvable"}
            x = res[0]
            sg = x.get("siege") or {}
            etat = x.get("etat_administratif") or sg.get("etat_administratif") or ""
            return {
                "raison_sociale": x.get("nom_raison_sociale") or x.get("nom_complet") or "",
                "etat": {"A": "active", "C": "fermée"}.get(etat, etat),
                "date_fermeture": x.get("date_fermeture") or "",
                "siege": sg.get("adresse") or "",
                "tranche_insee": x.get("tranche_effectif_salarie") or "",
            }
        except Exception:
            await asyncio.sleep(1)
    return {**vide, "etat": "erreur API"}


# ───────────────────────── listes de noms : retrouver le site ─────────────────────────
def norm_nom(x: str) -> str:
    x = unicodedata.normalize("NFKD", x or "").encode("ascii", "ignore").decode().lower()
    x = re.sub(r"[^a-z0-9 ]", " ", x)
    x = re.sub(FORMES, " ", x)
    return " ".join(x.split())


def ressemblance(a: str, b: str) -> float:
    a, b = norm_nom(a), norm_nom(b)
    if not a or not b:
        return 0.0
    if a == b or a.replace(" ", "") == b.replace(" ", ""):
        return 1.0
    ta, tb = set(a.split()), set(b.split())
    inclus = len(ta & tb) / min(len(ta), len(tb))
    return max(SequenceMatcher(None, a, b).ratio(), inclus * 0.95)


async def chercher_domaine(api: httpx.AsyncClient, nom: str, cp: str, ville: str) -> tuple:
    """Retrouve le site officiel d'une société à partir de son nom. Renvoie (domaine, source)."""
    if PAPPERS_KEY:
        try:
            p = {"api_token": PAPPERS_KEY, "q": nom, "par_page": 3}
            if cp:
                p["code_postal"] = cp
            r = await api.get("https://api.pappers.fr/v2/recherche", params=p)
            if r.status_code == 200:
                for x in r.json().get("resultats", []):
                    d = domaine(x.get("domaine_url") or x.get("site_web") or "")
                    if d and ressemblance(nom, x.get("nom_entreprise") or x.get("denomination") or "") >= 0.75:
                        return d, "Pappers"
        except Exception:
            pass
    if ANTHROPIC_KEY:
        lieu = f" ({ville or cp})" if (ville or cp) else ""
        prompt = (f"Quel est le site web officiel de la société française « {nom} »{lieu} ? "
                  "Pas un annuaire (Pappers, Societe.com, LinkedIn, Kompass…), le site de la société elle-même. "
                  "Réponds UNIQUEMENT par le nom de domaine (ex : exemple.fr), ou NON si tu n'es pas sûr.")
        try:
            r = await api.post("https://api.anthropic.com/v1/messages", timeout=60, headers={
                "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-sonnet-4-6", "max_tokens": 60,
                      "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 2}],
                      "messages": [{"role": "user", "content": prompt}]})
            if r.status_code == 200:
                txt = " ".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text").strip()
                m = re.findall(r"[a-z0-9][a-z0-9\-.]*\.[a-z]{2,}", txt.lower())
                if m and "non" != txt.lower().strip(" .") and domaine(m[-1]):
                    return domaine(m[-1]), "recherche web (Claude)"
        except Exception:
            pass
    return "", ""


async def insee_par_nom(api: httpx.AsyncClient, nom: str, cp: str) -> dict:
    """Dernier recours : SIREN par recherche du nom à l'INSEE (moins fiable)."""
    try:
        p = {"q": nom, "per_page": 5}
        if cp:
            p["code_postal"] = cp
        r = await api.get(INSEE_API, params=p)
        res = r.json().get("results", []) if r.status_code == 200 else []
        bons = [x for x in res if ressemblance(nom, x.get("nom_raison_sociale") or x.get("nom_complet") or "") >= 0.85]
        bons.sort(key=lambda x: (x.get("etat_administratif") != "A",))
        if bons:
            return {"siren": bons[0].get("siren", ""), "nb": len(bons)}
    except Exception:
        pass
    return {}


# ───────────────────────── décision ─────────────────────────
def statut(row: dict) -> tuple:
    st, act, conf = _statut(row)
    if row.get("controle_nom", "").startswith("⚠️") and conf == "haute":
        conf = "moyenne"
        act += " · site trouvé par recherche, nom différent : à vérifier"
    return st, act, conf


def _statut(row: dict) -> tuple:
    sp, ss = row["siren_crm"], row["siren_site"]
    if not ss and not sp and row.get("siren_nom"):
        e = row.get("etat_nom", "")
        return ("Active" if e == "active" else "Radiée, à vérifier" if e == "fermée" else ""), \
               f"🔎 SIREN trouvé par le nom à l'INSEE ({row.get('nb_homonymes', 1)} correspondance(s)) : à vérifier", "moyenne"
    e_crm, e_site = row.get("etat_crm", ""), row.get("etat_site", "")
    if ss and e_site == "active":
        if sp and sp != ss:
            return "Active", "🔄 SIREN du CRM à remplacer par celui du site", "haute"
        return "Active", "✅ SIREN confirmé par le site" if sp == ss else "🆕 SIREN trouvé sur le site", "haute"
    if ss and e_site == "fermée":
        if row["site_joignable"] == "oui":
            return "Radiée, à vérifier", "⚠️ le site affiche un SIREN fermé (site pas à jour ?)", "moyenne"
        return "Radiée", "🗑️ candidate à la suppression", "haute"
    if sp and e_crm == "fermée":
        if row["site_joignable"] == "oui":
            return "Radiée, à vérifier", "⚠️ SIREN du CRM fermé mais site en ligne : la société a peut-être changé de SIREN", "moyenne"
        return "Radiée", "🗑️ candidate à la suppression (SIREN fermé + site mort)", "haute"
    if sp and e_crm == "active":
        return "Active", "SIREN du CRM actif (non affiché sur le site)", "moyenne"
    return "", "⚪ rien de vérifiable", ""


# ───────────────────────── lecture du fichier ─────────────────────────
def _cp(l, c_cp, c_adr):
    for c in (c_cp, c_adr):
        if c:
            m = re.search(r"\b(\d{5})\b", str(l.get(c) or ""))
            if m:
                return m.group(1)
    return ""


def lire_liste(t: str) -> list:
    """Liste collée : une société par ligne."""
    return [{"id": "", "nom": n.strip(" \t;,"), "site_crm": "", "siren_crm": "", "adresse_crm": "", "cp": "", "ville": ""}
            for n in t.splitlines() if n.strip(" \t;,")]

def lire_fichier(nom: str, data: bytes) -> list:
    if nom.lower().endswith((".xlsx", ".xlsm")):
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb.worksheets[0]
        it = ws.iter_rows(values_only=True)
        tetes = [str(h or "").strip() for h in next(it)]
        lignes = [dict(zip(tetes, ["" if v is None else str(v) for v in r])) for r in it]
    else:
        t = data.decode("utf-8-sig", errors="replace")
        try:
            delim = csv.Sniffer().sniff(t[:4000], delimiters=",;\t").delimiter
        except csv.Error:                       # une seule colonne : pas de séparateur
            delim = ";" if ";" in t[:4000] else "\t" if "\t" in t[:4000] else ","
        lignes = list(csv.DictReader(io.StringIO(t), delimiter=delim))

    if lignes and len(lignes[0]) == 1:      # une seule colonne sans en-tête : la 1re ligne est déjà une société
        h = list(lignes[0].keys())[0]
        if norm_nom(h) not in ("nom", "societe", "entreprise", "company", "name", "raison sociale", "organisation",
                               "organisation nom", "nom societe", "nom entreprise", ""):
            lignes.insert(0, {h: h})

    def col(*mots, exclure=()):
        for h in (lignes[0].keys() if lignes else []):
            hl = h.lower()
            if all(m in hl for m in mots) and not any(x in hl for x in exclure):
                return h
        return None
    c_id = col("organisation", "id") or col("id", exclure=("personne",))
    c_nom = (col("organisation", "nom") or col("nom", exclure=("personne", "famille", "prénom", "prenom"))
             or col("société") or col("societe") or col("entreprise") or col("company") or col("raison sociale") or col("name"))
    if not c_nom and lignes:
        c_nom = list(lignes[0].keys())[0]       # fichier d'une seule colonne : la 1re colonne = les noms
    c_cp = col("code postal") or col("cp") or col("zip")
    c_ville = col("ville") or col("city")
    c_site = col("site internet") or col("site web") or col("website") or col("site")
    c_site2 = col("site web") if c_site and "internet" in c_site.lower() else None
    c_siren = col("siren")
    c_adr = col("adresse", exclure=("verifi", "vérifi")) or col("address")
    out = []
    for l in lignes:
        if not (l.get(c_nom) or "").strip():
            continue
        siren = re.sub(r"\D", "", l.get(c_siren) or "")[:9] if c_siren else ""
        out.append({"id": (l.get(c_id) or "").replace(".0", "") if c_id else "",
                    "nom": l.get(c_nom, "").strip(),
                    "site_crm": ((l.get(c_site) or "").strip() or (l.get(c_site2) or "").strip()) if c_site else "",
                    "siren_crm": siren if luhn_ok(siren) else "",
                    "adresse_crm": (l.get(c_adr) or "").strip() if c_adr else "",
                    "cp": _cp(l, c_cp, c_adr), "ville": (l.get(c_ville) or "").strip() if c_ville else ""})
    return out


# ───────────────────────── traitement ─────────────────────────
async def traiter(job_id: str, orgs: list):
    job = JOBS[job_id]
    sem = asyncio.Semaphore(8)
    limits = httpx.Limits(max_connections=20)
    async with httpx.AsyncClient(timeout=12, follow_redirects=True, headers={"User-Agent": UA,
            "Accept-Language": "fr-FR,fr;q=0.9"}, limits=limits, verify=False) as web, \
               httpx.AsyncClient(timeout=15) as api:
        api_sem = asyncio.Semaphore(4)   # l'API INSEE limite à ~7 requêtes/s

        async def un(o):
            async with sem:
                if job["stop"]:
                    return
                dom, src = domaine(o["site_crm"]), "CRM"
                if not dom:
                    dom, src = await chercher_domaine(api, o["nom"], o.get("cp", ""), o.get("ville", ""))
                s = await scanner_site(web, dom)
                row = {**o, "domaine": dom, "source_site": src if dom else "", **s,
                       "controle_nom": "", "siren_nom": "", "etat_nom": "", "nb_homonymes": ""}
                async with api_sem:
                    i_site = await insee(api, s["siren_site"]) if s["siren_site"] else {}
                    if not o["siren_crm"]:
                        i_crm = {}
                    elif o["siren_crm"] == s["siren_site"]:
                        i_crm = i_site
                    else:
                        i_crm = await insee(api, o["siren_crm"])
                    i_nom = {}
                    if not s["siren_site"] and not o["siren_crm"]:
                        trouve = await insee_par_nom(api, o["nom"], o.get("cp", ""))
                        if trouve.get("siren"):
                            i_nom = await insee(api, trouve["siren"])
                            row.update({"siren_nom": trouve["siren"], "etat_nom": i_nom.get("etat", ""),
                                        "nb_homonymes": trouve.get("nb", 1)})
                row["etat_site"] = i_site.get("etat", "")
                row["etat_crm"] = i_crm.get("etat", "")
                ref = i_site or i_crm or i_nom
                row.update({"raison_sociale": ref.get("raison_sociale", ""), "date_fermeture": ref.get("date_fermeture", ""),
                            "siege_insee": ref.get("siege", ""), "tranche_insee": ref.get("tranche_insee", "")})
                # site trouvé par recherche : on vérifie que c'est bien la bonne société
                if dom and src != "CRM":
                    if s["siren_site"] and i_site.get("raison_sociale"):
                        ok = ressemblance(o["nom"], i_site["raison_sociale"]) >= 0.6 or \
                             norm_nom(o["nom"]).replace(" ", "") in dom.replace("-", "").replace(".", "")
                        row["controle_nom"] = "✅ nom cohérent" if ok else f"⚠️ le site appartient à « {i_site['raison_sociale']} »"
                    elif s["site_joignable"] == "oui":
                        ok = norm_nom(o["nom"]).replace(" ", "")[:8] in dom.replace("-", "").replace(".", "")
                        row["controle_nom"] = "✅ nom dans le domaine" if ok else "⚠️ non vérifiable (pas de SIREN sur le site)"
                row["statut"], row["action"], row["confiance"] = statut(row)
                job["resultats"].append(row)
                job["fait"] += 1
        await asyncio.gather(*(un(o) for o in orgs))
    job["fin"] = time.time()
    job["etat"] = "terminé" if not job["stop"] else "arrêté"


# ───────────────────────── Excel ─────────────────────────
VIOLET = PatternFill("solid", fgColor="4A1E5C")


def feuille(wb, titre, entetes, lignes):
    ws = wb.create_sheet(titre)
    ws.append(entetes)
    for r in lignes:
        ws.append(r)
    for c in ws[1]:
        c.fill, c.font = VIOLET, Font(name="Arial", bold=True, color="FFFFFF")
        c.alignment = Alignment(wrap_text=True, vertical="center")
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.font = Font(name="Arial", size=10)
    for i, h in enumerate(entetes, 1):
        largeur = max([len(str(h))] + [len(str(r[i - 1])) for r in lignes[:200] if i - 1 < len(r)])
        ws.column_dimensions[ws.cell(1, i).column_letter].width = min(max(10, largeur + 2), 60)
    ws.freeze_panes = "C2"
    ws.auto_filter.ref = ws.dimensions


def excel(job) -> bytes:
    res = sorted(job["resultats"], key=lambda r: r["nom"].lower())
    auj = date.today().isoformat()
    n = len(res)
    k = lambda f: sum(1 for r in res if f(r))
    wb = Workbook()
    ws = wb.active
    ws.title = "Lisez-moi"
    lignes = [
        f"ENRICHISSEUR V3 · étapes 1 et 2 · {auj}", "",
        f"{n} organisations traitées",
        f"Site joignable : {k(lambda r: r['site_joignable'] == 'oui')}",
        f"Site retrouvé par recherche (absent du fichier) : {k(lambda r: r['source_site'] not in ('', 'CRM'))}",
        f"SIREN trouvé sur le site : {k(lambda r: r['siren_site'])}",
        f"SIREN trouvé seulement par le nom (à vérifier) : {k(lambda r: r['siren_nom'])}",
        f"Page LinkedIn trouvée sur le site : {k(lambda r: r['linkedin_site'])}",
        f"SIREN du CRM à remplacer : {k(lambda r: r['action'].startswith('🔄'))}",
        f"Radiées candidates à la suppression : {k(lambda r: r['statut'] == 'Radiée')}",
        f"Radiées à vérifier (site encore en ligne) : {k(lambda r: r['statut'] == 'Radiée, à vérifier')}", "",
        "ONGLETS",
        "• Résultats : tout le détail, ligne par ligne",
        "• Import HubSpot : uniquement les sociétés actives de confiance haute (clé = nom de domaine)",
        "• Suppressions : radiées avec site mort, à valider avant de les retirer",
        "• À vérifier : SIREN fermé mais site en ligne (changement de SIREN probable)", "",
        "RÈGLES",
        "• Le SIREN vient des mentions légales du site, contrôlé (clé de Luhn) puis vérifié à l'INSEE.",
        "• Société sans site connu : le site est recherché (Pappers, puis recherche web), puis on vérifie que le SIREN affiché correspond bien au nom.",
        "• Dernier recours, SIREN cherché par le nom à l'INSEE : confiance moyenne, jamais dans l'onglet Import HubSpot.",
        "• L'adresse INSEE est celle du SIÈGE LÉGAL (souvent une domiciliation) : elle n'est PAS proposée comme adresse des bureaux.",
        "• « Suppressions » = SIREN fermé + site mort. Le contrôle LinkedIn (3e signal) viendra à l'étape suivante : ne rien supprimer avant.",
        "• Rien n'est écrit dans Pipedrive ni HubSpot.",
    ]
    for l in lignes:
        ws.append([l])
    ws["A1"].font = Font(name="Arial", bold=True, size=13, color="4A1E5C")
    ws.column_dimensions["A"].width = 110

    cols = [("ID Pipedrive", "id"), ("Société", "nom"), ("Domaine", "domaine"), ("Site trouvé via", "source_site"),
            ("Contrôle du nom", "controle_nom"), ("Site joignable", "site_joignable"),
            ("SIREN CRM", "siren_crm"), ("État SIREN CRM", "etat_crm"), ("SIREN site", "siren_site"),
            ("État SIREN site", "etat_site"), ("Page où le SIREN a été trouvé", "page_siren"),
            ("SIREN par le nom (INSEE)", "siren_nom"), ("Raison sociale INSEE", "raison_sociale"), ("Date fermeture", "date_fermeture"),
            ("Siège légal INSEE", "siege_insee"), ("LinkedIn (site)", "linkedin_site"),
            ("Statut proposé", "statut"), ("Action", "action"), ("Confiance", "confiance"), ("Note", "note_site")]
    feuille(wb, "Résultats", [c for c, _ in cols], [[r.get(k_, "") for _, k_ in cols] for r in res])

    hub = [r for r in res if r["statut"] == "Active" and r["confiance"] == "haute" and r["domaine"]]
    feuille(wb, "Import HubSpot",
            ["Company Domain Name", "Name", "SIREN", "LinkedIn Company Page", "STATUT ACTIVITÉ", "Raison sociale", "ID Pipedrive", "Date relevé"],
            [[r["domaine"], r["nom"], r["siren_site"] or r["siren_crm"] or r["siren_nom"], r["linkedin_site"], "Active",
              r["raison_sociale"], r["id"], auj] for r in hub])
    sup = [r for r in res if r["statut"] == "Radiée"]
    feuille(wb, "Suppressions", ["ID Pipedrive", "Société", "Domaine", "SIREN", "Raison sociale INSEE", "Date fermeture", "Site joignable", "Supprimer ? (oui/non)"],
            [[r["id"], r["nom"], r["domaine"], r["siren_site"] or r["siren_crm"], r["raison_sociale"], r["date_fermeture"], r["site_joignable"], ""] for r in sup])
    av = [r for r in res if r["statut"] == "Radiée, à vérifier"]
    feuille(wb, "À vérifier", ["ID Pipedrive", "Société", "Domaine", "SIREN CRM", "SIREN site", "Raison sociale INSEE", "Date fermeture", "Action"],
            [[r["id"], r["nom"], r["domaine"], r["siren_crm"], r["siren_site"], r["raison_sociale"], r["date_fermeture"], r["action"]] for r in av])
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue()


# ───────────────────────── routes ─────────────────────────
@router.post("/v3/run")
async def run(fichier: UploadFile | None = File(None), liste: str = Form(""), limite: int = Form(50)):
    if fichier is not None and fichier.filename:
        orgs = lire_fichier(fichier.filename, await fichier.read())
    else:
        orgs = lire_liste(liste)
    if not orgs:
        return JSONResponse({"erreur": "Aucune société lue : déposez un fichier avec une colonne de noms, ou collez une liste."}, 400)
    if limite and limite > 0:
        orgs = orgs[:limite]
    jid = uuid.uuid4().hex[:8]
    JOBS[jid] = {"total": len(orgs), "fait": 0, "resultats": [], "etat": "en cours", "stop": False,
                 "debut": time.time(), "fin": None}
    asyncio.create_task(traiter(jid, orgs))
    return {"job": jid, "total": len(orgs)}


@router.get("/v3/status/{jid}")
async def status(jid: str):
    j = JOBS.get(jid)
    if not j:
        return JSONResponse({"erreur": "job inconnu (le service a peut-être redémarré)"}, 404)
    res = j["resultats"]
    return {"etat": j["etat"], "total": j["total"], "fait": j["fait"],
            "siren": sum(1 for r in res if r["siren_site"] or r["siren_crm"] or r["siren_nom"]), "linkedin": sum(1 for r in res if r["linkedin_site"]),
            "radiees": sum(1 for r in res if r["statut"].startswith("Radiée")),
            "secondes": int((j["fin"] or time.time()) - j["debut"])}


@router.post("/v3/stop/{jid}")
async def stop(jid: str):
    if jid in JOBS:
        JOBS[jid]["stop"] = True
    return {"ok": True}


@router.get("/v3/excel/{jid}")
async def telecharger(jid: str):
    j = JOBS.get(jid)
    if not j:
        return JSONResponse({"erreur": "job inconnu"}, 404)
    nom = f"enrichisseur_v3_{date.today().isoformat()}_{j['fait']}_societes.xlsx"
    return StreamingResponse(io.BytesIO(excel(j)), headers={"Content-Disposition": f'attachment; filename="{nom}"'},
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@router.get("/", response_class=HTMLResponse)
async def accueil():
    with open("templates/v3.html", encoding="utf-8") as f:
        return f.read()
