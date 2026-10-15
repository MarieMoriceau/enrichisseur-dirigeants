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

import v3_linkedin as LI

router = APIRouter()
JOBS: dict = {}

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
INSEE_API = "https://recherche-entreprises.api.gouv.fr/search"
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PIPEDRIVE_KEY = os.getenv("PIPEDRIVE_API_KEY", "")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN", "")   # à brancher après la migration
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


def domaines(url) -> list:
    """Tous les domaines d'un champ CRM (« mozoo.com | www.mozoo.fr » → 2 domaines)."""
    if not url or not isinstance(url, str):
        return []
    out = [domaine(m) for m in re.split(r"[|;,\s]+", url.strip()) if "." in m]
    return list(dict.fromkeys(d for d in out if d))


def domaine(url) -> str:
    if not url or not isinstance(url, str):
        return ""
    morceaux = [m for m in re.split(r"[|;,\s]+", url.strip().lower()) if "." in m]
    if len(morceaux) > 1:
        for m in morceaux:
            d = domaine(m)
            if d:
                return d
        return ""
    u = morceaux[0] if morceaux else ""
    if u in ("", "v", "-", "n/a"):
        return ""
    if not u.startswith("http"):
        u = "http://" + u
    h = urlparse(u).netloc.split("@")[-1].split(":")[0]
    h = h[4:] if h.startswith("www.") else h
    if "." not in h or any(x in h for x in (
            "linkedin.", "facebook.", "instagram.", "twitter.", "linktr.ee", "pappers.", "societe.com", "infogreffe.",
            "verif.com", "data.gouv", "annuaire", "kompass.", "manageo.", "societeinfo.", "corporama.", "dnb.com",
            "crunchbase.", "welcometothejungle.", "google.", "wikipedia.", "pagesjaunes.", "lefigaro.", "lesechos.")):
        return ""
    return h


def texte(html: str) -> str:
    html = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", unescape(html))


HEBERGEURS = {"424761419",   # OVH
               "423093459",   # Gandi
               "431303775",   # IONOS / 1&1
               "433115904",   # Scaleway / Online
               "510909807",   # o2switch
               "450453881",   # LWS
               "538176917",   # PlanetHoster France
               "813805735",   # Hostinger France
               }
RE_HEBERGEUR = re.compile(r"h[ée]berg|hosting|hosted by|h[ée]bergeur|prestataire technique", re.I)


def _hebergeur(t: str, pos: int) -> bool:
    """Vrai si ce numéro suit une mention « hébergé par… » sans autre n° d'immatriculation entre les deux."""
    seg = t[max(0, pos - 200):pos]
    mots = list(RE_HEBERGEUR.finditer(seg))
    if not mots:
        return False
    entre = seg[mots[-1].end():]
    return not re.search(r"\d{3}[\s.\u00a0]?\d{3}[\s.\u00a0]?\d{3}", entre)


def sirens_dans(t: str) -> list:
    """SIREN candidats trouvés dans un texte (contrôle de Luhn, hébergeurs écartés)."""
    c = []
    for m in RE_SIREN_CTX.finditer(t):
        if _hebergeur(t, m.start()):
            continue
        n = re.sub(r"\D", "", m.group(1))
        if len(n) == 11 and m.group(1).upper().startswith("FR"):   # n° TVA FRxx + SIREN
            n = n[2:]
        n = n[-9:]
        if luhn_ok(n) and n not in HEBERGEURS:
            c.append(n)
    for m in RE_SIRET.finditer(t):
        n = re.sub(r"\D", "", m.group(1))[:9]
        if luhn_ok(n) and n not in HEBERGEURS and not _hebergeur(t, m.start()):
            c.append(n)
    return c


def linkedins_dans(html: str) -> list:
    out = []
    html = unescape(html)
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


# ───────────────────────── étape 0 : la société est-elle déjà dans le CRM ? ─────────────────────────
_CHAMPS_PD = {}


async def _champs_pipedrive(api: httpx.AsyncClient) -> dict:
    """Clés internes des champs SIREN et SITE INTERNET des organisations Pipedrive (lues une fois)."""
    if not _CHAMPS_PD:
        try:
            r = await api.get("https://api.pipedrive.com/v1/organizationFields",
                              params={"api_token": PIPEDRIVE_KEY, "limit": 500})
            for f in r.json().get("data") or []:
                n = (f.get("name") or "").lower()
                if n == "siren":
                    _CHAMPS_PD["siren"] = f["key"]
                elif n in ("site internet", "site web", "website") and "site" not in _CHAMPS_PD:
                    _CHAMPS_PD["site"] = f["key"]
        except Exception as e:
            print(f"[V3 PIPEDRIVE CHAMPS] {e}")
        _CHAMPS_PD.setdefault("ok", True)
    return _CHAMPS_PD


async def chercher_crm(api: httpx.AsyncClient, nom: str, cp: str) -> dict:
    """Cherche la société dans Pipedrive par son nom (et son code postal s'il est connu)."""
    if not PIPEDRIVE_KEY:
        return {}
    try:
        champs = await _champs_pipedrive(api)
        r = await api.get("https://api.pipedrive.com/v1/organizations/search",
                          params={"term": nom, "fields": "name", "limit": 5, "api_token": PIPEDRIVE_KEY})
        items = [i.get("item", {}) for i in (r.json().get("data") or {}).get("items", [])]
        cands = [i for i in items if ressemblance(nom, i.get("name", "")) >= 0.9]
        if cp:
            meme_cp = [i for i in cands if cp in str((i.get("address") or ""))]
            cands = meme_cp or cands
        if not cands:
            return {}
        # doublons : on préfère la fiche qui a un SIREN (puis un site)
        choix = None
        for org in cands[:3]:
            d = (await api.get(f"https://api.pipedrive.com/v1/organizations/{org['id']}",
                               params={"api_token": PIPEDRIVE_KEY})).json().get("data") or {}
            sir = re.sub(r"\D", "", str(d.get(champs.get("siren", ""), "") or ""))[:9]
            note = (luhn_ok(sir), bool(d.get(champs.get("site", ""))))
            if choix is None or note > choix[0]:
                choix = (note, org, d, sir)
        _, org, d, siren = choix
        return {"id": str(org["id"]), "autres_fiches": ", ".join(str(c["id"]) for c in cands if c["id"] != org["id"]),
                "nom_crm": d.get("name", ""), "siren": siren if luhn_ok(siren) else "",
                "site": str(d.get(champs.get("site", ""), "") or ""), "adresse": d.get("address") or "",
                "doublons": len(cands)}
    except Exception as e:
        print(f"[V3 PIPEDRIVE] {nom}: {e}")
        return {}


# ───────────────────────── étape 1 : site web ─────────────────────────
async def scanner_site(client: httpx.AsyncClient, dom: str) -> dict:
    r = {"site_joignable": "non", "siren_site": "", "page_siren": "", "linkedin_site": "", "note_site": "", "candidats": []}
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
        r["candidats"] = sorted(compte, key=compte.get, reverse=True)[:3]
        r["pages"] = {c: next(p for s_, p in sirens if s_ == c) for c in r["candidats"]}
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


async def chercher_domaine(api: httpx.AsyncClient, nom: str, cp: str, ville: str, raison: str = "", siren: str = "") -> tuple:
    """Retrouve le site officiel d'une société à partir de son nom (recherche web Claude). Renvoie (domaine, source)."""
    if ANTHROPIC_KEY:
        lieu = f" ({ville or cp})" if (ville or cp) else ""
        ident = f", raison sociale {raison}, SIREN {siren}" if siren else ""
        prompt = (f"Quel est le site web officiel de la société française « {nom} »{lieu}{ident} ? "
                  "Pas un annuaire (Pappers, Societe.com, LinkedIn, Kompass…), le site de la société elle-même. "
                  "Réponds UNIQUEMENT par le nom de domaine (ex : exemple.fr), ou NON si tu n'es pas sûr.")
        try:
            r = await api.post("https://api.anthropic.com/v1/messages", timeout=60, headers={
                "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "anthropic-beta": "web-search-2025-03-05",
                "content-type": "application/json"},
                json={"model": "claude-sonnet-4-6", "max_tokens": 1024,
                      "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 2}],
                      "messages": [{"role": "user", "content": prompt}]})
            if r.status_code == 200:
                txt = " ".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text").strip()
                m = re.findall(r"[a-z0-9][a-z0-9\-.]*\.[a-z]{2,}", txt.lower())
                if m and "non" != txt.lower().strip(" .") and domaine(m[-1]):
                    return domaine(m[-1]), "recherche web (Claude)"
                return "", "recherche web : aucun site sûr"
            print(f"[V3 RECHERCHE SITE] {r.status_code} {r.text[:200]}")
            return "", f"recherche web en erreur ({r.status_code})"
        except Exception as e:
            print(f"[V3 RECHERCHE SITE] {e}")
            return "", "recherche web en erreur"
    return "", "recherche web indisponible (clé API absente)"


async def insee_par_nom(api: httpx.AsyncClient, nom: str, cp: str) -> dict:
    """SIREN par recherche du nom dans SIRENE (API recherche-entreprises, gratuite)."""
    try:
        bons = []
        for code in ([cp, ""] if cp else [""]):     # d'abord avec le code postal, puis sans (société qui a déménagé)
            p = {"q": nom, "per_page": 5}
            if code:
                p["code_postal"] = code
            r = await api.get(INSEE_API, params=p)
            res = r.json().get("results", []) if r.status_code == 200 else []
            bons = [x for x in res if ressemblance(nom, x.get("nom_raison_sociale") or x.get("nom_complet") or "") >= 0.85]
            if bons:
                break
        bons.sort(key=lambda x: (x.get("etat_administratif") != "A",))
        if bons:
            return {"siren": bons[0].get("siren", ""), "nb": len(bons),
                    "raison": bons[0].get("nom_raison_sociale") or bons[0].get("nom_complet") or ""}
    except Exception:
        pass
    return {}


# ───────────────────────── décision ─────────────────────────
def statut(row: dict) -> tuple:
    st, act, conf = _statut(row)
    if row.get("controle_nom", "").startswith("⚠️ le site appartient"):
        row["linkedin_site"] = ""
        return "", "⚠️ le site ne semble pas être celui de cette société : site à corriger dans le CRM", "basse"
    if row.get("controle_nom", "").startswith("⚠️") and conf == "haute":
        conf = "moyenne"
        act += " · site non vérifiable"
    return st, act, conf


def _statut(row: dict) -> tuple:
    sp, ss = row["siren_crm"], row["siren_site"]
    if row.get("mode", "").startswith("adresse"):
        e = row.get("etat_crm", "")
        if e == "active":
            return "Active", "📍 SIREN déjà connu (actif) : recherche de l'adresse uniquement", "haute"
        if e == "fermée":
            return "Radiée, à vérifier", "⚠️ SIREN du CRM fermé : la société a peut-être changé de SIREN", "moyenne"
        return "", "📍 SIREN déjà connu, introuvable à l'INSEE : à vérifier", "moyenne"
    if not ss and not sp and row.get("siren_nom"):
        e = row.get("etat_nom", "")
        return ("Active" if e == "active" else "Radiée, à vérifier" if e == "fermée" else ""), \
               f"🔎 SIREN trouvé par le nom dans SIRENE ({row.get('nb_homonymes', 1)} correspondance(s)) : à vérifier", "moyenne"
    e_crm, e_site = row.get("etat_crm", ""), row.get("etat_site", "")
    if ss and e_site == "active":
        if sp and sp != ss:
            if e_crm == "active":
                return "Active", "🔀 deux SIREN actifs (CRM et site) : entités du même groupe ? à choisir", "moyenne"
            return "Active", "🔄 SIREN du CRM (fermé) à remplacer par celui du site", "haute"
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
                # étape 0 : déjà dans le CRM ? (fichier avec SIREN, ou recherche Pipedrive par le nom)
                o = dict(o)
                o["dans_crm"] = "fichier" if o["siren_crm"] else ""
                if not o["siren_crm"]:
                    crm = await chercher_crm(api, o["nom"], o.get("cp", ""))
                    if crm:
                        o["dans_crm"] = "Pipedrive" + (f" ({crm['doublons']} fiches, doublons : {crm['autres_fiches']})" if crm["doublons"] > 1 else "")
                        o["id"] = o["id"] or crm["id"]
                        o["siren_crm"] = crm["siren"]
                        o["site_crm"] = o["site_crm"] or crm["site"]
                        o["adresse_crm"] = o["adresse_crm"] or crm["adresse"]
                adresse_seule = bool(o["siren_crm"])      # SIREN déjà connu : on ne cherche que l'adresse physique
                doms = domaines(o["site_crm"])
                dom, src = (doms[0] if doms else ""), "CRM"
                sirene = {}
                if not dom and not o["siren_crm"]:
                    async with api_sem:
                        sirene = await insee_par_nom(api, o["nom"], o.get("cp", ""))
                if not dom and not adresse_seule:
                    dom, src = await chercher_domaine(api, o["nom"], o.get("cp", ""), o.get("ville", ""),
                                                      sirene.get("raison", ""), sirene.get("siren", "") or o["siren_crm"])
                s = await scanner_site(web, dom)       # en mode adresse seule : sert seulement au lien LinkedIn
                if adresse_seule:
                    s = {**s, "siren_site": "", "candidats": [], "page_siren": ""}
                for autre in doms[1:]:                      # plusieurs sites dans le CRM : on essaie les suivants
                    if s["site_joignable"] == "oui":
                        break
                    s2 = await scanner_site(web, autre)
                    if s2["site_joignable"] == "oui":
                        dom, s = autre, s2
                row = {**o, "domaine": dom, "source_site": src, **s,
                       "controle_nom": "", "siren_nom": "", "etat_nom": "", "nb_homonymes": "",
                       "mode": "adresse seule (SIREN déjà au CRM)" if adresse_seule else "complet"}
                async with api_sem:
                    i_site = await insee(api, s["siren_site"]) if s["siren_site"] else {}
                    if len(s["candidats"]) > 1:      # plusieurs SIREN : on garde celui dont le nom colle
                        meilleurs = [(ressemblance(o["nom"], i_site.get("raison_sociale", "")), s["siren_site"], i_site)]
                        for autre in s["candidats"][1:]:
                            i_a = await insee(api, autre)
                            meilleurs.append((ressemblance(o["nom"], i_a.get("raison_sociale", "")), autre, i_a))
                        sc, choisi, i_site = max(meilleurs, key=lambda t: (t[0], t[2].get("etat") == "active"))
                        row["siren_site"], row["page_siren"] = choisi, s["pages"].get(choisi, "")
                        s = {**s, "siren_site": choisi}
                    if not o["siren_crm"]:
                        i_crm = {}
                    elif o["siren_crm"] == s["siren_site"]:
                        i_crm = i_site
                    else:
                        i_crm = await insee(api, o["siren_crm"])
                    i_nom = {}
                    if not s["siren_site"] and not o["siren_crm"] and not sirene and not adresse_seule:
                        sirene = await insee_par_nom(api, o["nom"], o.get("cp", ""))
                    if sirene.get("siren") and not s["siren_site"]:
                        i_nom = await insee(api, sirene["siren"])
                        row.update({"siren_nom": sirene["siren"], "etat_nom": i_nom.get("etat", ""),
                                    "nb_homonymes": sirene.get("nb", 1)})
                row["etat_site"] = i_site.get("etat", "")
                row["etat_crm"] = i_crm.get("etat", "")
                ref = i_site or i_crm or i_nom
                row.update({"raison_sociale": ref.get("raison_sociale", ""), "date_fermeture": ref.get("date_fermeture", ""),
                            "siege_insee": ref.get("siege", ""), "tranche_insee": ref.get("tranche_insee", "")})
                # le site est-il bien celui de cette société ? (vaut aussi pour les sites du CRM, souvent faux)
                if dom and s["site_joignable"] == "oui" and not adresse_seule:
                    compact = norm_nom(o["nom"]).replace(" ", "")
                    etiquette = dom.split(".")[0].replace("-", "")
                    dom_ok = bool(compact) and (etiquette == compact or (len(etiquette) >= 6 and compact.startswith(etiquette)))
                    if s["siren_site"] and s["siren_site"] in (sirene.get("siren"), o["siren_crm"]):
                        row["controle_nom"] = "✅ SIREN du site = SIREN connu"
                    elif s["siren_site"] and i_site.get("raison_sociale"):
                        if ressemblance(o["nom"], i_site["raison_sociale"]) >= 0.6:
                            row["controle_nom"] = "✅ raison sociale cohérente"
                        elif dom_ok:
                            row["controle_nom"] = "✅ nom dans le domaine (marque ≠ raison sociale)"
                        else:
                            row["controle_nom"] = f"⚠️ le site appartient à « {i_site['raison_sociale']} »"
                    else:
                        row["controle_nom"] = "➖ pas de SIREN sur le site : site non vérifié"
                row["statut"], row["action"], row["confiance"] = statut(row)
                job["resultats"].append(row)
                job["fait"] += 1
        await asyncio.gather(*(un(o) for o in orgs))
    if job.get("linkedin") and not job["stop"]:
        try:
            await LI.etapes_linkedin(job, ressemblance, domaine)
        except Exception as e:
            job.setdefault("journal", []).append(f"étapes LinkedIn interrompues : {e}")
            print(f"[V3 LINKEDIN] {e}")
    job["phase"] = ""
    job["fin"] = time.time()
    job["etat"] = "terminé" if not job["stop"] else "arrêté"


# ───────────────────────── Excel ─────────────────────────
def decouper_adresse(a: str) -> tuple:
    """(rue, code postal, ville) pour les 3 formats rencontrés :
    CRM « 17 Rue Montmartre, 75001 Paris » · Google « 24 Rue de l'Est, 75020 Paris, France » · LinkedIn « 24, Rue de l'est, Paris, 75020, FR »"""
    m = re.search(r"\b(\d{5})\b", a or "")
    if not m:
        return a, "", ""
    avant = a[:m.start()].strip(" ,–-")
    apres = re.sub(r",?\s*(FR|France)\s*$", "", a[m.end():], flags=re.I).strip(" ,–-")
    if apres:
        return avant, m.group(1), apres
    morceaux = [x.strip() for x in avant.split(",") if x.strip()]
    if len(morceaux) >= 2 and not re.search(r"\d", morceaux[-1]):
        ville = morceaux.pop()
        rue = ", ".join(morceaux)
        rue = re.sub(r"^(\d+\s*(?:bis|ter)?)\s*,\s*", r"\1 ", rue)       # « 24, Rue de l'est » → « 24 Rue de l'est »
        return rue, m.group(1), ville
    return avant, m.group(1), ""


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
    perim = job.get("perimetre") or ("tout" if job.get("linkedin") else "siren")
    libelle = {"siren": "SIREN et statut (étapes 0 à 2)", "societes": "Sociétés : SIREN, adresse, effectif (étapes 0 à 3)",
               "dirigeants": "Sociétés + dirigeants, sans e-mails (étapes 0 à 4)", "tout": "Tout (étapes 0 à 5)"}[perim]
    lignes = [
        f"ENRICHISSEUR V3 · {libelle} · {auj}", "",
        f"{n} organisations traitées",
        f"Déjà dans le CRM avec un SIREN (adresse seule) : {k(lambda r: r.get('mode', '').startswith('adresse'))}",
        f"Site joignable : {k(lambda r: r['site_joignable'] == 'oui')}",
        f"Site retrouvé par recherche (absent du fichier) : {k(lambda r: r['source_site'] not in ('', 'CRM'))}",
        f"SIREN trouvé sur le site : {k(lambda r: r['siren_site'])}",
        f"SIREN trouvé seulement par le nom (à vérifier) : {k(lambda r: r['siren_nom'])}",
        f"Page LinkedIn trouvée sur le site : {k(lambda r: r['linkedin_site'])}",
        f"SIREN du CRM à remplacer : {k(lambda r: r['action'].startswith('🔄'))}",
        f"Radiées candidates à la suppression : {k(lambda r: r['statut'] == 'Radiée')}",
        f"Radiées à vérifier (site encore en ligne) : {k(lambda r: r['statut'] == 'Radiée, à vérifier')}",
    ] + ([
        f"Effectif LinkedIn trouvé : {k(lambda r: r.get('effectif_linkedin') not in (None, ''))}",
        f"Adresse confirmée par LinkedIn : {k(lambda r: r.get('confiance_adresse') == 'haute')}",
        f"Adresse à corriger (LinkedIn + Google concordent) : {k(lambda r: str(r.get('action_adresse', '')).startswith('🔄'))}",
        f"Adresse à valider (une seule source ou désaccord) : {k(lambda r: r.get('confiance_adresse') == 'moyenne')}",
        f"Sociétés quasi inactives (≤ 7 salariés LinkedIn) : {k(lambda r: str(r.get('statut_activite', '')).startswith('Quasi'))}",
    ] if job.get("linkedin") else []) + ([
        f"Dirigeants trouvés : {sum(len(r.get('dirigeants', [])) for r in res)}"
        + (f" · avec e-mail : {sum(1 for r in res for d in r.get('dirigeants', []) if d['email'])}" if perim == "tout" else ""),
    ] if perim in ("dirigeants", "tout") else []) + ["",
        "ONGLETS",
        "• Résultats : tout le détail, ligne par ligne",
        "• Import HubSpot : uniquement les sociétés actives de confiance haute (clé = nom de domaine)",
        "• Suppressions : radiées avec site mort, à valider avant de les retirer",
        "• À vérifier : SIREN fermé mais site en ligne (changement de SIREN probable)", "",
        "RÈGLES",
        "• Étape 0 : si la société a déjà un SIREN (dans le fichier ou dans Pipedrive), on ne recherche que son adresse physique.",
        "  Son SIREN est seulement contrôlé à l'INSEE (gratuit), pour repérer les sociétés fermées.",
        "• Le SIREN vient des mentions légales du site, contrôlé (clé de Luhn) puis vérifié à l'INSEE.",
        "• Société sans site connu : on la cherche d'abord dans SIRENE par son nom (et son code postal), puis on cherche son site sur le web.",
        "  Le site n'est retenu avec confiance haute que si le SIREN qu'il affiche est le même que celui de SIRENE (ou que le nom colle).",
        "• SIREN trouvé seulement par le nom dans SIRENE (site introuvable) : confiance moyenne, jamais dans l'onglet Import HubSpot.",
        "• L'adresse INSEE est celle du SIÈGE LÉGAL (souvent une domiciliation) : elle n'est PAS proposée comme adresse des bureaux.",
        "• « Suppressions » = SIREN fermé + site mort. Le contrôle LinkedIn (3e signal) viendra à l'étape suivante : ne rien supprimer avant.",
        "• LinkedIn : la page vient du lien du site officiel ; sinon du Company URL Finder, vérifiée par le site ou le nom.",
        "  Adresse : validée quand 2 sources concordent (CRM, LinkedIn, Google Maps). Google n'est interrogé qu'en cas de désaccord,",
        "  et une fiche Google n'est retenue que si son site web est celui de la société. Siège LinkedIn à l'étranger = ignoré.",
        "  Dirigeants : Sales Navigator filtré par l'identifiant LinkedIn de la société (les homonymes sont écartés).",
        "  Mode « adresse seule » : pas de recherche de dirigeants.",
        "  E-mails : un dirigeant déjà présent dans Pipedrive (même société) n'est pas enrichi ; sinon Kaspr, puis Fullenrich en secours.",
        "  L'onglet Import HubSpot contacts ne contient que les NOUVEAUX dirigeants (absents de Pipedrive).",
        "• Rien n'est écrit dans Pipedrive ni HubSpot.",
    ]
    if job.get("journal"):
        lignes += ["", "JOURNAL DES ÉTAPES LINKEDIN"] + job["journal"]
    for l in lignes:
        ws.append([l])
    ws["A1"].font = Font(name="Arial", bold=True, size=13, color="4A1E5C")
    ws.column_dimensions["A"].width = 110

    cols = [("ID Pipedrive", "id"), ("Société", "nom"), ("Déjà au CRM", "dans_crm"), ("Traitement", "mode"), ("Domaine", "domaine"), ("Site trouvé via", "source_site"),
            ("Contrôle du nom", "controle_nom"), ("Site joignable", "site_joignable"),
            ("SIREN CRM", "siren_crm"), ("État SIREN CRM", "etat_crm"), ("SIREN site", "siren_site"),
            ("État SIREN site", "etat_site"), ("Page où le SIREN a été trouvé", "page_siren"),
            ("SIREN par le nom (SIRENE)", "siren_nom"), ("Raison sociale INSEE", "raison_sociale"), ("Date fermeture", "date_fermeture"),
            ("Siège légal INSEE", "siege_insee"), ("LinkedIn (site)", "linkedin_site"),
            ("Statut proposé", "statut"), ("Action", "action"), ("Confiance", "confiance"), ("Note", "note_site")]
    if job.get("linkedin"):
        cols += [("Page LinkedIn retenue", "linkedin"), ("Trouvée via", "source_linkedin"), ("Contrôle LinkedIn", "controle_linkedin"),
                 ("Adresse CRM", "adresse_crm"), ("Adresse LinkedIn", "adresse_linkedin"), ("Fiche Google", "fiche_google"),
                 ("Adresse Google", "adresse_google"), ("Adresse proposée", "adresse_proposee"),
                 ("Action adresse", "action_adresse"), ("Confiance adresse", "confiance_adresse"),
                 ("Effectif LinkedIn", "effectif_linkedin"), ("Tranche déclarée", "tranche_linkedin"), ("Statut activité", "statut_activite")]
    feuille(wb, "Résultats", [c for c, _ in cols], [[r.get(k_, "") for _, k_ in cols] for r in res])

    hub = [r for r in res if r["statut"] == "Active" and r["confiance"] == "haute" and r["domaine"]]
    ent = ["Company Domain Name", "Name", "SIREN", "LinkedIn Company Page", "STATUT ACTIVITÉ", "Raison sociale", "ID Pipedrive", "Date relevé"]
    if job.get("linkedin"):
        ent += ["Street Address", "Postal Code", "City", "EFFECTIF LINKEDIN", "DATE EFFECTIF"]

    def ligne_hub(r):
        l = [r["domaine"], r["nom"], r["siren_site"] or r["siren_crm"] or r["siren_nom"], r.get("linkedin") or r["linkedin_site"],
             r.get("statut_activite") or "Active", r["raison_sociale"], r["id"], auj]
        if job.get("linkedin"):
            rue = cp = ville = ""
            if r.get("confiance_adresse") == "haute" and r.get("adresse_proposee"):
                rue, cp, ville = decouper_adresse(r["adresse_proposee"])
            eff = r.get("effectif_linkedin", "")
            l += [rue, cp, ville, eff, auj if eff != "" else ""]
        return l
    feuille(wb, "Import HubSpot", ent, [ligne_hub(r) for r in hub])
    if job.get("linkedin"):
        adr = [r for r in res if r.get("confiance_adresse") == "moyenne"]
        feuille(wb, "Adresses à valider", ["ID Pipedrive", "Société", "Adresse CRM", "Adresse LinkedIn", "Adresse Google", "Action",
                                           "Effectif LinkedIn", "Valider ? (oui/non)"],
                [[r["id"], r["nom"], r.get("adresse_crm", ""), r.get("adresse_linkedin", ""), r.get("adresse_google", ""),
                  r.get("action_adresse", ""), r.get("effectif_linkedin", ""), ""] for r in adr])
        corr = [r for r in res if str(r.get("action_adresse", "")).startswith("🔄")]
        feuille(wb, "Adresses à corriger", ["ID Pipedrive", "Société", "Ancienne adresse (CRM)", "Nouvelle adresse", "Sources"],
                [[r["id"], r["nom"], r.get("adresse_crm", ""), r.get("adresse_proposee", ""), "LinkedIn + Google"] for r in corr])
    if perim in ("dirigeants", "tout"):
        dir_ = [(r, d) for r in res for d in sorted(r.get("dirigeants", []), key=lambda d: d["role"])]
        ent_d = ["ID Pipedrive", "Société", "Domaine", "Prénom", "Nom", "Fonction", "Rôle", "Lieu", "Île-de-France", "LinkedIn"]
        if perim == "tout":
            ent_d += ["Déjà dans Pipedrive", "E-mail", "Source e-mail"]

        def ligne_d(r, d):
            l = [r["id"], r["nom"], r["domaine"], d["prenom"], d["nom"], d["fonction"], d["role"], d["lieu"],
                 "oui" if d["idf"] else "non", d["linkedin"]]
            return l + ([d.get("dans_pipedrive", ""), d["email"], d.get("source_email", "")] if perim == "tout" else [])
        feuille(wb, "Dirigeants", ent_d, [ligne_d(r, d) for r, d in dir_])
    if perim == "tout":
        contacts = [(r, d) for r, d in dir_ if d["email"] and r["domaine"] and not d.get("dans_pipedrive")]
        feuille(wb, "Import HubSpot contacts", ["First Name", "Last Name", "Email", "Job Title", "LinkedIn URL",
                                               "Company Domain Name", "Rôle", "Date relevé"],
                [[d["prenom"], d["nom"], d["email"], d["fonction"], d["linkedin"], r["domaine"], d["role"], auj] for r, d in contacts])
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
async def run(fichier: UploadFile | None = File(None), liste: str = Form(""), limite: int = Form(50),
              linkedin: str = Form(""), perimetre: str = Form("")):
    # perimetre : siren | societes | dirigeants | tout (l'ancienne case « linkedin » vaut « tout »)
    if perimetre not in ("siren", "societes", "dirigeants", "tout"):
        perimetre = "tout" if linkedin in ("on", "1", "true") else "siren"
    if fichier is not None and fichier.filename:
        orgs = lire_fichier(fichier.filename, await fichier.read())
    else:
        orgs = lire_liste(liste)
    if not orgs:
        return JSONResponse({"erreur": "Aucune société lue : déposez un fichier avec une colonne de noms, ou collez une liste."}, 400)
    if limite and limite > 0:
        orgs = orgs[:limite]
    if perimetre != "siren" and not LI.pb_configure():
        return JSONResponse({"erreur": "PhantomBuster n'est pas configuré dans Render : seul « SIREN et statut » est possible."}, 400)
    avec_li = perimetre != "siren"
    if avec_li and len(orgs) > LI.MAX_LINKEDIN:
        return JSONResponse({"erreur": f"Avec les étapes LinkedIn, {LI.MAX_LINKEDIN} sociétés maximum par lancement "
                                       f"(quota LinkedIn). Mettez {LI.MAX_LINKEDIN} dans « Lignes à traiter »."}, 400)
    jid = uuid.uuid4().hex[:8]
    JOBS[jid] = {"id": jid, "linkedin": avec_li, "perimetre": perimetre, "phase": "Sites web et SIREN…", "journal": [],"total": len(orgs), "fait": 0, "resultats": [], "etat": "en cours", "stop": False,
                 "debut": time.time(), "fin": None}
    asyncio.create_task(traiter(jid, orgs))
    return {"job": jid, "total": len(orgs)}


@router.get("/v3/status/{jid}")
async def status(jid: str):
    j = JOBS.get(jid)
    if not j:
        return JSONResponse({"erreur": "job inconnu (le service a peut-être redémarré)"}, 404)
    res = j["resultats"]
    return {"etat": j["etat"], "total": j["total"], "fait": j["fait"], "phase": j.get("phase", ""),
            "journal": j.get("journal", [])[-12:], "mode_linkedin": j.get("linkedin", False), "perimetre": j.get("perimetre", ""),
            "effectifs": sum(1 for r in res if r.get("effectif_linkedin") not in (None, "")),
            "dirigeants": sum(len(r.get("dirigeants", [])) for r in res),
            "emails": sum(1 for r in res for d in r.get("dirigeants", []) if d.get("email")),
            "siren": sum(1 for r in res if r["siren_site"] or r["siren_crm"] or r["siren_nom"]), "linkedin": sum(1 for r in res if r["linkedin_site"]),
            "radiees": sum(1 for r in res if r["statut"].startswith("Radiée")),
            "secondes": int((j["fin"] or time.time()) - j["debut"])}


@router.get("/v3/config")
async def config():
    return {"linkedin": LI.pb_configure(), "url_finder": bool(LI.PB_URL_FINDER), "salesnav": bool(LI.PB_SALESNAV),
            "kaspr": bool(LI.KASPR_KEY), "fullenrich": bool(LI.FULLENRICH_KEY), "gmaps": bool(LI.PB_GMAPS), "max": LI.MAX_LINKEDIN}


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
