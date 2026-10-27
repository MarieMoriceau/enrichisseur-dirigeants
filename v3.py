"""
Enrichisseur v3 — étapes 1 et 2
  1. Site web de la société : SIREN (mentions légales) + page LinkedIn officielle (pied de page)
  2. INSEE (API recherche-entreprises, gratuite) : société active ou fermée, raison sociale officielle

Entrée : export Pipedrive des Organisations (xlsx ou csv).
Sortie : Excel prêt pour HubSpot (rien n'est écrit dans un CRM).
"""
import asyncio, csv, io, json, os, re, time, unicodedata, uuid
from difflib import SequenceMatcher
from datetime import date, datetime, timedelta, timezone
from html import unescape
from urllib.parse import urljoin, urlparse

import httpx
from fastapi import APIRouter, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment

import v3_linkedin as LI
import v3_naf as NAF
import v3_suivi as SUIVI
import v3_memoire as MEM
import v3_planning as PLAN

router = APIRouter()
JOBS: dict = {}

import v3_validation as VALID
import v3_partis as PARTIS
router.include_router(VALID.router)              # écrans de validation (admin / négos)
import v3_rattrapage as RATTRAPAGE
router.include_router(RATTRAPAGE.router)         # rattrapage du contrôle des contacts CRM
import v3_sirene as SIRENE
router.include_router(SIRENE.router)            # SIRENE : date d'installation (bail) et opérateurs de flex
import v3_import as IMPORT
router.include_router(IMPORT.router)             # import d'un Excel de lancement dans la mémoire
import v3_export as EXPORT
router.include_router(EXPORT.router)             # export des validations au format d'import HubSpot

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


MOTS_CONTACT = re.compile(r"contact|nous[\s\-_]*(trouver|joindre|rejoindre)|acc[eè]s|o[uù][\s\-_]*sommes|plan[\s\-_]*d[\s\-_]*acc|agences?|bureaux|offices?|locations?", re.I)
RE_ADRESSE = re.compile(
    r"(?<![\d])(\d{1,4}(?:\s?(?:bis|ter|b|t)\b)?(?:\s?[-–à]\s?\d{1,4})?[\s,]+"
    r"(?:rue|avenue|av\.?|boulevard|bd|blvd|place|pl\.|quai|all[ée]e|impasse|passage|cours|square|villa|chemin|route|"
    r"cit[ée]|faubourg|fbg|rond[\s\-]point|esplanade|parvis|promenade|sentier|hameau|r[ée]sidence|mail|parc)\b"
    r"[^|•\n]{2,70}?)[\s,\-–]*(?:(?:cs|bp)\s?\d+\s*)?\b((?:75|77|78|91|92|93|94|95|[0-9][0-9])\d{3})\b\s*,?\s*"
    r"([A-Za-zÀ-ÿ'][^\n]{0,40})",
    re.I)
CONTEXTE_SIEGE = re.compile(r"si[èe]ge\s+social|domicili|immatricul|rcs|capital\s+(social|de)|h[ée]berg", re.I)


RE_DOMICIL = re.compile(r"domicili[ée]e?s?\s+(?:chez|par|au|à|aupr[èe]s)[^.;|]{0,60}|\bc/o\b[^.;|]{0,50}|soci[ée]t[ée] de domiciliation[^.;|]{0,50}", re.I)


def mention_domiciliation(t: str) -> str:
    """« Domiciliée chez… », « c/o… » : le site dit lui-même que l'adresse est une domiciliation."""
    m = RE_DOMICIL.search(t)
    return re.sub(r"\s+", " ", m.group(0)).strip()[:90] if m else ""


SECTIONS_NAF = {
    "A": "Agriculture, sylviculture et pêche", "B": "Industries extractives", "C": "Industrie manufacturière",
    "D": "Énergie (électricité, gaz)", "E": "Eau, déchets, dépollution", "F": "Construction",
    "G": "Commerce, réparation d'automobiles", "H": "Transports et entreposage", "I": "Hébergement et restauration",
    "J": "Information et communication", "K": "Finance et assurance", "L": "Activités immobilières",
    "M": "Activités spécialisées, scientifiques et techniques", "N": "Services administratifs et de soutien",
    "O": "Administration publique", "P": "Enseignement", "Q": "Santé humaine et action sociale",
    "R": "Arts, spectacles et activités récréatives", "S": "Autres activités de services",
    "T": "Activités des ménages employeurs", "U": "Activités extra-territoriales"}


def _ville(suite: str) -> str:
    """La ville qui suit le code postal : un mot (avec ses tirets), « Paris La Défense », « Le Kremlin-Bicêtre », Cedex."""
    mots = re.findall(r"[A-Za-zÀ-ÿ'][A-Za-zÀ-ÿ'\-]*|\d{1,2}", suite)
    if not mots:
        return ""
    v = [mots[0]]
    i = 1
    if mots[0].lower() in ("le", "la", "les", "l'") and len(mots) > 1:
        v.append(mots[1]); i = 2
    elif mots[0].lower() == "paris" and len(mots) > 2 and mots[1].lower() == "la" and mots[2].lower().startswith("d"):
        v += mots[1:3]; i = 3
    if i < len(mots) and mots[i].lower() == "cedex":
        v.append("Cedex")
        if i + 1 < len(mots) and mots[i + 1].isdigit():
            v.append(mots[i + 1])
    return " ".join(v)


def adresses_dans(t: str) -> list:
    """Adresses postales françaises trouvées dans un texte : (adresse, 'siège' si citée comme siège social / mention légale)."""
    out = []
    for m in RE_ADRESSE.finditer(t):
        rue = re.sub(r"\s+", " ", m.group(1)).strip(" ,-–")
        ville = _ville(m.group(3))
        cp = m.group(2)
        if len(rue) > 80 or re.search(r"https?:|www\.|@", rue) or not ville:
            continue
        if cp.startswith("75") and not (cp == "75116" or (cp.startswith("750") and 1 <= int(cp[3:]) <= 20)):
            continue                                    # code postal parisien impossible (75000, 75045…)
        avant = t[max(0, m.start() - 120):m.start()]
        if _hebergeur(t, m.start()):
            continue
        out.append((f"{rue}, {m.group(2)} {ville}".strip(), "siège" if CONTEXTE_SIEGE.search(avant) else ""))
    return out


def liens_contact(html: str, base: str) -> list:
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', html):
        href, lib = m.group(1), texte(m.group(2))
        if href.startswith(("mailto:", "tel:")):
            continue
        if MOTS_CONTACT.search(urlparse(href).path) or (len(lib) < 40 and MOTS_CONTACT.search(lib)):
            u = urljoin(base, href)
            if urlparse(u).netloc.replace("www.", "") == urlparse(base).netloc.replace("www.", ""):
                out.append(u.split("?")[0])
    return list(dict.fromkeys(out))[:2]


def adresses_site(adresses: list, siege_insee: str = "") -> tuple:
    """Adresses distinctes du site, de la plus probable à la moins probable pour les bureaux, et leur page.
    Ordre : page contact, puis page d'accueil, puis mentions légales. En Île-de-France d'abord.
    Une adresse donnée comme siège social dans les mentions légales, et identique au siège INSEE, est écartée :
    c'est souvent une domiciliation."""
    rang = {"contact": 0, "accueil": 1, "légal": 2}
    cands = []
    for a, page, typ, ctx in adresses:
        if typ == "légal" and ctx == "siège" and siege_insee and LI.meme_adresse(a, siege_insee):
            continue
        cle = LI.cle_adresse(a)
        if not cle:
            continue
        idf = cle[2][:2] in LI.IDF
        cands.append(((not idf, rang.get(typ, 3), ctx == "siège"), a, page, typ))
    cands.sort(key=lambda c: c[0])
    liste, pages, vues = [], {}, []
    for _, a, page, _t in cands:
        if any(LI.meme_adresse(a, v) for v in vues):
            continue
        vues.append(a)
        liste.append(a)
        pages[a] = page
    return liste, pages


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
                elif "base quali" in norm_nom(n) and "base_quali" not in _CHAMPS_PD:
                    _CHAMPS_PD["base_quali"] = f["key"]          # liste déroulante : on garde les libellés
                    _CHAMPS_PD["base_quali_options"] = {str(o.get("id")): o.get("label", "") for o in (f.get("options") or [])}
        except Exception as e:
            print(f"[V3 PIPEDRIVE CHAMPS] {e}")
        _CHAMPS_PD.setdefault("ok", True)
    return _CHAMPS_PD


def base_quali_de(d: dict, champs: dict) -> str:
    """Valeur du champ « Base quali » d'une organisation Pipedrive, en clair (une liste déroulante renvoie des numéros)."""
    v = d.get(champs.get("base_quali", ""), "") if champs.get("base_quali") else ""
    if v in (None, ""):
        return ""
    opts = champs.get("base_quali_options") or {}
    if opts:
        return ", ".join(opts.get(x.strip(), x.strip()) for x in str(v).split(",") if x.strip())
    return str(v).strip()


async def base_quali_pipedrive(api: httpx.AsyncClient, org_id: str) -> str:
    """« Base quali » lue directement sur la fiche Pipedrive (quand on connaît son ID)."""
    org_id = re.sub(r"\D", "", str(org_id or ""))
    if not PIPEDRIVE_KEY or not org_id:
        return ""
    try:
        champs = await _champs_pipedrive(api)
        if not champs.get("base_quali"):
            return ""
        d = (await api.get(f"https://api.pipedrive.com/v1/organizations/{org_id}",
                           params={"api_token": PIPEDRIVE_KEY})).json().get("data") or {}
        return base_quali_de(d, champs)
    except Exception as e:
        print(f"[V3 PIPEDRIVE BASE QUALI] {org_id}: {e}")
        return ""


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
                "base_quali": base_quali_de(d, champs), "doublons": len(cands)}
    except Exception as e:
        print(f"[V3 PIPEDRIVE] {nom}: {e}")
        return {}


# ───────────────────────── étape 1 : site web ─────────────────────────
async def scanner_site(client: httpx.AsyncClient, dom: str) -> dict:
    r = {"site_joignable": "non", "siren_site": "", "page_siren": "", "linkedin_site": "", "note_site": "", "candidats": [],
         "adresses_site": [], "mention_domiciliation": ""}
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
    adresses = [(a, base, "accueil", ctx) for a, ctx in adresses_dans(texte(html))]
    domicil = mention_domiciliation(texte(html))
    # page contact : c'est là que les sociétés mettent l'adresse de leurs bureaux
    for p in liens_contact(html, base):
        try:
            resp = await client.get(p)
            if resp.status_code < 400:
                t = texte(resp.text)
                adresses += [(a, str(resp.url), "contact", ctx) for a, ctx in adresses_dans(t)]
                domicil = domicil or mention_domiciliation(t)
                sirens += [(s, str(resp.url)) for s in sirens_dans(t)]
                li += linkedins_dans(resp.text)
        except Exception:
            continue

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
            t = texte(resp.text)
            sirens += [(s, str(resp.url)) for s in sirens_dans(t)]
            adresses += [(a, str(resp.url), "légal", ctx) for a, ctx in adresses_dans(t)]
            domicil = domicil or mention_domiciliation(t)
            li += linkedins_dans(resp.text)
        except Exception:
            continue
    r["adresses_site"] = adresses
    r["mention_domiciliation"] = domicil

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
# L'API recherche-entreprises limite à ~7 requêtes/s PAR SERVEUR. Deux runs en parallèle (22/10 : compte de Marie +
# compte des négos) dépassaient la limite : des SIREN valides finissaient « introuvables à l'INSEE ». Toutes les
# requêtes passent donc par un seul guichet, espacées de INSEE_INTERVALLE, avec des reprises plus patientes.
INSEE_INTERVALLE = float(os.getenv("INSEE_INTERVALLE", "0.25"))
_INSEE_VERROU = asyncio.Lock()
_INSEE_DERNIER = [0.0]


async def _insee_get(client: httpx.AsyncClient, params: dict):
    """GET sur l'API INSEE, cadencé pour tout le serveur. None si toujours en échec après 6 essais (~1 min)."""
    for essai in range(6):
        async with _INSEE_VERROU:
            attente = _INSEE_DERNIER[0] + INSEE_INTERVALLE - time.monotonic()
            if attente > 0:
                await asyncio.sleep(attente)
            _INSEE_DERNIER[0] = time.monotonic()
        try:
            r = await client.get(INSEE_API, params=params)
        except Exception:
            await asyncio.sleep(1 + essai * 2)
            continue
        if r.status_code == 429 or r.status_code >= 500:
            await asyncio.sleep(2 + essai * 3)
            continue
        return r
    return None


async def insee(client: httpx.AsyncClient, siren: str) -> dict:
    vide = {"raison_sociale": "", "etat": "", "date_fermeture": "", "siege": "", "tranche_insee": "", "naf": "", "activite": "", "secteur": ""}
    if not siren:
        return vide
    for essai in range(2):
        resp = await _insee_get(client, {"q": siren, "per_page": 1})
        if resp is None:
            break
        try:
            res = resp.json().get("results", [])
            res = [x for x in res if x.get("siren") == siren]
            if not res:
                return {**vide, "etat": "introuvable"}
            x = res[0]
            sg = x.get("siege") or {}
            etat = x.get("etat_administratif") or sg.get("etat_administratif") or ""
            naf = x.get("activite_principale") or sg.get("activite_principale") or ""
            return {
                "raison_sociale": x.get("nom_raison_sociale") or x.get("nom_complet") or "",
                "etat": {"A": "active", "C": "fermée"}.get(etat, etat),
                "date_fermeture": x.get("date_fermeture") or "",
                "siege": sg.get("adresse") or "",
                "tranche_insee": x.get("tranche_effectif_salarie") or "",
                "naf": naf,
                "activite": NAF.libelle(naf),       # libellé exact du code (69.10Z → Activités juridiques)
                "secteur": SECTIONS_NAF.get(x.get("section_activite_principale") or "", ""),
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
            r = await _insee_get(api, p)
            res = r.json().get("results", []) if r is not None and r.status_code == 200 else []
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
    if row.get("mode", "").startswith("SIREN déjà"):
        e = row.get("etat_crm", "")
        if e == "active":
            return "Active", "📍 SIREN déjà connu (actif) : recherche de l'adresse uniquement", "haute"
        if e == "fermée":
            return "Radiée, à vérifier", "⚠️ SIREN du CRM fermé : la société a peut-être changé de SIREN", "moyenne"
        if e == "erreur API":
            return "", "📍 SIREN déjà connu, INSEE injoignable pendant le traitement : « Relire la fiche INSEE »", "moyenne"
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

def origine_fichier(nom: str, data: bytes) -> tuple:
    """(origine, libellé) détectés d'après la forme du fichier : listes géolocalisation et prospection hebdo = « outils »."""
    onglets, tetes = [], []
    if nom.lower().endswith((".xlsx", ".xlsm")):
        try:
            wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
            onglets = wb.sheetnames
            tetes = [str(h or "") for h in next(wb.worksheets[0].iter_rows(values_only=True), ())]
        except Exception:
            pass
    return PLAN.detecter_origine(nom, onglets, tetes)


def lire_fichier(nom: str, data: bytes) -> list:
    if nom.lower().endswith((".xlsx", ".xlsm")):
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        sans = [n for n in wb.sheetnames if n.strip().lower() == "sociétés sans contact"]
        ws = wb[sans[0]] if sans else wb.worksheets[0]     # export géolocalisation : la liste est dans cet onglet
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
    c_notion = col("adresse notion") or col("adresse des locaux") or col("adresse locaux")
    c_quali = col("base quali")
    c_bail = col("date debut de bail") or col("date début de bail") or col("debut de bail", exclure=("annee", "année")) \
        or col("début de bail", exclure=("annee", "année"))
    c_flex = col("operateur flex") or col("opérateur flex") or col("flexoffice")
    c_adr = col("adresse", exclure=("verifi", "vérifi", "notion", "locaux")) or col("address")
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
                    "cp": _cp(l, c_cp, c_adr), "ville": (l.get(c_ville) or "").strip() if c_ville else "",
                    **({"adresse_notion": v} if c_notion and (v := (l.get(c_notion) or "").strip()) else {}),
                    **({"base_quali_crm": q} if c_quali and (q := str(l.get(c_quali) or "").strip()) else {}),
                    **({"debut_bail_crm": b} if c_bail and (b := date_bail(l.get(c_bail))) else {}),
                    **({"flex_crm": x} if c_flex and (x := str(l.get(c_flex) or "").strip()) else {})})
    return out


def date_bail(v) -> str:
    """Date de début de bail d'un export CRM : « 2010-07-27 », « 2010-07-27 00:00:00 » ou « 27/07/2010 » → 2010-07-27."""
    t = str(v or "").strip()
    if m := re.match(r"(\d{4})-(\d{2})-(\d{2})", t):
        y, mo, d = m.groups()
    elif m := re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})$", t):
        d, mo, y = m.groups()
    else:
        return ""
    try:
        return date(int(y), int(mo), int(d)).isoformat()
    except ValueError:
        return ""


# ───────────────────────── traitement ─────────────────────────
def nom_fichier(job: dict) -> str:
    base = re.sub(r"[^A-Za-z0-9_-]+", "_", unicodedata.normalize("NFKD", job.get("nom", "")).encode("ascii", "ignore").decode())[:40].strip("_")
    return f"enrichisseur_v3_{date.today().isoformat()}_{base or job['fait']}.xlsx"


def jlog(job: dict, msg: str):
    job.setdefault("journal", []).append(f"{datetime.now(SUIVI.PARIS):%H:%M:%S} {msg}")
    print(f"[V3 {job['id']}] {msg}")


async def _programmer_reste(job: dict, reste: list) -> list:
    """Programme `reste` aux prochains jours (heure de reprise), sur le compte de l'origine du job ; renvoie [(départ, sociétés)]."""
    origine = job.get("origine", PLAN.NEGOS)
    lots = await SUIVI.planifier_suite(reste, origine)
    base = re.sub(r"( \(suite \d+/\d+\))+$", "", job["nom"])
    for n, (depart, lot) in enumerate(lots, 1):
        await SUIVI.programmer(f"{base} (suite {n}/{len(lots)})", job["perimetre"], job.get("mails", []), lot, depart, origine)
    job["reliquat"] = job.get("reliquat", 0) + sum(len(l) for _, l in lots)
    job.setdefault("lots_reportes", []).extend({"depart": d.isoformat(timespec="minutes"), "societes": len(l)} for d, l in lots)
    job["non_programmees"] = job.get("non_programmees", 0) + len(reste) - sum(len(l) for _, l in lots)
    return lots


def _quand(lots: list) -> str:
    return ", ".join(f"{len(l)} {PLAN.libelle_jour(d)}" for d, l in lots)


async def repartir(job: dict, orgs: list) -> list:
    """Ce qui part maintenant sur LinkedIn ; le reste est programmé aux jours suivants (heure de reprise).
    Quota : 150 sociétés par jour et par compte LinkedIn (un seul compte branché = 150 partagés). Aucun créneau horaire."""
    origine = job.get("origine", PLAN.NEGOS)
    c = PLAN.compte(origine)
    reserve = job.get("reserve", 0)                    # un lancement reporté s'est déjà compté sur son jour de départ
    utilise = await SUIVI.utilise_compte(PLAN.maintenant(), c) - reserve
    dispo = max(0, SUIVI.QUOTA_JOUR - utilise)
    maintenant, reste = orgs[:dispo], orgs[dispo:]
    if not reste:
        return maintenant
    lots = await _programmer_reste(job, reste)
    places = sum(len(l) for _, l in lots)
    job["motif_report"] = "quota"
    nom_c = "" if c == PLAN.UNIQUE else f", compte « {PLAN.NOM_COMPTE[c]} »"
    job["report"] = (f"Quota LinkedIn du jour atteint ({utilise}/{SUIVI.QUOTA_JOUR}{nom_c}) : {len(maintenant)} sociétés traitées maintenant, "
                     f"puis {_quand(lots)}.")
    if places < len(reste):
        job["report"] += f" {len(reste) - places} sociétés n'ont pas pu être programmées (plus de 30 jours) : relancez-les plus tard."
    # version sans jargon pour l'écran et les mails des négos (ni quota, ni compte LinkedIn)
    job["report_simple"] = ((f"Votre liste est traitée en plusieurs fois : {len(maintenant)} sociétés maintenant, puis "
                             if maintenant else "Votre liste partira ") + _quand(lots) + ". Un mail vous prévient à chaque fois.")
    jlog(job, job["report"])
    await SUIVI.prevenir_report(job.get("mails", []), job["nom"], [(None, maintenant)] + lots if maintenant else lots, job["perimetre"],
                                motif="quota", origine=origine)
    return maintenant


def motif_arret(job: dict) -> str:
    if job.get("erreur"):
        return f"erreur : {job['erreur']}"
    if job["stop"]:
        return "arrêt manuel"
    if job.get("motif_report") == "quota":
        return "quota atteint"
    return "fin de la liste"


async def traiter(job_id: str, orgs: list):
    """Mémoire (sociétés déjà traitées), quota du jour, traitement, puis Excel joint dans Notion et envoyé par mail."""
    job = JOBS[job_id]
    asyncio.create_task(SUIVI.rester_eveille(JOBS))
    if job.get("notion_id"):                          # lancement reporté qui démarre
        await SUIVI.maj(job["notion_id"], {"Statut": "EN_COURS", "Run ID": job_id,
                                           "Démarré le": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    else:
        job["notion_id"] = await SUIVI.notion_creer(job)
    try:
        if not job.get("forcer") and job["perimetre"] != "siren":
            job["phase"] = "Mémoire : sociétés déjà traitées depuis moins de 6 mois ?"
            reprises, orgs = await MEM.reprendre(job, orgs, norm_nom, domaines, lambda m: jlog(job, m))
            job["resultats"] += reprises
            job["fait"] += len(reprises)
            job["nb_memoire"] = len(reprises)
            job["noms_memoire"] = [r.get("nom", "") for r in reprises]
        if job["linkedin"] and orgs:
            job["phase"] = "Quota LinkedIn du jour…"
            avant = len(orgs)
            verrou = VERROUS_QUOTA.setdefault(PLAN.compte(job.get("origine", "")), asyncio.Lock())
            async with verrou:                           # lecture du quota puis réservation, sans croisement entre deux runs
                orgs = await repartir(job, orgs)
                await SUIVI.maj(job["notion_id"], {"Nb LinkedIn": len(orgs)})
            job["total"] -= avant - len(orgs)
        job["reparti"] = True                            # mémoire et quota du jour faits : la répartition est connue
        await SUIVI.maj(job["notion_id"], {"Nb sociétés": job["total"], "Nb LinkedIn": len(orgs) if job["linkedin"] else 0,
                                           "Nb mémoire": job.get("nb_memoire", 0)})
        if orgs:
            job["phase"] = "Sites web et SIREN…"
            await _traiter(job_id, orgs)
        if job["linkedin"] and any(r.get("effectif_linkedin") not in (None, "") for r in job["resultats"]):
            job["phase"] = "Croissance des effectifs…"
            job["_idx_tout"] = await MEM.index(lambda m: jlog(job, m), tout=True)
            for r in job["resultats"]:
                if not r.get("memoire"):
                    r.update(MEM.evolution(job["_idx_tout"], r, norm_nom))
    except Exception as e:
        job["erreur"] = f"{type(e).__name__} : {e}"
        jlog(job, f"erreur : {job['erreur']}")
    job["phase"] = ""
    job["fin"] = job.get("fin") or time.time()
    if job["etat"] == "en cours":
        job["etat"] = "erreur" if job.get("erreur") else ("arrêté" if job["stop"] else "terminé")
    job["motif_texte"] = motif_arret(job)
    if job.get("phantom_en_panne"):
        try:
            n = await SUIVI.suspendre_attente(job.get("origine", PLAN.NEGOS), f"phantom en panne ({job['nom']})")
            jlog(job, f"{n} liste(s) programmée(s) sur ce compte suspendue(s) en attendant la réparation")
            await SUIVI.alerte_phantom(job, n)
        except Exception as e:
            jlog(job, f"alerte phantom impossible : {e}")
    jlog(job, f"fin du run : {job['motif_texte']}" + (f" · {job['reliquat']} sociétés reportées" if job.get("reliquat") else ""))
    try:
        xl = excel(job) if job["resultats"] else None
        xl_nego = excel_nego(job) if job["resultats"] and job["perimetre"] in ("dirigeants", "tout") else None
    except Exception as e:
        xl = xl_nego = None
        jlog(job, f"Excel impossible : {e}")
    fn = nom_fichier(job)
    job["envoi"] = "en cours"
    await SUIVI.notion_finir(job, xl, fn)
    try:                                              # mémoire d'abord : le lien « Valider » du mail doit déjà marcher
        await MEM.enregistrer(job, norm_nom, lambda m: jlog(job, m))
        await MEM.ajouter_lanceurs(job, lambda m: jlog(job, m))
    except Exception as e:
        print(f"[V3 MEMOIRE] {e}")
    if job["resultats"] or not job.get("report"):     # tout reporté : le mail « programmé » suffit
        await SUIVI.envoyer_resultat(job, xl, fn, xl_nego, fn.replace(".xlsx", "_contacts.xlsx"))
    job["envoi"] = "fait"


async def _traiter(job_id: str, orgs: list):
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
                        o["base_quali_crm"] = o.get("base_quali_crm") or crm.get("base_quali", "")
                elif o.get("id") and not o.get("base_quali_crm"):    # fiche connue par son ID : on lit sa « Base quali »
                    o["base_quali_crm"] = await base_quali_pipedrive(api, o["id"])
                adresse_seule = bool(o["siren_crm"])      # SIREN déjà connu : pas de recherche de SIREN (adresse, effectif et dirigeants oui)
                doms = domaines(o["site_crm"])
                dom, src = (doms[0] if doms else ""), "CRM"
                sirene = {}
                if not dom and not o["siren_crm"]:
                    async with api_sem:
                        sirene = await insee_par_nom(api, o["nom"], o.get("cp", ""))
                if not dom and not adresse_seule:
                    dom, src = await chercher_domaine(api, o["nom"], o.get("cp", ""), o.get("ville", ""),
                                                      sirene.get("raison", ""), sirene.get("siren", "") or o["siren_crm"])
                s = await scanner_site(web, dom)       # SIREN déjà connu : le site sert au lien LinkedIn et à l'adresse
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
                       "mode": "SIREN déjà au CRM" if adresse_seule else "complet"}
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
                            "siege_insee": ref.get("siege", ""), "tranche_insee": ref.get("tranche_insee", ""),
                            "naf": ref.get("naf", ""), "activite": ref.get("activite", ""), "secteur": ref.get("secteur", "")})
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
                # adresse des bureaux affichée sur le site (sauf si le site semble appartenir à une autre société)
                if not str(row.get("controle_nom", "")).startswith("⚠️"):
                    row["adresses_site_liste"], row["pages_adresse_site"] = adresses_site(s.get("adresses_site", []), row.get("siege_insee", ""))
                row.update(LI.decider_adresse(row))
                job["resultats"].append(row)
                job["fait"] += 1
        await asyncio.gather(*(un(o) for o in orgs))
    if job.get("linkedin") and not job["stop"]:
        try:
            if await entrer_file(job):                 # un seul run à la fois sur les phantoms d'un même compte
                await LI.etapes_linkedin(job, ressemblance, domaine)
                if not job["stop"]:
                    job["li_fini"] = True
        except LI.PhantomEnPanne as e:                  # dirigeants impossibles : on s'arrête et on prévient
            job["erreur"] = job["phantom_en_panne"] = str(e)
            jlog(job, f"⛔ {e}")
        except Exception as e:
            job.setdefault("journal", []).append(f"étapes LinkedIn interrompues : {e}")
            print(f"[V3 LINKEDIN] {e}")
        finally:
            sortir_file(job)
    job["fin"] = time.time()


# ───────────────────────── file d'attente par compte LinkedIn ─────────────────────────
# Un phantom PhantomBuster ne traite qu'une liste à la fois : deux runs sur le même compte se gêneraient.
# Les étapes « sites web / SIREN » tournent en parallèle ; seules les étapes LinkedIn passent une par une,
# dans l'ordre d'arrivée, compte par compte (les deux comptes restent indépendants).
FILES: dict = {}                                       # compte -> [id des runs, le premier travaille]
VERROUS_QUOTA: dict = {}                               # compte -> asyncio.Lock : deux runs ne lisent pas le quota en même temps


def _file(job: dict) -> list:
    return FILES.setdefault(PLAN.compte(job.get("origine", "")), [])


async def entrer_file(job: dict) -> bool:
    """Attend son tour sur le compte LinkedIn du run. False si le run est arrêté pendant l'attente."""
    f = _file(job)
    f.append(job["id"])
    nom = PLAN.NOM_COMPTE[PLAN.compte(job.get("origine", ""))]
    annonce = False
    while f[0] != job["id"]:
        if job["stop"]:
            return False
        pos = f.index(job["id"])
        devant = JOBS.get(f[0], {}).get("nom", "un autre run")
        job["phase"] = (f"En file d'attente : compte LinkedIn « {nom} » occupé par « {devant} »"
                        + (f" ({pos} run(s) avant vous)" if pos > 1 else "") + ". Démarrage automatique dès qu'il se libère.")
        if not annonce:
            jlog(job, job["phase"])
            annonce = True
        await asyncio.sleep(3)
    if annonce:
        jlog(job, f"compte LinkedIn « {nom} » libre : c'est parti")
    return not job["stop"]


def sortir_file(job: dict):
    f = _file(job)
    if job["id"] in f:
        f.remove(job["id"])


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
        f"Reprises de la mémoire (traitées il y a moins de 6 mois, sans nouvelle recherche) : {k(lambda r: r.get('memoire'))}",
    ] + ([job["report"]] if job.get("report") else []) + [
        f"Déjà dans le CRM avec un SIREN (pas de recherche de SIREN) : {k(lambda r: r.get('mode', '').startswith('SIREN déjà'))}",
        f"Site joignable : {k(lambda r: r['site_joignable'] == 'oui')}",
        f"Site retrouvé par recherche (absent du fichier) : {k(lambda r: r['source_site'] not in ('', 'CRM'))}",
        f"SIREN trouvé sur le site : {k(lambda r: r['siren_site'])}",
        f"SIREN trouvé seulement par le nom (à vérifier) : {k(lambda r: r['siren_nom'])}",
        f"Page LinkedIn trouvée sur le site : {k(lambda r: r['linkedin_site'])}",
        f"SIREN du CRM à remplacer : {k(lambda r: r['action'].startswith('🔄'))}",
        f"Radiées candidates à la suppression : {k(lambda r: r['statut'] == 'Radiée')}",
        f"Radiées à vérifier (site encore en ligne) : {k(lambda r: r['statut'] == 'Radiée, à vérifier')}",
        f"Adresse trouvée sur le site : {k(lambda r: r.get('adresse_site'))}",
        f"Adresse sûre (2 sources concordantes) : {k(lambda r: r.get('confiance_adresse') == 'haute')}",
        f"Adresse à corriger ou à ajouter au CRM : {k(lambda r: str(r.get('action_adresse', '')).startswith(('🔄', '➕')))}",
        f"Adresse à valider (une seule source ou désaccord) : {k(lambda r: r.get('confiance_adresse') == 'moyenne')}",
        f"Domiciliation probable : {k(lambda r: str(r.get('verdict_domiciliation', '')).startswith('🎯'))}"
        f" · siège ≠ bureaux : {k(lambda r: str(r.get('verdict_domiciliation', '')).startswith('📍'))}",
    ] + ([
        f"Effectif LinkedIn trouvé : {k(lambda r: r.get('effectif_linkedin') not in (None, ''))}",
        f"Sociétés quasi inactives (≤ 7 salariés LinkedIn) : {k(lambda r: str(r.get('statut_activite', '')).startswith('Quasi'))}",
    ] if job.get("linkedin") else []) + ([
        f"Dirigeants trouvés : {sum(len(r.get('dirigeants', [])) for r in res)}"
        + (f" · avec e-mail : {sum(1 for r in res for d in r.get('dirigeants', []) if d['email'])}" if perim == "tout" else ""),
    ] if perim in ("dirigeants", "tout") else []) + ["",
        "ONGLETS",
        "• Résultats : tout le détail, ligne par ligne",
        "• Import HubSpot : uniquement les sociétés actives de confiance haute (clé = nom de domaine)",
        "• Adresses à valider : une seule source, ou des sources en désaccord : à regarder à la main",
        "• Adresses à corriger : nouvelle adresse donnée par 2 sources, à reporter dans le CRM",
        "• Domiciliations : sociétés domiciliées ou dont le siège n'est pas là où elles travaillent (cibles coworking, bureaux opérés)",
        "• Suppressions : radiées avec site mort, à valider avant de les retirer",
        "• À vérifier : SIREN fermé mais site en ligne (changement de SIREN probable)", "",
        "RÈGLES",
        "• Étape 0 : si la société a déjà un SIREN (dans le fichier ou dans Pipedrive), on ne recherche pas son SIREN ; adresse, effectif et dirigeants oui.",
        "  Son SIREN est seulement contrôlé à l'INSEE (gratuit), pour repérer les sociétés fermées.",
        "• Le SIREN vient des mentions légales du site, contrôlé (clé de Luhn) puis vérifié à l'INSEE.",
        "• Société sans site connu : on la cherche d'abord dans SIRENE par son nom (et son code postal), puis on cherche son site sur le web.",
        "  Le site n'est retenu avec confiance haute que si le SIREN qu'il affiche est le même que celui de SIRENE (ou que le nom colle).",
        "• SIREN trouvé seulement par le nom dans SIRENE (site introuvable) : confiance moyenne, jamais dans l'onglet Import HubSpot.",
        "• L'adresse INSEE est celle du SIÈGE LÉGAL (souvent une domiciliation) : elle n'est PAS proposée comme adresse des bureaux.",
        "• « Suppressions » = SIREN fermé + site mort. Le contrôle LinkedIn (3e signal) viendra à l'étape suivante : ne rien supprimer avant.",
        "• LinkedIn : la page vient du lien du site officiel ; sinon du Company URL Finder, vérifiée par le site ou le nom.",
        "• Adresse des bureaux : validée quand 2 sources concordent parmi le CRM, le site internet, LinkedIn et Google Maps.",
        "  Site : page contact d'abord, puis page d'accueil, puis mentions légales. Un siège social des mentions légales identique",
        "  au siège INSEE est écarté (souvent une domiciliation). Si le site affiche plusieurs bureaux, on garde celui qui concorde.",
        "  Google n'est interrogé qu'en cas de désaccord, et une fiche Google n'est retenue que si son site web est celui de la société.",
        "  Siège LinkedIn à l'étranger = ignoré.",
        "• Verdict domiciliation : 🎯 probable si le site dit « domiciliée chez », si le siège INSEE est « chez » un tiers",
        "  ou à une grande adresse de domiciliation (60 rue François 1er, 66 Champs-Élysées…). 📍 siège ≠ bureaux confirmés",
        "  (domiciliation ou ancien siège). 🏢 siège en région. ✅ cohérent : siège = bureaux. 🔍 à vérifier : bureaux non confirmés.",
        "  Dirigeants : Sales Navigator filtré par l'identifiant LinkedIn de la société (les homonymes sont écartés).",
        "  Seuls les dirigeants basés en France sont gardés (lieu LinkedIn), pour la liste comme pour les e-mails.",
        "  Niveaux recherchés : C-level, propriétaires et associés, VP, et directeurs jusqu'à 200 salariés LinkedIn. Rôles 1 à 5 enrichis en e-mail ;",
        "  6 (immobilier, services généraux) et 7 (autres C-level) seulement listés.",
        "  E-mails : un dirigeant déjà présent dans Pipedrive (même société) n'est pas enrichi ; sinon Kaspr, puis Fullenrich en secours.",
        "  L'onglet Import HubSpot contacts ne contient que les NOUVEAUX dirigeants (absents de Pipedrive).",
        "• Quota : 150 sociétés par jour passent par LinkedIn (tous négos confondus). Au-delà, la suite part le lendemain à 7 h.",
        "• Mémoire : une société traitée depuis moins de 6 mois est reprise sans nouvelle recherche (colonne « Reprise de la mémoire »).",
        "• Croissance des effectifs : comparée à la dernière mesure LinkedIn d'au moins 3 mois gardée par l'outil.",
        "  « première mesure » = société jamais mesurée avant : la croissance apparaîtra aux passages suivants.",
        "• Rien n'est écrit dans Pipedrive ni HubSpot.",
    ]
    if job.get("journal"):
        lignes += ["", "JOURNAL DES ÉTAPES LINKEDIN"] + job["journal"]
    for l in lignes:
        ws.append([l])
    ws["A1"].font = Font(name="Arial", bold=True, size=13, color="4A1E5C")
    ws.column_dimensions["A"].width = 110

    cols = [("ID Pipedrive", "id"), ("Société", "nom"), ("Reprise de la mémoire", "memoire"), ("Déjà au CRM", "dans_crm"), ("Traitement", "mode"), ("Domaine", "domaine"), ("Site trouvé via", "source_site"),
            ("Contrôle du nom", "controle_nom"), ("Site joignable", "site_joignable"),
            ("SIREN CRM", "siren_crm"), ("État SIREN CRM", "etat_crm"), ("SIREN site", "siren_site"),
            ("État SIREN site", "etat_site"), ("Page où le SIREN a été trouvé", "page_siren"),
            ("SIREN par le nom (SIRENE)", "siren_nom"), ("Raison sociale INSEE", "raison_sociale"), ("Date fermeture", "date_fermeture"),
            ("Siège légal INSEE", "siege_insee"), ("Code NAF", "naf"), ("Activité (NAF)", "activite"), ("Secteur", "secteur"), ("LinkedIn (site)", "linkedin_site"),
            ("Statut proposé", "statut"), ("Action", "action"), ("Confiance", "confiance"), ("Note", "note_site")]
    li_ = bool(job.get("linkedin"))
    if any(r.get("adresse_notion") for r in job.get("resultats", [])):
        cols += [("Adresse prospection physique (locaux)", "adresse_notion")]
    cols += [("Adresse CRM", "adresse_crm"), ("Adresse site", "adresse_site"), ("Page de l'adresse site", "page_adresse_site")]
    if li_:
        cols += [("Page LinkedIn retenue", "linkedin"), ("Trouvée via", "source_linkedin"), ("Contrôle LinkedIn", "controle_linkedin"),
                 ("Adresse LinkedIn", "adresse_linkedin"), ("Fiche Google", "fiche_google"), ("Adresse Google", "adresse_google")]
    cols += [("Adresse proposée", "adresse_proposee"), ("Sources concordantes", "sources_adresse"),
             ("Action adresse", "action_adresse"), ("Confiance adresse", "confiance_adresse"),
             ("Verdict domiciliation", "verdict_domiciliation"), ("Pourquoi", "raison_domiciliation")]
    if li_:
        cols += [("Effectif LinkedIn", "effectif_linkedin"), ("Évolution effectif LinkedIn", "evolution_effectif"),
                 ("Croissance effectif (%)", "croissance_effectif"), ("Tranche déclarée", "tranche_linkedin"), ("Statut activité", "statut_activite")]
    feuille(wb, "Résultats", [c for c, _ in cols], [[r.get(k_, "") for _, k_ in cols] for r in res])

    hub = [r for r in res if r["statut"] == "Active" and r["confiance"] == "haute" and r["domaine"]]
    ent = ["Company Domain Name", "Name", "SIREN", "LinkedIn Company Page", "STATUT ACTIVITÉ", "Raison sociale", "ID Pipedrive", "Date relevé",
           "CODE NAF", "ACTIVITÉ", "SECTEUR", "DOMICILIATION", "Street Address", "Postal Code", "City"]
    if li_:
        ent += ["EFFECTIF LINKEDIN", "DATE EFFECTIF", "CROISSANCE EFFECTIF (%)"]

    def ligne_hub(r):
        l = [r["domaine"], r["nom"], r["siren_site"] or r["siren_crm"] or r["siren_nom"], r.get("linkedin") or r["linkedin_site"],
             r.get("statut_activite") or "Active", r["raison_sociale"], r["id"], auj,
             r.get("naf", ""), r.get("activite", ""), r.get("secteur", ""), re.sub(r"^\W+", "", r.get("verdict_domiciliation", "")).strip()]
        rue = cp = ville = ""
        if r.get("confiance_adresse") == "haute" and r.get("adresse_proposee"):
            rue, cp, ville = decouper_adresse(r["adresse_proposee"])
        l += [rue, cp, ville]
        if li_:
            eff = r.get("effectif_linkedin", "")
            l += [eff, auj if eff != "" else "", r.get("croissance_effectif", "")]
        return l
    feuille(wb, "Import HubSpot", ent, [ligne_hub(r) for r in hub])
    adr = [r for r in res if r.get("confiance_adresse") == "moyenne"]
    ent_a = ["ID Pipedrive", "Société", "Adresse CRM", "Adresse site", "Page de l'adresse site"]
    if li_:
        ent_a += ["Adresse LinkedIn", "Adresse Google"]
    ent_a += ["Adresse proposée", "Action"] + (["Effectif LinkedIn"] if li_ else []) + ["Valider ? (oui/non)"]
    feuille(wb, "Adresses à valider", ent_a,
            [[r["id"], r["nom"], r.get("adresse_crm", ""), r.get("adresse_site", ""), r.get("page_adresse_site", "")]
             + ([r.get("adresse_linkedin", ""), r.get("adresse_google", "")] if li_ else [])
             + [r.get("adresse_proposee", ""), r.get("action_adresse", "")]
             + ([r.get("effectif_linkedin", "")] if li_ else []) + [""] for r in adr])
    dom_ = [r for r in res if str(r.get("verdict_domiciliation", "")).startswith(("🎯", "📍"))]
    feuille(wb, "Domiciliations", ["ID Pipedrive", "Société", "Verdict", "Pourquoi", "Siège légal INSEE", "Adresse des bureaux",
                                   "Confiance adresse", "Code NAF", "Activité (NAF)", "Secteur"] + (["Effectif LinkedIn"] if li_ else []),
            [[r["id"], r["nom"], r.get("verdict_domiciliation", ""), r.get("raison_domiciliation", ""), r.get("siege_insee", ""),
              r.get("adresse_proposee", ""), r.get("confiance_adresse", ""), r.get("naf", ""), r.get("activite", ""), r.get("secteur", "")]
             + ([r.get("effectif_linkedin", "")] if li_ else []) for r in dom_])
    corr = [r for r in res if str(r.get("action_adresse", "")).startswith(("🔄", "➕"))]
    feuille(wb, "Adresses à corriger", ["ID Pipedrive", "Société", "Ancienne adresse (CRM)", "Nouvelle adresse", "Sources", "Action"],
            [[r["id"], r["nom"], r.get("adresse_crm", ""), r.get("adresse_proposee", ""), r.get("sources_adresse", ""),
              r.get("action_adresse", "")] for r in corr])
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
        crm = [(r, p) for r in res for p in r.get("contacts_crm", [])]
        if crm:
            ordre = {"probablement parti": 0, "à vérifier": 1, "toujours là": 2}
            crm.sort(key=lambda x: (ordre.get(x[1]["statut"], 3), x[0]["nom"]))
            feuille(wb, "Contacts CRM", ["ID Pipedrive", "Société", "ID personne", "Nom", "Fonction CRM", "E-mail CRM",
                                         "LinkedIn", "Statut", "Détail"],
                    [[r["id"], r["nom"], p["id"], p["nom_complet"], p["fonction"], p["email"], p["linkedin"], p["statut"], p["detail"]]
                     for r, p in crm])
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


def excel_nego(job: dict) -> bytes:
    """Version courte pour les négos : seulement les dirigeants (en France) et leurs e-mails."""
    res = job["resultats"]
    tout = job["perimetre"] == "tout"
    wb = Workbook()
    ws = wb.active
    ws.title = "Contacts"
    ent = (["Société", "Effectif LinkedIn", "Évolution effectif", "Prénom", "Nom", "Fonction", "Rôle", "Lieu", "LinkedIn"]
           + (["E-mail", "Déjà dans Pipedrive"] if tout else []))
    lignes = []
    for r in sorted(res, key=lambda r: (r.get("nom") or "").lower()):
        for d in sorted(r.get("dirigeants", []), key=lambda d: d["role"]):
            l = [r.get("nom", ""), r.get("effectif_linkedin", ""), r.get("evolution_effectif", ""),
                 d["prenom"], d["nom"], d["fonction"], d["role"][2:], d["lieu"], d["linkedin"]]
            if tout:
                l += [d.get("email", ""), "oui" if d.get("dans_pipedrive") else ""]
            lignes.append(l)
    wb.remove(ws)
    feuille(wb, "Contacts", ent, lignes)
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue()


# ───────────────────────── routes ─────────────────────────
@router.post("/v3/run")
async def run(req: Request, fichier: UploadFile | None = File(None), liste: str = Form(""), limite: int = Form(50),
              linkedin: str = Form(""), perimetre: str = Form(""), mail: str = Form(""), forcer: str = Form(""),
              origine: str = Form("auto")):
    mails = SUIVI.mails_valides(mail)
    if mail.strip() and not mails:
        return JSONResponse({"erreur": "Adresse mail invalide."}, 400)
    # perimetre : siren | societes | dirigeants | tout (l'ancienne case « linkedin » vaut « tout »)
    if perimetre not in ("siren", "societes", "dirigeants", "tout"):
        perimetre = "tout" if linkedin in ("on", "1", "true") else "siren"
    detectee, outil = PLAN.NEGOS, ""
    if fichier is not None and fichier.filename:
        data = await fichier.read()
        orgs = lire_fichier(fichier.filename, data)
        detectee, outil = origine_fichier(fichier.filename, data)
    else:
        orgs = lire_liste(liste)
    if not VALID.gestion_auto(req):
        origine = PLAN.NEGOS                          # hors de l'espace de Marie : toujours le compte des négos, sans détection
    origine = detectee if origine not in (PLAN.OUTILS, PLAN.NEGOS) else origine     # « auto » (espace de Marie) : selon le fichier
    if not orgs:
        return JSONResponse({"erreur": "Aucune société lue : déposez un fichier avec une colonne de noms, ou collez une liste."}, 400)
    if limite and limite > 0:
        orgs = orgs[:limite]
    if perimetre != "siren" and not LI.pb_configure():
        return JSONResponse({"erreur": "PhantomBuster n'est pas configuré dans Render : seul « SIREN et statut » est possible."}, 400)
    if fichier is not None and fichier.filename:
        nom = re.sub(r"\.(xlsx|csv)$", "", fichier.filename, flags=re.I)
    else:
        nom = ", ".join(o["nom"] for o in orgs[:3]) + (f" (+{len(orgs) - 3})" if len(orgs) > 3 else "")
    jid = nouveau_job(nom, mails, perimetre, orgs, forcer=forcer in ("on", "1", "true"), origine=origine)
    jlog(JOBS[jid], f"origine : {PLAN.LIBELLE[origine]}" + (f" (fichier {outil} reconnu)" if outil and origine == detectee else
                                                             " (choisie à la main)" if origine != detectee else ""))
    return {"job": jid, "total": len(orgs), "origine": origine, "outil": outil, "compte": PLAN.compte_txt(origine)}


def nouveau_job(nom, mails, perimetre, orgs, forcer=False, notion_id="", reserve=0, origine=PLAN.NEGOS) -> str:
    jid = uuid.uuid4().hex[:8]
    JOBS[jid] = {"id": jid, "nom": nom, "mails": mails, "linkedin": perimetre != "siren", "perimetre": perimetre,
                 "forcer": forcer, "notion_id": notion_id, "reserve": reserve, "origine": PLAN.origine_valide(origine),
                 "phase": "Démarrage…", "journal": [], "total": len(orgs), "fait": 0, "resultats": [], "etat": "en cours",
                 "stop": False, "debut": time.time(), "fin": None}
    asyncio.create_task(traiter(jid, orgs))
    return jid


# ───────────────────────── démarrage automatique des listes reportées ─────────────────────────
# Les listes reportées (quota du jour atteint) repartent à l'heure de reprise (8 h par défaut), chacune sur son compte
# LinkedIn. C'est l'outil qui s'en charge : une boucle interne vérifie toutes les 2 minutes, et GitHub Actions appelle
# /cron vers 8 h pour réveiller Render s'il dort. Un seul enchaînement de listes par compte à la fois ; les deux
# comptes tournent en parallèle. Un appel qui arrive pendant qu'un compte travaille ne le relance pas.
_SESSIONS: dict = {}                                   # compte -> {"active", "debut", "source"}
VERIF_AUTO_S = int(os.getenv("VERIF_REPORTS_S", "120"))


def _session(c: str) -> dict:
    return _SESSIONS.setdefault(c, {"active": False, "debut": None, "source": ""})


def run_linkedin_en_cours(c: str = "") -> bool:
    """Un run LinkedIn tourne-t-il sur le compte c (sur n'importe quel compte si c est vide) ?"""
    if c:
        return _session(c)["active"] or any(j["etat"] == "en cours" and j.get("linkedin") and PLAN.compte(j.get("origine", "")) == c
                                             for j in JOBS.values())
    return any(run_linkedin_en_cours(x) for x in PLAN.comptes())


async def _demarrer_compte(c: str, lots_dus: list, source: str) -> dict:
    nom = PLAN.NOM_COMPTE[c]
    if run_linkedin_en_cours(c):                      # vérifié et posé sans « await » entre les deux : pas de double départ
        return {"compte": nom, "lance": False, "motif": "un run tourne déjà sur ce compte (l'appel n'est pas mis en attente)"}
    lots = [l for l in lots_dus if PLAN.compte(l[6]) == c]
    if not lots:
        return {"compte": nom, "lance": False, "motif": "rien à lancer : aucune liste reportée à échéance"}
    _session(c).update(active=True, debut=datetime.now(SUIVI.PARIS).isoformat(timespec="minutes"), source=source)
    asyncio.create_task(_enchainer(c, lots, source))
    return {"compte": nom, "lance": True, "listes": len(lots), "societes": sum(len(l[4]) for l in lots),
            "motif": f"{len(lots)} liste(s) lancée(s) l'une après l'autre"}


async def demarrer_cron(source: str) -> dict:
    """Lance, compte par compte, les listes reportées arrivées à échéance. Un compte occupé n'est pas relancé."""
    try:
        dus = await SUIVI.a_lancer(jusqua=PLAN.maintenant())
    except Exception as e:
        return {"lance": False, "motif": f"erreur de lecture des listes reportées : {e}"}
    res = [await _demarrer_compte(c, dus, source) for c in PLAN.comptes()]
    return {"lance": any(r["lance"] for r in res), "comptes": res,
            "motif": " · ".join(f"{r['compte']} : {r['motif']}" for r in res)}


async def _enchainer(c: str, lots: list, source: str):
    try:
        auj = PLAN.maintenant().date().isoformat()
        for i, (pid, nom, perim, mails, orgs, depart, origine) in enumerate(lots, 1):
            # réservation sur le quota : seulement si la liste était prévue aujourd'hui (une liste en retard
            # a été réservée sur son jour d'origine, elle se compte normalement aujourd'hui)
            reserve = len(orgs) if (depart or "")[:10] == auj else 0
            jid = nouveau_job(nom, mails, perim, orgs, notion_id=pid, reserve=reserve, origine=origine)
            jlog(JOBS[jid], f"démarré par {source} (liste {i}/{len(lots)}, compte {PLAN.NOM_COMPTE[c]})")
            attente = 0
            while JOBS[jid]["etat"] == "en cours" or JOBS[jid].get("envoi") != "fait":
                await asyncio.sleep(5)
                attente = attente + 5 if JOBS[jid]["etat"] != "en cours" else 0
                if attente > 900:                     # filet : mail ou Notion bloqués depuis 15 min après la fin
                    break
    finally:
        _session(c)["active"] = False


async def boucle_reports():
    """Toutes les 2 minutes : pour chaque compte libre, démarre ses listes reportées à échéance."""
    await asyncio.sleep(30)
    while True:
        try:
            if any(not run_linkedin_en_cours(c) for c in PLAN.comptes()):
                r = await demarrer_cron("le démarrage automatique")
                if r.get("lance"):
                    print(f"[V3 AUTO] {r['motif']}")
        except Exception as e:
            print(f"[V3 AUTO] {e}")
        await asyncio.sleep(VERIF_AUTO_S)


@router.on_event("startup")
async def _demarrer_boucle():
    if SUIVI.NOTION_KEY:
        asyncio.create_task(boucle_reports())


@router.get("/cron")
async def cron(cle: str = ""):
    """Réveil vers 8 h (GitHub Actions) : démarre les listes reportées à échéance, compte par compte."""
    if not SUIVI.CRON_SECRET or cle != SUIVI.CRON_SECRET:
        return JSONResponse({"erreur": "clé absente ou invalide"}, 403)
    return await demarrer_cron("/cron (réveil GitHub)")


@router.get("/v3/reprendre")
async def reprendre(cle: str = ""):
    """Ancienne adresse du réveil : mêmes règles que /cron."""
    if not SUIVI.CRON_SECRET or cle != SUIVI.CRON_SECRET:
        return JSONResponse({"erreur": "clé absente ou invalide"}, 403)
    return await demarrer_cron("/v3/reprendre")


@router.get("/v3/fenetre")
async def fenetre(cle: str = "", simuler: str = "", demarrer: str = ""):
    """Comptes LinkedIn et heure de reprise. Test : &simuler=07:58 (heure de Paris aujourd'hui) ou 2026-10-19T07:58,
    &simuler=off. &demarrer=1 lance aussitôt les listes reportées à échéance (sans attendre la boucle de 2 minutes)."""
    if not SUIVI.CRON_SECRET or cle != SUIVI.CRON_SECRET:
        return JSONResponse({"erreur": "clé absente ou invalide"}, 403)
    info = PLAN.simuler(simuler) if simuler else ""
    dem = {"demarrage": await demarrer_cron("/v3/fenetre (test)")} if demarrer else {}
    return PLAN.etat() | ({"simulation": info} if info else {}) | dem | {"run_en_cours": run_linkedin_en_cours()}


@router.get("/v3/journee")
async def journee():
    """Aujourd'hui en un coup d'œil : runs du jour, motif d'arrêt, reliquat, prochain départ, comptes LinkedIn."""
    try:
        j = await SUIVI.journee()
    except Exception as e:
        j = {"runs": [], "prochains": [], "erreur": str(e)}
    en_cours = [{"nom": x["nom"], "debut": datetime.fromtimestamp(x["debut"], SUIVI.PARIS).isoformat(timespec="minutes"),
                 "phase": x.get("phase", "")} for x in JOBS.values() if x["etat"] == "en cours"]
    return j | {"fenetre": PLAN.etat(), "en_cours": en_cours, "session": any(x["active"] for x in _SESSIONS.values())}


@router.get("/v3/quota")
async def quota():
    try:
        return await SUIVI.quota_du_jour()
    except Exception as e:
        return {"quota": SUIVI.QUOTA_JOUR, "utilise": None, "reste": None, "erreur": str(e)}


# ───────────────────────── rattrapage : « Lancé par » des sociétés enrichies avant le 16/10 ─────────────────────────
@router.get("/v3/validation/api/attribuer-anciens")
async def attribuer_anciens(req: Request, mail: str = "mmoriceau@equation-sie.com", avant: str = "2026-10-16",
                            confirmer: int = 0, limite: int = 50):
    """Admin seulement. Sans &confirmer=1 : essai à blanc (on compte, rien n'est écrit).
    Avec &confirmer=1 : met « mail » dans « Lancé par » de « limite » sociétés au plus (50 par défaut)."""
    if (x := VALID._refus(req, "admin")):
        return x
    m = SUIVI.mails_valides(mail)
    if len(m) != 1 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", avant):
        return JSONResponse({"erreur": "mail ou date (AAAA-MM-JJ) invalide"}, 400)
    return await MEM.attribuer_anciens(m[0], avant, bool(confirmer), min(max(limite, 0), 5000))


# ───────────────────────── relance des sociétés traitées sans dirigeants (phantom en panne) ─────────────────────────
CHAMPS_ORG = ("id", "nom", "site_crm", "siren_crm", "adresse_crm", "cp", "ville", "adresse_notion", "infos_source", "base_quali_crm",
              "debut_bail_crm", "flex_crm")


@router.get("/v3/validation/api/relancer-vides")
async def relancer_vides(req: Request, lancement: str = "", confirmer: int = 0, origine: str = PLAN.OUTILS):
    """Admin. Sociétés d'un lancement enregistrées avec 0 dirigeant (ex. phantom Sales Navigator en panne le 20/10).
    Sans &confirmer=1 : essai à blanc (liste, rien n'est lancé). Avec : relance ces sociétés, telles qu'elles avaient été
    envoyées (nom, SIREN, site, adresse, adresse prospection physique, négo), sur le compte « origine » (outils par défaut)."""
    if (x := VALID._refus(req, "admin")):
        return x
    lancement = lancement.strip()
    if len(lancement) < 3:
        return JSONResponse({"erreur": "précisez le nom du lancement (?lancement=Prospection hebdo 20/10)"}, 400)
    if origine not in (PLAN.OUTILS, PLAN.NEGOS):
        return JSONResponse({"erreur": "origine : outils ou negos"}, 400)
    idx = await MEM.index(lambda m: None, tout=True)
    ecartees = {s["page"] for s in await VALID._toutes() if s["validation"] == "Écartée"}
    try:                                               # déjà dans une liste programmée (ou suspendue) : on ne double pas
        prog = await SUIVI.orgs_programmees()
    except RuntimeError as e:
        return JSONResponse({"erreur": str(e)}, 502)
    noms_prog = {norm_nom(o.get("nom") or "") for o in prog} - {""}
    sirens_prog = {o.get("siren_crm") for o in prog if o.get("siren_crm")}
    vides = [e for e in idx if lancement in [x.strip() for x in (e.get("lancement") or "").split(";")]
             and e.get("nb_dir") == 0 and e.get("perim") in ("dirigeants", "tout") and e["page"] not in ecartees]
    cibles = [e for e in vides if e.get("nom") not in noms_prog and not (e.get("siren") and e["siren"] in sirens_prog)]
    rep = {"essai_a_blanc": not confirmer, "lancement": lancement, "a_relancer": len(cibles),
           "deja_programmees": len(vides) - len(cibles),
           "compte_linkedin": PLAN.NOM_COMPTE[PLAN.compte(origine)], "exemples": [e["nom"] for e in cibles[:30]]}
    if not confirmer or not cibles:
        return rep
    sem = asyncio.Semaphore(3)
    async def org(e):
        async with sem:
            d = await MEM.lire(e["page"])
        o = {k: str(d.get(k) or "") for k in ("id", "nom", "site_crm", "siren_crm", "adresse_crm", "cp", "ville")}   # comme un fichier
        if e.get("siren_corrige") and luhn_ok(e.get("siren") or ""):
            o["siren_crm"] = e["siren"]                 # SIREN corrigé à la main : c'est lui qu'on cherche
        return o | {k: d[k] for k in CHAMPS_ORG[7:] if d.get(k) not in (None, "", {})}
    orgs = [o for o in await asyncio.gather(*(org(e) for e in cibles)) if o.get("nom")]
    jid = nouveau_job(f"{lancement} (relance sans dirigeants)", [VALID.utilisateur(req)] if VALID.utilisateur(req) else [],
                      "tout", orgs, origine=origine)
    jlog(JOBS[jid], f"relance de {len(orgs)} sociétés du lancement « {lancement} » enregistrées sans dirigeant")
    return rep | {"lance": jid, "relancees": len(orgs), "suivi": f"/v3/status/{jid}"}


@router.post("/v3/validation/api/nom/{page_id}")
async def corriger_nom(page_id: str, req: Request):
    """Admin : corrige le nom d'une société mal saisi en prospection (« Godim » pour « Godin Associés »).
    Change le titre et le nom normalisé de la fiche ; une relance cherchera ensuite sous ce nom. Corps : {"nom": "…"}."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    nom = re.sub(r"\s+", " ", str(b.get("nom") or "")).strip()[:100]
    if len(nom) < 2:
        return JSONResponse({"erreur": "nom trop court"}, 400)
    maj = await VALID._maj(page_id, {"Société": {"title": [{"text": {"content": nom}}]}, "Nom normalisé": MEM._txt(norm_nom(nom))})
    if isinstance(maj, JSONResponse):
        return maj
    return {"ok": True, "nom": nom}


async def _orgs_a_relancer(pages: list) -> list:
    """Reconstruit chaque société comme dans un fichier, avec le nom de la fiche (corrigé à la main le cas échéant)
    et le SIREN corrigé à la main s'il y en a un."""
    sem = asyncio.Semaphore(3)
    async def une(p):
        async with sem:
            info, d = await asyncio.gather(VALID._une(p), MEM.lire(p))
        if not info:
            return {}
        o = {k: str(d.get(k) or "") for k in ("id", "nom", "site_crm", "siren_crm", "adresse_crm", "cp", "ville")}
        o["nom"] = info.get("nom") or o["nom"]
        if info.get("siren_corrige") and luhn_ok(info.get("siren") or ""):
            o["siren_crm"] = info["siren"]
        return o | {k: d[k] for k in CHAMPS_ORG[7:] if d.get(k) not in (None, "", {})}
    return [o for o in await asyncio.gather(*(une(p) for p in pages)) if o.get("nom")]


async def _relancer(req: Request, pages: list, confirmer: bool, origine: str):
    pages = [p.strip() for p in pages if VALID.RE_PAGE.fullmatch(p.strip())][:50]
    if not pages:
        return JSONResponse({"erreur": "aucune fiche valide"}, 400)
    if origine not in (PLAN.OUTILS, PLAN.NEGOS):
        return JSONResponse({"erreur": "origine : outils ou negos"}, 400)
    if any(j.get("etat") == "en cours" for j in JOBS.values()):
        return JSONResponse({"erreur": "un enrichissement tourne déjà : relancez quand il sera fini"}, 409)
    orgs = await _orgs_a_relancer(pages)
    rep = {"essai_a_blanc": not confirmer, "a_relancer": len(orgs), "compte_linkedin": PLAN.NOM_COMPTE[PLAN.compte(origine)],
           "societes": [{"nom": o["nom"], "siren": o.get("siren_crm", "")} for o in orgs]}
    if not confirmer or not orgs:
        return rep
    nom = f"Relance manuelle {datetime.now(SUIVI.PARIS):%d/%m %Hh%M}" + (f" : {orgs[0]['nom']}" if len(orgs) == 1 else "")
    jid = nouveau_job(nom, [VALID.utilisateur(req)] if VALID.utilisateur(req) else [], "tout", orgs, forcer=True, origine=origine)
    jlog(JOBS[jid], f"relance manuelle de {len(orgs)} société(s) depuis l'écran de validation")
    return rep | {"lance": jid, "suivi": f"/v3/status/{jid}"}


@router.post("/v3/validation/api/relancer/{page_id}")
async def relancer_une(page_id: str, req: Request):
    """Admin : bouton « Relancer cette société » (après correction du nom ou du SIREN). Compte LinkedIn « outils »."""
    if (x := VALID._refus(req, "admin")):
        return x
    return await _relancer(req, [page_id], True, PLAN.OUTILS)


@router.get("/v3/validation/api/relancer-societes")
async def relancer_plusieurs(req: Request, pages: str = "", confirmer: int = 0, origine: str = PLAN.OUTILS):
    """Admin : relance plusieurs fiches (?pages=id1,id2). Sans &confirmer=1 : essai à blanc."""
    if (x := VALID._refus(req, "admin")):
        return x
    return await _relancer(req, pages.split(","), bool(confirmer), origine)


# ───────────────────────── contacts « partis » : nouvelle société, poste, e-mail ─────────────────────────
@router.get("/v3/validation/api/partis")
async def partis_liste(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    try:
        socs = await VALID._toutes()
    except RuntimeError as e:
        return JSONResponse({"erreur": str(e)}, 502)
    return {"a_rechercher": len(PARTIS.a_rechercher(socs)), "liste": PARTIS.liste(socs),
            "etat": {k: PARTIS.ETAT[k] for k in ("actif", "debut", "phase", "total", "fait")} | {"journal": PARTIS.ETAT["journal"][-8:]}}


@router.post("/v3/validation/api/partis/lancer")
async def partis_lancer(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    return await PARTIS.lancer(entrer_file, sortir_file, JOBS)


@router.post("/v3/validation/api/partis/arreter")
async def partis_arreter(req: Request):
    if (x := VALID._refus(req, "admin")):
        return x
    PARTIS.ETAT["stop"] = True
    return {"ok": True}


@router.get("/v3/validation/api/base-quali/{page_id}")
async def base_quali_lire(page_id: str, req: Request):
    """Admin : « Base quali » de la société. crm = valeur lue maintenant dans Pipedrive (sinon celle du fichier),
    retenue = valeur enregistrée dans l'outil, options = choix de la liste déroulante Pipedrive (vide = texte libre)."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    info, data = await asyncio.gather(VALID._une(page_id), MEM.lire(page_id))
    if not info:
        return JSONResponse({"erreur": "société introuvable"}, 404)
    async with httpx.AsyncClient(timeout=15) as api:
        champs = await _champs_pipedrive(api) if PIPEDRIVE_KEY else {}
        en_direct = await base_quali_pipedrive(api, (data or {}).get("id", "")) if (data or {}).get("id") else ""
    return {"crm": en_direct or (data or {}).get("base_quali_crm", ""), "retenue": info.get("base_quali", ""),
            "options": [o for o in (champs.get("base_quali_options") or {}).values() if o],
            "champ_trouve": bool(champs.get("base_quali")), "id_pipedrive": (data or {}).get("id", "")}


@router.post("/v3/validation/api/base-quali/{page_id}")
async def base_quali_ecrire(page_id: str, req: Request):
    """Admin : enregistre la « Base quali » retenue (rien n'est écrit dans Pipedrive). Corps : {"valeur": "…"}."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    valeur = re.sub(r"\s+", " ", str(b.get("valeur") or "")).strip()[:500]
    if not await VALID.assurer_colonne("Base quali"):
        return JSONResponse({"erreur": "Notion refuse de créer la colonne « Base quali »"}, 502)
    maj = await VALID._maj(page_id, {"Base quali": MEM._txt(valeur)})
    if isinstance(maj, JSONResponse):
        return maj
    return {"ok": True, "retenue": valeur}


@router.post("/v3/validation/api/debut-bail/{page_id}")
async def debut_bail_ecrire(page_id: str, req: Request):
    """Admin : date de début du bail des bureaux, saisie à la main quand on la connaît.
    Corps : {"date": "2024-03-01"} ; date vide = on efface."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    d = str(b.get("date") or "").strip()
    if d:
        try:
            jour = date.fromisoformat(d)
        except ValueError:
            return JSONResponse({"erreur": "date invalide"}, 400)
        if not (1950 <= jour.year <= date.today().year + 5):
            return JSONResponse({"erreur": f"année {jour.year} improbable, faute de frappe ?"}, 400)
    if not await VALID.assurer_colonne("Début de bail", "date"):
        return JSONResponse({"erreur": "Notion refuse de créer la colonne « Début de bail »"}, 502)
    source = await VALID.assurer_colonne("Source début de bail")
    maj = await VALID._maj(page_id, {"Début de bail": {"date": {"start": d} if d else None},
                                     **({"Source début de bail": MEM._txt("saisie à la main" if d else "")} if source else {})})
    if isinstance(maj, JSONResponse):
        return maj
    return {"ok": True, "debut_bail": d}


@router.get("/v3/validation/api/flexoffice-fichier")
async def flexoffice_fichier(req: Request, lancement: str = "", confirmer: int = 0):
    """Admin : coche « Flexoffice » pour toutes les sociétés d'un fichier lancé (toutes ses suites).
    ?lancement=locataires_flex_paris : essai à blanc ; &confirmer=1 : écrit dans Notion."""
    if (x := VALID._refus(req, "admin")):
        return x
    base = re.sub(r"( \((suite \d+/\d+|relance[^)]*)\))+$", "", lancement.strip())
    if len(base) < 3:
        return JSONResponse({"erreur": "précisez le fichier (?lancement=…)"}, 400)
    def du_fichier(s):
        return any(re.sub(r"( \((suite \d+/\d+|relance[^)]*)\))+$", "", l.strip()) == base for l in (s.get("lancement") or "").split(";"))
    socs = [s for s in await VALID._toutes() if du_fichier(s)]
    a_cocher = [s for s in socs if not s.get("flexoffice")]
    rep = {"essai_a_blanc": not confirmer, "fichier": base, "societes": len(socs), "deja_cochees": len(socs) - len(a_cocher),
           "a_cocher": len(a_cocher), "exemples": [s["nom"] for s in a_cocher[:30]]}
    if not confirmer or not a_cocher:
        return rep
    if not await VALID.assurer_colonne("Flexoffice", "checkbox"):
        return JSONResponse({"erreur": "Notion refuse de créer la colonne « Flexoffice »"}, 502)
    sem, erreurs = asyncio.Semaphore(3), []
    async def une(s):
        async with sem:
            r = await VALID._maj(s["page"], {"Flexoffice": {"checkbox": True}})
        if isinstance(r, JSONResponse):
            erreurs.append(s["nom"])
    await asyncio.gather(*(une(s) for s in a_cocher))
    return rep | {"cochees": len(a_cocher) - len(erreurs), "erreurs": erreurs}


@router.post("/v3/validation/api/flexoffice/{page_id}")
async def flexoffice_ecrire(page_id: str, req: Request):
    """Admin : case « Flexoffice » (la société est en bureau flexible). Corps : {"coche": true}."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    corps = await req.json()
    coche = bool(corps.get("coche"))
    if not await VALID.assurer_colonne("Flexoffice", "checkbox"):
        return JSONResponse({"erreur": "Notion refuse de créer la colonne « Flexoffice »"}, 502)
    props = {"Flexoffice": {"checkbox": coche}}
    info = await VALID._une(page_id)
    op = (info or {}).get("operateur_flex", "")
    if coche and op.endswith("(probable)"):           # flex confirmé : l'opérateur probable devient sûr
        props["Opérateur flex"] = MEM._txt(op.replace("(probable)", "").strip())
    elif coche and not op.strip() and (o := str(corps.get("operateur") or "").strip()[:60]) \
            and await VALID.assurer_colonne("Opérateur flex"):   # opérateur lu dans SIRENE à l'affichage de la fiche
        props |= await SIRENE.props_operateur(o, str(corps.get("source") or "SIRENE")[:300])
    maj = await VALID._maj(page_id, props)
    if isinstance(maj, JSONResponse):
        return maj
    return {"ok": True, "flexoffice": coche, "operateur_flex": maj.get("operateur_flex", "") if isinstance(maj, dict) else ""}


AUCUN_OPERATEUR = "(aucun)"     # opérateur retiré à la main : SIRENE ne le repropose plus


@router.post("/v3/validation/api/operateur-flex/{page_id}")
async def operateur_flex_ecrire(page_id: str, req: Request):
    """Admin : {"operateur": "Morning"} enregistre l'opérateur saisi à la main (sûr, coche Flexoffice) ;
    {"retirer": true} retire l'opérateur (SIRENE ne le repropose plus) ; {"retirer": false} efface la décision pour
    relancer la lecture SIRENE."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    corps = await req.json()
    saisi = re.sub(r"\s+", " ", str(corps.get("operateur") or "")).replace("(probable)", "").strip()[:60]
    if not await VALID.assurer_colonne("Opérateur flex"):
        return JSONResponse({"erreur": "Notion refuse de créer la colonne « Opérateur flex »"}, 502)
    if saisi:
        valeur = next((o for o in SIRENE.OPERATEURS_FLEX if o.lower() == saisi.lower()), saisi)   # orthographe de la liste
        props = await SIRENE.props_operateur(valeur, "saisi à la main")
        if await VALID.assurer_colonne("Flexoffice", "checkbox"):
            props["Flexoffice"] = {"checkbox": True}
    else:
        valeur = AUCUN_OPERATEUR if corps.get("retirer") else ""
        props = await SIRENE.props_operateur(valeur, "retiré à la main" if valeur else "")
    maj = await VALID._maj(page_id, props)
    if isinstance(maj, JSONResponse):
        return maj
    return {"ok": True, "operateur_flex": valeur, "flexoffice": True if saisi else None,
            "source_flex": "saisi à la main" if saisi else ("retiré à la main" if valeur else "")}


@router.post("/v3/validation/api/siren/{page_id}")
async def corriger_siren(page_id: str, req: Request):
    """Admin : remplace un SIREN faux. Le nouveau est contrôlé (9 chiffres, clé) et sa fiche INSEE est relue.
    Corps : {"siren": "123456789"} ; {"siren": <le même>, "relire": true} relit seulement la fiche INSEE.
    L'ancien SIREN est gardé dans « SIREN corrigé » (JSON) pour l'historique."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    siren = re.sub(r"\D", "", str(b.get("siren") or ""))
    if not luhn_ok(siren):
        return JSONResponse({"erreur": "SIREN invalide : 9 chiffres, et la clé de contrôle ne correspond pas (faute de frappe ?)"}, 400)
    info = await VALID._une(page_id)
    if not info:
        return JSONResponse({"erreur": "société introuvable"}, 404)
    relire = siren == info["siren"] and bool(b.get("relire"))   # même SIREN : on relit seulement sa fiche INSEE
    if siren == info["siren"] and not relire:
        return JSONResponse({"erreur": "c'est déjà ce SIREN"}, 400)
    return await _ecrire_fiche_insee(page_id, info, siren, relire, VALID.utilisateur(req) or "admin")


async def _ecrire_fiche_insee(page_id: str, info: dict, siren: str, relire: bool, par: str):
    async with httpx.AsyncClient(timeout=20) as c:
        i = await insee(c, siren)
    if i["etat"] == "introuvable":
        return JSONResponse({"erreur": f"SIREN {siren} introuvable à l'INSEE"}, 400)
    if i["etat"] == "erreur API":
        return JSONResponse({"erreur": "l'INSEE ne répond pas, réessayez dans une minute"}, 502)
    if not await VALID.assurer_colonne("SIREN corrigé"):
        return JSONResponse({"erreur": "Notion refuse de créer la colonne « SIREN corrigé »"}, 502)
    avant = (VALID._json(info.get("siren_corrige"), {}).get("avant") or info["siren"])   # le tout premier SIREN
    quand = datetime.now(SUIVI.PARIS)
    garde = ""                                         # relecture : on garde une mise en garde utile (« SIREN trouvé par le nom… »)
    if relire:
        ancienne = str((await MEM.lire(page_id) or {}).get("action") or "")
        if ancienne and not re.search(r"introuvable|injoignable", ancienne):
            garde = " · " + ancienne
    fiche = {"raison_sociale": i["raison_sociale"], "siege_insee": i["siege"], "tranche_insee": i["tranche_insee"],
             "naf": i["naf"], "activite": i.get("activite") or NAF.libelle(i["naf"]), "secteur": i["secteur"], "date_fermeture": i["date_fermeture"],
             "statut": "Active" if i["etat"] == "active" else "Radiée, à vérifier",
             "action": f"🔄 Fiche INSEE relue le {quand:%d/%m}{garde}" if relire and avant == siren
                       else f"✏️ SIREN corrigé à la main le {quand:%d/%m} (l'outil avait trouvé {avant})"}
    corr = {"avant": avant, "le": quand.isoformat(timespec="seconds"), "par": par, "insee": fiche}
    maj = await VALID._maj(page_id, {"SIREN": MEM._txt(siren), "SIREN corrigé": MEM._txt(json.dumps(corr, ensure_ascii=False))})
    if isinstance(maj, JSONResponse):
        return maj
    return {"ok": True, "siren": siren, "avant": avant, **fiche}


# Rattrapage : fiches dont la lecture INSEE a échoué pendant le run (SIREN connu, pas de raison sociale)
RELIRE = {"actif": False, "phase": "", "total": 0, "fait": 0, "ok": 0, "echecs": [], "candidats": None}


async def _candidats_relire() -> list:
    socs = [s for s in await VALID._toutes() if s["validation"] in ("À valider", "Société validée") and luhn_ok(s.get("siren") or "")]
    sem = asyncio.Semaphore(4)
    async def voir(s):
        if VALID._json(s.get("siren_corrige"), {}).get("insee"):
            return None                                    # déjà corrigée ou relue à la main
        async with sem:
            d = await MEM.lire(s["page"])
        return s if d and not d.get("raison_sociale") else None
    return [s for s in await asyncio.gather(*(voir(s) for s in socs)) if s]


async def _relire_tout(cibles: list):
    RELIRE.update(actif=True, total=len(cibles), fait=0, ok=0, echecs=[], phase="relecture INSEE")
    try:
        for s in cibles:
            r = await _ecrire_fiche_insee(s["page"], s, s["siren"], True, "rattrapage INSEE")
            RELIRE["fait"] += 1
            if isinstance(r, JSONResponse):
                RELIRE["echecs"].append(f"{s['nom']} : {json.loads(r.body).get('erreur', '')}")
            else:
                RELIRE["ok"] += 1
    finally:
        RELIRE.update(actif=False, phase="terminé", candidats=None)


@router.get("/v3/validation/api/relire-insee")
async def relire_insee(req: Request, confirmer: int = 0):
    """Admin. Sans &confirmer=1 : liste les fiches à valider dont le SIREN est connu mais la fiche INSEE vide
    (API saturée pendant le run). Avec : relit leur fiche INSEE, une par une, en tâche de fond. Rappeler l'adresse
    pour suivre l'avancement."""
    if (x := VALID._refus(req, "admin")):
        return x
    if RELIRE["actif"]:
        return {k: RELIRE[k] for k in ("actif", "phase", "total", "fait", "ok", "echecs")}
    if any(j.get("etat") == "en cours" for j in JOBS.values()):
        return JSONResponse({"erreur": "un enrichissement tourne : relancez quand il sera fini"}, 409)
    cibles = await _candidats_relire()
    rep = {"essai_a_blanc": not confirmer, "a_relire": len(cibles), "exemples": [f"{s['nom']} ({s['siren']})" for s in cibles[:40]],
           "dernier_passage": {k: RELIRE[k] for k in ("phase", "total", "ok", "echecs")} if RELIRE["phase"] else None}
    if confirmer and cibles:
        asyncio.create_task(_relire_tout(cibles))
        rep["lance"] = True
    return rep


@router.post("/v3/validation/api/partis/{page_id}/{pid}")
async def partis_decider(page_id: str, pid: str, req: Request):
    """Valider / écarter un parti retrouvé, avec corrections éventuelles (e-mail, fonction, rôle)."""
    if (x := VALID._refus(req, "admin")):
        return x
    if not VALID.RE_PAGE.fullmatch(page_id) or not re.fullmatch(r"[\w-]{1,40}", pid):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    etat = b.get("etat")
    if etat not in PARTIS.ETATS:
        return JSONResponse({"erreur": "état inconnu"}, 400)
    champs, erreur = VALID.verifier_saisie({k: b.get(k) for k in ("email", "fonction", "role") if b.get(k)})
    if erreur:
        return JSONResponse({"erreur": erreur}, 400)
    try:
        suivis = await PARTIS.enregistrer(page_id, {pid: {**champs, "etat": etat,
                                                          "decide_le": datetime.now(timezone.utc).isoformat(timespec="seconds")}})
    except RuntimeError as e:
        return JSONResponse({"erreur": str(e)}, 502)
    return {"ok": True, **suivis.get(pid, {}), "id": pid}


# ───────────────────────── API : listes envoyées par les autres outils (prospection-hebdo…) ─────────────────────────
# Tout ce qui arrive ici part sur le compte LinkedIn « automatisations » (origine « outils »), jamais sur celui des négos.
API_CLE = os.getenv("ENRICHISSEUR_API_KEY", "")
API_MAX_SOCIETES = int(os.getenv("API_MAX_SOCIETES", "2000"))


def _api_refus(req: Request):
    import hmac
    if not API_CLE:
        return JSONResponse({"ok": False, "erreur": "API non configurée : ENRICHISSEUR_API_KEY absente dans Render"}, 503)
    if not hmac.compare_digest(req.headers.get("X-Enrichisseur-Cle", ""), API_CLE):
        return JSONResponse({"ok": False, "erreur": "clé absente ou invalide (en-tête X-Enrichisseur-Cle)"}, 401)
    return None


def _org_api(x: dict) -> dict:
    """Une société reçue en JSON → le format interne (comme une ligne d'export Pipedrive)."""
    t = lambda k: str(x.get(k) or "").strip()
    siren = re.sub(r"\D", "", t("siren"))[:9]
    infos = {k: t(k) for k in ("statut_pipedrive", "nego", "campagne", "ref_notion") if t(k)}
    notion = t("adresse_notion") or t("adresse_locaux")
    return {"id": re.sub(r"\.0$", "", t("id_pipedrive")), "nom": t("nom"), "site_crm": t("site"),
            "siren_crm": siren if luhn_ok(siren) else "", "adresse_crm": t("adresse"),
            "cp": re.sub(r"\D", "", t("code_postal"))[:5] or _cp_de(notion), "ville": t("ville"),
            **({"adresse_notion": notion} if notion else {}), **({"infos_source": infos} if infos else {}),
            **({"base_quali_crm": t("base_quali")} if t("base_quali") else {})}


def _fusion_doublons_api(orgs: list) -> tuple:
    """Une même société relevée deux fois dans un envoi (deux négos) : une seule recherche, les prénoms sont cumulés.
    Même société = même SIREN, sinon même nom normalisé + même code postal."""
    import v3_equipe as EQ
    garde, index, n = [], {}, 0
    for o in orgs:
        k = ("s", o["siren_crm"]) if o.get("siren_crm") else ("n", norm_nom(o.get("nom", "")), o.get("cp", ""))
        if k not in index:
            index[k] = o
            garde.append(o)
            continue
        n += 1
        g = index[k]
        neg = EQ.fusion((g.get("infos_source") or {}).get("nego", ""), [(o.get("infos_source") or {}).get("nego", "")])
        if neg:
            g.setdefault("infos_source", {})["nego"] = neg
        for c in ("site_crm", "adresse_crm", "adresse_notion", "id", "ville"):
            if not g.get(c) and o.get(c):
                g[c] = o[c]
    return garde, n


def _cp_de(a: str) -> str:
    m = re.search(r"\b(\d{5})\b", a or "")
    return m.group(1) if m else ""


@router.post("/v3/api/lancer")
async def api_lancer(req: Request):
    """Lance un enrichissement sur le compte « automatisations ». Corps JSON : voir README, section API."""
    if (x := _api_refus(req)):
        return x
    try:
        b = await req.json()
    except Exception:
        return JSONResponse({"ok": False, "erreur": "corps JSON illisible"}, 400)
    if not isinstance(b, dict) or not isinstance(b.get("societes"), list):
        return JSONResponse({"ok": False, "erreur": "champ « societes » (liste) manquant"}, 400)
    perimetre = b.get("perimetre") or "tout"
    if perimetre not in ("siren", "societes", "dirigeants", "tout"):
        return JSONResponse({"ok": False, "erreur": "perimetre : siren, societes, dirigeants ou tout"}, 400)
    orgs, ignorees = [], []
    for i, soc in enumerate(b["societes"]):
        o = _org_api(soc) if isinstance(soc, dict) else {}
        (orgs if o.get("nom") else ignorees).append(o if o.get("nom") else i)
    if not orgs:
        return JSONResponse({"ok": False, "erreur": "aucune société avec un nom"}, 400)
    orgs, fusionnees = _fusion_doublons_api(orgs)
    if len(orgs) > API_MAX_SOCIETES:
        return JSONResponse({"ok": False, "erreur": f"{len(orgs)} sociétés : {API_MAX_SOCIETES} au plus par envoi, découpez la liste"}, 413)
    if perimetre != "siren" and not LI.pb_configure():
        return JSONResponse({"ok": False, "erreur": "PhantomBuster n'est pas configuré dans Render"}, 503)
    mails = SUIVI.mails_valides(",".join(b.get("mails") or []) if isinstance(b.get("mails"), list) else str(b.get("mails") or ""))
    source = str(b.get("source") or "api")[:40]
    nom = str(b.get("nom") or f"{source} {datetime.now(SUIVI.PARIS):%d/%m %H:%M}")[:100]
    jid = nouveau_job(nom, mails, perimetre, orgs, forcer=bool(b.get("forcer")), origine=PLAN.OUTILS)
    JOBS[jid]["recues"] = len(orgs)
    jlog(JOBS[jid], f"reçu par l'API ({source}) : {len(orgs)} sociétés, compte LinkedIn « {PLAN.NOM_COMPTE[PLAN.compte(PLAN.OUTILS)]} »"
                    + (f", {len(ignorees)} lignes sans nom ignorées" if ignorees else "")
                    + (f", {fusionnees} doublons fusionnés (négos cumulés)" if fusionnees else ""))
    base = str(req.base_url).rstrip("/")
    return JSONResponse({"ok": True, "lancement": jid, "nom": nom, "recues": len(orgs), "ignorees_sans_nom": ignorees,
                         "perimetre": perimetre, "compte_linkedin": PLAN.NOM_COMPTE[PLAN.compte(PLAN.OUTILS)],
                         "suivi": f"{base}/v3/api/lancement/{jid}"}, 202)


@router.get("/v3/api/lancement/{jid}")
async def api_suivi(jid: str, req: Request):
    """État d'un lancement envoyé par l'API : mémoire, part du jour, parts reportées, résultat."""
    if (x := _api_refus(req)):
        return x
    j = JOBS.get(jid)
    if not j:
        return JSONResponse({"ok": False, "erreur": "lancement inconnu : l'enrichisseur a peut-être redémarré (voir l'historique)"}, 404)
    res = j["resultats"]
    connu = bool(j.get("reparti")) or j["etat"] != "en cours"
    return {"ok": True, "lancement": jid, "nom": j.get("nom", ""),
            "etat": {"en cours": "en_cours", "terminé": "termine", "arrêté": "arrete"}.get(j["etat"], "erreur"),
            "repartition_connue": connu, "etape": j.get("phase", ""), "recues": j.get("recues", j.get("total", 0)),
            "deja_enrichies": {"nombre": j.get("nb_memoire", 0), "societes": j.get("noms_memoire", [])},
            "aujourdhui": (j.get("total", 0) - j.get("nb_memoire", 0)) if connu else None,
            "reportees": j.get("lots_reportes", []), "non_programmees": j.get("non_programmees", 0),
            "traitees": j["fait"], "dirigeants": sum(len(r.get("dirigeants", [])) for r in res),
            "emails": sum(1 for r in res for d in r.get("dirigeants", []) if d.get("email")),
            "erreur": j.get("erreur", "")}


@router.get("/v3/status/{jid}")
async def status(jid: str):
    j = JOBS.get(jid)
    if not j:
        return JSONResponse({"erreur": "job inconnu (le service a peut-être redémarré)"}, 404)
    res = j["resultats"]
    return {"etat": j["etat"], "total": j["total"], "fait": j["fait"], "phase": j.get("phase", ""),
            "nom": j.get("nom", ""), "mails": j.get("mails", []), "envoi": j.get("envoi", ""),
            "memoire": j.get("nb_memoire", 0), "report": j.get("report", ""), "report_simple": j.get("report_simple", ""), "motif": j.get("motif_texte", ""),
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
            "kaspr": bool(LI.KASPR_KEY), "fullenrich": bool(LI.FULLENRICH_KEY), "gmaps": bool(LI.PB_GMAPS), "max": LI.MAX_LINKEDIN,
            "deux_comptes": PLAN.deux_comptes(), "compte_negos": {"scraper": bool(LI.PB_SCRAPER_NEGOS),
            "salesnav": bool(LI.PB_SALESNAV_NEGOS), "profils": bool(LI.PB_PROFILS_NEGOS),
            "url_finder": bool(LI.PB_URL_FINDER_NEGOS), "gmaps": bool(LI.PB_GMAPS_NEGOS)}}


@router.post("/v3/stop/{jid}")
async def stop(jid: str):
    if jid in JOBS:
        JOBS[jid]["stop"] = True
    return {"ok": True}


@router.get("/v3/excel/{jid}")
async def telecharger(jid: str, version: str = ""):
    j = JOBS.get(jid)
    if not j:
        return JSONResponse({"erreur": "job inconnu"}, 404)
    nom = nom_fichier(j)
    if version == "contacts" and j["perimetre"] in ("dirigeants", "tout"):
        return StreamingResponse(io.BytesIO(excel_nego(j)), headers={"Content-Disposition": f'attachment; filename="{nom.replace(".xlsx", "_contacts.xlsx")}"'},
                                 media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return StreamingResponse(io.BytesIO(excel(j)), headers={"Content-Disposition": f'attachment; filename="{nom}"'},
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@router.get("/v3/csv/{jeton}.csv")
async def csv_phantom(jeton: str):
    """Liste de sociétés lue par les phantoms PhantomBuster (adresse à usage unique, gardée 24 h)."""
    x = LI.CSV_HEBERGES.get(jeton)
    if not x:
        return JSONResponse({"erreur": "fichier inconnu ou expiré"}, 404)
    return StreamingResponse(io.BytesIO(x[0].encode("utf-8")), media_type="text/csv; charset=utf-8")


@router.get("/v3/historique")
async def historique():
    try:
        return {"notion": bool(SUIVI.NOTION_KEY), "mail": bool(SUIVI.SMTP_USER and SUIVI.SMTP_PASS),
                "lancements": await SUIVI.historique(JOBS)}
    except Exception as e:
        return {"notion": bool(SUIVI.NOTION_KEY), "lancements": [], "erreur": str(e)}


@router.get("/v3/historique/{page_id}/excel")
async def excel_historique(page_id: str):
    if not re.fullmatch(r"[0-9a-f\-]{32,36}", page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    url = await SUIVI.url_fichier(page_id)
    if not url:
        return JSONResponse({"erreur": "pas de fichier pour ce lancement"}, 404)
    return RedirectResponse(url)


_PAGE_CONNEXION_AUTO = """<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Compte automatisations</title>
<style>body{font:15px system-ui,sans-serif;background:#f4f2f7;color:#2b2140;display:grid;place-items:center;min-height:100vh;margin:0}
form{background:#fff;padding:28px;border-radius:10px;box-shadow:0 2px 12px #0001;width:min(340px,90vw)}
h1{font-size:19px;margin:0 0 6px}p{color:#6b6478;font-size:13.5px;margin:0 0 16px}
input{width:100%;box-sizing:border-box;padding:10px;border:1px solid #d9d4e2;border-radius:6px;font-size:15px}
button{margin-top:12px;width:100%;padding:10px;border:0;border-radius:6px;background:#3a2a5c;color:#fff;font-size:15px;cursor:pointer}
.err{color:#a01c22}</style></head><body><form method="post" action="/v3/automatisations">
<h1>Compte automatisations</h1><p>Runs manuels sur les crédits LinkedIn restants du compte automatisations.</p>
{{MSG}}<input type="password" name="mdp" placeholder="Mot de passe" autofocus required>
<button type="submit">Entrer</button></form></body></html>"""


@router.get("/v3/automatisations", response_class=HTMLResponse)
async def page_automatisations(req: Request, erreur: str = ""):
    """Page privée de Marie : même formulaire, mais tout part sur le compte « automatisations » (quota 150/jour de ce compte)."""
    if not VALID.gestion_auto(req):
        msg = ("<p class='err'>Mot de passe incorrect.</p>" if erreur else
               "<p class='err'>Accès non configuré : ajoutez MDP_AUTOMATISATIONS dans Render.</p>" if not VALID.MDP_AUTO else "")
        return _PAGE_CONNEXION_AUTO.replace("{{MSG}}", msg)
    with open("templates/v3.html", encoding="utf-8") as f:
        html = f.read()
    a, b = html.index("<!--ORIGINE-->"), html.index("<!--/ORIGINE-->") + len("<!--/ORIGINE-->")
    html = (html[:a] + '<input type="hidden" id="origine" name="origine" value="outils">'
            '<small id="origineInfo" style="display:block;margin-top:14px;color:var(--muted)">Tout part sur le compte LinkedIn '
            '« automatisations ».</small>' + html[b:])
    try:
        q = await SUIVI.quota_du_jour()
        c = q.get("comptes", {}).get(PLAN.NOM_COMPTE[PLAN.compte(PLAN.OUTILS)], {})
        reste = f"{c.get('reste', '?')} sociétés restantes aujourd'hui sur {SUIVI.QUOTA_JOUR}"
    except Exception:
        reste = "crédits du jour indisponibles"
    avert = "" if PLAN.deux_comptes() else " (compte négos pas encore branché : quota partagé avec les négos)"
    bandeau = ('<p style="background:#3a2a5c;color:#fff;padding:10px 14px;border-radius:8px;margin:0 0 14px">'
               f'<b>Compte automatisations</b> · {reste}{avert}. Au-delà, la suite part le lendemain à {PLAN.etat()["reprise"]}. '
               '<a href="/v3/automatisations/sortir" style="color:#fff;float:right">Se déconnecter</a></p>')
    return html.replace("<!--BANDEAU-->", bandeau, 1)


@router.post("/v3/automatisations")
async def connexion_automatisations(mdp: str = Form("")):
    import hmac
    if not (VALID.MDP_AUTO and hmac.compare_digest(mdp, VALID.MDP_AUTO)):
        return RedirectResponse("/v3/automatisations?erreur=1", 303)
    rep = RedirectResponse("/v3/automatisations", 303)
    rep.set_cookie(VALID.COOKIE_AUTO, VALID._jeton("auto"), max_age=60 * 60 * 24 * 30, httponly=True, samesite="lax", secure=True)
    return rep


@router.get("/v3/automatisations/sortir")
async def sortir_automatisations():
    rep = RedirectResponse("/", 303)
    rep.delete_cookie(VALID.COOKIE_AUTO)
    return rep


@router.get("/", response_class=HTMLResponse)
async def accueil():
    """Page d'accueil : aiguillage vers le lancement, la validation des négos, l'admin et l'espace automatisations."""
    with open("templates/accueil.html", encoding="utf-8") as f:
        return f.read()


@router.get("/v3/lancer", response_class=HTMLResponse)
async def lancer():
    """Lancement d'un enrichissement par les négos : page simple, toujours sur le compte LinkedIn des négos."""
    with open("templates/negos.html", encoding="utf-8") as f:
        return f.read()
