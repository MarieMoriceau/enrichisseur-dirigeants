"""
Enrichisseur v3 — validation des résultats, en deux écrans

  /v3/validation  (mot de passe ADMIN : Marie et Michel Bastian)
      Écran Société : adresses CRM / site / LinkedIn / Google, recommandation, effectif LinkedIn, domiciliation.
                      On coche l'adresse retenue (ou on la saisit), puis on valide ou on écarte la société.
      Écran Contacts CRM : contacts déjà dans Pipedrive « probablement parti » / « à vérifier » / « toujours là ».
                      Seuls ces deux-là décident (garder / marquer parti / à supprimer / à revoir).

  /v3/contacts    (négos, connectés par lien e-mail ; l'admin y a aussi accès)
      Nouveaux dirigeants trouvés, dès la fin du lancement : profil LinkedIn visible, téléphone à compléter,
      « Valider pour HubSpot » ou « Écarter ». Les contacts déjà au CRM sont visibles en lecture seule.
      Un négo ne voit que ses sociétés : son prénom dans « Négos » (champ « nego » de prospection-hebdo)
      ou son e-mail dans « Lancé par » (ce qu'il a lancé lui-même). Voir v3_equipe.py.

  Connexion : lien envoyé par e-mail aux membres « Actif » de la table Notion « Équipe négos » (v3_equipe.py) ;
  « Admin » coché = écran admin. Le mot de passe admin reste en secours ; le mot de passe négos partagé n'ouvre plus rien.

Tout est gardé dans la mémoire Notion (« Enrichisseur v3 — Mémoire des sociétés »). Rien n'est envoyé dans un CRM
depuis ces écrans : l'envoi HubSpot viendra après la migration de fin octobre, avec un test à blanc sur 50 lignes.
"""
import asyncio, base64, hashlib, hmac, json, os, re, time
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

import v3_memoire as MEM
import v3_linkedin as LI
import v3_equipe as EQ

router = APIRouter()
SOURCES = ("Notion", "CRM", "Site", "LinkedIn", "Google", "Saisie")
ACTIONS_CRM = ("garder", "marquer parti", "à supprimer", "à revoir")
ACTIONS_AILLEURS = ("à décider", "changer de société", "deux sociétés")   # contact déjà fiché chez une autre société
ETATS_NOUVEAU = ("à traiter", "prêt pour HubSpot", "écarté")
ROLES = ["1 CEO / Président / DG", "2 Fondateur", "3 COO / Opérations", "4 CFO / DAF", "5 Associé / Gérant",
         "6 Immobilier / Services généraux", "7 Autre C-level / VP", "8 Autre"]
CHAMPS_MODIFIABLES = ("fonction", "role", "email", "telephone", "linkedin")   # saisis à la main par l'admin ou les négos
RE_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", re.I)


def fiche_nouveau(d: dict, e: dict) -> dict:
    """Nouveau dirigeant tel qu'affiché : valeurs trouvées par l'enrichisseur, remplacées par les saisies manuelles."""
    f = {"cle": cle_contact(d), "nom_complet": d.get("nom_complet", ""), "lieu": d.get("lieu", ""),
         "ailleurs": d.get("ailleurs_crm") or {}, "autres_societes": d.get("autres_societes") or [],
         "etat": e.get("etat", "à traiter"), "le": e.get("le", ""), "modifie": e.get("modifie", []),
         "source_email": d.get("source_email", "")}
    for k in CHAMPS_MODIFIABLES:
        f[k] = e[k] if k in e.get("modifie", []) else (e.get(k) or d.get(k, "")) if k == "telephone" else d.get(k, "")
    return f


def verifier_saisie(b: dict) -> tuple:
    """Champs saisis à la main → (valeurs propres, erreur)."""
    v = {k: re.sub(r"\s+", " ", str(b.get(k) or "")).strip()[:300] for k in CHAMPS_MODIFIABLES if k in b}
    if v.get("telephone"):
        t = _telephone(v["telephone"])
        if not t:
            return {}, "téléphone illisible (ex. 06 12 34 56 78 ou +33 6 12 34 56 78)"
        v["telephone"] = t
    if v.get("email") and not RE_EMAIL.fullmatch(v["email"]):
        return {}, "e-mail illisible"
    if v.get("linkedin"):
        if "linkedin.com/in/" not in v["linkedin"].lower():
            return {}, "URL LinkedIn de profil attendue (linkedin.com/in/…)"
        if not v["linkedin"].lower().startswith("http"):
            v["linkedin"] = "https://" + v["linkedin"]
    return v, ""
RE_PAGE = re.compile(r"[0-9a-f\-]{32,36}")

MDP_ADMIN = os.getenv("VALIDATION_MDP_ADMIN", "")
MDP_NEGOS = os.getenv("VALIDATION_MDP_NEGOS", "")
SECRET = (os.getenv("VALIDATION_SECRET") or hashlib.sha256(f"{MDP_ADMIN}|{MDP_NEGOS}|v3".encode()).hexdigest()).encode()
COOKIE = "v3_validation"
# Page privée de Marie : runs manuels sur le compte LinkedIn « automatisations » (ses crédits restants du jour)
MDP_AUTO = os.getenv("MDP_AUTOMATISATIONS", "")
COOKIE_AUTO = "v3_auto"


# ───────────────────────── accès ─────────────────────────
def _jeton(role: str, mail: str = "") -> str:
    v = f"{role}|{mail}"
    return v + "." + hmac.new(SECRET, v.encode(), hashlib.sha256).hexdigest()


# Rôles gardés dans le cookie : « admin » (mot de passe, ou lien e-mail d'un membre Admin) et « nego » (lien e-mail).
# Les anciens cookies « negos » du mot de passe partagé ne sont plus acceptés : chacun se reconnecte une fois par lien.
ROLES_COOKIE = {"admin": "admin", "nego": "negos"}


def _session(req: Request) -> tuple:
    """(rôle, e-mail) lus dans le cookie signé ; ('', '') si absent ou falsifié. L'e-mail sert à filtrer l'écran des négos."""
    v = req.cookies.get(COOKIE, "")
    corps = v.rsplit(".", 1)[0]
    r, _, mail = corps.partition("|")
    if r in ROLES_COOKIE and hmac.compare_digest(v, _jeton(r, mail)):
        return ROLES_COOKIE[r], mail
    return "", ""


def gestion_auto(req: Request) -> bool:
    """Connecté à la page privée du compte « automatisations » (cookie signé, mot de passe MDP_AUTOMATISATIONS) ?"""
    return bool(MDP_AUTO) and hmac.compare_digest(req.cookies.get(COOKIE_AUTO, ""), _jeton("auto"))


def role(req: Request) -> str:
    return _session(req)[0]


def utilisateur(req: Request) -> str:
    return _session(req)[1]


def mails_de(txt: str) -> set:
    return {m.strip().lower() for m in re.split(r"[;,\s]+", txt or "") if "@" in m}


async def moi(req: Request) -> dict:
    """La personne connectée : {"role", "mail", "membre" (sa ligne de la table Équipe négos, ou {})}.
    Un négo retiré de la table (ou décoché « Actif ») perd l'accès tout de suite, même avec un cookie encore valable."""
    r, mail = _session(req)
    liste = await EQ.membres() if (r == "negos" or mail) else []
    m = EQ.par_mail(liste, mail) if mail else {}
    if r == "negos" and not (m and m.get("nego")):
        r = ""
    return {"role": r, "mail": mail, "membre": m, "equipe": liste}


def visible_par(qui: dict, s: dict) -> bool:
    """L'admin voit tout ; un négo voit les sociétés à son prénom et celles qu'il a lancées lui-même."""
    return qui["role"] == "admin" or (qui["role"] == "negos" and EQ.visible_pour(qui["membre"], s, qui["equipe"]))


def avec_attribution(s: dict, liste: list) -> dict:
    """Ajoute à une société : à qui elle revient, les prénoms reçus inconnus, et « non attribué »."""
    a = EQ.attribution(liste, s.get("negos", ""), s.get("lance_par", ""))
    return {**s, "attribue_a": a["a"], "prenoms_inconnus": a["inconnus"], "non_attribue": a["non_attribue"]}


async def _refus_negos(req: Request):
    """Comme _refus(req, "negos"), mais vérifie aussi que le négo est toujours dans la table. Renvoie (refus, qui)."""
    qui = await moi(req)
    if qui["role"] in ("admin", "negos"):
        return None, qui
    if req.url.path.startswith("/v3/contacts/api"):
        return JSONResponse({"erreur": "accès refusé : reconnectez-vous"}, 401), qui
    return RedirectResponse(f"/v3/connexion?suite={quote(req.url.path)}" + ("&retire=1" if _session(req)[0] else ""), 303), qui


def _refus(req: Request, besoin: str):
    """Page : renvoi vers la connexion. API : 401/403."""
    r = role(req)
    ok = r == "admin" or (besoin == "negos" and r == "negos")
    if ok:
        return None
    if req.url.path.startswith(("/v3/validation/api", "/v3/contacts/api")):
        return JSONResponse({"erreur": "accès refusé : reconnectez-vous"}, 401 if not r else 403)
    return RedirectResponse(f"/v3/connexion?suite={quote(req.url.path)}" + ("&refus=1" if r else ""), 303)


SUITES = ("/v3/validation", "/v3/contacts")
LIEN_MINUTES = 15
_DERNIER_ENVOI = {}                      # e-mail → heure du dernier lien envoyé (un lien par minute au plus)


def _jeton_lien(mail: str, expire: int) -> str:
    corps = base64.urlsafe_b64encode(f"lien|{mail}|{expire}".encode()).decode().rstrip("=")
    return corps + "." + hmac.new(SECRET, corps.encode(), hashlib.sha256).hexdigest()[:32]


def _lire_lien(t: str) -> str:
    """E-mail contenu dans un lien de connexion, ou "" s'il est falsifié ou expiré."""
    corps, _, sig = (t or "").partition(".")
    if not corps or not hmac.compare_digest(sig, hmac.new(SECRET, corps.encode(), hashlib.sha256).hexdigest()[:32]):
        return ""
    try:
        quoi, mail, expire = base64.urlsafe_b64decode(corps + "=" * (-len(corps) % 4)).decode().split("|")
    except ValueError:
        return ""
    return mail if quoi == "lien" and int(expire) >= time.time() else ""


MESSAGES = {
    "envoye": ("ok", "C'est parti : un lien de connexion vient d'être envoyé à cette adresse. Il est valable 15 minutes. "
                     "Pensez à regarder dans les indésirables."),
    "inconnu": ("err", "Cette adresse n'est pas dans l'équipe de l'enrichisseur (ou n'est plus active). Demandez à Marie de vous ajouter."),
    "attendre": ("err", "Un lien vient déjà de partir vers cette adresse. Attendez une minute avant d'en redemander un."),
    "expire": ("err", "Ce lien a expiré ou a déjà servi à une autre adresse. Redemandez-en un ci-dessous."),
    "retire": ("err", "Votre accès a été retiré ou votre session a expiré. Redemandez un lien ci-dessous."),
    "smtp": ("err", "L'envoi de mail n'est pas configuré (SMTP_USER / SMTP_PASS dans Render)."),
    "envoi": ("err", "Le mail n'a pas pu partir. Réessayez dans un instant ; si ça persiste, prévenez Marie."),
    "mdp": ("err", "Mot de passe incorrect."),
    "refus": ("err", "Votre compte ne donne pas accès à cette page."),
}


@router.get("/v3/connexion", response_class=HTMLResponse)
async def connexion(suite: str = "/v3/contacts", refus: str = "", erreur: str = "", info: str = "", retire: str = ""):
    import html as _h
    cle = info or erreur or ("refus" if refus else "") or ("retire" if retire else "")
    if erreur == "1":
        cle = "mdp"
    genre, msg = MESSAGES.get(cle, ("", ""))
    await EQ.membres()
    if EQ.erreur():
        genre, msg = "err", EQ.erreur()
    suite = suite if suite in SUITES else "/v3/contacts"
    return (_page_html("connexion.html").replace("{{SUITE}}", suite).replace("{{MSG}}", _h.escape(msg))
            .replace("{{MSG_CLASSE}}", genre))


@router.post("/v3/connexion/lien")
async def demander_lien(req: Request, mail: str = Form(""), suite: str = Form("/v3/contacts")):
    import v3_suivi as SUIVI
    suite = suite if suite in SUITES else "/v3/contacts"
    mail = mail.strip().lower()
    retour = lambda cle: RedirectResponse(f"/v3/connexion?suite={quote(suite)}&info={cle}", 303)
    m = EQ.par_mail(await EQ.membres(forcer=True), mail)
    if not m or not (m["nego"] or m["admin"]):
        return retour("inconnu")
    if time.time() - _DERNIER_ENVOI.get(mail, 0) < 60:
        return retour("attendre")
    if not (SUIVI.SMTP_USER and SUIVI.SMTP_PASS):
        return retour("smtp")
    base = (os.getenv("RENDER_EXTERNAL_URL", "") or str(req.base_url)).rstrip("/")
    lien = f"{base}/v3/connexion/lien?t={_jeton_lien(mail, int(time.time()) + LIEN_MINUTES * 60)}&suite={quote(suite)}"
    corps = (f"Bonjour {m['prenom'] or ''},\n\n"
             f"Voici votre lien pour entrer dans l'enrichisseur (valable {LIEN_MINUTES} minutes) :\n{lien}\n\n"
             f"Une fois connecté, vous le restez 30 jours sur cet appareil.\n"
             f"Si vous n'avez rien demandé, ignorez ce message.")
    try:
        await asyncio.to_thread(SUIVI._envoyer, [mail], "Votre lien de connexion à l'enrichisseur", corps, None, "")
    except Exception as e:
        print(f"[V3 CONNEXION] envoi du lien à {mail} : {e}")
        return retour("envoi")
    _DERNIER_ENVOI[mail] = time.time()
    return retour("envoye")


@router.get("/v3/connexion/lien")
async def ouvrir_lien(t: str = "", suite: str = "/v3/contacts"):
    suite = suite if suite in SUITES else "/v3/contacts"
    mail = _lire_lien(t)
    m = EQ.par_mail(await EQ.membres(forcer=True), mail) if mail else {}
    if not m or not (m["nego"] or m["admin"]):
        return RedirectResponse(f"/v3/connexion?suite={quote(suite)}&info={'expire' if not mail else 'inconnu'}", 303)
    r = "admin" if m["admin"] else "nego"
    if suite == "/v3/validation" and r != "admin":
        suite = "/v3/contacts"
    rep = RedirectResponse(suite, 303)
    rep.set_cookie(COOKIE, _jeton(r, mail), max_age=60 * 60 * 24 * 30, httponly=True, samesite="lax", secure=True)
    return rep


@router.post("/v3/connexion")
async def se_connecter(mdp: str = Form(""), suite: str = Form("/v3/validation")):
    """Mot de passe administrateur (secours, Marie et Michel). Le mot de passe négos partagé n'ouvre plus rien."""
    suite = suite if suite in SUITES else "/v3/validation"
    if not (MDP_ADMIN and hmac.compare_digest(mdp, MDP_ADMIN)):
        return RedirectResponse(f"/v3/connexion?suite={quote(suite)}&erreur=1", 303)
    rep = RedirectResponse(suite, 303)
    rep.set_cookie(COOKIE, _jeton("admin", ""), max_age=60 * 60 * 24 * 30, httponly=True, samesite="lax", secure=True)
    return rep


@router.get("/v3/deconnexion")
async def deconnexion():
    rep = RedirectResponse("/v3/connexion", 303)
    rep.delete_cookie(COOKIE)
    return rep


# ───────────────────────── Notion ─────────────────────────
def _json(v, defaut):
    try:
        return json.loads(v) if v else defaut
    except ValueError:
        return defaut


def _props(pg: dict) -> dict:
    p = pg.get("properties") or {}
    nb = lambda n: (p.get(n) or {}).get("number")
    return {"page": pg["id"], "nom": MEM._val(p, "Société"), "siren": MEM._val(p, "SIREN"), "domaine": MEM._val(p, "Domaine"),
            "perimetre": MEM._val(p, "Périmètre"), "traitee": MEM._val(p, "Traitée le")[:10], "lancement": MEM._val(p, "Lancement"),
            "effectif": nb("Effectif LinkedIn"), "nb_nouveaux": nb("Nb nouveaux contacts"), "nb_prets": nb("Nb prêts HubSpot"),
            "validation": MEM._val(p, "Validation") or "À valider", "adresse_retenue": MEM._val(p, "Adresse retenue"),
            "source_retenue": MEM._val(p, "Source adresse retenue"), "validee": MEM._val(p, "Validée le")[:10],
            "decisions": MEM._val(p, "Décisions contacts"), "nouveaux": MEM._val(p, "Nouveaux contacts"),
            "hubspot": MEM._val(p, "Envoyée à HubSpot le")[:10], "lance_par": MEM._val(p, "Lancé par"),
            "negos": MEM._val(p, "Négos"),
            "decision_ecart": MEM._val(p, "Écartée pour"), "motif_ecart": MEM._val(p, "Motif écart"),
            "partis": MEM._val(p, "Partis suivis")}


async def _toutes() -> list:
    out, curseur = [], None
    async with httpx.AsyncClient(timeout=30) as c:
        while True:
            corps = {"sorts": [{"property": "Traitée le", "direction": "descending"}], "page_size": 100}
            if curseur:
                corps["start_cursor"] = curseur
            r = await MEM._req(c, "POST", f"https://api.notion.com/v1/databases/{MEM.MEMOIRE_DB}/query", json=corps)
            if r.status_code != 200:
                raise RuntimeError(f"mémoire Notion inaccessible ({r.status_code})")
            j = r.json()
            out += [_props(pg) for pg in j.get("results", [])]
            if not j.get("has_more"):
                return out
            curseur = j.get("next_cursor")


async def _une(page_id: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "GET", f"https://api.notion.com/v1/pages/{page_id}")
    return _props(r.json()) if r.status_code == 200 else {}


async def _maj(page_id: str, props: dict):
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "PATCH", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    if r.status_code != 200:
        return JSONResponse({"erreur": f"Notion a refusé la mise à jour ({r.status_code})"}, 502)
    return _props(r.json())


def cle_contact(d: dict) -> str:
    return (d.get("linkedin") or d.get("nom_complet") or "").strip().lower()


def nouveaux_dirigeants(data: dict) -> list:
    return sorted([d for d in data.get("dirigeants", []) if not d.get("dans_pipedrive")], key=lambda d: str(d.get("role", "")))


def _sans_listes(s: dict) -> dict:
    return {k: v for k, v in s.items() if k not in ("decisions", "nouveaux")}


def _page_html(nom: str) -> str:
    with open(f"templates/{nom}", encoding="utf-8") as f:
        return f.read()


# ───────────────────────── écran admin : société + contacts CRM ─────────────────────────
@router.get("/v3/validation", response_class=HTMLResponse)
async def page_admin(req: Request):
    return _refus(req, "admin") or HTMLResponse(_page_html("validation.html"))


@router.get("/v3/validation/api/societes")
async def admin_societes(req: Request):
    if (x := _refus(req, "admin")):
        return x
    try:
        toutes = await _toutes()
    except RuntimeError as e:
        return JSONResponse({"erreur": str(e)}, 502)
    liste = await EQ.membres()
    return {"societes": [avec_attribution(_sans_listes(s), liste) for s in toutes],
            "negos": [m["prenom"] for m in EQ.actifs(liste) if m["nego"]], "equipe_erreur": EQ.erreur()}


@router.get("/v3/validation/api/societe/{page_id}")
async def admin_societe(page_id: str, req: Request):
    if (x := _refus(req, "admin")):
        return x
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    info = await _une(page_id)
    if not info:
        return JSONResponse({"erreur": "société introuvable (retraitée depuis ? rechargez la liste)"}, 404)
    info["decisions"] = _json(info["decisions"], [])
    info["nouveaux"] = _json(info["nouveaux"], {})
    info["data"] = await MEM.lire(page_id)
    for p in info["data"].get("contacts_crm", []):
        p.setdefault("role", LI.role(p.get("fonction", "")))
    info["fiches"] = [fiche_nouveau(d, info["nouveaux"].get(cle_contact(d), {})) for d in nouveaux_dirigeants(info["data"])]
    info["roles"] = ROLES
    return avec_attribution(info, await EQ.membres())


DECISIONS_ECART = ["Hors cible HubSpot", "À supprimer de la base"]
MOTIFS_ECART = ["Effectif trop faible", "Hors Île-de-France", "Société fermée", "Doublon"]
_COLONNES_ECART = {"ok": False}


async def _assurer_colonnes_ecart():
    """Crée une fois les colonnes « Écartée pour » et « Motif écart » dans la base mémoire Notion si elles manquent."""
    if _COLONNES_ECART["ok"]:
        return
    async with httpx.AsyncClient(timeout=30) as c:
        r = await MEM._req(c, "PATCH", f"https://api.notion.com/v1/databases/{MEM.MEMOIRE_DB}", json={"properties": {
            "Écartée pour": {"select": {"options": [{"name": n} for n in DECISIONS_ECART]}},
            "Motif écart": {"rich_text": {}}}})
    _COLONNES_ECART["ok"] = r.status_code == 200
    if r.status_code != 200:
        print(f"[V3 VALIDATION] colonnes d'écart : Notion {r.status_code} {r.text[:200]}")


@router.post("/v3/validation/api/societe/{page_id}")
async def valider_societe(page_id: str, req: Request):
    if (x := _refus(req, "admin")):
        return x
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    statut = b.get("statut", "")
    if statut not in ("Société validée", "Écartée", "À valider"):
        return JSONResponse({"erreur": "statut inconnu"}, 400)
    adresse = re.sub(r"\s+", " ", str(b.get("adresse") or "")).strip()
    source = b.get("source", "")
    if statut == "Société validée" and not adresse:
        return JSONResponse({"erreur": "cochez ou saisissez l'adresse retenue"}, 400)
    if source and source not in SOURCES:
        return JSONResponse({"erreur": "source inconnue"}, 400)
    decision, motif = "", ""
    if statut == "Écartée":
        decision = b.get("decision") or "Hors cible HubSpot"
        if decision not in DECISIONS_ECART:
            return JSONResponse({"erreur": "décision inconnue"}, 400)
        motifs = [m for m in (b.get("motifs") or []) if m in MOTIFS_ECART]
        libre = re.sub(r"\s+", " ", str(b.get("motif_libre") or "")).strip()[:300]
        motif = " · ".join(motifs + ([libre] if libre else []))
    await _assurer_colonnes_ecart()
    props = {"Validation": {"select": {"name": statut}},
             "Validée le": {"date": {"start": datetime.now(timezone.utc).isoformat(timespec="seconds")} if statut != "À valider" else None}}
    if _COLONNES_ECART["ok"]:                     # écartée : la décision ; sinon on efface une ancienne décision
        props["Écartée pour"] = {"select": {"name": decision}} if decision else {"select": None}
        props["Motif écart"] = MEM._txt(motif)
    if statut == "Société validée":
        props["Adresse retenue"] = MEM._txt(adresse)
        props["Source adresse retenue"] = {"select": {"name": source}} if source else {"select": None}
    return await _maj(page_id, props)


@router.post("/v3/validation/api/contacts/{page_id}")
async def valider_contacts_crm(page_id: str, req: Request):
    if (x := _refus(req, "admin")):
        return x
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    info = await _une(page_id)
    if not info:
        return JSONResponse({"erreur": "société introuvable"}, 404)
    if info["validation"] not in ("Société validée", "Contacts validés"):
        return JSONResponse({"erreur": "validez d'abord la société"}, 409)
    b = await req.json()
    decisions = []
    for d in b.get("decisions") or []:
        t = d.get("type") or "crm"
        if t not in ("crm", "ailleurs") or d.get("action") not in (ACTIONS_CRM if t == "crm" else ACTIONS_AILLEURS):
            return JSONResponse({"erreur": f"décision invalide : {t} / {d.get('action')}"}, 400)
        propres, erreur = verifier_saisie({k: d.get(k) for k in ("email", "telephone", "linkedin", "fonction", "role") if d.get(k)})
        if erreur:
            return JSONResponse({"erreur": f"{d.get('nom_complet', '')} : {erreur}"}, 400)
        d = {**d, **propres, "le": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        decisions.append({"type": t, **{k: str(d.get(k) or "")[:300] for k in
                          ("action", "id", "nom_complet", "fonction", "email", "linkedin", "statut", "societe_actuelle",
                           "telephone", "role", "le")}})
    return await _maj(page_id, {"Validation": {"select": {"name": "Contacts validés"}},
                                "Décisions contacts": MEM._txts(json.dumps(decisions, ensure_ascii=False))})


# ───────────────────────── écran négos : nouveaux contacts ─────────────────────────
@router.get("/v3/contacts", response_class=HTMLResponse)
async def page_negos(req: Request):
    import html as _h
    x, qui = await _refus_negos(req)
    if x:
        return x
    m = qui["membre"]
    nom = (m.get("prenom") or qui["mail"]) if m else (qui["mail"] or "")
    texte = (f"{nom} (admin : toutes les sociétés)" if nom else "admin : toutes les sociétés") if qui["role"] == "admin" else nom
    return HTMLResponse(_page_html("contacts.html").replace("{{ROLE}}", qui["role"]).replace("{{QUI}}", _h.escape(texte))
                        .replace("{{MOI}}", _h.escape(m.get("prenom", "") if m.get("nego") else "")))


@router.get("/v3/contacts/api/societes")
async def negos_societes(req: Request):
    x, qui = await _refus_negos(req)
    if x:
        return x
    try:
        toutes = await _toutes()
    except RuntimeError as e:
        return JSONResponse({"erreur": str(e)}, 502)
    out = []
    for s in toutes:
        if s["validation"] == "Écartée" or s["perimetre"] not in ("dirigeants", "tout") or not visible_par(qui, s):
            continue
        if s["nb_nouveaux"] == 0:                       # None = société enregistrée avant ce compteur : on l'affiche
            continue
        etats = _json(s["nouveaux"], {})
        s["nb_traites"] = sum(1 for e in etats.values() if e.get("etat") in ("prêt pour HubSpot", "écarté"))
        s = avec_attribution(s, qui["equipe"] or await EQ.membres())
        s["a_moi"] = EQ.visible_pour(qui["membre"], s, qui["equipe"])
        out.append({k: s[k] for k in ("page", "nom", "domaine", "traitee", "lancement", "effectif", "nb_nouveaux",
                                      "nb_prets", "nb_traites", "validation", "attribue_a", "prenoms_inconnus",
                                      "non_attribue", "a_moi")})
    return {"societes": out}


@router.get("/v3/contacts/api/societe/{page_id}")
async def negos_societe(page_id: str, req: Request):
    x, qui = await _refus_negos(req)
    if x:
        return x
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    info = await _une(page_id)
    if not info or info["validation"] == "Écartée" or not visible_par(qui, info):
        return JSONResponse({"erreur": "société introuvable (retraitée depuis ? rechargez la liste)"}, 404)
    data = await MEM.lire(page_id)
    etats = _json(info["nouveaux"], {})
    nouveaux = []
    for d in nouveaux_dirigeants(data):
        nouveaux.append(fiche_nouveau(d, etats.get(cle_contact(d), {})))
    corr = {str(x.get("id")): x for x in _json(info["decisions"], []) if (x.get("type") or "crm") == "crm"}
    crm = []
    for p in data.get("contacts_crm", []):
        c = {k: p.get(k, "") for k in ("nom_complet", "fonction", "linkedin", "email", "telephone", "statut", "detail")}
        c["role"] = p.get("role") or LI.role(p.get("fonction", ""))
        c.update({k: v for k, v in corr.get(str(p.get("id")), {}).items() if k in CHAMPS_MODIFIABLES and v})
        crm.append(c)
    adresse_validee = info["validation"] in ("Société validée", "Contacts validés")
    return {"page": page_id, "nom": info["nom"], "domaine": info["domaine"], "siren": info["siren"], "effectif": info["effectif"],
            "lancement": info["lancement"], "traitee": info["traitee"],
            "attribue_a": avec_attribution(info, qui["equipe"] or await EQ.membres())["attribue_a"],
            "adresse": info["adresse_retenue"] if adresse_validee else (data.get("adresse_proposee") or data.get("adresse_crm") or ""),
            "adresse_validee": adresse_validee, "linkedin_societe": data.get("linkedin", ""),
            "nouveaux": nouveaux, "crm": crm, "roles": ROLES}


def _telephone(t: str) -> str:
    t = re.sub(r"[^\d+]", "", str(t or ""))
    if re.fullmatch(r"0\d{9}", t):
        t = "+33" + t[1:]
    return t if re.fullmatch(r"\+?\d{8,15}", t) else ""


@router.post("/v3/contacts/api/contact/{page_id}")
async def negos_contact(page_id: str, req: Request):
    x, qui = await _refus_negos(req)
    if x:
        return x
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    cle, etat = str(b.get("cle") or "").lower(), b.get("etat")
    if etat is not None and etat not in ETATS_NOUVEAU:
        return JSONResponse({"erreur": "état inconnu"}, 400)
    saisie, erreur = verifier_saisie(b.get("champs") or {})
    if erreur:
        return JSONResponse({"erreur": erreur}, 400)
    info = await _une(page_id)                          # relu juste avant d'écrire : deux négos peuvent travailler en même temps
    if not info or info["validation"] == "Écartée" or not visible_par(qui, info):
        return JSONResponse({"erreur": "société introuvable"}, 404)
    data = await MEM.lire(page_id)
    d = next((d for d in nouveaux_dirigeants(data) if cle_contact(d) == cle), None)
    if not d:
        return JSONResponse({"erreur": "contact introuvable dans cette société"}, 404)
    if saisie.get("role") and saisie["role"] not in ROLES + [d.get("role", "")]:
        return JSONResponse({"erreur": "rôle inconnu"}, 400)
    etats = _json(info["nouveaux"], {})
    e = dict(etats.get(cle, {}))
    modifie = set(e.get("modifie", []))
    for k, v in saisie.items():                         # on ne garde comme « modifié » que ce qui diffère de l'enrichisseur
        if v != (d.get(k) or ""):
            e[k] = v
            modifie.add(k)
        else:
            e.pop(k, None)
            modifie.discard(k)
    e["modifie"] = sorted(modifie)
    if etat is not None:
        e["etat"] = etat
        e["le"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    e.setdefault("etat", "à traiter")
    e.update({"prenom": d.get("prenom", ""), "nom": d.get("nom", ""), "nom_complet": d.get("nom_complet", ""),
              # déjà fiché ailleurs : le négo décide de l'ajouter ; l'admin décide « changer de société » ou « deux sociétés »
              "action": "existant" if d.get("ailleurs_crm") else "créer",
              "fiche_existante": (d.get("ailleurs_crm") or {}).get("id", ""),
              "modifie_par": utilisateur(req) or role(req), "modifie_le": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    fiche = fiche_nouveau(d, e)
    for k in CHAMPS_MODIFIABLES:                        # valeurs finales gardées telles quelles pour l'envoi HubSpot
        e["final_" + k] = fiche[k]
    if e["etat"] == "à traiter" and not e["modifie"]:
        etats.pop(cle, None)
    else:
        etats[cle] = e
    maj = await _maj(page_id, {"Nouveaux contacts": MEM._txts(json.dumps(etats, ensure_ascii=False)),
                               "Nb prêts HubSpot": {"number": sum(1 for x in etats.values() if x.get("etat") == "prêt pour HubSpot")}})
    if isinstance(maj, JSONResponse):
        return maj
    return fiche | {"nb_prets": maj["nb_prets"]}
