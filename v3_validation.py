"""
Enrichisseur v3 — validation des résultats, en deux écrans

  /v3/validation  (mot de passe ADMIN : Marie et Michel Bastian)
      Écran Société : adresses CRM / site / LinkedIn / Google, recommandation, effectif LinkedIn, domiciliation.
                      On coche l'adresse retenue (ou on la saisit), puis on valide ou on écarte la société.
      Écran Contacts CRM : contacts déjà dans Pipedrive « probablement parti » / « à vérifier » / « toujours là ».
                      Seuls ces deux-là décident (garder / marquer parti / à revoir).

  /v3/contacts    (mot de passe NÉGOS ; l'admin y a aussi accès)
      Nouveaux dirigeants trouvés, dès la fin du lancement : profil LinkedIn visible, téléphone à compléter,
      « Valider pour HubSpot » ou « Écarter ». Les contacts déjà au CRM sont visibles en lecture seule.

Tout est gardé dans la mémoire Notion (« Enrichisseur v3 — Mémoire des sociétés »). Rien n'est envoyé dans un CRM
depuis ces écrans : l'envoi HubSpot viendra après la migration de fin octobre, avec un test à blanc sur 50 lignes.
"""
import hashlib, hmac, json, os, re
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

import v3_memoire as MEM

router = APIRouter()
SOURCES = ("CRM", "Site", "LinkedIn", "Google", "Saisie")
ACTIONS_CRM = ("garder", "marquer parti", "à revoir")
ACTIONS_AILLEURS = ("à décider", "changer de société", "deux sociétés")   # contact déjà fiché chez une autre société
ETATS_NOUVEAU = ("à traiter", "prêt pour HubSpot", "écarté")
RE_PAGE = re.compile(r"[0-9a-f\-]{32,36}")

MDP_ADMIN = os.getenv("VALIDATION_MDP_ADMIN", "")
MDP_NEGOS = os.getenv("VALIDATION_MDP_NEGOS", "")
SECRET = (os.getenv("VALIDATION_SECRET") or hashlib.sha256(f"{MDP_ADMIN}|{MDP_NEGOS}|v3".encode()).hexdigest()).encode()
COOKIE = "v3_validation"


# ───────────────────────── accès ─────────────────────────
def _jeton(role: str) -> str:
    return role + "." + hmac.new(SECRET, role.encode(), hashlib.sha256).hexdigest()


def role(req: Request) -> str:
    v = req.cookies.get(COOKIE, "")
    r = v.split(".", 1)[0]
    return r if r in ("admin", "negos") and hmac.compare_digest(v, _jeton(r)) else ""


def _refus(req: Request, besoin: str):
    """Page : renvoi vers la connexion. API : 401/403."""
    r = role(req)
    ok = r == "admin" or (besoin == "negos" and r == "negos")
    if ok:
        return None
    if req.url.path.startswith(("/v3/validation/api", "/v3/contacts/api")):
        return JSONResponse({"erreur": "accès refusé : reconnectez-vous"}, 401 if not r else 403)
    return RedirectResponse(f"/v3/connexion?suite={quote(req.url.path)}" + ("&refus=1" if r else ""), 303)


@router.get("/v3/connexion", response_class=HTMLResponse)
async def connexion(suite: str = "/v3/contacts", refus: str = "", erreur: str = ""):
    with open("templates/connexion.html", encoding="utf-8") as f:
        html = f.read()
    msg = ("Ce mot de passe ne donne pas accès à cette page." if refus else
           "Mot de passe incorrect." if erreur else
           "Accès non configuré : ajoutez VALIDATION_MDP_ADMIN et VALIDATION_MDP_NEGOS dans Render." if not (MDP_ADMIN and MDP_NEGOS) else "")
    suite = suite if suite in ("/v3/validation", "/v3/contacts") else "/v3/contacts"
    return html.replace("{{SUITE}}", suite).replace("{{MSG}}", msg)


@router.post("/v3/connexion")
async def se_connecter(mdp: str = Form(""), suite: str = Form("/v3/contacts")):
    suite = suite if suite in ("/v3/validation", "/v3/contacts") else "/v3/contacts"
    r = ("admin" if MDP_ADMIN and hmac.compare_digest(mdp, MDP_ADMIN) else
         "negos" if MDP_NEGOS and hmac.compare_digest(mdp, MDP_NEGOS) else "")
    if not r:
        return RedirectResponse(f"/v3/connexion?suite={quote(suite)}&erreur=1", 303)
    if suite == "/v3/validation" and r != "admin":
        suite = "/v3/contacts"
    rep = RedirectResponse(suite, 303)
    rep.set_cookie(COOKIE, _jeton(r), max_age=60 * 60 * 24 * 30, httponly=True, samesite="lax", secure=True)
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
            "hubspot": MEM._val(p, "Envoyée à HubSpot le")[:10]}


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
        return {"societes": [_sans_listes(s) for s in await _toutes()]}
    except RuntimeError as e:
        return JSONResponse({"erreur": str(e)}, 502)


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
    return info


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
    props = {"Validation": {"select": {"name": statut}},
             "Validée le": {"date": {"start": datetime.now(timezone.utc).isoformat(timespec="seconds")} if statut != "À valider" else None}}
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
        decisions.append({"type": t, **{k: str(d.get(k) or "")[:300] for k in
                          ("action", "id", "nom_complet", "fonction", "email", "linkedin", "statut", "societe_actuelle")}})
    return await _maj(page_id, {"Validation": {"select": {"name": "Contacts validés"}},
                                "Décisions contacts": MEM._txts(json.dumps(decisions, ensure_ascii=False))})


# ───────────────────────── écran négos : nouveaux contacts ─────────────────────────
@router.get("/v3/contacts", response_class=HTMLResponse)
async def page_negos(req: Request):
    return _refus(req, "negos") or HTMLResponse(_page_html("contacts.html").replace("{{ROLE}}", role(req)))


@router.get("/v3/contacts/api/societes")
async def negos_societes(req: Request):
    if (x := _refus(req, "negos")):
        return x
    try:
        toutes = await _toutes()
    except RuntimeError as e:
        return JSONResponse({"erreur": str(e)}, 502)
    out = []
    for s in toutes:
        if s["validation"] == "Écartée" or s["perimetre"] not in ("dirigeants", "tout"):
            continue
        if s["nb_nouveaux"] == 0:                       # None = société enregistrée avant ce compteur : on l'affiche
            continue
        etats = _json(s["nouveaux"], {})
        s["nb_traites"] = sum(1 for e in etats.values() if e.get("etat") in ("prêt pour HubSpot", "écarté"))
        out.append({k: s[k] for k in ("page", "nom", "domaine", "traitee", "lancement", "effectif", "nb_nouveaux",
                                      "nb_prets", "nb_traites", "validation")})
    return {"societes": out}


@router.get("/v3/contacts/api/societe/{page_id}")
async def negos_societe(page_id: str, req: Request):
    if (x := _refus(req, "negos")):
        return x
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    info = await _une(page_id)
    if not info or info["validation"] == "Écartée":
        return JSONResponse({"erreur": "société introuvable (retraitée depuis ? rechargez la liste)"}, 404)
    data = await MEM.lire(page_id)
    etats = _json(info["nouveaux"], {})
    nouveaux = []
    for d in nouveaux_dirigeants(data):
        e = etats.get(cle_contact(d), {})
        nouveaux.append({"cle": cle_contact(d), "nom_complet": d.get("nom_complet", ""), "fonction": d.get("fonction", ""),
                         "role": d.get("role", ""), "lieu": d.get("lieu", ""), "linkedin": d.get("linkedin", ""),
                         "email": d.get("email", ""), "telephone": e.get("telephone", ""),
                         "ailleurs": d.get("ailleurs_crm") or {}, "autres_societes": d.get("autres_societes") or [],
                         "etat": e.get("etat", "à traiter"), "le": e.get("le", "")})
    crm = [{k: p.get(k, "") for k in ("nom_complet", "fonction", "linkedin", "statut", "detail")} for p in data.get("contacts_crm", [])]
    adresse_validee = info["validation"] in ("Société validée", "Contacts validés")
    return {"page": page_id, "nom": info["nom"], "domaine": info["domaine"], "siren": info["siren"], "effectif": info["effectif"],
            "lancement": info["lancement"], "traitee": info["traitee"],
            "adresse": info["adresse_retenue"] if adresse_validee else (data.get("adresse_proposee") or data.get("adresse_crm") or ""),
            "adresse_validee": adresse_validee, "linkedin_societe": data.get("linkedin", ""),
            "nouveaux": nouveaux, "crm": crm}


def _telephone(t: str) -> str:
    t = re.sub(r"[^\d+]", "", str(t or ""))
    if re.fullmatch(r"0\d{9}", t):
        t = "+33" + t[1:]
    return t if re.fullmatch(r"\+?\d{8,15}", t) else ""


@router.post("/v3/contacts/api/contact/{page_id}")
async def negos_contact(page_id: str, req: Request):
    if (x := _refus(req, "negos")):
        return x
    if not RE_PAGE.fullmatch(page_id):
        return JSONResponse({"erreur": "identifiant invalide"}, 400)
    b = await req.json()
    cle, etat, tel_brut = str(b.get("cle") or "").lower(), b.get("etat"), str(b.get("telephone") or "").strip()
    if etat not in ETATS_NOUVEAU:
        return JSONResponse({"erreur": "état inconnu"}, 400)
    tel = _telephone(tel_brut)
    if tel_brut and not tel:
        return JSONResponse({"erreur": "téléphone illisible (ex. 06 12 34 56 78 ou +33 6 12 34 56 78)"}, 400)
    info = await _une(page_id)                          # relu juste avant d'écrire : deux négos peuvent travailler en même temps
    if not info or info["validation"] == "Écartée":
        return JSONResponse({"erreur": "société introuvable"}, 404)
    data = await MEM.lire(page_id)
    d = next((d for d in nouveaux_dirigeants(data) if cle_contact(d) == cle), None)
    if not d:
        return JSONResponse({"erreur": "contact introuvable dans cette société"}, 404)
    etats = _json(info["nouveaux"], {})
    if etat == "à traiter" and not tel:
        etats.pop(cle, None)
    else:
        etats[cle] = {"etat": etat, "telephone": tel, "le": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                      "prenom": d.get("prenom", ""), "nom": d.get("nom", ""), "nom_complet": d.get("nom_complet", ""),
                      "fonction": d.get("fonction", ""), "email": d.get("email", ""), "linkedin": d.get("linkedin", ""),
                      # déjà fiché ailleurs : le négo décide de l'ajouter ; l'admin décide « changer de société » ou « deux sociétés »
                      "action": "existant" if d.get("ailleurs_crm") else "créer",
                      "fiche_existante": (d.get("ailleurs_crm") or {}).get("id", "")}
    maj = await _maj(page_id, {"Nouveaux contacts": MEM._txts(json.dumps(etats, ensure_ascii=False)),
                               "Nb prêts HubSpot": {"number": sum(1 for e in etats.values() if e["etat"] == "prêt pour HubSpot")}})
    if isinstance(maj, JSONResponse):
        return maj
    return {"cle": cle, "etat": etat, "telephone": tel, "le": etats.get(cle, {}).get("le", ""), "nb_prets": maj["nb_prets"]}
