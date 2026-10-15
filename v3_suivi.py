"""
Enrichisseur v3 — suivi des lancements
  • chaque lancement est noté dans la base Notion « Enrichisseur Dirigeants — Runs » (celle de l'ancien outil),
    avec l'Excel joint à la fin : c'est l'historique affiché dans l'outil
  • l'Excel est aussi envoyé par mail à la personne qui a lancé (SMTP OVH, comme l'ancien outil)
  • tant qu'un lancement tourne, le service se réveille lui-même : on peut fermer l'onglet
"""
import asyncio, os, re, smtplib, time
from datetime import datetime, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx

NOTION_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_DB = "0631870b-4265-4ccf-8284-d62d0633acdb"
SMTP_HOST = os.getenv("SMTP_HOST", "pro2.mail.ovh.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
URL_PUBLIQUE = os.getenv("RENDER_EXTERNAL_URL", "")   # fournie automatiquement par Render
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
RE_MAIL = re.compile(r"^[\w.+\-']+@[\w\-]+(\.[\w\-]+)+$")
LIBELLES = {"siren": "SIREN et statut", "societes": "Sociétés", "dirigeants": "Sociétés + dirigeants", "tout": "Tout"}


def _h(json_=True):
    h = {"Authorization": f"Bearer {NOTION_KEY}", "Notion-Version": "2022-06-28"}
    if json_:
        h["Content-Type"] = "application/json"
    return h


def mails_valides(texte: str) -> list:
    return [m for m in re.split(r"[\s,;]+", texte or "") if RE_MAIL.match(m)]


def _txt(v):
    return {"rich_text": [{"text": {"content": str(v)[:2000]}}]}


# ───────────────────────── Notion ─────────────────────────
async def notion_creer(job: dict) -> str:
    if not NOTION_KEY:
        return ""
    props = {
        "Nom": {"title": [{"text": {"content": job["nom"][:100]}}]},
        "Statut": {"select": {"name": "EN_COURS"}},
        "Phases": _txt(f"v3 · {LIBELLES.get(job['perimetre'], job['perimetre'])}"),
        "Run ID": _txt(job["id"]),
        "Emails destinataires": _txt(", ".join(job.get("mails", []))),
        "Démarré le": {"date": {"start": datetime.now(timezone.utc).isoformat(timespec="seconds")}},
        "Nb sociétés": {"number": job["total"]},
        "Nb LinkedIn": {"number": 0},                   # fixé juste après, une fois la mémoire et le quota passés
    }
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post("https://api.notion.com/v1/pages", headers=_h(),
                             json={"parent": {"database_id": NOTION_DB}, "properties": props})
            if r.status_code == 200:
                return r.json()["id"]
            print(f"[V3 SUIVI] Notion création {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"[V3 SUIVI] Notion création : {e}")
    return ""


async def notion_finir(job: dict, excel: bytes | None, nom_fichier: str):
    pid = job.get("notion_id")
    if not NOTION_KEY or not pid:
        return
    res = job["resultats"]
    statut = {"terminé": "TERMINE", "arrêté": "ARRETE"}.get(job["etat"], "ERREUR")
    props = {
        "Statut": {"select": {"name": statut}},
        "Nb sociétés": {"number": len(res)},
        "Nb contacts": {"number": sum(len(r.get("dirigeants", [])) for r in res)},
        "Nb emails": {"number": sum(1 for r in res for d in r.get("dirigeants", []) if d.get("email"))},
        "Durée (min)": {"number": round(((job["fin"] or time.time()) - job["debut"]) / 60, 1)},
        "Nb LinkedIn": {"number": job.get("li_utilise", 0)},   # la réservation du départ est remplacée par l'usage réel
        "Terminé le": {"date": {"start": datetime.now(timezone.utc).isoformat(timespec="seconds")}},
    }
    if job.get("erreur"):
        props["Erreur"] = _txt(job["erreur"])
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            if excel:
                r1 = await c.post("https://api.notion.com/v1/file_uploads", headers=_h(),
                                  json={"filename": nom_fichier, "content_type": XLSX})
                fid = r1.json().get("id") if r1.status_code == 200 else None
                if fid:
                    r2 = await c.post(f"https://api.notion.com/v1/file_uploads/{fid}/send", headers=_h(False),
                                      files={"file": (nom_fichier, excel, XLSX)})
                    if r2.status_code == 200:
                        props["Fichier Excel"] = {"files": [{"name": nom_fichier, "type": "file_upload",
                                                             "file_upload": {"id": fid}}]}
                    else:
                        print(f"[V3 SUIVI] Notion envoi fichier {r2.status_code} {r2.text[:200]}")
                else:
                    print(f"[V3 SUIVI] Notion upload {r1.status_code} {r1.text[:200]}")
            r = await c.patch(f"https://api.notion.com/v1/pages/{pid}", headers=_h(), json={"properties": props})
            if r.status_code != 200:
                print(f"[V3 SUIVI] Notion fin {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"[V3 SUIVI] Notion fin : {e}")


def _val(p: dict, nom: str):
    x = p.get(nom) or {}
    t = x.get("type")
    if t in ("title", "rich_text"):
        return "".join(b.get("plain_text", "") for b in x[t])
    if t == "select":
        return (x["select"] or {}).get("name", "")
    if t == "number":
        return x["number"]
    if t == "date":
        return (x["date"] or {}).get("start", "")
    if t == "files":
        return bool(x["files"])
    return ""


async def historique(jobs: dict, n: int = 40) -> list:
    if not NOTION_KEY:
        return []
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"https://api.notion.com/v1/databases/{NOTION_DB}/query", headers=_h(), json={
            "filter": {"property": "Phases", "rich_text": {"starts_with": "v3"}},
            "sorts": [{"timestamp": "created_time", "direction": "descending"}], "page_size": n})
        if r.status_code != 200:
            print(f"[V3 SUIVI] Notion historique {r.status_code} {r.text[:200]}")
            return []
        out = []
        for pg in r.json().get("results", []):
            p = pg["properties"]
            rid, statut = _val(p, "Run ID"), _val(p, "Statut")
            if statut == "EN_COURS" and rid not in jobs:
                # le service a redémarré pendant ce lancement : il ne reprendra pas
                statut = "ERREUR"
                await c.patch(f"https://api.notion.com/v1/pages/{pg['id']}", headers=_h(), json={"properties": {
                    "Statut": {"select": {"name": "ERREUR"}},
                    "Erreur": _txt("Interrompu : le service a redémarré pendant le traitement")}})
            out.append({"page": pg["id"], "job": rid, "nom": _val(p, "Nom"), "statut": statut,
                        "perimetre": _val(p, "Phases").replace("v3 · ", ""), "par": _val(p, "Emails destinataires"),
                        "debut": _val(p, "Démarré le"), "societes": _val(p, "Nb sociétés"),
                        "dirigeants": _val(p, "Nb contacts"), "emails": _val(p, "Nb emails"),
                        "duree": _val(p, "Durée (min)"), "fichier": _val(p, "Fichier Excel"),
                        "en_memoire": rid in jobs})
        return out


async def url_fichier(page_id: str) -> str:
    """Lien de téléchargement frais (Notion ne donne que des liens valables 1 heure)."""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"https://api.notion.com/v1/pages/{page_id}", headers=_h())
        if r.status_code != 200:
            return ""
        fs = r.json()["properties"].get("Fichier Excel", {}).get("files", [])
        if not fs:
            return ""
        f = fs[0]
        return (f.get("file") or f.get("external") or {}).get("url", "")


# ───────────────────────── mail ─────────────────────────
def _envoyer(dest: list, sujet: str, corps: str, excel: bytes | None, nom_fichier: str):
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(dest)
    msg["Subject"] = sujet
    msg.attach(MIMEText(corps, "plain", "utf-8"))
    if excel:
        part = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.set_payload(excel)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{nom_fichier}"')
        msg.attach(part)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_USER, dest, msg.as_string())


COPIE_COMPLETE = mails_valides(os.getenv("COPIE_FICHIER_COMPLET", "mmoriceau@equation-sie.com"))


async def envoyer_resultat(job: dict, excel: bytes | None, nom_fichier: str, excel_nego: bytes | None = None, nom_nego: str = ""):
    """Le négo reçoit la version courte (contacts et e-mails) quand il y a des dirigeants ;
    l'adresse COPIE_FICHIER_COMPLET (Marie) reçoit toujours le fichier complet."""
    dest = job.get("mails") or []
    if not SMTP_USER or not SMTP_PASS:
        return
    res = job["resultats"]
    etat = {"terminé": "est terminé", "arrêté": "a été arrêté avant la fin"}.get(job["etat"], "s'est interrompu sur une erreur")
    def corps(version):
        l = ["Bonjour,", "", f"L'enrichissement « {job['nom']} » {etat}.", "",
             f"Choix : {LIBELLES.get(job['perimetre'], job['perimetre'])}",
             f"Sociétés : {len(res)}" + (f" (dont {job['nb_memoire']} reprises de la mémoire, sans nouvelle recherche)" if job.get("nb_memoire") else "")]
        if job["perimetre"] in ("dirigeants", "tout"):
            l.append(f"Dirigeants : {sum(len(r.get('dirigeants', [])) for r in res)}")
        if job["perimetre"] == "tout":
            l.append(f"E-mails : {sum(1 for r in res for d in r.get('dirigeants', []) if d.get('email'))}")
        if job.get("report"):
            l += ["", job["report"]]
        if job.get("erreur"):
            l += ["", f"Erreur : {job['erreur']}"]
        l += ["", version, "Rien n'a été modifié dans Pipedrive ni dans HubSpot.", "", "L'enrichisseur v3"]
        return "\n".join(l)
    envois = []
    if dest and excel_nego:
        envois.append((dest, "Vos contacts", corps("En pièce jointe : les dirigeants et leurs e-mails."), excel_nego, nom_nego))
        complets = [m for m in COPIE_COMPLETE if m.lower() not in {d.lower() for d in dest}]
        # si la personne qui lance est aussi le destinataire du complet, elle reçoit les deux fichiers
        if any(m.lower() in {d.lower() for d in dest} for m in COPIE_COMPLETE):
            envois.append(([m for m in COPIE_COMPLETE if m.lower() in {d.lower() for d in dest}], "Fichier complet",
                           corps("En pièce jointe : le fichier complet (sociétés, adresses, effectifs, dirigeants)."), excel, nom_fichier))
    else:
        envois.append((dest, "Enrichissement terminé", corps("L'Excel est en pièce jointe. Il reste aussi dans l'outil, rubrique « Lancements précédents »."), excel, nom_fichier))
        complets = [m for m in COPIE_COMPLETE if m.lower() not in {d.lower() for d in dest}]
    if complets and excel:
        par = ", ".join(dest) or "un utilisateur"
        envois.append((complets, "Copie du fichier complet", corps(f"Copie du fichier complet, lancement fait par {par}."), excel, nom_fichier))
    for qui, sujet, texte, fichier, nom in envois:
        if not qui:
            continue
        try:
            await asyncio.to_thread(_envoyer, qui, f"{sujet} : {job['nom']}", texte, fichier, nom)
            job.setdefault("journal", []).append(f"{sujet} envoyé à {', '.join(qui)}")
        except Exception as e:
            job.setdefault("journal", []).append(f"envoi du mail impossible ({', '.join(qui)}) : {e}")
            print(f"[V3 SUIVI] mail : {e}")


# ───────────────────────── rester éveillé ─────────────────────────
async def rester_eveille(jobs: dict):
    """Render (offre gratuite) endort le service après 15 min sans visite.
    Tant qu'un lancement tourne, on se rend visite à soi-même toutes les 4 minutes."""
    global _EVEIL
    if not URL_PUBLIQUE or _EVEIL:
        return
    _EVEIL = True
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            while any(j["etat"] == "en cours" for j in jobs.values()):
                await asyncio.sleep(240)
                try:
                    await c.get(URL_PUBLIQUE.rstrip("/") + "/v3/config")
                except Exception:
                    pass
    finally:
        _EVEIL = False


_EVEIL = False


# ───────────────────────── quota LinkedIn par jour ─────────────────────────
import json
from datetime import timedelta
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")
QUOTA_JOUR = int(os.getenv("QUOTA_LINKEDIN_JOUR", "150"))   # sociétés passées par LinkedIn par jour, tous négos confondus
HEURE_REPORT = 7                                            # les lancements reportés partent à 7 h (heure de Paris)
CRON_SECRET = os.getenv("CRON_SECRET", "")


def _jour(d) -> tuple:
    debut = datetime(d.year, d.month, d.day, tzinfo=PARIS)
    return debut, debut + timedelta(days=1)


async def utilise_le(d) -> int:
    """Sociétés passées (ou programmées) par LinkedIn le jour d, d'après la base Notion des lancements."""
    if not NOTION_KEY:
        return 0
    debut, fin = _jour(d)
    total, curseur = 0, None
    async with httpx.AsyncClient(timeout=15) as c:
        while True:
            corps = {"filter": {"and": [
                {"property": "Phases", "rich_text": {"starts_with": "v3"}},
                {"property": "Phases", "rich_text": {"does_not_contain": LIBELLES["siren"]}},
                {"property": "Démarré le", "date": {"on_or_after": debut.isoformat()}},
                {"property": "Démarré le", "date": {"before": fin.isoformat()}},
                {"property": "Statut", "select": {"does_not_equal": "ARRETE"}}]}, "page_size": 100}
            if curseur:
                corps["start_cursor"] = curseur
            r = await c.post(f"https://api.notion.com/v1/databases/{NOTION_DB}/query", headers=_h(), json=corps)
            if r.status_code != 200:
                print(f"[V3 QUOTA] Notion {r.status_code} {r.text[:200]}")
                return total
            j = r.json()
            for p in j.get("results", []):
                nl = _val(p["properties"], "Nb LinkedIn")
                total += int(nl if nl is not None and nl != "" else (_val(p["properties"], "Nb sociétés") or 0))
            if not j.get("has_more"):
                return total
            curseur = j.get("next_cursor")


async def quota_du_jour() -> dict:
    u = await utilise_le(datetime.now(PARIS))
    return {"quota": QUOTA_JOUR, "utilise": u, "reste": max(0, QUOTA_JOUR - u)}


async def planifier_suite(reste: list) -> list:
    """Répartit ce qui ne passe pas aujourd'hui sur les jours suivants, à 7 h : [(date de départ, lot)]."""
    lots, reste = [], list(reste)
    jour = datetime.now(PARIS)
    for _ in range(30):
        if not reste:
            break
        jour = jour + timedelta(days=1)
        depart = datetime(jour.year, jour.month, jour.day, HEURE_REPORT, tzinfo=PARIS)
        dispo = QUOTA_JOUR - await utilise_le(depart)
        if dispo > 0:
            lots.append((depart, reste[:dispo]))
            reste = reste[dispo:]
    return lots


async def programmer(nom: str, perimetre: str, mails: list, orgs: list, depart) -> str:
    """Enregistre un lancement reporté dans Notion (statut EN_ATTENTE), avec sa liste de sociétés dans la page."""
    if not NOTION_KEY:
        return ""
    data = json.dumps(orgs, ensure_ascii=False)
    blocs = [{"object": "block", "type": "code", "code": {"language": "json",
              "rich_text": [{"type": "text", "text": {"content": data[i:i + 1900]}}]}} for i in range(0, len(data), 1900)]
    props = {
        "Nom": {"title": [{"text": {"content": nom[:100]}}]},
        "Statut": {"select": {"name": "EN_ATTENTE"}},
        "Phases": _txt(f"v3 · {LIBELLES.get(perimetre, perimetre)}"),
        "Emails destinataires": _txt(", ".join(mails)),
        "Démarré le": {"date": {"start": depart.isoformat(timespec="minutes")}},
        "Nb sociétés": {"number": len(orgs)},
        "Nb LinkedIn": {"number": len(orgs)},           # réservation sur le quota du jour de départ
    }
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post("https://api.notion.com/v1/pages", headers=_h(),
                         json={"parent": {"database_id": NOTION_DB}, "properties": props, "children": blocs[:100]})
        if r.status_code != 200:
            print(f"[V3 QUOTA] programmation {r.status_code} {r.text[:200]}")
            return ""
        pid = r.json()["id"]
        for i in range(100, len(blocs), 100):                # au-delà de 100 blocs, on complète
            await c.patch(f"https://api.notion.com/v1/blocks/{pid}/children", headers=_h(), json={"children": blocs[i:i + 100]})
        return pid


async def a_lancer() -> list:
    """Lancements reportés dont l'heure est venue : [(page_id, nom, perimetre, mails, orgs)]."""
    if not NOTION_KEY:
        return []
    out = []
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"https://api.notion.com/v1/databases/{NOTION_DB}/query", headers=_h(), json={"filter": {"and": [
            {"property": "Phases", "rich_text": {"starts_with": "v3"}},
            {"property": "Statut", "select": {"equals": "EN_ATTENTE"}},
            {"property": "Démarré le", "date": {"on_or_before": datetime.now(PARIS).isoformat()}}]}, "page_size": 20})
        if r.status_code != 200:
            return []
        for pg in r.json().get("results", []):
            p = pg["properties"]
            texte, curseur = "", None
            while True:
                b = await c.get(f"https://api.notion.com/v1/blocks/{pg['id']}/children", headers=_h(),
                                params={"page_size": 100, **({"start_cursor": curseur} if curseur else {})})
                bj = b.json()
                for bl in bj.get("results", []):
                    if bl.get("type") == "code":
                        texte += "".join(t.get("plain_text", "") for t in bl["code"]["rich_text"])
                if not bj.get("has_more"):
                    break
                curseur = bj.get("next_cursor")
            try:
                orgs = json.loads(texte)
            except ValueError:
                await c.patch(f"https://api.notion.com/v1/pages/{pg['id']}", headers=_h(), json={"properties": {
                    "Statut": {"select": {"name": "ERREUR"}}, "Erreur": _txt("liste de sociétés illisible")}})
                continue
            perim = next((k for k, v in LIBELLES.items() if v == _val(p, "Phases").replace("v3 · ", "")), "tout")
            out.append((pg["id"], _val(p, "Nom"), perim, mails_valides(_val(p, "Emails destinataires")), orgs))
    return out


async def prevenir_report(mails: list, nom: str, lots: list, perimetre: str):
    """Mail immédiat au négo : sa liste est coupée, la suite part demain (ou plus tard)."""
    if not mails or not SMTP_USER or not SMTP_PASS:
        return
    tout = sum(len(l) for _, l in lots)
    lignes = ["Bonjour,", "", f"Votre liste « {nom} » compte {tout} sociétés. Pour ne pas dépasser le quota LinkedIn "
              f"de {QUOTA_JOUR} sociétés par jour (tous négos confondus), elle a été découpée :", ""]
    for depart, lot in lots:
        quand = "tout de suite" if depart is None else depart.strftime("le %d/%m à %H h")
        lignes.append(f"  • {len(lot)} sociétés : {quand}")
    lignes += ["", "Vous recevrez un Excel par mail à la fin de chaque partie. Rien à faire de votre côté.",
               f"Choix : {LIBELLES.get(perimetre, perimetre)}", "", "L'enrichisseur v3"]
    try:
        await asyncio.to_thread(_envoyer, mails, f"Enrichissement programmé : {nom}", "\n".join(lignes), None, "")
    except Exception as e:
        print(f"[V3 QUOTA] mail : {e}")


async def maj(page_id: str, props: dict):
    """Met à jour quelques propriétés d'un lancement dans Notion (valeurs simples : nombre, texte, statut)."""
    if not NOTION_KEY or not page_id:
        return
    p = {}
    for k, v in props.items():
        if k == "Statut":
            p[k] = {"select": {"name": v}}
        elif k == "Démarré le":
            p[k] = {"date": {"start": v}}
        elif isinstance(v, (int, float)):
            p[k] = {"number": v}
        else:
            p[k] = _txt(v)
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            await c.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=_h(), json={"properties": p})
    except Exception as e:
        print(f"[V3 SUIVI] maj : {e}")
