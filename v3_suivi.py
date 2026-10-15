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


async def envoyer_resultat(job: dict, excel: bytes | None, nom_fichier: str):
    dest = job.get("mails") or []
    if not dest or not SMTP_USER or not SMTP_PASS:
        return
    res = job["resultats"]
    etat = {"terminé": "est terminé", "arrêté": "a été arrêté avant la fin"}.get(job["etat"], "s'est interrompu sur une erreur")
    lignes = [f"Bonjour,", "", f"Votre enrichissement « {job['nom']} » {etat}.", "",
              f"Choix : {LIBELLES.get(job['perimetre'], job['perimetre'])}",
              f"Sociétés traitées : {len(res)} sur {job['total']}",
              f"SIREN trouvés : {sum(1 for r in res if r.get('siren_site') or r.get('siren_crm') or r.get('siren_nom'))}"]
    if job.get("linkedin"):
        lignes.append(f"Effectifs LinkedIn : {sum(1 for r in res if r.get('effectif_linkedin') not in (None, ''))}")
    if job["perimetre"] in ("dirigeants", "tout"):
        lignes.append(f"Dirigeants : {sum(len(r.get('dirigeants', [])) for r in res)}")
    if job["perimetre"] == "tout":
        lignes.append(f"E-mails : {sum(1 for r in res for d in r.get('dirigeants', []) if d.get('email'))}")
    if job.get("erreur"):
        lignes += ["", f"Erreur : {job['erreur']}"]
    lignes += ["", "L'Excel est en pièce jointe. Il reste aussi téléchargeable dans l'outil, rubrique « Lancements précédents ».",
               "Rien n'a été modifié dans Pipedrive ni dans HubSpot.", "", "L'enrichisseur v3"]
    try:
        await asyncio.to_thread(_envoyer, dest, f"Enrichissement terminé : {job['nom']}", "\n".join(lignes), excel, nom_fichier)
        job.setdefault("journal", []).append(f"Excel envoyé par mail à {', '.join(dest)}")
    except Exception as e:
        job.setdefault("journal", []).append(f"envoi du mail impossible : {e}")
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
