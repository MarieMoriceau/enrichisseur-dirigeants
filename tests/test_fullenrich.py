"""Fullenrich : envoi par lots de 100 (« Too many data » au-delà)."""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import v3_linkedin as LI


def test_lots(monkeypatch):
    envois = []
    async def lot(c, h, datas, journal, n, total):
        envois.append(len(datas))
        return {x["custom"]["idx"]: f"x{x['custom']['idx']}@a.fr" for x in datas[:2]}
    monkeypatch.setattr(LI, "_fullenrich_lot", lot)
    contacts = [({"prenom": "A", "nom": str(i), "linkedin": ""}, {"domaine": "a.fr", "nom": "A"}) for i in range(245)]
    out = asyncio.run(LI.fullenrich_emails(None, contacts, lambda m: None))
    assert envois == [100, 100, 45]
    assert set(out) == {"0", "1", "100", "101", "200", "201"}     # index d'origine conservés
