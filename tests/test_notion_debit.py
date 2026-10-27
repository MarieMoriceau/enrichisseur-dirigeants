import asyncio, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import notion_debit as D


class R:
    def __init__(self, code, ra=None):
        self.status_code, self.headers = code, ({"retry-after": ra} if ra else {})


class C:
    def __init__(self, codes): self.codes, self.quand = list(codes), []
    async def request(self, m, url, **kw):
        self.quand.append(time.monotonic()); return self.codes.pop(0)


def test_cadence_et_pause(monkeypatch):
    monkeypatch.setattr(D, "INTERVALLE", 0.05)
    D._ETAT.update(dernier=0.0, pause_jusqua=0.0)
    c = C([R(200)] * 4)
    async def tout():
        await asyncio.gather(*(D.requete(c, "GET", "u") for _ in range(4)))
    asyncio.run(tout())
    ecarts = [b - a for a, b in zip(c.quand, c.quand[1:])]
    assert all(e >= 0.045 for e in ecarts)               # jamais deux requêtes collées, même en parallèle
    c = C([R(429, "1"), R(200)])
    t = time.monotonic()
    r = asyncio.run(D.requete(c, "GET", "u"))
    assert r.status_code == 200 and time.monotonic() - t >= 0.95   # Retry-After respecté
