"""Libellé exact d'un code NAF (sous-classe, ex. 69.10Z → « Activités juridiques »).

Table : nomenclature NAF rév. 2 de l'INSEE, 732 sous-classes (naf_rev2.json, reprise du paquet
@socialgouv/codes-naf, licence Apache-2.0). Un code absent de la table (NAF 2025, faute de frappe)
renvoie "" : l'outil affiche alors la seule section, comme avant."""
import json, os, re

_TABLE = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "naf_rev2.json"), encoding="utf-8"))


def libelle(code: str) -> str:
    c = re.sub(r"\s", "", str(code or "")).upper()
    if re.fullmatch(r"\d{4}[A-Z]", c):          # 6910Z → 69.10Z
        c = c[:2] + "." + c[2:]
    return _TABLE.get(c, "")
