"""
diferencia_sanchez_aliaga.py
----------------------------
Muestra la diferencia de votos en bruto Sanchez - Lopez Aliaga
para cada timestamp disponible (sin proyeccion, votos tal cual).

Uso:
    python diferencia_sanchez_aliaga.py
    python diferencia_sanchez_aliaga.py --timestamp 20260417_0605
    python diferencia_sanchez_aliaga.py --ubigeo 15        # departamento Lima
    python diferencia_sanchez_aliaga.py --ubigeo 1501      # provincia Lima
    python diferencia_sanchez_aliaga.py --timestamp 20260417_0605 --ubigeo 15
"""

import argparse
import csv
import glob
import os
import re
from collections import defaultdict

PATRON = re.compile(r"participantes_distritos_(\d{8}_\d{4})\.csv")
UBIGEOS_FILE = "ubigeos_completo.csv"


def nombre_ubigeo(prefijo: str) -> str:
    """Devuelve el nombre del departamento o provincia dado su prefijo de ubigeo."""
    prefijo = prefijo.ljust(6, "0")
    try:
        with open(UBIGEOS_FILE, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                ub = row.get("ubigeo_distrito", "")
                if len(prefijo.rstrip("0")) <= 2:   # departamento
                    if ub.startswith(prefijo[:2]):
                        return row.get("nombre_departamento", prefijo)
                else:                               # provincia
                    if ub.startswith(prefijo[:4]):
                        prov = row.get("nombre_provincia", "")
                        dept = row.get("nombre_departamento", "")
                        return f"{prov}, {dept}"
    except FileNotFoundError:
        pass
    return prefijo


def procesar(path: str, ubigeo: str | None) -> dict[str, dict[str, int]]:
    """Devuelve votos[scope][candidato] donde scope in ("1","2","ubigeo","total")."""
    votos: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("error"):
                continue
            nombre = row.get("nombreCandidato", "").upper()
            ambito   = row.get("id_ambito_geografico", "1")
            ub_depto = row.get("ubigeo_departamento", "").zfill(6)
            ub_prov  = row.get("ubigeo_provincia",    "").zfill(6)
            v = int(float(row.get("totalVotosValidos", 0) or 0))

            scopes = [ambito, "total"]
            if ubigeo:
                eff = len((ubigeo.rstrip("0") or ubigeo))
                if eff <= 2:
                    match = ub_depto[:2] == ubigeo[:2]
                elif eff <= 4:
                    match = ub_prov[:4] == ubigeo[:4]
                else:
                    match = row.get("ubigeo_distrito", "").zfill(6) == ubigeo.zfill(6)
                if match:
                    scopes.append("ubigeo")

            for scope in scopes:
                if "SANCHEZ" in nombre:
                    votos[scope]["sanchez"] += v
                elif "ALIAGA" in nombre:
                    votos[scope]["aliaga"] += v
                votos[scope]["total"] += v

    return votos


def imprimir(ts: str, votos: dict, ubigeo: str | None) -> None:
    print(f"\n{ts}")
    print(f"  {'Ambito':<18} {'Sanchez':>12} {'Aliaga':>12} {'Diferencia':>12} {'%Dif':>8}")
    print(f"  {'-'*60}")

    scopes = [("1", "nacional"), ("2", "exterior")]
    if ubigeo:
        scopes.append(("ubigeo", nombre_ubigeo(ubigeo)))
    scopes.append(("total", "TOTAL"))

    for scope, label in scopes:
        d = votos.get(scope, {})
        v_s  = d.get("sanchez", 0)
        v_a  = d.get("aliaga",  0)
        tot  = d.get("total",   0)
        dif  = v_s - v_a
        pct  = dif / tot * 100 if tot else 0
        print(f"  {label:<18} {v_s:>12,} {v_a:>12,} {dif:>+12,} {pct:>+7.3f}%")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timestamp", default=None,
                        help="Timestamp a analizar (default: todos)")
    parser.add_argument("--ubigeo", default=None,
                        help="Prefijo de ubigeo para filtrar (2 digitos=depto, 4=provincia)")
    args = parser.parse_args()

    archivos = sorted(
        (PATRON.search(os.path.basename(f)).group(1), f)
        for f in glob.glob("data/participantes_distritos_*.csv")
        if PATRON.search(os.path.basename(f))
    )

    if not archivos:
        print("No se encontraron archivos data/participantes_distritos_*.csv")
        return

    if args.timestamp:
        archivos = [(ts, f) for ts, f in archivos if ts == args.timestamp]
        if not archivos:
            print(f"Timestamp {args.timestamp} no encontrado.")
            return

    for ts, path in archivos:
        votos = procesar(path, args.ubigeo)
        imprimir(ts, votos, args.ubigeo)


if __name__ == "__main__":
    main()
