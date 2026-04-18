"""
margen_error.py
---------------
Calcula el margen de error de la ventaja Sanchez-Aliaga en funcion
de los votos aun no contabilizados.

    margen = (votos_sanchez - votos_aliaga) / votos_faltantes

Un margen < 1 significa que los votos faltantes podrian revertir el resultado.

Uso:
    python margen_error.py
    python margen_error.py --timestamp 20260416_0032
"""

import argparse
import csv
import glob
import os
import re


def ultimo_timestamp() -> str:
    patron = re.compile(r"proyeccion_final_(\d{8}_\d{4})\.csv")
    ts = sorted(
        m.group(1)
        for f in glob.glob("data/proyeccion_final_*.csv")
        if (m := patron.search(os.path.basename(f)))
    )
    if not ts:
        raise FileNotFoundError("No se encontraron archivos data/proyeccion_final_*.csv")
    return ts[-1]


def votos_faltantes(ts: str) -> float:
    totales = {}
    with open(f"data/totales_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            totales[row["ubigeo_distrito"].zfill(6)] = row

    votos_por_distrito: dict[str, int] = {}
    with open(f"data/participantes_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("error"):
                continue
            ub = row.get("ubigeo_distrito", "").zfill(6)
            try:
                votos_por_distrito[ub] = votos_por_distrito.get(ub, 0) + int(float(row.get("totalVotosValidos", 0) or 0))
            except (ValueError, TypeError):
                pass

    total_proyectado = 0.0
    votos_actuales   = 0.0
    with open(f"data/totales_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                pct   = float(row.get("actasContabilizadas", 0) or 0)
                votos = float(row.get("totalVotosValidos", 0) or 0)
                if pct > 0 and votos > 0:
                    total_proyectado += votos / (pct / 100)
                    votos_actuales   += votos
            except (ValueError, TypeError):
                pass

    total_mesas      = sum(int(float(r.get("totalActas", 0) or 0)) for r in totales.values())
    total_votos_real = sum(votos_por_distrito.values())

    votos_imputados = 0.0
    with open(f"data/imputaciones_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        for imp in csv.DictReader(f):
            ub      = imp["ubigeo"].zfill(6)
            donors  = [d.zfill(6) for d in imp.get("donor_ubigeo", "").split("+") if d.strip()]
            def mesas(u):
                try: return int(float(totales.get(u, {}).get("totalActas", 0) or 0))
                except (ValueError, TypeError): return 0
            m_imp   = mesas(ub)
            m_donor = sum(mesas(d) for d in donors)
            v_donor = sum(votos_por_distrito.get(d, 0) for d in donors)
            if m_donor > 0:
                votos_imputados += v_donor * m_imp / m_donor
            elif total_mesas:
                votos_imputados += total_votos_real / total_mesas * m_imp

    return (total_proyectado + votos_imputados) - votos_actuales


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timestamp", help="Timestamp a analizar (default: ultimo disponible)")
    args = parser.parse_args()

    if args.timestamp == "last":
        timestamps = [ultimo_timestamp()]
    elif args.timestamp:
        timestamps = [args.timestamp]
    else:
        patron = re.compile(r"proyeccion_final_(\d{8}_\d{4})\.csv")
        timestamps = sorted(
            m.group(1)
            for f in glob.glob("data/proyeccion_final_*.csv")
            if (m := patron.search(os.path.basename(f)))
        )

    for ts in timestamps:
        with open(f"data/proyeccion_final_{ts}.csv", newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))

        def buscar(nombre: str) -> dict:
            return next(r for r in rows if nombre.upper() in r["nombreCandidato"].upper())

        sanchez    = buscar("SANCHEZ")
        aliaga     = buscar("ALIAGA")
        v_sanchez  = float(sanchez["votos_proyectados"])
        v_aliaga   = float(aliaga["votos_proyectados"])
        diferencia = v_sanchez - v_aliaga

        faltantes = votos_faltantes(ts)
        margen    = diferencia / faltantes if faltantes else float("inf")

        print(f"[{ts}]  dif={diferencia:>8,.0f}  faltantes={faltantes:>10,.0f}  margen={margen:.4f}x ({margen*100:.2f}%)")


if __name__ == "__main__":
    main()
