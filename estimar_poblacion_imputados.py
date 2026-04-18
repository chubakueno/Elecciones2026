"""
estimar_poblacion_imputados.py
------------------------------
Estima el peso electoral de los distritos imputados en el último snapshot.

Método:
  Para cada distrito imputado se usa totalActas (mesas electorales) como
  proxy del tamaño del electorado. El peso proyectado de votos se estima
  escalando los votos reales del donor por el ratio de mesas:

      votos_estimados = votos_donor * (mesas_imputado / mesas_donor)

  Cuando el donor es compuesto (varios ubigeos) se usa la suma de sus mesas
  y la suma de sus votos como referencia.

Salida:
  - Resumen global (% de mesas y votos bajo imputación)
  - Detalle por tipo de imputación
  - Tabla de los 20 distritos imputados más grandes
"""

import csv
import glob
import os
import re
from collections import defaultdict

PATRON_IMP  = re.compile(r"imputaciones_(\d{8}_\d{4})\.csv")
PATRON_TOT  = re.compile(r"totales_distritos_(\d{8}_\d{4})\.csv")
PATRON_PART = re.compile(r"participantes_distritos_(\d{8}_\d{4})\.csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ultimo_timestamp() -> str:
    ts = sorted(
        m.group(1)
        for f in glob.glob("data/imputaciones_*.csv")
        if (m := PATRON_IMP.match(os.path.basename(f)))
    )
    if not ts:
        raise FileNotFoundError("No se encontraron archivos imputaciones_*.csv")
    return ts[-1]


def load_totales(ts: str) -> dict[str, dict]:
    """ubigeo -> fila de totales_distritos_TS.csv"""
    rows = {}
    with open(f"data/totales_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows[row["ubigeo_distrito"].zfill(6)] = row
    return rows


def load_votos_por_distrito(ts: str) -> dict[str, int]:
    """ubigeo -> suma de votos válidos de todos los candidatos en ese distrito."""
    votos: dict[str, int] = defaultdict(int)
    with open(f"data/participantes_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("error"):
                continue
            ub = row.get("ubigeo_distrito", "").zfill(6)
            try:
                votos[ub] += int(float(row.get("totalVotosValidos", 0) or 0))
            except (ValueError, TypeError):
                pass
    return dict(votos)


def load_imputaciones(ts: str) -> list[dict]:
    rows = []
    with open(f"data/imputaciones_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return rows


def mesas(row: dict) -> int:
    try:
        return int(float(row.get("totalActas", 0) or 0))
    except (ValueError, TypeError):
        return 0


def scope_tipo(scope: str) -> str:
    s = scope.lower()
    if s.startswith("centroide"):          return "centroide"
    if "provincia" in s:                   return "provincia"
    if "departamento" in s:               return "departamento"
    if "global" in s or "extranjero" in s: return "extranjero"
    return "sin referencia"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = ultimo_timestamp()
    print(f"Snapshot: {ts}\n")

    totales = load_totales(ts)
    votos   = load_votos_por_distrito(ts)
    imps    = load_imputaciones(ts)

    # ── Universo total ────────────────────────────────────────────────────
    total_mesas = sum(mesas(r) for r in totales.values())
    total_votos = sum(votos.values())

    # ── Procesar imputados ────────────────────────────────────────────────
    # Para cada distrito imputado: estimar votos = votos_donor * ratio_mesas
    detalle = []
    for imp in imps:
        ub    = imp["ubigeo"].zfill(6)
        tipo  = scope_tipo(imp.get("scope", ""))
        m_imp = mesas(totales.get(ub, {}))

        # Donors: pueden ser varios separados por "+"
        donors_ub = [d.zfill(6) for d in imp.get("donor_ubigeo", "").split("+") if d.strip()]
        m_donor   = sum(mesas(totales.get(d, {})) for d in donors_ub)
        v_donor   = sum(votos.get(d, 0) for d in donors_ub)

        if m_donor > 0:
            v_estimado = round(v_donor * m_imp / m_donor)
        else:
            # Sin datos del donor: estimación por media nacional
            v_estimado = round(total_votos / total_mesas * m_imp) if total_mesas else 0

        detalle.append({
            "ubigeo":      ub,
            "ambito":      imp.get("ambito", "1"),
            "nombre":      imp.get("nombre", ""),
            "provincia":   imp.get("provincia", ""),
            "departamento":imp.get("departamento", ""),
            "tipo":        tipo,
            "mesas":       m_imp,
            "votos_est":   v_estimado,
            "donor":       imp.get("donor_nombre", ""),
        })

    # ── Totales imputados ─────────────────────────────────────────────────
    mesas_imp = sum(d["mesas"]     for d in detalle)
    votos_imp = sum(d["votos_est"] for d in detalle)

    print(f"  Distritos imputados : {len(detalle):>6,}")
    print(f"  Distritos totales   : {len(totales):>6,}")
    print(f"  Mesas imputadas     : {mesas_imp:>6,}  /  {total_mesas:,}  ({mesas_imp/total_mesas*100:.2f}%)")
    if total_votos:
        print(f"  Votos imputados est.: {votos_imp:>6,}  /  {total_votos:,}  ({votos_imp/total_votos*100:.2f}%)")

    # ── Detalle completo por ámbito ───────────────────────────────────────
    COL = f"  {'Ubigeo':<8} {'Nombre':<28} {'Prov/Dpto':<22} {'Tipo':<14} {'Mesas':>6} {'Votos est.':>12}"
    SEP = "  " + "-" * 96

    for ambito_id, ambito_label in [("1", "NACIONAL"), ("2", "EXTRANJERO")]:
        grupo = [d for d in detalle if d["ambito"] == ambito_id]
        if not grupo:
            continue
        grupo_sorted = sorted(grupo, key=lambda x: -x["mesas"])
        mesas_g = sum(d["mesas"]     for d in grupo)
        votos_g = sum(d["votos_est"] for d in grupo)

        print(f"\n-- {ambito_label} ({len(grupo)} distritos) ----------------------------------")
        print(COL)
        print(SEP)
        for d in grupo_sorted:
            loc = f"{d['provincia'][:10]}/{d['departamento'][:10]}"
            print(f"  {d['ubigeo']:<8} {d['nombre'][:27]:<28} {loc:<22} "
                  f"{d['tipo'][:13]:<14} {d['mesas']:>6,} {d['votos_est']:>12,}")
        print(SEP)
        pct_m = mesas_g / total_mesas * 100 if total_mesas else 0
        pct_v = votos_g / total_votos * 100 if total_votos else 0
        print(f"  {'SUBTOTAL ' + ambito_label:<60} {mesas_g:>6,} {votos_g:>12,}"
              f"  ({pct_m:.2f}% mesas, {pct_v:.2f}% votos)")

    # ── Total global ──────────────────────────────────────────────────────
    print(f"\n{'='*98}")
    pct_m = mesas_imp / total_mesas * 100 if total_mesas else 0
    pct_v = votos_imp / total_votos * 100 if total_votos else 0
    print(f"  {'TOTAL IMPUTADOS':<60} {mesas_imp:>6,} {votos_imp:>12,}"
          f"  ({pct_m:.2f}% mesas, {pct_v:.2f}% votos)")
    print(f"  {'TOTAL GLOBAL':<60} {total_mesas:>6,} {total_votos:>12,}")


if __name__ == "__main__":
    main()
