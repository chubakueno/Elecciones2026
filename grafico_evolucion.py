"""
grafico_evolucion.py
--------------------
Lee todos los archivos proyeccion_final_YYYYMMDD_HHMM.csv y genera
una polilínea interactiva que muestra cómo evolucionó el porcentaje
proyectado de cada candidato a lo largo del tiempo.

Salida: evolucion_proyeccion.html

Uso:
    python grafico_evolucion.py
    python grafico_evolucion.py --top 5        # solo los 5 primeros del ultimo snapshot
    python grafico_evolucion.py --output mi_grafico.html
"""

import argparse
import csv
import glob
import re
from datetime import datetime

import plotly.graph_objects as go


PATRON_ARCHIVO = "proyeccion_final_*.csv"
OUTPUT_HTML    = "evolucion_proyeccion.html"


def parsear_timestamp(nombre: str) -> datetime | None:
    """Extrae el datetime del nombre proyeccion_final_YYYYMMDD_HHMM.csv."""
    m = re.search(r"proyeccion_final_(\d{8})_(\d{4})\.csv", nombre)
    if not m:
        return None
    return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")


def cargar_snapshot(path: str) -> dict[str, float]:
    """Devuelve {dniCandidato: porcentaje_proyectado} para un snapshot."""
    datos = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dni = row.get("dniCandidato", "").strip()
            pct = float(row.get("porcentaje_proyectado", 0) or 0)
            datos[dni] = pct
    return datos


def cargar_meta(path: str) -> dict[str, dict]:
    """Devuelve {dniCandidato: {nombre, partido}} del snapshot más reciente."""
    meta = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dni = row.get("dniCandidato", "").strip()
            meta[dni] = {
                "nombre":  row.get("nombreCandidato", dni),
                "partido": row.get("nombreAgrupacionPolitica", ""),
                "votos":   int(row.get("votos_proyectados", 0) or 0),
            }
    return meta


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=None,
                        help="Mostrar solo los N candidatos con mas votos en el ultimo snapshot")
    parser.add_argument("--output", type=str, default=OUTPUT_HTML,
                        help=f"Archivo HTML de salida (default: {OUTPUT_HTML})")
    args = parser.parse_args()

    # --- Cargar snapshots ---
    archivos = sorted(glob.glob(PATRON_ARCHIVO))
    if not archivos:
        print(f"No se encontraron archivos con patron {PATRON_ARCHIVO}")
        return

    snapshots = []
    for archivo in archivos:
        ts = parsear_timestamp(archivo)
        if ts:
            snapshots.append((ts, archivo))

    snapshots.sort(key=lambda x: x[0])
    print(f"  {len(snapshots)} snapshots encontrados:")
    for ts, path in snapshots:
        print(f"    {ts:%Y-%m-%d %H:%M}  {path}")

    # Meta del snapshot mas reciente
    _, ultimo = snapshots[-1]
    meta = cargar_meta(ultimo)

    # Filtrar top N si se especifica
    if args.top:
        top_dnis = sorted(meta, key=lambda d: meta[d]["votos"], reverse=True)[:args.top]
    else:
        top_dnis = list(meta.keys())

    # Construir series temporales: {dni: [(ts, pct), ...]}
    series: dict[str, list] = {dni: [] for dni in top_dnis}

    for ts, path in snapshots:
        datos = cargar_snapshot(path)
        for dni in top_dnis:
            pct = datos.get(dni, None)
            series[dni].append((ts, pct))

    # --- Construir gráfico Plotly ---
    fig = go.Figure()

    # Ordenar por porcentaje final descendente
    top_dnis_ordenados = sorted(
        top_dnis,
        key=lambda d: series[d][-1][1] if series[d][-1][1] is not None else 0,
        reverse=True,
    )

    for dni in top_dnis_ordenados:
        m    = meta[dni]
        pts  = series[dni]
        xs   = [ts for ts, _ in pts]
        ys   = [pct for _, pct in pts]
        ultimo_pct = ys[-1] if ys else 0

        fig.add_trace(go.Scatter(
            x=xs,
            y=ys,
            mode="lines+markers",
            name=f"{m['nombre']} ({ultimo_pct:.2f}%)",
            hovertemplate=(
                f"<b>{m['nombre']}</b><br>"
                f"{m['partido']}<br>"
                "%{y:.3f}%<br>"
                "%{x|%Y-%m-%d %H:%M}"
                "<extra></extra>"
            ),
            line=dict(width=2),
            marker=dict(size=6),
        ))

    fig.update_layout(
        title=dict(
            text="Evolución de la proyección electoral",
            font=dict(size=20),
        ),
        xaxis=dict(
            title="Fecha / hora del snapshot",
            tickformat="%d-%b %H:%M",
            showgrid=True,
            gridcolor="#eeeeee",
        ),
        yaxis=dict(
            title="Porcentaje proyectado (%)",
            showgrid=True,
            gridcolor="#eeeeee",
            ticksuffix="%",
        ),
        hovermode="x unified",
        legend=dict(
            orientation="v",
            x=1.02,
            y=1,
            xanchor="left",
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=650,
    )

    fig.write_html(args.output, include_plotlyjs="cdn")
    print(f"\nGrafico guardado -> {args.output}")


if __name__ == "__main__":
    main()
