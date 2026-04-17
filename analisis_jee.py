"""
analisis_jee.py
---------------
Corre proyectar_resultados.py con --actas-jee de 0 a 100 (paso 1) y grafica
la diferencia de votos proyectados entre JUNTOS POR EL PERU y RENOVACION POPULAR.

Uso:
    python analisis_jee.py
    python analisis_jee.py --timestamp 20260416_1807 --paso 2
"""

import argparse
import csv
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


BUSCAR = {
    "juntos":     "JUNTOS POR EL",
    "renovacion": "RENOVACI",
}


def leer_proyeccion(ts: str) -> dict[str, float]:
    path = Path(f"data/proyeccion_final_{ts}.csv")
    resultado = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            partido = row.get("nombreAgrupacionPolitica", "").upper()
            votos   = float(row.get("votos_proyectados", 0) or 0)
            for clave, patron in BUSCAR.items():
                if patron in partido:
                    resultado[clave] = votos
                    break
    return resultado


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timestamp", default="20260416_1807")
    parser.add_argument("--n-donors",  type=int, default=3)
    parser.add_argument("--paso",      type=int, default=20,
                        help="Incremento de --actas-jee (default: 20)")
    args = parser.parse_args()

    valores_x   = list(range(0, 101, args.paso))
    diferencias = []
    v_juntos_l  = []
    v_renov_l   = []

    total = len(valores_x)
    for i, x in enumerate(valores_x):
        print(f"[{i+1}/{total}] actas-jee={x}%", end=" ... ", flush=True)
        subprocess.run(
            [sys.executable, "proyectar_resultados.py",
             "--actas-jee",  str(x),
             "--n-donors",   str(args.n_donors),
             "--timestamp",  args.timestamp],
            capture_output=True,
        )
        votos = leer_proyeccion(args.timestamp)
        v_j = votos.get("juntos",     0)
        v_r = votos.get("renovacion", 0)
        dif = v_j - v_r
        v_juntos_l.append(v_j)
        v_renov_l.append(v_r)
        diferencias.append(dif)
        print(f"Juntos={v_j:,.0f}  Renovacion={v_r:,.0f}  dif={dif:+,.0f}")

    # ── Grafico ──────────────────────────────────────────────────────────────
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    fig.suptitle(
        f"Sensibilidad al % de actas JEE anuladas\n(timestamp {args.timestamp}, n-donors={args.n_donors})",
        fontsize=13,
    )

    # Panel superior: votos absolutos de cada candidato
    ax1.plot(valores_x, [v/1e6 for v in v_juntos_l],  label="Juntos por el Perú",  color="#e63946")
    ax1.plot(valores_x, [v/1e6 for v in v_renov_l],   label="Renovación Popular",  color="#1d3557")
    ax1.set_ylabel("Votos proyectados (millones)")
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.2f}M"))
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Panel inferior: diferencia (Juntos - Renovación)
    colores = ["#e63946" if d > 0 else "#1d3557" for d in diferencias]
    ax2.bar(valores_x, diferencias, width=args.paso * 0.8, color=colores, alpha=0.8)
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_xlabel("% de actas enviadasJee anuladas")
    ax2.set_ylabel("Diferencia de votos\n(Juntos − Renovación)")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+,.0f}"))
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    out_png = f"analisis_jee_{args.timestamp}.png"
    plt.savefig(out_png, dpi=150)
    print(f"\nGrafico guardado -> {out_png}")
    plt.show()


if __name__ == "__main__":
    main()
