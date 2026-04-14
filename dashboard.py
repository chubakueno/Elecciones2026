"""
dashboard.py
------------
Genera dashboard.html: mapa de imputaciones + grafico de evolucion
en una sola pagina con slider para navegar entre snapshots.

Uso:
    python dashboard.py
    python dashboard.py --top 5
    python dashboard.py --output mi_dashboard.html
"""

import argparse
import csv
import glob
import json
import os
import re
from datetime import datetime

# ---------------------------------------------------------------------------
# Archivos
# ---------------------------------------------------------------------------
GEOJSON_FILE    = "peru_distrital.geojson"
CENTROIDES_FILE = "ubigeo_centroides.csv"
UBIGEOS_FILE    = "ubigeos_completo.csv"
OUTPUT_HTML     = "dashboard.html"

COLORES = {
    "datos propios":            "#a8d5a2",
    "centroide":                "#f4a261",
    "provincia":                "#e63946",
    "departamento":             "#e63946",
    "perfil global extranjero": "#9b5de5",
    "sin referencia":           "#aaaaaa",
}


# ---------------------------------------------------------------------------
# Helpers de carga
# ---------------------------------------------------------------------------

def scope_a_categoria(scope: str) -> str:
    s = scope.lower()
    if s.startswith("centroide"):           return "centroide"
    if "provincia"   in s:                  return "provincia"
    if "departamento" in s:                 return "departamento"
    if "global" in s or "extranjero" in s:  return "perfil global extranjero"
    return "sin referencia"


def load_inei_to_reniec(path: str) -> dict[str, str]:
    m = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                inei   = str(row["inei"]).strip().zfill(6)
                reniec = str(row["reniec"]).strip().zfill(6)
                m[inei] = reniec
    except FileNotFoundError:
        print(f"  [warn] No se encontro {path}")
    return m


def load_todos_ubigeos(path: str) -> set[str]:
    ubigeos = set()
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("id_ambito_geografico", "1") == "1":
                    ubigeos.add(row["ubigeo_distrito"].zfill(6))
    except FileNotFoundError:
        pass
    return ubigeos


def load_imputaciones(path: str) -> dict[str, dict]:
    imp = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                imp[row["ubigeo"].zfill(6)] = row
    except FileNotFoundError:
        pass
    return imp


def load_actas(path: str) -> dict[str, str]:
    actas = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                ub = row.get("ubigeo_distrito", "").zfill(6)
                actas[ub] = row.get("actasContabilizadas", "")
    except FileNotFoundError:
        pass
    return actas


# ---------------------------------------------------------------------------
# Snapshots disponibles
# ---------------------------------------------------------------------------

def encontrar_timestamps() -> list[str]:
    patron = re.compile(r"imputaciones_(\d{8}_\d{4})\.csv")
    return sorted(
        m.group(1)
        for f in glob.glob("imputaciones_*.csv")
        if (m := patron.match(os.path.basename(f)))
    )


def ts_to_label(ts: str) -> str:
    return datetime.strptime(ts, "%Y%m%d_%H%M").strftime("%d-%b %H:%M")


# ---------------------------------------------------------------------------
# Datos por snapshot
# ---------------------------------------------------------------------------

def build_snapshot(ts: str, inei_to_reniec: dict, todos_ubigeos: set,
                   geojson_iddists: set) -> dict:
    """Devuelve {iddist: {color, scope, razon, actas, donor}} para un timestamp."""
    imputaciones = load_imputaciones(f"imputaciones_{ts}.csv")
    actas_dict   = load_actas(f"totales_distritos_{ts}.csv")

    data = {}
    for iddist in geojson_iddists:
        reniec = inei_to_reniec.get(iddist, iddist)

        if reniec in imputaciones:
            imp = imputaciones[reniec]
            cat = scope_a_categoria(imp.get("scope", ""))
            data[iddist] = {
                "color": COLORES.get(cat, COLORES["sin referencia"]),
                "scope": imp.get("scope", ""),
                "razon": imp.get("razon", ""),
                "actas": imp.get("actas_pct", ""),
                "donor": imp.get("donor_nombre", ""),
            }
        elif reniec in todos_ubigeos:
            data[iddist] = {
                "color": COLORES["datos propios"],
                "scope": "datos propios",
                "razon": "",
                "actas": actas_dict.get(reniec, ""),
                "donor": "",
            }
        else:
            data[iddist] = {
                "color": COLORES["sin referencia"],
                "scope": "sin poligono",
                "razon": "",
                "actas": "",
                "donor": "",
            }

    return {"ts": ts, "label": ts_to_label(ts), "data": data}


# ---------------------------------------------------------------------------
# Colores fijos por candidato (DNI como clave)
# ---------------------------------------------------------------------------
COLORES_CANDIDATOS: dict[str, str] = {
    "10001088": "#FF8000",   # Keiko Fujimori     — naranja
    "16002918": "#1a7a1a",   # Roberto Sanchez    — verde
    "07845838": "#00AEEF",   # Rafael Lopez Aliaga — celeste
    "06506278": "#FFD700",   # Jorge Nieto        — amarillo
    "09177250": "#76C442",   # Ricardo Belmont    — verde claro
}


# ---------------------------------------------------------------------------
# Datos para el gráfico Plotly
# ---------------------------------------------------------------------------

def build_chart_traces(timestamps: list[str], top_n: int | None) -> list[dict]:
    if not timestamps:
        return []

    # Meta del ultimo snapshot
    last = f"proyeccion_final_{timestamps[-1]}.csv"
    if not os.path.exists(last):
        return []

    meta = {}
    with open(last, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dni = row.get("dniCandidato", "").strip()
            meta[dni] = {
                "nombre":  row.get("nombreCandidato", dni),
                "partido": row.get("nombreAgrupacionPolitica", ""),
                "votos":   int(float(row.get("votos_proyectados", 0) or 0)),
            }

    top_dnis = sorted(meta, key=lambda d: meta[d]["votos"], reverse=True)
    if top_n:
        top_dnis = top_dnis[:top_n]

    # Series temporales: porcentaje y votos absolutos
    xs = [ts_to_label(ts) for ts in timestamps]
    series_pct:   dict[str, list] = {dni: [] for dni in top_dnis}
    series_votos: dict[str, list] = {dni: [] for dni in top_dnis}

    for ts in timestamps:
        path = f"proyeccion_final_{ts}.csv"
        snap_pct   = {}
        snap_votos = {}
        if os.path.exists(path):
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    dni = row.get("dniCandidato", "").strip()
                    snap_pct[dni]   = float(row.get("porcentaje_proyectado", 0) or 0)
                    snap_votos[dni] = int(float(row.get("votos_proyectados", 0) or 0))
        for dni in top_dnis:
            series_pct[dni].append(snap_pct.get(dni, None))
            series_votos[dni].append(snap_votos.get(dni, None))

    # Ordenar por porcentaje final descendente
    top_dnis_sorted = sorted(
        top_dnis,
        key=lambda d: (series_pct[d][-1] or 0),
        reverse=True,
    )

    traces = []
    for dni in top_dnis_sorted:
        m        = meta[dni]
        last_pct   = series_pct[dni][-1]   or 0
        last_votos = series_votos[dni][-1] or 0
        color = COLORES_CANDIDATOS.get(dni)
        trace: dict = {
            "type": "scatter",
            "mode": "lines+markers",
            "name": f"{m['nombre']} ({last_pct:.2f}% · {last_votos:,})",
            "x": xs,
            "y": series_pct[dni],
            "customdata": series_votos[dni],
            "hovertemplate": (
                f"<b>{m['nombre']}</b><br>"
                f"{m['partido']}<br>"
                "%{y:.3f}%  ·  %{customdata:,} votos"
                "<extra></extra>"
            ),
            "line":   {"width": 2},
            "marker": {"size": 5},
        }
        if color:
            trace["line"]["color"]   = color
            trace["marker"]["color"] = color
        traces.append(trace)

    return traces


# ---------------------------------------------------------------------------
# Generar HTML
# ---------------------------------------------------------------------------

def generate_html(slim_geojson: dict, snapshots: list[dict],
                  chart_traces: list[dict]) -> str:
    geojson_js   = json.dumps(slim_geojson,  ensure_ascii=False, separators=(",", ":"))
    snapshots_js = json.dumps(snapshots,     ensure_ascii=False, separators=(",", ":"))
    traces_js    = json.dumps(chart_traces,  ensure_ascii=False, separators=(",", ":"))
    n            = len(snapshots)
    init_idx     = n - 1

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard Electoral Peru 2026</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: sans-serif; background: #f0f0f0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }}

#header {{ background: #1a1a2e; color: white; padding: 10px 20px; flex-shrink: 0; }}
#header h1 {{ font-size: 17px; font-weight: 600; letter-spacing: 0.3px; }}

#controls {{
  background: white;
  padding: 8px 20px;
  display: flex;
  align-items: center;
  gap: 14px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.1);
  flex-shrink: 0;
}}
#controls label {{ font-size: 12px; color: #666; white-space: nowrap; }}
#slider {{ flex: 1; accent-color: #1a1a2e; cursor: pointer; }}
#ts-label {{ font-size: 14px; font-weight: 700; color: #1a1a2e; min-width: 100px; }}
#ts-counter {{ font-size: 11px; color: #999; white-space: nowrap; }}

#main {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  padding: 8px;
  flex: 1;
  min-height: 0;
}}

.panel {{
  background: white;
  border-radius: 6px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.1);
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-height: 0;
}}

.panel-title {{
  padding: 7px 14px;
  font-size: 12px;
  font-weight: 600;
  color: #555;
  border-bottom: 1px solid #eee;
  flex-shrink: 0;
}}

#map {{ flex: 1; min-height: 0; }}
#chart {{ flex: 1; min-height: 0; }}
</style>
</head>
<body>

<div id="header"><h1>Dashboard Electoral &mdash; Peru 2026</h1></div>

<div id="controls">
  <label>Snapshot:</label>
  <input type="range" id="slider" min="0" max="{n - 1}" value="{init_idx}" step="1">
  <span id="ts-label"></span>
  <span id="ts-counter"></span>
</div>

<div id="main">
  <div class="panel">
    <div class="panel-title">Metodo de imputacion por distrito</div>
    <div id="map"></div>
  </div>
  <div class="panel">
    <div class="panel-title">Evolucion de la proyeccion electoral</div>
    <div id="chart"></div>
  </div>
</div>

<script>
const GEOJSON   = {geojson_js};
const SNAPSHOTS = {snapshots_js};
const TRACES    = {traces_js};

// ── Mapa ──────────────────────────────────────────────────────────────────
const map = L.map('map', {{ zoomControl: true }}).setView([-9.2, -75.0], 6);
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  subdomains: 'abcd', maxZoom: 19,
}}).addTo(map);

// Leyenda fija
const legend = L.control({{ position: 'bottomleft' }});
legend.onAdd = () => {{
  const div = L.DomUtil.create('div');
  div.style.cssText = 'background:white;padding:10px 14px;border-radius:6px;box-shadow:0 2px 8px rgba(0,0,0,.25);font-size:12px;line-height:2';
  div.innerHTML = `<b style="font-size:13px;display:block;margin-bottom:2px">Metodo de imputacion</b>
    ${{[
      ['#a8d5a2','Datos propios'],
      ['#f4a261','Imputado — centroide'],
      ['#e63946','Imputado — prov/depto'],
      ['#9b5de5','Imputado — extranjero'],
      ['#aaaaaa','Sin referencia / sin poligono'],
    ].map(([c,l]) => `<span style="display:inline-block;width:12px;height:12px;background:${{c}};border-radius:3px;margin-right:6px;vertical-align:middle"></span>${{l}}<br>`).join('')}}`;
  return div;
}};
legend.addTo(map);

// Capa GeoJSON
let currentIdx = {init_idx};
const layerByDist = {{}};
let geoLayer;

function distData(idx, iddist) {{
  return (SNAPSHOTS[idx].data[iddist] || {{ color: '#aaaaaa', scope: '—', razon: '', actas: '', donor: '' }});
}}

function makeStyle(idx, iddist) {{
  return {{ fillColor: distData(idx, iddist).color, color: '#555', weight: 0.4, fillOpacity: 0.75 }};
}}

function makeTooltip(idx, feat) {{
  const d = distData(idx, feat.properties.id);
  const p = feat.properties;
  return `<b>${{p.dist}}</b><br><span style="color:#777">${{p.prov}} — ${{p.dep}}</span><br>
    <b>Metodo:</b> ${{d.scope || '—'}}<br>
    <b>Actas:</b> ${{d.actas !== '' ? d.actas + '%' : '—'}}`
    + (d.donor ? `<br><b>Donor:</b> ${{d.donor}}` : '')
    + (d.razon ? `<br><b>Razon:</b> ${{d.razon}}` : '');
}}

geoLayer = L.geoJSON(GEOJSON, {{
  style: feat => makeStyle(currentIdx, feat.properties.id),
  onEachFeature(feat, layer) {{
    layerByDist[feat.properties.id] = layer;
    layer.bindTooltip(makeTooltip(currentIdx, feat), {{ sticky: true, maxWidth: 260 }});
    layer.on('mouseover', () => layer.setStyle({{ weight: 1.5, color: '#000', fillOpacity: 0.95 }}));
    layer.on('mouseout',  () => layer.setStyle(makeStyle(currentIdx, feat.properties.id)));
  }},
}}).addTo(map);

function updateMap(idx) {{
  for (const [iddist, layer] of Object.entries(layerByDist)) {{
    layer.setStyle(makeStyle(idx, iddist));
    layer.setTooltipContent(makeTooltip(idx, layer.feature));
  }}
}}

// ── Plotly ────────────────────────────────────────────────────────────────
function markerShape(label) {{
  return {{
    type: 'line', xref: 'x', yref: 'paper',
    x0: label, x1: label, y0: 0, y1: 1,
    line: {{ color: '#1a1a2e', width: 1.5, dash: 'dot' }},
  }};
}}

const initLabel = SNAPSHOTS[{init_idx}].label;

Plotly.newPlot('chart', TRACES, {{
  xaxis: {{ showgrid: true, gridcolor: '#eee', tickangle: -30, tickfont: {{ size: 11 }} }},
  yaxis: {{ title: '%', showgrid: true, gridcolor: '#eee', ticksuffix: '%', tickfont: {{ size: 11 }}, autorange: true }},
  hovermode: 'x unified',
  legend: {{ orientation: 'v', x: 1.02, y: 1, xanchor: 'left', font: {{ size: 11 }} }},
  plot_bgcolor: 'white', paper_bgcolor: 'white',
  margin: {{ l: 45, r: 170, t: 8, b: 60 }},
  shapes: [markerShape(initLabel)],
  autosize: true,
}}, {{ responsive: true }});

// ── Slider ────────────────────────────────────────────────────────────────
function updateLabel(idx) {{
  document.getElementById('ts-label').textContent = SNAPSHOTS[idx].label;
  document.getElementById('ts-counter').textContent = `(${{idx + 1}} / ${{SNAPSHOTS.length}})`;
}}

document.getElementById('slider').addEventListener('input', e => {{
  currentIdx = +e.target.value;
  updateLabel(currentIdx);
  updateMap(currentIdx);
  Plotly.relayout('chart', {{ shapes: [markerShape(SNAPSHOTS[currentIdx].label)] }});
}});

// Init
updateLabel({init_idx});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Genera dashboard electoral interactivo.")
    parser.add_argument("--output", default=OUTPUT_HTML,
                        help=f"Archivo HTML de salida (default: {OUTPUT_HTML})")
    parser.add_argument("--top", type=int, default=5,
                        help="Mostrar solo los N candidatos con mas votos en el grafico (default: 5)")
    args = parser.parse_args()

    timestamps = encontrar_timestamps()
    if not timestamps:
        print("No se encontraron snapshots con timestamp (imputaciones_*.csv).")
        return
    print(f"{len(timestamps)} snapshots: {', '.join(timestamps)}")

    # GeoJSON
    print("Cargando GeoJSON...")
    with open(GEOJSON_FILE, encoding="utf-8") as f:
        geojson = json.load(f)

    inei_to_reniec = load_inei_to_reniec(CENTROIDES_FILE)
    todos_ubigeos  = load_todos_ubigeos(UBIGEOS_FILE)

    # Slim GeoJSON: solo geometría + campos mínimos
    geojson_iddists = set()
    slim_features   = []
    for feat in geojson["features"]:
        props  = feat["properties"]
        iddist = str(props.get("IDDIST", "")).zfill(6)
        geojson_iddists.add(iddist)
        slim_features.append({
            "type": "Feature",
            "geometry": feat["geometry"],
            "properties": {
                "id":   iddist,
                "dist": props.get("NOMBDIST", ""),
                "prov": props.get("NOMBPROV", ""),
                "dep":  props.get("NOMBDEP",  ""),
            },
        })
    slim_geojson = {"type": "FeatureCollection", "features": slim_features}
    print(f"  {len(slim_features)} poligonos")

    # Snapshots
    print("Procesando snapshots...")
    snapshots = []
    for ts in timestamps:
        print(f"  {ts}")
        snapshots.append(build_snapshot(ts, inei_to_reniec, todos_ubigeos, geojson_iddists))

    # Trazas del grafico
    print("Construyendo trazas del grafico...")
    chart_traces = build_chart_traces(timestamps, args.top)
    print(f"  {len(chart_traces)} candidatos en el grafico")

    # HTML
    print(f"Generando {args.output}...")
    html = generate_html(slim_geojson, snapshots, chart_traces)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    size_mb = os.path.getsize(args.output) / 1024 / 1024
    print(f"Dashboard guardado -> {args.output}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
