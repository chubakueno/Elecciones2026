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
import base64
import csv
import glob
import gzip
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


def load_votos_por_distrito(path: str) -> dict[str, dict[str, int]]:
    """ubigeo → {dni → votos} para ambito nacional."""
    from collections import defaultdict
    votos: dict = defaultdict(lambda: defaultdict(int))
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("error"):
                    continue
                if row.get("id_ambito_geografico", "1") != "1":
                    continue
                ub  = row.get("ubigeo_distrito", "").zfill(6)
                dni = row.get("dniCandidato", "").strip()
                try:
                    votos[ub][dni] += int(float(row.get("totalVotosValidos", 0) or 0))
                except (ValueError, TypeError):
                    pass
    except FileNotFoundError:
        pass
    return dict(votos)


def ganador_distrito(votos: dict[str, int]) -> tuple[str, str, float]:
    if not votos:
        return "", "Sin datos", 0.0
    total = sum(votos.values())
    if total == 0:
        return "", "Sin datos", 0.0
    dni_win = max(votos, key=lambda d: votos[d])
    pct = votos[dni_win] / total * 100
    return dni_win, NOMBRES_CORTOS.get(dni_win, dni_win[:8]), pct


def load_actas_pct_global(path: str) -> float:
    """Calcula el % global de actas contabilizadas: sum(contabilizadas)/sum(totalActas)*100."""
    total = cont = 0
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    total += int(float(row.get("totalActas", 0) or 0))
                    cont  += int(float(row.get("contabilizadas", 0) or 0))
                except (ValueError, TypeError):
                    pass
    except FileNotFoundError:
        pass
    return round(cont / total * 100, 2) if total else 0.0


# ---------------------------------------------------------------------------
# Snapshots disponibles
# ---------------------------------------------------------------------------

def encontrar_timestamps(solo_peru: bool = False, solo_extranjero: bool = False) -> list[str]:
    if solo_peru:
        patron = re.compile(r"imputaciones_peru_(\d{8}_\d{4})\.csv")
        return sorted(
            m.group(1)
            for f in glob.glob("data/imputaciones_peru_*.csv")
            if (m := patron.match(os.path.basename(f)))
        )
    if solo_extranjero:
        patron = re.compile(r"imputaciones_extranjero_(\d{8}_\d{4})\.csv")
        return sorted(
            m.group(1)
            for f in glob.glob("data/imputaciones_extranjero_*.csv")
            if (m := patron.match(os.path.basename(f)))
        )
    patron = re.compile(r"imputaciones_(\d{8}_\d{4})\.csv")
    return sorted(
        m.group(1)
        for f in glob.glob("data/imputaciones_*.csv")
        if (m := patron.match(os.path.basename(f)))
    )


def ts_to_label(ts: str) -> str:
    return datetime.strptime(ts, "%Y%m%d_%H%M").strftime("%d-%b %H:%M")

def ts_to_iso(ts: str) -> str:
    return datetime.strptime(ts, "%Y%m%d_%H%M").strftime("%Y-%m-%dT%H:%M:00")

def ts_to_unix(ts: str) -> int:
    return int(datetime.strptime(ts, "%Y%m%d_%H%M").timestamp())


# ---------------------------------------------------------------------------
# Datos por snapshot
# ---------------------------------------------------------------------------

def build_snapshot(ts: str, inei_to_reniec: dict, todos_ubigeos: set,
                   geojson_iddists: set) -> dict:
    """Devuelve {iddist: {c,p,s,r,a,d,g,t,i}} para un timestamp.
    c=color, p=pol, s=scope, r=razon, a=actas, d=donor, g=ganador(dni), t=pct, i=imp"""
    imputaciones = load_imputaciones(f"data/imputaciones_{ts}.csv")
    actas_dict   = load_actas(f"data/totales_distritos_{ts}.csv")
    votos_dist   = load_votos_por_distrito(f"data/participantes_distritos_{ts}.csv")

    data = {}
    for iddist in geojson_iddists:
        reniec = inei_to_reniec.get(iddist, iddist)

        if reniec in imputaciones:
            imp      = imputaciones[reniec]
            cat      = scope_a_categoria(imp.get("scope", ""))
            donor_ub = imp.get("donor_ubigeo", "").split("+")[0].strip().zfill(6)
            votos    = votos_dist.get(donor_ub, {})
            dni_win, _nom_win, pct_win = ganador_distrito(votos)
            data[iddist] = {
                "c": COLORES.get(cat, COLORES["sin referencia"]),
                "p": COLORES_CANDIDATOS.get(dni_win, COLOR_OTROS_POL),
                "s": imp.get("scope", ""),
                "r": imp.get("razon", ""),
                "a": imp.get("actas_pct", ""),
                "d": imp.get("donor_nombre", ""),
                "g": dni_win,
                "t": f"{pct_win:.1f}" if pct_win else "",
                "i": True,
            }
        elif reniec in todos_ubigeos:
            votos = votos_dist.get(reniec, {})
            dni_win, _nom_win, pct_win = ganador_distrito(votos)
            data[iddist] = {
                "c": COLORES["datos propios"],
                "p": COLORES_CANDIDATOS.get(dni_win, COLOR_OTROS_POL),
                "s": "datos propios",
                "r": "",
                "a": actas_dict.get(reniec, ""),
                "d": "",
                "g": dni_win,
                "t": f"{pct_win:.1f}" if pct_win else "",
                "i": False,
            }
        else:
            data[iddist] = {
                "c": COLORES["sin referencia"],
                "p": COLOR_OTROS_POL,
                "s": "sin poligono",
                "r": "",
                "a": "",
                "d": "",
                "g": "",
                "t": "",
                "i": False,
            }

    actas_pct = load_actas_pct_global(f"data/totales_distritos_{ts}.csv")
    return {"ts": ts, "label": ts_to_label(ts), "iso": ts_to_iso(ts), "unix": ts_to_unix(ts),
            "actas_pct": actas_pct, "data": data}


# ---------------------------------------------------------------------------
# Colores fijos por candidato (DNI como clave)
# ---------------------------------------------------------------------------
COLORES_CANDIDATOS: dict[str, str] = {}
NOMBRES_CORTOS:    dict[str, str] = {}
COLOR_OTROS_POL = "#9ca3af"

_PALETTE = [
    "#f97316", "#dc2626", "#1d4ed8", "#0d9488", "#ca8a04",
    "#7c3aed", "#0ea5e9", "#db2777", "#65a30d", "#92400e",
    "#84cc16", "#06b6d4", "#f59e0b", "#10b981", "#6366f1",
]

# Overrides manuales: DNI → color hex. Tienen prioridad sobre _PALETTE.
# Los DNIs correctos se imprimen al correr el script.
_COLOR_OVERRIDES: dict[str, str] = {
    "10001088": "#FF8000",   # Keiko Fujimori     — naranja
    "16002918": "#1a7a1a",   # Roberto Sanchez    — verde
    "07845838": "#00AEEF",   # Rafael Lopez Aliaga — celeste
    "06506278": "#FFD700",   # Jorge Nieto        — amarillo
    "09177250": "#76C442",   # Ricardo Belmont
}

# Overrides manuales: DNI → nombre a mostrar en mapa y leyenda.
_NAME_OVERRIDES: dict[str, str] = {
    "10001088": "KEIKO FUJIMORI",
    "16002918": "ROBERTO SANCHEZ",
    "07845838": "RAFAEL LOPEZ ALIAGA",
    "07552706": "JORGE NIETO",
    "09177250": "RICARDO BELMONT",
}

def build_candidatos_meta(path: str, top_n: int = 12) -> None:
    """Puebla COLORES_CANDIDATOS y NOMBRES_CORTOS desde el CSV de participantes."""
    from collections import defaultdict
    totales: dict[str, int] = defaultdict(int)
    nombres: dict[str, str] = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("error"):
                    continue
                dni = row.get("dniCandidato", "").strip()
                nom = row.get("nombreCandidato", "").strip()
                if not dni:
                    continue
                nombres[dni] = nom
                try:
                    totales[dni] += int(float(row.get("totalVotosValidos", 0) or 0))
                except (ValueError, TypeError):
                    pass
    except FileNotFoundError:
        return

    todos = sorted(totales, key=lambda d: -totales[d])
    top   = todos[:top_n]
    COLORES_CANDIDATOS.clear()
    NOMBRES_CORTOS.clear()
    palette_idx = 0
    for dni in todos:
        if dni in _COLOR_OVERRIDES:
            COLORES_CANDIDATOS[dni] = _COLOR_OVERRIDES[dni]
        else:
            COLORES_CANDIDATOS[dni] = _PALETTE[palette_idx % len(_PALETTE)]
            palette_idx += 1
        if dni in _NAME_OVERRIDES:
            NOMBRES_CORTOS[dni] = _NAME_OVERRIDES[dni]
        else:
            NOMBRES_CORTOS[dni] = nombres.get(dni, dni)
    print(f"  {len(top)} candidatos con color propio:"
          + "".join(f"\n    {NOMBRES_CORTOS[d]} ({totales[d]:,})  {COLORES_CANDIDATOS[d]}" for d in top))


# ---------------------------------------------------------------------------
# Datos para el gráfico Plotly
# ---------------------------------------------------------------------------

def build_chart_traces(timestamps: list[str], top_n: int | None,
                       sufijo: str = "") -> list[dict]:
    if not timestamps:
        return []

    # Meta del ultimo snapshot
    last = f"data/proyeccion_final{sufijo}_{timestamps[-1]}.csv"
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
    xs = [ts_to_iso(ts) for ts in timestamps]
    series_pct:   dict[str, list] = {dni: [] for dni in top_dnis}
    series_votos: dict[str, list] = {dni: [] for dni in top_dnis}

    for ts in timestamps:
        path = f"data/proyeccion_final{sufijo}_{ts}.csv"
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

SCOPE_LABELS = {
    "todos":      "Todos",
    "peru":       "Per\u00fa",
    "extranjero": "PEX",
}


def generate_html(slim_geojson: dict, snapshots: list[dict],
                  all_traces: dict[str, list[dict]],
                  pol_order: list[str] | None = None) -> str:
    """all_traces: dict con claves "todos", "peru", "extranjero" (cualquier subconjunto)."""
    def gz_b64(obj) -> str:
        raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return base64.b64encode(gzip.compress(raw, compresslevel=9)).decode("ascii")

    geojson_gz    = gz_b64(slim_geojson)
    snapshots_gz  = gz_b64(snapshots)
    all_traces_gz = gz_b64(all_traces)

    # Orden para la leyenda política: proyección > votos brutos
    ordered_dnis = pol_order if pol_order else list(COLORES_CANDIDATOS.keys())
    pol_order_js = json.dumps(ordered_dnis, ensure_ascii=False)

    n          = len(snapshots)
    init_idx   = n - 1
    unix_first = snapshots[0]["unix"]
    unix_last  = snapshots[-1]["unix"]
    unix_init  = snapshots[init_idx]["unix"]

    scopes        = list(all_traces.keys())
    default_scope = scopes[0]
    multi_scope   = len(scopes) > 1

    # Tabs HTML (solo si hay más de un scope)
    if multi_scope:
        tabs_html = '<div id="scope-tabs">' + "".join(
            f'<label class="scope-tab{"  scope-active" if s == default_scope else ""}">'
            f'<input type="radio" name="scope" value="{s}"'
            f'{" checked" if s == default_scope else ""}> {SCOPE_LABELS.get(s, s)}</label>'
            for s in scopes
        ) + "</div>"
        tabs_css = """
#scope-tabs { display: flex; gap: 4px; flex-shrink: 0; }
.scope-tab {
  font-size: 12px; padding: 4px 10px; border-radius: 4px; cursor: pointer;
  border: 1px solid #ccc; color: #555; background: #f8f8f8; white-space: nowrap;
}
.scope-tab input { display: none; }
.scope-active { background: #1a1a2e; color: white; border-color: #1a1a2e; }"""
        tabs_js = f"""
const scopeTabs = document.querySelectorAll('.scope-tab');
scopeTabs.forEach(label => {{
  label.querySelector('input').addEventListener('change', e => {{
    scopeTabs.forEach(l => l.classList.remove('scope-active'));
    label.classList.add('scope-active');
    setScope(e.target.value);
  }});
}});"""
    else:
        tabs_html = ""
        tabs_css  = ""
        tabs_js   = ""

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
body {{ font-family: sans-serif; background: #f0f0f0; display: flex; flex-direction: column; }}

#header {{ background: #1a1a2e; color: white; padding: 10px 20px; flex-shrink: 0; }}
#header h1 {{ font-size: 17px; font-weight: 600; letter-spacing: 0.3px; }}

#controls {{
  background: white;
  padding: 8px 14px;
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px 14px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.1);
  flex-shrink: 0;
}}
#controls > label {{ font-size: 12px; color: #666; white-space: nowrap; }}
#slider {{ flex: 1; min-width: 120px; accent-color: #1a1a2e; cursor: pointer; }}
#ts-label {{ font-size: 14px; font-weight: 700; color: #1a1a2e; white-space: nowrap; }}
#ts-counter {{ font-size: 11px; color: #999; white-space: nowrap; }}
{tabs_css}

#tab-group {{ display: flex; gap: 4px; flex-wrap: nowrap; align-items: center; flex-shrink: 0; }}
#view-tabs {{ display: flex; gap: 4px; flex-shrink: 0; }}
.view-tab {{
  font-size: 12px; padding: 4px 10px; border-radius: 4px; cursor: pointer;
  border: 1px solid #ccc; color: #555; background: #f8f8f8; white-space: nowrap;
}}
.view-tab input {{ display: none; }}
.view-active {{ background: #1a1a2e; color: white; border-color: #1a1a2e; }}

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

/* ── Desktop: ocupa toda la pantalla sin scroll ─────────────────────────── */
@media (min-width: 769px) {{
  body {{ height: 100vh; overflow: hidden; }}
  #main {{ height: 0; }}
}}

/* ── Mobile: columna única, scroll vertical ─────────────────────────────── */
@media (max-width: 768px) {{
  #main {{ grid-template-columns: 1fr; }}
  .panel {{ height: 72vw; min-height: 320px; max-height: 500px; }}
  .panel:first-child {{ min-height: 320px; max-height: none; }}
}}
</style>
</head>
<body>

<div id="header"><h1>Dashboard Electoral &mdash; Peru 2026</h1></div>

<div id="controls">
  <div id="tab-group">
    {tabs_html}
    <div id="view-tabs">
      <label class="view-tab"><input type="radio" name="viewmode" value="imp"> Imputaciones</label>
      <label class="view-tab view-active"><input type="radio" name="viewmode" value="pol" checked> Ganador</label>
    </div>
  </div>
  <input type="range" id="slider" min="{unix_first}" max="{unix_last}" value="{unix_init}" step="60">
  <span id="ts-label"></span>
  <span id="ts-counter"></span>
  <span style="margin-left:8px;font-size:13px;color:#444;display:none;">Actas contabilizadas:</span>
  <span id="actas-pct" style="font-size:15px;font-weight:700;color:#1a1a2e;display:none;"></span>
</div>

<div id="main">
  <div class="panel">
    <div class="panel-title" id="map-panel-title">Ganador por distrito</div>
    <div id="map"></div>
  </div>
  <div class="panel">
    <div class="panel-title">Evolucion de la proyeccion electoral</div>
    <div id="chart"></div>
  </div>
</div>

<script>
async function _gz(b64) {{
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const ds = new DecompressionStream('gzip');
  const writer = ds.writable.getWriter();
  writer.write(bytes); writer.close();
  const chunks = []; const reader = ds.readable.getReader();
  while (true) {{ const {{done, value}} = await reader.read(); if (done) break; chunks.push(value); }}
  const out = new Uint8Array(chunks.reduce((n,c) => n+c.length, 0));
  let off = 0; for (const c of chunks) {{ out.set(c, off); off += c.length; }}
  return JSON.parse(new TextDecoder().decode(out));
}}
(async () => {{
const GEOJSON    = await _gz('{geojson_gz}');
const SNAPSHOTS  = await _gz('{snapshots_gz}');
const ALL_TRACES = await _gz('{all_traces_gz}');
let currentScope  = '{default_scope}';
let viewMode      = 'pol';

const POL_COLORS = {json.dumps(COLORES_CANDIDATOS, ensure_ascii=False)};
const POL_NAMES  = {json.dumps(NOMBRES_CORTOS, ensure_ascii=False)};
const POL_OTROS  = '{COLOR_OTROS_POL}';
const POL_ORDER  = {pol_order_js};

// ── Mapa ──────────────────────────────────────────────────────────────────
const isMobile = window.innerWidth <= 768;
const map = L.map('map', {{ zoomControl: true }}).setView([-9.2, -75.0], isMobile ? 5 : 6);
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  subdomains: 'abcd', maxZoom: 19,
}}).addTo(map);

// Leyenda dinamica
const IMP_LEGEND = [
  ['#a8d5a2','Datos propios'],
  ['#f4a261','Imputado — centroide'],
  ['#e63946','Imputado — prov/depto'],
  ['#9b5de5','Imputado — extranjero'],
  ['#aaaaaa','Sin referencia / sin poligono'],
];
const POL_LEGEND = POL_ORDER.slice(0, 5).map(dni => [POL_COLORS[dni], POL_NAMES[dni] || dni])
;

function legendHTML(entries, title) {{
  return `<b style="font-size:13px;display:block;margin-bottom:2px">${{title}}</b>`
    + entries.map(([c,l]) =>
        `<span style="display:inline-block;width:12px;height:12px;background:${{c}};border-radius:3px;margin-right:6px;vertical-align:middle"></span>${{l}}<br>`
      ).join('');
}}

const legend = L.control({{ position: 'bottomleft' }});
legend.onAdd = () => {{
  const div = L.DomUtil.create('div');
  const mobile = window.innerWidth <= 768;
  div.id = 'map-legend';
  div.style.cssText = `background:white;padding:${{mobile ? '5px 8px' : '10px 14px'}};border-radius:6px;box-shadow:0 2px 8px rgba(0,0,0,.25);font-size:${{mobile ? '10px' : '12px'}};line-height:${{mobile ? '1.7' : '2'}}`;
  div.innerHTML = legendHTML(POL_LEGEND, 'Ganador por distrito');
  return div;
}};
legend.addTo(map);

function updateLegend() {{
  const div = document.getElementById('map-legend');
  if (!div) return;
  if (viewMode === 'pol') {{
    div.innerHTML = legendHTML(POL_LEGEND, 'Ganador por distrito');
  }} else {{
    div.innerHTML = legendHTML(IMP_LEGEND, 'Metodo de imputacion');
  }}
}}

// ── Patron SVG para distritos imputados ──────────────────────────────────
// Se inyecta en el SVG de Leaflet la primera vez que se agrega una capa.
let hatchInjected = false;
function ensureHatchDefs() {{
  if (hatchInjected) return;
  const svgEl = document.querySelector('#map svg');
  if (!svgEl) return;
  const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
  defs.innerHTML = `
    <pattern id="imp-hatch" patternUnits="userSpaceOnUse" width="7" height="7" patternTransform="rotate(45)">
      <rect width="7" height="7" fill="transparent"/>
      <line x1="0" y1="0" x2="0" y2="7" stroke="rgba(0,0,0,0.30)" stroke-width="8"/>
    </pattern>`;
  svgEl.insertBefore(defs, svgEl.firstChild);
  hatchInjected = true;
}}
map.on('layeradd', ensureHatchDefs);

// Capa GeoJSON
let currentIdx = {init_idx};
const layerByDist = {{}};
const hatchByDist = {{}};
let geoLayer, hatchLayer;

function distData(idx, iddist) {{
  return (SNAPSHOTS[idx].data[iddist] || {{ c: '#aaaaaa', p: '#aaaaaa', s: '—', r: '', a: '', d: '', g: '', t: '', i: false }});
}}

function makeStyle(idx, iddist) {{
  const d = distData(idx, iddist);
  return {{ fillColor: viewMode === 'pol' ? d.p : d.c, color: '#555', weight: 0.4, fillOpacity: 0.75 }};
}}

function makeHatchStyle(idx, iddist) {{
  const d = distData(idx, iddist);
  const visible = viewMode === 'pol' && d.i;
  return {{ fillColor: 'url(#imp-hatch)', fillOpacity: visible ? 1 : 0, color: 'none', weight: 0 }};
}}

function makeTooltip(idx, feat) {{
  const d = distData(idx, feat.properties.id);
  const p = feat.properties;
  const header = `<b>${{p.dist}}</b><br><span style="color:#777">${{p.prov}} — ${{p.dep}}</span><br>`;
  if (viewMode === 'pol') {{
    return header
      + `<b>Ganador:</b> ${{(d.g ? (POL_NAMES[d.g] || d.g) : '—')}}<br>`
      + (d.t ? `<b>% del total:</b> ${{d.t}}%<br>` : '')
      + `<b>Actas:</b> ${{d.a !== '' ? d.a + '%' : '—'}}`
      + (d.i ? `<br><i style="color:#888">via donor: ${{d.d}}</i>` : '');
  }}
  return header
    + `<b>Metodo:</b> ${{d.s || '—'}}<br>`
    + `<b>Actas:</b> ${{d.a !== '' ? d.a + '%' : '—'}}`
    + (d.d ? `<br><b>Donor:</b> ${{d.d}}` : '')
    + (d.r ? `<br><b>Razon:</b> ${{d.r}}` : '');
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

// Capa overlay de hatch (encima de geoLayer)
hatchLayer = L.geoJSON(GEOJSON, {{
  style: feat => makeHatchStyle(currentIdx, feat.properties.id),
  onEachFeature(feat, layer) {{
    hatchByDist[feat.properties.id] = layer;
    // Redirigir eventos al layerByDist correspondiente para que el tooltip funcione
    layer.on('mouseover', () => {{
      const base = layerByDist[feat.properties.id];
      if (base) base.fire('mouseover');
    }});
    layer.on('mouseout', () => {{
      const base = layerByDist[feat.properties.id];
      if (base) base.fire('mouseout');
    }});
  }},
}}).addTo(map);

function updateMap(idx, prevIdx = null) {{
  const forceAll = prevIdx === null;
  for (const [iddist, layer] of Object.entries(layerByDist)) {{
    const d    = distData(idx, iddist);
    const dOld = forceAll ? null : distData(prevIdx, iddist);
    const styleChanged = forceAll || (viewMode === 'pol' ? d.p !== dOld.p : d.c !== dOld.c);
    if (styleChanged) layer.setStyle(makeStyle(idx, iddist));
    layer.setTooltipContent(makeTooltip(idx, layer.feature));
  }}
  for (const [iddist, layer] of Object.entries(hatchByDist)) {{
    const d    = distData(idx, iddist);
    const dOld = forceAll ? null : distData(prevIdx, iddist);
    if (forceAll || d.i !== dOld.i) layer.setStyle(makeHatchStyle(idx, iddist));
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

function chartLayout() {{
  const legendY = currentScope === 'extranjero' ? 0.75 : 0.90;
  return {{
    xaxis: {{ type: 'date', showgrid: true, gridcolor: '#eee', tickangle: -30, tickfont: {{ size: 11 }}, tickformat: '%d-%b %H:%M' }},
    yaxis: {{ title: '%', showgrid: true, gridcolor: '#eee', ticksuffix: '%', tickfont: {{ size: 11 }}, autorange: true }},
    hovermode: 'x unified',
    legend: {{
      orientation: 'v',
      x: 0.99, xanchor: 'right',
      y: legendY, yanchor: 'top',
      bgcolor: 'rgba(255,255,255,0.85)',
      bordercolor: '#ddd', borderwidth: 1,
      font: {{ size: 11 }},
    }},
    plot_bgcolor: 'white', paper_bgcolor: 'white',
    margin: {{ l: 45, r: 20, t: 8, b: 60 }},
    shapes: [markerShape(SNAPSHOTS[currentIdx].iso)],
    autosize: true,
  }};
}}

function setScope(scope) {{
  currentScope = scope;
  Plotly.react('chart', ALL_TRACES[scope], chartLayout());
}}

Plotly.newPlot('chart', ALL_TRACES[currentScope], chartLayout(), {{ responsive: true }});

// ── Tabs de ambito ────────────────────────────────────────────────────────
{tabs_js}

// ── Slider ────────────────────────────────────────────────────────────────
function updateLabel(idx) {{
  document.getElementById('ts-label').textContent = SNAPSHOTS[idx].label;
  document.getElementById('ts-counter').textContent = `(${{idx + 1}} / ${{SNAPSHOTS.length}})`;
  document.getElementById('actas-pct').textContent = SNAPSHOTS[idx].actas_pct.toFixed(2) + '%';
}}

document.getElementById('slider').addEventListener('input', e => {{
  const unix = +e.target.value;
  const prevIdx = currentIdx;
  currentIdx = SNAPSHOTS.reduce((best, s, i) =>
    Math.abs(s.unix - unix) < Math.abs(SNAPSHOTS[best].unix - unix) ? i : best, 0);
  updateLabel(currentIdx);
  updateMap(currentIdx, prevIdx);
  Plotly.relayout('chart', {{ shapes: [markerShape(SNAPSHOTS[currentIdx].iso)] }});
}});

// ── Toggle imputaciones / ganador ────────────────────────────────────────
document.querySelectorAll('#view-tabs input').forEach(input => {{
  input.addEventListener('change', e => {{
    document.querySelectorAll('.view-tab').forEach(l => l.classList.remove('view-active'));
    e.target.closest('.view-tab').classList.add('view-active');
    viewMode = e.target.value;
    document.getElementById('map-panel-title').textContent =
      viewMode === 'pol' ? 'Ganador por distrito' : 'Metodo de imputacion por distrito';
    updateLegend();
    updateMap(currentIdx);
  }});
}});

// Init
updateLabel({init_idx});

// Redibuja Leaflet si el contenedor cambia de tamaño (orientacion, resize)
new ResizeObserver(() => map.invalidateSize()).observe(document.getElementById('map'));

// En mobile: reparte el espacio restante entre mapa (60%) y grafico (40%)
function ajustarAlturasMobile() {{
  if (window.innerWidth > 768) return;
  const main      = document.getElementById('main');
  const top       = main.getBoundingClientRect().top;
  const gap       = 8;   // gap entre paneles
  const padding   = 8;   // padding de #main
  const available = window.innerHeight - top - padding * 2 - gap;
  const mapPanel   = main.querySelector('.panel:first-child');
  const chartPanel = main.querySelector('.panel:last-child');
  mapPanel.style.height   = Math.round(available * 0.60) + 'px';
  chartPanel.style.height = Math.round(available * 0.40) + 'px';
}}

ajustarAlturasMobile();
window.addEventListener('resize', ajustarAlturasMobile);
}})();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Genera dashboard electoral interactivo.")
    parser.add_argument("--output", default=None,
                        help="Archivo HTML de salida (default: dashboard.html o dashboard_peru.html)")
    parser.add_argument("--top", type=int, default=5,
                        help="Mostrar solo los N candidatos con mas votos en el grafico (default: 5)")
    parser.add_argument("--solo-peru", action="store_true",
                        help="Usar proyecciones solo-peru (proyeccion_final_peru_*.csv)")
    parser.add_argument("--solo-extranjero", action="store_true",
                        help="Usar proyecciones solo-extranjero (proyeccion_final_extranjero_*.csv)")
    args = parser.parse_args()

    # Determinar modo y output
    if args.solo_peru:
        sufijo = "_peru"
    elif args.solo_extranjero:
        sufijo = "_extranjero"
    else:
        sufijo = None   # combinado

    output = args.output or (f"dashboard{sufijo}.html" if sufijo else "dashboard.html")

    # Timestamps base: siempre desde imputaciones_*.csv (mapa siempre es completo)
    timestamps = encontrar_timestamps()
    if not timestamps:
        print("No se encontraron snapshots con timestamp (imputaciones_*.csv).")
        return
    print(f"{len(timestamps)} snapshots: {', '.join(timestamps)}")

    # Construir metadatos de candidatos desde el ultimo snapshot
    print("Construyendo metadatos de candidatos...")
    build_candidatos_meta(f"data/participantes_distritos_{timestamps[-1]}.csv")

    # Orden de candidatos segun ultima proyeccion
    proy_order: list[str] = []
    proy_path = f"data/proyeccion_final_{timestamps[-1]}.csv"
    if os.path.exists(proy_path):
        with open(proy_path, newline="", encoding="utf-8-sig") as f:
            rows_proy = sorted(csv.DictReader(f),
                               key=lambda r: float(r.get("votos_proyectados", 0) or 0),
                               reverse=True)
        proy_order = [r["dniCandidato"].strip() for r in rows_proy
                      if r.get("dniCandidato", "").strip() in COLORES_CANDIDATOS]

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

    # Snapshots (el mapa siempre usa imputaciones completas)
    print("Procesando snapshots...")
    snapshots = []
    for ts in timestamps:
        print(f"  {ts}")
        snapshots.append(build_snapshot(ts, inei_to_reniec, todos_ubigeos, geojson_iddists))

    # Trazas del grafico
    print("Construyendo trazas del grafico...")
    if sufijo is not None:
        # Modo único (--solo-peru o --solo-extranjero)
        all_traces = {"todos": build_chart_traces(timestamps, args.top, sufijo=sufijo)}
        print(f"  {len(all_traces['todos'])} candidatos ({sufijo})")
    else:
        # Modo combinado: los tres ámbitos
        all_traces = {}
        for key, suf in [("todos", ""), ("peru", "_peru"), ("extranjero", "_extranjero")]:
            top_n = 8 if key == "extranjero" else args.top
            traces = build_chart_traces(timestamps, top_n, sufijo=suf)
            if traces:
                all_traces[key] = traces
                print(f"  {len(traces)} candidatos ({key})")
            else:
                print(f"  [warn] Sin datos para ambito '{key}', omitido del dashboard")

    if not all_traces:
        print("Sin trazas disponibles, abortando.")
        return

    # HTML
    print(f"Generando {output}...")
    html = generate_html(slim_geojson, snapshots, all_traces, pol_order=proy_order)
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)

    size_mb = os.path.getsize(output) / 1024 / 1024
    print(f"Dashboard guardado -> {output}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
