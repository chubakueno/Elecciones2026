"""
historico_jerarquico.py
-----------------------
Genera un reporte HTML de comparación jerárquica para todos los timestamps
donde existan tanto totales_distritos_TS.csv como totales_jerarquico_TS.csv
en el directorio data/.

El HTML incluye un selector de timestamp para navegar entre snapshots.

Uso:
    python historico_jerarquico.py
    python historico_jerarquico.py --campo contabilizadas
    python historico_jerarquico.py --output reporte.html
"""

import argparse
import base64
import glob
import gzip
import os
import re
from collections import defaultdict


from comparar_jerarquico import (
    CAMPOS_NUMERICOS,
    load_distritos,
    load_jerarquico,
    comparar,
    CSS,
)

DATA_DIR  = "data"
PAT_DIST  = re.compile(r"totales_distritos_(\d{8}_\d{4})\.csv")
PAT_HIER  = re.compile(r"totales_jerarquico_(\d{8}_\d{4})\.csv")

NIVELES = {
    "1": {"distritos": "distritos", "provincia": "provincia", "provincias": "provincias",
          "departamento": "departamento", "departamentos": "departamentos"},
    "2": {"distritos": "ciudades",  "provincia": "pais",      "provincias": "paises",
          "departamento": "continente", "departamentos": "continentes"},
}


def encontrar_timestamps() -> list[str]:
    dist_ts = {
        PAT_DIST.match(os.path.basename(f)).group(1)
        for f in glob.glob(os.path.join(DATA_DIR, "totales_distritos_*.csv"))
        if PAT_DIST.match(os.path.basename(f))
    }
    hier_ts = {
        PAT_HIER.match(os.path.basename(f)).group(1)
        for f in glob.glob(os.path.join(DATA_DIR, "totales_jerarquico_*.csv"))
        if PAT_HIER.match(os.path.basename(f))
    }
    return sorted(dist_ts & hier_ts)


def datos_para_ts(ts: str, campo: str,
                   solo_discrepancias: bool, top: int | None) -> list[dict]:
    """
    Devuelve los datos del snapshot como lista de secciones (dicts),
    sin generar HTML. El HTML se renderiza en el navegador desde JSON.

    Sección tipo 'table': {type, title, total_diff, rows: [[ub, nombre, suma, api, diff], ...]}
    Sección tipo 'chain': {type, title, rows: [[etiqueta, valor, diff|null], ...]}
    """
    dist_path = os.path.join(DATA_DIR, f"totales_distritos_{ts}.csv")
    hier_path = os.path.join(DATA_DIR, f"totales_jerarquico_{ts}.csv")

    prov_sumas, dep_sumas = load_distritos(dist_path)
    prov_api, dep_api, cabeceras = load_jerarquico(hier_path)

    secciones = []

    # Provincias
    for amb, etq in [("1", "PERU"), ("2", "EXTERIOR")]:
        if prov_api[amb]:
            niv = NIVELES[amb]
            filas = comparar(
                prov_sumas[amb], prov_api[amb],
                lambda v: f"{v['nombre_provincia']}, {v['nombre_departamento']}",
                campo, solo_discrepancias, top,
            )
            secciones.append({
                "type":       "table",
                "title":      f"{niv['provincias'].capitalize()} {etq}: suma de {niv['distritos']} vs API {niv['provincia']}",
                "total_diff": sum(f["diff"] for f in filas),
                "rows":       [[f["ubigeo"], f["nombre"], f["suma_distritos"], f["api"], f["diff"]] for f in filas],
            })

    # Departamentos
    for amb, etq in [("1", "PERU"), ("2", "EXTERIOR")]:
        if dep_api[amb] and prov_api[amb]:
            niv = NIVELES[amb]
            prov_by_dep: dict = defaultdict(lambda: defaultdict(int))
            for ub_prov, vals in prov_api[amb].items():
                ub_dep = vals.get("ubigeo_departamento", ub_prov[:4].ljust(6, "0")).zfill(6)
                for c in CAMPOS_NUMERICOS:
                    prov_by_dep[ub_dep][c] += vals.get(c, 0)
            filas = comparar(
                prov_by_dep, dep_api[amb],
                lambda v: v["nombre_departamento"],
                campo, solo_discrepancias, top,
            )
            secciones.append({
                "type":       "table",
                "title":      f"{niv['departamentos'].capitalize()} {etq}: suma de {niv['provincias']} API vs API {niv['departamento']}",
                "total_diff": sum(f["diff"] for f in filas),
                "rows":       [[f["ubigeo"], f["nombre"], f["suma_distritos"], f["api"], f["diff"]] for f in filas],
            })

    # Cadena jerárquica
    chain_rows = []

    def cadena_ambito(amb, etq_ambito, clave_cab):
        dep_a  = dep_api[amb]
        prov_a = prov_api[amb]
        if not dep_a:
            return None
        niv       = NIVELES[amb]
        suma_dist = sum(dep_sumas[amb].get(ub, {}).get(campo, 0) for ub in dep_a)
        suma_prov = sum(v.get(campo, 0) for v in prov_a.values()) if prov_a else None
        suma_dep  = sum(v.get(campo, 0) for v in dep_a.values())
        cab_val   = cabeceras.get(clave_cab, {}).get(campo)

        chain_rows.append([f"— {etq_ambito} —", 0, None])
        chain_rows.append([f"Suma {niv['distritos']}", suma_dist, None])
        if suma_prov is not None:
            chain_rows.append([f"Suma API {niv['provincias']}", suma_prov, suma_prov - suma_dist])
        prev = suma_prov if suma_prov is not None else suma_dist
        chain_rows.append([f"Suma API {niv['departamentos']}", suma_dep, suma_dep - prev])
        if cab_val is not None:
            chain_rows.append([f"API {clave_cab}", cab_val, cab_val - suma_dep])
        return cab_val if cab_val is not None else suma_dep

    nac = cadena_ambito("1", "Peru (ambito 1)",     "nacional")
    ext = cadena_ambito("2", "Exterior (ambito 2)", "exterior")
    tot = cabeceras.get("total", {}).get(campo)
    if tot is not None:
        ref_tot = (nac or 0) + (ext or 0) if nac is not None and ext is not None else None
        chain_rows.append(["— Gran total —", 0, None])
        if ref_tot is not None:
            chain_rows.append(["Nacional + exterior", ref_tot, None])
        chain_rows.append(["API total (eleccion)", tot,
                            tot - ref_tot if ref_tot is not None else None])

    secciones.append({"type": "chain", "title": "Cadena jerárquica", "rows": chain_rows})
    return secciones


CAMPOS_DEFAULT = ["totalVotosValidos", "enviadasJee", "pendientesJee"]

CAMPO_LABELS = {
    "totalVotosValidos": "Votos válidos",
    "enviadasJee":       "Actas enviadas JEE",
    "contabilizadas":    "Actas contabilizadas",
    "totalActas":        "Total actas",
    "pendientesJee":     "Actas pendientes",
    "totalVotosEmitidos": "Votos emitidos",
}


def generar_historico(timestamps: list[str], campos: list[str],
                       solo_discrepancias: bool, top: int | None,
                       output: str):
    import json

    # data[campo][ts] = sections
    all_data: dict[str, dict] = {c: {} for c in campos}
    for ts in timestamps:
        print(f"  Procesando {ts}...")
        for campo in campos:
            try:
                all_data[campo][ts] = datos_para_ts(ts, campo, solo_discrepancias, top)
            except Exception as e:
                print(f"  [warn] {ts} / {campo}: {e}")

    # Quitar campos sin ningún dato
    all_data = {c: d for c, d in all_data.items() if d}
    if not all_data:
        print("Sin datos para generar.")
        return

    ts_list  = sorted(next(iter(all_data.values())).keys())
    last_idx = len(ts_list) - 1

    raw     = json.dumps(all_data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    data_gz = base64.b64encode(gzip.compress(raw, compresslevel=9)).decode("ascii")

    campo_labels_js = json.dumps({c: CAMPO_LABELS.get(c, c) for c in all_data}, ensure_ascii=False)
    default_campo   = campos[0]
    fmt_last = f'{ts_list[last_idx][:4]}-{ts_list[last_idx][4:6]}-{ts_list[last_idx][6:8]} {ts_list[last_idx][9:11]}:{ts_list[last_idx][11:]}'

    js = f"""
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
const ALL_DATA    = await _gz('{data_gz}');
const CAMPO_LABELS = {campo_labels_js};
var currentCampo = '{default_campo}';
var TS  = Object.keys(ALL_DATA[currentCampo]);
var idx = {last_idx};

function fmtTs(ts) {{
  return ts.slice(0,4)+'-'+ts.slice(4,6)+'-'+ts.slice(6,8)+' '+ts.slice(9,11)+':'+ts.slice(11);
}}
function fmtNum(n) {{
  if (n === null || n === undefined) return '';
  return n.toLocaleString('es-PE');
}}
function rowCls(diff) {{
  if (diff === 0) return 'ok';
  if (Math.abs(diff) < 100) return 'warn';
  return 'bad';
}}
function chainCls(diff) {{
  if (diff === null) return '';
  if (diff === 0) return 'ok';
  return Math.abs(diff) > 10 ? 'bad' : 'warn';
}}

function renderSections(sections) {{
  var html = '';
  sections.forEach(function(sec) {{
    if (sec.type === 'table') {{
      var diff_class = sec.total_diff !== 0 ? ' diff' : '';
      html += '<h2>' + sec.title + '</h2>';
      html += '<p class="total' + diff_class + '">Diferencia total: ' + fmtNum(sec.total_diff) + '</p>';
      html += '<table style="table-layout:fixed;width:100%"><colgroup>'
            + '<col style="width:7em"><col style="width:auto"><col style="width:10em"><col style="width:10em"><col style="width:8em">'
            + '</colgroup><thead><tr><th>Ubigeo</th><th>Nombre</th><th>Suma distritos</th><th>API</th><th>Diff</th></tr></thead><tbody>';
      sec.rows.forEach(function(r) {{
        var d = r[4];
        html += '<tr class="' + rowCls(d) + '"><td>' + r[0] + '</td><td>' + r[1] + '</td><td class="num">' + fmtNum(r[2]) + '</td><td class="num">' + fmtNum(r[3]) + '</td><td class="num">' + fmtNum(d) + '</td></tr>';
      }});
      html += '</tbody></table>';
    }} else if (sec.type === 'chain') {{
      html += '<h2>' + sec.title + '</h2>';
      html += '<table style="table-layout:fixed;width:100%"><colgroup>'
            + '<col style="width:auto"><col style="width:10em"><col style="width:8em">'
            + '</colgroup><thead><tr><th>Nivel</th><th>Valor</th><th>Diferencia</th></tr></thead><tbody>';
      sec.rows.forEach(function(r) {{
        var label = r[0], val = r[1], diff = r[2];
        var is_header = label.startsWith('\u2014');
        var cls = is_header ? 'section-header' : chainCls(diff);
        var val_str  = is_header ? '' : fmtNum(val);
        var diff_str = diff !== null ? fmtNum(diff) : '';
        html += '<tr class="' + cls + '"><td>' + label + '</td><td class="num">' + val_str + '</td><td class="num">' + diff_str + '</td></tr>';
      }});
      html += '</tbody></table>';
    }}
  }});
  return html;
}}

function render() {{
  var ts = TS[idx];
  var sections = ALL_DATA[currentCampo][ts];
  document.getElementById('content').innerHTML = sections ? renderSections(sections) : '<p style="color:#aaa">Sin datos</p>';
  document.getElementById('ts-slider').value = idx;
  document.getElementById('ts-label').textContent = fmtTs(ts);
}}

function setIdx(i) {{
  if (i < 0 || i >= TS.length) return;
  idx = i;
  render();
}}

function setCampo(c) {{
  currentCampo = c;
  TS  = Object.keys(ALL_DATA[c]);
  idx = Math.min(idx, TS.length - 1);
  document.querySelectorAll('.campo-tab').forEach(function(btn) {{
    btn.classList.toggle('active', btn.dataset.campo === c);
  }});
  document.getElementById('ts-slider').max = TS.length - 1;
  render();
}}

document.querySelectorAll('.campo-tab').forEach(function(btn) {{
  btn.addEventListener('click', function() {{ setCampo(this.dataset.campo); }});
}});
document.getElementById('ts-slider').addEventListener('input', function() {{
  setIdx(parseInt(this.value));
}});
document.addEventListener('keydown', function(e) {{
  if (e.target.tagName === 'INPUT') return;
  if (e.key === 'ArrowLeft')  setIdx(idx - 1);
  if (e.key === 'ArrowRight') setIdx(idx + 1);
}});

render();
}})();
"""

    campo_tabs_html = "".join(
        f'<button class="campo-tab{" active" if c == default_campo else ""}" data-campo="{c}">'
        f'{CAMPO_LABELS.get(c, c)}</button>'
        for c in all_data
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Histórico comparación jerárquica</title>
  <style>
{CSS}
.selector-bar {{ position: sticky; top: 0; z-index: 100;
                 background: #0f1117; border-bottom: 1px solid #1e2130;
                 margin: 0 -24px 28px; padding: 10px 24px;
                 display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }}
.selector-bar label {{ color: #aaa; font-size: 0.85rem; white-space: nowrap; }}
#ts-label {{ color: #7ec8e3; font-size: 0.9rem; font-variant-numeric: tabular-nums;
             min-width: 140px; }}
input[type=range] {{ flex: 1; min-width: 120px; accent-color: #7ec8e3; cursor: pointer; }}
input[type=range]:focus {{ outline: none; }}
.campo-tabs {{ display: flex; gap: 6px; flex-shrink: 0; }}
.campo-tab {{
  font-size: 0.82rem; padding: 4px 12px; border-radius: 4px; cursor: pointer;
  border: 1px solid #444; color: #aaa; background: #1a1d2e;
}}
.campo-tab.active {{ background: #7ec8e3; color: #0f1117; border-color: #7ec8e3; font-weight: 600; }}
  </style>
</head>
<body>
  <h1>Histórico comparación jerárquica</h1>
  <p class="subtitle">{len(ts_list)} snapshots</p>

  <div class="selector-bar">
    <div class="campo-tabs">{campo_tabs_html}</div>
    <label>Snapshot:</label>
    <input type="range" id="ts-slider" min="0" max="{last_idx}" value="{last_idx}" step="1">
    <span id="ts-label">{fmt_last}</span>
  </div>

  <div id="content"></div>

  <script>{js}</script>
</body>
</html>
"""
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML generado -> {output}  ({len(ts_list)} snapshots, {len(all_data)} campos)")


def main():
    parser = argparse.ArgumentParser(
        description="Reporte histórico de comparación jerárquica.")
    parser.add_argument("--campos",   nargs="+", default=CAMPOS_DEFAULT,
                        choices=CAMPOS_NUMERICOS,
                        help=f"Campos a incluir (default: {' '.join(CAMPOS_DEFAULT)})")
    parser.add_argument("--top",      type=int, default=None)
    parser.add_argument("--solo-discrepancias", action="store_true")
    parser.add_argument("--output",   default="historico_jerarquico.html")
    parser.add_argument("--abrir",    action="store_true",
                        help="Abrir el resultado en el navegador")
    args = parser.parse_args()

    timestamps = encontrar_timestamps()
    if not timestamps:
        print(f"No se encontraron pares (totales_distritos_TS + totales_jerarquico_TS) en {DATA_DIR}/")
        return

    print(f"{len(timestamps)} snapshot(s) con ambos CSVs: {', '.join(timestamps)}")
    generar_historico(timestamps, args.campos, args.solo_discrepancias, args.top, args.output)

    if args.abrir:
        import webbrowser
        webbrowser.open(os.path.abspath(args.output))


if __name__ == "__main__":
    main()
