"""
desglose_jee.py
---------------
Calcula los votos de Sanchez y Aliaga por componente:
  1. JNE contabilizadas  (votos duros oficiales)
  2. JEE con PDF         (votos OCR/manual/digital de votos_mesa.csv)
  3. JEE sin PDF         (mesas imputadas al distrito, con fallback geografico)
  4. Pendientes puros    (actas restantes imputadas igual)

El HTML generado muestra ambas versiones (no procesables = 0 vs imputadas al
distrito) y un anexo con el detalle de actas JEE sin PDF y no procesables.
"""

import csv, glob, json, math
from collections import defaultdict

MESAS_OBS  = "data/mesas_observadas.csv"
VOTOS_MESA = "data/votos_mesa.csv"
CENTROIDES = "ubigeo_centroides.csv"

COD_S = "10"   # Juntos por el Peru - Sanchez
COD_A = "35"   # Renovacion Popular - Aliaga
FILA_S = 16
FILA_A = 33

NO_PROC_CODES = {'X', 'Y', 'N'}
NO_PROC_LABEL = {'X': 'Extraviada', 'Y': 'Siniestrada', 'N': 'Sol. nulidad'}


# ---------- helpers ----------------------------------------------------------

def flt(v):
    try: return float(v or 0)
    except: return 0.0

def inte(v):
    try: return int(float(v or 0))
    except: return 0

def ubigeo_desde_id_acta(codigo_mesa, id_acta):
    n = len(str(int(codigo_mesa)))
    return id_acta[n:n+6]

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2-lat1); dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def load_centroides():
    c = {}
    try:
        with open(CENTROIDES, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                ub  = str(r["reniec"]).strip().zfill(6)
                lat = flt(r.get("latitude") or r.get("latitud"))
                lon = flt(r.get("longitude") or r.get("longitud"))
                if lat and lon:
                    c[ub] = (lat, lon)
    except FileNotFoundError:
        pass
    return c


# ---------- cargar datos JNE -------------------------------------------------

def ultimo_timestamp() -> str:
    archivos = sorted(glob.glob("data/totales_distritos_????????_????.csv"))
    if not archivos:
        raise FileNotFoundError("No hay archivos totales_distritos_*.csv")
    nombre = archivos[-1].replace("\\", "/").split("/")[-1]
    return nombre.replace("totales_distritos_", "").replace(".csv", "")


def cargar_jne(ts: str | None = None):
    if ts is None:
        ts = ultimo_timestamp()
    tot_file  = f"data/totales_distritos_{ts}.csv"
    part_file = f"data/participantes_distritos_{ts}.csv"
    print(f"JNE snapshot: totales_distritos_{ts}.csv")

    votos: dict[tuple, dict] = defaultdict(lambda: {'S': 0, 'A': 0})
    s_raw = a_raw = 0
    with open(part_file, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ub  = r["ubigeo_distrito"]
            amb = r.get("id_ambito_geografico", "")
            cod = r["codigoAgrupacionPolitica"]
            v   = inte(r.get("totalVotosValidos"))
            if cod == COD_S:
                votos[(ub, amb)]['S'] += v
                s_raw += v
            elif cod == COD_A:
                votos[(ub, amb)]['A'] += v
                a_raw += v

    cont   = {}
    total  = {}
    jee_jn = {}
    with open(tot_file, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ub  = r["ubigeo_distrito"]
            amb = r.get("id_ambito_geografico", "")
            cont  [(ub, amb)] = inte(r.get("contabilizadas"))
            total [(ub, amb)] = inte(r.get("totalActas"))
            jee_jn[(ub, amb)] = inte(r.get("enviadasJee"))

    return votos, cont, total, jee_jn, s_raw, a_raw


# ---------- construir avg por distrito (con fallback geografico) -------------

def build_avg(votos, cont, centroides):
    valid = {k for k, c in cont.items() if c > 0}

    def avg_propio(k):
        c = cont.get(k, 0)
        if c == 0:
            return None
        return votos[k]['S'] / c, votos[k]['A'] / c

    def avg_geo(k):
        ub, amb = k
        if amb == "1" and centroides:
            coord = centroides.get(ub)
            if coord:
                lat0, lon0 = coord
                cands = [(u, a) for (u, a) in valid if a == amb and centroides.get(u)]
                if cands:
                    best = min(cands, key=lambda x: haversine(lat0, lon0, *centroides[x[0]]))
                    c = cont[best]
                    return votos[best]['S']/c, votos[best]['A']/c
        prov = ub[:4]; dept = ub[:2]
        for scope in (prov, dept):
            cands = [(u, a) for (u, a) in valid if a == amb and u.startswith(scope)]
            if cands:
                best = min(cands, key=lambda x: abs(int(x[0]) - int(ub)))
                c = cont[best]
                return votos[best]['S']/c, votos[best]['A']/c
        cands = [(u, a) for (u, a) in valid if a == amb]
        if cands:
            ts = sum(votos[x]['S'] for x in cands)
            ta = sum(votos[x]['A'] for x in cands)
            tc = sum(cont[x] for x in cands)
            return ts/tc, ta/tc
        return 0.0, 0.0

    avg = {}
    for k in set(cont.keys()) | set(votos.keys()):
        r = avg_propio(k)
        avg[k] = r if r else avg_geo(k)
    return avg


# ---------- calculo de filas -------------------------------------------------

def compute_rows(imputar_no_proc, imputar_jee,
                 s_raw, a_raw, n_cont,
                 votos_mesa_data, ya_contabilizadas, no_procesables,
                 todas_obs, id_acta, ub_amb, avg, jee_jn, cont, total,
                 noproc_por_k, n_no_proc, n_noproc_pdf, n_noproc_sin_pdf):
    rows = [{"label": "1. JNE contabilizadas", "s": s_raw, "a": a_raw, "n": n_cont}]

    if imputar_jee:
        s_jee_imp = a_jee_imp = 0.0
        n_jee_imp = 0
        for k, jee_n in jee_jn.items():
            if not imputar_no_proc:
                jee_n -= noproc_por_k.get(k, 0)
            if jee_n <= 0:
                continue
            av = avg.get(k, (0.0, 0.0))
            s_jee_imp += av[0] * jee_n
            a_jee_imp += av[1] * jee_n
            n_jee_imp += jee_n
        rows.append({"label": "2+3. JEE todas (imputadas distrito)", "s": s_jee_imp, "a": a_jee_imp,
                     "imputed": True, "n": n_jee_imp, "note": "enviadasJee API"})
        if not imputar_no_proc:
            rows.append({"label": "  No procesables (extraviadas/siniestradas/nulidad)", "s": 0, "a": 0,
                         "sub": True, "n": n_no_proc, "note": "contabilizadas como 0"})
            rows.append({"label": "    Con PDF (votos ignorados)", "s": 0, "a": 0,
                         "subsub": True, "n": n_noproc_pdf})
            rows.append({"label": "    Sin PDF (no imputadas)", "s": 0, "a": 0,
                         "subsub": True, "n": n_noproc_sin_pdf})
        s_jee = s_jee_imp; a_jee = a_jee_imp
        s_sin = a_sin = 0.0
    else:
        mesas_jee_local = set()
        votos_por_tipo: dict[str, dict[str, int]] = defaultdict(lambda: {'S': 0, 'A': 0})
        mesas_por_tipo: dict[str, set] = defaultdict(set)
        for r in votos_mesa_data:
            fila = int(r["fila"]); v = inte(r["valor"]); tipo = r.get("tipo", "")
            mesa = r["codigoMesa"]
            mesas_jee_local.add(mesa)
            if mesa in ya_contabilizadas or (mesa in no_procesables and not imputar_no_proc):
                continue
            mesas_por_tipo[tipo].add(mesa)
            if fila == FILA_S: votos_por_tipo[tipo]['S'] += v
            if fila == FILA_A: votos_por_tipo[tipo]['A'] += v

        n_con_pdf = len(set().union(*mesas_por_tipo.values())) if mesas_por_tipo else 0
        s_jee = a_jee = 0
        for tipo, label_tipo in [("digital", "digital"), ("manual", "escaneado")]:
            d = {'S': 0, 'A': 0}
            n_tipo = set()
            for t, vals in votos_por_tipo.items():
                if (tipo == "digital" and t == "digital") or (tipo == "manual" and t != "digital"):
                    d['S'] += vals['S']; d['A'] += vals['A']
                    n_tipo |= mesas_por_tipo.get(t, set())
            s_jee += d['S']; a_jee += d['A']
            rows.append({"label": f"  2{'a' if tipo=='digital' else 'b'}. JEE PDF {label_tipo}",
                         "s": d['S'], "a": d['A'], "sub": True, "n": len(n_tipo)})
        rows.append({"label": "2. JEE con PDF", "s": s_jee, "a": a_jee, "n": n_con_pdf,
                     "note": f"{len(ya_contabilizadas)} contabilizadas excluidas"})

        sin_pdf = {c: i for c, i in todas_obs.items()
                   if c not in mesas_jee_local and (c not in no_procesables or imputar_no_proc)}
        s_sin = a_sin = 0.0
        for mesa, id_a in sin_pdf.items():
            ub  = ubigeo_desde_id_acta(mesa, id_a)
            amb = ub_amb.get(ub, "1")
            av  = avg.get((ub, amb), (0.0, 0.0))
            s_sin += av[0]; a_sin += av[1]
        rows.append({"label": "3. JEE sin PDF (imputadas)", "s": s_sin, "a": a_sin,
                     "imputed": True, "n": len(sin_pdf)})
        if not imputar_no_proc:
            rows.append({"label": "  No procesables (extrav./siniest./nulidad)", "s": 0, "a": 0,
                         "sub": True, "n": n_no_proc, "note": "contabilizadas como 0"})
            rows.append({"label": "    Con PDF (votos ignorados)", "s": 0, "a": 0,
                         "subsub": True, "n": n_noproc_pdf})
            rows.append({"label": "    Sin PDF (no imputadas)", "s": 0, "a": 0,
                         "subsub": True, "n": n_noproc_sin_pdf})

    s_pend = a_pend = 0.0
    n_pend = 0
    for k, ta in total.items():
        c  = cont.get(k, 0)
        jj = jee_jn.get(k, 0)
        p  = ta - c - jj
        if p <= 0: continue
        av = avg.get(k, (0.0, 0.0))
        s_pend += av[0] * p
        a_pend += av[1] * p
        n_pend += p
    rows.append({"label": "4. Pendientes puros (imputados)", "s": s_pend, "a": a_pend,
                 "imputed": True, "n": n_pend})

    st = s_raw + s_jee + s_sin + s_pend
    at = a_raw + a_jee + a_sin + a_pend
    rows.append({"label": "TOTAL ESTIMADO", "s": st, "a": at, "total": True, "imputed": True})
    return rows


# ---------- renderizado consola ----------------------------------------------

def render_console(rows):
    print(f"\n{'Componente':<46} {'Actas':>7}  {'Sanchez':>12}  {'Aliaga':>12}  {'S - A':>12}")
    print("-" * 96)
    for r in rows:
        label = r["label"]
        s, a  = r["s"], r["a"]
        diff  = s - a
        sign  = f"{diff:+,.0f}" if r.get("imputed") else f"{int(diff):+,}"
        sv    = f"{s:,.0f}"     if r.get("imputed") else f"{int(s):,}"
        av    = f"{a:,.0f}"     if r.get("imputed") else f"{int(a):,}"
        nv    = f"{r['n']:,}"   if r.get("n") is not None else ""
        sep   = "=" * 96 if r.get("total") else ""
        if sep: print(sep)
        print(f"{label:<46} {nv:>7}  {sv:>12}  {av:>12}  {sign:>12}")
    print("=" * 96)


# ---------- renderizado HTML -------------------------------------------------

def render_html(rows_default, rows_imputar, sin_pdf_annex, noproc_annex,
                snapshot, obs_path, detalle_path, out_path="data/desglose_jee.html"):
    from datetime import datetime

    def fmt_int(v, imputed=False):
        return f"{v:,.0f}" if imputed else f"{int(v):,}"

    def sign_cls(v):
        return "pos" if v > 0 else ("neg" if v < 0 else "")

    def bar_pct(s, a):
        t = s + a
        if t == 0: return 50.0
        return round(100 * s / t, 2)

    def build_tbody(rows):
        html = ""
        for r in rows:
            s, a  = r["s"], r["a"]
            diff  = s - a
            imp   = r.get("imputed", False)
            sv    = fmt_int(s, imp); av = fmt_int(a, imp)
            dv    = fmt_int(abs(diff), imp)
            dsign = "+" if diff >= 0 else "&#8722;"
            dcls  = sign_cls(diff)
            rcls  = "total" if r.get("total") else ("subsub" if r.get("subsub") else ("sub" if r.get("sub") else ""))
            pct   = bar_pct(s, a)
            bar   = (f'<div class="bar"><div class="bar-s" style="width:{pct}%"></div>'
                     f'<div class="bar-a" style="width:{100-pct}%"></div></div>')
            parts = []
            if r.get("n") is not None: parts.append(f"{r['n']:,} actas")
            if r.get("note"): parts.append(r["note"])
            note  = f'<span class="note">{" — ".join(parts)}</span>' if parts else ""
            html += (f'<tr class="{rcls}"><td class="lbl">{r["label"]}{note}</td>'
                     f'<td class="num s">{sv}</td><td class="num a">{av}</td>'
                     f'<td class="num diff {dcls}">{dsign}{dv}</td>'
                     f'<td class="barcell">{bar}</td></tr>\n')
        return html

    def scenario_table(rows):
        return (
            "<table><thead><tr>"
            "<th class=\"lbl\">Componente</th>"
            "<th>S&aacute;nchez</th><th>Aliaga</th><th>S&minus;A</th><th></th>"
            "</tr></thead><tbody>\n"
            + build_tbody(rows)
            + "</tbody></table>"
        )

    def total_row(rows):
        return next(r for r in reversed(rows) if r.get("total"))

    td   = total_row(rows_default)
    ti   = total_row(rows_imputar)
    dd   = td["s"] - td["a"]
    di   = ti["s"] - ti["a"]
    sign = lambda v: ("+" if v >= 0 else "&#8722;")

    # Annex A: sin PDF
    ann_a = ""
    for a in sin_pdf_annex:
        ann_a += (f'<tr><td>{a["mesa"]}</td><td>{a["dpto"]}</td><td>{a["prov"]}</td>'
                  f'<td>{a["dist"]}</td><td>{a["local"]}</td><td>{a["res"]}</td></tr>\n')

    # Annex B: no procesables
    ann_b = ""
    for a in noproc_annex:
        badge = ('<span class="badge pdf">Con PDF</span>' if a["tiene_pdf"]
                 else '<span class="badge nopdf">Sin PDF</span>')
        ann_b += (f'<tr><td>{a["mesa"]}</td><td>{a["dpto"]}</td><td>{a["prov"]}</td>'
                  f'<td>{a["dist"]}</td><td>{a["local"]}</td>'
                  f'<td>{a["razon"]}</td><td class="ctr">{badge}</td></tr>\n')

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Desglose JEE &mdash; Proyecci&oacute;n</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',system-ui,sans-serif;background:#0f1117;color:#e2e8f0;padding:24px}}
  h1{{font-size:1.4rem;font-weight:600;margin-bottom:4px;color:#f8fafc}}
  h2{{font-size:.95rem;font-weight:600;margin:32px 0 10px;color:#94a3b8;
      border-bottom:1px solid #1e293b;padding-bottom:5px;text-transform:uppercase;letter-spacing:.05em}}
  .meta{{font-size:.8rem;color:#64748b;margin-bottom:28px}}
  /* dual layout */
  .dual{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:8px}}
  @media(max-width:900px){{.dual{{grid-template-columns:1fr}}}}
  .scenario-hdr{{font-size:.8rem;font-weight:600;padding:6px 10px;border-radius:6px;
                 display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}}
  .hdr-conservador{{background:#1e293b;color:#94a3b8}}
  .hdr-imputar{{background:#14291f;color:#6ee7b7}}
  .diff-chip{{font-family:'Cascadia Code','Consolas',monospace;font-size:.9rem}}
  /* tables */
  table{{width:100%;border-collapse:collapse;font-size:.82rem}}
  th{{text-align:right;padding:6px 8px;color:#475569;font-weight:500;
      border-bottom:1px solid #1e293b;font-size:.72rem;text-transform:uppercase;letter-spacing:.04em}}
  th.lbl{{text-align:left}}
  td{{padding:5px 8px;border-bottom:1px solid #1e293b}}
  td.lbl{{color:#cbd5e1}}
  td.num{{text-align:right;font-variant-numeric:tabular-nums;font-family:'Cascadia Code','Consolas',monospace}}
  td.s{{color:#93c5fd}} td.a{{color:#fca5a5}}
  td.diff.pos{{color:#4ade80}} td.diff.neg{{color:#f87171}} td.diff{{color:#64748b}}
  tr.sub td{{color:#475569;font-size:.78rem}} tr.sub td.lbl{{padding-left:18px}}
  tr.subsub td{{color:#334155;font-size:.75rem}} tr.subsub td.lbl{{padding-left:34px}}
  tr.total td{{font-weight:700;color:#f8fafc;border-top:2px solid #334155;border-bottom:none}}
  tr.total td.s{{color:#60a5fa}} tr.total td.a{{color:#f87171}}
  .note{{display:block;font-size:.68rem;color:#475569;margin-top:1px}}
  .barcell{{width:80px;padding:5px 5px;vertical-align:middle}}
  .bar{{display:flex;height:5px;border-radius:3px;overflow:hidden;background:#1e293b}}
  .bar-s{{background:#3b82f6}} .bar-a{{background:#ef4444}}
  @media(max-width:600px){{.barcell{{display:none}}}}
  /* annexes */
  .annex-wrap{{overflow-x:auto;margin-top:4px}}
  table.annex td{{color:#64748b;font-size:.77rem;padding:4px 8px;white-space:nowrap}}
  table.annex th{{font-size:.68rem}}
  .badge{{font-size:.68rem;padding:2px 6px;border-radius:999px;font-weight:600;white-space:nowrap}}
  .badge.pdf{{background:#14291f;color:#6ee7b7}}
  .badge.nopdf{{background:#2a1515;color:#fca5a5}}
  td.ctr{{text-align:center}}
</style>
</head>
<body>
<h1>Proyecci&oacute;n Electoral &mdash; Desglose JEE</h1>
<p class="meta">
  Snapshot JNE: <b>{snapshot}</b> &nbsp;|&nbsp;
  Actas obs.: <b>{obs_path.replace(chr(92), "/").split("/")[-1]}</b> &nbsp;|&nbsp;
  Detalle: <b>{(detalle_path or "").replace(chr(92), "/").split("/")[-1]}</b> &nbsp;|&nbsp;
  Generado: {now}
</p>

<div class="dual">
  <div>
    <div class="scenario-hdr hdr-conservador">
      <span>No procesables = 0 (conservador)</span>
      <span class="diff-chip">{sign(dd)}{abs(dd):,.0f} S&minus;A</span>
    </div>
    {scenario_table(rows_default)}
  </div>
  <div>
    <div class="scenario-hdr hdr-imputar">
      <span>No procesables imputadas al distrito</span>
      <span class="diff-chip">{sign(di)}{abs(di):,.0f} S&minus;A</span>
    </div>
    {scenario_table(rows_imputar)}
  </div>
</div>

<h2>Anexo A &mdash; JEE sin PDF ({len(sin_pdf_annex)} actas &mdash; imputadas al promedio del distrito)</h2>
<div class="annex-wrap">
<table class="annex"><thead><tr>
  <th class="lbl">Mesa</th><th class="lbl">Dpto</th><th class="lbl">Provincia</th>
  <th class="lbl">Distrito</th><th class="lbl">Local de votaci&oacute;n</th><th class="lbl">Observaci&oacute;n</th>
</tr></thead><tbody>
{ann_a}</tbody></table>
</div>

<h2>Anexo B &mdash; Actas no procesables ({len(noproc_annex)})</h2>
<div class="annex-wrap">
<table class="annex"><thead><tr>
  <th class="lbl">Mesa</th><th class="lbl">Dpto</th><th class="lbl">Provincia</th>
  <th class="lbl">Distrito</th><th class="lbl">Local de votaci&oacute;n</th>
  <th class="lbl">Raz&oacute;n</th><th>PDF</th>
</tr></thead><tbody>
{ann_b}</tbody></table>
</div>
</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML -> {out_path}")


# ---------- main -------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--imputar-jee",   action="store_true",
                        help="Imputar todas las actas JEE al promedio del distrito (ignora OCR)")
    parser.add_argument("--timestamp",     default=None,
                        help="Timestamp JNE, ej: 20260421_0011 (default: el mas reciente)")
    parser.add_argument("--actas-obs",     default=None,
                        help="CSV de actas observadas (default: data/mesas_observadas.csv)")
    parser.add_argument("--detalle-jsonl", default=None,
                        help="JSONL con detalle de actas (default: el mas reciente)")
    args = parser.parse_args()

    ts = args.timestamp or ultimo_timestamp()
    votos, cont, total, jee_jn, s_raw, a_raw = cargar_jne(ts)
    centroides = load_centroides()
    avg = build_avg(votos, cont, centroides)

    snapshot = ts.replace("_", " ")
    n_cont   = sum(cont.values())

    # ---- cargar JSONL ----
    estado_mesa:        dict[str, str]  = {}
    no_procesables:     set[str]        = set()
    mesas_presidenciales: set[str]      = set()
    detalle_obj:        dict[str, dict] = {}
    detalle_path = args.detalle_jsonl or (
        (sorted(glob.glob("data/detalle_actas_????????_????.jsonl")) or [None])[-1]
    )
    if detalle_path:
        with open(detalle_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                mesa = str(obj.get("codigoMesa", "")).strip()
                if not mesa:
                    continue
                if obj.get("idEleccion") == 10:
                    mesas_presidenciales.add(mesa)
                codigo = obj.get("codigoEstadoActa", "")
                estado_mesa[mesa] = codigo
                res = (obj.get("estadoActaResolucion") or "")
                if codigo not in ("C", "L") and {r.strip() for r in res.split(",")} & NO_PROC_CODES:
                    no_procesables.add(mesa)
                detalle_obj[mesa] = obj

    ya_contabilizadas = {m for m, e in estado_mesa.items() if e in ("C", "L")}

    # ---- mapa ubigeo -> ambito ----
    ub_amb = {}
    with open(f"data/totales_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ub_amb[r["ubigeo_distrito"]] = r.get("id_ambito_geografico", "1")

    # ---- cargar actas observadas ----
    obs_path = args.actas_obs or MESAS_OBS
    id_acta  = {}
    todas_obs = {}
    with open(obs_path, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            cm = r["codigoMesa"].strip()
            if not cm:
                continue
            if mesas_presidenciales and cm not in mesas_presidenciales:
                continue
            id_acta[cm]   = r["id"]
            todas_obs[cm] = r["id"]

    # ---- no procesables por distrito ----
    noproc_por_k: dict[tuple, int] = defaultdict(int)
    for mesa in no_procesables:
        if mesa in id_acta:
            ub  = ubigeo_desde_id_acta(mesa, id_acta[mesa])
            amb = ub_amb.get(ub, "1")
            noproc_por_k[(ub, amb)] += 1
    n_no_proc = len({c for c in todas_obs if c in no_procesables})

    # ---- cargar votos_mesa en memoria (se reutiliza en ambas versiones) ----
    votos_mesa_data = []
    mesas_jee       = set()
    with open(VOTOS_MESA, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            votos_mesa_data.append(r)
            mesas_jee.add(r["codigoMesa"])

    n_noproc_pdf     = len({c for c in todas_obs if c in no_procesables and c in mesas_jee})
    n_noproc_sin_pdf = n_no_proc - n_noproc_pdf

    # ---- calcular ambas versiones ----
    shared = dict(
        imputar_jee=args.imputar_jee,
        s_raw=s_raw, a_raw=a_raw, n_cont=n_cont,
        votos_mesa_data=votos_mesa_data,
        ya_contabilizadas=ya_contabilizadas,
        no_procesables=no_procesables,
        todas_obs=todas_obs, id_acta=id_acta, ub_amb=ub_amb,
        avg=avg, jee_jn=jee_jn, cont=cont, total=total,
        noproc_por_k=noproc_por_k, n_no_proc=n_no_proc,
        n_noproc_pdf=n_noproc_pdf, n_noproc_sin_pdf=n_noproc_sin_pdf,
    )
    rows_default = compute_rows(imputar_no_proc=False, **shared)
    rows_imputar = compute_rows(imputar_no_proc=True,  **shared)

    # ---- anexo A: sin PDF (version conservadora, sin no-procesables) ----
    sin_pdf_default = {c: i for c, i in todas_obs.items()
                       if c not in mesas_jee and c not in no_procesables}
    sin_pdf_annex = []
    for mesa, id_a in sorted(sin_pdf_default.items()):
        obj = detalle_obj.get(mesa, {})
        sin_pdf_annex.append({
            "mesa" : mesa,
            "dpto" : obj.get("ubigeoNivel01", ""),
            "prov" : obj.get("ubigeoNivel02", ""),
            "dist" : obj.get("ubigeoNivel03", ""),
            "local": obj.get("nombreLocalVotacion", ""),
            "res"  : obj.get("estadoDescripcionActaResolucion", "") or obj.get("estadoActaResolucion", ""),
        })

    # ---- anexo B: no procesables con indicador de PDF ----
    noproc_annex = []
    for mesa in sorted(no_procesables):
        obj   = detalle_obj.get(mesa, {})
        res   = (obj.get("estadoActaResolucion") or "")
        codes = {r.strip() for r in res.split(",")} & NO_PROC_LABEL.keys()
        noproc_annex.append({
            "mesa"     : mesa,
            "dpto"     : obj.get("ubigeoNivel01", ""),
            "prov"     : obj.get("ubigeoNivel02", ""),
            "dist"     : obj.get("ubigeoNivel03", ""),
            "local"    : obj.get("nombreLocalVotacion", ""),
            "razon"    : ", ".join(NO_PROC_LABEL[c] for c in sorted(codes)),
            "tiene_pdf": mesa in mesas_jee,
        })

    render_console(rows_default)
    render_html(rows_default, rows_imputar, sin_pdf_annex, noproc_annex,
                snapshot, obs_path, detalle_path)


if __name__ == "__main__":
    main()
