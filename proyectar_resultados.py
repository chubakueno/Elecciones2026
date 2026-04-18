"""
proyectar_resultados.py
-----------------------
Proyecta los resultados electorales al 100% de actas contabilizadas.

Metodología:
  1. Estima el total de votos válidos de cada distrito usando el candidato
     con más votos (menor error de redondeo):
       total_validos = totalVotosValidos / (porcentajeVotosValidos / 100)

  2. Proyecta cada distrito al 100% dividiendo por (actasContabilizadas / 100).

  3. Imputa los distritos sin datos (actasContabilizadas=0 o todos los
     candidatos con 0 votos) usando el distrito con ubigeo numéricamente
     más cercano dentro de la misma provincia que sí tenga datos.
     Fallback: mismo departamento.

  4. Para el tamaño del distrito imputado usa la proporción de totalActas
     (mesas electorales) respecto al donor.

  5. Agrega todos los distritos y calcula el resultado proyectado nacional.

Entradas : totales_distritos.csv, participantes_distritos.csv
Salida   : proyeccion_final.csv
"""

import argparse
import csv
import glob
import os
import re
import sys
import math
from collections import defaultdict

INPUT_TOTALES       = "data/totales_distritos.csv"
INPUT_PARTICIPANTES = "data/participantes_distritos.csv"
INPUT_CENTROIDES    = "ubigeo_centroides.csv"
OUTPUT_CSV          = "data/proyeccion_final.csv"

N_DONORS = 2   # valor por defecto; sobreescrito por --n-donors en CLI

# Distritos excluidos explicitamente de la proyeccion (override manual, casos excepcionales)
UBIGEOS_EXCLUIR: set[str] = {}


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------

def load_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))

def flt(val) -> float:
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0

def inte(val) -> int:
    try:
        return int(float(val or 0))
    except (ValueError, TypeError):
        return 0


def linreg(xs: list, ys: list) -> tuple:
    """
    Regresión lineal OLS: y = a*x + b.
    Retorna (a, b) o (None, None) si indeterminado.
    """
    n = len(xs)
    if n < 2:
        return None, None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None, None
    a = num / den
    b = my - a * mx
    return a, b


def build_hist_series(timestamps: list) -> dict:
    """
    Para cada (ubigeo_distrito, id_ambito_geografico, dniCandidato) carga
    la serie histórica de (contabilizadas, totalVotosValidos) a través de
    los snapshots indicados.

    Se usa para proyectar distritos parciales mediante regresión lineal:
        votos = a * contabilizadas + b  →  predict en totalActas
    """
    series: dict = defaultdict(list)
    for ts in timestamps:
        cont_map: dict = {}
        try:
            with open(f"data/totales_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    ub   = row["ubigeo_distrito"]
                    amb  = row.get("id_ambito_geografico", "")
                    cont = inte(row.get("contabilizadas", 0))
                    cont_map[(ub, amb)] = cont
        except FileNotFoundError:
            continue

        try:
            with open(f"data/participantes_distritos_{ts}.csv", newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    if row.get("error"):
                        continue
                    ub    = row["ubigeo_distrito"]
                    amb   = row.get("id_ambito_geografico", "")
                    dni   = row.get("dniCandidato", "").strip()
                    votos = inte(row.get("totalVotosValidos", 0))
                    cont  = cont_map.get((ub, amb), 0)
                    if cont > 0:
                        series[(ub, amb, dni)].append((cont, votos))
        except FileNotFoundError:
            continue

    return dict(series)


# ---------------------------------------------------------------------------
# Lógica de imputación
# ---------------------------------------------------------------------------

def province_of(ubigeo: str) -> str:
    return ubigeo[:4]          # primeros 4 dígitos = depto + provincia

def department_of(ubigeo: str) -> str:
    return ubigeo[:2]


def build_valid_set(all_ubigeos, needs_imputation_fn) -> set:
    """Ubigeos que tienen datos reales (no necesitan imputación)."""
    return {u for u in all_ubigeos if not needs_imputation_fn(u)}


def load_centroides(path: str) -> dict[str, tuple[float, float]]:
    """Devuelve {ubigeo: (lat, lon)} leyendo el CSV de centroides del INEI."""
    centroides = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                ub  = str(row["reniec"]).strip().zfill(6)   # ONPE usa códigos RENIEC
                lat = flt(row.get("latitude") or row.get("latitud"))
                lon = flt(row.get("longitude") or row.get("longitud"))
                if lat and lon:
                    centroides[ub] = (lat, lon)
        print(f"  {len(centroides)} centroides cargados desde {path}")
    except FileNotFoundError:
        print(f"  [warn] No se encontró {path} — imputación nacional usará proximidad numérica")
    return centroides


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_donors(key: tuple, valid_set: set, centroides: dict, n: int = N_DONORS) -> tuple[list, str]:
    """
    Busca hasta 2 donors para un distrito sin datos.

    - Nacional (ambito=1) con centroide disponible: los 2 distritos válidos
      con centroide más cercano. Devuelve lista de (donor_key, peso) donde
      peso = 1/distancia_km (inverso de distancia).
    - Fallback / extranjero: 1 donor por proximidad numérica con peso=1.0.

    Retorna ([(donor_key, peso), ...], scope_label).
    Si no hay ningún donor, retorna ([], "sin referencia de imputación").
    """
    ubigeo, ambito = key

    if ambito == "1" and centroides:
        coord = centroides.get(ubigeo)
        if coord:
            lat0, lon0 = coord
            candidates = [
                (u, a) for (u, a) in valid_set
                if (u, a) != key and centroides.get(u)
            ]
            if candidates:
                ranked = sorted(candidates, key=lambda k: haversine_km(lat0, lon0, *centroides[k[0]]))
                top2   = ranked[:n]
                dists  = [haversine_km(lat0, lon0, *centroides[k[0]]) for k in top2]
                # peso = 1/distancia; si distancia==0 se le da peso absoluto
                pesos  = [1 / d if d > 0 else 1e9 for d in dists]
                donors = list(zip(top2, pesos))
                label  = "centroide (" + ", ".join(f"{d:.1f} km" for d in dists) + ")"
                return donors, label

    # Fallback numérico — único camino para extranjero
    ub_int = int(ubigeo)
    prov   = province_of(ubigeo)
    dept   = department_of(ubigeo)

    for scope_name, scope in (("provincia", prov), ("departamento", dept)):
        candidates = [
            (u, a) for (u, a) in valid_set
            if a == ambito and u.startswith(scope) and u != ubigeo
        ]
        if candidates:
            best = min(candidates, key=lambda k: abs(int(k[0]) - ub_int))
            return [(best, 1.0)], scope_name

    return [], "sin referencia de imputación"


# ---------------------------------------------------------------------------
# Proyección para un timestamp dado
# ---------------------------------------------------------------------------

def proyectar(timestamp: str | None, n_donors: int,
              solo_peru: bool = False, solo_extranjero: bool = False,
              hist_series: dict | None = None,
              actas_jee: float = 0.0):
    sufijo = "_peru" if solo_peru else "_extranjero" if solo_extranjero else ""

    def con_ts(base: str) -> str:
        if timestamp:
            return base.replace(".csv", f"{sufijo}_{timestamp}.csv")
        return base.replace(".csv", f"{sufijo}.csv") if sufijo else base

    input_totales       = con_ts(INPUT_TOTALES).replace(f"{sufijo}_", "_") if sufijo else con_ts(INPUT_TOTALES)
    input_participantes = con_ts(INPUT_PARTICIPANTES).replace(f"{sufijo}_", "_") if sufijo else con_ts(INPUT_PARTICIPANTES)
    output_csv          = con_ts(OUTPUT_CSV)

    # Para entradas no llevan sufijo _peru (son los mismos CSVs fuente)
    if timestamp:
        input_totales       = INPUT_TOTALES.replace(".csv",       f"_{timestamp}.csv")
        input_participantes = INPUT_PARTICIPANTES.replace(".csv", f"_{timestamp}.csv")
        output_csv          = OUTPUT_CSV.replace(".csv", f"{sufijo}_{timestamp}.csv")
    else:
        input_totales       = INPUT_TOTALES
        input_participantes = INPUT_PARTICIPANTES
        output_csv          = OUTPUT_CSV.replace(".csv", f"{sufijo}.csv") if sufijo else OUTPUT_CSV

    print(f"\n{'='*60}")
    ambito_str = "solo_peru" if solo_peru else "solo_extranjero" if solo_extranjero else "todos"
    print(f"Timestamp: {timestamp or '(sin timestamp)'}  |  ambito={ambito_str}")

    # --- Cargar archivos ---
    print(f"Cargando datos... (n_donors={n_donors})")
    totales_list = load_csv(input_totales)
    partic_list  = load_csv(input_participantes)
    centroides   = load_centroides(INPUT_CENTROIDES)

    # totales[(ubigeo, ambito)] -> fila
    totales = {(r["ubigeo_distrito"], r["id_ambito_geografico"]): r for r in totales_list}

    # participantes[(ubigeo, ambito)] -> [filas de candidatos]
    participantes: dict[tuple, list] = defaultdict(list)
    for r in partic_list:
        participantes[(r["ubigeo_distrito"], r["id_ambito_geografico"])].append(r)

    # Filtrar por ambito si se solicita
    if solo_peru:
        totales       = {k: v for k, v in totales.items()       if k[1] == "1"}
        participantes = {k: v for k, v in participantes.items() if k[1] == "1"}
    elif solo_extranjero:
        totales       = {k: v for k, v in totales.items()       if k[1] == "2"}
        participantes = {k: v for k, v in participantes.items() if k[1] == "2"}

    all_ubigeos = list(totales.keys())
    print(f"  {len(all_ubigeos)} distritos en totales")
    print(f"  {len(participantes)} distritos en participantes")

    # --- Ajuste por actas JEE ---
    # Las actas anuladas salen del universo: totalActas -= anuladas, contabilizadas -= anuladas
    if actas_jee > 0:
        frac = actas_jee / 100.0
        total_anuladas = 0
        adjusted = {}
        for k, r in totales.items():
            jee         = inte(r.get("enviadasJee", 0))
            anuladas    = round(jee * frac)
            cont_orig   = inte(r.get("contabilizadas", 0))
            total_orig  = inte(r.get("totalActas", 0))
            total_adj   = max(total_orig - anuladas, 0)
            pct_adj     = round(cont_orig / total_adj * 100, 4) if total_adj > 0 else 0.0
            adjusted[k] = {**r,
                           "totalActas":          str(total_adj),
                           "actasContabilizadas": str(pct_adj)}
            total_anuladas += anuladas
        totales = adjusted
        print(f"  [JEE] {actas_jee:.2f}% de enviadasJee anuladas ({total_anuladas:,} actas retiradas del universo)")

    # --- Detectar qué distritos necesitan imputación ---
    def imputation_reason(key: tuple) -> str | None:
        """Devuelve la razón de imputación, o None si el distrito tiene datos."""
        actas_pct   = flt(totales.get(key, {}).get("actasContabilizadas", 0))
        total_votos = sum(inte(c["totalVotosValidos"]) for c in participantes.get(key, []))
        if total_votos == 0 and actas_pct >= 100:
            return None  # 100% contabilizadas y 0 votos: genuinamente 0, no imputar
        if actas_pct == 0:
            return "0% actas contabilizadas"
        if total_votos == 0:
            return "actas contabilizadas pero 0 votos válidos"
        return None

    def needs_imputation(key: tuple) -> bool:
        return imputation_reason(key) is not None

    valid_set  = build_valid_set(all_ubigeos, needs_imputation)
    to_impute  = [k for k in all_ubigeos if needs_imputation(k) and k[0] not in UBIGEOS_EXCLUIR]

    def ambito_label(key: tuple) -> str:
        return "nacional" if key[1] == "1" else "extranjero"

    for label in ("nacional", "extranjero"):
        con_datos = sum(1 for k in valid_set  if ambito_label(k) == label)
        a_imputar = sum(1 for k in to_impute  if ambito_label(k) == label)
        print(f"  {label}: {con_datos} con datos  |  {a_imputar} a imputar")

    # Desglose de razones
    reasons: dict[str, list] = defaultdict(list)
    for k in to_impute:
        reasons[imputation_reason(k)].append(k)
    for reason, lst in reasons.items():
        print(f"    · {reason}: {len(lst)}")

    # --- Paso 1: proyectar distritos con datos propios ---
    # district_proj[ubigeo] = {"total": float, "cands": [{...votos_proy...}]}
    district_proj: dict[str, dict] = {}

    n_reg_used = n_reg_fallback = 0

    for key in valid_set:
        actas_pct   = flt(totales[key]["actasContabilizadas"])
        total_actas = inte(totales[key].get("totalActas", 0))
        cont_actual = inte(totales[key].get("contabilizadas", 0))
        cands       = participantes[key]

        # ¿Usar regresión? Solo para distritos parciales con historia disponible.
        usar_reg = (
            hist_series is not None
            and total_actas > 0
            and cont_actual > 0
            and cont_actual < total_actas
        )

        cands_proy = []

        for c in cands:
            votos_actual = inte(c["totalVotosValidos"])
            votos_proy   = None

            if usar_reg:
                dni  = c.get("dniCandidato", "").strip()
                pts  = hist_series.get((key[0], key[1], dni), [])
                # Deduplicar por x (quedarse con el último si hay repetidos)
                pts_dd = sorted({x: y for x, y in pts}.items())

                if len(pts_dd) >= 2:
                    xs = [p[0] for p in pts_dd]
                    ys = [p[1] for p in pts_dd]
                    a, b = linreg(xs, ys)
                    if a is not None and a >= 0:
                        pred = a * total_actas + b
                        # No puede haber menos votos que los ya contabilizados
                        votos_proy = max(pred, votos_actual)

            if votos_proy is None:
                # Fallback: ratio simple
                votos_proy = votos_actual / (actas_pct / 100) if actas_pct > 0 else votos_actual
                if usar_reg:
                    n_reg_fallback += 1
            elif usar_reg:
                n_reg_used += 1

            cands_proy.append({**c, "votos_proyectados": votos_proy})

        total_proyectado = sum(c["votos_proyectados"] for c in cands_proy)
        district_proj[key] = {"total": total_proyectado, "cands": cands_proy}

    if hist_series is not None:
        print(f"  Regresion lineal: {n_reg_used} candidaturas proyectadas "
              f"| {n_reg_fallback} con fallback a ratio")

    # --- Perfil global extranjero (fallback para imputación sin donor) ---
    # Suma de votos proyectados de todos los distritos extranjeros válidos,
    # indexada por clave de candidato. Se usa cuando un distrito extranjero
    # no tiene donor en su provincia ni departamento.
    perfil_ext_votos: dict[tuple, float] = defaultdict(float)
    perfil_ext_meta:  dict[tuple, dict]  = {}
    perfil_ext_actas: float = 0.0

    for k, data in district_proj.items():
        if k[1] != "2":
            continue
        perfil_ext_actas += flt(totales[k].get("totalActas", 0))
        for c in data["cands"]:
            ck = (c.get("codigoAgrupacionPolitica", ""), c.get("dniCandidato", ""))
            perfil_ext_votos[ck] += c["votos_proyectados"]
            if ck not in perfil_ext_meta:
                perfil_ext_meta[ck] = c

    # --- Paso 2: imputar distritos sin datos ---
    imputed_ok   = defaultdict(int)   # ambito -> count
    imputed_fail = defaultdict(int)   # ambito -> count
    imputation_log = []

    for key in to_impute:
        donors, scope = find_donors(key, valid_set, centroides, n_donors)
        row_target = totales[key]
        a_label = ambito_label(key)

        if not donors:
            if key[1] == "2" and perfil_ext_votos:
                # Extranjero sin donor: imputar con perfil global extranjero
                actas_target = flt(row_target.get("totalActas", 0))
                ratio = (actas_target / perfil_ext_actas) if perfil_ext_actas > 0 else 0.0
                ubigeo_fields = {
                    "ubigeo_distrito":      key[0],
                    "id_ambito_geografico": key[1],
                    "nombre_distrito":      row_target.get("nombre_distrito", key[0]),
                    "ubigeo_provincia":     row_target.get("ubigeo_provincia", ""),
                    "nombre_provincia":     row_target.get("nombre_provincia", ""),
                    "ubigeo_departamento":  row_target.get("ubigeo_departamento", ""),
                    "nombre_departamento":  row_target.get("nombre_departamento", ""),
                }
                cands_proy = [
                    {**perfil_ext_meta[ck], **ubigeo_fields, "votos_proyectados": v * ratio}
                    for ck, v in perfil_ext_votos.items()
                ]
                district_proj[key] = {
                    "total": sum(perfil_ext_votos.values()) * ratio,
                    "cands": cands_proy,
                }
                imputation_log.append({
                    "ubigeo":       key[0],
                    "ambito":       key[1],
                    "nombre":       row_target.get("nombre_distrito", ""),
                    "provincia":    row_target.get("nombre_provincia", ""),
                    "departamento": row_target.get("nombre_departamento", ""),
                    "razon":        imputation_reason(key),
                    "actas_pct":    flt(row_target.get("actasContabilizadas", 0)),
                    "donor_ubigeo": "PERFIL_GLOBAL_EXTRANJERO",
                    "donor_nombre": "",
                    "scope":        "perfil global extranjero",
                })
                imputed_ok[a_label] += 1
            else:
                nombre = row_target.get("nombre_distrito", key[0])
                print(f"  [warn] Sin referencia de imputación para {key[0]} {nombre} ({a_label}) — se omite")
                imputed_fail[a_label] += 1
                imputation_log.append({
                    "ubigeo":       key[0],
                    "ambito":       key[1],
                    "nombre":       row_target.get("nombre_distrito", ""),
                    "provincia":    row_target.get("nombre_provincia", ""),
                    "departamento": row_target.get("nombre_departamento", ""),
                    "razon":        imputation_reason(key),
                    "actas_pct":    flt(row_target.get("actasContabilizadas", 0)),
                    "donor_ubigeo": "",
                    "donor_nombre": "",
                    "scope":        "sin referencia de imputación",
                })
            continue

        actas_target = flt(row_target.get("totalActas", 0))

        ubigeo_fields = {
            "ubigeo_distrito":      key[0],
            "id_ambito_geografico": key[1],
            "nombre_distrito":      row_target.get("nombre_distrito", key[0]),
            "ubigeo_provincia":     row_target.get("ubigeo_provincia", ""),
            "nombre_provincia":     row_target.get("nombre_provincia", ""),
            "ubigeo_departamento":  row_target.get("ubigeo_departamento", ""),
            "nombre_departamento":  row_target.get("nombre_departamento", ""),
        }

        if key[1] == "2":
            # Extranjero: donor único, escala directa (lógica original)
            donor_key, _ = donors[0]
            donor       = district_proj[donor_key]
            row_donor   = totales[donor_key]
            actas_donor = flt(row_donor.get("totalActas", 0))
            ratio       = (actas_target / actas_donor) if actas_donor > 0 else 1.0

            cands_proy = [
                {**c, **ubigeo_fields, "votos_proyectados": c["votos_proyectados"] * ratio}
                for c in donor["cands"]
            ]
            total_pond = donor["total"] * ratio

        else:
            # Nacional: media ponderada por 1/distancia entre los 2 más cercanos
            peso_total = sum(p for _, p in donors)
            votos_pond: dict[tuple, float] = defaultdict(float)
            cand_meta:  dict[tuple, dict]  = {}

            for donor_key, peso in donors:
                donor       = district_proj[donor_key]
                row_donor   = totales[donor_key]
                actas_donor = flt(row_donor.get("totalActas", 0))
                ratio = (actas_target / actas_donor) if actas_donor > 0 else 1.0
                w = peso / peso_total

                for c in donor["cands"]:
                    ck = (c.get("codigoAgrupacionPolitica", ""), c.get("dniCandidato", ""))
                    votos_pond[ck] += c["votos_proyectados"] * ratio * w
                    if ck not in cand_meta:
                        cand_meta[ck] = c

            total_pond = sum(votos_pond.values())
            cands_proy = [
                {**cand_meta[ck], **ubigeo_fields, "votos_proyectados": votos}
                for ck, votos in votos_pond.items()
            ]

        district_proj[key] = {"total": total_pond, "cands": cands_proy}

        donor_ubigeos = "+".join(k[0] for k, _ in donors)
        donor_nombres = "+".join(totales[k].get("nombre_distrito", "") for k, _ in donors)
        imputation_log.append({
            "ubigeo":       key[0],
            "ambito":       key[1],
            "nombre":       row_target.get("nombre_distrito", ""),
            "provincia":    row_target.get("nombre_provincia", ""),
            "departamento": row_target.get("nombre_departamento", ""),
            "razon":        imputation_reason(key),
            "actas_pct":    flt(row_target.get("actasContabilizadas", 0)),
            "donor_ubigeo": donor_ubigeos,
            "donor_nombre": donor_nombres,
            "scope":        scope,
        })
        imputed_ok[a_label] += 1

    for ambito_label in ("nacional", "extranjero"):
        ok   = imputed_ok[ambito_label]
        fail = imputed_fail[ambito_label]
        if ok or fail:
            print(f"  Imputados {ambito_label}: {ok} correctos  |  {fail} sin referencia de imputación")

    # Guardar log de imputaciones
    LOG_CSV = f"data/imputaciones{sufijo}_{timestamp}.csv" if timestamp else f"data/imputaciones{sufijo}.csv"
    log_cols = ["ubigeo", "ambito", "nombre", "provincia", "departamento",
                "razon", "actas_pct", "donor_ubigeo", "donor_nombre", "scope"]
    with open(LOG_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=log_cols)
        writer.writeheader()
        writer.writerows(imputation_log)
    print(f"  Detalle de imputaciones -> {LOG_CSV}")

    # --- Paso 3: agregar resultados nacionales ---
    # Clave: (codigoAgrupacionPolitica, dniCandidato)
    aggregate: dict[tuple, float] = defaultdict(float)
    cand_info:  dict[tuple, dict]  = {}

    for ubigeo, data in district_proj.items():
        for c in data["cands"]:
            key = (c.get("codigoAgrupacionPolitica", ""), c.get("dniCandidato", ""))
            aggregate[key] += c["votos_proyectados"]
            if key not in cand_info:
                cand_info[key] = {
                    "nombreAgrupacionPolitica": c.get("nombreAgrupacionPolitica", ""),
                    "codigoAgrupacionPolitica": c.get("codigoAgrupacionPolitica", ""),
                    "nombreCandidato":          c.get("nombreCandidato", ""),
                    "dniCandidato":             c.get("dniCandidato", ""),
                }

    total_nacional = sum(aggregate.values())

    results = []
    for key, votos in aggregate.items():
        results.append({
            **cand_info[key],
            "votos_proyectados":    round(votos),
            "porcentaje_proyectado": round(votos / total_nacional * 100, 4) if total_nacional else 0,
        })

    results.sort(key=lambda r: r["votos_proyectados"], reverse=True)

    # Diferencia respecto al siguiente (el último no tiene siguiente)
    for i, r in enumerate(results):
        if i < len(results) - 1:
            siguiente = results[i + 1]
            r["diferencia_votos"]      = r["votos_proyectados"] - siguiente["votos_proyectados"]
            r["diferencia_porcentaje"] = round(
                r["porcentaje_proyectado"] - siguiente["porcentaje_proyectado"], 4
            )
        else:
            r["diferencia_votos"]      = ""
            r["diferencia_porcentaje"] = ""

    # --- Paso 4: guardar CSV ---
    cols = [
        "nombreAgrupacionPolitica", "codigoAgrupacionPolitica",
        "nombreCandidato", "dniCandidato",
        "votos_proyectados", "porcentaje_proyectado",
        "diferencia_votos", "diferencia_porcentaje",
    ]
    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(results)

    # --- Resumen en consola ---
    distritos_usados = len(district_proj)
    print(f"\nProyección completada.")
    print(f"  Distritos usados:        {distritos_usados} / {len(all_ubigeos)}")
    print(f"  Total votos proyectados: {round(total_nacional):,}")
    print(f"  Resultado -> {output_csv}\n")
    print(f"{'Candidato':<45} {'Partido':<35} {'%':>7}  {'Votos':>10}  {'Dif.%':>7}  {'Dif.votos':>10}")
    print("-" * 120)
    for r in results:
        dif_pct   = f"{r['diferencia_porcentaje']:>+7.3f}%" if r["diferencia_porcentaje"] != "" else f"{'':>8}"
        dif_votos = f"{r['diferencia_votos']:>+10,}"        if r["diferencia_votos"]      != "" else f"{'':>10}"
        print(
            f"{r['nombreCandidato']:<45} "
            f"{r['nombreAgrupacionPolitica'][:34]:<35} "
            f"{r['porcentaje_proyectado']:>6.3f}%  "
            f"{r['votos_proyectados']:>10,}  "
            f"{dif_pct}  {dif_votos}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def encontrar_timestamps() -> list[str]:
    """Devuelve todos los timestamps disponibles donde existan ambos CSVs."""
    patron = re.compile(r"totales_distritos_(\d{8}_\d{4})\.csv")
    timestamps = []
    for path in glob.glob("data/totales_distritos_*.csv"):
        m = patron.match(os.path.basename(path))
        if not m:
            continue
        ts = m.group(1)
        if os.path.exists(f"data/participantes_distritos_{ts}.csv"):
            timestamps.append(ts)
    return sorted(timestamps)


def main():
    parser = argparse.ArgumentParser(description="Proyecta resultados electorales al 100%.")
    parser.add_argument("--n-donors", type=int, default=N_DONORS,
                        help=f"Vecinos mas cercanos para imputacion nacional (default: {N_DONORS})")
    parser.add_argument("--solo-peru", action="store_true",
                        help="Proyectar solo distritos nacionales (excluye extranjero)")
    parser.add_argument("--solo-extranjero", action="store_true",
                        help="Proyectar solo distritos extranjeros (excluye nacionales)")
    parser.add_argument("--timestamp", type=str, default=None,
                        help="Timestamp especifico a procesar, e.g. 20260414_0105. "
                             "Si se omite procesa todos los timestamps disponibles.")
    parser.add_argument("--con-regresion", action="store_true",
                        help="Activa la regresion lineal para parciales; por defecto usa solo ratio simple.")
    parser.add_argument("--actas-jee", type=float, default=0.0,
                        help="Porcentaje de enviadasJee (por distrito) que seran anuladas (ej: 2.5 = 2.5%%).")
    args = parser.parse_args()

    all_timestamps = encontrar_timestamps()

    if args.timestamp:
        timestamps = [args.timestamp]
    else:
        timestamps = all_timestamps
        if timestamps:
            print(f"{len(timestamps)} timestamp(s) encontrados: {', '.join(timestamps)}")
        else:
            print("No se encontraron archivos con timestamp — usando archivos base.")
            timestamps = [None]

    for ts in timestamps:
        # Series históricas: todos los snapshots hasta el actual (inclusive)
        if not args.con_regresion or ts is None:
            hist = None
        else:
            hist_ts = [t for t in all_timestamps if t <= ts]
            print(f"\nCargando series históricas ({len(hist_ts)} snapshots)...")
            hist = build_hist_series(hist_ts)

        if args.solo_peru or args.solo_extranjero:
            proyectar(ts, args.n_donors,
                      solo_peru=args.solo_peru,
                      solo_extranjero=args.solo_extranjero,
                      hist_series=hist,
                      actas_jee=args.actas_jee)
        else:
            proyectar(ts, args.n_donors, hist_series=hist, actas_jee=args.actas_jee)
            proyectar(ts, args.n_donors, solo_peru=True,       hist_series=hist, actas_jee=args.actas_jee)
            proyectar(ts, args.n_donors, solo_extranjero=True, hist_series=hist, actas_jee=args.actas_jee)


if __name__ == "__main__":
    main()
