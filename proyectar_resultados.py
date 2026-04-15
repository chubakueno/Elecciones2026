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

INPUT_TOTALES       = "totales_distritos.csv"
INPUT_PARTICIPANTES = "participantes_distritos.csv"
INPUT_CENTROIDES    = "ubigeo_centroides.csv"
OUTPUT_CSV          = "proyeccion_final.csv"

N_DONORS = 2   # valor por defecto; sobreescrito por --n-donors en CLI


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

def proyectar(timestamp: str | None, n_donors: int, solo_peru: bool = False, solo_extranjero: bool = False):
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

    # --- Detectar qué distritos necesitan imputación ---
    def imputation_reason(key: tuple) -> str | None:
        """Devuelve la razón de imputación, o None si el distrito tiene datos."""
        actas_pct   = flt(totales.get(key, {}).get("actasContabilizadas", 0))
        total_votos = sum(inte(c["totalVotosValidos"]) for c in participantes.get(key, []))
        if actas_pct == 0:
            return "0% actas contabilizadas"
        if total_votos == 0:
            return "actas contabilizadas pero 0 votos válidos"
        return None

    def needs_imputation(key: tuple) -> bool:
        return imputation_reason(key) is not None

    valid_set  = build_valid_set(all_ubigeos, needs_imputation)
    to_impute  = [k for k in all_ubigeos if needs_imputation(k)]

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

    for key in valid_set:
        actas_pct = flt(totales[key]["actasContabilizadas"])
        cands     = participantes[key]

        # Estimar total votos válidos usando el candidato de mayor votación
        top = max(cands, key=lambda c: inte(c["totalVotosValidos"]))
        top_votes = inte(top["totalVotosValidos"])
        top_pct   = flt(top["porcentajeVotosValidos"])

        total_validos_actual = top_votes / (top_pct / 100) if top_pct > 0 else 0
        total_proyectado     = total_validos_actual / (actas_pct / 100)

        cands_proy = []
        for c in cands:
            votos_proy = inte(c["totalVotosValidos"]) / (actas_pct / 100)
            cands_proy.append({**c, "votos_proyectados": votos_proy})

        district_proj[key] = {"total": total_proyectado, "cands": cands_proy}

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
    LOG_CSV = f"imputaciones{sufijo}_{timestamp}.csv" if timestamp else f"imputaciones{sufijo}.csv"
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
    for path in glob.glob("totales_distritos_*.csv"):
        m = patron.match(path)
        if not m:
            continue
        ts = m.group(1)
        if os.path.exists(f"participantes_distritos_{ts}.csv"):
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
    args = parser.parse_args()

    if args.timestamp:
        timestamps = [args.timestamp]
    else:
        timestamps = encontrar_timestamps()
        if timestamps:
            print(f"{len(timestamps)} timestamp(s) encontrados: {', '.join(timestamps)}")
        else:
            print("No se encontraron archivos con timestamp — usando archivos base.")
            timestamps = [None]

    if args.solo_peru or args.solo_extranjero:
        # Modo específico
        for ts in timestamps:
            proyectar(ts, args.n_donors, solo_peru=args.solo_peru, solo_extranjero=args.solo_extranjero)
    else:
        # Por defecto: proyectar los tres ámbitos
        for ts in timestamps:
            proyectar(ts, args.n_donors)
            proyectar(ts, args.n_donors, solo_peru=True)
            proyectar(ts, args.n_donors, solo_extranjero=True)


if __name__ == "__main__":
    main()
