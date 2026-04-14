"""
mapa_imputaciones.py
--------------------
Genera mapa_imputaciones.html con los distritos coloreados según
cómo fueron tratados en la proyección electoral:

  Verde claro  — proyectado con datos propios
  Naranja      — imputado por centroide geográfico más cercano
  Rojo         — imputado por proximidad numérica (provincia/departamento)
  Morado       — imputado por perfil global extranjero
  Gris         — sin polígono en el GeoJSON (distrito posterior a 2016)

Uso:
    pip install folium
    python mapa_imputaciones.py
"""

import argparse
import csv
import json
import folium
from folium.features import GeoJsonTooltip

# ---------------------------------------------------------------------------
# Archivos
# ---------------------------------------------------------------------------
GEOJSON_FILE    = "peru_distrital.geojson"
CENTROIDES_FILE = "ubigeo_centroides.csv"
IMPUT_FILE      = "imputaciones.csv"
UBIGEOS_FILE    = "ubigeos_completo.csv"
OUTPUT_HTML     = "mapa_imputaciones.html"

# ---------------------------------------------------------------------------
# Colores por categoría de imputación
# ---------------------------------------------------------------------------
COLORES = {
    "datos propios":           "#a8d5a2",   # verde claro
    "centroide":               "#f4a261",   # naranja
    "provincia":               "#e63946",   # rojo
    "departamento":            "#e63946",   # rojo (mismo que provincia)
    "perfil global extranjero":"#9b5de5",   # morado
    "sin referencia":          "#aaaaaa",   # gris
}

LEYENDA = {
    "Datos propios":              COLORES["datos propios"],
    "Imputado — centroide":       COLORES["centroide"],
    "Imputado — prov/depto":      COLORES["provincia"],
    "Imputado — perfil extranjero": COLORES["perfil global extranjero"],
    "Sin referencia / sin polígono": COLORES["sin referencia"],
}


def scope_a_categoria(scope: str) -> str:
    s = scope.lower()
    if s.startswith("centroide"):
        return "centroide"
    if "provincia" in s:
        return "provincia"
    if "departamento" in s:
        return "departamento"
    if "global" in s or "extranjero" in s:
        return "perfil global extranjero"
    return "sin referencia"


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

def load_inei_to_reniec(path: str) -> dict[str, str]:
    """Mapeo inei→reniec desde ubigeo_centroides.csv."""
    m = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                inei   = str(row["inei"]).strip().zfill(6)
                reniec = str(row["reniec"]).strip().zfill(6)
                m[inei] = reniec
    except FileNotFoundError:
        print(f"  [warn] No se encontró {path}")
    return m


def load_imputaciones(path: str) -> dict[str, dict]:
    """ubigeo_reniec → fila de imputaciones."""
    imp = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                imp[row["ubigeo"].zfill(6)] = row
    except FileNotFoundError:
        print(f"  [warn] No se encontró {path}")
    return imp


def load_todos_ubigeos(path: str) -> set[str]:
    """Conjunto de todos los ubigeos nacionales conocidos."""
    ubigeos = set()
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("id_ambito_geografico", "1") == "1":
                    ubigeos.add(row["ubigeo_distrito"].zfill(6))
    except FileNotFoundError:
        pass
    return ubigeos


def load_actas(path: str) -> dict[str, str]:
    """ubigeo -> actasContabilizadas desde totales_distritos.csv."""
    actas = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                ub = row.get("ubigeo_distrito", "").zfill(6)
                actas[ub] = row.get("actasContabilizadas", "")
    except FileNotFoundError:
        print(f"  [warn] No se encontro {path}, actas no disponibles para verdes")
    return actas


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Genera mapa de imputaciones.")
    parser.add_argument("--timestamp", type=str, default=None,
                        help="Timestamp de los archivos a usar, e.g. 20260414_0105.")
    args = parser.parse_args()

    def con_ts(base: str) -> str:
        if args.timestamp:
            return base.replace(".csv", f"_{args.timestamp}.csv").replace(".html", f"_{args.timestamp}.html")
        return base

    imput_file   = con_ts(IMPUT_FILE)
    totales_file = con_ts("totales_distritos.csv")
    output_html  = con_ts(OUTPUT_HTML)

    print(f"Cargando datos... (timestamp={args.timestamp or 'latest'})")
    inei_to_reniec = load_inei_to_reniec(CENTROIDES_FILE)
    imputaciones   = load_imputaciones(imput_file)
    todos_ubigeos  = load_todos_ubigeos(UBIGEOS_FILE)
    actas_dict     = load_actas(totales_file)

    with open(GEOJSON_FILE, encoding="utf-8") as f:
        geojson = json.load(f)

    print(f"  {len(geojson['features'])} polígonos en GeoJSON")
    print(f"  {len(imputaciones)} distritos imputados")
    print(f"  {len(todos_ubigeos)} distritos nacionales totales")

    # Enriquecer cada feature con categoría y metadatos
    conteo = {k: 0 for k in COLORES}

    for feat in geojson["features"]:
        props  = feat["properties"]
        iddist = str(props.get("IDDIST", "")).zfill(6)

        # Convertir INEI → RENIEC (código que usa ONPE)
        reniec = inei_to_reniec.get(iddist, iddist)

        if reniec in imputaciones:
            imp  = imputaciones[reniec]
            cat  = scope_a_categoria(imp.get("scope", ""))
            props["_categoria"]    = cat
            props["_scope"]        = imp.get("scope", "")
            props["_razon"]        = imp.get("razon", "")
            props["_donor_ubigeo"] = imp.get("donor_ubigeo", "")
            props["_donor_nombre"] = imp.get("donor_nombre", "")
            props["_actas_pct"]    = imp.get("actas_pct", "")
        elif reniec in todos_ubigeos:
            cat = "datos propios"
            props["_categoria"]    = cat
            props["_scope"]        = "datos propios"
            props["_razon"]        = ""
            props["_donor_ubigeo"] = ""
            props["_donor_nombre"] = ""
            props["_actas_pct"]    = actas_dict.get(reniec, "")
        else:
            cat = "sin referencia"
            props["_categoria"]    = cat
            props["_scope"]        = "sin polígono coincidente"
            props["_razon"]        = ""
            props["_donor_ubigeo"] = ""
            props["_donor_nombre"] = ""
            props["_actas_pct"]    = ""

        props["_color"] = COLORES.get(cat, COLORES["sin referencia"])
        conteo[cat] = conteo.get(cat, 0) + 1

    for cat, n in conteo.items():
        if n:
            print(f"  {cat}: {n}")

    # ---------------------------------------------------------------------------
    # Mapa con Folium
    # ---------------------------------------------------------------------------
    mapa = folium.Map(
        location=[-9.2, -75.0],
        zoom_start=6,
        tiles="CartoDB positron",
    )

    def style_fn(feature):
        return {
            "fillColor":   feature["properties"]["_color"],
            "color":       "#555555",
            "weight":      0.4,
            "fillOpacity": 0.75,
        }

    def highlight_fn(feature):
        return {
            "fillColor": feature["properties"]["_color"],
            "color":     "#000000",
            "weight":    1.5,
            "fillOpacity": 0.95,
        }

    folium.GeoJson(
        geojson,
        name="Distritos",
        style_function=style_fn,
        highlight_function=highlight_fn,
        tooltip=GeoJsonTooltip(
            fields=[
                "NOMBDIST", "NOMBPROV", "NOMBDEP",
                "_scope", "_razon", "_actas_pct",
                "_donor_nombre", "_donor_ubigeo",
            ],
            aliases=[
                "Distrito", "Provincia", "Departamento",
                "Método", "Razón", "Actas contabilizadas (%)",
                "Donor", "Ubigeo donor",
            ],
            localize=True,
            sticky=True,
        ),
    ).add_to(mapa)

    # Leyenda HTML
    leyenda_html = """
    <div style="
        position: fixed; bottom: 30px; left: 30px; z-index: 9999;
        background: white; padding: 12px 16px; border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3); font-family: sans-serif;
        font-size: 13px; line-height: 1.8;
    ">
        <b style="font-size:14px;">Método de imputación</b><br>
    """
    for label, color in LEYENDA.items():
        leyenda_html += (
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{color};border-radius:3px;margin-right:6px;'
            f'vertical-align:middle;"></span>{label}<br>'
        )
    leyenda_html += "</div>"

    mapa.get_root().html.add_child(folium.Element(leyenda_html))

    folium.LayerControl().add_to(mapa)

    mapa.save(output_html)
    print(f"\nMapa guardado -> {output_html}")


if __name__ == "__main__":
    main()
