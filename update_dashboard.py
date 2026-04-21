#!/usr/bin/env python3
"""
update_dashboard.py
===================
Consulta Snowflake y regenera dashboard_poked_noahs.html con datos frescos.

USO LOCAL:
  pip install snowflake-connector-python
  export SNOWFLAKE_ACCOUNT=...  SNOWFLAKE_USER=...  SNOWFLAKE_PASSWORD=...
  export SNOWFLAKE_WAREHOUSE=... SNOWFLAKE_DATABASE=... SNOWFLAKE_SCHEMA=...
  python update_dashboard.py

USO EN CI (GitHub Actions):
  Los secrets se inyectan como variables de entorno automáticamente.
"""

import os
import re
import json
import datetime
from pathlib import Path

# ── Snowflake ────────────────────────────────────────────────────
try:
    import snowflake.connector
    SF_AVAILABLE = True
except ImportError:
    SF_AVAILABLE = False
    print("⚠️  snowflake-connector-python no instalado. Usando datos demo.")

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════

PARTNER_IDS = {
    "PK_REG":   648300,   # Poked — Regular
    "PK_TURBO": 650709,   # Poked — Turbo
    "NK_REG":   648301,   # Noah's Green Kitchen — Regular
    "NK_TURBO": 650702,   # Noah's Green Kitchen — Turbo
}

# Meses a incluir (últimos N meses completos + mes en curso como parcial)
MONTHS_BACK = 6

TEMPLATE_PATH = Path(__file__).parent / "dashboard_poked_noahs.html"
OUTPUT_PATH   = TEMPLATE_PATH  # sobreescribe el mismo archivo (en CI se hace commit)


# ══════════════════════════════════════════════════════════════════
#  SNOWFLAKE CONNECTION
# ══════════════════════════════════════════════════════════════════

def get_connection():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE", "RAPPI_MX"),
        schema    = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role      = os.environ.get("SNOWFLAKE_ROLE", ""),
    )


# ══════════════════════════════════════════════════════════════════
#  QUERIES
# ══════════════════════════════════════════════════════════════════

GROWTH_QUERY = """
SELECT
    PARTNER_ID,
    DATE_TRUNC('month', DATE)                          AS MES,
    SUM(ORDERS)                                        AS ORDERS,
    SUM(UNIQUE_USERS)                                  AS UNIQUE_USERS,
    SUM(SALES_ML)                                      AS SALES_ML_USD,
    SUM(SESSION_STARTS)                                AS SESSION_STARTS,
    SUM(PLACED_ORDERS)                                 AS PLACED_ORDERS,
    SUM(MKD)                                           AS MKD_USD
FROM FOLLOW_UP_GROWTH_METRICS_DIMENSIONS
WHERE PARTNER_ID IN ({ids})
  AND DATE >= DATEADD('month', -{months_back}, DATE_TRUNC('month', CURRENT_DATE()))
GROUP BY 1, 2
ORDER BY 1, 2
""".strip()

OPS_QUERY = """
SELECT
    PARTNER_ID,
    DATE_TRUNC('month', DATE)                          AS MES,
    SUM(ORDERS)                                        AS ORDERS,
    AVG(DEFECT_RATE)                                   AS DEFECT_RATE,
    AVG(CANCEL_RATE)                                   AS CANCEL_RATE,
    AVG(AVAILABILITY)                                  AS AVAILABILITY,
    AVG(COOKING_TIME_MIN)                              AS COOKING_TIME_MIN,
    AVG(DELIVERY_TIME_MIN)                             AS DELIVERY_TIME_MIN
FROM FOLLOW_UP_OPS_METRICS
WHERE PARTNER_ID IN ({ids})
  AND DATE >= DATEADD('month', -{months_back}, DATE_TRUNC('month', CURRENT_DATE()))
GROUP BY 1, 2
ORDER BY 1, 2
""".strip()


def run_query(cur, sql):
    """Ejecuta query y devuelve lista de dicts."""
    cur.execute(sql)
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ══════════════════════════════════════════════════════════════════
#  DATA PROCESSING
# ══════════════════════════════════════════════════════════════════

def month_label(dt):
    """datetime/date → 'Ene 2026' etc."""
    MESES_ES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    if isinstance(dt, str):
        dt = datetime.date.fromisoformat(str(dt)[:10])
    suffix = '*' if dt.month == datetime.date.today().month and dt.year == datetime.date.today().year else ''
    return f"{MESES_ES[dt.month-1]} {dt.year}{suffix}"


def build_growth_series(rows, partner_id):
    """
    Filtra filas para un partner_id y devuelve lista de arrays JS:
    [mes_label, orders, unique_users, sales_ml_usd, session_starts, placed_orders, mkd_usd]
    """
    filtered = sorted([r for r in rows if int(r['partner_id']) == partner_id],
                      key=lambda r: str(r['mes']))
    result = []
    for r in filtered:
        result.append([
            month_label(r['mes']),
            int(r.get('orders') or 0),
            int(r.get('unique_users') or 0),
            round(float(r.get('sales_ml_usd') or 0), 2),
            int(r.get('session_starts') or 0),
            int(r.get('placed_orders') or 0),
            round(float(r.get('mkd_usd') or 0), 2),
        ])
    return result


def build_ops_series(rows, partner_id, partner_name, brand_key):
    """
    Devuelve dict compatible con OPS[n] del HTML:
    {id, name, brand, monthly: [[mes,orders,defect,cancel,ava,cook,del], ...]}
    """
    filtered = sorted([r for r in rows if int(r['partner_id']) == partner_id],
                      key=lambda r: str(r['mes']))
    monthly = []
    for r in filtered:
        monthly.append([
            month_label(r['mes']),
            int(r.get('orders') or 0),
            round(float(r.get('defect_rate') or 0), 2),
            round(float(r.get('cancel_rate') or 0), 2),
            round(float(r.get('availability') or 0), 4),
            round(float(r.get('cooking_time_min') or 0), 1),
            round(float(r.get('delivery_time_min') or 0), 1),
        ])
    return {"id": str(partner_id), "name": partner_name, "brand": brand_key, "monthly": monthly}


def build_meses(growth_rows):
    """Extrae lista ordenada de labels de meses desde las filas de growth."""
    labels_set = set()
    for r in growth_rows:
        labels_set.add((str(r['mes']), month_label(r['mes'])))
    return [label for _, label in sorted(labels_set)]


# ══════════════════════════════════════════════════════════════════
#  FALLBACK DEMO DATA
# ══════════════════════════════════════════════════════════════════

def demo_data():
    """Devuelve datos demo cuando no hay conexión a Snowflake."""
    print("📋 Usando datos demo (sin conexión a Snowflake)")
    meses = ["Nov 2025","Dic 2025","Ene 2026","Feb 2026","Mar 2026","Abr 2026*"]
    growth = {
        "PK_REG":   [["Nov 2025",1820,1780,18450,11200,2010,0],["Dic 2025",2450,2380,24800,15600,2710,320],["Ene 2026",2980,2890,30200,18900,3290,480],["Feb 2026",3210,3120,32500,20400,3540,560],["Mar 2026",3580,3470,36200,22800,3950,620],["Abr 2026*",2100,2040,21300,14600,2320,380]],
        "PK_TURBO": [["Nov 2025",680,660,6900,3800,750,0],["Dic 2025",920,890,9300,5200,1010,0],["Ene 2026",1120,1080,11350,6300,1230,0],["Feb 2026",1240,1200,12580,7000,1360,0],["Mar 2026",1380,1340,13980,7800,1510,0],["Abr 2026*",810,780,8200,4600,890,0]],
        "NK_REG":   [["Nov 2025",1240,1210,12580,8200,1370,0],["Dic 2025",1680,1640,17050,11100,1850,180],["Ene 2026",2050,1990,20800,13500,2260,260],["Feb 2026",2310,2250,23450,15200,2540,310],["Mar 2026",2620,2560,26600,17200,2890,380],["Abr 2026*",1530,1490,15550,10800,1690,220]],
        "NK_TURBO": [["Nov 2025",490,475,4970,2900,540,0],["Dic 2025",660,640,6700,3900,730,0],["Ene 2026",810,785,8210,4800,890,0],["Feb 2026",910,880,9230,5400,1000,0],["Mar 2026",1020,990,10350,6000,1120,0],["Abr 2026*",600,580,6080,3600,660,0]],
    }
    ops = [
        {"id":"648300","name":"Poked Regular","brand":"PK","monthly":[["Nov 2025",1820,4.8,1.4,0.94,13.2,41.5],["Dic 2025",2450,4.2,1.1,0.96,13.8,43.2],["Ene 2026",2980,3.9,1.0,0.97,13.5,42.1],["Feb 2026",3210,3.6,0.9,0.97,14.1,43.8],["Mar 2026",3580,3.3,0.8,0.98,13.9,42.6],["Abr 2026*",2100,3.5,0.9,0.97,14.0,41.9]]},
        {"id":"650709","name":"Poked Turbo","brand":"PK_TURBO","monthly":[["Nov 2025",680,4.1,1.0,0.95,11.5,33.2],["Dic 2025",920,3.8,0.9,0.96,11.8,34.1],["Ene 2026",1120,3.5,0.8,0.97,11.6,33.8],["Feb 2026",1240,3.2,0.7,0.98,11.9,34.5],["Mar 2026",1380,3.0,0.6,0.98,11.7,33.9],["Abr 2026*",810,3.1,0.7,0.97,11.8,34.2]]},
        {"id":"648301","name":"Noah's Green Kitchen Regular","brand":"NK","monthly":[["Nov 2025",1240,4.5,1.3,0.93,15.8,44.2],["Dic 2025",1680,4.0,1.1,0.95,16.2,45.1],["Ene 2026",2050,3.8,1.0,0.96,15.9,44.8],["Feb 2026",2310,3.5,0.9,0.96,16.4,45.6],["Mar 2026",2620,3.2,0.8,0.97,16.1,44.9],["Abr 2026*",1530,3.4,0.9,0.96,16.3,45.2]]},
        {"id":"650702","name":"Noah's Green Kitchen Turbo","brand":"NK_TURBO","monthly":[["Nov 2025",490,3.8,0.8,0.96,13.1,35.8],["Dic 2025",660,3.5,0.7,0.97,13.4,36.2],["Ene 2026",810,3.2,0.7,0.98,13.2,35.9],["Feb 2026",910,3.0,0.6,0.98,13.5,36.5],["Mar 2026",1020,2.8,0.5,0.99,13.3,36.1],["Abr 2026*",600,3.0,0.6,0.98,13.4,36.3]]},
    ]
    return meses, growth, ops


# ══════════════════════════════════════════════════════════════════
#  SNOWFLAKE FETCH
# ══════════════════════════════════════════════════════════════════

def fetch_from_snowflake():
    ids_str = ",".join(str(v) for v in PARTNER_IDS.values())
    conn = get_connection()
    cur = conn.cursor()
    try:
        print("📡 Consultando FOLLOW_UP_GROWTH_METRICS_DIMENSIONS...")
        growth_rows = run_query(cur, GROWTH_QUERY.format(
            ids=ids_str, months_back=MONTHS_BACK))
        print(f"   {len(growth_rows):,} filas growth")

        print("📡 Consultando FOLLOW_UP_OPS_METRICS...")
        ops_rows = run_query(cur, OPS_QUERY.format(
            ids=ids_str, months_back=MONTHS_BACK))
        print(f"   {len(ops_rows):,} filas ops")
    finally:
        cur.close()
        conn.close()

    meses = build_meses(growth_rows)
    growth = {
        key: build_growth_series(growth_rows, pid)
        for key, pid in PARTNER_IDS.items()
    }

    store_meta = {
        "PK_REG":   (PARTNER_IDS["PK_REG"],   "Poked Regular",               "PK"),
        "PK_TURBO": (PARTNER_IDS["PK_TURBO"], "Poked Turbo",                 "PK_TURBO"),
        "NK_REG":   (PARTNER_IDS["NK_REG"],   "Noah's Green Kitchen Regular", "NK"),
        "NK_TURBO": (PARTNER_IDS["NK_TURBO"], "Noah's Green Kitchen Turbo",  "NK_TURBO"),
    }
    ops = [build_ops_series(ops_rows, pid, name, brand)
           for pid, name, brand in store_meta.values()]

    return meses, growth, ops


# ══════════════════════════════════════════════════════════════════
#  INJECT INTO HTML
# ══════════════════════════════════════════════════════════════════

def js_array(data):
    return json.dumps(data, ensure_ascii=False, separators=(',', ':'))


def inject(html: str, meses, growth, ops, updated_at: str) -> str:
    """Reemplaza los bloques de datos en el HTML con los datos frescos."""

    def replace_const(html, name, value_js):
        # Matches: const NAME = ...;  (single line or simple)
        pattern = rf'(const {re.escape(name)}\s*=\s*)([^;]+?)(;)'
        replacement = rf'\g<1>{value_js}\g<3>'
        new_html, count = re.subn(pattern, replacement, html, count=1)
        if count == 0:
            print(f"⚠️  No se encontró const {name} en el HTML")
        return new_html

    def replace_multiline_const(html, name, value_js):
        # Matches multiline: const NAME = { ... };
        pattern = rf'(const {re.escape(name)}\s*=\s*)\{{.*?\}}(\s*;)'
        replacement = rf'\g<1>{value_js}\g<2>'
        new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)
        if count == 0:
            print(f"⚠️  No se encontró const {name} (multiline) en el HTML")
        return new_html

    def replace_ops_array(html, value_js):
        pattern = r'(const OPS\s*=\s*)\[.*?\](\s*;)'
        replacement = rf'\g<1>{value_js}\g<2>'
        new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)
        if count == 0:
            print("⚠️  No se encontró const OPS en el HTML")
        return new_html

    html = replace_const(html, "UPDATED_AT", f'"{updated_at}"')
    html = replace_const(html, "MESES", js_array(meses))

    # GROWTH object — replace each sub-key
    growth_lines = []
    for key, series in growth.items():
        growth_lines.append(f'  {key}: {js_array(series)}')
    growth_js = '{\n' + ',\n'.join(growth_lines) + '\n}'
    html = replace_multiline_const(html, "GROWTH", growth_js)

    # OPS array
    ops_js = js_array(ops)
    html = replace_ops_array(html, ops_js)

    return html


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    updated_at = datetime.date.today().isoformat()
    print(f"\n🚀 update_dashboard.py — {updated_at}")

    has_env = all(os.environ.get(k) for k in
                  ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"])

    if SF_AVAILABLE and has_env:
        meses, growth, ops = fetch_from_snowflake()
    else:
        if not has_env:
            print("⚠️  Variables de entorno de Snowflake no encontradas.")
        meses, growth, ops = demo_data()

    print(f"\n📝 Leyendo template: {TEMPLATE_PATH}")
    html = TEMPLATE_PATH.read_text(encoding="utf-8")

    print("💉 Inyectando datos...")
    html = inject(html, meses, growth, ops, updated_at)

    print(f"💾 Escribiendo: {OUTPUT_PATH}")
    OUTPUT_PATH.write_text(html, encoding="utf-8")

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\n✅ Dashboard actualizado — {size_kb:.0f} KB")
    print(f"   Fecha:    {updated_at}")
    print(f"   Meses:    {len(meses)}  ({meses[0]} → {meses[-1]})")
    print(f"   Partners: {', '.join(str(v) for v in PARTNER_IDS.values())}")


if __name__ == "__main__":
    main()
