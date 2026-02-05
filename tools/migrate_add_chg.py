import duckdb
import sys

db_path = "/mnt/markets_dashboard/Dashboard/data_v2/markets.duckdb"

try:
    print(f"🔌 Conectando a {db_path}...")
    con = duckdb.connect(db_path)
    
    # Verificar si ya existe
    cols = [c[0] for c in con.execute("DESCRIBE indicators").fetchall()]
    if 'chg_pct' in cols:
        print("✅ La columna 'chg_pct' ya existe. No se requiere acción.")
    else:
        print("⚠️ Agregando columna 'chg_pct'...")
        con.execute("ALTER TABLE indicators ADD COLUMN chg_pct DOUBLE")
        print("✅ Columna agregada con éxito.")
        
    con.close()
except Exception as e:
    print(f"❌ Error migrando DB: {e}")
    sys.exit(1)
