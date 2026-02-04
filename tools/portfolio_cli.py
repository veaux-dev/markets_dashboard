import argparse
import sys
import pandas as pd
import csv
import logging
from pathlib import Path
from datetime import datetime

# Ajustar path para importar módulos del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from svc_v2.db import Database
from svc_v2.config_loader import load_settings, HoldingConfig

# Silenciar logs de duckdb si ensucian
logging.getLogger('duckdb').setLevel(logging.WARNING)

def get_db():
    cfg = load_settings()
    db_path = PROJECT_ROOT / "data" / cfg.system.db_filename
    return Database(str(db_path))

def cmd_add(args):
    """Agrega una transacción manual."""
    db = get_db()
    total = args.qty * args.price
    date_str = args.date if args.date else "HOY (Ahora)"
    currency = args.currency.upper()
    
    print(f"📝 Registrando: {args.side.upper()} {args.qty} {args.ticker} @ ${args.price:.2f} {currency} (Total: ${total:.2f})")
    print(f"   📅 Fecha: {date_str}")
    
    confirm = input("¿Confirmar? [y/N]: ")
    if confirm.lower() == 'y':
        db.add_transaction(
            ticker=args.ticker.upper(),
            side=args.side.upper(),
            qty=args.qty,
            price=args.price,
            fees=args.fees,
            notes=args.notes,
            timestamp=args.date,
            currency=currency
        )
        print("✅ Guardado.")
    else:
        print("❌ Cancelado.")

def cmd_import_csv(args):
    """Importa transacciones desde un archivo CSV."""
    path = Path(args.file)
    if not path.exists():
        print(f"❌ Archivo no encontrado: {args.file}")
        return

    print(f"📄 Procesando {args.file}...")
    db = get_db()
    try:
        with open(path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                db.add_transaction(
                    ticker=row['ticker'].strip().upper(),
                    side=row['side'].strip().upper(),
                    qty=float(row['qty']),
                    price=float(row['price']),
                    currency=row.get('currency', 'MXN').strip().upper(),
                    timestamp=row.get('date'),
                    notes=row.get('notes', 'Bulk Import')
                )
                count += 1
        print(f"✅ Importación completada. {count} registros procesados.")
    except Exception as e:
        print(f"❌ Error durante la importación: {e}")

def cmd_list(args):
    """Muestra el estado actual del portafolio (Vista Consolidada)."""
    db = get_db()
    try:
        df = db.conn.execute("SELECT * FROM view_portfolio_holdings ORDER BY ticker").df()
        if df.empty:
            print("📭 Portafolio vacío.")
            return
        if 'avg_buy_price' in df.columns:
             df['avg_buy_price'] = df['avg_buy_price'].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "$0.00")
        print("\n📊 PORTAFOLIO ACTUAL (Consolidado):")
        try:
            print(df.to_markdown(index=False))
        except ImportError:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"❌ Error consultando vista: {e}")

def cmd_history(args):
    """Muestra el historial de transacciones (Ledger)."""
    db = get_db()
    query = "SELECT timestamp, side, ticker, qty, price, currency, notes FROM portfolio_transactions"
    if args.ticker:
        query += f" WHERE ticker = '{args.ticker.upper()}'"
    query += " ORDER BY timestamp DESC"
    try:
        df = db.conn.execute(query).df()
        if df.empty:
            print("📭 Sin transacciones.")
        else:
            print(f"\n📜 HISTORIAL ({len(df)} registros):")
            try:
                print(df.to_markdown(index=False))
            except ImportError:
                print(df.to_string(index=False))
    except Exception as e:
         print(f"❌ Error leyendo historial: {e}")

def cmd_init_from_config(args):
    """Carga inicial desde settings.yaml."""
    print("🔄 Leyendo configuración de holdings...")
    cfg = load_settings()
    db = get_db()
    count = 0
    for h in cfg.portfolios.holdings:
        ticker = h.ticker if hasattr(h, 'ticker') else h
        qty = h.qty if hasattr(h, 'qty') else 0
        price = h.avg_price if hasattr(h, 'avg_price') else 0
        if qty > 0:
            db.add_transaction(ticker=ticker, side="BUY", qty=qty, price=price, notes="INIT from Config")
            count += 1
    print(f"✅ Carga inicial completada. {count} registros.")

def main():
    parser = argparse.ArgumentParser(description="Gestor de Portafolio V2 (CLI)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. Add
    p_add = subparsers.add_parser("add", help="Registrar nueva transacción")
    p_add.add_argument("side", choices=["buy", "sell", "dividend"])
    p_add.add_argument("ticker")
    p_add.add_argument("qty", type=float)
    p_add.add_argument("price", type=float)
    p_add.add_argument("--fees", type=float, default=0.0)
    p_add.add_argument("--notes", type=str, default="")
    p_add.add_argument("--date", type=str, default=None)
    p_add.add_argument("--currency", type=str, default="MXN")

    # 2. Import CSV
    p_imp = subparsers.add_parser("import-csv", help="Carga masiva desde archivo CSV")
    p_imp.add_argument("file", help="Ruta al archivo .csv")

    # 3. List
    subparsers.add_parser("list", help="Ver portafolio")

    # 4. History
    p_hist = subparsers.add_parser("history", help="Ver historial")
    p_hist.add_argument("--ticker")

    # 5. Init
    subparsers.add_parser("init-from-config")

    args = parser.parse_args()
    if args.command == "add": cmd_add(args)
    elif args.command == "import-csv": cmd_import_csv(args)
    elif args.command == "list": cmd_list(args)
    elif args.command == "history": cmd_history(args)
    elif args.command == "init-from-config": cmd_init_from_config(args)

if __name__ == "__main__":
    main()