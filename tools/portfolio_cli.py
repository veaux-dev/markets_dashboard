import argparse
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Ajustar path para importar módulos del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from svc_v2.db import Database
from svc_v2.config_loader import load_settings, HoldingConfig

def get_db():
    # Asegurar que usamos la DB configurada
    cfg = load_settings()
    db_path = PROJECT_ROOT / "data" / cfg.system.db_filename
    return Database(str(db_path))

def cmd_init_from_config(args):
    """Lee settings.yaml e inserta transacciones iniciales para los holdings definidos."""
    print("🔄 Leyendo configuración de holdings...")
    cfg = load_settings()
    db = get_db()
    
    count = 0
    for h in cfg.portfolios.holdings:
        # Normalizar a objeto HoldingConfig si es string
        if isinstance(h, str):
            ticker = h
            qty = 0
            price = 0
            notes = "Config Load (No details)"
        else:
            ticker = h.ticker
            qty = h.qty
            price = h.avg_price
            notes = h.notes or "Config Load"

        if qty > 0:
            print(f"   -> Procesando {ticker}: {qty} @ {price}")
            # Verificamos si ya existe alguna transacción para no duplicar ciegamente
            # (Aunque idealmente esto se corre una sola vez con la DB limpia)
            db.add_transaction(
                ticker=ticker,
                side="BUY",
                qty=qty,
                price=price,
                notes=f"INIT: {notes}"
            )
            count += 1
        else:
            print(f"   ⚠️ Saltando {ticker} (Qty=0 o no especificada en YAML)")

    print(f"✅ Carga inicial completada. {count} transacciones registradas.")

def cmd_add(args):
    """Agrega una transacción manual."""
    db = get_db()
    
    # Calcular total para confirmar
    total = args.qty * args.price
    print(f"📝 Registrando: {args.side.upper()} {args.qty} {args.ticker} @ ${args.price:.2f} (Total: ${total:.2f})")
    
    confirm = input("¿Confirmar? [y/N]: ")
    if confirm.lower() == 'y':
        db.add_transaction(
            ticker=args.ticker.upper(),
            side=args.side.upper(),
            qty=args.qty,
            price=args.price,
            fees=args.fees,
            notes=args.notes
        )
        print("✅ Guardado.")
    else:
        print("❌ Cancelado.")

def cmd_list(args):
    """Muestra el estado actual del portafolio (Vista Consolidada)."""
    db = get_db()
    try:
        # Consultamos la vista directamente
        df = db.conn.execute("SELECT * FROM view_portfolio_holdings ORDER BY ticker").df()
        
        if df.empty:
            print("📭 Portafolio vacío.")
            return

        # Formateo bonito
        df['avg_buy_price'] = df['avg_buy_price'].map('${:,.2f}'.format)
        
        print("\n📊 PORTAFOLIO ACTUAL (Consolidado):")
        print(df.to_markdown(index=False))
        
        # Calcular valor total estimado (si tuviéramos precio actual, por ahora solo Cost Basis)
        # Esto requeriría cruzar con tabla ohlcv (próximo paso del dashboard)
        
    except Exception as e:
        print(f"❌ Error consultando vista: {e}")

def cmd_history(args):
    """Muestra el historial de transacciones (Ledger)."""
    db = get_db()
    query = "SELECT * FROM portfolio_transactions"
    
    if args.ticker:
        query += f" WHERE ticker = '{args.ticker.upper()}'"
    
    query += " ORDER BY timestamp DESC"
    
    df = db.conn.execute(query).df()
    if df.empty:
        print("📭 Sin transacciones.")
    else:
        print(f"\n📜 HISTORIAL ({len(df)} registros):")
        # Ocultar ID para display limpio
        print(df.drop(columns=['id']).to_markdown(index=False))

def main():
    parser = argparse.ArgumentParser(description="Gestor de Portafolio V2 (CLI)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. Init from Config
    p_init = subparsers.add_parser("init-from-config", help="Cargar holdings desde settings.yaml como compras iniciales")

    # 2. Add Transaction
    p_add = subparsers.add_parser("add", help="Registrar nueva transacción")
    p_add.add_argument("side", choices=["buy", "sell", "dividend"], help="Tipo de operación")
    p_add.add_argument("ticker", help="Símbolo (ej. AAPL)")
    p_add.add_argument("qty", type=float, help="Cantidad")
    p_add.add_argument("price", type=float, help="Precio unitario")
    p_add.add_argument("--fees", type=float, default=0.0, help="Comisiones")
    p_add.add_argument("--notes", type=str, default="", help="Notas opcionales")

    # 3. List Portfolio
    p_list = subparsers.add_parser("list", help="Ver portafolio consolidado")

    # 4. History
    p_hist = subparsers.add_parser("history", help="Ver historial de transacciones")
    p_hist.add_argument("--ticker", help="Filtrar por ticker")

    args = parser.parse_args()

    # Dispatch
    if args.command == "init-from-config":
        cmd_init_from_config(args)
    elif args.command == "add":
        cmd_add(args)
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "history":
        cmd_history(args)

if __name__ == "__main__":
    main()
