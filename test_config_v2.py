from svc_v2.config_loader import load_settings

try:
    cfg = load_settings()
    print("✅ Config loaded successfully")
    print("Holdings:")
    for h in cfg.portfolios.holdings:
        print(f"  - {h.ticker}: Qty={h.qty}, Price={h.avg_price}")
except Exception as e:
    print(f"❌ Error: {e}")
