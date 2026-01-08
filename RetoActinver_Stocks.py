import builtins, pandas as pd
from svc import notifier, collector, analyzer,test
from svc.load_config import discord_webhook_url_reto
from typing import Dict, Any
import numpy as np
from pandas._typing import FormattersType
from datetime import datetime
import math
#from IPython import embed

# notifier.send_discord(webhook_url=discord_webhook_url_reto,msg='### test')
# notifier.send_discord(webhook_url=discord_webhook_url_reto,msg='## test')
# notifier.send_discord(webhook_url=discord_webhook_url_reto,msg='# test')
# notifier.send_discord(webhook_url=discord_webhook_url_reto,msg='-# test')

# Lista de tickers
tickers = [
    "AA1.MX","AAL.MX","AAPL.MX","ABBV.MX","ABNB.MX","AC.MX","ACTINVRB.MX",      #STOCKS
    "AFRM.MX","AGNC.MX","ALFAA.MX","ALPEKA.MX","ALSEA.MX","AMAT.MX","AMD.MX",
    "AMXB.MX","AMZN.MX","ASURB.MX","AVGO.MX","AXP.MX","BA.MX","BABA.MX","BAC.MX",
    "BBAJIOO.MX","BIMBOA.MX","BMY.MX","BOLSAA.MX","BRKB.MX","C.MX","CAT.MX",
    "CCL1N.MX","CEMEXCPO.MX","CHDRAUIB.MX","CLF.MX","COST.MX","CPE.MX","CRM.MX",
    "CSCO.MX","CUERVO.MX","CVS.MX","CVX.MX","DAL.MX","DIS.MX","DVN.MX",
    "ELEKTRA.MX","ETSY.MX","F.MX","FANG.MX","FCX.MX","FDX.MX","FEMSAUBD.MX",
    "FIBRAMQ12.MX","FIBRAPL14.MX","FSLR.MX","FUBO.MX","FUNO11.MX","GAPB.MX",
    "GCARSOA1.MX","GCC.MX","GE.MX","GENTERA.MX","GFINBURO.MX","GFNORTEO.MX",
    "GM.MX","GME.MX","GMEXICOB.MX","GOLDN.MX","GOOGL.MX","GRUMAB.MX","HD.MX",
    "INTC.MX","JNJ.MX","JPM.MX","KIMBERA.MX","KO.MX","KOFUBL.MX","LABB.MX",
    "LASITEB-1.MX","LCID.MX","LIVEPOLC-1.MX","LLY.MX","LUV.MX","LVS.MX","MA.MX",
    "MARA.MX","MCD.MX","MEGACPO.MX","MELIN.MX","META.MX","MFRISCOA-1.MX",
    "MRK.MX","MRNA.MX","MRO.MX","MSFT.MX","MU.MX","NCLHN.MX","NFLX.MX",
    "NKE.MX","NUN.MX","NVAX.MX","NVDA.MX","OMAB.MX","ORBIA.MX","ORCL.MX",
    "OXY1.MX","PARA.MX","PE&OLES.MX","PEP.MX","PFE.MX","PG.MX","PINFRA.MX",
    "PINS.MX","PLTR.MX","PYPL.MX","Q.MX","QCOM.MX","RA.MX","RCL.MX","RIOT.MX",
    "RIVN.MX","SBUX.MX","SHOPN.MX","SITES1A-1.MX","SOFI.MX","SPCE.MX","T.MX",
    "TERRA13.MX","TGT.MX","TLEVISACPO.MX","TMO.MX","TSLA.MX","TSMN.MX","TX.MX",
    "UAL.MX","UBER.MX","UNH.MX","UPST.MX","V.MX","VESTA.MX","VOLARA.MX","VZ.MX",
    "WALMEX.MX","WFC.MX","WMT.MX","XOM.MX","XYZ.MX","ZM.MX",
    "AAXJ.MX","ACWI.MX","ANGELD.MX","BIL.MX","BOTZ.MX","DIA.MX","DIABLOI.MX", #ETFS
    "EEM.MX","EWZ.MX","FAS.MX","FAZ.MX","GDX.MX","GLD.MX","IAU.MX","ICLN.MX",
    "INDA.MX","IVV.MX","KWEB.MX","LIT.MX","MCHI.MX","NAFTRACISHRS.MX","PSQ.MX",
    "QCLN.MX","QLD.MX","QQQ.MX","SHV.MX","SHY.MX","SLV.MX","SOXL.MX","SOXS.MX",
    "SOXX.MX","SPLG.MX","SPXL.MX","SPXS.MX","SPY.MX","SQQQ.MX","TAN.MX","TECL.MX",
    "TECS.MX","TLT.MX","TNA.MX","TQQQ.MX","TZA.MX","USO.MX","VEA.MX","VGT.MX","VNQ.MX",
    "VOO.MX","VT.MX","VTI.MX","VWO.MX","VYM.MX","XLE.MX","XLF.MX","XLK.MX","XLV.MX",
    "ACTDUALB.MX","ACTI500B.MX","ACTICOBB.MX","ACTICREB.MX","ACTIG+B.MX","ACTIG+2B.MX", #FONDOS
    "ACTIGOBB.MX","ACTIMEDB.MX","ACTIPLUB.MX","ACTIRENB.MX","ACTIVARB.MX","ALTERNB.MX",
    "DIGITALB.MX","DINAMOB.MX","ESCALAB.MX","ESFERAB.MX","JPMRVUSB.MX","MAXIMOB.MX",
    "MAYAB.MX","OPORT1B.MX","ROBOTIKB.MX","SALUDB.MX","TEMATIKB.MX",
]

# # Revisar existencia en Yahoo Finance
# data = []
# for t in tickers:
#     try:
#         info = yf.Ticker(t).info
#         name = info.get("shortName", "")
#         sector = info.get("sector", "")
#         data.append([t, bool(name), name, sector])
#     except Exception as e:
#         data.append([t, False, "", ""])

# # Guardar a Excel
# df = pd.DataFrame(data, columns=["Ticker", "ExistsYF", "CompanyName", "Sector"])
# df.to_excel("tickers_verificados.xlsx", index=False)

DictOfDict = Dict[str, Dict[str, pd.DataFrame]]

reto_db : DictOfDict

reto_db =collector.download_tickers(tickers=tickers, output_path='data')


reto_db = collector.save_local(reto_db,'data')


reto_db = analyzer.analyse_data(reto_db)


reto_db = analyzer.det_buy_sell(reto_db)

# notifier.send_discord(discord_webhook_url_reto,'hello')

message_discord=""
urgentmessage="Urgent signals:\n"

df_out: list[dict[str, Any]] = []

message_discord=f"📊**Market Snapshot** - {'now'}\n\n"
message_discord+=f"```{'TICKER':<12} {'CLOSE':>8} {'BIAS':^4} {'Δ-1d':>5} {'Δ-2d':>5} {'Δ-5d':>5} {'Δ-lf':>5} {'Δ-pf':>5} {'RSI':>3} {'MACD':>5} {'Support':>9} {'Resist.':>9}  |  {'PhaseW':>4}   {'Force%':>4}    {'Sup60':>4}    {'Res60':>4}" + "\n"


for ticker in tickers:
    try:

        # 1) Anotar por timeframe (si alguno no existe, sáltalo con try/except fino)
        for tf in ("1d","1h","2h","15m"):
            try:
                reto_db[ticker][tf] = test.annotate_weinstein(reto_db[ticker][tf])
            except Exception:
                pass  # no bloquees el resto si falta un TF
        

        fund = collector.GetTickerInfo(ticker)   # dict con las claves finales

        # después de annotate:
        df1d = reto_db[ticker]["1d"]
        lastrecord=df1d.tail(1)


        # 2) Bias
        bias_val = lastrecord['bias'].iloc[-1]
        signalmsg = "🟢" if bias_val == "buy" else "🔴" if bias_val == "sell" else "⚪"

        # 3) Deltas diarios (tu función)
        d1, d2, d5, df1, df2 = analyzer.get_deltas(reto_db,ticker=ticker,timeframe="1d", weekly_mode='friday')

        # 4) Donchian (proxis de soporte y resistencia)
        donch_low  = lastrecord['donchian_low'].iloc[-1]
        donch_high = lastrecord['donchian_high'].iloc[-1]
        
        # 5) Intradía + MACD slope
        snap = test.get_intraday_snapshot(reto_db, ticker, intra_tf="1h")  # como definimos antes
        macd_icon, macd_slope = test.macd_slope_icon(reto_db[ticker]["1d"]["macd_hist"])
        
        # 6) pw desde columnas (evita recomputar)
        _last = df1d.iloc[-1]
        pw = {
                "fase":         _last["fase"],
                "fuerza_%":     float(_last["fuerza_%"]),
                "soporte":      float(_last.get("soporte20", _last.get("donchian_low", float("nan")))),
                "resistencia":  float(_last.get("resistencia20", _last.get("donchian_high", float("nan")))),
                "m50":          float(_last["ma50"]),
                "m200":         float(_last["ma200"]),
                "slope200":     float(_last["slope200"] * 100.0),
                "dias_en_fase": int(_last["dias_en_fase"]),
                "fase_previa":  _last["fase_previa"],
                "fase_cambio":  bool(_last["fase_cambio"]),
                "U2_entry":     bool(_last["U2_entry"]),
                "support_20" :    float(_last.get('donchian20_low',  _last.get('donchian_low',  float('nan')))),
                "resist_20"  :    float(_last.get('donchian20_high', _last.get('donchian_high', float('nan')))),
                "sup_60"     :    float(_last.get('donchian60_low',  float('nan'))),
                "res_60"     :    float(_last.get('donchian60_high', float('nan'))),
                "macd_slope3_norm": float(_last['macd_slope3_norm'])
        }

        # === Cálculos para flags y % (usando tus mismas vars) ===
        _close       = float(lastrecord['close'].iloc[-1])
        _ma50        = float(pw["m50"])
        _ma200       = float(pw["m200"])
        _res20       = float(pw["resist_20"])
        _res60       = float(pw["res_60"])
        _macdh_last  = float(lastrecord['macd_hist'].iloc[-1])
        _slope_norm  = float(pw["macd_slope3_norm"])

        def _ok(x):
            try:
                return not math.isnan(float(x))
            except:
                return False

        AboveMA50     = (_ok(_ma50)  and _close >= _ma50)
        AboveMA200    = (_ok(_ma200) and _close >= _ma200)
        BreakRes20    = (_ok(_res20) and _close >= _res20)
        BreakRes60    = (_ok(_res60) and _close >= _res60)
        ReaceleraMACD = (_ok(_macdh_last) and _macdh_last > 0) and (_ok(_slope_norm) and _slope_norm > 0)
        DistMA50_pct  = ((_close / _ma50 - 1.0) * 100.0) if _ok(_ma50) else float("nan")
        #message+=f"<b>{ticker}</b> = Value:{lastrecord['close'].iloc[-1]:.2f} {signalmsg} RSI:{lastrecord['rsi'].iloc[-1]:.0f} MACD:{lastrecord['macd_hist'].iloc[-1]:.2f}" + "\n"
        # message_discord+=(
        #         f"{ticker:<14} "
        #         f"{lastrecord['close'].iloc[-1]:>8.2f} "
        #         f"{signalmsg:^4} "
        #         f"{d1:>+5.1f}% {d2:>+5.1f}% {d5:>+5.1f}% {df1:>+5.1f}% {df2:>+5.1f}% "
        #         f"{lastrecord['rsi'].iloc[-1]:>3.0f} {lastrecord['macd_hist'].iloc[-1]:>+5.1f}" 
        #         f"{donch_low:>9.2f} {donch_high:>9.2f}"
        #         f"   |   {pw["fase"]:<6} {pw["fuerza_%"]:>+3.1f}%  {pw["soporte"]:>9.2f}  {pw["resistencia"]:>9.2f}"
        #         "\n")
        
        df_out.append({
        'Ticker': ticker,

        # === Precio diario (tu campo actual) ===
        'Close': round(lastrecord['close'].iloc[-1], 2),
        'Bias': signalmsg,

        # === Deltas diarios que ya calculas ===
        "Δ-1d": d1, "Δ-2d": d2, "Δ-5d": d5, "Δ-lf": df1, "Δ-pf": df2,

        # === Indicadores diarios actuales ===
        "RSI": lastrecord['rsi'].iloc[-1],
        "MACDh": lastrecord['macd_hist'].iloc[-1],

        # === Donchian / soportes diarios actuales ===
        "Sup20": pw["support_20"], "Res20": pw["resist_20"],
        "Sup60": pw["sup_60"], "Res60": pw["res_60"],

        # === Fase (ahora con metadatos) ===
        "PhaseW": pw["fase"], "Force": pw["fuerza_%"],
        "DiasFase": pw["dias_en_fase"],
        "PhasePrev": pw["fase_previa"],
        "PhaseChanged": pw["fase_cambio"],
        "U2_entry": pw["U2_entry"],

        # === Intradía (snapshot “del momento”) ===
        "TF": snap["intra_tf"],               # ej. '1h'
        "AsOf": snap["as_of"],                # timestamp de la última vela intradía
        "PriceNow": round(snap["price_now"], 2),
        "ClosePrev": round(snap["prev_close"], 2),
        "Δ-1d(now)": round(snap["delta_today"], 1),     # ahora vs cierre de ayer
        "Δ-open(now)": round(snap["delta_vs_open"], 1), # ahora vs apertura de hoy

        # === Momentum adicional ===
        "MACD_slope3": round(macd_slope, 6),  # pendiente suavizada (3)
        "MACD_slope3_norm": pw["macd_slope3_norm"],

        # === Indicadores adicionales ===
        "AboveMA50": AboveMA50,
        "AboveMA200": AboveMA200,
        "BreakRes20": BreakRes20,
        "BreakRes60": BreakRes60,
        "ReaceleraMACD": ReaceleraMACD,
        "DistMA50%": DistMA50_pct,

        })
        df_out[-1].update(fund)               # después de tu df_out.append({...})
                
        
    except Exception as e:
          print(f'{e} con {ticker}')
          continue
        

message_discord+="```"

df_out_df = pd.DataFrame(df_out)

res_col = 'Res60' if 'Res60' in df_out_df.columns else ('Resist.' if 'Resist.' in df_out_df.columns else None)
df_out_df['DistRes%'] = np.where(
    df_out_df['Close'] > 0,
    (df_out_df[res_col] - df_out_df['Close']) / df_out_df['Close'] * 100.0,
    np.nan
).round(2)

if 'Force' in df_out_df.columns:
    df_out_df = df_out_df.sort_values(['Force', 'DistRes%'], ascending=[False, True], ignore_index=True)
else:
    df_out_df = df_out_df.sort_values('DistRes%', ascending=True, ignore_index=True)

fmt: FormattersType = {
    # === TUS ORIGINALES ===
    'Ticker':   lambda x: f"{x:<12}",
    'Close':    lambda x: "" if x!=x else f"{x:>,.2f}",
    'Bias':     lambda x: f"{x:^3}",
    'Δ-1d':     lambda x: "" if x!=x else f"{x:>+.1f}%",
    'Δ-2d':     lambda x: "" if x!=x else f"{x:>+.1f}%",
    'Δ-5d':     lambda x: "" if x!=x else f"{x:>+.1f}%",
    'Δ-lf':     lambda x: "" if x!=x else f"{x:>+.1f}%",
    'Δ-pf':     lambda x: "" if x!=x else f"{x:>+.1f}%",
    'RSI':      lambda x: "" if x!=x else f"{x:>.0f}",
    'MACDh':    lambda x: "" if x!=x else f"{x:>+.1f}",
    'Support':  lambda x: "" if x!=x else f"{x:>,.2f}",
    'Resist.':  lambda x: "" if x!=x else f"{x:>,.2f}",
    'Sup60':    lambda x: "" if x!=x else f"{x:>,.2f}",
    'Res60':    lambda x: "" if x!=x else f"{x:>,.2f}",
    'PhaseW':   lambda x: f"{x:^6}",
    'Force':    lambda x: "" if x!=x else f"{x:>+.2f}",
    'DistRes%': lambda x: "" if x!=x else f"{x:>.1f}%",

    # === INTRADÍA / SNAPSHOT ===
    'PriceNow':     lambda x: "" if x!=x else f"{x:>,.2f}",
    'ClosePrev':    lambda x: "" if x!=x else f"{x:>,.2f}",
    'Δ-1d(now)':    lambda x: "" if x!=x else f"{x:>+.1f}%",
    'Δ-open(now)':  lambda x: "" if x!=x else f"{x:>+.1f}%",
    'TF':           lambda x: f"{x}",
    'AsOf':         lambda x: f"{x}",

    # === SOPORTES/RESISTENCIAS CORTO ===
    'Sup20':    lambda x: "" if x!=x else f"{x:>,.2f}",
    'Res20':    lambda x: "" if x!=x else f"{x:>,.2f}",

    # === META FASE ===
    'DiasFase':     lambda x: "" if x!=x else f"{int(x)}",
    'PhasePrev':    lambda x: f"{x:^6}",
    'PhaseChanged': lambda x: "✅" if bool(x) else "—",
    'U2_entry':     lambda x: "✅" if bool(x) else "—",

    # === MOMENTUM EXTRA ===
    'MACD_slope3':       lambda x: "" if x!=x else f"{x:>+.6f}",
    'MACD_slope3_norm':  lambda x: "" if x!=x else f"{x:>+.2f}",  # NO es %

    # === FLAGS TÉCNICOS ===
    'AboveMA50':     lambda x: "✅" if bool(x) else "—",
    'AboveMA200':    lambda x: "✅" if bool(x) else "—",
    'BreakRes20':    lambda x: "✅" if bool(x) else "—",
    'BreakRes60':    lambda x: "✅" if bool(x) else "—",
    'ReaceleraMACD': lambda x: "✅" if bool(x) else "—",
    'DistMA50%':     lambda x: "" if x!=x else f"{x:>+.2f}%",

    # === FUNDAMENTALES / PERFIL ===
    'Name':          lambda x: f"{str(x)[:40]}",
    'Asset':         lambda x: f"{x}",
    'Sector':        lambda x: f"{x}",
    'Industry':      lambda x: f"{x}",
    'MarketCap':     lambda x: "" if x!=x else f"{x:,.0f}",
    'TrailingPE':    lambda x: "" if x!=x else f"{x:,.2f}",
    'ForwardPE':     lambda x: "" if x!=x else f"{x:,.2f}",
    'PriceToBook':   lambda x: "" if x!=x else f"{x:,.2f}",
    'EVtoEBITDA':    lambda x: "" if x!=x else f"{x:,.2f}",
    'ProfitMargin%': lambda x: "" if x!=x else f"{x:,.2f}%",   # solo formato, sin re-escalar
    'ROE%':          lambda x: "" if x!=x else f"{x:,.2f}%",
    'FreeCashFlow':  lambda x: "" if x!=x else f"{x:,.0f}",
    'TotalDebt':     lambda x: "" if x!=x else f"{x:,.0f}",
    'DividendYield%':lambda x: "" if x!=x else f"{x:,.2f}%",
    'PayoutRatio%':  lambda x: "" if x!=x else f"{x:,.2f}%",
    'Beta':          lambda x: "" if x!=x else f"{x:,.2f}",
    'ExpenseRatio%': lambda x: "" if x!=x else f"{x:,.2f}%",
    'AUM':           lambda x: "" if x!=x else f"{x:,.0f}",
    'Category':      lambda x: f"{x}",
    'FundYield%':    lambda x: "" if x!=x else f"{x:,.2f}%",
    'Ret3Y%':        lambda x: "" if x!=x else f"{x:,.2f}%",
    'Ret5Y%':        lambda x: "" if x!=x else f"{x:,.2f}%",
    'recommendationKey':    lambda x: f"{x}",
    'averageAnalystRating': lambda x: f"{x}",
}

test.save_json_data(df_out_df, "u2_screener.json")
# HTML embebido (sin fetch externo) reutilizando la UI de u2_screener_FIJO.html
from pathlib import Path
import json

def build_embedded_html(json_path, template_path, out_path):
    data_text = Path(json_path).read_text(encoding="utf-8")
    html_text = Path(template_path).read_text(encoding="utf-8")
    inject = f'<script id="preloadedData" type="application/json">{data_text}</script>\n'
    if "</body>" in html_text:
        html_text = html_text.replace("</body>", inject + "</body>", 1)
    else:
        html_text = html_text + inject
    Path(out_path).write_text(html_text, encoding="utf-8")

build_embedded_html(
    json_path="u2_screener.json",
    template_path="u2_screener_FIJO.html",
    out_path="u2_screener_FIJO_embedded.html",
)

df_out_df = df_out_df.loc[df_out_df['PhaseW'].eq('U2')].reset_index(drop=True)

now=datetime.now()


print("```")
print(df_out_df.to_string(index=False, formatters=fmt, justify="left", max_colwidth=20))
print(f"end>{now.strftime("%Y-%m-%d %H:%M:%S")}")


# 1) Construir el texto formateado una sola vez
text = df_out_df.to_string(index=False, formatters=fmt, justify="left", max_colwidth=20)

# # 2) Partir por líneas y separar header + filas
# lines = text.splitlines()
# if not lines:
#     discord_blocks = []
# else:
#     header = lines[0]
#     rows = lines[1:]

#     MAX = 1950  # margen bajo el límite de 2000 de Discord
#     fence_open, fence_close = "```txt\n", "```"
#     # Base de cada bloque: fence + header + salto
#     base_len = len(fence_open) + len(header) + 1  # +1 por '\n'
#     end_len = len(fence_close)

#     blocks = []
#     current = fence_open + header + "\n"
#     current_len = base_len

#     for row in rows:
#         add = row + "\n"
#         # ¿cabe si añadimos esta línea + el cierre ``` ?
#         if current_len + len(add) + end_len > MAX:
#             # cerrar bloque actual
#             blocks.append(current + fence_close)
#             # iniciar uno nuevo con header
#             current = fence_open + header + "\n" + add
#             current_len = base_len + len(add)
#         else:
#             current += add
#             current_len += len(add)

#     # empujar el último bloque si tiene contenido
#     if current_len > base_len:
#         blocks.append(current + fence_close)

#     discord_blocks = blocks

# # 3) Enviar/imprimir los bloques (uno por mensaje en Discord)
# for b in discord_blocks:
#     notifier.send_discord(webhook_url=discord_webhook_url_reto,msg=b)  



