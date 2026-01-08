import numpy as np
import pandas as pd

def slope_norm(s: pd.Series, lookback: int=40) -> float:
    s = pd.to_numeric(s, errors='coerce').dropna().iloc[-lookback:]
    if len(s) < 5: return 0.0
    x = np.arange(len(s),dtype=float)
    y = s.to_numpy(dtype=float)
    b = float(np.polyfit(x, y, 1)[0])
    return float(b / (abs(s.mean()) + 1e-9))  # pendiente normalizada

def fase_weinstein(df: pd.DataFrame) -> dict:
    c  = pd.to_numeric(df['close'], errors='coerce')
    ma50  = c.rolling(50, min_periods=1).mean()
    ma200 = c.rolling(200, min_periods=1).mean()

    price = float(c.iloc[-1])
    m50   = float(ma50.iloc[-1])
    m200  = float(ma200.iloc[-1])
    macdh = float(pd.to_numeric(df['macd_hist'], errors='coerce').iloc[-1])

    s200  = slope_norm(ma200, 40)  # ~8 semanas en diario

    above50  = price > m50
    above200 = price > m200
    trend_up = (m50 > m200) and (s200 > 0)
    trend_dn = (m50 < m200) and (s200 < 0)

    # fuerza de tendencia (similar a “Muy Alcista 7.41%”)
    fuerza = (price / (m200 + 1e-9) - 1.0) * 100.0

    # Reglas compactas
    if above50 and above200 and trend_up and macdh > 0:
        fase = "U2"   # Etapa II (Markup)
    elif (not above50) and (not above200) and trend_dn and macdh < 0:
        fase = "D4"   # Etapa IV (Markdown)
    elif abs(s200) < 3e-4 and abs((price-m200)/(m200+1e-9)) < 0.05:
        fase = "U1/D3"  # Base/Distribución (zona plana)
    else:
        fase = "Mixta"

    # Soporte/Resistencia por swings sencillos (últ. 20 barras)
    win = c.tail(20)
    soporte = float(win.min())
    resistencia = float(win.max())

    # === NUEVO: metadatos consistentes con la fase actual ===
    # si ya existe columna histórica "fase", la copiamos;
    # si no existe, creamos una serie vacía del mismo tamaño.
    if "fase" in df.columns:
        serie_fase = df["fase"].astype(str).copy()
    else:
        serie_fase = pd.Series([""] * len(df), index=df.index, dtype="object")

    # Forzamos el último valor a la fase recién calculada (coherencia “hoy”)
    serie_fase.iloc[-1] = fase

    meta = _fase_meta_from_series(serie_fase)
    # ========================================================

    return {
        "fase": fase,
        "fuerza_%": round(fuerza, 2),
        "soporte": round(soporte, 4),
        "resistencia": round(resistencia, 4),
        "m50": round(m50, 4),
        "m200": round(m200, 4),
        "slope200": round(s200*100, 4),  # % normalizado

        "dias_en_fase": meta["dias_en_fase"],
        "fase_previa": meta["fase_previa"],
        "fase_cambio": meta["fase_cambio"],
        "U2_entry": meta["U2_entry"],
    }

def annotate_weinstein(df: pd.DataFrame) -> pd.DataFrame:
    """
    Anota el df diario con columnas vectorizadas del modelo Weinstein:
    - ma50, ma200, slope200
    - fase, fase_cambio, dias_en_fase, fase_previa, U2_entry
    - fuerza_%, soporte20, resistencia20
    - macd_slope3

    No sobrescribe tus columnas actuales.
    """
    out = df.copy()

    # --------- Bases ----------
    c = pd.to_numeric(out['close'], errors='coerce')

    # MAs para tendencia de fondo (SMA); no pisamos tus ema_short/ema_long
    out['ma50']  = c.rolling(50, min_periods=1).mean()
    out['ma200'] = c.rolling(200, min_periods=1).mean()

    # Pendiente normalizada de ma200 en ventana 40 (≈ 8 semanas)
    m2 = out['ma200']
    out['slope200'] = (m2 - m2.shift(40)) / ((m2.shift(40) + 1e-9) * 40)

    # MACD hist ya lo traes
    macdh = pd.to_numeric(out['macd_hist'], errors='coerce')

    # --------- Reglas de fase (vector) ----------
    above50  = c > out['ma50']
    above200 = c > out['ma200']
    trend_up = (out['ma50'] > out['ma200']) & (out['slope200'] > 0)
    trend_dn = (out['ma50'] < out['ma200']) & (out['slope200'] < 0)

    # zona plana: pendiente muy pequeña y precio cerca de ma200 (±5%)
    flat = (out['slope200'].abs() < 3e-4) & ((c - out['ma200']).abs() / (out['ma200'].abs() + 1e-9) < 0.05)

    out['fase'] = np.where(above50 & above200 & trend_up & (macdh > 0), 'U2',
                   np.where((~above50) & (~above200) & trend_dn & (macdh < 0), 'D4',
                   np.where(flat, 'U1/D3', 'Mixta')))

    # --------- Metadatos de fase (sin loops) ----------
    out['fase_cambio'] = out['fase'] != out['fase'].shift(1)
    run_id = out['fase_cambio'].cumsum()
    out['dias_en_fase'] = run_id.groupby(run_id).cumcount() + 1

    grp_first = out.groupby(run_id, sort=False)['fase'].first()
    prev_phase_map = grp_first.shift(1)                 # fase del bloque anterior
    out['fase_previa'] = run_id.map(prev_phase_map)     # mapea por bloque
    out['U2_entry'] = (out['fase'] == 'U2') & (out['dias_en_fase'] == 1)


    # --------- Fuerza y momentum ----------
    out['fuerza_%']   = (c / (out['ma200'] + 1e-9) - 1.0) * 100.0
    out['macd_slope3'] = macdh.diff().rolling(3, min_periods=1).mean()
    out['macd_slope3_norm'] = out['macd_slope3'] / (macdh.abs().rolling(5, min_periods=1).mean() + 1e-9)


    # Donchian corto / medio / “largo”
    out['donchian20_low']   = c.rolling(20,  min_periods=1).min()
    out['donchian20_high']  = c.rolling(20,  min_periods=1).max()
    out['donchian60_low']   = c.rolling(60,  min_periods=1).min()
    out['donchian60_high']  = c.rolling(60,  min_periods=1).max()
    out['donchian120_low']  = c.rolling(120, min_periods=1).min()
    out['donchian120_high'] = c.rolling(120, min_periods=1).max()

    return out


def _conteo_consecutivo_final(serie):
    """Cuenta cuántos valores finales consecutivos son iguales al último valor."""
    if len(serie) == 0:
        return 0, None
    last = serie.iloc[-1]
    n = 1
    prev = None
    # Recorremos hacia atrás (excluyendo el último)
    for v in serie.iloc[-2::-1]:
        if v == last:
            n += 1
        else:
            prev = v
            break
    return n, prev

def _fase_meta_from_series(serie_fase):
    dias, fase_previa = _conteo_consecutivo_final(serie_fase)
    fase_actual = serie_fase.iloc[-1] if len(serie_fase) else None
    return {
        "dias_en_fase": dias,
        "fase_previa": fase_previa,
        "fase_cambio": dias == 1,
        "U2_entry": (fase_actual == "U2" and fase_previa not in (None, "U2")),
    }


def get_intraday_snapshot(reto_db, ticker, intra_tf="1h"):
    df_d = reto_db[ticker]["1d"]
    df_i = reto_db[ticker][intra_tf]

    if len(df_d) < 3 or len(df_i) < 1:
        # valores seguros si no hay datos suficientes
        return {
            "price_now": float("nan"),
            "as_of": "",
            "intra_tf": intra_tf,
            "delta_today": float("nan"),
            "delta_vs_open": float("nan"),
            "delta_2d": float("nan"),
            "delta_5d": float("nan"),
            "prev_close": float("nan"),
            "open_today": float("nan"),
        }

    price_now = float(df_i["close"].iloc[-1])
    ts = df_i.index[-1]
    as_of_str = ts.strftime("%Y-%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)

    prev_close = float(df_d["close"].iloc[-2])
    open_today = float(df_d["open"].iloc[-1])

    delta_today   = (price_now/prev_close - 1.0) * 100.0
    delta_vs_open = (price_now/open_today - 1.0) * 100.0
    # ojo con ventanas cortas:
    delta_2d = (df_d["close"].iloc[-2]/df_d["close"].iloc[-3] - 1.0) * 100.0 if len(df_d) >= 3 else float("nan")
    delta_5d = (df_d["close"].iloc[-2]/df_d["close"].iloc[-6] - 1.0) * 100.0 if len(df_d) >= 6 else float("nan")

    return {
        "price_now": price_now,
        "as_of": as_of_str,
        "intra_tf": intra_tf,
        "delta_today": delta_today,
        "delta_vs_open": delta_vs_open,
        "delta_2d": delta_2d,
        "delta_5d": delta_5d,
        "prev_close": prev_close,
        "open_today": open_today,
    }


def macd_slope_icon(macd_hist_series, smooth=3):
    """
    Pendiente del histograma MACD (dif. primera) con suavizado simple.
    Retorna (icono, slope), donde icono ∈ {'🟢↗','🔴↘','⚪→'}.
    """
    # diff() ≈ derivada discreta; rolling(mean) para quitar ruido
    slope = macd_hist_series.diff().rolling(smooth, min_periods=1).mean().iloc[-1]
    if slope > 0:
        return "🟢↗", float(slope)
    elif slope < 0:
        return "🔴↘", float(slope)
    else:
        return "⚪→", 0.0


import json

def save_json_data(df : pd.DataFrame, path="u2_screener.json"):
    df = df.replace([np.nan, np.inf, -np.inf], None)
    records = df.to_dict(orient="records")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)




# parea display


import pandas as pd
from pathlib import Path

def save_filterable_table(df, path="u2_screener.html"):
    """
    Guarda un HTML con DataTables 2 totalmente interactivo a partir de un DataFrame.
    - Sort múltiple, búsqueda global y por columna, colReorder, fixedHeader, export (Copy/CSV/Excel),
      stateSave, filtros rápidos de Bias y PhaseW, formatos bonitos.
    """
    import json
    import pandas as pd

    # Opcional: si no existe DistRes%, lo calculamos (usa 'Resist.' y 'Close')
    if 'DistRes%' not in df.columns and {'Resist.', 'Close'}.issubset(df.columns):
        with pd.option_context('mode.use_inf_as_na', True):
            df = df.copy()
            df['DistRes%'] = (df['Resist.'] - df['Close']) / df['Close'] * 100

    # Convertimos a JSON embebible
    data_json = json.dumps(df.to_dict(orient="records"), ensure_ascii=False)

    # --- Banner: TF y timestamp global (si existen en df) ---
    tf_used = ""
    as_of_global = ""
    if "TF" in df.columns:
        # el TF más común (si hay mezcla); si no, toma el primero disponible
        tf_mode = df["TF"].mode(dropna=True)
        tf_used = (tf_mode.iloc[0] if len(tf_mode) else df["TF"].dropna().iloc[0]) if df["TF"].notna().any() else ""
    if "AsOf" in df.columns and df["AsOf"].notna().any():
        try:
            # si ya es datetime, max() funciona; si es string, igual
            as_of_global = str(df["AsOf"].max())
        except Exception:
            as_of_global = str(df["AsOf"].astype(str).max())

    banner_html = ""
    if tf_used or as_of_global:
        banner_html = (
            f"📌 Close intradía <strong>{tf_used or '—'}</strong> actualizado a "
            f"<strong>{as_of_global or '—'}</strong>. "
            f"<code>Δ-1d(now)</code> = precio actual vs cierre de ayer; "
            f"<code>Δ-open(now)</code> = precio actual vs apertura de hoy; "
            f"<code>Δ-5d</code> = cierre de ayer vs hace 5 sesiones."
        )

    titles=list(df.columns)
    thead_html = "<thead><tr class='headers'>" + ''.join(f"<th>{t}</th>" for t in titles) + "</tr><tr class='filters' style='display:none;'></tr></thead>"

    # HTML + JS (plantilla compacta)
    html = f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8"><title>U2 Screener</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://cdn.datatables.net/2.1.8/css/dataTables.dataTables.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/3.1.2/css/buttons.dataTables.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/fixedheader/4.0.1/css/fixedHeader.dataTables.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/colreorder/2.0.3/css/colReorder.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/2.1.8/js/dataTables.min.js"></script>
<script src="https://cdn.datatables.net/buttons/3.1.2/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/3.1.2/js/buttons.html5.min.js"></script>
<script src="https://cdn.datatables.net/fixedheader/4.0.1/js/dataTables.fixedHeader.min.js"></script>
<script src="https://cdn.datatables.net/colreorder/2.0.3/js/colReorder.min.js"></script>
<style>
  :root{{ --tbl-font: 14px }}
  body{{ font-size: var(--tbl-font) }}
  body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:16px}}
  h1{{font-size:1.15rem;margin:0 0 10px}}
  .controls{{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:10px 0 12px}}
  .controls label{{display:flex;gap:6px;align-items:center}}
  .controls input,.controls select{{padding:6px 8px}}
  .controls button{{padding:6px 10px;cursor:pointer}}
  table.dataTable thead th{{ white-space:normal; line-height:1.2; }}
  .dt-center{{text-align:center}}
  .badge{{padding:2px 6px;border-radius:6px;font-weight:600;font-size:.95em}}
  .badge.green{{background:#e6f4ea;color:#0b6b2b}}
  .badge.red{{background:#fde8e8;color:#a1121c}}
  .badge.gray{{background:#eef2f7;color:#39465a}}
  .banner{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:8px 0 12px;padding:10px 12px;
          background:#101318;border:1px solid #2a2f3a;border-radius:10px;color:#e6e6e6}}
  .banner code{{background:#0f172a;padding:2px 6px;border-radius:6px}}
  .legend{{margin:6px 0 12px;color:#cbd5e1;font-size:.92em;line-height:1.35}}
  .legend ul{{margin:6px 0 0 18px;padding:0}}
  .legend li code{{background:#0f172a;padding:1px 5px;border-radius:6px}}
  #tbl thead tr.filters th{{padding:4px 8px}}
  #tbl{{table-layout: fixed;}}
  #tbl thead tr.filters input,#tbl thead tr.filters select{{width:100%;box-sizing:border-box;padding:4px 6px}}
  #tbl{{ font-size: inherit }}
  .pos{{ color:#15803d; font-weight:600 }}   /* verde */
  .neg{{ color:#b91c1c; font-weight:600 }}   /* rojo  */
  .neu{{ color:#64748b }}                    /* gris  */
  .chip{{ padding:2px 6px; border-radius:6px; background:#0f172a; display:inline-block }}
  #toggleFundamentals{{
    padding:6px 10px; border:1px solid #2a2f3a; border-radius:8px;
    background:#0f172a; color:#e5e7eb; cursor:pointer;

  .tt{{ position:relative; cursor:help }}
  .tt:hover .tt-card{{ opacity:1; transform:translateY(0); pointer-events:auto }}
  .tt-card{{
    position:absolute; z-index:50; top:110%; left:0; min-width:280px;
    background:#0b1220; color:#e5e7eb; border:1px solid #1f2937; border-radius:10px;
    box-shadow:0 12px 30px rgba(0,0,0,.35); padding:10px 12px; font-size:.9em;
    opacity:0; transform:translateY(4px); transition:.16s ease; pointer-events:none;
  }}
  .tt-card .k{{color:#93c5fd}} .tt-card .v{{color:#f3f4f6}}

  }}
  #toggleFundamentals:hover{{ background:#111827 }}

</style></head>
<body>
<h1>U2 Screener</h1>
<div class="banner">{banner_html}</div>
<div class="legend">
  <strong>Definiciones rápidas:</strong>
  <ul>
    <li><code>PriceNow</code>: último cierre de la vela intradía (p. ej., 1h).</li>
    <li><code>ClosePrev</code>: cierre oficial de <em>ayer</em> (diario).</li>
    <li><code>Close</code>: última vela diaria (si el día no ha cerrado, puede ser parcial).</li>
    <li><code>Δ-1d(now)</code>: <code>PriceNow / ClosePrev - 1</code> &rarr; variación “en momento” vs cierre de ayer.</li>
    <li><code>Δ-open(now)</code>: <code>PriceNow / OpenToday - 1</code> &rarr; variación desde la apertura de hoy.</li>
    <li><code>Δ-1d</code>, <code>Δ-2d</code>, <code>Δ-5d</code>: variaciones entre <strong>cierres</strong> (ayer vs hace N sesiones).</li>
    <li><code>Δ-lf</code>, <code>Δ-pf</code>: variaciones <strong>semanales</strong> (viernes vs viernes) de tu función.</li>
  </ul>
</div>
<div class="controls">
  <label>Buscar: <input type="search" id="globalSearch" placeholder="Buscar..."></label>
  <label>Bias:
    <select id="biasFilter"><option value="">(Todos)</option><option value="🟢">🟢</option><option value="🔴">🔴</option><option value="⚪">⚪</option></select>
  </label>
  <label>PhaseW:
    <select id="phaseFilter"><option value="">(Todos)</option><option>U2</option><option>U1/D3</option><option>D4</option><option>Mixta</option></select>
  </label>
  <button id="toggleColFilters">Mostrar/Ocultar filtros por columna</button>
  <button id="resetAll">Reset filtros</button>
  <button id="clearState">Borrar estado guardado</button>
  <button id="toggleFundamentals" title="Mostrar/ocultar columnas fundamentales largas"> Ocultar fund. </button>
  <label>Tamaño:<input id="fontSlider" type="range" min="08" max="18" value="14" step="1" style="width:120px"></label>
</div>

<table id="tbl" class="display compact stripe" style="width:100%">

{thead_html}
  <tbody></tbody>
</table>

<script>
const DATA = {data_json};

const COLMAP = [
  'Ticker',
  'PriceNow','ClosePrev','Close','Bias',
  'Δ-1d(now)','Δ-open(now)',
  'Δ-1d','Δ-2d','Δ-5d','Δ-lf','Δ-pf',
  'RSI','MACDh','MACD_slope3','MACD_slope3_norm',
  'Sup20','Res20','Sup60','Res60',
  'PhaseW','DiasFase','PhasePrev','PhaseChanged','U2_entry',
  'Force','DistRes%','TF','AsOf',
  // Flags de MAs / resistencias
  'AboveMA50','AboveMA200','BreakRes20','BreakRes60',
  // OJO: esta key existe en tus datos y faltaba en headers
  'ReaceleraMACD',
  'DistMA50%',
  // Fundamentales
  'Name','Asset','Sector','Industry',
  'MarketCap','TrailingPE','ForwardPE','PriceToBook','EVtoEBITDA',
  'ProfitMargin%','ROE%','FreeCashFlow','TotalDebt',
  'DividendYield%','PayoutRatio%','Beta',
  'ExpenseRatio%','AUM','Category',
  'FundYield%','Ret3Y%','Ret5Y%',
  'recommendationKey','averageAnalystRating'
];

// what the user sees in the header (can include <br>)
const TITLES = [
  'Ticker',
  'PriceNow','Close (ayer)','Close (hoy)','Bias',
  'Δ-1d (now)','Δ-open (now)',
  'Δ-1d','Δ-2d','Δ-5d','Δ-lf','Δ-pf',
  'RSI','MACDh','MACD<br>slope3','MACD<br>slope3_norm',
  'Sup20','Res20','Sup60','Res60',
  'PhaseW','Días Fase','PhasePrev','PhaseChanged','U2 entry',
  'Force','DistRes%','TF','AsOf',
  'MA50+','MA200+','Res20+','Res60+',
  'Reacelera MACD',         // << añadido
  'DistMA50%',
  'Name','Asset','Sector','Industry',
  'MarketCap','TrailingPE','ForwardPE','PriceToBook','EV/EBITDA',
  'Profit Margin %','ROE %','Free Cash Flow','Total Debt',
  'Dividend Yield %','Payout Ratio %','Beta',
  'Expense Ratio %','AUM','Category',
  'Fund Yield %','Ret 3Y %','Ret 5Y %',
  'Reco Key','Avg Analyst Rating'
];

function pct(v){{ if(v==null||v==="")return ""; const n=Number(v); if(!isFinite(n))return ""; const s=n>0?"+":(n<0?"":""); return s+n.toFixed(1)+"%"; }}
function smart(v){{ if(v==null||v==="")return ""; const n=Number(v); if(!isFinite(n))return ""; const prec=Math.abs(n)>=1000?0:2; return n.toLocaleString(undefined,{{minimumFractionDigits:prec,maximumFractionDigits:prec}}); }}
function pint(v){{ if(v==null||v==="")return ""; const n=Number(v); if(!isFinite(n))return ""; return String(Math.round(n)); }}
function sgn(v,d=2){{ if(v==null||v==="")return ""; const n=Number(v); if(!isFinite(n))return ""; const s=n>0?"+":(n<0?"":""); return s+n.toFixed(d); }}
function biasBadge(b){{ if(b==="🟢")return '<span class="badge green">🟢</span>'; if(b==="🔴")return '<span class="badge red">🔴</span>'; return '<span class="badge gray">'+(b||"–")+'</span>'; }}
function pctSpan(v){{
  if(v==null||v==="") return "";
  const n = Number(v); if(!isFinite(n)) return "";
  const cls = n>0 ? "pos" : n<0 ? "neg" : "neu";
  const sgn = n>0?"+":(n<0?"":"");
  return `<span class="${{cls}}">${{sgn}}${{n.toFixed(1)}}%</span>`;
}}
function sgnSpan(v, d=2){{
  if(v==null||v==="") return "";
  const n = Number(v); if(!isFinite(n)) return "";
  const cls = n>0 ? "pos" : n<0 ? "neg" : "neu";
  const sgn = n>0?"+":(n<0?"":"");
  return `<span class="${{cls}}">${{sgn}}${{n.toFixed(d)}}</span>`;
}}
function slopeNormChip(v){{
  if(v==null||v==="") return "";
  const n = Number(v); if(!isFinite(n)) return "";
  const up = n>0, flat = n===0;
  const arrow = flat ? "→" : (up ? "↗" : "↘");
  const cls = up ? "pos" : (!up && !flat ? "neg" : "neu");
  // dos decimales; NO es %
  return `<span class="${{cls}}">${{arrow}} ${{n.toFixed(2)}}</span>`;
}}
// % SOLO FORMATO (no cambia el valor)
function pctPlainSpan(v, d=2){{
  if(v==null || v==="") return "";
  const n = Number(v); if(!isFinite(n)) return "";
  const cls = n>0 ? "pos" : n<0 ? "neg" : "neu";
  const sgn = n>0 ? "+" : n<0 ? "" : "";
  return `<span class="${{cls}}">${{sgn}}${{n.toFixed(d)}}%</span>`;
}}

// formatea a Billions/Trillions (o unidades si es chico)
function humanMoney(x){{
  const n = Number(x); if(!isFinite(n)) return '';
  const ab = Math.abs(n);
  if (ab >= 1e12) return (n/1e12).toFixed(2)+'T';
  if (ab >= 1e9)  return (n/1e9 ).toFixed(2)+'B';
  if (ab >= 1e6)  return (n/1e6 ).toFixed(2)+'M';
  return n.toLocaleString();
}}

const rows = Array.isArray(DATA) ? DATA.map(r => COLMAP.map(k => (r[k] ?? ""))) : [];

const table = new DataTable('#tbl', {{
  data: rows,
  columns: COLMAP.map((k,i)=>({{ title: (TITLES[i] && TITLES[i].trim()) ? TITLES[i] : k }})), // <= titles shown
  pageLength: 50,
  order: [],
  stateSave: true,
  fixedHeader: true,
  colReorder: true,
  dom: 'QBfrtip',
  buttons: ['copy','csv','excel'],
  
  language: {{ url: 'https://cdn.datatables.net/plug-ins/2.1.8/i18n/es-ES.json' }},
    columnDefs: [
      // centrados: Bias, PhaseW, PhasePrev, PhaseChanged, U2_entry, TF, AsOf
      {{targets:[4,20,22,23,24,27,28], className:'dt-center'}},

      // columnas numéricas para orden correcto
      {{targets:[1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,25,26], type:'num'}},

      // formateos: precios/soportes → 2 decimales bonitos
      {{targets:[1,2,3,16,17,18,19], render:(d,t)=> t==='display'? smart(d):d}},

      // porcentajes (deltas, Force, DistRes%)
      {{targets:[5,6,7,8,9,10,11,25,26], render:(d,t)=> t==='display'? pct(d):d}},

      // enteros: RSI y DiasFase
      {{targets:[12,21], render:(d,t)=> t==='display'? pint(d):d}},

      // MACDh con signo y 1 decimal
      {{targets:13, render:(d,t)=> t==='display'? sgn(d,1):d}},

      // pendiente MACD (absoluta) con más precisión
      {{targets:14, render:(d,t)=> t==='display'? sgn(d,6):d}},

      // pendiente MACD normalizada (comparables entre tickers) — no es %
      {{targets:15, render:(d,t)=> t==='display'? sgn(d,2):d}},

      // Bias con badge
      {{targets:4,  render:(d,t)=> t==='display'? biasBadge(d):d}},

      // --- extras para las 6 columnas nuevas (al final) ---
      {{targets:[29,30,31,32,33], className:'dt-center'}},         // centrar booleanos
      {{targets:[29,30,31,32,33], render:(d,t)=> t==='display'? (d ? '✅' : '—') : d}},  // booleans
      {{targets:[34], type:'num'}},                                 // DistMA50% como num
      {{targets:[34], render:(d,t)=> t==='display'? pct(d):d}},     // formatear como %

      {{targets:[23,24], className:'dt-center'}},
      {{targets:[23,24], render:(d,t)=> t==='display'? (d ? '✅' : '—') : d}},

      // colorear %: Δ-1d(now) Δ-open(now) Δ-1d Δ-2d Δ-5d Δ-lf Δ-pf  Force DistRes%  y DistMA50% (col 34)
    {{targets:[5,6,7,8,9,10,11,25,26,34], render:(d,t)=> t==='display'? pctSpan(d):d}},

    // colorear MACDh y MACD_slope3 (absoluto)
    {{targets:13, render:(d,t)=> t==='display'? sgnSpan(d,1):d}},
    {{targets:14, render:(d,t)=> t==='display'? sgnSpan(d,6):d}},

    // chip para MACD_slope3_norm (no %)
    {{targets:15, render:(d,t)=> t==='display'? slopeNormChip(d):d}},

    // Números grandes sin % (MarketCap, FCF, Deuda, AUM)
    {{targets:[39,46,47,52], render:(d,t)=> t==='display'? smart(d):d}},

    // Multiplicadores simples (PE, PB, EV/EBITDA, Beta)
    {{targets:[40,41,42,43,50], render:(d,t)=> t==='display'? sgn(d,2):d}},

    // % SOLO FORMATO (no reescala)
    {{targets:[44,45,48,49,51,54,55,56], render:(d,t)=> t==='display'? pctPlainSpan(d):d}},

    // Centrados para strings cortos
    {{targets:[36,57], className:'dt-center'}},  // Asset, reco key

    // averageAnalystRating: mostrar "2.1 - Buy" tal cual si viene string; si num, 1 decimal
    {{targets:[58], render:(d,t)=> {{
      if(t!=='display') return d;
      if(d==null) return '';
      const s = String(d), n = Number(s);
      return isFinite(n) ? n.toFixed(1) : s;
    }}}}, 
  ],

 rowCallback: function(row, data){{
    // arma el texto (líneas separadas con \\n)
    const tip = [
      `Name: ${{data[iName]  || ''}}`,
      `Industry: ${{data[iInd] || ''}}`,
      `Market Cap: ${{humanMoney(data[iMC])}}`,
      `Free Cash Flow: ${{humanMoney(data[iFCF])}}`,
      `Total Debt: ${{humanMoney(data[iDebt])}}`
    ].join('\\n');

    // setea el atributo title de la celda del Ticker
    if (iTicker >= 0) {{
      $('td', row).eq(iTicker).attr('title', tip);
    }}
  }}

}});

// Índices actuales (para referencia rápida)
/*
0 Ticker
1 PriceNow  2 Close(ayer)  3 Close(hoy)  4 Bias
5 Δ-1d(now) 6 Δ-open(now)
7 Δ-1d 8 Δ-2d 9 Δ-5d 10 Δ-lf 11 Δ-pf
12 RSI 13 MACDh 14 MACD_slope3 15 MACD_slope3_norm
16 Sup20 17 Res20 18 Sup60 19 Res60
20 PhaseW 21 DiasFase 22 PhasePrev 23 PhaseChanged 24 U2_entry
25 Force 26 DistRes% 27 TF 28 AsOf
29 AboveMA50 30 AboveMA200 31 BreakRes20 32 BreakRes60 33 ReaceleraMACD 34 DistMA50%
35 Name, 36 Asset, 37 Sector, 38 Industry,
39 MarketCap, 40 TrailingPE, 41 ForwardPE, 42 PriceToBook, 43 EVtoEBITDA,
44 ProfitMargin%, 45 ROE%, 46 FreeCashFlow, 47 TotalDebt,
48 DividendYield%, 49 PayoutRatio%, 50 Beta,
51 ExpenseRatio%, 52 AUM, 53 Category,
54 FundYield%, 55 Ret3Y%, 56 Ret5Y%,
57 recommendationKey, 58 averageAnalystRating
*/

const H = $('#tbl thead th');
H.eq(1).attr('title','Último close de la vela intradía (p.ej. 1h)');
H.eq(2).attr('title','Cierre oficial de AYER (diario)');
H.eq(3).attr('title','Última vela DIARIA (puede ser parcial si aún no cierra)');

H.eq(5).attr('title','PriceNow vs Close(ayer)');
H.eq(6).attr('title','PriceNow vs Open(hoy)');
H.eq(7).attr('title','Cierre de ayer vs anteayer');
H.eq(8).attr('title','Ayer vs hace 2 sesiones');
H.eq(9).attr('title','Ayer vs hace 5 sesiones');
H.eq(10).attr('title','Último viernes vs viernes previo');
H.eq(11).attr('title','Idem semanal (tu función)');

H.eq(13).attr('title','Histograma MACD (impulso), con signo');
H.eq(14).attr('title','Pendiente del MACDh (absoluta)');
H.eq(15).attr('title','Pendiente normalizada: comparables entre tickers (no %)');

H.eq(16).attr('title','Soporte Donchian 20 (corto plazo)');
H.eq(17).attr('title','Resistencia Donchian 20 (corto plazo)');
H.eq(18).attr('title','Soporte Donchian 60 (medio plazo)');
H.eq(19).attr('title','Resistencia Donchian 60 (medio plazo)');

H.eq(29).attr('title','Close(hoy) ≥ MA50 (diario)');
H.eq(30).attr('title','Close(hoy) ≥ MA200 (diario)');
H.eq(31).attr('title','Close(hoy) ≥ Res20');
H.eq(32).attr('title','Close(hoy) ≥ Res60');
H.eq(33).attr('title','MACDh>0 y slope_norm>0');
H.eq(34).attr('title','Distancia a MA50 diaria en %');

H.eq(39).attr('title','Capitalización de mercado');
H.eq(40).attr('title','P/E trailing (12m)');
H.eq(41).attr('title','P/E forward (estimado)');
H.eq(42).attr('title','Precio / Valor en libros');
H.eq(43).attr('title','Enterprise Value / EBITDA');
H.eq(44).attr('title','Margen neto %');
H.eq(45).attr('title','Return on Equity %');
H.eq(48).attr('title','Rendimiento de dividendo %');
H.eq(49).attr('title','Payout de utilidades %');
H.eq(51).attr('title','Gasto anual del fondo/ETF %');
H.eq(57).attr('title','Clave de recomendación (buy/hold/sell)');
H.eq(58).attr('title','Promedio rating analistas (menor=mejor)');

//Tooltips
const iTicker = COLMAP.indexOf('Ticker');
const iName   = COLMAP.indexOf('Name');
const iInd    = COLMAP.indexOf('Industry');
const iMC     = COLMAP.indexOf('MarketCap');
const iFCF    = COLMAP.indexOf('FreeCashFlow');
const iDebt   = COLMAP.indexOf('TotalDebt');





// Filtros rápidos
$('#globalSearch').on('input', function(){{ table.search(this.value).draw(); }});
$('#biasFilter').on('change', function(){{ const v=this.value; table.column(4).search(v?`^${{v}}$`:"",true,false).draw(); }});
$('#phaseFilter').on('change', function(){{ const v=this.value; table.column(20).search(v?`^${{v}}$`:"",true,false).draw(); }});

// Filtros por columna
function buildColumnFilters(){{
  const $row = $('#tbl thead tr.filters').empty();
  $('#tbl thead tr.headers th').each(function(i){{
    if(i===4){{ // Bias
      $row.append(`<th><select data-col="${{i}}">
        <option value="">(Todos)</option><option>🟢</option><option>🔴</option><option>⚪</option>
      </select></th>`);
    }} else if(i===20){{ // PhaseW
      $row.append(`<th><select data-col="${{i}}">
        <option value="">(Todos)</option><option>U2</option><option>U1/D3</option><option>D4</option><option>Mixta</option>
      </select></th>`);
    }} else {{
      $row.append(`<th><input type="text" placeholder="Filtrar..." data-col="${{i}}"></th>`);
    }}
  }});
  $('#tbl thead tr.filters input').on('keyup change', function(){{ 
    table.column(parseInt(this.dataset.col,10)).search(this.value).draw(); 
  }});
  $('#tbl thead tr.filters select').on('change', function(){{ 
    const v=this.value, c=parseInt(this.dataset.col,10); 
    table.column(c).search(v?`^${{v}}$`:"", true, false).draw(); 
  }});
}}
buildColumnFilters();

let filtersVisible=false;
$('#toggleColFilters').on('click', function(){{ filtersVisible=!filtersVisible; $('#tbl thead tr.filters').css('display', filtersVisible?'':'none'); }});
$('#resetAll').on('click', function(){{
  $('#globalSearch').val(''); $('#biasFilter').val(''); $('#phaseFilter').val('');
  table.search(''); table.columns().search(''); table.order([]).draw();
  $('#tbl thead tr.filters input').val(''); $('#tbl thead tr.filters select').val('');
}});
$('#clearState').on('click', function(){{ table.state.clear(); location.reload(); }});

// Columnas a ocultar/mostrar con el botón
const FUND_KEYS = [
  'Name','Sector','Industry','MarketCap',
  'FreeCashFlow','TotalDebt','AUM','Category'
];

// Mapa nombre->índice según COLMAP
const IDX = Object.fromEntries(COLMAP.map((k,i)=>[k,i]));
const FUND_IDX = FUND_KEYS.map(k => IDX[k]).filter(i => i >= 0);

// Función para aplicar visibilidad y actualizar botón + persistencia
function setFundamentalsVisible(visible){{
  FUND_IDX.forEach(i => table.column(i).visible(visible, false));
  table.columns.adjust().draw(false);
  localStorage.setItem('fundColsVisible', visible ? '1' : '0');
  const btn = document.getElementById('toggleFundamentals');
  if(btn) btn.textContent = visible ? 'Ocultar fund.' : 'Mostrar fund.';
}}

// Estado inicial (por defecto: ocultas)
const savedfund = localStorage.getItem('fundColsVisible');
const startVisible = savedfund === '1';  // si nunca se guardó, será false (ocultas)
setFundamentalsVisible(startVisible);

// Click para alternar
document.getElementById('toggleFundamentals').addEventListener('click', () => {{
  const nowVisible = table.column(FUND_IDX[0]).visible(); // mira el estado de la primera
  setFundamentalsVisible(!nowVisible);
}});
const slider = document.getElementById('fontSlider');
const saved2  = localStorage.getItem('tblFontPx');
if(saved2){{ document.documentElement.style.setProperty('--tbl-font', saved2+'px'); slider.value = saved2; }}
slider.addEventListener('input', e=>{{
  const v = e.target.value;
  document.documentElement.style.setProperty('--tbl-font', v+'px');
  localStorage.setItem('tblFontPx', v);
}});
</script>
</body></html>"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path
