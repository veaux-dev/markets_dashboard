import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from svc import analyzer, collector, notifier, test


DEFAULT_CONFIG_PATH = "config/u2_screener_config.json"


def load_config(path: str) -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    return json.loads(cfg_path.read_text(encoding="utf-8"))


def build_embedded_html(json_path: str, template_path: str, out_path: str) -> None:
    data_text = Path(json_path).read_text(encoding="utf-8")
    html_text = Path(template_path).read_text(encoding="utf-8")
    inject = f'<script id="preloadedData" type="application/json">{data_text}</script>\n'
    if "</body>" in html_text:
        html_text = html_text.replace("</body>", inject + "</body>", 1)
    else:
        html_text = html_text + inject
    Path(out_path).write_text(html_text, encoding="utf-8")


def _safe_float(v):
    if v is None:
        return None
    try:
        fv = float(v)
    except Exception:
        return None
    return None if pd.isna(fv) else fv


def _clean_list(values):
    return [_safe_float(v) for v in values]


def _series_line(times, series):
    out = []
    for t, v in zip(times, series):
        fv = _safe_float(v)
        if t is None or fv is None:
            continue
        out.append({"time": t, "value": fv})
    return out


def build_details(working_db, tickers, details_dir: Path, points_by_tf: dict) -> None:
    details_dir.mkdir(parents=True, exist_ok=True)
    for ticker in tickers:
        tfs = working_db.get(ticker, {})
        if not tfs:
            continue

        payload = {
            "ticker": ticker,
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "timeframes": {},
        }

        for tf, df in tfs.items():
            if tf not in points_by_tf or df is None or df.empty:
                continue
            n = int(points_by_tf.get(tf, 0) or 0)
            if n <= 0:
                continue

            tail = df.tail(n)
            ts = pd.to_datetime(tail.index, errors="coerce", utc=True)
            ts_str = [t.isoformat() if pd.notna(t) else None for t in ts]
            ts_s = [int(t.timestamp()) if pd.notna(t) else None for t in ts]

            closes = _clean_list(tail.get("close", pd.Series(dtype=float)).tolist())
            rsi = tail.get("rsi", pd.Series(dtype=float)).iloc[-1] if "rsi" in tail else None
            macdh = tail.get("macd_hist", pd.Series(dtype=float)).iloc[-1] if "macd_hist" in tail else None
            bias = tail.get("bias", pd.Series(dtype=object)).iloc[-1] if "bias" in tail else None

            candles = []
            if all(c in tail.columns for c in ("open", "high", "low", "close")):
                opens = tail["open"].tolist()
                highs = tail["high"].tolist()
                lows = tail["low"].tolist()
                closes_c = tail["close"].tolist()
                for t, o, h, l, c in zip(ts_s, opens, highs, lows, closes_c):
                    if t is None:
                        continue
                    fo = _safe_float(o)
                    fh = _safe_float(h)
                    fl = _safe_float(l)
                    fc = _safe_float(c)
                    if None in (fo, fh, fl, fc):
                        continue
                    candles.append({"time": t, "open": fo, "high": fh, "low": fl, "close": fc})

            ema_short = _series_line(ts_s, tail.get("ema_short", pd.Series(dtype=float)).tolist())
            ema_long = _series_line(ts_s, tail.get("ema_long", pd.Series(dtype=float)).tolist())
            sup = _series_line(ts_s, tail.get("donchian_low", pd.Series(dtype=float)).tolist())
            res = _series_line(ts_s, tail.get("donchian_high", pd.Series(dtype=float)).tolist())
            rsi_series = _series_line(ts_s, tail.get("rsi", pd.Series(dtype=float)).tolist())
            macd_hist = _series_line(ts_s, tail.get("macd_hist", pd.Series(dtype=float)).tolist())
            macd_signal = _series_line(ts_s, tail.get("macd_signal", pd.Series(dtype=float)).tolist())
            volume_series = _series_line(ts_s, tail.get("volume", pd.Series(dtype=float)).tolist())

            payload["timeframes"][tf] = {
                "as_of": ts_str[-1] if ts_str else None,
                "close": closes[-1] if closes else None,
                "rsi": _safe_float(rsi),
                "macd_hist": _safe_float(macdh),
                "bias": bias,
                "series": {
                    "ts": ts_str,
                    "close": closes,
                    "candles": candles,
                    "ema_short": ema_short,
                    "ema_long": ema_long,
                    "support": sup,
                    "resistance": res,
                    "volume": volume_series,
                    "rsi": rsi_series,
                    "macd_hist": macd_hist,
                    "macd_signal": macd_signal,
                },
            }

        if not payload["timeframes"]:
            continue

        out_path = details_dir / f"{ticker}.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state(state_path: str) -> dict:
    state_file = Path(state_path)
    if state_file.exists():
        return json.loads(state_file.read_text(encoding="utf-8"))
    return {"notified": {}}


def save_state(state_path: str, state: dict) -> None:
    state_file = Path(state_path)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")


def notify_u2_entries(df: pd.DataFrame, cfg: dict, state_path: str) -> None:
    if df.empty or not cfg.get("alerts", {}).get("notify_on_u2_entry", True):
        return

    alerts_cfg = cfg.get("alerts", {})
    discord_webhook = os.getenv("U2_DISCORD_WEBHOOK_URL", alerts_cfg.get("discord_webhook"))
    telegram_token = os.getenv("U2_TELEGRAM_TOKEN", alerts_cfg.get("telegram_token"))
    telegram_chat_id = os.getenv("U2_TELEGRAM_CHAT_ID", alerts_cfg.get("telegram_chat_id"))

    state = load_state(state_path)
    notified = state.get("notified", {})

    rows = df[df.get("U2_entry", False) == True]
    if rows.empty:
        return

    for _, row in rows.iterrows():
        ticker = str(row.get("Ticker", ""))
        as_of = str(row.get("AsOf", ""))
        key = f"{ticker}|{as_of}"
        if notified.get(ticker) == key:
            continue

        msg = f"🚨 U2 entry: {ticker} (AsOf {as_of})"
        if discord_webhook:
            notifier.send_discord(discord_webhook, msg)
        if telegram_token and telegram_chat_id:
            notifier.send_msg(telegram_token, telegram_chat_id, msg)

        notified[ticker] = key

    state["notified"] = notified
    save_state(state_path, state)


def run_once(cfg: dict) -> None:
    tickers = cfg.get("tickers", [])
    if not tickers:
        logging.warning("No tickers configured.")
        return

    output_dir = Path(cfg.get("output_dir", "out"))
    data_dir = Path(cfg.get("data_dir", "data"))
    state_path = cfg.get("state_path", "state/u2_alert_state.json")
    template_path = cfg.get("template_path", "u2_screener_FIJO.html")
    include_fundamentals = cfg.get("include_fundamentals", True)
    intra_tf = cfg.get("intra_tf", "1h")
    details_subdir = cfg.get("details_subdir", "details")

    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    intervals_cfg = cfg.get("intervals_cfg")
    if not intervals_cfg:
        intervals_cfg = [
            {"name": "1d", "interval": "1d", "period": "20y"},
            {"name": "1h", "interval": "1h", "period": "1y"},
            {"name": "15m", "interval": "15m", "period": "60d"},
        ]

    logging.info("Downloading tickers...")
    working_db = collector.download_tickers(
        tickers, intervals_cfg=intervals_cfg, output_path=str(data_dir)
    )
    if cfg.get("save_local", True):
        working_db = collector.save_local(working_db, str(data_dir))

    logging.info("Analyzing data...")
    working_db = analyzer.analyse_data(working_db)
    working_db = analyzer.det_buy_sell(working_db)

    df_out = []
    for ticker in tickers:
        try:
            for tf in ("1d", "1h", "2h", "15m"):
                try:
                    working_db[ticker][tf] = test.annotate_weinstein(
                        working_db[ticker][tf]
                    )
                except Exception:
                    pass

            df1d = working_db[ticker].get("1d")
            if df1d is None or df1d.empty:
                logging.warning("Missing 1d data for %s", ticker)
                continue

            lastrecord = df1d.tail(1)
            bias_val = lastrecord.get("bias", pd.Series([""])).iloc[-1]
            signalmsg = "🟢" if bias_val == "buy" else "🔴" if bias_val == "sell" else "⚪"

            d1, d2, d5, df1, df2 = analyzer.get_deltas(
                working_db, ticker=ticker, timeframe="1d", weekly_mode="friday"
            )

            donch_low = lastrecord.get("donchian_low", pd.Series([None])).iloc[-1]
            donch_high = lastrecord.get("donchian_high", pd.Series([None])).iloc[-1]

            snap = test.get_intraday_snapshot(working_db, ticker, intra_tf=intra_tf)
            _, macd_slope = test.macd_slope_icon(df1d["macd_hist"])

            _last = df1d.iloc[-1]
            pw = {
                "fase": _last.get("fase"),
                "fuerza_%": float(_last.get("fuerza_%", float("nan"))),
                "soporte": float(_last.get("soporte20", _last.get("donchian_low", float("nan")))),
                "resistencia": float(_last.get("resistencia20", _last.get("donchian_high", float("nan")))),
                "m50": float(_last.get("ma50", float("nan"))),
                "m200": float(_last.get("ma200", float("nan"))),
                "slope200": float(_last.get("slope200", float("nan")) * 100.0),
                "dias_en_fase": int(_last.get("dias_en_fase", 0)),
                "fase_previa": _last.get("fase_previa"),
                "fase_cambio": bool(_last.get("fase_cambio", False)),
                "U2_entry": bool(_last.get("U2_entry", False)),
                "support_20": float(_last.get("donchian20_low", _last.get("donchian_low", float("nan")))),
                "resist_20": float(_last.get("donchian20_high", _last.get("donchian_high", float("nan")))),
                "sup_60": float(_last.get("donchian60_low", float("nan"))),
                "res_60": float(_last.get("donchian60_high", float("nan"))),
                "macd_slope3_norm": float(_last.get("macd_slope3_norm", float("nan"))),
            }

            _close = float(lastrecord["close"].iloc[-1])
            _ma50 = float(pw["m50"])
            _ma200 = float(pw["m200"])
            _res20 = float(pw["resist_20"])
            _res60 = float(pw["res_60"])
            _macdh_last = float(lastrecord["macd_hist"].iloc[-1])
            _slope_norm = float(pw["macd_slope3_norm"])

            def _ok(x):
                try:
                    return pd.notna(float(x))
                except Exception:
                    return False

            above_ma50 = _ok(_ma50) and _close >= _ma50
            above_ma200 = _ok(_ma200) and _close >= _ma200
            break_res20 = _ok(_res20) and _close >= _res20
            break_res60 = _ok(_res60) and _close >= _res60
            reacelera_macd = (_ok(_macdh_last) and _macdh_last > 0) and (
                _ok(_slope_norm) and _slope_norm > 0
            )
            dist_ma50_pct = ((_close / _ma50 - 1.0) * 100.0) if _ok(_ma50) else float("nan")

            row = {
                "Ticker": ticker,
                "Close": round(_close, 2),
                "Bias": signalmsg,
                "Δ-1d": d1,
                "Δ-2d": d2,
                "Δ-5d": d5,
                "Δ-lf": df1,
                "Δ-pf": df2,
                "RSI": lastrecord["rsi"].iloc[-1],
                "MACDh": lastrecord["macd_hist"].iloc[-1],
                "Sup20": pw["support_20"],
                "Res20": pw["resist_20"],
                "Sup60": pw["sup_60"],
                "Res60": pw["res_60"],
                "PhaseW": pw["fase"],
                "Force": pw["fuerza_%"],
                "DiasFase": pw["dias_en_fase"],
                "PhasePrev": pw["fase_previa"],
                "PhaseChanged": pw["fase_cambio"],
                "U2_entry": pw["U2_entry"],
                "TF": snap["intra_tf"],
                "AsOf": snap["as_of"],
                "PriceNow": round(snap["price_now"], 2),
                "ClosePrev": round(snap["prev_close"], 2),
                "Δ-1d(now)": round(snap["delta_today"], 1),
                "Δ-open(now)": round(snap["delta_vs_open"], 1),
                "MACD_slope3": round(macd_slope, 6),
                "MACD_slope3_norm": pw["macd_slope3_norm"],
                "AboveMA50": above_ma50,
                "AboveMA200": above_ma200,
                "BreakRes20": break_res20,
                "BreakRes60": break_res60,
                "ReaceleraMACD": reacelera_macd,
                "DistMA50%": dist_ma50_pct,
                "Support": donch_low,
                "Resist.": donch_high,
            }

            if include_fundamentals:
                try:
                    row.update(collector.GetTickerInfo(ticker))
                except Exception as e:
                    logging.warning("Fundamentals failed for %s: %s", ticker, e)

            df_out.append(row)

        except Exception as e:
            logging.exception("Ticker failed: %s (%s)", ticker, e)

    df_out_df = pd.DataFrame(df_out)
    output_json = output_dir / "u2_screener.json"
    test.save_json_data(df_out_df, str(output_json))

    output_html = output_dir / "u2_screener_FIJO_embedded.html"
    build_embedded_html(str(output_json), template_path, str(output_html))

    index_html = output_dir / "index.html"
    index_html.write_text(
        "\n".join(
            [
                "<!doctype html>",
                "<html lang=\"en\">",
                "<head>",
                "  <meta charset=\"utf-8\">",
                "  <meta http-equiv=\"refresh\" content=\"0; url=u2_screener_FIJO_embedded.html\">",
                "  <title>U2 Screener</title>",
                "</head>",
                "<body>",
                "  <p>Redirecting to screener...</p>",
                "</body>",
                "</html>",
            ]
        ),
        encoding="utf-8",
    )

    if cfg.get("publish_template", True):
        template_out = output_dir / "u2_screener_FIJO.html"
        template_out.write_text(Path(template_path).read_text(encoding="utf-8"), encoding="utf-8")

    triple_template = Path(cfg.get("triple_template_path", "triple_screen.html"))
    if triple_template.exists():
        triple_out = output_dir / "triple_screen.html"
        triple_out.write_text(triple_template.read_text(encoding="utf-8"), encoding="utf-8")

    points_by_tf = cfg.get("detail_points", {"1d": 120, "2h": 120, "15m": 120})
    build_details(working_db, tickers, output_dir / details_subdir, points_by_tf)

    notify_u2_entries(df_out_df, cfg, state_path)

    logging.info("Generated %s (%d rows)", output_json, len(df_out_df))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    config_path = os.getenv("U2_CONFIG_PATH", DEFAULT_CONFIG_PATH)
    last_minutes = 30.0
    while True:
        try:
            cfg = load_config(config_path)
            run_once(cfg)
            last_minutes = float(cfg.get("update_minutes", last_minutes))
        except Exception as e:
            logging.exception("Runner error: %s", e)

        time.sleep(max(60, int(last_minutes * 60)))


if __name__ == "__main__":
    main()
