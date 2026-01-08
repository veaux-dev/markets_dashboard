import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import pandas as pd

def quick_viz(df, title="Chart"):
    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df.index.to_pydatetime().tolist(),
        open=df['open'].astype(float).tolist(),
        high=df['high'].astype(float).tolist(),
        low=df['low'].astype(float).tolist(),
        close=df['close'].astype(float).tolist(),
        name='Candles'
    ))

    # EMAs
    if "ema_short" in df.columns:
        fig.add_trace(go.Scatter(x=df.index.to_pydatetime().tolist(), y=df["ema_short"].astype(float).fillna(0).tolist(), 
                                 line=dict(color="blue"), name="EMA Short"))
    if "ema_long" in df.columns:
        fig.add_trace(go.Scatter(x=df.index.to_pydatetime().tolist(), y=df["ema_long"].astype(float).fillna(0).tolist(),
                                 line=dict(color="orange"), name="EMA Long"))

    # Donchian
    if "donchian_high" in df.columns:
        fig.add_trace(go.Scatter(x=df.index.to_pydatetime().tolist(), y=df["donchian_high"].astype(float).fillna(0).tolist(), 
                                 line=dict(color="green", dash="dot"), name="Donch High"))
    if "donchian_low" in df.columns:
        fig.add_trace(go.Scatter(x=df.index.to_pydatetime().tolist(), y=df["donchian_low"].astype(float).fillna(0).tolist(), 
                                 line=dict(color="red", dash="dot"), name="Donch Low"))

    fig.update_layout(title=title, xaxis_rangeslider_visible=False, yaxis_range_Mode='tozero')

    pio.renderers.default = "browser"
    fig.show()

def quick_viz_triple_screen(dfs, tick="TICKER"):
    """
    dfs = dict con claves '1d', '2h', '15m' y valores DataFrames
    """
    fig = make_subplots(
        rows=3, cols=3, shared_xaxes=False,
        row_heights=[0.6, 0.2, 0.2],
        column_titles=("1D", "2H", "15M"),
        vertical_spacing=0.05, horizontal_spacing=0.05,
        subplot_titles=(f"", "", "", "RSI", "RSI", "RSI", "MACD", "MACD", "MACD")
    )

  

    colmap = {"1d":1, "2h":2, "15m":3}

    for tf, col in colmap.items():
        df = dfs[tf]

        # Row 1 → Price + EMAs + BBands shaded
        fig.add_trace(go.Candlestick(
            x=df.index.to_pydatetime().tolist(),
            open=df["open"].astype(float).fillna(0).tolist(),
            high=df["high"].astype(float).fillna(0).tolist(),
            low=df["low"].astype(float).fillna(0).tolist(),
            close=df["close"].astype(float).fillna(0).tolist(),
            name=f"Candles {tf}"
        ), row=1, col=col)

        if "ema_short" in df:
            fig.add_trace(go.Scatter(
                x=df.index.to_pydatetime().tolist(),
                y=df["ema_short"].astype(float).fillna(0).tolist(),
                line=dict(color="blue"), name=f"EMA Short {tf}"
            ), row=1, col=col)

        if "ema_long" in df:
            fig.add_trace(go.Scatter(
                x=df.index.to_pydatetime().tolist(),
                y=df["ema_long"].astype(float).fillna(0).tolist(),
                line=dict(color="orange"), name=f"EMA Long {tf}"
            ), row=1, col=col)

        if "bb_upper" in df and "bb_lower" in df:
            fig.add_trace(go.Scatter(
                x=df.index.to_pydatetime().tolist(),
                y=df["bb_upper"].astype(float).fillna(0).tolist(),
                line=dict(color="rgba(0,0,0,0)"), showlegend=False
            ), row=1, col=col)
            fig.add_trace(go.Scatter(
                x=df.index.to_pydatetime().tolist(),
                y=df["bb_lower"].astype(float).fillna(0).tolist(),
                line=dict(color="rgba(0,0,0,0)"),
                fill="tonexty", fillcolor="rgba(0,0,255,0.1)",
                showlegend=False
            ), row=1, col=col)

        # Row 2 → RSI
        if "rsi" in df:
            fig.add_trace(go.Scatter(
                x=df.index.to_pydatetime().tolist(),
                y=df["rsi"].astype(float).fillna(0).tolist(),
                line=dict(color="purple"), name=f"RSI {tf}"
            ), row=2, col=col)

        # Row 3 → MACD
        if "macd_hist" in df:
            fig.add_trace(go.Bar(
                x=df.index.to_pydatetime().tolist(),
                y=df["macd_hist"].astype(float).fillna(0).tolist(),
                name=f"MACD Hist {tf}", marker_color="grey"
            ), row=3, col=col)

        if "macd" in df:
            fig.add_trace(go.Scatter(
                x=df.index.to_pydatetime().tolist(),
                y=df["macd"].astype(float).fillna(0).tolist(),
                line=dict(color="blue"), name=f"MACD {tf}"
            ), row=3, col=col)

        if "macd_signal" in df:
            fig.add_trace(go.Scatter(
                x=df.index.to_pydatetime().tolist(),
                y=df["macd_signal"].astype(float).fillna(0).tolist(),
                line=dict(color="red"), name=f"Signal {tf}"
            ), row=3, col=col)

    

    for r in [1,2,3]:
        for c in [1,2,3]:
            fig.update_yaxes(autorange=True, title_text="", row=r, col=c, overlaying=None)
            fig.update_xaxes(type="category", row=r, col=c)

    fig.update_xaxes(matches="x1", row=2, col=1)  # RSI 1D = mismo X que velas 1D
    fig.update_xaxes(matches="x1", row=3, col=1)  # MACD 1D = mismo X que velas 1D

    fig.update_xaxes(matches="x2", row=2, col=2)  # RSI 2H con velas 2H
    fig.update_xaxes(matches="x2", row=3, col=2)  # MACD 2H con velas 2H

    fig.update_xaxes(matches="x3", row=2, col=3)  # RSI 15m con velas 15m
    fig.update_xaxes(matches="x3", row=3, col=3)  # MACD 15m con velas 15m

    for i in range(1, 10):   # xaxis, xaxis2, ..., xaxis9
        fig.update_layout({f"xaxis{i}": dict(rangeslider=dict(visible=False))})
    
    fig.update_layout(title=f"{tick} – Triple Screen", xaxis_rangeslider_visible=False)



    pio.renderers.default = "browser"
    fig.show()
    