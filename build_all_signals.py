from svc import screener



if __name__ == "__main__":
    # ...
    all_signals = screener.run_screener(timeframe="1d",all_data=True)
    print(f'All signals processed....{len(all_signals)} signasls were computed')
