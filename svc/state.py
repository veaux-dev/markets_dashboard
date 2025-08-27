# State.py handles internal memory of the APP (last notification sent, tickers etc.. to avoid notifications spam)

import os
import json

STATE_FILE = "app.json"

last_snapshots_ts = ''
last_alerts={}

def load_state():
    global last_snapshots_ts, last_alerts
    if os.path.exists(f"state/{STATE_FILE}"):
        with open(f"state/{STATE_FILE}", "r") as f:
            saved_state = json.load(f)
            return saved_state, saved_state.get('last_snapshot_ts',''), saved_state.get('last_alerts',{})
    else:
        last_snapshots_ts = ''
        last_alerts = {}
        return {'last_snapshot_ts':'', 'last_alerts':{}}, last_snapshots_ts, last_alerts

def save_state(state):
    os.makedirs("state", exist_ok=True)
    with open(f"state/{STATE_FILE}", "w") as f:
        json.dump(state, f)
        print(f'New state saved: {state}')

def last_closed_bar(df):
    if df is not None and not df.empty:
        return df.index[-1].isoformat()
    return None

def should_send_snapshot(df_2h,state):
    
    if df_2h is None or df_2h.empty:
        return False

    last_bar = last_closed_bar(df_2h)
    if last_bar is None:
        return False

    print(f"Last Bar:{str(last_bar)} vs Last Saved State:{str(state.get('last_snapshot_ts'))}")

    if str(last_bar) == str(state.get('last_snapshot_ts')):
        return False

    print(f'Sending Snapshot for new bar {str(last_bar)}')
    return True