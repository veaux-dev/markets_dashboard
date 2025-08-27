#handles the notifications to Telegram. 
import requests

def send_msg(token, chat_id, msg, parse_mode="HTML"):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": msg, "parse_mode": parse_mode}
    response = requests.post(url, data=data)
    return response.ok

def notif(): 
    pass

def send_discord(webhook_url, msg):
    if not webhook_url:
        print("No Discord webhook URL provided.")
        return False
    data = {"content": msg}
    response=requests.post(webhook_url, json=data)
 
    return response.ok
