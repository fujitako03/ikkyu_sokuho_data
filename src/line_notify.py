import os

import requests


def send_line_message(message):
    access_token = os.getenv("LINE_TOKEN")
    url = "https://notify-api.line.me/api/notify"
    headers = {'Authorization': 'Bearer ' + access_token}
    payload = {'message': message}
    r = requests.post(url, headers=headers, params=payload,)
