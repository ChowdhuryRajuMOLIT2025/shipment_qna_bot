import json

import requests

url = "http://127.0.0.1:8000/api/chat"
payload = {"question": "test string input", "consignee_codes": "CODE1,CODE2"}
headers = {"Content-Type": "application/json"}

try:
    response = requests.post(url, json=payload, headers=headers)
    print(f"Status Code: {response.status_code}")
    print(f"Response Body: {response.text}")

    if response.status_code == 200:
        print("SUCCESS: Request processed successfully.")
    else:
        print("FAILURE: Request failed.")
except Exception as e:
    print(f"ERROR: {e}")
