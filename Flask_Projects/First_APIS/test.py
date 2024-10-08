import requests
import json

url = 'http://127.0.0.1:5000/add-item'
data = [
  {
    "name": "Green Apple Mojito",
    "price": 60
  },
  {
    "name": "Momos",
    "price": 80
  },
  {
    "name": "Somosha Chat",
    "price": 40
  }
]

response = requests.post(url, json=json.dumps(data))
print(response.status_code)
print(response.json())