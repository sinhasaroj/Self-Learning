from flask import Flask,jsonify,request
import json

app = Flask(__name__)

items = [
    {
        "name":"Green Apple Mojito",
        "price":60
    },
    {
        "name":"Momos",
        "price":80
    },
    {
        "name":"Somosha Chat",
        "price":40
    }
]

@app.route('/get-items', methods=["GET"])
def get_items():
    return jsonify(items),200

@app.route('/add-item', methods=["POST"])
def add_items():
    request_data = request.get_json()
    items.append(request_data)
    return {"msg":"Item added successfully"},201

# Get a particular item
    


