from flask import request
from flask_smorest import Blueprint
from flask.views import MethodView
import uuid

from schemas import ItemSchema, ItemGetSchema, ItemOptionalQuerySchema, ItemQuerySchema
from db import items

blp = Blueprint("items", __name__, description="Operations on Items")

@blp.route("/item")
class Item(MethodView):

    @blp.response(200,ItemGetSchema(many=True))
    @blp.arguments(ItemOptionalQuerySchema,location="query")
    def get(self,args):
        id = args.get("id")
        if id:
            for item in items:
                if item["id"] == id:
                    return [item]
            return {"message":"Item doesn't exists!"},404
        else:
            return items

    @blp.arguments(ItemSchema)
    def post(self, request_data):

        new_data = {
            "id": uuid.uuid4().hex
        }
 
        new_data.update(request_data)
        items.append(new_data)
        return {"message":"Item is successfully added"},201

    @blp.arguments(ItemSchema)
    @blp.arguments(ItemQuerySchema,location="query")
    def put(self, request_data, args): 
        id = args.get("id")
        if id is None:
            return {"message":"Given id not found!"},404
        for item in items:
            if item["id"] == id:
                item["name"] = request_data["name"]
                item["price"] = request_data["price"]
                return {"message":"Item is successfully updated"}
            
        return {"message":"Item doesn't exists"},404

    @blp.arguments(ItemOptionalQuerySchema,location="query")
    def delete(self, args):
        id = args.get("id")
        if id is None:
            return {"message":"Given id not found!"},404
        
        for item in items:
                if item["id"] == id:
                    items.remove(item)
                    return {"message":"Item deleted Successfully!"}
                
        return {"message":"Item doesn't exists!"},404