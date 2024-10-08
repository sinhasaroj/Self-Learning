from marshmallow import Schema, fields

class ItemSchema(Schema):
    # id = fields.Str(dump_only=True)
    name = fields.Str(required=True)
    price = fields.Str(required=True)

class ItemGetSchema(Schema):
    id = fields.Str(dump_only=True)
    name = fields.Str(required=True)
    price = fields.Str(required=True)


class ItemQuerySchema(Schema):
    id = fields.Str(required=True)


class ItemOptionalQuerySchema(Schema):
    id = fields.Str(required=False)