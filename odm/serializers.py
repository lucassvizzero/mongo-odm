import json
from datetime import datetime

from bson import ObjectId


class ODMSerializer(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat() + 'Z'
        return json.JSONEncoder.default(self, o)
