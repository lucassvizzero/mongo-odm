import json
from datetime import datetime

from bson import ObjectId, Timestamp


class ODMSerializer(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Timestamp):
            return o.as_datetime().isoformat() + 'Z'
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat() + 'Z'
        return json.JSONEncoder.default(self, o)
