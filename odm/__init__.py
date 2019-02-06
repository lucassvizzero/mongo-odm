"""
odm module.
Basic odm functions.
"""

import copy
from datetime import datetime
import logging

from bson.objectid import ObjectId
from pymongo import ReturnDocument
from pymongo.results import UpdateResult, DeleteResult

from .data_types import Relations, Types
from .exceptions import DocumentNotFound


class BaseModel:
    """
    BaseModel class.
    It serves as a base model class for all child models.

    :method filter(params): Filters a query.
    :method preparseFields(params): Preparses the input fields.
    :method dict_rep(params): Iterates through a query and checks it.
    :method paginate(params): Paginates a result.
    :method find(params, force_single_result, relations, force_fetch_protected_fields): Finds a query.
    :method first(params, relations): Returns the first find of a query.
    :method paged(params, pagination, relations, force_fetch_protected_fields): Pages a result.
    :method remove(_id): Removes a result.
    :method save(bus_object): Saves a result.
    :method _clear_protected_fields(model, result, force_fetch_protected_fields): Cleans the protected fields.
    :method _relationships(criteria, keyArray, force_fetch_protected_fields): Check the relationships.
    """

    PRE_CREATE = 'pre_create'
    POST_CREATE = 'post_crete'
    PRE_UPDATE = 'pre_update'
    POST_UPDATE = 'post_update'
    PRE_DELETE = 'pre_delete'
    POST_DELETE = 'post_delete'

    db = None
    fields = dict()
    collection_name = None
    protected_fields = []
    relations = {}
    softDeletes = False
    hooks = list()
    debug = False

    def __init__(self, db):
        self.db = db
    
    async def pre_delete(self, model_id):
        pass

    async def post_delete(self, model_id, saved, softDeletes):
        pass

    async def pre_create(self):
        pass

    async def post_create(self, model_id, saved):
        pass

    async def pre_update(self, model_id):
        pass

    async def post_update(self, model_id, saved):
        pass

    def sort_query(self, params: dict, tuples=False):
        """
        Generates SORT query according to pymongo standard.

        :param params: Parameters to be added to the function.
        :param tuples: tuple list to be used for sorting => [(key, order)].
        :return: Sort query.
        """

        sort = None
        if params.get('sort_asc'):
            sort = {}
            for field in params.get('sort_asc').split(','):
                sort[field] = 1
        if params.get('sort_desc'):
            sort = {}
            for field in params.get('sort_desc').split(','):
                sort[field] = -1
        if params.get('sort'):
            try:
                sort = {"_id": int(params.get('sort'))}
            except Exception as e:
                sort = params.get('sort')

        if sort is None:
            sort_query = {"_id": 1}
        else:
            sort_query = {k: sort[k] for k in sort}

        if tuples:
            sort_query = [(k, sort_query[k]) for k in sort_query]

        return sort_query
    
    def _split(self, string, sep: str=','):
        tokens = string.split(sep)
        tokens = [t.strip() for t in tokens]  # rm white spaces 
        tokens = list(filter(None, tokens))  # filter out empty strings
        return tokens

    def filter(self, params: dict) -> dict:
        """
        Filters a query.

        :param params: Parameters to be added to the function.
        :return: Filtered query.
        """
        query = dict()
        fields = self.fields
        fields["created_at"] = Types.ISODate
        fields["updated_at"] = Types.ISODate
        text_fields = params.get('text_fields', [])
        if type(text_fields) == str:
            text_fields = self._split(text_fields)
        for name in params:
            param = params[name]
            if fields.get(name) is not None:
                if fields[name] == Types.ObjectId:
                    if type(param) == str:
                        query[name] = ObjectId(param)
                    elif type(param) == dict:
                        if param.get("$in"):
                            param["$in"] = [ObjectId(_id)
                                            for _id in param["$in"]]
                            query[name] = param
                    elif type(param) == Types.ObjectId:
                        query[name] = param
                    else:
                        raise NotImplementedError

                elif fields[name] == Types.ObjectIdList:
                    if type(param) == list:
                        query[name] = {'$all': [ObjectId(s)for s in param]}
                    elif type(param) == str:
                        query[name] = {'$all': [ObjectId(s)for s in self._split(param)]}

                elif fields[name] == Types.ISODate:
                    if isinstance(param, str):
                        query[name] = datetime.strptime(param[:19], "%Y-%m-%dT%H:%M:%S")
                    else:
                        query[name] = param
                elif fields[name] == Types.Object:
                    query[name] = param

                elif fields[name] == Types.Array:
                    query[name] = param

                elif fields[name] == Types.Integer:
                    if not isinstance(param, int):
                        query[name] = int(param)
                    else:
                        query[name] = param

                elif fields[name] == Types.Double:
                    query[name] = float(param)

                elif fields[name] == Types.Boolean:
                    query[name] = param

                elif fields[name] == Types.String:
                    if name in text_fields:
                        query[name] = {
                            "$regex": ".*" + str(param) + ".*",
                            "$options": "ig"
                        }
                    else:
                        query[name] = param
            else:
                reserved_names = [
                    "sort",
                    "sort_asc",
                    "sort_desc",
                    "page",
                    "page_size",
                    "relations",
                    "text_fields"
                ]
                if name not in reserved_names and '.' not in name:
                    query[name] = param

        if params.get("$or"):
            query["$or"] = params.get("$or")

        return query

    def preparse_fields(self, params: dict) -> dict:
        """
        Preparses the input fields.

        :param params: Parameters to be added to the function.
        :return: A parsed field.
        """
        query = dict()
        for name in self.fields:
            if params.get(name) is not None:
                param = params.get(name)
                if self.fields[name] == Types.ObjectId:
                    query[name] = ObjectId(param)
                elif self.fields[name] == Types.ObjectIdList:
                    if type(param) == list:
                        query[name] = [ObjectId(s) for s in param]
                elif self.fields[name] == Types.ISODate:
                    if isinstance(param, str):
                        query[name] = datetime.strptime(
                            param[:19], "%Y-%m-%dT%H:%M:%S"
                        )
                    elif isinstance(param, datetime):
                        query[name] = param
                    else:
                        raise Exception('wrong type [{}] for {}'.format(
                            type(param),
                            name
                        ))

                elif self.fields[name] == Types.Object:
                    query[name] = param

                elif self.fields[name] == Types.Array:
                    query[name] = param

                elif self.fields[name] == Types.Integer:
                    query[name] = int(param)

                elif self.fields[name] == Types.Double:
                    query[name] = float(param)

                elif self.fields[name] == Types.Boolean:
                    query[name] = param

                elif self.fields[name] == Types.String:

                    query[name] = str(param)
                else:
                    query[name] = param
        return query

    def dict_rep(self, params: dict) -> dict:
        """
        Iterates through a query and checks it.

        :param params: Parameters to be added to the function.
        :return: Iterated query.
        """
        query = dict()
        fields = self.fields
        fields["created_at"] = Types.ISODate
        fields["updated_at"] = Types.ISODate
        fields["deleted_at"] = Types.ISODate
        for name in fields:
            if params.get(name) is not None:
                param = params.get(name)
                if fields[name] == Types.ObjectId:
                    query[name] = str(param)
                elif fields[name] == Types.ObjectIdList:
                    query[name] = [str(s) for s in param]
                elif fields[name] == Types.ISODate:
                    if isinstance(param, str):
                        query[name] = datetime.strptime(param[:19], "%Y-%m-%dT%H:%M:%S")
                    else:
                        query[name] = param.isoformat() + 'Z'

                elif fields[name] == Types.Object:
                    query[name] = param

                elif fields[name] == Types.Array:
                    query[name] = param

                elif fields[name] == Types.Integer:
                    query[name] = int(param)

                elif fields[name] == Types.Double:
                    query[name] = float(param)

                elif fields[name] == Types.Boolean:
                    query[name] = param

                elif fields[name] == Types.String:
                    query[name] = str(param)
                else:
                    query[name] = param

        for name in self.relations:
            if params.get(name) is not None:
                m = self.relations[name]["model"](self.db)
                if self.relations[name]["type"] in [Relations.hasManyLocally, Relations.hasMany, Relations.belongsToMany]:
                    items = params[name]
                    query[name] = []
                    for k, item in enumerate(items):
                        query[name].append(m.dict_rep(item))
                else:
                    query[name] = m.dict_rep(params[name])

        return query

    def paginate(self, params: dict) -> dict:
        """
        Paginates a result.

        :param params: Parameters to be added to the function.
        :return: Paginated result.
        """
        pagination = {}

        sort_query = self.sort_query(params)

        pagination["sort"] = sort_query

        if params.get('page'):
            pagination["page"] = int(params.get('page'))
        else:
            pagination["page"] = 0

        if params.get('page_size'):
            pagination["page_size"] = int(params.get('page_size'))
        else:
            pagination["page_size"] = 50

        return pagination

    async def find(self, params: dict, force_single_result: bool = False, relations: list = list(),
                   force_fetch_protected_fields: list = list()):
        """
        Finds a query.

        :param params: Parameters to be added to the function.
        :param force_single_result: Boolean value.
        :param relations: List of relations.
        :param force_fetch_protected_fields: List of protected fields to be fetched.
        :return: Query to be found.
        """

        criteria = self.filter(params)
        criteria["deleted_at"] = {"$exists": False}

        if len(relations):
            sort_query = self.sort_query(params)
            ag = self._relationships(criteria, relations, force_fetch_protected_fields)
            ag.insert(1, {'$sort': sort_query})

            # Allows dot notation filters to be considered in queries
            extra_filters = {}
            for key, value in params.items():
                if '.' in key:
                    parts = key.split('.')
                    rel_name = parts[0]
                    rel_filter_field = parts[1]
                    rel = self.relations.get(rel_name)
                    if rel:
                        rel_instance = rel['model'](self.db)
                        rel_filter = rel_instance.filter({rel_filter_field: value})
                        if rel_filter.get(rel_filter_field):
                            extra_filters[key] = rel_filter.get(rel_filter_field)
            if extra_filters:
                ag.append({
                    '$match': extra_filters
                })
            
            if self.debug:
                print('aggregation', ag)

            cursor = self.db[self.collection_name].aggregate(ag)
            results = list()
            async for doc in cursor:
                results.append(self.dict_rep(doc))
        else:
            sort_query = self.sort_query(params, tuples=True)
            cursor = self.db[self.collection_name].find(criteria, sort=sort_query)
            results = list()
            async for doc in cursor:
                results.append(self.dict_rep(doc))

        for result in results:
            results = self._clear_protected_fields(
                self, results, force_fetch_protected_fields)

            for key in self.relations:
                relation = self.relations[key]["model"](self.db)
                if result.get(key) is not None:
                    if type(result[key]) == list:
                        for k, val in enumerate(result[key]):
                            result[key][k] = self._clear_protected_fields(
                                relation, val, force_fetch_protected_fields)
                    else:
                        result[key] = self._clear_protected_fields(
                            relation, result[key], force_fetch_protected_fields)

            if len(results) == 0:
                return results

            if isinstance(results, list) and results and force_single_result:
                # if (type(results)==list and len(results) and force_single_result):
                return results[0]
            else:
                return results

    async def count(self, params: dict):
        """
        Finds a query.

        :param params: Parameters to be added to the function.
        :return: Query to be found.
        """

        params = self.filter(params)
        # logging.info(params)
        params["deleted_at"] = {"$exists": False}

        cursor = await self.db[self.collection_name].count(params)
        return cursor

    async def find_and_update(self, criteria, update):

        sort_query = self.sort_query(criteria, tuples=True)
        criteria = self.filter(criteria)

        set_query = update.get('$set', dict())
        set_query = self.preparse_fields(set_query)
        set_query['updated_at'] = datetime.utcnow()
        set_query.pop('_id', None)  # remove _id from $set operation if it exists 
        update['$set'] = set_query


        if self.PRE_UPDATE in self.hooks:
            pre_doc = await self.db[self.collection_name].find_one(criteria, sort=sort_query)
            await self.pre_update(str(pre_doc['_id']))

        r = await self.db[self.collection_name].find_one_and_update(criteria, update,
                                                                    sort=sort_query,
                                                                    return_document=ReturnDocument.AFTER)

        if self.POST_UPDATE in self.hooks:
            post_doc = self.dict_rep(r)
            await self.post_update(post_doc.get('_id'), self.dict_rep(post_doc))

        if r:
            return self.dict_rep(r)

    def first(self, params: dict, relations: list = list()):
        """
        Returns the first find of a query.

        :param params: Parameters to be added to the function.
        :param relations: List of relations.
        :return: First result of found query.
        """
        return self.find(params, True, relations)

    def _clear_protected_fields(self, model, result, force_fetch_protected_fields: list = list()):
        """
        Cleans the protected fields.

        :param model: Model instance.
        :param result: Result variable.
        :param force_fetch_protected_fields: List of protected fields to be fetched.
        :return: Protected fields already cleaned.
        """
        if type(result) == list:
            a = []
            for r in result:
                a.append(self._clear_protected_fields(
                    model, r, force_fetch_protected_fields))
            return a
        else:
            for p in model.protected_fields:
                if result.get(p) is not None and p not in force_fetch_protected_fields:
                    del result[p]
        return result

    def _relationships(self, criteria: dict, key_array: list, force_fetch_protected_fields: list = list(),
                      pagination: dict = dict()):
        """
        Check the relationships.

        :param criteria: Criteria to be used.
        :param key_array: List of keys to be checked.
        :param force_fetch_protected_fields: List of protected fields to be fetched.
        :param pagination: pagination options.
        :return: List of checked relations.
        """
        key_array.sort()
        the_keys = dict()
        for k, val in enumerate(key_array):
            # logging.info(k)
            key = key_array[k]
            the_keys[key] = []

        tgt = dict()
        for i in the_keys:
            if "." not in i:
                tgt[i] = []
            # rever nested fields
            # else:
            #    if(!dot.pick(i,tgt,false))
            #        dot.str(i, the_keys[i], tgt)
        project = {"$project": {}}
        aggregation = [{"$match": criteria}]

        if pagination != dict():
            aggregation.append({"$sort": pagination["sort"]})
            aggregation.append({"$skip": pagination["page_size"] * pagination["page"]})
            aggregation.append({"$limit": pagination["page_size"]})

        for j in self.fields:
            if j in self.fields:
                project["$project"][j] = True
        a = copy.deepcopy(project["$project"])
        for k in a:
            if k in self.protected_fields and k not in force_fetch_protected_fields:
                del project["$project"][k]

        for i in self.relations:
            if i in key_array:
                if self.relations[i]["type"] == Relations.hasManyLocally:
                    lookup = {
                        '$lookup': {
                            "from": self.relations[i]["model"](self.db).collection_name,
                            'let': {'id_list': '$'+self.relations[i]["localKey"]},
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {'$in': ['$_id', '$$id_list']}
                                    }
                                }
                            ],
                            'as': i
                        }
                    }
                    aggregation.append(lookup)
                else:
                    lookup = {"$lookup": {
                        "from": self.relations[i]["model"](self.db).collection_name,
                        "localField": self.relations[i]["localKey"],
                        "foreignField": self.relations[i]["foreignKey"],
                        "as": i
                    }}
                    aggregation.append(lookup)

                if self.relations[i]["type"] == Relations.belongsTo:
                    project["$project"][i] = {"$arrayElemAt": ["$" + i, 0]}

                if self.relations[i]["type"] == Relations.hasOne:
                    project["$project"][i] = {"$arrayElemAt": ["$" + i, 0]}

                if self.relations[i]["type"] == Relations.belongsToMany:
                    project["$project"][i] = {"$filter": {
                        "input": "$" + str(i),
                        "as": str(i),
                        "cond": {"$ifNull": ["$$" + str(i) + ".deleted_at", True]}
                    }}

                if self.relations[i]["type"] == Relations.hasMany:
                    project["$project"][i] = {"$filter": {
                        "input": "$" + str(i),
                        "as": str(i),
                        "cond": {"$ifNull": ["$$" + str(i) + ".deleted_at", True]}
                    }}

                if self.relations[i]["type"] == Relations.hasManyLocally:
                    project["$project"][i] = {"$filter": {
                        "input": "$" + str(i),
                        "as": str(i),
                        "cond": {"$ifNull": ["$$" + str(i) + ".deleted_at", True]}
                    }}

        aggregation.append(project)
        if pagination.get("sort"):
            aggregation.append({"$sort": pagination["sort"]})
        return aggregation

    async def paged(self, params: dict, pagination: dict, relations: list,
                    force_fetch_protected_fields: list = list()) -> dict:
        """
        Pages a result.

        :param params: Parameters to be added to the function.
        :param pagination: Dictionary of pagination.
        :param relations: List of relations.
        :param force_fetch_protected_fields: List of protected fields to be fetched.
        :return: Paged result.
        """
        pagination = self.paginate(pagination)
        criteria = self.filter(params)
        ag = self._relationships(criteria, relations, force_fetch_protected_fields, pagination=pagination)
        criteria["deleted_at"] = {"$exists": False}
        # Allows dot notation filters to be considered in queries
        extra_filters = {}
        for key, value in params.items():
            if '.' in key:
                parts = key.split('.')
                rel_name = parts[0]
                rel_filter_field = parts[1]
                rel = self.relations.get(rel_name)
                if rel:
                    rel_instance = rel['model'](self.db)
                    rel_filter = rel_instance.filter({rel_filter_field: value})
                    if rel_filter.get(rel_filter_field):
                        extra_filters[key] = rel_filter.get(rel_filter_field)
        if extra_filters:
            ag.append({
                '$match': extra_filters
            })

        if self.debug:
            print('aggregation', ag)

        cursor = self.db[self.collection_name].aggregate(ag)

        count = await self.db[self.collection_name].find(criteria).count()
        results = list()
        async for doc in cursor:
            results.append(self.dict_rep(doc))

        for result in results:
            results = self._clear_protected_fields(
                self, results, force_fetch_protected_fields)

            for key in self.relations:
                relation = self.relations[key]["model"](self.db)
                if result.get(key) is not None:
                    if type(result[key]) == list:
                        for k, val in enumerate(result[key]):
                            result[key][k] = self._clear_protected_fields(
                                relation, val, force_fetch_protected_fields)
                    else:
                        if result.get(key) is not None:
                            result[key] = self._clear_protected_fields(
                                relation, result[key], force_fetch_protected_fields)
        return {
            "results": results,
            "count": count,
            "page": pagination["page"],
            "page_size": pagination["page_size"]
        }

    async def remove(self, _id: str, force: bool = False) -> dict:
        """
        Removes a result.

        :param _id: Identifier of result to be removed.
        :param force: Permanently removes the doc even if soft-delete is enabled.
        :return: Database with object removed.
        """
        now = datetime.utcnow()
        saved = None
        removed = False

        if self.PRE_DELETE in self.hooks:
            await self.pre_delete(str(_id))

        if not self.softDeletes or force:
            r = await self.db[self.collection_name].delete_one({"_id": ObjectId(_id)})
            if isinstance(r, DeleteResult):
                removed = bool(r.deleted_count)
            else:
                raise Exception('Unexpected query result')
        else:
            cache_rel = await self.first({"_id": _id}, [])
            if not cache_rel:
                raise DocumentNotFound()
            cache_rel = self.preparse_fields(cache_rel)
            cache_rel["deleted_at"] = now
            del cache_rel["_id"]
            r = await self.db[self.collection_name].update_one({"_id": ObjectId(_id)}, {"$set": cache_rel})
            if isinstance(r, UpdateResult):
                removed = bool(r.modified_count)
            else:
                raise Exception('Unexpected query result')
            saved = copy.deepcopy(cache_rel)
            saved['_id'] = _id
            saved = self.dict_rep(saved)

        if self.POST_DELETE in self.hooks:
            await self.post_delete(str(_id), saved, self.softDeletes)

        return removed

    async def _db_update_one(self, where, to_save):
        if self.PRE_UPDATE in self.hooks:
            await self.pre_update(str(where.get('_id')))

        r = await self.db[self.collection_name].update_one(where, {'$set': to_save})

        if self.POST_UPDATE in self.hooks:
            post_doc = self.dict_rep(to_save)
            await self.post_update(str(post_doc.get('_id')), post_doc)

        return r

    async def _db_save(self, to_save):
        _id = to_save.get('_id')
        is_update = bool(_id)  # is update if there is an _id

        # Pre hooks
        if is_update and self.PRE_UPDATE in self.hooks:
            await self.pre_update(str(_id))
        elif self.PRE_CREATE in self.hooks:
            await self.pre_create()

        # actual persistance
        _id = await self.db[self.collection_name].save(to_save)
        post_doc = self.dict_rep(dict(to_save, **{'_id': _id}))

        # Post hooks
        if is_update and self.POST_UPDATE in self.hooks:
            await self.post_update(str(_id), post_doc)
        elif self.POST_CREATE in self.hooks:
            await self.post_create(str(_id), post_doc)

        return _id

    async def save(self, bus_object: dict) -> dict:
        """
        Saves a result.

        :param bus_object: Object to be saved.
        :return: Saved dictionary.
        """
        to_save = self.preparse_fields(bus_object)
        if to_save.get("_id") is None:
            to_save["created_at"] = datetime.utcnow()
            to_save["updated_at"] = datetime.utcnow()
        else:
            to_save["updated_at"] = datetime.utcnow()
        _id = await self._db_save(to_save)
        to_save["_id"] = _id
        return self.dict_rep(to_save)
