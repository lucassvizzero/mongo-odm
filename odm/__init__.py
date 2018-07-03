"""
odm module.
Basic odm functions.
"""

import copy
from datetime import datetime

from bson import ObjectId
from pymongo import ReturnDocument

from .data_types import Relations, Types


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
    :method cleanProtectedFields(model, result, force_fetch_protected_fields): Cleans the protected fields.
    :method relationships(criteria, keyArray, force_fetch_protected_fields): Check the relationships.
    :method paged(params, pagination, relations, force_fetch_protected_fields): Pages a result.
    :method remove(_id): Removes a result.
    :method save(bus_object): Saves a result.
    :method checkCascadePolicy(cascade, cascade_policy): Check the CascadePolicy.
    :method saveCollection(model, properti, relation, result_model): Saves the collection.
    """

    def __init__(self, db):
        self.db = db
        self.fields = dict()
        self.collection_name = None
        self.protected_fields = []
        self.relations = {}
        self.softDeletes = False

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
                    if isinstance(param, int):
                        query[name] = int(param)
                    else:
                        query[name] = param

                elif fields[name] == Types.Double:
                    query[name] = float(param)

                elif fields[name] == Types.Boolean:
                    query[name] = param

                elif fields[name] == Types.String:
                    if "$regex" in param:
                        query[name] = param
                    else:
                        query[name] = {"$regex": ".*" +
                                                 str(param) + ".*", "$options": 'ig'}
            else:
                if name not in ["sort", "sort_asc", "sort_desc", "page", "page_size"]:
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

                elif self.fields[name] == Types.ISODate:
                    query[name] = datetime.strptime(
                        param[:19], "%Y-%m-%dT%H:%M:%S")

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
        for name in fields:
            if params.get(name) is not None:
                param = params.get(name)
                if fields[name] == Types.ObjectId:
                    query[name] = str(param)

                elif fields[name] == Types.ISODate:
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
                if self.relations[name]["type"] in [Relations.hasMany, Relations.belongsToMany, Relations.manyToMany]:
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
            ag = self.relationships(criteria, relations, force_fetch_protected_fields)
            ag.insert(1, {'$sort': sort_query})

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
            results = self.clean_protected_fields(
                self, results, force_fetch_protected_fields)

            for key in self.relations:
                relation = self.relations[key]["model"](self.db)
                if result.get(key) is not None:
                    if type(result[key]) == list:
                        for k, val in enumerate(result[key]):
                            result[key][k] = self.clean_protected_fields(
                                relation, val, force_fetch_protected_fields)
                    else:
                        result[key] = self.clean_protected_fields(
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
        # print(params)
        params["deleted_at"] = {"$exists": False}

        cursor = await self.db[self.collection_name].count(params)
        return cursor

    async def find_and_update(self, criteria, update):

        sort_query = self.sort_query(criteria, tuples=True)
        criteria = self.filter(criteria)

        # print(criteria)
        r = await self.db[self.collection_name].find_one_and_update(criteria, update,
                                                                    sort=sort_query,
                                                                    return_document=ReturnDocument.AFTER)

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

    def clean_protected_fields(self, model, result, force_fetch_protected_fields: list = list()):
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
                a.append(self.clean_protected_fields(
                    model, r, force_fetch_protected_fields))
            return a
        else:
            for p in model.protected_fields:
                if result.get(p) is not None and p not in force_fetch_protected_fields:
                    del result[p]
        return result

    def relationships(self, criteria: dict, key_array: list, force_fetch_protected_fields: list = list(),
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
            # print(k)
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
                if self.relations[i]["type"] == Relations.manyToMany:
                    lookup = {"$lookup": {
                        "from": self.relations[i]["joinCollection"],
                        "localField": self.relations[i]["localKey"],
                        "foreignField": self.relations[i]["localJoinKey"],
                        "as": i
                    }}
                    aggregation.append(lookup)
                    aggregation.append(
                        {"$unwind": {"path": "$" + i, "preserveNullAndEmptyArrays": True}})
                    join_lookup = {"$lookup": {
                        "from": self.relations[i]["model"](self.db).collection_name,
                        "localField": i + "." + self.relations[i]["foreignJoinKey"],
                        "foreignField": self.relations[i]["foreignKey"],
                        "as": i
                    }}
                    aggregation.append(join_lookup)
                    aggregation.append(
                        {"$unwind": {"path": "$" + i, "preserveNullAndEmptyArrays": True}})
                    group = {"$group": {"_id": "$_id"}}
                    group["$group"][i] = {"$push": "$" + i}
                    for k in project["$project"]:
                        if k != "_id":
                            group["$group"][k] = {"$first": "$" + k}

                    aggregation.append(group)
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

                if self.relations[i]["type"] == Relations.manyToMany:
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
        ag = self.relationships(criteria, relations, force_fetch_protected_fields, pagination=pagination)
        criteria["deleted_at"] = {"$exists": False}

        cursor = self.db[self.collection_name].aggregate(ag)

        count = await self.db[self.collection_name].find(criteria).count()
        results = list()
        async for doc in cursor:
            results.append(self.dict_rep(doc))

        for result in results:
            results = self.clean_protected_fields(
                self, results, force_fetch_protected_fields)

            for key in self.relations:
                relation = self.relations[key]["model"](self.db)
                if result.get(key) is not None:
                    if type(result[key]) == list:
                        for k, val in enumerate(result[key]):
                            result[key][k] = self.clean_protected_fields(
                                relation, val, force_fetch_protected_fields)
                    else:
                        if result.get(key) is not None:
                            result[key] = self.clean_protected_fields(
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
        related_models = []
        for k in self.relations:
            if self.check_cascade_policy(self.relations[k]["cascade"], ["DELETE"]):
                related_models.append(k)
        if len(related_models):
            cache = await self.first({"_id": _id}, related_models)
            for model in related_models:
                collection_name = self.relations[model]["model"](
                    self.db).collection_name
                if cache.get(model):
                    if self.relations[model]["type"] in [Relations.hasMany, Relations.belongsToMany]:
                        for item in cache[model]:
                            soft_delete_support = self.relations[model].get("softDeletes")
                            if not soft_delete_support or soft_delete_support is None:
                                to_delete = await self.db[collection_name].remove({"_id": ObjectId(item["_id"])})
                            else:
                                cache_rel = self.preparse_fields(item)
                                cache_rel["deleted_at"] = now
                                del cache_rel["_id"]
                                to_delete = await self.db[collection_name].update({"_id": ObjectId(item["_id"])},
                                                                                  {"$set": cache_rel})
                    elif self.relations[model]["type"] in [Relations.manyToMany]:
                        to_remove = {}
                        to_remove[self.relations[model]["localJoinKey"]] = ObjectId(cache["_id"])
                        to_delete = await self.db[self.relations[model]["joinCollection"]].remove(to_remove)
                    else:
                        item = cache[model]
                        soft_delete_support = self.relations[model].get("softDeletes")
                        if not soft_delete_support or soft_delete_support is None:
                            to_delete = await self.db[collection_name].remove({"_id": ObjectId(item["_id"])})
                        else:
                            cache_rel = self.preparse_fields(item)
                            cache_rel["deleted_at"] = now
                            del cache_rel["_id"]
                            to_delete = await self.db[collection_name].update({"_id": ObjectId(item["_id"])},
                                                                              {"$set": cache_rel})

        if not self.softDeletes or force:
            r = await self.db[self.collection_name].remove({"_id": ObjectId(_id)})
        else:
            cache_rel = await self.first({"_id": _id}, [])
            cache_rel = self.preparse_fields(cache_rel)
            cache_rel["deleted_at"] = now
            del cache_rel["_id"]
            r = await self.db[self.collection_name].update({"_id": ObjectId(_id)}, {"$set": cache_rel})
        return r

    async def save(self, bus_object: dict) -> dict:
        """
        Saves a result.

        :param bus_object: Object to be saved.
        :return: Saved dictionary.
        """
        to_save = self.preparse_fields(bus_object)
        cascade_policy = []
        if bus_object.get("_id"):
            cascade_policy.append("UPDATE")
        else:
            cascade_policy.append("CREATE")
        if to_save.get("_id") is None:
            to_save["created_at"] = datetime.utcnow()
            to_save["updated_at"] = datetime.utcnow()
        else:
            to_save["updated_at"] = datetime.utcnow()
        saved = await self.db[self.collection_name].save(to_save)
        to_save["_id"] = saved
        for k in self.relations:
            if k in bus_object.keys() and self.check_cascade_policy(self.relations[k]["cascade"], cascade_policy):
                await self.save_collection(bus_object, k, self.relations[k], to_save)
        return self.dict_rep(to_save)

    def check_cascade_policy(self, cascade, cascade_policy):
        """
        Check the CascadePolicy.

        :param cascade: List of cascades.
        :param cascade_policy: Cascade policy to be checked.
        :return: Cascade policy evaluated.
        """
        should_cascade = False
        for i in cascade:
            if i in cascade_policy:
                should_cascade = True
        return should_cascade

    async def save_collection(self, model, properti, relation, result_model):
        """
        Saves the collection.

        :param model: Model instance.
        :param properti: Property to be used.
        :param relation: Relation to be used.
        :param result_model: Result model to be saved.
        :return: Saved collection.
        """
        upserted = []
        cursor = self.db[relation["model"](self.db).collection_name]
        bulk = cursor.initialize_ordered_bulk_op()
        if type(model[properti]) == list and relation["type"] != Relations.manyToMany:
            for item in model[properti]:
                item[relation["foreignKey"]] = result_model[relation["localKey"]]
                if item.get("_id"):
                    _id = item["_id"]
                    item["updated_at"] = datetime.utcnow()
                    upserted.append(ObjectId(_id))
                    del item["_id"]
                    bulk.find({"_id": ObjectId(_id)}
                              ).upsert().update({'$set': item})
                else:
                    item["updated_at"] = datetime.utcnow()
                    item["created_at"] = datetime.utcnow()
                    bulk.insert(item)
        if type(model[properti]) == list and relation["type"] == Relations.manyToMany:
            # deletion of existing records
            cursor_delete = self.db[relation["joinCollection"]]
            delete_bulk = cursor_delete.initialize_ordered_bulk_op()
            to_delete = {}
            _id = result_model[relation["localKey"]]
            to_delete[relation["localJoinKey"]] = ObjectId(_id)
            delete_bulk.find(to_delete).remove()
            try:
                result = await delete_bulk.execute()
            except Exception as bwe:
                print("Error on manyToMany deletion bulk..")
            # insertion of current records
            cursor_insert = self.db[relation["joinCollection"]]
            insert_bulk = cursor_insert.initialize_ordered_bulk_op()
            for item in model[properti]:
                to_insert = {}
                to_insert[relation["localJoinKey"]] = ObjectId(
                    result_model[relation["localKey"]])
                to_insert[relation["foreignJoinKey"]] = ObjectId(
                    item[relation["foreignKey"]])
                insert_bulk.find(to_insert).upsert().update({'$set': to_insert})
            try:
                result = await insert_bulk.execute()
            except Exception as bwe:
                print("Error on manyToMany insertion bulk..")

        if type(model[properti]) == dict and type(model[properti]) != list:
            item = model[properti]
            if item.get("_id"):
                _id = item["_id"]
                item["updated_at"] = datetime.utcnow()
                upserted.append(ObjectId(_id))
                del model[properti]["_id"]
                bulk.find({"_id": ObjectId(_id)}
                          ).upsert().update({'$set': item})
                # ops.append({"updateOne":{"filter":{"_id":ObjectId(_id)},"update":{"$set":item},"upsert":True}})
            else:
                item["created_at"] = datetime.utcnow()
                item["updated_at"] = datetime.utcnow()
                bulk.insert(item)
        try:
            result = await bulk.execute()
        except Exception as bwe:
            print("Error writing bulk..")

        ops = []
        for i in result["upserted"]:
            ops.append(i["_id"])
        fetched = self.db[relation["model"](self.db).collection_name].find({
            '_id': {"$in": upserted}})

        wololo = []
        async for i in fetched:
            wololo.append(i)
        if relation["type"] == Relations.belongsTo and len(fetched) > 0:
            result_model[properti] = wololo[0]
        else:
            result_model[properti] = wololo

        return result_model
