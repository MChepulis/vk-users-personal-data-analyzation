import pymongo
import db_keys


def add_object_id(data):
    copy = data.copy()
    copy['_id'] = data['id']
    return copy


def extract_id(data):
    return data["id"]


class DatabaseClient:
    def __init__(self, addr, port, username, password, authSource):
        self.client = pymongo.MongoClient(addr,
                                          port=port,
                                          username=username,
                                          password=password,
                                          authSource=authSource)

    def _replace_or_insert_one_by_id(self, collection, data):
        return collection.replace_one({"_id": data["_id"]}, data, upsert=True)

    def replace_or_insert_one_by_id(self, collection, data):
        return self._replace_or_insert_one_by_id(collection, data)

    def replace_or_insert_many_by_id(self, collection, data):
        def f(d):
            return self._replace_or_insert_one_by_id(collection, d)
        return list(map(f, data))

    def delete_and_insert_one(self, collection, delete_query, data):
        delete_result = collection.delete_one(delete_query)
        insert_result = collection.insert_one(data)
        return delete_result, insert_result

    def delete_and_insert_many(self, collection, delete_query, data):
        delete_result = collection.delete_many(delete_query)
        insert_result = collection.insert_many(data)
        return delete_result, insert_result


class VkDatabaseClient(DatabaseClient):
    def __init__(self):
        super(VkDatabaseClient, self).__init__(
            addr=db_keys.db_addr,
            port=db_keys.db_port,
            username=db_keys.db_username,
            password=db_keys.db_password,
            authSource=db_keys.db_authSource
        )
        self.db = self.client[db_keys.db_name]

    def put_one_user_info(self, data):
        # using replace
        # return self.replace_or_insert_one_by_id(add_object_id(data))
        # using delete and insert
        return self.delete_and_insert_one(self.db.available, {"_id": data["id"]}, add_object_id(data))

    def put_many_user_info(self, data):
        # using replace
        # return self.replace_or_insert_many_by_id(list(map(add_object_id, data)))
        # using delete and insert
        return self.delete_and_insert_many(self.db.available, {"_id": {"$in": list(map(extract_id, data))}}, list(map(add_object_id, data)))

    def count_of_available_users(self):
        return self.db.available.count_documents({})
