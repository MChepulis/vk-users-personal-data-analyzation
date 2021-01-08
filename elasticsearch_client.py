import logging
from elasticsearch import Elasticsearch, ConnectionTimeout
import db_keys
import json

f = open('log', "w")

class DatabaseClient:
    def __init__(self, hosts):
        self._es = Elasticsearch(hosts)
        self._bulk = []
        if not self._es.ping():
            print ("Can't connect to elastcsearch")
            raise RuntimeError()
        # print ("Connected to elastcsearch")

    def _create_indices(self, indices: list) -> None:
        for index in indices:
            print(self._es.indices.create(index=index, ignore=400))

    def _insert_one(self, index, data, id=None):
        print(self._es.index(index=index, id=id, body=data))

    def _add_insert_one(self, index, data, id=None):
        command = {'index': {'_index': index}}
        if not id is None:
            command['index']['_id'] = id

        self._bulk.append(command)
        self._bulk.append(data)
        return self
    
    def _delete_one(self, index, id):
        self._bulk.append(
            {'delete': {'_index': index, '_id': id}}
        )
        return self

    def _execute_bulk(self):
        output = self._es.bulk(body=self._bulk)
        self._bulk = []
        return output
    
    def _min_max_id(self, index):
        output = self._es.search(
            body={
                "aggs": {
                    "max_id": {"max": { "field": "id"}},
                    "min_id": {"min": { "field": "id"}}
                }
            }, 
            index=index
        )
        return int(output['aggregations']['min_id']['value']), int(output['aggregations']['max_id']['value'])


def parse_bulk_output(output):
    if output['errors']:
        successful = 0
        failed = 0
        for item in output['items']:
            if item['status'] == 200:
                successful += 1
            else:
                failed += 1
        return successful, failed
    else:
        return len(output['items']), 0


def add_object_id(data):
    copy = data.copy()
    copy['_id'] = data['id']
    return copy


def extract_id(data):
    return data["id"]



class VkDataDatabaseClient(DatabaseClient):

    def __init__(self):
        super().__init__([{'host': db_keys.db_addr, 'port': db_keys.db_port}])

    def _add_many(self, index, user_info_list):
        output_list = []
        for i in range(len(user_info_list) // 100 + 1):
            while True:
                try:
                    if len(user_info_list[i * 100:(i + 1)*100]) != 0:
                        for user_info in user_info_list[i * 100:(i + 1)*100]:
                            self._add_insert_one(index, user_info, extract_id(user_info))
                        output_list.append(self._execute_bulk())
                    break
                except ConnectionTimeout:
                    self._bulk = []
                    print("connection timeout to database")
                    continue
        return output_list

    def add_available_user_info_list(self, user_info_list):
        index = 'available_users'
        output = self._add_many(index, user_info_list)
        ovr_succ, ovr_fail = 0, 0
        for succ, fail in list(map(parse_bulk_output, output)):
            ovr_succ += succ
            ovr_fail += fail
        return ovr_succ, ovr_fail

    def add_unavailable_user_info_list(self, user_info_list):
        index = 'unavailable_users'
        output = self._add_many(index, user_info_list)
        ovr_succ, ovr_fail = 0, 0
        for succ, fail in list(map(parse_bulk_output, output)):
            ovr_succ += succ
            ovr_fail += fail
        return ovr_succ, ovr_fail

    def count_of_available_users(self):
        return self._es.count(index='available_users')['count']

    def count_of_unavailable_users(self):
        return self._es.count(index='unavailable_users')['count']

    def min_max_available_users_id(self):
        return self._min_max_id('available_users')

    def min_max_unavailable_users_id(self):
        return self._min_max_id('unavailable_users')


if __name__ == "__main__":
    db_client = VkDataDatabaseClient()

    print(db_client.min_max_available_users_id())
    print(db_client.min_max_unavailable_users_id())
