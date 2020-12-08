from elasticsearch import Elasticsearch


class VkElasticSearchDatabase:
    def __init__(self, host='localhost', port=9200, index="vk_users"):
        self.host = host
        self.port = port
        self.index = index
        self.elastic_object = self.get_connection()

    def get_connection(self):
        es = Elasticsearch([{'host': self.host, 'port': self.port}])
        if es.ping():
            print('Yay Connect')
        else:
            print('Awww it could not connect!')
        return es

    # FIXME можно ли складывать не по одной записи а сразу несколько?
    # FIXME как проверить, чколько записей удалось положить?
    # при передачи id запись автоматически будет обновляться (перезаписываться)
    def store_records(self, records):
        for record in records:
            self.elastic_object.index(index=self.index, body=record, id=record["id"])

    def get_elastic_object(self):
        return self.elastic_object

    def search(self, query, q_size=None, source=None):
        return self.elastic_object.search(index=self.index, body=query, size=q_size, _source=source)





