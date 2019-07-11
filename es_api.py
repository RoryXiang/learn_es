from elasticsearch import Elasticsearch


class EsApi(object):

    def __init__(self):
        self.es = Elasticsearch(hosts="10.13.4.99")
        self.index_name = "movies"
        self.index_type = "movies_doc"

    def create_index(self, index_name="movies", index_type="movies_doc"):
        mappings = {
            "mappings": {  # type_doc_test为doc_type
                "properties": {
                    "id": {
                        "type": "integer",
                        "index": True
                    },
                    "title": {
                        "type": "text",  # keyword不会进行分词,text会分词
                        "index": True  # False 不建索引
                    },
                    "genre": {
                        "type": "text",
                        "index": True
                    },
                    "year": {
                        "type": "integer",
                        "index": True
                    },
                    # tags可以存json格式，访问tags.content
                    # "tags": {
                    #     "type": "object",
                    #     "properties": {
                    #         "content": {"type": "keyword", "index": True},
                    #         "dominant_color_name": {"type": "keyword", "index": True},
                    #         "skill": {"type": "keyword", "index": True},
                    #     }
                    # },
                    # "hasTag": {
                    #     "type": "long",
                    #     "index": True
                    # },
                    # "status": {
                    #     "type": "long",
                    #     "index": True
                    # },
                    # "createTime": {
                    #     "type": "date",
                    #     "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                    # },
                }}
        }

        if self.es.indices.exists(index=index_name) is not True:
            res = self.es.indices.create(
                index=index_name, body=mappings)
            print(res)

    def delete_index(self, index_name):
        if self.es.indices.exists(index=index_name) is not True:
            res = self.es.indices.delete(index=index_name)
            print(res)

    def insert(self, json_data):
        res = self.es.index(index=self.index_name,
                            doc_type=self.index_type, body=json_data)
        print("???? ", res)


if __name__ == '__main__':
    es = EsApi()
    es.create_index()
