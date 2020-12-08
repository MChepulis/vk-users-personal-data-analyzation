import json
import logging
from elasticsearch import Elasticsearch
import json
import VkElasticSearchDatabase
import elasticsearch_dsl

def my_print(req):
    res_str = ""
    i = 0
    total = req["hits"]["total"]["value"]
    print(f"total = {total}")
    for smt in req['hits']['hits']:
        i += 1
        profile = smt['_source']
        id_tmp = profile.get('id')
        country = profile.get('country')
        if country is not None:
            country_title = country.get('title')
            country_id = country.get('id')
        else:
            country_title = None
            country_id = None
        res_str += f"{i}) {id_tmp}\t{profile.get('first_name')}\t{profile.get('last_name')}\t"
        res_str += f"{country_id}\t{country_title}\n"
    return res_str


def country_hist1(vk_elastic_db: VkElasticSearchDatabase.VkElasticSearchDatabase):
    client = vk_elastic_db.get_elastic_object()
    s = elasticsearch_dsl.Search(using=client, index=vk_elastic_db.index)
    # q = elasticsearch_dsl.Q('terms', ** {'country.id': '1'})
    a = elasticsearch_dsl.A('terms', field="country.id")
    aggs_name = "countries_count"
    s.aggs.bucket(aggs_name, a)
    country_aggs = s.execute()

    ms = elasticsearch_dsl.MultiSearch(using=client, index=vk_elastic_db.index)
    for hit in country_aggs.aggregations[aggs_name].buckets:
        print(hit.key, hit.doc_count)
        ms = ms.add(elasticsearch_dsl.Search().source("country").filter("term", **{'country.id': f'{hit.key}'}))

    responses = ms.execute()
    countries = {}
    for response in responses:
        for hit in response:
            countries[f"{hit.country.id}"] = hit.country.title
            break

    for hit in country_aggs.aggregations[aggs_name].buckets:
        print(hit.key, hit.doc_count, countries[f"{hit.key}"])



    '''
    for hit in s[10:20]:
        print(hit.last_name)

    for hit in s.scan():
        print(hit.last_name)

    '''
    '''
    s.query("match", first_name="Pavel")
    response = s.execute()
    print(response)
    for hit in s:
        print(hit.first_name)
    '''


def country_hist(vk_elastic_db: VkElasticSearchDatabase.VkElasticSearchDatabase):
    aggs = {
        "aggregations": {
            "country_id": {
                "terms": {
                    "field": "country.id",
                }
            }
        }
    }
    country_id_aggs = vk_elastic_db.search(aggs, q_size=0)
    country_pairs = country_id_aggs['aggregations']['country_id']['buckets']
    country_titles = []
    country_ids = [country_pair["key"] for country_pair in country_pairs]
    country_values = [country_pair["doc_count"] for country_pair in country_pairs]
    for country_id in country_ids:

        source = "country.title"
        query = {
            "query": {
                "bool": {
                    "filter": {
                        "term": {
                            "country.id": country_id
                        }
                    }
                }
            }
        }
        title = vk_elastic_db.search(query, q_size=1, source=source)["hits"]["hits"][0]["_source"]["country"]["title"]
        country_titles.append(title)

    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(1, 1)
    ax.bar(country_titles, country_values)
    plt.show()






    pass



