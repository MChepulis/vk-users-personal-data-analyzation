import elasticsearch_dsl

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, response_process, index


def count_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True, is_need_print=False,
                     is_need_plot=True, is_need_active=False, days_delta=20):
    title = "count by country"
    if is_need_active:
        title += " active"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    a = elasticsearch_dsl.A('terms', field="country.title.keyword", size=size)
    aggs_name = "countries_count"
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    response_process(response, aggs_name, title, is_need_other, is_need_print, is_need_plot)
