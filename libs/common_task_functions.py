import elasticsearch_dsl
import datetime
import time
import matplotlib.pyplot as plt

import elasticsearch_client as es_client


index = "available_users"
save_path = "../results/plots"


def get_elastic_object(vk_elastic_db: es_client.VkDataDatabaseClient):
    return vk_elastic_db._es


def get_active_users_filter(es, es_index, s, days_delta=20):
    agg_name = "last_time"
    day_s = elasticsearch_dsl.Search(using=es, index=es_index)
    day_a = elasticsearch_dsl.A('max', field="last_seen.time")
    day_s.aggs.bucket(agg_name, day_a)
    resp = day_s.execute()

    latest_day_timestamp = resp.aggregations[agg_name].value
    value = datetime.datetime.fromtimestamp(latest_day_timestamp)
    barier_data = value - datetime.timedelta(days=days_delta)
    barier_timestamp = time.mktime(barier_data.timetuple())
    ret_s = s.filter("range", last_seen__time={'gt': barier_timestamp})
    return ret_s


def response_process(response, aggs_name, title, is_need_other, is_need_print, is_need_plot):

    x_axis = [hit.key for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]
    if is_need_other:
        x_axis.append("other")
        y_axis.append(response.aggregations[aggs_name].sum_other_doc_count)

    if is_need_print:
        print(title)
        for i in range(len(x_axis)):
            print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

    if is_need_plot:
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.barh(x_axis, y_axis)
        plt.show()
        fig.savefig(f"{save_path}/{title.replace(' ', '_')}.png", dpi=300, format='png', bbox_inches='tight')
        plt.close(fig)


def name_count(vk_elastic_db: es_client.VkDataDatabaseClient, aggs_name, sex=None, size=10, is_need_active=False,
               days_delta=20):
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    if sex is not None:
        s = s.filter('term', sex=sex)
    a = elasticsearch_dsl.A('terms', field="first_name.keyword", size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    return response


def get_active_title(title):
    return "active/" + title
