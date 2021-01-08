import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def has_city(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True,
             is_need_active=False, days_delta=20):
    aggs_name = "has_city"
    title = "has city"
    if is_need_active:
        title += " active"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    s = s.filter()
    size = 10000
    missing_str = "missing"
    a = elasticsearch_dsl.A('terms', field="city.title.keyword", missing=missing_str, size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()

    data = {
        "has city": 0,
        "missing city": 0
    }
    for hit in response.aggregations[aggs_name].buckets:
        if hit.key == missing_str:
            data["missing city"] += hit.doc_count
        else:
            data["has city"] += hit.doc_count
    x_axis = [key for key in data]
    y_axis = [data[key] for key in data]

    if is_need_print:
        print(title)
        for i in range(len(x_axis)):
            print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

    if is_need_plot:
        sizes = [elem / sum(y_axis) for elem in y_axis]
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.pie(sizes, labels=x_axis, autopct='%1.1f%%', startangle=90)
        plt.show()
        fig.savefig(f"{save_path}/{title.replace(' ', '_')}.png", dpi=300, format='png', bbox_inches='tight')
        plt.close(fig)
