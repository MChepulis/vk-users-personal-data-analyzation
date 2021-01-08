import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def sex_distribution(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=False, is_need_print=False,
                     is_need_plot=True, is_need_active=False, days_delta=20):
    aggs_name = "sex_distribution"
    title = "sex distribution"
    if is_need_active:
        title += " active"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    a = elasticsearch_dsl.A('terms', field="sex", missing="-1", size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    label_dict = {
        "0": "unknown",
        "1": "woman",
        "2": "man",
        "-1": "missing"
    }
    x_axis = [label_dict[str(hit.key)] for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]
    if is_need_other:
        x_axis.append("other")
        y_axis.append(response.aggregations[aggs_name].sum_other_doc_count)

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
