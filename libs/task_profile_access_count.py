import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def profile_access_count(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True,
                         is_need_active=False, days_delta=20):
    aggs_name = "profile_access_count"
    title = "profile access count"
    if is_need_active:
        title += " active"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    a = elasticsearch_dsl.A('terms', field="is_closed", size=10)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    lable_dict = {
        "0": "opened",
        "1": "closed"
    }
    x_axis = [lable_dict[str(hit.key)] for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]

    if is_need_print:
        print(title)
        for i in range(len(x_axis)):
            print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

    if is_need_plot:
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.barh(x_axis, y_axis)
        plt.show()

        sizes = [elem / sum(y_axis) for elem in y_axis]
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.pie(sizes, labels=x_axis, autopct='%1.1f%%', startangle=90)
        plt.show()
        fig.savefig(f"{save_path}/{title.replace(' ', '_')}.png", dpi=300, format='png', bbox_inches='tight')
        plt.close(fig)
