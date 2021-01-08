import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def last_name_count(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=False, is_need_print=False,
                    is_need_plot=True, is_need_active=False, days_delta=20):
    aggs_name = "last_name_count"
    sex_aggs_name = "sex_aggs"
    title = "last name count"
    if is_need_active:
        title += " active"
    sex_size = 2
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    sex_a = elasticsearch_dsl.A('terms', field="sex", missing="-1", size=sex_size)
    a = elasticsearch_dsl.A('terms', field="last_name.keyword", size=size)
    s.aggs.bucket(sex_aggs_name, sex_a).bucket(aggs_name, a)
    response = s.execute()

    data_dict = {}
    sex_dict = {
        "0": "unknown",
        "1": "woman",
        "2": "man",
        "-1": "missing"
    }
    for sex_hit in response.aggregations[sex_aggs_name].buckets:
        x_axis = [hit.key for hit in sex_hit[aggs_name].buckets]
        y_axis = [hit.doc_count for hit in sex_hit[aggs_name].buckets]
        if is_need_other:
            x_axis.append("other")
            y_axis.append(sex_hit[aggs_name].sum_other_doc_count)
        data_dict[sex_dict[str(sex_hit.key)]] = {}
        data_dict[sex_dict[str(sex_hit.key)]]["x_axis"] = x_axis
        data_dict[sex_dict[str(sex_hit.key)]]["y_axis"] = y_axis

    for sex in data_dict:
        x_axis = data_dict[sex]["x_axis"]
        y_axis = data_dict[sex]["y_axis"]
        cur_title = f"{title}\n{sex}"
        figname = f"{title.replace(' ', '_')}_{sex}"

        if is_need_print:
            print(cur_title)
            for i in range(len(x_axis)):
                print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

        if is_need_plot:
            fig, ax = plt.subplots(1, 1)
            ax.set_title(cur_title)
            ax.barh(x_axis, y_axis)
            plt.show()
            fig.savefig(f"{save_path}/{figname}.png", dpi=300, format='png', bbox_inches='tight')
            plt.close(fig)
