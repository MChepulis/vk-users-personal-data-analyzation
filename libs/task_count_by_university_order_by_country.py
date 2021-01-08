import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def count_by_university_order_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True,
                                         is_need_print=False, is_need_plot=True, is_need_active=False, days_delta=20):
    country_aggs_name = "country_count"
    university_aggs_name = "university_count"
    title = "count university by country"
    if is_need_active:
        title += " active"
    missing_str = ""
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    a = elasticsearch_dsl.A('terms', field="country.title.keyword", size=size, collect_mode="breadth_first")
    a1 = elasticsearch_dsl.A('terms', field="university_name.keyword",  missing=missing_str, size=size)
    s.aggs.bucket(country_aggs_name, a1).bucket(university_aggs_name, a)
    response = s.execute()

    data_dict = {}
    for country_hit in response.aggregations[country_aggs_name].buckets:

        x_axis = []
        y_axis = []
        for hit in country_hit[university_aggs_name].buckets:
            if hit.key == missing_str:
                continue
            x_axis.append(hit.key)
            y_axis.append(hit.doc_count)
        if is_need_other:
            x_axis.append("other")
            y_axis.append(country_hit[university_aggs_name].sum_other_doc_count)
        data_dict[country_hit.key] = {}
        data_dict[country_hit.key]["x_axis"] = x_axis
        data_dict[country_hit.key]["y_axis"] = y_axis

    for country in data_dict:
        x_axis = data_dict[country]["x_axis"]
        y_axis = data_dict[country]["y_axis"]
        cur_title = f"{title}\n{country}"
        figname = f"{title.replace(' ', '_')}_{country}"
        if is_need_print:
            print(cur_title)
            for i in range(len(x_axis)):
                print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

        if is_need_plot:
            fig, ax = plt.subplots(1, 1)
            ax.set_title(cur_title)
            ax.barh(x_axis, y_axis)
            # plt.show()
            fig.savefig(f"{save_path}/{figname}.png", dpi=300, format='png', bbox_inches='tight')
            plt.close(fig)
