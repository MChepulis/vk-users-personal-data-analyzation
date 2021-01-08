import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def count_by_city_order_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True,
                                   is_need_print=False, is_need_plot=True, is_need_active=False, days_delta=20):
    country_aggs_name = "country_count"
    city_aggs_name = "city_count"
    title = "count by city"
    if is_need_active:
        title += " active"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    s = s.filter("bool", must=[elasticsearch_dsl.Q("exists", field="country.title.keyword")])
    s = s.filter("bool", must=[elasticsearch_dsl.Q("exists", field="city.title.keyword")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("match", country__title__keywordd="")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("match", city__title__keyword="")])
    a = elasticsearch_dsl.A('terms', field="country.title.keyword", size=size, collect_mode="breadth_first")
    a1 = elasticsearch_dsl.A('terms', field="city.title.keyword", size=size)
    s.aggs.bucket(country_aggs_name, a).bucket(city_aggs_name, a1)
    response = s.execute()

    data_dict = {}
    for country_hit in response.aggregations[country_aggs_name].buckets:

        x_axis = [hit.key for hit in country_hit[city_aggs_name].buckets]
        y_axis = [hit.doc_count for hit in country_hit[city_aggs_name].buckets]
        if is_need_other:
            x_axis.append("other")
            y_axis.append(country_hit[city_aggs_name].sum_other_doc_count)
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
            plt.show()
            fig.savefig(f"{save_path}/{figname}.png", dpi=300, format='png', bbox_inches='tight')
            plt.close(fig)
