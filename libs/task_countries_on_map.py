import elasticsearch_client as es_client
import elasticsearch_dsl
import plotly.graph_objects as go
import plotly

from data_preparation import add_geoposition
from common_task_functions import get_elastic_object, index, get_scale, \
    get_random_color, get_count_of_users, save_path, get_active_users_filter


def count_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_active=False, days_delta=20):
    country_aggs_name = "country_count"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    s = s.filter("bool", must=[elasticsearch_dsl.Q("exists", field="country.title.keyword")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("match", country__title__keywordd="")])
    a = elasticsearch_dsl.A('terms', field="country.title.keyword", size=size)
    s.aggs.bucket(country_aggs_name, a)
    response = s.execute()

    data = []
    for country_hit in response.aggregations[country_aggs_name].buckets:
        country_dict = {'country': country_hit.key, 'count': country_hit.doc_count}
        data.append(country_dict)
    add_geoposition(data)
    return data


def countries_on_map(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_active=False, days_delta=20):
    scale = get_scale(get_count_of_users(vk_elastic_db, is_need_active, days_delta))
    countries = count_by_country(vk_elastic_db, size, is_need_active, days_delta)
    fig = go.Figure()

    for country in countries:
        names = [country['country'] + "<br>Users=" + str(country['count'])]
        if not country['location'] is None:
            lat = [country['location']['latitude']]
            lon = [country['location']['longitude']]
            count = [max(country['count'] / scale, 20)]
            color = get_random_color()
            fig.add_trace(go.Scattergeo(
                lon=lon,
                lat=lat,
                text=names,
                marker=dict(
                    size=count,
                    color=color,
                    sizemode='area'
                ),
                name=country['country']
            ))
    fig.update_layout(geo=dict(
        showland=True,
        showcountries=True
    ))
    plotly.offline.plot(fig, filename=f"{save_path}/countries_on_map.html")
    print(f"report: {save_path}/countries_on_map.html")
