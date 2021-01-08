import elasticsearch_client as es_client
import elasticsearch_dsl
import plotly.graph_objects as go
import plotly

from data_preparation import add_geoposition
from common_task_functions import get_elastic_object, index, get_scale, \
    get_random_color, get_count_of_users, save_path, get_active_users_filter

def count_by_city_order_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10,
                                   is_need_active=False, days_delta=20):
    country_aggs_name = "country_count"
    city_aggs_name = "city_count"
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

    data = []
    for country_hit in response.aggregations[country_aggs_name].buckets:
        country_dict = {'country': country_hit.key, 'cities': []}
        for city_hit in country_hit[city_aggs_name].buckets:
            country_dict['cities'].append({'city': city_hit.key, 'count': city_hit.doc_count})
        data.append(country_dict)
    add_geoposition(data)
    return data


def cities_on_map(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_active=False, days_delta=20):
    scale = get_scale(get_count_of_users(vk_elastic_db, is_need_active, days_delta))
    countries = count_by_city_order_by_country(vk_elastic_db, size, is_need_active, days_delta)
    fig = go.Figure()

    for country in countries:
        names = [city['city'] + "<br>Users=" + str(city['count']) for city in country['cities']]
        not_null_locations = []
        for city in country['cities']:
            if not city['location'] is None:
                not_null_locations.append(city['location'])
        lat = [location['latitude'] for location in not_null_locations]
        lon = [location['longitude'] for location in not_null_locations]
        count = [max(city['count'] / scale, 20) for city in country['cities']]
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
    plotly.offline.plot(fig, filename=f"{save_path}/cities_on_map.html")
    print(f"report: {save_path}/cities_on_map.html")
