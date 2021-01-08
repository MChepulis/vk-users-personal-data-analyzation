import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def other_social_network(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True,
                         is_need_active=False, days_delta=20):
    title = "has other site"
    if is_need_active:
        title += " active"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        s = get_active_users_filter(es, index, s, days_delta=days_delta)
    q = elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="twitter")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="site")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="skype")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="livejournal")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="instagram")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="facebook")])

    s = s.query(q)
    s.execute()
    '''
    # запрос: найти всех тех, у кого не указано ничего в сторонних сайтах
    s = elasticsearch_dsl.Search(using=es, index=index)
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("exists", field="twitter")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("exists", field="site")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("exists", field="skype")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("exists", field="livejournal")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("exists", field="instagram")])
    s = s.filter("bool", must_not=[elasticsearch_dsl.Q("exists", field="facebook")])
    response = s.execute()
    '''
    total_search = elasticsearch_dsl.Search(using=es, index=index)
    if is_need_active:
        total_search = get_active_users_filter(es, index, total_search, days_delta=days_delta)
    total_num = total_search.count()
    other_sn_num = s.count()

    x_axis = ["has", "has not"]
    y_axis = [other_sn_num, total_num-other_sn_num]
    if is_need_print:
        print(title)
        for i in range(len(x_axis)):
            print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

    if is_need_plot:
        sizes = [elem / sum(y_axis) for elem in y_axis]
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.pie(sizes, labels=x_axis, autopct='%1.1f%%', startangle=90)
        # plt.show()
        fig.savefig(f"{save_path}/{title.replace(' ', '_')}.png", dpi=300, format='png', bbox_inches='tight')
        plt.close(fig)
