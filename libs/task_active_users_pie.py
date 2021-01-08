import elasticsearch_dsl
import matplotlib.pyplot as plt

import elasticsearch_client as es_client
from common_task_functions import get_elastic_object, get_active_users_filter, index, save_path


def active_users_pie(vk_elastic_db: es_client.VkDataDatabaseClient, days_delta=20, is_need_print=False,
                     is_need_plot=True):
    title = "active users"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    s = get_active_users_filter(es, index, s, days_delta=days_delta)
    s.execute()

    total_search = elasticsearch_dsl.Search(using=es, index=index)
    total_num = total_search.count()
    active_num = s.count()

    x_axis = ["active", "inactive"]
    y_axis = [active_num, total_num - active_num]
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
