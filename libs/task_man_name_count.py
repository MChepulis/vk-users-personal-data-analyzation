import elasticsearch_client as es_client
from common_task_functions import response_process, name_count


def man_name_count(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=False, is_need_print=False,
                   is_need_plot=True, is_need_active=False, days_delta=20):
    aggs_name = "man_first_name_count"
    title = "man_first_name_count"
    if is_need_active:
        title += " active"
    response = name_count(vk_elastic_db, aggs_name, sex=2, size=size, is_need_active=is_need_active,
                          days_delta=days_delta)
    response_process(response, aggs_name, title, is_need_other, is_need_print, is_need_plot)
