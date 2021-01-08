import elasticsearch_client as es_client
from libs.task_active_users_pie import active_users_pie
from libs.task_count_by_city import count_by_city
from libs.task_count_by_city_order_by_country import count_by_city_order_by_country
from libs.task_count_by_country import count_by_country
from libs.task_count_by_university import count_by_university
from libs.task_count_by_university_order_by_country import count_by_university_order_by_country
from libs.task_has_city import has_city
from libs.task_has_county import has_country
from libs.task_has_mobile import has_mobile
from libs.task_has_photo import has_photo
from libs.task_has_university import has_university
from libs.task_last_name_count import last_name_count
from libs.task_man_name_count import man_name_count
from libs.task_other_social_network import other_social_network
from libs.task_profile_access_count import profile_access_count
from libs.task_sex_distribution import sex_distribution
from libs.task_woman_name_count import woman_name_count


def start():
    client = es_client.VkDataDatabaseClient()
    days_delta = 20
    sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True)
    sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    man_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    man_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    woman_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    woman_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    last_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    last_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True)
    sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    has_country(client, is_need_print=True, is_need_plot=True)
    has_country(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    count_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    count_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    has_city(client, is_need_print=True, is_need_plot=True)
    has_city(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    count_by_city(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    count_by_city(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    count_by_city_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    count_by_city_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    has_photo(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    has_photo(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    has_mobile(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    has_mobile(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    has_university(client, is_need_print=True, is_need_plot=True)
    has_university(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    count_by_university(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    count_by_university(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    count_by_university_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    count_by_university_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    profile_access_count(client, is_need_print=True, is_need_plot=True)
    profile_access_count(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    other_social_network(client, is_need_print=True, is_need_plot=True)
    other_social_network(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    active_users_pie(client, days_delta=20, is_need_print=True, is_need_plot=True)


if __name__ == "__main__":
    start()
