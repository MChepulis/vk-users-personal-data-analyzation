import elasticsearch_client as es_client
from task_active_users_pie import active_users_pie
from task_count_by_city import count_by_city
from task_count_by_city_order_by_country import count_by_city_order_by_country
from task_count_by_country import count_by_country
from task_count_by_university import count_by_university
from task_count_by_university_order_by_country import count_by_university_order_by_country
from task_has_city import has_city
from task_has_county import has_country
from task_has_mobile import has_mobile
from task_has_photo import has_photo
from task_has_university import has_university
from task_last_name_count import last_name_count
from task_man_name_count import man_name_count
from task_other_social_network import other_social_network
from task_profile_access_count import profile_access_count
from task_sex_distribution import sex_distribution
from task_woman_name_count import woman_name_count
from task_cities_on_map import cities_on_map
from task_countries_on_map import countries_on_map

def start():
    client = es_client.VkDataDatabaseClient()
    days_delta = 20
    # sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True)
    # sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # man_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    # man_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # woman_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    # woman_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # last_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    # last_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True)
    # sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # has_country(client, is_need_print=True, is_need_plot=True)
    # has_country(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # count_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # count_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # has_city(client, is_need_print=True, is_need_plot=True)
    # has_city(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # count_by_city(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # count_by_city(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # count_by_city_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # count_by_city_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # has_photo(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    # has_photo(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # has_mobile(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    # has_mobile(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # has_university(client, is_need_print=True, is_need_plot=True)
    # has_university(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # count_by_university(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    # count_by_university(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # count_by_university_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # count_by_university_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # profile_access_count(client, is_need_print=True, is_need_plot=True)
    # profile_access_count(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # other_social_network(client, is_need_print=True, is_need_plot=True)
    # other_social_network(client, is_need_print=True, is_need_plot=True, is_need_active=True, days_delta=days_delta)
    # active_users_pie(client, days_delta=20, is_need_print=True, is_need_plot=True)
    # cities_on_map(client, size=10, is_need_active=False, days_delta=days_delta)
    # countries_on_map(client, size=10, is_need_active=False, days_delta=days_delta)


tasks_dict = {
    1: {
        "function": man_name_count,
        "description": "Топ N (default: 20) мужских имён по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 20
    },
    2: {
        "function": woman_name_count,
        "description": "Топ N (default: 20) женских имён по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 20
    },
    3: {
        "function": last_name_count,
        "description": "Топ N (default: 20) мужских и женских фамилий по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 20
    },
    4: {
        "function": sex_distribution,
        "description": "Доли полов пользователей",
        "has_size": False
    },
    5: {
        "function": has_country,
        "description": "Доли пользователей, которые указали/не указали страну проживания",
        "has_size": False
    },
    6: {
        "function": count_by_country,
        "description": "топ N (default: 10) стран по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 10
    },
    7: {
        "function": has_city,
        "description": "Доли пользователей, которые указали/не указали город проживания",
        "has_size": False
    },
    8: {
        "function": count_by_city,
        "description": "топ N (default: 10) городов по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 10
    },
    9: {
        "function": count_by_city_order_by_country,
        "description": "топ N (default: 10) стран и топ N городов в них по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 10
    },
    10: {
        "function": has_photo,
        "description": "Доли пользователей, которые установили / не установили аватар",
        "has_size": False
    },
    11: {
        "function": has_mobile,
        "description": "Доли пользователей, которые указали телефон",
        "has_size": False
    },
    12: {
        "function": has_university,
        "description": "Доли пользователей, которые указали место учёбы",
        "has_size": False
    },
    13: {
        "function": count_by_university,
        "description": "топ N (default: 10) университетов по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 10
    },
    14: {
        "function": count_by_university_order_by_country,
        "description": "топ N (default: 10) стран в которых проживают студенты и выпускники топ N университетов по количеству зарегистрированных пользователей",
        "has_size": True,
        "default": 10
    },
    15: {
        "function": profile_access_count,
        "description": "Доли открытых и закрытых страниц пользователей",
        "has_size": False
    },
    16: {
        "function": other_social_network,
        "description": "Доли пользователей, которые заполнили графу \"сайт\"",
        "has_size": False
    },
    17: {
        "function": active_users_pie,
        "description": "доли активных и неактивных пользователей",
        "has_size": False
    },
    18: {
        "function": cities_on_map,
        "description": "Изображает на карте топ N (default: 10) городов из топ N стран по количеству пользователей",
        "has_size": True,
        "default": 10
    },
    19: {
        "function": countries_on_map,
        "description": "Изображает на карте топ N (default: 10) стран по количеству пользователей",
        "has_size": True,
        "default": 10
    }
}

import sys

if __name__ == '__main__':
    # start()
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "description":
            print("tasks")
            for id, val in tasks_dict.items():
                print(str(id) + "\t" + val['description'])
        elif command == "task":
            task_id = int(sys.argv[2])
            function = tasks_dict[task_id]['function']
            has_size = tasks_dict[task_id]['has_size']
            size = None
            if has_size:
                size = int(sys.argv[3])
                is_active = bool(int(sys.argv[4]))
                last_days = int(sys.argv[5])
            else:
                is_active = bool(int(sys.argv[3]))
                last_days = int(sys.argv[4])
            client = es_client.VkDataDatabaseClient()
            if (size is None) or (not has_size):
                function(client, is_need_active=is_active, days_delta=last_days)
            else:
                function(client, size, is_need_active=is_active, days_delta=last_days)
        elif command == "default_size":
            task_id = int(sys.argv[2])
            has_size = tasks_dict[task_id]['has_size']
            if has_size:
                print(tasks_dict[task_id]['default'])
            else:
                print("")
