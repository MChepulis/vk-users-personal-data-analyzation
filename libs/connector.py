import vk_api
import elasticsearch_client as es_client
from keys import app_id, client_secret, token, api_version
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from data_preparation import data_preparation
import sys

default_photos = ['https://vk.com/images/camera_50.png', 'https://vk.com/images/camera_100.png',
                  'https://vk.com/images/camera_200.png', 'https://vk.com/images/camera_400.png',
                  'https://vk.com/images/deactivated_50.png', 'https://vk.com/images/deactivated_100.png',
                  'https://vk.com/images/deactivated_200.png', 'https://vk.com/images/deactivated_400.png']


class VKConnector:

    def __init__(self):
        self.vk = None
        self.default_fields = ["about", "activities", "bdate", "books", "career", "city", "connections", "country",
                               "education", "has_mobile", "has_photo", "followers_count", "games", "home_town",
                               "interests", "last_seen", "maiden_name", "military", "movies", "music", "occupation",
                               "personal", "photo_50", "photo_100", "photo_200_orig", "photo_200", "photo_400_orig",
                               "photo_400", "photo_max", "photo_max_orig", "quotes", "relatives", "relation", "schools",
                               "sex", "site", "status", "trending", "tv", "universities", "verified"]
        pass

    def connect(self):
        try:
            vk_session = vk_api.VkApi(token=token, app_id=int(app_id), client_secret=client_secret,
                                      api_version=api_version)
            self.vk = vk_session.get_api()
            return True
        except vk_api.AuthError as error_msg:
            print(error_msg)
            self.vk = None
        return False

    def get_profiles(self, user_ids, fields=None):
        # не более 1000 пользователей за 1 запрос
        if fields is None:
            fields = self.default_fields
        search_data = self.vk.users.get(user_ids=user_ids, fields=fields)
        return search_data


class DrawProfiles:

    def __init__(self, data):
        self.data = data

    def __str__(self):
        res = "["
        for node in self.data:
            res += f"{node['id']}, "
        res += "]"
        return res


''' id=3 удалён
    можно использовать дополнительные критерии того, что пользователь удалён
    можно проверять когда пользователь был онлайн, чтобы найти +- активных
'''


def is_valid_profile(profile):
    if profile.get('deactivated') == 'deleted':
        return False
    return True


def separate_deleted_profile(data):
    result = []
    del_res = []
    for node in data:
        if not is_valid_profile(node):
            del_res.append(node)
        else:
            result.append(node)
    return result, del_res


def download_user_infos(ids):
    connector = VKConnector()
    if not connector.connect():
        return
    data = connector.get_profiles(ids)
    # убираем удалённых пользователей, можно их всёравно положить в базу, лучше отдельно, чтобы как-то обработать
    clear_data, deleted_data = separate_deleted_profile(data)
    clear_data = data_preparation(clear_data)

    client = es_client.VkDataDatabaseClient()
    if len(clear_data) != 0:
        succ, fail = client.add_available_user_info_list(clear_data)
        if succ != len(clear_data):
            raise RuntimeError("Expected to insert " + str(len(clear_data)) + " objects. Inserted " + str(succ))

    if len(deleted_data) != 0:
        succ, fail = client.add_unavailable_user_info_list(deleted_data)
        if succ != len(deleted_data):
            raise RuntimeError("Expected to insert " + str(len(deleted_data)) + " objects. Inserted " + str(succ))
    #print(f"[{ids[0]}, {ids[-1]}]")


def get_all_profiles(start_id, end_id, step=1000, max_workers=10):
    ids_arr = []
    while start_id < (end_id + 1) - step:
        ids_arr.append([i for i in range(start_id, start_id + step)])
        start_id += step
        pass

    ids_arr.append([i for i in range(start_id, end_id + 1)])

    with PoolExecutor(max_workers=max_workers) as executor:
        for _ in executor.map(download_user_infos, ids_arr):
            pass
    client = es_client.VkDataDatabaseClient()
    print("count of available users in database - ", client.count_of_available_users())
    print("count of unavailable users in database - ", client.count_of_unavailable_users())


max_id = 633228552      # на 08.01.2021


def main(download_start_id=1, download_end_id=max_id, step=50000, max_workers=10):
    for start_id in range(download_start_id, download_end_id, step):
        end_id = start_id + step - 1
        get_all_profiles(start_id, end_id, max_workers=max_workers)
        print(f"[{start_id}, {end_id}], step = {step}")
        print("---------------------------------------------------------")
        fd = open("download_log.txt", 'w')
        fd.write(f"[{start_id}, {end_id}], step = {step}")
        fd.close()
        # fd = open("download_log_copy.txt", 'w')
        # fd.write(f"[{start_id}, {end_id}], step = {step}")
        # fd.close()


if __name__ == "__main__":
    _, start_id, end_id, step, max_workers = sys.argv
    start_id = int(start_id)
    end_id = int(end_id)
    step = int(step)
    max_workers = int(max_workers)
    print(start_id, end_id, step, max_workers)
    main(start_id, end_id, step, max_workers)
