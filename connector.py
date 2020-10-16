import requests
import vk_api
import db_client
import threading

from keys import app_id, client_secret, safe_key, token, api_version

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
    print(len(clear_data), len(deleted_data))

    client = db_client.VkDatabaseClient()
    deleted_result, inserted_result = client.put_many_user_info(clear_data)
    if len(inserted_result.inserted_ids) != len(clear_data):
        raise RuntimeError("Expected to insert " + str(len(clear_data)) + " objects. "
                                                                          "Inserted " + str(len(inserted_result.inserted_ids)))


def get_all_profiles(start_id, end_id, step=1000):
    ids_arr = []

    while start_id < (end_id + 1) - step:
        ids_arr.append([i for i in range(start_id, start_id + step)])
        start_id += step
        pass

    ids_arr.append([i for i in range(start_id, end_id + 1)])
    threads = [threading.Thread(target=download_user_infos, args=(ids,)) for ids in ids_arr]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("count of available users in database - ", db_client.VkDatabaseClient().count_of_available_users())


def main():
    get_all_profiles(1, 5000)


if __name__ == "__main__":
    main()
