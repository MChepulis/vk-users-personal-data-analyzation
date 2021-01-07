import elasticsearch_client as es_client
import elasticsearch_dsl
import matplotlib.pyplot as plt

index = "available_users"


def get_elastic_object(vk_elastic_db: es_client.VkDataDatabaseClient):
    return vk_elastic_db._es


def response_process(response, aggs_name, title, is_need_other, is_need_print, is_need_plot):

    x_axis = [hit.key for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]
    if is_need_other:
        x_axis.append("other")
        y_axis.append(response.aggregations[aggs_name].sum_other_doc_count)

    if is_need_print:
        print(title)
        for i in range(len(x_axis)):
            print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

    if is_need_plot:
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.barh(x_axis, y_axis)
        plt.show()


def count_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True, is_need_print=False,
                     is_need_plot=True):
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="country.title.keyword", size=size)
    aggs_name = "countries_count"
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    response_process(response, aggs_name, "count by country", is_need_other, is_need_print, is_need_plot)


def name_count(vk_elastic_db: es_client.VkDataDatabaseClient, aggs_name, sex=None, size=10):
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    if sex is not None:
        s = s.filter('term', sex=sex)
    a = elasticsearch_dsl.A('terms', field="first_name.keyword", size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    return response


def man_name_count(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=False, is_need_print=False,
                   is_need_plot=True):
    aggs_name = "man_first_name_count"
    title = "man_first_name_count"
    response = name_count(vk_elastic_db, aggs_name, sex=2, size=size)
    response_process(response, aggs_name, title, is_need_other, is_need_print, is_need_plot)


def woman_name_count(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=False, is_need_print=False,
                     is_need_plot=True):
    aggs_name = "woman_first_name_count"
    title = "woman_first_name_count"
    response = name_count(vk_elastic_db, aggs_name, sex=1, size=size)
    response_process(response, aggs_name, title, is_need_other, is_need_print, is_need_plot)


def last_name_count(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=False, is_need_print=False,
                    is_need_plot=True):
    aggs_name = "last_name_count"
    sex_aggs_name = "sex_aggs"
    title = "last name count"
    sex_size = 2
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    sex_a = elasticsearch_dsl.A('terms', field="sex", missing="-1", size=sex_size)
    a = elasticsearch_dsl.A('terms', field="last_name.keyword", size=size)
    s.aggs.bucket(sex_aggs_name, sex_a).bucket(aggs_name, a)
    response = s.execute()

    data_dict = {}
    sex_dict = {
        "0": "unknown",
        "1": "woman",
        "2": "man",
        "-1": "missing"
    }
    for sex_hit in response.aggregations[sex_aggs_name].buckets:
        x_axis = [hit.key for hit in sex_hit[aggs_name].buckets]
        y_axis = [hit.doc_count for hit in sex_hit[aggs_name].buckets]
        if is_need_other:
            x_axis.append("other")
            y_axis.append(sex_hit[aggs_name].sum_other_doc_count)
        data_dict[sex_dict[str(sex_hit.key)]] = {}
        data_dict[sex_dict[str(sex_hit.key)]]["x_axis"] = x_axis
        data_dict[sex_dict[str(sex_hit.key)]]["y_axis"] = y_axis

    for sex in data_dict:
        x_axis = data_dict[sex]["x_axis"]
        y_axis = data_dict[sex]["y_axis"]
        cur_title = f"{title}\n{sex}"

        if is_need_print:
            print(cur_title)
            for i in range(len(x_axis)):
                print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

        if is_need_plot:
            fig, ax = plt.subplots(1, 1)
            ax.set_title(cur_title)
            ax.barh(x_axis, y_axis)
            plt.show()



def sex_distribution(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=False, is_need_print=False,
                     is_need_plot=True):
    aggs_name = " sex_distribution"
    title = " sex distribution"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="sex", missing="-1", size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    label_dict = {
        "0": "unknown",
        "1": "woman",
        "2": "man",
        "-1": "missing"
    }
    x_axis = [label_dict[str(hit.key)] for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]
    if is_need_other:
        x_axis.append("other")
        y_axis.append(response.aggregations[aggs_name].sum_other_doc_count)

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


def has_country(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True):
    aggs_name = "has_country"
    title = "has country"
    aggs_name = "has_country"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    size = 10000
    missing_str = "missing"
    a = elasticsearch_dsl.A('terms', field="country.title.keyword", missing=missing_str, size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()

    data = {
        "has country": 0,
        "missing country": 0
    }
    for hit in response.aggregations[aggs_name].buckets:
        if hit.key == missing_str:
            data["missing country"] += hit.doc_count
        else:
            data["has country"] += hit.doc_count
    x_axis = [key for key in data]
    y_axis = [data[key] for key in data]

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


def has_city(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True):
    aggs_name = "has_city"
    title = "has city"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    s = s.filter()
    size = 10000
    missing_str = "missing"
    a = elasticsearch_dsl.A('terms', field="city.title.keyword", missing=missing_str, size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()

    data = {
        "has city": 0,
        "missing city": 0
    }
    for hit in response.aggregations[aggs_name].buckets:
        if hit.key == missing_str:
            data["missing city"] += hit.doc_count
        else:
            data["has city"] += hit.doc_count
    x_axis = [key for key in data]
    y_axis = [data[key] for key in data]

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


def count_by_city(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True, is_need_print=False,
                  is_need_plot=True):
    aggs_name = "city_count"
    title = "count by city"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="city.title.keyword", size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()

    response_process(response, aggs_name, title, is_need_other, is_need_print, is_need_plot)


def count_by_city_order_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True,
                                   is_need_print=False, is_need_plot=True):
    country_aggs_name = "country_count"
    city_aggs_name = "city_count"
    title = "count by city"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
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

        if is_need_print:
            print(cur_title)
            for i in range(len(x_axis)):
                print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

        if is_need_plot:
            fig, ax = plt.subplots(1, 1)
            ax.set_title(cur_title)
            ax.barh(x_axis, y_axis)
            plt.show()


def has_photo(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True, is_need_print=False,
               is_need_plot=True):
    aggs_name = "has_photo"
    title = "has_photo"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="has_photo", size=size, missing="-1")
    s.aggs.bucket(aggs_name, a)
    response = s.execute()

    label_dict = {
        "1": "has",
        "0": "has not",
        "-1": "missing"
    }
    x_axis = [label_dict[str(hit.key)] for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]
    if is_need_other:
        x_axis.append("other")
        y_axis.append(response.aggregations[aggs_name].sum_other_doc_count)

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


def has_mobile(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True, is_need_print=False,
               is_need_plot=True):
    aggs_name = "has_mobile"
    title = "has_mobile"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="has_mobile", size=size, missing="0")
    s.aggs.bucket(aggs_name, a)
    response = s.execute()

    label_dict = {
        "1": "has",
        "0": "has not",
        "-1": "missing"
    }
    x_axis = [label_dict[str(hit.key)] for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]
    if is_need_other:
        x_axis.append("other")
        y_axis.append(response.aggregations[aggs_name].sum_other_doc_count)

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


def has_university(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True):
    aggs_name = "has_university"
    title = "has university"
    missing_str = "missing"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="university_name.keyword", size=1000, missing=missing_str)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    data = {
        "has university": 0,
        "has not university": 0
    }
    for hit in response.aggregations[aggs_name].buckets:
        if hit.key == missing_str:
            data["has not university"] += hit.doc_count
        else:
            data["has university"] += hit.doc_count

    x_axis = [key for key in data]
    y_axis = [data[key] for key in data]

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

def count_by_university(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True, is_need_print=False,
                  is_need_plot=True):
    aggs_name = "university_count"
    title = "university count"
    missing_str = ""
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="university_name.keyword", missing=missing_str, size=size)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()

    x_axis = []
    y_axis = []
    for hit in response.aggregations[aggs_name].buckets:
        if hit.key == missing_str:
            continue
        x_axis.append(hit.key)
        y_axis.append(hit.doc_count)
    if is_need_other:
        x_axis.append("other")
        y_axis.append(response.aggregations[aggs_name].sum_other_doc_count)

    if is_need_print:
        print(title)
        for i in range(len(x_axis)):
            print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

    if is_need_plot:
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.barh(x_axis, y_axis)
        plt.show()

        sizes = [elem / sum(y_axis) for elem in y_axis]
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.pie(sizes, labels=x_axis, autopct='%1.1f%%', startangle=90)
        plt.show()

def count_by_university_order_by_country(vk_elastic_db: es_client.VkDataDatabaseClient, size=10, is_need_other=True,
                                   is_need_print=False, is_need_plot=True):
    country_aggs_name = "country_count"
    university_aggs_name = "university_count"
    title = "count by city"
    missing_str = ""
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="country.title.keyword", size=size, collect_mode="breadth_first")
    a1 = elasticsearch_dsl.A('terms', field="university_name.keyword",  missing=missing_str, size=size)
    s.aggs.bucket(country_aggs_name, a1).bucket(university_aggs_name, a)
    response = s.execute()

    data_dict = {}
    for country_hit in response.aggregations[country_aggs_name].buckets:

        x_axis = []
        y_axis = []
        for hit in country_hit[university_aggs_name].buckets:
            if hit.key == missing_str:
                continue
            x_axis.append(hit.key)
            y_axis.append(hit.doc_count)
        if is_need_other:
            x_axis.append("other")
            y_axis.append(country_hit[university_aggs_name].sum_other_doc_count)
        data_dict[country_hit.key] = {}
        data_dict[country_hit.key]["x_axis"] = x_axis
        data_dict[country_hit.key]["y_axis"] = y_axis

    for country in data_dict:
        x_axis = data_dict[country]["x_axis"]
        y_axis = data_dict[country]["y_axis"]
        cur_title = f"{title}\n{country}"

        if is_need_print:
            print(cur_title)
            for i in range(len(x_axis)):
                print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

        if is_need_plot:
            fig, ax = plt.subplots(1, 1)
            ax.set_title(cur_title)
            ax.barh(x_axis, y_axis)
            plt.show()


def profile_access_count(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True):
    aggs_name = "profile_access_count"
    title = "profile access count"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    a = elasticsearch_dsl.A('terms', field="is_closed", size=10)
    s.aggs.bucket(aggs_name, a)
    response = s.execute()
    lable_dict = {
        "0": "opened",
        "1": "closed"
    }
    x_axis = [lable_dict[str(hit.key)] for hit in response.aggregations[aggs_name].buckets]
    y_axis = [hit.doc_count for hit in response.aggregations[aggs_name].buckets]

    if is_need_print:
        print(title)
        for i in range(len(x_axis)):
            print(f"{i + 1}\t{x_axis[i]} {y_axis[i]}")

    if is_need_plot:
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.barh(x_axis, y_axis)
        plt.show()

        sizes = [elem / sum(y_axis) for elem in y_axis]
        fig, ax = plt.subplots(1, 1)
        ax.set_title(title)
        ax.pie(sizes, labels=x_axis, autopct='%1.1f%%', startangle=90)
        plt.show()


def other_social_network(vk_elastic_db: es_client.VkDataDatabaseClient, is_need_print=False, is_need_plot=True):
    title = "has other site"
    es = get_elastic_object(vk_elastic_db)
    s = elasticsearch_dsl.Search(using=es, index=index)
    q = elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="twitter")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="site")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="skype")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="livejournal")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="instagram")]) |\
        elasticsearch_dsl.Q("bool", must=[elasticsearch_dsl.Q("exists", field="facebook")])

    s = s.query(q)
    response = s.execute()
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
    total_num = elasticsearch_dsl.Search(using=es, index=index).count()
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
        plt.show()

    pass

def start():
    client = es_client.VkDataDatabaseClient()
    # man_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    # woman_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    # last_name_count(client, size=20, is_need_other=False, is_need_print=True, is_need_plot=True)
    # sex_distribution(client, size=2, is_need_other=False, is_need_print=True, is_need_plot=True)
    # has_country(client, is_need_print=True, is_need_plot=True)
    # count_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # has_city(client, is_need_print=True, is_need_plot=True)
    # count_by_city(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # count_by_city_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # has_photo(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    # has_mobile(client, size=10, is_need_other=False, is_need_print=True, is_need_plot=True)
    # has_university(client, is_need_print=True, is_need_plot=True)
    # count_by_university(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # count_by_university_order_by_country(client, size=10, is_need_other=True, is_need_print=True, is_need_plot=True)
    # profile_access_count(client, is_need_print=True, is_need_plot=True)
    # other_social_network(client, is_need_print=True, is_need_plot=True)

if __name__ == "__main__":
    start()
