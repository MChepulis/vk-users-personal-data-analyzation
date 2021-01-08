from geoposition import Geoposition


city_dict = {}
country_dict = {}


def add_geoposition(profiles):
    geo = Geoposition()
    for profile in profiles:
        city = None
        country = None
        city_id = None
        country_id = None
        if not profile.get('city') is None:
            city = profile['city']['title']
            city_id = profile['city']['id']
        if not profile.get('country') is None:
            country = profile['country']['title']
            country_id = profile['country']['id']
        if not city is None:
            if city_dict.get(city_id) is None:
                city_location = geo.get_geoposition(city, country)
                city_dict[city_id] = city_location
            else:
                city_location = city_dict[city_id]
            profile['city']['location'] = city_location
        if not country is None:
            if country_dict.get(country_id) is None:
                country_location = geo.get_geoposition(country=country)
                country_dict[country_id] = country_location
            else:
                country_location = country_dict[country_id]
            profile['country']['location'] = country_location
    return profiles


def check_bdate_fullness(bdate):
    bdate_parts = bdate.split('.')
    return len(bdate_parts) == 3


def check_bdate_correctness(bdate):
    bdate_parts = bdate.split('.')
    return len(bdate_parts) == 2 or (1920 < int(bdate_parts[2]) < 2010)


def process_bday(profiles):
    for profile in profiles:
        if (profile.get('bdate') is None) or profile['bdate'] == '':
            profile['has_bdate'] = False
        else:
            profile['has_bdate'] = True
            profile['bdate_fullness'] = check_bdate_fullness(profile['bdate'])
            profile['bdate_correctness'] = check_bdate_correctness(profile['bdate'])

    return profiles


def process_career(profiles):
    for profile in profiles:
        profile['has_career'] = (not profile.get('career') is None) and (len(profile['career']) > 0)

    return profiles


def process_relatives(profiles):
    for profile in profiles:
        profile['has_relatives'] = (not profile.get('relatives') is None) and (len(profile['relatives']) > 0)

    return profiles


def data_preparation(profiles):
    # profiles = add_geoposition(profiles)
    profiles = process_bday(profiles)
    profiles = process_career(profiles)
    profiles = process_relatives(profiles)
    return profiles
