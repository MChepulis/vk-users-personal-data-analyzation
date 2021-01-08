from geoposition import Geoposition


city_dict = {}
country_dict = {}


def add_geoposition(countries):
    geo = Geoposition()
    for country in countries:
        if not country.get('cities') is None:
            for city in country['cities']:
                if city_dict.get(city['city']) is None:
                    city_location = geo.get_geoposition(city['city'], country['country'])
                    city_dict[city['city']] = city_location
                else:
                    city_location = city_dict[city['city']]
                city['location'] = city_location
        if country_dict.get(country['country']) is None:
            country_location = geo.get_geoposition(country=country['country'])
            country_dict[country['country']] = country_location
        else:
            country_location = country_dict[country['country']]
        country['location'] = country_location
    return countries


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
    profiles = process_bday(profiles)
    profiles = process_career(profiles)
    profiles = process_relatives(profiles)
    return profiles
