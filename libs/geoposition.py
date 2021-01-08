from geopy.geocoders import Nominatim
# import cartopy

id = 0


class Geoposition:

    def __init__(self):
        self.geolocator = Nominatim(user_agent=("vk-users-personal-data-analyzing-" + str(id)), timeout=10)

    def get_geoposition(self, city=None, country=None):
        query = []
        if not city is None:
            query.append(city)
        if not country is None:
            query.append(country)
        location = self.geolocator.geocode(", ".join(query))
        if location is None:
            return None
        else:
            return {
                'latitude': location.latitude,
                'longitude': location.longitude
            }


# def make_geoplot():
#     city_ids = {}
#     for profile in new_profiles:
#         if not profile.get('city') is None:
#             if city_ids.get(profile['city']['id']) is None:
#                 city_ids[profile['city']['id']] = profile['city']
#
#         for profile in new_profiles:
#             if not profile.get('city') is None:
#                 if not city_ids.get(profile['city']['id']) is None:
#                 city_ids[profile['city']['id']]['count'] = city_ids[profile['city']['id']].setdefault('count', 0) + 1