"""The values that are access by several other modules are placed here."""
from typing import Dict, Union, List


# Dict of all tags considered in Juan's Master Thesis with OSM data
stations_tags_dict = {
    "fuel_stations": {
        "amenity": "fuel"
    },
    "charging_stations": {"amenity": "charging_station"},
    "bicycle_stations": {
        "amenity": [
            "bicycle_rental",  # A place, often unattended, where you can pick up and drop off rented bikes.
            "bicylce_parking",
        ]
    },  # A parking space designed for bicycles.
    "bus_stations": {"amenity": "bus_station", "highway": "bus_stop"},
    "airport_stations": {
        "aeroway": [
            "aerodrome",  # Aerodrome in the United Kingdom or Airport in North America. An aerodrome, airport or airfield
            "aporn",  # An apron or ramp is the surfaced part of an airport where planes park.
            "fuel",
        ],
        "amenity": "airport",
    },  # An aviation fuel station.
    "railway_station": {
        "railway": [
            "station",  # A railway station
            "stop",  # The spot on a railway track where a train stops at a station
            "train_station_entrance",  # The entrance point of a train station
            "platform",
        ]
    }, # a railway platform / Una plataforma ferrioviaria
    "train_station": {"building": "train_station"},  # A train station building
    "subway_station": {"station": "subway"},  # Subway station
    "lightrail_station": {
        "station": "light_rail",  # Lightrail station
        "railway": "tram_stop",
    },  # A tram stop is a place where a passenger can embark / disembark a tram.
    "shipping_station": {
        "amenity": "ferry_terminal"
    },  # A place where people, cars etc. can board and leave a ferry.
    "helicopter_station": {"aeroway": "helipad"},  # A place for landing helicopters.
}

networks_tags_dict = {
    "bicycle_network": {
        "bicycle_road": "yes",  # A bicycle road is a road designated for bicycles.
        "cyclestreet": "yes",
    },  # A bicycle road is a road designated for bicycles.
    "bus_network": {
        "route": "bus",  # The route of a public bus service. (index error)
        "service": "bus",
    },  # Road typically found at bus stations
    "railways_network": {
        "railway": "rail"
    },  # Rails of a standard gauge track  / Rieles de una vía de ancho estándar.
    "road_major_network": {
        "highway": [
            "motorway",  # Motorway/freeway
            "trunk",  # Important roads, typically divided
            "primary",  # Primary roads, typically national.
            "secondary",  # Secondary roads, typically regional.
            "tertiary",  # Tertiary roads, typically local.
        ]
    },
    # # "road_minor_network": {
    # #     "highway": [
    # #         "unclassified",  # Smaller local roads
    # #         "residential",  # Roads in residential areas
    # #         "living_street",  # Streets where pedestrians have priority
    # #         "pedestrian",  # Pedestrian only streets
    # #         "busway",  # Dedicated roads for bus, usually closed for any mode of transport except public transport.
    # #     ]
    # # },
    "shipping_network": {"route": "ferry"},  # The route of a public ferry or water bus
}

name_tags_dict: Dict[str, Dict[str, Union[List, str]]] = {
    "fuel_stations": {
        "amenity": "fuel"
    },  # second problem, how to give several tags at once
    "charging_stations": {"amenity": "charging_station"},
    "bicycle_stations": {
        "amenity": [
            "bicycle_rental",  # A place, often unattended, where you can pick up and drop off rented bikes.
            "bicylce_parking",
        ]
    },  # A parking space designed for bicycles.
    "bus_stations": {"amenity": "bus_station", "highway": "bus_stop"},
    "airport_stations": {
        "aeroway": [
            "aerodrome",  # Aerodrome in the United Kingdom or Airport in North America is used
            # to map the main area details (other than helicopters only). An aerodrome, airport or airfield
            "aporn",  # An apron or ramp is the surfaced part of an airport where planes park.
            # Aprons are usually well visible on aerial and satellite imagery as an area
            "fuel",
        ],
        "amenity": "airport",
    },  # An aviation fuel station.
    "railway_station": {
        "railway": [
            "station",  # A railway station
            "stop",  # The spot on a railway track where a train stops at a station
            "train_station_entrance",  # The entrance point of a train station
            "platform",
        ]
    },  # a railway platform / Una plataforma ferrioviaria
    "train_station": {"building": "train_station"},  # A train station building
    "subway_station": {"station": "subway"},  # Subway station
    "lightrail_station": {
        "station": "light_rail",  # Lightrail station
        "railway": "tram_stop",
    },  # A tram stop is a place where a passenger can embark / disembark a tram.
    "shipping_station": {
        "amenity": "ferry_terminal"
    },  # A place where people, cars etc. can board and leave a ferry.
    "helicopter_station": {"aeroway": "helipad"},  # A place for landing helicopters.
    "bicycle_network": {
        "bicycle_road": "yes",  # A bicycle road is a road designated for bicycles.
        "cyclestreet": "yes",
    },  # A bicycle road is a road designated for bicycles.
    "bus_network": {
        "route": "bus",  # The route of a public bus service. (index error)
        "service": "bus",
    },  # Road typically found at bus stations
    "railways_network": {
        "railway": "rail"
    },  # Rails of a standard gauge track  / Rieles de una vía de ancho estándar.
    "road_network": {
        "highway": [
            "motorway",  # Motorway/freeway
            "trunk",  # Important roads, typically divided
            "primary",  # Primary roads, typically national.
            "secondary",  # Secondary roads, typically regional.
            "tertiary",  # Tertiary roads, typically local.
            "unclassified",  # Smaller local roads
            "residential",
        ]
    },
    "shipping_network": {"route": "ferry"},  # The route of a public ferry or water bus
}

# Sources used to find the tags description and definition:
# Tag finder: http://tagfinder.herokuapp.com/
# Geofabrik: https://www.bing.com/ck/a?!&&p=97583475d4ae4622JmltdHM9MTY2ODU1NjgwMCZpZ
# 3VpZD0zZTIyOTgwMC1mYjMyLTZjYzQtMzFjYi04YTJiZmEwMTZkN2ImaW5zaWQ9NTE5MA&ptn=3&hsh=3&f
# clid=3e229800-fb32-6cc4-31cb-8a2bfa016d7b&psq=geofabrik+in+osm+data+gis+format+pdf&
# u=a1aHR0cHM6Ly93d3cuZ2VvZmFicmlrLmRlL2RhdGEvZ2VvZmFicmlrLW9zbS1naXMtc3RhbmRhcmQtMC43LnBkZg&ntb=1
