"""Class required to parallelize the network length data."""
from typing import List, Tuple, Any
from pyproj import Geod
import geopandas as gpd


# class Calculator:
#     """Network length"""

#     def __init__(self, polygons: List, railways: gpd.GeoDataFrame) -> None:
#         """Initialize polygon and networks attributes"""
#         self.polygons = polygons
#         self.railways = railways

#     def calculate(self, index: int) -> Tuple[Any, int, Any]:
#         """Intersect Polygon with Linestring and measure the length being intersected"""
#         polygon = self.polygons[index]["geometry"]
#         region_code = self.polygons[index]["region_code"]
#         total = 0
#         for linestring in self.railways["geometry"]:
#             if polygon.intersects(linestring):
#                 linestring_intersection = polygon.intersection(linestring)
#                 geod = Geod(ellps="WGS84")
#                 single_network_lenght = geod.geometry_length(linestring_intersection)
#                 total = total + int(single_network_lenght)

#         return region_code, total, polygon

class Calculator:
    """Network length"""

    def __init__(self, polygons, railways):
        """Initialize polygon and networks attributes"""
        self.polygons = polygons
        self.railways = railways

    def calculate(self, index):
        """Intersect Polygon with Linestring and measure the length being intersected"""
        polygon = self.polygons[index]["geometry"]
        region_code = self.polygons[index]["region_code"]
        total = 0
        for linestring in self.railways["geometry"]:
            if polygon.intersects(linestring):
                linestring_intersection = polygon.intersection(linestring)
                geod = Geod(ellps="WGS84")
                single_network_lenght = geod.geometry_length(linestring_intersection)
                total = total + int(single_network_lenght)

        return region_code, total, polygon