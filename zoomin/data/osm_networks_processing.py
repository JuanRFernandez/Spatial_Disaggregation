"""Code to process the network data"""
import os
from typing import Any
from multiprocessing import Pool
import pandas as pd
import geopandas as gpd
from zoomin.data.constants import countries_dict
from zoomin.data.osmtags import networks_tags_dict
from zoomin.data import osm_networks_class_parallelization

cwd = os.getcwd()
DATA_PATH = os.path.join(cwd, "..", "..", "..", "data", "input")
RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")
PROCESSED_DATA_PATH = os.path.join(DATA_PATH, "processed")

"""
Provides geospatial functions
"""


# def setup_polygons(territorial_unit: Any, country_tag: Any) -> gpd.GeoDataFrame:
#     """Get polygons geodataframe for each country at a territoriial unit."""
#     polygon_shp_path = os.path.join(
#         PROCESSED_DATA_PATH, "shapefiles", f"{territorial_unit}.shp"
#     )
#     polygon_gdf = gpd.read_file(polygon_shp_path)
#     polygon_gdf = polygon_gdf[polygon_gdf["prnt_code"].str.contains(f"{country_tag}")]
#     polygon_gdf.drop(
#         [
#             col
#             for col in polygon_gdf.columns
#             if "geometry" not in col and "code" not in col
#         ],
#         axis=1,
#         inplace=True,
#     )
#     polygon_gdf.drop(
#         [col for col in polygon_gdf.columns if col.startswith("prnt")],
#         axis=1,
#         inplace=True,
#     )
#     polygon_gdf.rename(columns={"code": "region_code"}, inplace=True)
#     polygon_gdf.reset_index(drop=True, inplace=True)
#     return polygon_gdf


def setup_polygons(territorial_unit, country_tag):
    """Get polygons geodataframe for each country at a territoriial unit."""
    polygon_shp_path = os.path.join(
        PROCESSED_DATA_PATH, "shapefiles", f"{territorial_unit}.shp"
    )
    polygon_gdf = gpd.read_file(polygon_shp_path)
    polygon_gdf = polygon_gdf[polygon_gdf["prnt_code"].str.contains(f"{country_tag}")]
    polygon_gdf.drop(
        [
            col
            for col in polygon_gdf.columns
            if "geometry" not in col and "code" not in col
        ],
        axis=1,
        inplace=True,
    )
    polygon_gdf.drop(
        [col for col in polygon_gdf.columns if col.startswith("prnt")],
        axis=1,
        inplace=True,
    )
    polygon_gdf.rename(columns={"code": "region_code"}, inplace=True)
    polygon_gdf.reset_index(drop=True, inplace=True)
    return polygon_gdf


# def setup_linestrings(
#     territorial_unit: Any, component_name: Any, country_tag: Any
# ) -> gpd.GeoDataFrame:
#     """Get linestring geodataframe for each component_name in each country at a territoriial unit."""
#     linestring_gdf_path_destination = os.path.join(
#         PROCESSED_DATA_PATH,
#         "osm_data",
#         "countries",
#         f"{country_tag}",
#         f"{component_name}_linestring_gdf_data_{country_tag}_{territorial_unit}.csv",
#     )
#     linestring_df_path_source = os.path.join(
#         PROCESSED_DATA_PATH,
#         "osm_data",
#         "countries",
#         f"{country_tag}",
#         f"{component_name}_{country_tag}_from_place.csv",
#     )
#     if not os.path.exists(linestring_gdf_path_destination):
#         print(
#             f'The "{component_name}_linestring_gdf_data_{country_tag}_{territorial_unit}.csv" file \
# does not exists; the Linesrtrings are being setup'
#         )
#         linestring_df = pd.read_csv(linestring_df_path_source)
#         linestring_df.drop(
#             [col for col in linestring_df.columns if "geometry" not in col],
#             axis=1,
#             inplace=True,
#         )
#         linestring_df["geometry"] = gpd.GeoSeries.from_wkt(linestring_df["geometry"])
#         linestring_gdf = gpd.GeoDataFrame(linestring_df, geometry="geometry")
#         if linestring_gdf["geometry"].isnull().sum() > 0:
#             print(
#                 f"The number of NaN values present in {country_tag} \
#                     {linestring_gdf['geometry'].isnull().sum()}"
#             )
#         linestring_gdf.to_csv(linestring_gdf_path_destination, index=False)
#     else:
#         linestring_df = pd.read_csv(linestring_gdf_path_destination)
#         linestring_df["geometry"] = gpd.GeoSeries.from_wkt(linestring_df["geometry"])
#         linestring_df.drop(
#             [col for col in linestring_df.columns if "geometry" not in col],
#             axis=1,
#             inplace=True,
#         )
#         linestring_gdf = gpd.GeoDataFrame(linestring_df, geometry="geometry")
#     return linestring_gdf


def setup_linestrings(territorial_unit, component_name, country_tag):
    """Get linestring geodataframe for each component_name in each country at a territoriial unit."""
    linestring_gdf_path_destination = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_linestring_gdf_data_{country_tag}_{territorial_unit}.csv",
    )
    linestring_df_path_source = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_{country_tag}_from_place.csv",
    )
    if not os.path.exists(linestring_gdf_path_destination):
        print(
            f'The "{component_name}_linestring_gdf_data_{country_tag}_{territorial_unit}.csv" file \
does not exists; the Linesrtrings are being setup'
        )
        linestring_df = pd.read_csv(linestring_df_path_source)
        linestring_df.drop(
            [col for col in linestring_df.columns if "geometry" not in col],
            axis=1,
            inplace=True,
        )
        linestring_df["geometry"] = gpd.GeoSeries.from_wkt(linestring_df["geometry"])
        linestring_gdf = gpd.GeoDataFrame(linestring_df, geometry="geometry")
        if linestring_gdf["geometry"].isnull().sum() > 0:
            print(
                f"The number of NaN values present in {country_tag} \
                    {linestring_gdf['geometry'].isnull().sum()}"
            )
        linestring_gdf.to_csv(linestring_gdf_path_destination, index=False)
    else:
        linestring_df = pd.read_csv(linestring_gdf_path_destination)
        linestring_df["geometry"] = gpd.GeoSeries.from_wkt(linestring_df["geometry"])
        linestring_df.drop(
            [col for col in linestring_df.columns if "geometry" not in col],
            axis=1,
            inplace=True,
        )
        linestring_gdf = gpd.GeoDataFrame(linestring_df, geometry="geometry")
    return linestring_gdf


# def get_length_gdf(
#     territorial_unit: Any, component_name: Any, country_tag: Any
# ) -> gpd.GeoDataFrame:
#     """Get length geo data frame for each component_name in each country at a territorial_unit."""
#     overlap_gdf_path_destination = os.path.join(
#         PROCESSED_DATA_PATH,
#         "osm_data",
#         "countries",
#         f"{country_tag}",
#         f"{component_name}_overlap_gdf_{country_tag}_{territorial_unit}.csv",
#     )
#     if not os.path.exists(overlap_gdf_path_destination):
#         print(
#             f'The "{component_name}_overlap_gdf_{country_tag}_{territorial_unit}.csv" file \
# does not exists so the "getLength_gdf" function is measuring the length'
#         )
#         polygons = setup_polygons(territorial_unit, country_tag)
#         linestring_gdf = setup_linestrings(
#             territorial_unit, component_name, country_tag
#         )
#         # if polygons.crs != 4326:
#         #     polygons = polygons.to_crs(epsg=4326)
#         polygons_list = []
#         for index, row in polygons.iterrows():
#             polygons_list.append(row)
#         calculator = osm_networks_class_parallelization.Calculator(
#             polygons_list, linestring_gdf
#         )
#         number_of_polygons = len(polygons_list)
#         with Pool(4) as pool:
#             result_list_of_tuples = pool.map(
#                 calculator.calculate, range(0, number_of_polygons)
#             )
#         overlap_gdf = pd.DataFrame(
#             result_list_of_tuples, columns=["region_code", "value", "geometry"]
#         )
#         overlap_gdf["geometry"] = gpd.GeoSeries.from_wkt(overlap_gdf["geometry"])
#         overlap_gdf = gpd.GeoDataFrame(overlap_gdf, geometry="geometry")
#         overlap_gdf.to_csv(overlap_gdf_path_destination, index=False)
#     else:
#         overlap_df = pd.read_csv(overlap_gdf_path_destination)
#         overlap_df["geometry"] = gpd.GeoSeries.from_wkt(overlap_df["geometry"])
#         overlap_df.drop(
#             [
#                 col
#                 for col in overlap_df.columns
#                 if "geometry" not in col
#                 and "region code" not in col
#                 and "value" not in col
#             ],
#             axis=1,
#             inplace=True,
#         )
#         overlap_gdf = gpd.GeoDataFrame(overlap_df, geometry="geometry")
#     return overlap_gdf


def get_length_gdf(territorial_unit, component_name, country_tag):
    """Get length geo data frame for each component_name in each country at a territorial_unit."""
    overlap_gdf_path_destination = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_overlap_gdf_{country_tag}_{territorial_unit}.csv",
    )
    if not os.path.exists(overlap_gdf_path_destination):
        print(
            f'The "{component_name}_overlap_gdf_{country_tag}_{territorial_unit}.csv" file \
does not exists so the "getLength_gdf" function is measuring the length'
        )
        polygons = setup_polygons(territorial_unit, country_tag)
        linestring_gdf = setup_linestrings(
            territorial_unit, component_name, country_tag
        )
        if polygons.crs != 4326:
            polygons = polygons.to_crs(epsg=4326)
        polygons_list = []
        for index, row in polygons.iterrows():
            polygons_list.append(row)
        calculator = osm_networks_class_parallelization.Calculator(
            polygons_list, linestring_gdf
        )
        number_of_polygons = len(polygons_list)
        with Pool(4) as pool:
            result_list_of_tuples = pool.map(
                calculator.calculate, range(0, number_of_polygons)
            )
        overlap_gdf = pd.DataFrame(
            result_list_of_tuples, columns=["region_code", "value", "geometry"]
        )
        overlap_gdf["geometry"] = gpd.GeoSeries.from_wkt(overlap_gdf["geometry"])
        overlap_gdf = gpd.GeoDataFrame(overlap_gdf, geometry="geometry")
        overlap_gdf.to_csv(overlap_gdf_path_destination, index=False)
    else:
        overlap_df = pd.read_csv(overlap_gdf_path_destination)
        overlap_df["geometry"] = gpd.GeoSeries.from_wkt(overlap_df["geometry"])
        overlap_df.drop(
            [
                col
                for col in overlap_df.columns
                if "geometry" not in col
                and "region code" not in col
                and "value" not in col
            ],
            axis=1,
            inplace=True,
        )
        overlap_gdf = gpd.GeoDataFrame(overlap_df, geometry="geometry")
    return overlap_gdf


# def get_length_df(
#     territorial_unit: Any, component_name: Any, country_tag: Any
# ) -> pd.DataFrame:
#     """Get length data frame for each component_name in each country at a territorial_unit"""
#     overlap_df_path_destination = os.path.join(
#         PROCESSED_DATA_PATH,
#         "osm_data",
#         "countries",
#         f"{country_tag}",
#         f"{component_name}_overlap_df_{country_tag}_{territorial_unit}.csv",
#     )
#     if not os.path.exists(overlap_df_path_destination):
#         overlap_df = get_length_gdf(territorial_unit, component_name, country_tag)
#         overlap_df.drop(
#             [
#                 col
#                 for col in overlap_df.columns
#                 if "region_code" not in col and "value" not in col
#             ],
#             axis=1,
#             inplace=True,
#         )
#         overlap_df.to_csv(overlap_df_path_destination, index=False)
#         print(overlap_df.head(2))
#     else:
#         overlap_df = pd.read_csv(overlap_df_path_destination)
#         print(overlap_df.head(2))
#     return overlap_df


def get_length_df(territorial_unit, component_name, country_tag):
    """Get length data frame for each component_name in each country at a territorial_unit"""
    overlap_df_path_destination = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_overlap_df_{country_tag}_{territorial_unit}.csv",
    )
    if not os.path.exists(overlap_df_path_destination):
        overlap_df = get_length_gdf(territorial_unit, component_name, country_tag)
        overlap_df.drop(
            [
                col
                for col in overlap_df.columns
                if "region_code" not in col and "value" not in col
            ],
            axis=1,
            inplace=True,
        )
        overlap_df.to_csv(overlap_df_path_destination, index=False)
        print(overlap_df.head(2))
    else:
        overlap_df = pd.read_csv(overlap_df_path_destination)
        overlap_df.drop(
            [
                col
                for col in overlap_df.columns
                if "region_code" not in col and "value" not in col
            ],
            axis=1,
            inplace=True,
        )
        print(overlap_df.head(2))
    return overlap_df


# def get_length_gdf_eu() -> pd.DataFrame:
#     """Get length geo data frame for each component_name in the EU at a territorial_unit"""
#     territorial_unit = input(
#         "Please enter a character from: LAU, NUTS3, NUTS2, NUTS1, NUTS0, Europe"
#     )
#     for component_name in networks_tags_dict.keys():
#         print(component_name)
#         overlap_gdf_path_destination = os.path.join(
#             PROCESSED_DATA_PATH,
#             "osm_data",
#             "countries",
#             "networks_overlap_gdf_data",
#             f"{component_name}_overlap_gdf_{territorial_unit}.csv",
#         )
#         if not os.path.exists(overlap_gdf_path_destination):
#             overlap_gdf_df_list = []
#             for country_tag in countries_dict.values():
#                 print(country_tag)
#                 overlap_gdf_path_source = os.path.join(
#                     PROCESSED_DATA_PATH,
#                     "osm_data",
#                     "countries",
#                     f"{country_tag}",
#                     f"{component_name}_overlap_gdf_{country_tag}_{territorial_unit}.csv",
#                 )
#                 if os.path.exists(overlap_gdf_path_source):
#                     overlap_gdf = pd.read_csv(overlap_gdf_path_source)
#                     overlap_gdf_df_list.append(overlap_gdf)
#                 else:
#                     continue
#             # concat all the data in one dataframe
#             overlap_gdf = pd.concat(overlap_gdf_df_list)
#             overlap_gdf.reset_index(drop=True, inplace=True)
#             overlap_gdf.drop(
#                 [
#                     col
#                     for col in overlap_gdf.columns
#                     if "region_code" not in col
#                     and "value" not in col
#                     and "geometry" not in col
#                 ],
#                 axis=1,
#                 inplace=True,
#             )
#             overlap_gdf.to_csv(overlap_gdf_path_destination, index=False)
#         else:
#             overlap_gdf = pd.read_csv(overlap_gdf_path_destination)
#             print(
#                 f"The file {component_name}_overlap_gdf_{territorial_unit}.csv exists and it is read it: "
#             )
#             print(overlap_gdf.head(1))
#     return overlap_gdf


def get_length_gdf_eu():
    """Get length geo data frame for each component_name in the EU at a territorial_unit"""
    territorial_unit = input(
        "Please enter a character from: LAU, NUTS3, NUTS2, NUTS1, NUTS0, Europe"
    )
    for component_name in networks_tags_dict.keys():
        print(component_name)
        overlap_gdf_path_destination = os.path.join(
            PROCESSED_DATA_PATH,
            "osm_data",
            "countries",
            "networks_overlap_gdf_data",
            f"{component_name}_overlap_gdf_{territorial_unit}.csv",
        )
        if not os.path.exists(overlap_gdf_path_destination):
            overlap_gdf_df_list = []
            for country_tag in countries_dict.values():
                print(country_tag)
                overlap_gdf_path_source = os.path.join(
                    PROCESSED_DATA_PATH,
                    "osm_data",
                    "countries",
                    f"{country_tag}",
                    f"{component_name}_overlap_gdf_{country_tag}_{territorial_unit}.csv",
                )
                if os.path.exists(overlap_gdf_path_source):
                    overlap_gdf = pd.read_csv(overlap_gdf_path_source)
                    overlap_gdf_df_list.append(overlap_gdf)
                else:
                    continue
            # concat all the data in one dataframe
            overlap_gdf = pd.concat(overlap_gdf_df_list)
            overlap_gdf.reset_index(drop=True, inplace=True)
            overlap_gdf.drop(
                [
                    col
                    for col in overlap_gdf.columns
                    if "region_code" not in col
                    and "value" not in col
                    and "geometry" not in col
                ],
                axis=1,
                inplace=True,
            )
            overlap_gdf.to_csv(overlap_gdf_path_destination, index=False)
        else:
            overlap_gdf = pd.read_csv(overlap_gdf_path_destination)
            print(
                f"The file {component_name}_overlap_gdf_{territorial_unit}.csv exists and it is read it: "
            )
            print(overlap_gdf.head(1))
    return overlap_gdf


# def get_length_df_eu() -> pd.DataFrame:
#     """Get length data frame for each component_name in the EU at a territorial_unit"""
#     territorial_unit = input(
#         "Please enter a character from: LAU, NUTS3, NUTS2, NUTS1, NUTS0, Europe"
#     )
#     for component_name in networks_tags_dict.keys():
#         print(component_name)
#         overlap_df_path_destination = os.path.join(
#             PROCESSED_DATA_PATH,
#             "osm_data",
#             "countries",
#             "networks_overlap_df_data",
#             f"{component_name}_overlap_df_{territorial_unit}.csv",
#         )
#         overlap_gdf_path_source = os.path.join(
#             PROCESSED_DATA_PATH,
#             "osm_data",
#             "countries",
#             "networks_overlap_gdf_data",
#             f"{component_name}_overlap_gdf_{territorial_unit}.csv",
#         )
#         if not os.path.exists(overlap_df_path_destination):
#             if os.path.exists(overlap_gdf_path_source):
#                 overlap_df = pd.read_csv(overlap_gdf_path_source)
#                 overlap_df.drop(
#                     [
#                         col
#                         for col in overlap_df.columns
#                         if "region_code" not in col and "value" not in col
#                     ],
#                     axis=1,
#                     inplace=True,
#                 )
#                 overlap_df.to_csv(overlap_df_path_destination, index=False)
#                 print(overlap_df.head(2))
#         else:
#             overlap_df = pd.read_csv(overlap_df_path_destination)
#             print(overlap_df.head(2))
#     return overlap_df


def get_length_df_eu():
    """Get length data frame for each component_name in the EU at a territorial_unit"""
    territorial_unit = input(
        "Please enter a character from: LAU, NUTS3, NUTS2, NUTS1, NUTS0, Europe"
    )
    for component_name in networks_tags_dict.keys():
        print(component_name)
        overlap_df_path_destination = os.path.join(
            PROCESSED_DATA_PATH,
            "osm_data",
            "countries",
            "networks_overlap_df_data",
            f"{component_name}_overlap_df_{territorial_unit}.csv",
        )
        overlap_gdf_path_source = os.path.join(
            PROCESSED_DATA_PATH,
            "osm_data",
            "countries",
            "networks_overlap_gdf_data",
            f"{component_name}_overlap_gdf_{territorial_unit}.csv",
        )
        if not os.path.exists(overlap_df_path_destination):
            if os.path.exists(overlap_gdf_path_source):
                overlap_df = pd.read_csv(overlap_gdf_path_source)
                overlap_df.rename(columns={"value": f"{component_name}_value"}, inplace=True)
                overlap_df.drop(
                    [
                        col
                        for col in overlap_df.columns
                        if "region_code" not in col and f"{component_name}_value" not in col
                    ],
                    axis=1,
                    inplace=True,
                )
                print(f'The total number of "{territorial_unit}" regions mapped for {component_name} in the EU27 are: ',
                len(overlap_df))
                overlap_df.to_csv(overlap_df_path_destination, index=False)
        else:
            overlap_df = pd.read_csv(overlap_df_path_destination)
            
            overlap_df.drop(
                [
                    col
                    for col in overlap_df.columns
                    if "region_code" not in col and f"{component_name}_value" not in col
                ],
                axis=1,
                inplace=True,
            )
            print(f'The total number of "{territorial_unit}" regions mapped for {component_name} in the EU27 are: ',
            len(overlap_df))
        print(overlap_df.head(2))
    return overlap_df
