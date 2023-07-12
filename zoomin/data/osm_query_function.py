"""Query code with OSMnx api in combination with Nominatim api"""
import warnings
import os
import osmnx as ox
import pandas as pd
import geopandas as gpd
from zoomin.data.osmtags import name_tags_dict, networks_tags_dict

cwd = os.getcwd()
DATA_PATH = os.path.join(cwd, "..", "..", "..", "data", "input")

RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")

PROCESSED_DATA_PATH = os.path.join(DATA_PATH, "processed")

"""
Functions to query the osm data
"""


def get_polygon_gdf(country_tag: str, territorial_unit: str) -> gpd.GeoDataFrame:
    """Get polygon geodataframe for each country-region."""
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


def get_osm_data(country_tag: str, territorial_unit: str) -> pd.DataFrame:
    """Get osm data"""
    polygon_gdf = get_polygon_gdf(country_tag, territorial_unit)
    for component_name, tags in networks_tags_dict.items():
        data_gdf_destination = os.path.join(
            PROCESSED_DATA_PATH,
            "osm_data",
            "countries",
            f"{country_tag}",
            f"{component_name}_{country_tag}_{territorial_unit}.csv",
        )
        print(component_name)
        print(tags)
        if not os.path.exists(data_gdf_destination):
            # check if the crs is 4326 to be able to map Europe
            if polygon_gdf.crs != 4326:
                # it is almost always 4326, we ensure to match the locations on data
                polygon_gdf = polygon_gdf.to_crs(epsg=4326)
            # unprint the following statement when working with spatial resolution higher than LAU:
            print(
                f'The data "{component_name}_{country_tag}_{territorial_unit}" is been queried'
            )
            data_gdf_list = []
            for index_row in polygon_gdf.iterrows():
                row = index_row[1]
                data_gdf = ox.geometries_from_polygon(row["geometry"], tags)
                # create a list with each key of the tags in name_tags_dict, example: for the
                # tag "aeroway": "aerodrome", we add to the list the key "aeroway", and so on
                tag_key = list(tags.keys())[0]
                try:
                    # extract the gdf as two columns: key "aeroway" and geometry
                    data_gdf = data_gdf.loc[:, [tag_key, "geometry"]]
                    # print(polygon_gdf['region_code'])
                    data_gdf["region_code"] = row["region_code"]
                    data_gdf["region_geometry"] = row["geometry"]
                    data_gdf_list.append(data_gdf)
                    # print the following statement when working with spatial resolution higher than LAU:
                    print(
                        f'The data "{component_name}_{country_tag}_{territorial_unit}" is been queried'
                    )
                except IndexError:
                    continue
                    # print the following warning when working with spatial resolution higher than LAU:
                    # warnings.warn(
                    #     f"The data \"{component_name}_{country_tag}_{territorial_unit}_{tag_key}\" was not found"
                    #     )
            try:
                # TRY to concatenate becasue if concatenated directly wihtout
                # TRY and data_gdf_list is an empty list (because data was not found);
                # will receive an error.
                data_gdf = pd.concat(data_gdf_list)
                data_gdf.reset_index(drop=True, inplace=True)
                data_gdf.to_csv(data_gdf_destination)
                print(
                    f' The data_gdf_list "{component_name}_{country_tag}_{territorial_unit}" \
has been concatenated to the data_gdf and save it'
                )
            except IndexError:
                print(
                    f'There is nothing to concat beacuse data \
"{component_name}_{country_tag}_{territorial_unit}" \
was not found and the data_gdf_list is empty'
                )
        else:
            print(
                f'The file "{component_name}_{country_tag}_{territorial_unit}.csv" \
already exist'
            )
            data_gdf = pd.read_csv(data_gdf_destination)

    return data_gdf
