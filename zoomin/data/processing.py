"""The functions required to process data."""
import os
from io import BytesIO
from urllib.request import urlopen, Request
import zipfile
import json
from collections import OrderedDict
from typing import Any, Dict, Optional, List, cast, Union
import warnings

import numpy as np
import pandas as pd
import geopandas as gpd

from zoomin.data.constants import countries_dict
from zoomin.database.db_access import get_on_the_fly_calculation_vars, get_proxy_vars

PROCESSED_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "input", "processed"
)


def overlap_point_and_polygon_data(
    point_df: Union[pd.DataFrame, gpd.GeoDataFrame],
    latitude_col: Optional[str] = "y",
    longitude_col: Optional[str] = "x",
    spatial_resolution: Optional[str] = "NUTS0",
    country_code: Optional[str] = None,
    join_type: Optional[str] = "outer",
) -> pd.DataFrame:
    """Overlap gridded data with polygon data."""
    # convert the point_df into a geodataframe
    if isinstance(point_df, pd.DataFrame):
        point_df = gpd.GeoDataFrame(
            point_df,
            geometry=gpd.points_from_xy(
                point_df[longitude_col], point_df[latitude_col]
            ),
        )

    # change to 3035 to match the locations on data
    point_df.crs = (
        "EPSG:4326"  # first set it. if it is not specified, it is almost always 4326
    )
    point_df = point_df.to_crs(3035)

    # read in the shapefile from DB
    polygon_gdf = gpd.read_file(
        os.path.join(PROCESSED_DATA_PATH, "shapefiles", f"{spatial_resolution}.shp")
    )
    if country_code is not None:
        region_list = [i for i in polygon_gdf["prnt_code"] if i[:2] == country_code]
        polygon_gdf = polygon_gdf.loc[polygon_gdf["prnt_code"].isin(region_list)]

    # overlap the dataframes
    overlap_df = gpd.sjoin(point_df, polygon_gdf, how=join_type, predicate="intersects")
    overlap_df.drop(columns=["name", "year"], inplace=True)

    return overlap_df


def check_locations(
    point_df: pd.DataFrame,
    latitude_col: Optional[str] = "y",
    longitude_col: Optional[str] = "x",
    country_col: Optional[str] = "country",
) -> pd.DataFrame:
    """Check which locations are claimed to be in a certian country are not."""
    data_gdf = overlap_point_and_polygon_data(
        point_df,
        latitude_col,
        longitude_col,
        spatial_resolution="NUTS0",
        join_type="inner",
    )

    countries = pd.DataFrame(
        countries_dict.items(), columns=["country_actual", "region_code"]
    )
    merged_df = pd.merge(data_gdf, countries, how="inner")

    mislocated_df = merged_df.loc[
        np.where(merged_df[country_col] != merged_df["country_actual"])
    ]

    return mislocated_df


def get_data_from_zipped_url(data_url: str) -> pd.DataFrame:  #
    """Take a url with zipped data and return the contained pandas dataframe."""
    req = Request(url=data_url, headers={"User-Agent": "Mozilla/5.0"})

    webpage = urlopen(req)  # noqa
    zip_file = zipfile.ZipFile(BytesIO(webpage.read()))  # noqa

    for filename in zip_file.namelist():
        if ".csv" in filename:
            data_df = pd.read_csv(zip_file.open(filename))  # noqa

    return data_df


def process_eucalc_data(path_to_data: str, country: str) -> pd.DataFrame:
    """Get the EUCalc pathway data in the form that can be dumped into the database."""

    with open(path_to_data, "r", encoding="utf-8") as f_c:
        data = json.load(f_c)

    eucalc_df = pd.DataFrame(data.get("outputs"))

    df_list = []

    for row_info in eucalc_df.iterrows():

        row = row_info[1]

        sub_df = pd.DataFrame({"year": row["timeAxis"], "value": row.data.get(country)})

        var_name = row["id"].split("[")[0]

        sub_df["var_name"] = var_name
        df_list.append(sub_df)

    processed_df = pd.concat(df_list, ignore_index=True)

    return processed_df
