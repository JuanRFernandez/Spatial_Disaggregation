"""The functions required to process data."""
import os
import pandas as pd
import geopandas as gpd
from zoomin.data.constants import countries_dict

cwd = os.getcwd()
DATA_PATH = os.path.join(cwd, "..", "..", "..", "data", "input")
RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")
PROCESSED_DATA_PATH = os.path.join(DATA_PATH, "processed")


def setup_polygon_for_point(
    territorial_unit: str, country_tag: str
) -> gpd.GeoDataFrame:
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


def add_centroid_row(point_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Get the centroid of each geometry to make the overlapping"""
    return point_gdf["geometry"].centroid


def setup_point_gdf(
    component_name: str, territorial_unit: str, country_name: str, country_tag: str
) -> gpd.GeoDataFrame:
    """Get point geodataframe for each component_name in each country at a territoriial unit."""
    point_gdf_path_destination = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_gdf_data_{territorial_unit}.csv",
    )
    point_df_path_source = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_{country_tag}_from_place.csv",
    )
    if not os.path.exists(point_gdf_path_destination):
        point_df = pd.read_csv(point_df_path_source)
        print(
            f'The total number of "{component_name}" point_df in "{country_tag}" are: ',
            len(point_df),
        )
        point_df["geometry"] = gpd.GeoSeries.from_wkt(point_df["geometry"])
        point_gdf = gpd.GeoDataFrame(point_df, geometry="geometry")
        point_gdf["geometry"] = point_gdf.apply(add_centroid_row, axis=1)
        point_gdf.reset_index(drop=True, inplace=True)
        print(
            f'The total number of "{component_name}" centroid_point_df in "{country_tag}" are: ',
            len(point_gdf),
        )
        point_gdf.drop(
            [col for col in point_df.columns if "geometry" not in col],
            axis=1,
            inplace=True,
        )
        point_gdf.drop(
            [col for col in point_gdf.columns if "region_geometry" in col],
            axis=1,
            inplace=True,
        )
        point_gdf.to_csv(point_gdf_path_destination)
    else:
        point_df = pd.read_csv(point_gdf_path_destination)
        point_df["geometry"] = gpd.GeoSeries.from_wkt(point_df["geometry"])
        point_gdf = gpd.GeoDataFrame(point_df, geometry="geometry")
        point_gdf.drop(
            [col for col in point_gdf.columns if "geometry" not in col],
            axis=1,
            inplace=True,
        )
    return point_gdf


def overlap_point_and_polygon(
    component_name: str, territorial_unit: str, country_name: str, country_tag: str
) -> gpd.GeoDataFrame:
    """Overlap gridded data with polygon data."""
    overlap_gdf_path_destination = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_OverlapGdf_{territorial_unit}.csv",
    )
    if not os.path.exists(overlap_gdf_path_destination):
        point_gdf = setup_point_gdf(
            component_name, territorial_unit, country_name, country_tag
        )
        point_gdf = point_gdf.set_crs(epsg=4326)
        polygon_gdf = setup_polygon_for_point(territorial_unit, country_tag)
        if polygon_gdf.crs != 4326:
            polygon_gdf = polygon_gdf.to_crs(epsg=4326)
        overlap_gdf = gpd.sjoin(
            point_gdf, polygon_gdf, how="left", predicate="intersects"
        )
        print(
            f'The total number of "{component_name}" overlaped points in "{country_tag}" are: ',
            len(overlap_gdf),
        )
        overlap_gdf.to_csv(overlap_gdf_path_destination)
    else:
        overlap_df = pd.read_csv(overlap_gdf_path_destination)
        overlap_df["geometry"] = gpd.GeoSeries.from_wkt(overlap_df["geometry"])
        overlap_gdf = gpd.GeoDataFrame(overlap_df, geometry="geometry")
    return overlap_gdf


def count_point_on_polygon(
    component_name: str, territorial_unit: str, country_name: str, country_tag: str
) -> pd.DataFrame:
    """Count the total number of features foe each component per region at country level."""
    overlap_df_path_destination = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        f"{country_tag}",
        f"{component_name}_OverlapDf_{territorial_unit}.csv",
    )
    if not os.path.exists(overlap_df_path_destination):
        overlap_gdf = overlap_point_and_polygon(
            component_name, territorial_unit, country_name, country_tag
        )
        # count the number of stations in each region_code
        for names, values in overlap_gdf[["region_code"]].items():
            overlap_count_gdf = overlap_gdf["region_code"].value_counts()
        overlap_count_df = overlap_count_gdf.to_frame()
        number_of_stations = overlap_count_df["region_code"].sum()
        print(
            f'The total number of "{component_name}" counted in "{country_tag}" are: ',
            number_of_stations,
        )
        overlap_count_df.index.name = "region_code"
        overlap_count_df.rename(columns={"region_code": "value"}, inplace=True)
        overlap_df = overlap_count_df.reset_index()
        overlap_df.drop(
            [
                col
                for col in overlap_df.columns
                if "region_code" not in col and "value" not in col
            ],
            axis=1,
            inplace=True,
        )
        overlap_df.to_csv(overlap_df_path_destination)
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
    return overlap_df


def count_point_on_polygon_eu(
    component_name: str, territorial_unit: str
) -> pd.DataFrame:
    """Overlap gridded data with polygon data for the eu countries."""
    overlap_gdf_path_destination = os.path.join(
        PROCESSED_DATA_PATH,
        "osm_data",
        "countries",
        "stations_overlap_df_data",
        f"{component_name}_OverlapDf_{territorial_unit}.csv",
    )
    if not os.path.exists(overlap_gdf_path_destination):
        overlap_df_list = []
        for country_tag in countries_dict.values():
            overlap_gdf_path_source = os.path.join(
                PROCESSED_DATA_PATH,
                "osm_data",
                "countries",
                f"{country_tag}",
                f"{component_name}_OverlapDf_{territorial_unit}.csv",
            )
            if os.path.exists(overlap_gdf_path_source):
                overlap_df = pd.read_csv(overlap_gdf_path_source)
                overlap_df_list.append(overlap_df)
        overlap_df = pd.concat(overlap_df_list)
        overlap_df.rename(columns={"value": f"{component_name}_value"}, inplace=True)
        print(
            f'The total number of "{territorial_unit}" regions mapped in the EU27 are: ',
            len(overlap_df),
        )
        overlap_df.drop(
            [
                col
                for col in overlap_df.columns
                if "region_code" not in col and "value" not in col
            ],
            axis=1,
            inplace=True,
        )
        overlap_df.to_csv(overlap_gdf_path_destination)
    else:
        overlap_df = pd.read_csv(overlap_gdf_path_destination)
        overlap_df.drop(
            [
                col
                for col in overlap_df.columns
                if "value" not in col and f"{component_name}_value" not in col
            ],
            axis=1,
            inplace=True,
        )
        print(
            f'The total number of "{territorial_unit}" regions mapped in the EU27 are: ',
            len(overlap_df),
        )
    return overlap_df
