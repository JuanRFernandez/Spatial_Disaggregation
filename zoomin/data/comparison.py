"""Data comparison, quality assessment, filling missing values is done here."""
import os
import itertools
from typing import Optional, Union, Dict

import numpy as np
import pandas as pd

from zoomin.database.db_access import (
    get_primary_key,
    get_table,
    get_regions,
    get_col_values,
    add_col_values,
    add_to_input_var_values,
)
from zoomin.data.constants import resolution_hierarchy
from zoomin.disaggregation.disaggregation import disaggregate_value


# get var qualities
fk_good_quality = get_primary_key(
    "input_var_qualities", cols_criteria={"var_quality_level": "good"}
)
fk_okay_quality = get_primary_key(
    "input_var_qualities", cols_criteria={"var_quality_level": "okay"}
)
fk_bad_quality = get_primary_key(
    "input_var_qualities", cols_criteria={"var_quality_level": "bad"}
)


def get_regions_df(resolution: str = "LAU") -> pd.DataFrame:
    """Get region data based on mini_db or not."""
    if bool(os.environ.get("MINI_DB", 0)):
        regions_df = get_regions(resolution, country="PT")
    else:
        regions_df = get_regions(resolution)

    return regions_df


def get_regions_and_data(
    resolution: str, input_var_detail_id: Optional[int] = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Get region data and the input var data."""
    regions_df = get_regions_df(resolution)

    # get data
    if len(regions_df) == 1:
        sql_cmd = (
            f"SELECT * FROM input_var_values WHERE region_id={regions_df.id.values[0]}"
        )
    else:
        sql_cmd = f"SELECT * FROM input_var_values WHERE region_id IN {tuple(regions_df.id.values)}"

    if input_var_detail_id is not None:
        sql_cmd = f"{sql_cmd} AND input_var_detail_id={input_var_detail_id}"

    data_df = get_table(sql_cmd=sql_cmd)

    return (regions_df, data_df)


def get_proxy_data() -> pd.DataFrame:
    """Get population data at LAU, use it as proxy to disaggregate all upper level data (temp solution)."""
    fk_var_name = get_primary_key(
        "input_var_details", cols_criteria={"var_name": "population"}
    )

    sql_cmd = f"SELECT * FROM input_var_values WHERE input_var_detail_id={fk_var_name} AND source_for_internal_ref='GHSL'"

    proxy_data_at_lau = get_table(sql_cmd=sql_cmd)
    proxy_data_at_lau = proxy_data_at_lau[["value", "region_id"]]

    lau_regions = get_regions_df()

    proxy_data_at_lau = pd.merge(
        proxy_data_at_lau, lau_regions, left_on="region_id", right_on="id", how="left"
    )

    return proxy_data_at_lau


def map_region_data_to_lau(
    regions_df: pd.DataFrame, var_df: pd.DataFrame, n_chars_to_del: int
) -> pd.DataFrame:
    """Map region data to lau regions."""
    lau_regions = get_regions_df()

    if n_chars_to_del != 0:
        lau_regions["current_parent_region_code"] = lau_regions[
            "parent_region_code"
        ].str[:-n_chars_to_del]
    else:
        lau_regions["current_parent_region_code"] = lau_regions["parent_region_code"]

    region_lau_mapping = pd.merge(
        regions_df,
        lau_regions,
        left_on="region_code",
        right_on="current_parent_region_code",
        how="left",
    )

    def _f(data_df: pd.DataFrame) -> Dict[str, Union[pd.DataFrame, tuple]]:
        return {
            "region_id": data_df["id_x"].values[0],
            "region_code": data_df["current_parent_region_code"].values[0],
            "lau_region_ids": set(data_df["id_y"].values),
        }

    region_lau_mapping = region_lau_mapping.groupby("current_parent_region_code").apply(
        _f
    )
    region_lau_mapping = pd.DataFrame(region_lau_mapping.values.tolist())

    region_lau_mapping = pd.merge(region_lau_mapping, var_df, how="left")

    region_lau_mapping = region_lau_mapping[["lau_region_ids", "value"]]

    return region_lau_mapping


def disaggregate_and_annotate_lau_data(
    input_var_detail_id: int, var_df: pd.DataFrame, regions_df: pd.DataFrame
) -> None:
    """Disaggregate, annotate data quality, and chosen as 1 for all lau level data."""
    # FIXME: currently skpping GHSL population data;
    # this is disaggregated from Eurostat to LAU;
    # GHSL population data is only used as proxy!!
    var_name = get_col_values(
        "input_var_details",
        col="var_name",
        cols_criteria={"id": input_var_detail_id},
    )
    if var_name != "population":
        # annotate data; chosen=1 and quality=good
        add_col_values(
            table="input_var_values",
            col_values={
                "chosen": 1,
                "input_var_quality_id": fk_good_quality,
            },
            cols_criteria={"id": tuple(var_df["id"].values)},
        )

        # if data is incomplete; fill missing values with 0; annotate chosen=1 and quality=bad
        missing_regions = set(regions_df["id"].values) - set(var_df["region_id"].values)
        if len(missing_regions) > 0:
            missing_df = pd.DataFrame(sorted(missing_regions), columns=["region_id"])

            col_val_dict = {
                "source_for_internal_ref": "disaggregated data",
                "input_var_detail_id": input_var_detail_id,
                # NOTE: here were are assuming that all other data is from same year;
                # so, assigning same year to missing values as well
                "year": int(var_df["year"].values[0]),
                "climate_model_detail_id": var_df["climate_model_detail_id"].values[0],
                "chosen": 1,
                "input_var_quality_id": fk_bad_quality,
                "value": 0,
            }
            for col, val in col_val_dict.items():
                missing_df[col] = np.repeat(val, len(missing_df))

            add_to_input_var_values(missing_df)


def disaggregate_and_annotate_nuts_data(
    input_var_detail_id: int,
    regions_df: pd.DataFrame,
    var_df: pd.DataFrame,
    resolution_index: int,
) -> None:
    """Disaggregate, annotate data quality, and chosen as 1 for all nuts level data."""
    # proceed only if the data is not already disaggregated to LAU and chosen;
    # can be the case when for ex.: the data is present at NUTS3 and NUTS2 and currently we are looking data at NUTS2
    sql_cmd = f"SELECT * FROM input_var_values WHERE input_var_detail_id={input_var_detail_id} AND chosen=1"
    chosen_data = get_table(sql_cmd)

    if len(chosen_data) == 0:
        # collect current regions, list of corresponding lau regions and the var values for current regions in a df
        n_chars_to_del = resolution_index - 1
        region_data_to_lau_df = map_region_data_to_lau(
            regions_df, var_df, n_chars_to_del
        )

        # if data missing values;
        if region_data_to_lau_df["value"].isna().any():

            # check if upper level data exists;
            for j in range(resolution_index + 1, len(resolution_hierarchy[:-1])):
                (
                    upper_lvl_regions_df,
                    upper_lvl_var_df,
                ) = get_regions_and_data(resolution_hierarchy[j], input_var_detail_id)

                if len(upper_lvl_var_df) != 0:
                    n_chars_to_del = j - 1
                    upper_lvl_region_data_to_lau_df = map_region_data_to_lau(
                        upper_lvl_regions_df,
                        upper_lvl_var_df,
                        n_chars_to_del,
                    )

                    for key_group in upper_lvl_region_data_to_lau_df.iterrows():
                        group = key_group[1]

                        lower_lvl_regions = group["lau_region_ids"].values

                        region_data_to_lau_df["issubset"] = region_data_to_lau_df.apply(
                            lambda x: x["lau_region_ids"].issubset(lower_lvl_regions),
                            axis=1,
                        )

                        na_regions_df = region_data_to_lau_df[
                            (region_data_to_lau_df["value"].isna())
                            & (region_data_to_lau_df["issubset"] is True)
                        ]

                        if len(na_regions_df) != 0:
                            residual_lau_regions = set(
                                itertools.chain.from_iterable(
                                    na_regions_df["lau_region_ids"].values
                                )
                            )

                            residual_value = group["value"] - np.nansum(
                                region_data_to_lau_df["value"].values
                            )

                            # remove LAU region sets with missing values
                            region_data_to_lau_df.drop(
                                (na_regions_df["lau_region_ids"]).index,
                                inplace=True,
                            )

                            temp_df = pd.DataFrame(
                                {
                                    "lau_region_ids": [residual_lau_regions],
                                    "value": residual_value,
                                }
                            )
                            region_data_to_lau_df = pd.concat(
                                [region_data_to_lau_df, temp_df]
                            )

                            if not region_data_to_lau_df["value"].isna().any():
                                break
                    else:
                        continue  # only executed if the inner loop did NOT break
                    break  # only executed if the inner loop DID break

        # disaggregation to LAU
        disagg_df_list = []
        proxy_df = get_proxy_data()
        for key_row in region_data_to_lau_df.iterrows():
            row = key_row[1]
            sub_proxy_df = proxy_df[proxy_df["id"].isin(row["lau_region_ids"])]
            disagg_df = disaggregate_value(row["value"], sub_proxy_df)
            disagg_df_list.append(disagg_df)

        db_ready_df = pd.concat(disagg_df_list)

        # where values missing - annotate quality=bad fill with 0, rest annotate quality=okay
        db_ready_df.loc[
            db_ready_df["value"].isna(), "input_var_quality_id"
        ] = fk_bad_quality
        db_ready_df.loc[db_ready_df["value"].isna(), "value"] = 0
        db_ready_df.loc[
            db_ready_df["input_var_quality_id"].isna(),
            "input_var_quality_id",
        ] = fk_okay_quality

        db_ready_df = db_ready_df[["value", "region_id", "input_var_quality_id"]]
        db_ready_df["input_var_quality_id"] = db_ready_df[
            "input_var_quality_id"
        ].astype(
            int
        )  # for some reason,its float

        # fill rest of the columns
        col_val_dict = {
            "source_for_internal_ref": "disaggregated data",
            "input_var_detail_id": input_var_detail_id,
            # NOTE: here were are assuming that all other data is from same year
            # as the var_df is
            "year": int(var_df["year"].values[0]),
            "climate_model_detail_id": var_df["climate_model_detail_id"].values[0],
            "chosen": 1,
        }

        for col, val in col_val_dict.items():
            db_ready_df[col] = np.repeat(val, len(db_ready_df))

        # add to db
        add_to_input_var_values(db_ready_df)


def disaggregate_and_annotate_data() -> None:
    """Disaggregate, annotate data quality, and chosen as 1."""
    for resolution_index, resolution in enumerate(resolution_hierarchy[:-1]):
        (regions_df, input_var_values_df) = get_regions_and_data(resolution)

        # process each variable
        # NOTE: here we are assuming that each variable is collected at a certain level only from 1 source
        for input_var_detail_id in input_var_values_df["input_var_detail_id"].unique():

            var_df = input_var_values_df[
                input_var_values_df["input_var_detail_id"] == input_var_detail_id
            ]

            # if data is at LAU level;
            if resolution == "LAU":
                disaggregate_and_annotate_lau_data(
                    input_var_detail_id, var_df, regions_df
                )

            # if data is at levels other than LAU;
            else:
                disaggregate_and_annotate_nuts_data(
                    input_var_detail_id, regions_df, var_df, resolution_index
                )
