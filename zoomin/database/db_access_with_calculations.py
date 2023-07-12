"""Functions to help disaggregate values to LAU and populate DB with data."""
import os
import json
import traceback
import psutil
import time
import logging
import warnings

import pandas as pd

from zoomin.data.constants import del_dict

from zoomin.database.db_access import (
    get_regions,
    get_primary_key,
    add_to_region_data,
    get_table,
    get_col_values,
    add_to_proxy_metrics,
    get_var_data_for_disaggregation,
)
from zoomin.disaggregation.disaggregation import disaggregate_value, add_proxy_vars

db_access_with_calculations_log = logging.getLogger("db_access_with_calculations")
logging.basicConfig(level=logging.INFO)

RAW_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "input", "raw"
)
PROCESSED_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "input", "processed"
)

LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "output", "logs")


def aggregate_and_add_to_db(db_ready_df, var_name):
    db_access_with_calculations_log.info(
        f"aggregating {var_name} data to higher levels ---------"
    )
    agg_method = get_col_values(
        "var_details", "var_aggregation_method", {"var_name": var_name}
    )

    def _perform_aggregation(data_group):
        # aggregate the value
        if agg_method == "sum":
            agg_val = data_group["value"].sum()
        elif agg_method == "mean":
            agg_val = data_group["value"].mean()
        elif agg_method == "bool":
            agg_val = data_group["value"].unique().item()

        data_dict = {
            "value": agg_val,
            "var_detail_id": data_group["var_detail_id"].unique().item(),
            "citation_id": data_group["citation_id"].unique().item(),
            "original_resolution_id": data_group["original_resolution_id"]
            .unique()
            .item(),
            "chosen": data_group["chosen"].unique().item(),
            "parent_region_code": data_group["parent_region_code"].unique().item(),
        }

        # get the most repeated quality_rating
        if "quality_rating" in data_group.columns:
            agg_quality_rating = data_group["quality_rating"].value_counts().idxmax()
            data_dict.update({"quality_rating": agg_quality_rating})

        for var in [
            "year",
            "climate_experiment_id",
            "disaggregation_method_id",
            "pathway_id",
            "confidence_interval",
        ]:
            if var in data_group.columns:
                data_dict.update({var: data_group[var].unique().item()})

        return pd.Series(data_dict)

    for resolution in ["NUTS0", "NUTS1", "NUTS2", "NUTS3"]:

        agg_df = db_ready_df.copy(deep=True)

        if resolution != "NUTS3":
            n_del = del_dict[resolution]
            agg_df["parent_region_code"] = agg_df["parent_region_code"].str[:-n_del]

        group_vars = list(
            set(agg_df.columns)
            & {"parent_region_code", "climate_experiment_id", "year"}
        )

        agg_df = agg_df.groupby(group_vars, dropna=False).apply(_perform_aggregation)
        agg_df.reset_index(drop=True, inplace=True)

        regions_df = get_regions(resolution)

        agg_df = pd.merge(
            agg_df,
            regions_df,
            left_on=["parent_region_code"],
            right_on=["region_code"],
            how="left",
        )

        agg_df.rename(columns={"id": "region_id"}, inplace=True)
        agg_df.drop(
            columns=[
                "parent_region_code_x",
                "parent_region_code_y",
                "region_code",
            ],
            inplace=True,
        )

        add_to_region_data(agg_df)


def process_and_add_lau_data(data_df, var_name, details_dict):
    regions_df = get_regions("LAU")

    db_ready_df = pd.merge(
        data_df,
        regions_df,
        left_on=["reg_code", "prnt_code"],
        right_on=["region_code", "parent_region_code"],
        how="left",
    )

    db_ready_df.rename(columns={"id": "region_id"}, inplace=True)

    _fk_citation = get_primary_key(
        "citations", {"data_source_citation": details_dict["citation"]}
    )
    db_ready_df["citation_id"] = _fk_citation

    _fk_org_res = get_primary_key(
        "original_resolutions", {"original_resolution": details_dict["resolution"]}
    )
    db_ready_df["original_resolution_id"] = _fk_org_res

    _fk_var_detail = get_primary_key("var_details", {"var_name": var_name})
    db_ready_df["var_detail_id"] = _fk_var_detail

    if "climate_experiment" in db_ready_df.columns:
        clt_expt_df = get_table(sql_cmd="SELECT * FROM climate_experiments")
        clt_expt_df.rename(columns={"id": "climate_experiment_id"}, inplace=True)

        db_ready_df = pd.merge(
            db_ready_df,
            clt_expt_df,
            left_on="climate_experiment",
            right_on="climate_experiment",
            how="left",
        )
        db_ready_df.drop(["climate_experiment"], inplace=True)

    db_ready_df["chosen"] = 1
    db_access_with_calculations_log.info(f"{var_name} has {len(db_ready_df)} rows")

    db_ready_df.drop(
        columns=[
            "reg_code",
            "prnt_code",
            "region_code",
        ],
        inplace=True,
    )

    # dump LAU region data
    lau_db_df = db_ready_df.drop(
        columns=[
            "parent_region_code",
        ]
    )

    add_to_region_data(lau_db_df)

    # aggregate and dump upper level region data
    aggregate_and_add_to_db(db_ready_df, var_name)


def disaggregate_and_add_data(data_df, var_name, details_dict, proxy):
    if "+" in proxy:  # assuming only sum of vars or a var could be proxy
        proxy_data, proxy_vars = add_proxy_vars(proxy)
    else:
        proxy_data = get_var_data_for_disaggregation(proxy)
        proxy_vars = [proxy]

    # add a match_region_code on proxy data based on spatial resolution of the data
    if details_dict["resolution"] != "NUTS3":
        n_del = del_dict[details_dict["resolution"]]
        proxy_data["match_region_code"] = proxy_data["parent_region_code"].str[:-n_del]
    else:
        proxy_data["match_region_code"] = proxy_data["parent_region_code"]

    disagg_df_list = []
    for key_row in data_df.iterrows():
        row = key_row[1]
        _proxy_data = proxy_data[proxy_data["match_region_code"] == row["reg_code"]]
        disagg_df = disaggregate_value(row["value"], _proxy_data)
        disagg_df["quality_rating"] = row[
            "quality_rating"
        ]  # same quality rating as the disagg.ed
        # value to all
        disagg_df["year"] = row["year"]  # same year as the disagg.ed
        # value to all
        disagg_df_list.append(disagg_df)

    db_ready_df = pd.concat(disagg_df_list)

    _fk_var_detail = get_primary_key("var_details", {"var_name": var_name})
    db_ready_df["var_detail_id"] = _fk_var_detail

    _fk_citation = get_primary_key(
        "citations", {"data_source_citation": details_dict["citation"]}
    )
    db_ready_df["citation_id"] = _fk_citation

    _fk_org_res = get_primary_key(
        "original_resolutions", {"original_resolution": details_dict["resolution"]}
    )
    db_ready_df["original_resolution_id"] = _fk_org_res

    _fk_disagg_method = get_primary_key(
        "disaggregation_methods", {"disaggregation_method": "Using proxy metrics"}
    )
    db_ready_df["disaggregation_method_id"] = _fk_disagg_method

    if "climate_experiment" in db_ready_df.columns:
        clt_expt_df = get_table(sql_cmd="SELECT * FROM climate_experiments")
        clt_expt_df.rename(columns={"id": "climate_experiment_id"}, inplace=True)

        db_ready_df = pd.merge(
            db_ready_df,
            clt_expt_df,
            left_on="climate_experiment",
            right_on="climate_experiment",
            how="left",
        )
        db_ready_df.drop(["climate_experiment"], inplace=True)

    db_ready_df["chosen"] = 1

    db_ready_df.drop(columns=["region_code", "match_region_code"], inplace=True)

    # dump LAU region data
    lau_db_df = db_ready_df.drop(
        columns=[
            "parent_region_code",
        ]
    )

    add_to_region_data(lau_db_df)

    # aggregate and dump upper level region data
    aggregate_and_add_to_db(db_ready_df, var_name)

    # add to proxy_metrics
    add_to_proxy_metrics(_fk_var_detail, proxy_vars)


def perform_post_calculation_and_add_data(data_df, var_name, details_dict):
    regions_df = get_regions("LAU")

    # add a match_region_code on regions_df based on spatial resolution of the data
    if details_dict["resolution"] != "NUTS3":
        n_del = del_dict[details_dict["resolution"]]
        regions_df["match_region_code"] = regions_df["parent_region_code"].str[:-n_del]
    else:
        regions_df["match_region_code"] = regions_df["parent_region_code"]

    # merge dfs
    db_ready_df = pd.merge(
        regions_df,
        data_df,
        left_on="match_region_code",
        right_on="reg_code",
        how="right",
    )

    db_ready_df.rename(columns={"id": "region_id"}, inplace=True)

    _fk_var_detail = get_primary_key("var_details", {"var_name": var_name})
    db_ready_df["var_detail_id"] = _fk_var_detail

    _fk_citation = get_primary_key(
        "citations", {"data_source_citation": details_dict["citation"]}
    )
    db_ready_df["citation_id"] = _fk_citation

    _fk_org_res = get_primary_key(
        "original_resolutions", {"original_resolution": details_dict["resolution"]}
    )
    db_ready_df["original_resolution_id"] = _fk_org_res

    _fk_disagg_method = get_primary_key(
        "disaggregation_methods",
        {"disaggregation_method": "Same value as parent region in all child regions"},
    )
    db_ready_df["disaggregation_method_id"] = _fk_disagg_method

    if "climate_experiment" in db_ready_df.columns:
        clt_expt_df = get_table(sql_cmd="SELECT * FROM climate_experiments")
        clt_expt_df.rename(columns={"id": "climate_experiment_id"}, inplace=True)

        db_ready_df = pd.merge(
            db_ready_df,
            clt_expt_df,
            left_on="climate_experiment",
            right_on="climate_experiment",
            how="left",
        )
        db_ready_df.drop(columns=["climate_experiment"], inplace=True)

    db_ready_df["chosen"] = 1

    db_ready_df.drop(
        columns=["region_code", "reg_code", "match_region_code"],
        inplace=True,
    )

    # dump LAU region data
    lau_db_df = db_ready_df.drop(
        columns=[
            "parent_region_code",
        ]
    )

    add_to_region_data(lau_db_df)

    # aggregate and dump upper level region data
    aggregate_and_add_to_db(db_ready_df, var_name)


def process_and_add_input_data(values: set) -> None:
    """Take processed and saved data, disaggregate to LAU regions and add to the database."""
    var_name, on_the_fly_calculation, proxy, post_calculation, data_year = values
    try:
        process = psutil.Process(os.getpid())
        memory_used = process.memory_info().rss / (1024 * 1024)
        waiting_count = 0
        while memory_used > 2000:
            time.sleep(10)
            db_access_with_calculations_log.info(
                f"used more than 2gb of memory, waiting since {waiting_count * 10} seconds"
            )
            memory_used = process.memory_info().rss / (1024 * 1024)
            waiting_count = waiting_count + 1

        if not isinstance(on_the_fly_calculation, str):
            # if on_the_fly_calculation is required for a var, this will be done on API side

            # get data and details dict
            if isinstance(data_year, str):
                DATA_PATH = os.path.join(PROCESSED_DATA_PATH, var_name, data_year)
            else:
                DATA_PATH = os.path.join(PROCESSED_DATA_PATH, var_name)

            data_df = pd.read_csv(
                os.path.join(DATA_PATH, "data.csv"), dtype={"reg_code": object}
            )

            with open(os.path.join(DATA_PATH, "details.json")) as f:
                details_dict = json.load(f)

            # subset only PT if working locally
            if bool(os.environ.get("MINI_DB", 0)):
                if details_dict["resolution"] == "LAU":
                    subset_df = data_df.loc[data_df["prnt_code"].str.startswith(("PT"))]
                else:
                    subset_df = data_df.loc[data_df["reg_code"].str.startswith(("PT"))]

                data_df = subset_df.copy(deep=True)

            else:
                if details_dict["resolution"] == "LAU":
                    subset_df = data_df.loc[
                        data_df["prnt_code"].str.startswith(("PL", "DE", "ES"))
                    ]
                else:
                    subset_df = data_df.loc[
                        data_df["reg_code"].str.startswith(("PL", "DE", "ES"))
                    ]

                data_df = subset_df.copy(deep=True)

            if len(data_df) > 0:
                if isinstance(data_year, str):
                    db_access_with_calculations_log.info(
                        f"currently working on {var_name} with year {data_year} ===================="
                    )
                else:
                    db_access_with_calculations_log.info(
                        f"currently working on {var_name} ===================="
                    )

                # if data is for LAU regions, directly add to DB
                if details_dict["resolution"] == "LAU":
                    process_and_add_lau_data(data_df, var_name, details_dict)

                # else perform further operations before adding to db
                else:
                    if isinstance(
                        proxy, str
                    ):  # NOTE: want to use math.isnan() or np.nan to check but this seems problematic with concurrent futures
                        disaggregate_and_add_data(
                            data_df, var_name, details_dict, proxy
                        )

                    elif isinstance(post_calculation, str):
                        if post_calculation == "same value all regions":
                            perform_post_calculation_and_add_data(
                                data_df, var_name, details_dict
                            )
                        else:
                            raise ValueError("unknown post_calculation")

                    else:
                        raise ValueError(
                            "Either the data should be at LAU resolution. or one of proxy or post_calculation should be provided"
                        )

            else:
                db_access_with_calculations_log.info(
                    f"for {var_name} data_df has 0 rows. Nothing to add!"
                )

        else:
            db_access_with_calculations_log.info(
                f"for {var_name} skipping due to on_the_fly_calculation"
            )

    except:
        db_access_with_calculations_log.error(
            f"!!!!for {var_name} job failed!!!!{traceback.format_exc()}"
        )


def process_and_add_eucalc_data(values: set) -> None:
    """Disaggregate EUCALC data and add to the database."""
    (
        var_name,
        value,
        year,
        proxy,
        post_calculation,
        country_code,
        main_pathway,
        reference,
        pathway_variant,
    ) = values

    db_access_with_calculations_log.info(
        f"currently working on {var_name} for year {year}===================="
    )
    if isinstance(proxy, str):
        if "+" in proxy:  # assuming only sum of vars or a var could be proxy
            proxy_data, proxy_vars = add_proxy_vars(proxy, country_code)
        else:
            proxy_data = get_var_data_for_disaggregation(proxy, country_code)
            proxy_vars = [proxy]

        # NOTE: it could happen that proxy is 0 in all regions which leads to "divide by 0" problem.
        # For now, these values are skipped!!
        if len(proxy_data["value"].unique()) == 1:
            if proxy_data["value"].unique().item() == 0:
                warnings.warn(
                    f"{proxy} is 0 for all regions. Using population as proxy instead"
                )
                proxy_data = get_var_data_for_disaggregation("population", country_code)
                proxy_vars = ["population"]

        db_ready_df = disaggregate_value(value, proxy_data)

    elif isinstance(post_calculation, str):
        if post_calculation == "same value all regions":

            db_ready_df = get_regions("LAU")

            db_ready_df["value"] = value

            db_ready_df = db_ready_df.rename(columns={"id": "region_id"})

        elif "+" not in post_calculation:
            raise ValueError("unknown post_calculation")

    else:
        raise ValueError("atleast one of proxy and post_calculation should be provided")

    # additional cols
    db_ready_df["year"] = year

    _fk_citation = get_primary_key(
        "citations",
        {
            "data_source_citation": "Costa L., (2022), Documentation of decarbonisation scenarios for usage in the project (LOCALISED Deliverable 2.1)"
        },
    )
    db_ready_df["citation_id"] = _fk_citation

    _fk_pathway = get_primary_key(
        "pathways",
        {
            "pathway_main": main_pathway,
            "pathway_reference": reference,
            "pathway_variant": pathway_variant,
        },
    )
    db_ready_df["pathway_id"] = _fk_pathway

    _fk_var_detail = get_primary_key("var_details", {"var_name": var_name})
    db_ready_df["var_detail_id"] = _fk_var_detail

    _fk_org_res = get_primary_key(
        "original_resolutions", {"original_resolution": "NUTS0"}
    )
    db_ready_df["original_resolution_id"] = _fk_org_res

    _fk_disagg_method = get_primary_key(
        "disaggregation_methods", {"disaggregation_method": "Using proxy metrics"}
    )
    db_ready_df["disaggregation_method_id"] = _fk_disagg_method

    db_ready_df["chosen"] = 1

    db_ready_df.drop(columns=["region_code"], inplace=True)

    # dump LAU region data
    lau_db_df = db_ready_df.drop(
        columns=[
            "parent_region_code",
        ]
    )

    add_to_region_data(lau_db_df)

    # aggregate and dump upper level region data
    aggregate_and_add_to_db(db_ready_df, var_name)

    add_to_proxy_metrics(_fk_var_detail, proxy_vars)
