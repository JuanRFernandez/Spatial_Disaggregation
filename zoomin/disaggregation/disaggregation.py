"""Disaggregation techniques."""
# from functools import reduce
from typing import Optional
import warnings

import pandas as pd

from zoomin.database.db_access import get_var_data_for_disaggregation

# def get_values_list(string, symbol):
#     str_list = string.split(symbol)
#     val_list = [values.get(var) if var in values.keys() else int(var) for var in str_list]
#     return val_list

# if '/' in equation: # yet to test this part ===================================
#     org_div_list = equation.split('/')
#     final_div_list = []
#     for item in org_div_list:
#         if '*' in item:
#             val_list = get_values_list(item, "*")
#             result = reduce(lambda x, y: x*y, val_list)
#         elif '+' in item:
#             val_list = get_values_list(item, "+")
#             result = reduce(lambda x, y: x+y, val_list)
#         elif '-' in item:
#             val_list = get_values_list(item, "-")
#             result = reduce(lambda x, y: x-y, val_list)
#         else:
#             if item in values.keys():
#                 result = values.get(item)
#             else:
#                 result = int(item)

#         final_div_list.append(result)

#     return reduce(lambda x, y: x/y, final_div_list) # ============================


def add_proxy_vars(equation: str, country: Optional[str] = "all"):
    if not "+" in equation:
        raise ValueError("unknown operation.")
    else:
        proxy_vars = equation.split("+")

        for i, var_name in enumerate(proxy_vars):
            var_data = get_var_data_for_disaggregation(var_name, country)
            if i == 0:
                result = var_data
            else:
                _result = pd.merge(
                    result,
                    var_data,
                    left_on=["region_code", "parent_region_code"],
                    right_on=["region_code", "parent_region_code"],
                    how="inner",
                )

                _result["value"] = _result[["value_x", "value_y"]].sum(axis=1)
                result = _result.drop(
                    columns=["value_x", "value_y", "region_id_x"]
                ).rename(columns={"region_id_y": "region_id"})

        return result, proxy_vars


def disaggregate_value(value: float, proxy_data: pd.DataFrame) -> pd.DataFrame:
    """Disaggregates `value` to each region in proxy data based on the proxy values."""
    disagg_data = proxy_data.copy(deep=True)

    # calculate shares
    total = disagg_data["value"].values.sum()
    if total == 0:
        warnings.warn(
            "The proxy data is 0 in all child region. Therefore, the value is being equally distributed to all child regions"
        )
        disagg_data = disagg_data.drop(["value"], axis=1)
        disagg_data["value"] = value / len(proxy_data)
    else:
        disagg_data["share"] = disagg_data["value"] / total

        # disaggregation
        disagg_data["disagg_value"] = disagg_data["share"] * value

        # clean up columns
        disagg_data = disagg_data.drop(["value", "share"], axis=1).rename(
            columns={"disagg_value": "value"}
        )

    return disagg_data
