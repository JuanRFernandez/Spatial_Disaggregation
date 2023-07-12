"""Functions to help populate DB with data."""
import logging

import os
from typing import Any, Iterable, Optional
import csv
from io import StringIO

import numpy as np
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
from sqlalchemy import create_engine

from zoomin.database.db_connection import with_db_connection
from zoomin.gen_utils import measure_time, measure_memory_leak

db_access_log = logging.getLogger("db_access")
logging.basicConfig(level=logging.INFO)

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_pwd = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")


def get_db_uri() -> str:
    """Return db uri."""
    return f"postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"


def get_db_engine() -> Any:
    """Set up database connection engine."""
    db_uri = get_db_uri()
    engine = create_engine(db_uri, pool_pre_ping=True)

    return engine


# ===================================================================================================
# GET functions
# ===================================================================================================


@with_db_connection()
def get_col_values(
    cursor: Any, table: str, col: str, cols_criteria: Optional[dict] = None
) -> Any:
    """Return all `col` values or a subset corresponding to other column values in a table."""
    sql_cmd = f"SELECT {col} FROM {table}"

    if cols_criteria is not None:
        where_clause = " AND ".join(
            [
                f"{key}='{val}'" if isinstance(val, str) else f"{key}={val}"
                for (key, val) in cols_criteria.items()
            ]
        )

        sql_cmd = f"{sql_cmd} WHERE {where_clause}"

    cursor.execute(sql_cmd)

    result = cursor.fetchall()

    if result == []:
        raise ValueError("the value/values do not exist in the DB")

    # return a list of values if there is more than 1 unique
    # value, else just the unique value
    result_list = [res[0] for res in result]
    if len(np.unique(result_list)) == 1:
        return result_list[0]

    return result_list


@with_db_connection()
def get_table(cursor: Any, sql_cmd: str) -> pd.DataFrame:
    """Return a table as dataframe based on the sql_cmd."""
    engine = get_db_engine()
    engine_conn = engine.connect()

    table_df = pd.read_sql(sql=sql_cmd, con=engine_conn)
    # TODO: find out what is the diff between read_sql and read_sql_query. Can get_table() be used instead of get_region_data() and get_eucalc_pathway_data()???

    return table_df


def get_primary_key(table: str, cols_criteria: dict) -> Any:
    """Return primary key/keys corresponding to other column values in a table."""
    col_vals = get_col_values(table, "id", cols_criteria)

    if not isinstance(col_vals, int):
        raise ValueError("many primary keys returned.")
    return col_vals


@with_db_connection()
def get_regions(
    cursor: Any,
    resolution: str,
    country: Optional[str] = "all",
) -> pd.DataFrame:
    """Return dataframe of region codes and their primary keys corresponding to the specified resolution from the DB."""
    # Construct sql command
    sql_cmd = f"SELECT id, region_code, parent_region_code FROM regions WHERE resolution='{resolution}'"

    # subset on a country
    if country != "all":
        if resolution == "LAU":  # NOTE: LAU regions ids do not contain country codes
            sql_cmd = f"{sql_cmd} AND parent_region_code LIKE '{country}%%'"
        else:
            sql_cmd = f"{sql_cmd} AND region_code LIKE '{country}%%'"

    # get table
    regions_df = get_table(sql_cmd=sql_cmd)

    return regions_df


@with_db_connection()
def get_var_data_for_disaggregation(
    cursor: Any, var_name: str, country: Optional[str] = "all"
) -> pd.DataFrame:
    """Return dataframe from region_data table."""
    engine = get_db_engine()

    _fk_var_name = get_primary_key("var_details", {"var_name": var_name})
    data_df = pd.read_sql_query(
        f"SELECT region_id, value FROM region_data \
            WHERE var_detail_id={_fk_var_name}",
        con=engine,
    )

    regions_df = get_regions("LAU", country=country)

    final_df = pd.merge(
        data_df, regions_df, left_on="region_id", right_on="id", how="inner"
    )
    final_df.drop(columns=["id"], inplace=True)

    return final_df


@with_db_connection()
def get_var_data_for_eucalc_post_calculation(
    cursor: Any, var_name: str
) -> pd.DataFrame:
    """Return dataframe from region_data table."""
    engine = get_db_engine()

    _fk_var_name = get_primary_key("var_details", {"var_name": var_name})
    data_df = pd.read_sql_query(
        f"SELECT region_id, pathway_id, year, value FROM region_data \
            WHERE var_detail_id={_fk_var_name}",
        con=engine,
    )

    return data_df


@with_db_connection()
def get_var_data_for_kpi_post_calculation(cursor: Any, var_name: str) -> pd.DataFrame:
    """Return dataframe from region_data table."""
    engine = get_db_engine()

    _fk_var_name = get_primary_key("var_details", {"var_name": var_name})
    data_df = pd.read_sql_query(
        f"SELECT region_id, value, year, pathway_id FROM region_data \
            WHERE var_detail_id={_fk_var_name}",
        con=engine,
    )

    return data_df


@with_db_connection()
def get_region_data(  # TODO: rename this, check if its being used anywhere
    cursor: Any, var_name: str, resolution: str, country: str
) -> pd.DataFrame:
    """Return dataframe from region_data table."""
    engine = get_db_engine()

    _fk_var_name = get_primary_key("var_details", {"var_name": var_name})

    regions_df = get_regions(resolution, country=country)
    _fk_region_ids = tuple(regions_df["id"].values)

    data_df = pd.read_sql_query(
        f"SELECT region_id, value FROM region_data \
            WHERE var_detail_id={_fk_var_name} \
            AND region_id IN {_fk_region_ids}",
        con=engine,
    )

    return data_df


def get_proxy_vars(var_name: str, country: str) -> list:
    """Return list of proxies corresponding to a variable and country."""
    _fk_var_name = get_primary_key("var_details", {"var_name": var_name})
    _fk_country_code = get_primary_key("regions", {"region_code": country})

    try:
        proxy_vars_fk = get_col_values(
            "proxy_metrics",
            "proxy_var_detail_id",
            {"var_detail_id": _fk_var_name, "region_id": _fk_country_code},
        )

        if isinstance(proxy_vars_fk, list):
            proxy_var_list = []
            for proxy_var_fk in proxy_vars_fk:
                proxy_var = get_col_values(
                    "var_details", "var_name", {"id": proxy_var_fk}
                )
                proxy_var_list.append(proxy_var)

            return proxy_var_list
        else:
            proxy_var = get_col_values("var_details", "var_name", {"id": proxy_vars_fk})
            return [proxy_var]

    except ValueError:
        return None


@with_db_connection()
def get_on_the_fly_calculation_vars(
    cursor: Any,
) -> pd.DataFrame:
    """Return dataframe from var_details table where vars have on_the_fly_calculation."""
    engine = get_db_engine()

    data_df = pd.read_sql_query(
        f"SELECT var_name, var_description, var_unit, on_the_fly_calculation FROM var_details \
            WHERE on_the_fly_calculation IS NOT NULL",
        con=engine,
    )

    # TODO: add tags to data_df in the correct format

    return data_df


@with_db_connection()
def get_eucalc_pathway_data(
    cursor: Any, _fk_pathway: Optional[int] = None
) -> pd.DataFrame:
    """Return dataframe of eucalc pathway data from region_data table."""
    engine = get_db_engine()
    if _fk_pathway is None:
        data_df = pd.read_sql_query(
            "SELECT var_detail_id, value, year, region_id FROM region_data",
            con=engine,
        )
    else:
        data_df = pd.read_sql_query(
            f"SELECT var_detail_id, value, year, region_id FROM region_data WHERE pathway_id={_fk_pathway}",
            con=engine,
        )

    return data_df


# ===================================================================================================
# POST functions
# ===================================================================================================


def _psql_insert_copy(table: Any, conn: Any, keys: list, data_iter: Iterable) -> None:
    """Execute SQL statement inserting data.

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
        Database table
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        Database connection
    keys : list of str
        Column names
    data_iter : Iterable
        Iterable that iterates the values to be inserted

    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(keys)
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)


@with_db_connection()
def add_col_values(
    cursor: Any,
    table: str,
    col_values: dict,
    cols_criteria: Optional[dict] = None,
) -> None:
    """Post `col_values` corresponding to other column values in a table."""
    values = ", ".join(
        [
            f"{key}='{val}'" if isinstance(val, str) else f"{key}={val}"
            for key, val in col_values.items()
        ]
    )
    sql_cmd = f"UPDATE {table} SET {values}"

    if cols_criteria is not None:
        where_clause = " AND ".join(
            [
                f"{key} IN {val}" if len(val) > 1 else f"{key}={val[0]}"
                for key, val in cols_criteria.items()
            ]
        )

        sql_cmd = f"{sql_cmd} WHERE {where_clause}"

    cursor.execute(sql_cmd)


@with_db_connection()
def add_to_citations(
    cursor: Any,
    source_name: str,
    source_link: str,
    source_citation: str,
) -> None:
    """Add citation to DB if not already present."""
    cursor.execute(
        f"SELECT id FROM citations WHERE data_source_citation='{source_citation}'"
    )

    if cursor.fetchone() is None:
        cursor.execute(
            f"INSERT INTO citations (data_source_name, data_source_link, data_source_citation) \
                        VALUES ('{source_name}', '{source_link}', '{source_citation}')"
        )
    else:
        db_access_log.info("citation already in DB!")


@measure_time
@measure_memory_leak
@with_db_connection()
def add_to_proxy_metrics(cursor: Any, var_detail_id: int, proxy_vars: list) -> None:
    """Add proxy metrics of data."""
    cursor.execute(f"SELECT id FROM proxy_metrics WHERE var_detail_id={var_detail_id}")

    if cursor.fetchone() is None:
        regions_df = get_regions("NUTS0")
        _fk_var_df = pd.DataFrame(
            regions_df["id"].values, columns=["region_id"]
        )  # INFO: proxy data added for each country. currently assuming same proxy in all countries. this will change in the futute
        _fk_var_df["var_detail_id"] = var_detail_id

        # get var_detail_ids
        _fk_var_detail_list = []
        for var_name in proxy_vars:
            _fk_var_detail = get_primary_key("var_details", {"var_name": var_name})
            _fk_var_detail_list.append(_fk_var_detail)

        final_df_list = []
        for _fk_var_detail in _fk_var_detail_list:
            _df = _fk_var_df.copy(deep=True)
            _df["proxy_var_detail_id"] = _fk_var_detail
            final_df_list.append(_df)

        final_df = pd.concat(final_df_list)

        engine = get_db_engine()

        if len(final_df) > 10000:
            db_uri = get_db_uri()

            ddf = dd.from_pandas(final_df, npartitions=100)

            ddf.to_sql(
                name="proxy_metrics",
                uri=db_uri,
                index=False,
                if_exists="append",
                parallel=True,
            )

        # for smaller datasets make a normal entry
        else:
            engine = get_db_engine()
            final_df.to_sql(
                "proxy_metrics",
                engine,
                index=False,
                if_exists="append",
                method=_psql_insert_copy,
            )


@measure_time
@measure_memory_leak
def add_to_region_data(db_ready_df: pd.DataFrame) -> None:
    """Add the data to region_data table."""
    if len(db_ready_df) > 10000:
        db_uri = get_db_uri()

        ddf = dd.from_pandas(db_ready_df, npartitions=100)

        ddf.to_sql(
            name="region_data",
            uri=db_uri,
            index=False,
            if_exists="append",
            parallel=True,
        )

    # for smaller datasets make a normal entry
    else:
        engine = get_db_engine()
        db_ready_df.to_sql(
            "region_data",
            engine,
            index=False,
            if_exists="append",
            method=_psql_insert_copy,
        )


@with_db_connection()
def add_to_pathways(
    cursor: Any, pathway_main: str, pathway_reference: str, pathway_variant: str
) -> None:
    """Add pathway to the database if not already present."""

    cursor.execute(
        f"SELECT id FROM pathways WHERE pathway_main='{pathway_main}' AND pathway_variant='{pathway_variant}'"
    )

    if cursor.fetchone() is None:
        cursor.execute(
            f"INSERT INTO pathways (pathway_main, pathway_reference, pathway_variant) \
                VALUES ('{pathway_main}', '{pathway_reference}', '{pathway_variant}')"
        )

    else:
        print("pathway already in DB!")
