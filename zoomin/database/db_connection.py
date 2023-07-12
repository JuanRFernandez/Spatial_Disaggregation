"""Module to help connect to DB."""
import os
import logging
from typing import Any, Callable
from functools import wraps
import psycopg2
from dotenv import load_dotenv, find_dotenv

db_connection_log = logging.getLogger("db_connection")

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_pwd = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")


def with_db_connection() -> Any:
    """Wrap a set up-tear down Postgres connection while providing a cursor object to make queries with."""

    def wrap(func_call: Callable) -> Any:
        @wraps(func_call)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                # Setup postgres connection
                conn_info = {
                    "database": db_name,
                    "user": db_user,
                    "password": db_pwd,
                    "host": db_host,
                    "port": db_port,
                }

                connection = psycopg2.connect(**conn_info)
                cursor = connection.cursor()

                # Call function passing in cursor
                return_val = func_call(cursor, *args, **kwargs)

                # close communication with the PostgreSQL database server
                cursor.close()

                # commit the changes
                connection.commit()

                # Close connection
                if connection is not None:
                    connection.close()

                # return value
                return return_val

            except psycopg2.DatabaseError as error:
                db_connection_log.error(error)

                # Close connection
                if connection is not None:
                    connection.close()

                return None

        return wrapper

    return wrap
