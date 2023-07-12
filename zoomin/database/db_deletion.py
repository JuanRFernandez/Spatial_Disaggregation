"""Module to delete DB."""
import os
from typing import Optional
import warnings
import psycopg2
from dotenv import find_dotenv, load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

db_user = os.environ.get("DB_USER")
db_pwd = os.environ.get("DB_PASSWORD")


def delete_db(db_name: Optional[str] = None) -> None:
    """Delete a postgres database."""
    if db_name is None:
        db_name = os.environ.get("DB_NAME")

    connection = psycopg2.connect(f"user={db_user} password={db_pwd}")
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connection.cursor()

    # check if the DB exists
    cursor.execute("SELECT datname FROM pg_database")

    database_list = cursor.fetchall()
    if (db_name,) not in database_list:
        warnings.warn("Nothing to delete because database does not exist")

    else:
        # disconnect and delete the db and then delete the db
        commands = (
            f"REVOKE CONNECT ON DATABASE {db_name} FROM public;",
            f"SELECT pg_terminate_backend(pg_stat_activity.pid) \
                        FROM pg_stat_activity\
                        WHERE pg_stat_activity.datname = '{db_name}';",
            f"DROP database {db_name};",
        )

        for command in commands:
            cursor.execute(command)


if __name__ == "__main__":

    delete_db(db_name=None)
