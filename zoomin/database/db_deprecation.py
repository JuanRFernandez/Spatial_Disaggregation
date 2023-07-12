"""Module to deprecate DB."""

import os
import warnings
import psycopg2
from dotenv import find_dotenv, load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from zoomin.database.db_deletion import delete_db

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_pwd = os.environ.get("DB_PASSWORD")


def deprecate_db() -> None:
    """Deprecate existing postgres database by appending its name with '_old'."""
    connection = psycopg2.connect(f"user={db_user} password={db_pwd}")
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connection.cursor()

    cursor.execute("SELECT datname FROM pg_database")
    database_list = cursor.fetchall()

    # check if the DB exists
    if (db_name,) not in database_list:
        warnings.warn("Nothing to deprecate because database does not exist")

    else:
        # check if a previously deprecated DB exists and delete it
        deprecate_db_name = f"{db_name}_old"
        if (deprecate_db_name,) in database_list:
            warnings.warn(
                "A previously deprecated database exists. This will be deleted!"
            )

            delete_db(deprecate_db_name)

        # disconnect and rename the db
        commands = (
            f"SELECT pg_terminate_backend(pg_stat_activity.pid) \
                        FROM pg_stat_activity\
                        WHERE pg_stat_activity.datname = '{db_name}';",
            f"ALTER DATABASE {db_name} RENAME TO {deprecate_db_name};",
        )

        for command in commands:
            cursor.execute(command)


if __name__ == "__main__":

    deprecate_db()
