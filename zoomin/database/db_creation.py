"""Module to create new DB."""
import os
from typing import Any
import psycopg2
from dotenv import load_dotenv, find_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from zoomin.database.db_connection import with_db_connection

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_pwd = os.environ.get("DB_PASSWORD")


def create_new_db() -> None:
    """Create a new postgres database, with postgis extension. DB details such as name, user, password are taken from the .env variables."""
    connection = psycopg2.connect(f"user={db_user} password={db_pwd}")
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connection.cursor()

    cursor.execute(f"CREATE database {db_name};")

    @with_db_connection()
    def grant_privileges(cursor: Any) -> None:
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user};")

    grant_privileges()


if __name__ == "__main__":

    create_new_db()
