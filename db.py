import inspect
import os

import exceptions
import db_engines

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_SETTINGS = {
    'DB_ENGINE': db_engines.SQLITE3,
    'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
}

db_engine = DB_SETTINGS.get('DB_ENGINE')
db_name = DB_SETTINGS.get("NAME")

if db_engine not in db_engines.db_engines_info:
    if not inspect.isclass(db_engine):
        raise exceptions.DBException(f"DB_ENGINE must be type, not instance of {type(db_engine)}")
    raise exceptions.DBException(f"DB_ENGINE must be type from list {db_engines.db_engines_info},"
                                 f" not {type(db_engine)}")

if not isinstance(db_name, str):
    raise exceptions.DBException(f"NAME must be type {type('')}, not {type(db_name)}")


def db_connection():
    if db_engine == db_engines.SQLITE3:
        import sqlite3
        connection = sqlite3.connect(db_name)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        return cursor

