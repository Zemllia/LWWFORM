from os import listdir
import importlib.util
from os.path import isfile, join

import db
import db_engines


class MigrationManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MigrationManager, cls).__new__(cls)
        return cls.instance

    def migrate(self):
        migrations_folder_path = "/home/zemlia/Документы/Projects/LiWeORM/migrations/"
        migrations_files = [f for f in listdir(migrations_folder_path) if isfile(join(migrations_folder_path, f))
                            and not f.startswith("__") and f.endswith(".py")]
        migrations_files.reverse()
        for file_name in migrations_files:
            spec = importlib.util.spec_from_file_location(file_name.replace('.py', ''),
                                                          join(migrations_folder_path, file_name))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            print("Start migrating")
            module.run_migration()
            print("Successfully migrated")

    def create_model(self, model):
        table_name = model.table_name if model.table_name and model.table_name != '' else type(model).__name__
        table_columns = [item for item in set(list(model.__dict__) + list(model.__base__.__dict__)) if not item.startswith("__")]
        table_columns.remove('pk')
        table_columns.remove("table_name")
        table_columns.remove("save")
        table_columns_pregen = []
        for item in table_columns:
            if item == "id":
                table_columns_pregen.append(model.__base__.__dict__[item].get_table_creation_parameters(item))
                continue
            table_columns_pregen.append(model.__dict__[item].get_table_creation_parameters(item))

        table_columns_query = ', '.join(table_columns_pregen)

        query = f"CREATE TABLE {table_name} ({table_columns_query})"
        cursor = db.db_connection()
        cursor.execute(query)
        cursor.connection.commit()

    def remove_model(self, table_name):
        query = f"DROP TABLE {table_name}"
        cursor = db.db_connection()
        cursor.execute(query)
        cursor.connection.commit()

    def add_field(self, model, field):
        table_name = model.table_name if model.table_name and model.table_name != '' else type(model).__name__
        table_columns = [item for item in set(list(model.__dict__) + list(model.__base__.__dict__)) if
                         not item.startswith("__")]
        table_columns.remove('pk')
        table_columns.remove("table_name")
        table_columns.remove("save")
        new_column_name = ""
        for item in table_columns:
            if item == "id":
                continue
            if model.__dict__[item] == field:
                new_column_name = item
        query = f"ALTER TABLE {table_name} ADD {field.get_table_creation_parameters(new_column_name)}"
        cursor = db.db_connection()
        cursor.execute(query)
        cursor.connection.commit()

    def remove_field(self, model, field_name):
        if db.DB_SETTINGS.get("DB_ENGINE") == db_engines.SQLITE3:
            self._remove_field_sqlite(model, field_name)
            return
        self._remove_field_other(model, field_name)

    def _remove_field_sqlite(self, model, field_name):
        table_name = model.table_name if model.table_name and model.table_name != '' else type(model).__name__
        cursor = db.db_connection()
        query = f"ALTER TABLE {table_name} RENAME TO {table_name}_old"
        cursor.execute(query)

        table_columns = [item for item in set(list(model.__dict__) + list(model.__base__.__dict__)) if
                         not item.startswith("__")]
        table_columns.remove('pk')
        table_columns.remove("table_name")
        table_columns.remove("save")
        table_columns.remove(field_name)
        table_columns_pregen = []
        for item in table_columns:
            if item == "id":
                table_columns_pregen.append(model.__base__.__dict__[item].get_table_creation_parameters(item))
                continue
            table_columns_pregen.append(model.__dict__[item].get_table_creation_parameters(item))

        table_columns_query = ', '.join(table_columns_pregen)

        query = f"CREATE TABLE {table_name} ({table_columns_query})"
        cursor = db.db_connection()
        cursor.execute(query)
        new_table_sql_fields = ', '.join(table_columns)
        cursor.execute(f"INSERT INTO {table_name} SELECT {new_table_sql_fields} FROM {table_name}_old")
        cursor.execute(f"DROP TABLE {table_name}_old")
        cursor.connection.commit()

    def _remove_field_other(self, model, field_name):
        table_name = model.table_name if model.table_name and model.table_name != '' else type(model).__name__
        query = f"ALTER TABLE {table_name} DROP COLUMN {field_name};"
        cursor = db.db_connection()
        cursor.execute(query)
        cursor.connection.commit()


if __name__ == "__main__":
    mm = MigrationManager()
    mm.migrate()

