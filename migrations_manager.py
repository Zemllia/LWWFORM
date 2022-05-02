import datetime
import json
from os import listdir
import importlib.util
from os.path import isfile, join

import db
from models import BaseModel


class MigrationManager:
    migrations_folder_path = "/home/zemlia/Документы/Projects/LiWeORM/migrations/"

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MigrationManager, cls).__new__(cls)
        return cls.instance

    def migrate(self):
        migrations_files = [f for f in listdir(self.migrations_folder_path) if isfile(join(self.migrations_folder_path, f))
                            and not f.startswith("__") and f.endswith(".py")]
        migrations_files.reverse()
        is_something_applied = False
        print("Start migrating")
        for file_name in migrations_files:
            cursor = db.db_connection()
            cursor.execute("SELECT migration_name, is_applied FROM lwwf_migrations")
            migrations_data_raw = cursor.fetchall()
            migrations_data = []
            for row in migrations_data_raw:
                if row["is_applied"] == 1 or row["is_applied"] is True:
                    migrations_data.append(row["migration_name"])
            if file_name.replace('.py', '') in migrations_data:
                continue
            is_something_applied = True
            spec = importlib.util.spec_from_file_location(file_name.replace('.py', ''),
                                                          join(self.migrations_folder_path, file_name))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            module.run_migration()
            self._save_migration_info(file_name.replace('.py', ''))
        if is_something_applied:
            print("Successfully migrated")
        else:
            print("Everything up to date")

    def _save_migration_info(self, migration_name):
        query = f"UPDATE lwwf_migrations SET is_applied = 1 WHERE migration_name = \"{migration_name}\""
        cursor = db.db_connection()
        cursor.execute(query)
        cursor.connection.commit()

    def make_migrations(self):
        generation_time = datetime.datetime.now().strftime("%b %d %Y %H:%M:%S")
        generation_time_for_name = datetime.datetime.now().strftime("%d%m%Y%H_%M_%S")
        db_structure = self._generate_current_db_structure_from_db()
        models_structure = self._generate_current_db_structure_from_models()
        instructions = self._generate_instructions(db_structure, models_structure)
        print(f"instructions: {instructions}")
        if instructions is None or len(instructions.get("create").keys()) == 0 and len(instructions.get("delete").keys()) == 0:
            print("Everything up to date")
            return
        actions = self._generate_actions(instructions, f"automigration_{generation_time_for_name}")
        self._generate_template(actions, generation_time, generation_time_for_name)

    def _generate_current_db_structure_from_db(self):
        cursor = db.db_connection()
        query = "SELECT name FROM sqlite_master"
        cursor.execute(query)
        table_names = []
        for item in cursor.fetchall():
            table_names += [key for key in item if "sqlite_" not in str(key)]
        if "lwwf_migrations" not in table_names:
            query = "CREATE TABLE lwwf_migrations (\"id\" INTEGER, \"migration_name\" TEXT, " \
                    "\"migration_data\"	TEXT," \
                    "\"is_applied\"	INTEGER NOT NULL DEFAULT 0," \
                    "\"is_last\" INTEGER NOT NULL DEFAULT 1," \
                    " PRIMARY KEY(\"id\" AUTOINCREMENT))"
            cursor.execute(query)
            cursor.connection.commit()
        else:
            table_names.remove("lwwf_migrations")
        tables = {}
        for table_name in table_names:
            query = f"PRAGMA table_info({table_name})"
            cursor.execute(query)
            result = cursor.fetchall()
            table_columns = {}
            for column_info in result:
                table_columns[column_info['name']] = {
                    "type": column_info['type'],
                    "null": column_info['notnull'] != 1,
                    "default": column_info['dflt_value'],
                    "pk": column_info['pk'] == 1,
                }
            tables[table_name] = table_columns
        return tables

    def _generate_current_db_structure_from_models(self):
        tables = {}
        for model in BaseModel.__subclasses__():
            model_fields = dict(model.__dict__)
            model_fields.pop('__module__')
            if model_fields.get('table_name'):
                model_fields.pop('table_name')
            model_fields.pop('__doc__')
            model_fields["id"] = model.__base__.__dict__["id"]
            columns_info = {}
            for key in model_fields:
                column_info = dict(model_fields[key].__dict__)
                column_info["sql_type"] = model_fields[key].__class__.__dict__["sql_type"]
                columns_info[key] = {
                    "type": column_info['sql_type'],
                    "null": column_info['null'],
                    "default": column_info['default'],
                    "pk": column_info['pk'] if column_info.get('pk') else False,
                }
            tables[model.table_name] = columns_info
        return tables

    def _generate_instructions(self, db_structure, models_structure):
        migrations_data_query = "SELECT COUNT(id) FROM lwwf_migrations"
        cursor = db.db_connection()
        cursor.execute(migrations_data_query)
        migrations_count = cursor.fetchall()[0][0]
        if str(migrations_count) != "0":
            db_structure = self._generate_instructions_from_migrations()
        instructions = self._generate_instructions_from_db_structure(db_structure, models_structure)
        return instructions

    def _generate_instructions_from_db_structure(self, db_structure, models_structure):
        instructions = {
            "create": {

            },
            "delete": {

            }
        }
        for table_name in models_structure:
            if table_name not in db_structure:
                instructions["create"][table_name] = {"data": models_structure[table_name], "type": "table"}
            else:
                db_column_structure = db_structure[table_name]
                model_column_structure = models_structure[table_name]
                for column_name in model_column_structure:
                    if not column_name in db_column_structure:
                        instructions["create"][column_name] = {"data": model_column_structure[column_name],
                                                               "type": "column",
                                                               "table_name": table_name}
                    else:
                        db_structure[table_name].pop(column_name)
                for column_to_delete_name in db_structure[table_name]:
                    instructions["delete"][column_to_delete_name] = {"type": "column", "table_name": table_name}
                db_structure.pop(table_name)
        for table_to_delete_name in db_structure:
            instructions["delete"][table_to_delete_name] = {"type": "table"}
        migrations_data_query = "SELECT * FROM lwwf_migrations WHERE is_last = 1"
        cursor = db.db_connection()
        cursor.execute(migrations_data_query)
        migrations_data = cursor.fetchall()
        for row in migrations_data:
            if row["migration_data"] == str(instructions):
                return None

        return instructions

    # TODO check if unapplied custom migrations exists
    def _generate_instructions_from_migrations(self):
        migrations_data_query = "SELECT id, migration_data FROM lwwf_migrations"
        cursor = db.db_connection()
        cursor.execute(migrations_data_query)
        migrations_data = cursor.fetchall()
        db_structure = {}
        for raw_row in migrations_data:
            row = dict(raw_row)
            migrations_data = json.loads(row['migration_data'].replace('\'', '"').replace('False', 'false')
                                         .replace('None', 'null').replace('True', 'true'))
            create_actions = migrations_data["create"]
            delete_actions = migrations_data["delete"]
            print(delete_actions)
            print(create_actions)
            for create_action in create_actions:
                if create_actions[create_action].get("type") == "table":
                    db_structure[create_action] = create_actions[create_action]['data']
            for delete_action in delete_actions:
                if delete_actions[delete_action].get("type") == "column":
                    db_structure.pop(delete_action)
            for delete_action in delete_actions:
                if delete_actions[delete_action].get("type") == "table":
                    db_structure.pop(delete_action)
        return db_structure

    def _generate_actions(self, instructions, migration_name):
        delete_actions = instructions["delete"]
        create_actions = instructions["create"]
        query_first = []
        query_second = []
        query_third = []
        query_last = []
        for item in delete_actions.keys():
            if delete_actions[item].get("type") == "column":
                query_first.append(
                    f"""
    delete_column(\"{delete_actions[item].get("table_name")}\", \"{item}\")
                    """
                )
            elif delete_actions[item].get("type") == "table":
                query_third.append(
                    f"""
    delete_table(\"{item}\")
                    """
                )
        for item in create_actions.keys():
            if create_actions[item].get("type") == "column":
                query_second.append(
                    f"""
    add_column(\"{create_actions[item].get("table_name")}\", \"{item}\", {create_actions[item].get("data")})
                    """
                )
            elif create_actions[item].get("type") == "table":
                query_last.append(
                    f"""
    create_table(\"{item}\", {create_actions[item].get("data")})
                    """
                )

        query = f"INSERT INTO lwwf_migrations (migration_name, migration_data)" \
                f" VALUES (\"{migration_name}\", \"{str(instructions)}\")"
        cursor = db.db_connection()
        cursor.execute(query)
        query = f"UPDATE lwwf_migrations SET is_last = 0 WHERE migration_name != \"{migration_name}\""
        cursor.execute(query)
        cursor.connection.commit()

        return query_first + query_second + query_third + query_last

    def _generate_template(self, actions, generation_time, generation_time_for_name):
        raw_migration_template_file = open("templates/migration_template.txt", "r")
        raw_migration_template = raw_migration_template_file.read()
        actions_query = ''.join(actions)
        raw_migration_template = raw_migration_template.replace("_!_generation_date_!_", generation_time)
        raw_migration_template = raw_migration_template.replace("_!_imports_!_", "")
        raw_migration_template = raw_migration_template.replace("_!_actions_!_", actions_query)
        new_template = open(f"{self.migrations_folder_path}automigration_{generation_time_for_name}.py", "w")
        new_template.write(raw_migration_template)


if __name__ == "__main__":
    mm = MigrationManager()
    mm.make_migrations()
