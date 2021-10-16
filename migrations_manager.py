import datetime
from os import listdir
import importlib.util
from os.path import isfile, join

import db
from models import BaseModel


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

    def make_migrations(self):
        db_structure = self._generate_current_db_structure_from_db()
        models_structure = self._generate_current_db_structure_from_models()
        instructions = self._generate_instructions(db_structure, models_structure)
        if len(instructions.get("create").keys()) == 0 and len(instructions.get("delete").keys()) == 0:
            print("Everything up to date")
            return
        actions = self._generate_actions(instructions)
        self._generate_template(actions)

    def _generate_current_db_structure_from_db(self):
        cursor = db.db_connection()
        query = "SELECT name FROM sqlite_master"
        cursor.execute(query)
        table_names = []
        for item in cursor.fetchall():
            table_names += [key for key in item if "sqlite_" not in str(key)]
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
        return instructions

    def _generate_actions(self, instructions):
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
        return query_first + query_second + query_third + query_last

    def _generate_template(self, actions):
        raw_migration_template_file = open("templates/migration_template.txt", "r")
        raw_migration_template = raw_migration_template_file.read()
        actions_query = ''.join(actions)
        generation_time = datetime.datetime.now().strftime("%b %d %Y %H:%M:%S")
        generation_time_for_name = datetime.datetime.now().strftime("%d%m%Y%H_%M_%S")
        raw_migration_template = raw_migration_template.replace("_!_generation_date_!_", generation_time)
        raw_migration_template = raw_migration_template.replace("_!_imports_!_", "")
        raw_migration_template = raw_migration_template.replace("_!_actions_!_", actions_query)
        new_template = open(f"migrations/automigration_{generation_time_for_name}.py", "w")
        new_template.write(raw_migration_template)


if __name__ == "__main__":
    mm = MigrationManager()
    mm.make_migrations()
