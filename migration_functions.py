import db
import db_engines


def create_model(model):
    table_name = model.table_name if model.table_name and model.table_name != '' else type(model).__name__
    table_columns = [item for item in set(list(model.__dict__) + list(model.__base__.__dict__)) if
                     not item.startswith("__")]
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


def delete_model(table_name):
    query = f"DROP TABLE {table_name}"
    cursor = db.db_connection()
    cursor.execute(query)
    cursor.connection.commit()


def add_field(model, field):
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


def delete_field(model, field_name):
    if db.DB_SETTINGS.get("DB_ENGINE") == db_engines.SQLITE3:
        _delete_field_sqlite(model, field_name)
        return
    _delete_field_other(model, field_name)


def _delete_field_sqlite(model, field_name):
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


def _delete_field_other(model, field_name):
    table_name = model.table_name if model.table_name and model.table_name != '' else type(model).__name__
    query = f"ALTER TABLE {table_name} DROP COLUMN {field_name};"
    cursor = db.db_connection()
    cursor.execute(query)
    cursor.connection.commit()


def create_table(table_name, data):
    table_columns = []
    for raw_column in data:
        column_type = data[raw_column].get("type")
        not_null = " NOT NULL" if data[raw_column].get("null") else ""
        default = f" DEFAULT \"{data[raw_column].get('default')}\"" if data[raw_column].get("default") else ""
        pk = f" PRIMARY KEY AUTOINCREMENT" if data[raw_column].get('pk') else ""
        table_columns.append(f"\"{raw_column}\" {column_type}" + not_null + default + pk)

    table_columns_query = ', '.join(table_columns)
    query = f"CREATE TABLE {table_name} ({table_columns_query})"
    cursor = db.db_connection()
    cursor.execute(query)
    cursor.connection.commit()


def add_column(table_name, column_name, data):
    column_type = data.get("type")
    not_null = " NOT NULL" if data.get("null") else ""
    default = f" DEFAULT \"{data.get('default')}\"" if data.get("default") else ""
    pk = f" PRIMARY KEY AUTOINCREMENT" if data.get('pk') else ""
    column_data = f"{column_name} {column_type}" + not_null + default + pk
    query = f"ALTER TABLE {table_name} ADD {column_data}"
    cursor = db.db_connection()
    cursor.execute(query)
    cursor.connection.commit()


def delete_table(table_name):
    query = f"DROP TABLE {table_name}"
    cursor = db.db_connection()
    cursor.execute(query)
    cursor.connection.commit()


def delete_column(table_name, column_name):
    if db.DB_SETTINGS.get("DB_ENGINE") == db_engines.SQLITE3:
        _delete_column_sqlite(table_name, column_name)
        return
    _delete_column_other(table_name, column_name)


def _delete_column_sqlite(table_name, column_name):
    cursor = db.db_connection()
    query = f"ALTER TABLE {table_name} RENAME TO {table_name}_old"
    cursor.execute(query)

    query = f"PRAGMA table_info({table_name}_old)"
    cursor.execute(query)
    result = cursor.fetchall()
    data = {}
    for column_info in result:
        data[column_info['name']] = {
            "type": column_info['type'],
            "null": column_info['notnull'] != 1,
            "default": column_info['dflt_value'],
            "pk": column_info['pk'] == 1,
        }
    data.pop(column_name)

    table_columns = []
    for raw_column in data:
        column_type = data[raw_column].get("type")
        not_null = " NOT NULL" if data[raw_column].get("null") else ""
        default = f" DEFAULT \"{data[raw_column].get('default')}\"" if data[raw_column].get("default") else ""
        pk = f" PRIMARY KEY AUTOINCREMENT" if data[raw_column].get('pk') else ""
        table_columns.append(f"\"{raw_column}\" {column_type}" + not_null + default + pk)

    table_columns_query = ', '.join(table_columns)

    query = f"CREATE TABLE {table_name} ({table_columns_query})"
    cursor = db.db_connection()
    cursor.execute(query)
    new_table_sql_fields = ', '.join(list(data.keys()))
    cursor.execute(f"INSERT INTO {table_name} SELECT {new_table_sql_fields} FROM {table_name}_old")
    cursor.execute(f"DROP TABLE {table_name}_old")
    cursor.connection.commit()


def _delete_column_other(table_name, column_name):
    query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
    cursor = db.db_connection()
    cursor.execute(query)
    cursor.connection.commit()
