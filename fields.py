import exceptions


class Field:
    # TODO add a pk param
    val = None

    null = False

    pk = False

    unique = False

    default = None

    sql_wrapper = ""
    sql_type = ""

    python_type = None

    def get_sql_value(self):
        if self.val is None:
            return "null"
        return f"{self.sql_wrapper}{self.val}{self.sql_wrapper}"

    def get_table_creation_parameters(self, field_name):
        query_parameter = f"\"{field_name}\" {self.sql_type}"
        if not self.null:
            query_parameter += " NOT NULL"
        if self.unique:
            query_parameter += " UNIQUE"
        if self.default:
            query_parameter += f" DEFAULT {self.sql_wrapper}{self.default}{self.sql_wrapper}"
        if self.pk:
            query_parameter += f" PRIMARY KEY AUTOINCREMENT"
        return query_parameter

    def __init__(self, null=False, pk=False, unique=False, default=None):
        if not isinstance(null, bool):
            raise exceptions.DBException("null parameter must be boolean")
        if not isinstance(pk, bool):
            raise exceptions.DBException("pk parameter must be boolean")
        if not isinstance(unique, bool):
            raise exceptions.DBException("unique parameter must be boolean")
        if not isinstance(default, self.python_type) and default is not None:
            raise exceptions.DBException(f"default parameter must be type {str(self.python_type)}")
        self.null = null
        self.pk = pk
        self.unique = unique
        self.default = default

    def get_val(self):
        return self.val

    def set_val(self, val):
        self.validate_contain(val)
        self.val = val

    def validate_contain(self, val):
        # In parent class u can put everything
        return True


class IntegerField(Field):
    sql_type = "INTEGER"
    python_type = int

    def validate_contain(self, val):
        if not self.pk and not self.null and val is None:
            raise exceptions.DBException("Not nullable field cant get None value")
        if val is not None:
            if not isinstance(val, int):
                raise exceptions.DBException("Value must be an integer")


class FloatField(Field):
    sql_type = "real"
    python_type = float

    def validate_contain(self, val):
        if not self.pk and not self.null and val is None:
            raise exceptions.DBException("Not nullable field cant get None value")
        if val is not None:
            if not isinstance(val, float):
                raise exceptions.DBException("Value must be a float")


class CharField(Field):
    sql_type = "text"

    max_length = 255

    sql_wrapper = "\""
    python_type = str

    def __init__(self, null=False, max_length=255, default=None):
        if not isinstance(null, bool):
            raise exceptions.DBException("null parameter must be boolean")
        if not isinstance(max_length, int):
            raise exceptions.DBException("max_length parameter must be integer")
        if max_length > 255:
            raise exceptions.DBException("max_length can't be greater than 255")
        if not (isinstance(default, self.python_type)) and default is not None:
            raise exceptions.DBException(f"default parameter must be type {str(self.python_type)}")
        if default and len(default) > self.max_length:
            raise exceptions.DBException("Default string length can't be greater than max_length parameter")
        self.null = null
        self.max_length = max_length
        self.default = default

    def validate_contain(self, val):
        if not self.pk and not self.null and val is None:
            raise exceptions.DBException("Not nullable field cant get None value")
        if val is not None:
            if not isinstance(val, str):
                raise exceptions.DBException("Value must be a string")
            if len(val) > self.max_length:
                raise exceptions.DBException("String length can't be greater than max_length parameter")