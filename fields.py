import exceptions


class Field:
    # TODO add a pk param
    val = None

    null = False

    sql_wrapper = ""

    def get_sql_value(self):
        if self.val is None:
            return "null"
        return f"{self.sql_wrapper}{self.val}{self.sql_wrapper}"

    def __init__(self, null=False):
        if not isinstance(null, bool):
            raise exceptions.DBException("null parameter must be boolean")
        self.null = null

    def get_val(self):
        return self.val

    def set_val(self, val):
        self.validate_contain(val)
        self.val = val

    def validate_contain(self, val):
        # In parent class u can put everything
        return True


class IntegerField(Field):

    def validate_contain(self, val):
        if not self.null and val is None:
            raise exceptions.DBException("Not nullable field cant get None value")
        if val is not None:
            if not isinstance(val, int):
                raise exceptions.DBException("Value must be an integer")


class FloatField(Field):

    def validate_contain(self, val):
        if not self.null and val is None:
            raise exceptions.DBException("Not nullable field cant get None value")
        if val is not None:
            if not isinstance(val, float):
                raise exceptions.DBException("Value must be a float")


class CharField(Field):
    max_length = 255

    sql_wrapper = "\""

    def __init__(self, null=False, max_length=255):
        if not isinstance(null, bool):
            raise exceptions.DBException("null parameter must be boolean")
        if not isinstance(max_length, int):
            raise exceptions.DBException("max_length parameter must be integer")
        if max_length > 255:
            raise exceptions.DBException("max_length can't be greater than 255")
        self.null = null
        self.max_length = max_length

    def validate_contain(self, val):
        if not self.null and val is None:
            raise exceptions.DBException("Not nullable field cant get None value")
        if val is not None:
            if not isinstance(val, str):
                raise exceptions.DBException("Value must be a string")
            if len(val) > self.max_length:
                raise exceptions.DBException("String length can't be greater than max_length parameter")