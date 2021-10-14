import db
import exceptions
import querysets
from exceptions import QuerySetException


class BaseManager:

    def __init__(self, model_class):
        self.model_class = model_class

    def all(self):
        query = f"SELECT * FROM {self.model_class.table_name}"
        cursor = db.db_connection()
        cursor.execute(query)
        result = cursor.fetchall()
        qs = querysets.BaseQuerySet(self.model_class, result)
        return qs

    def filter(self, **kwargs):
        for key in kwargs:
            if key not in dir(self.model_class):
                raise QuerySetException(f"Field <{key}> does not exists in model {self.model_class.__name__}")
        query_args = ' AND '.join(
            [f"{item}={kwargs[item]}" if not isinstance(kwargs[item], str) else f"{item}='{kwargs[item]}'" for item in
             kwargs])
        query = f"SELECT * FROM {self.model_class.table_name} WHERE {query_args};"
        print(query)
        cursor = db.db_connection()
        cursor.execute(query)
        result = cursor.fetchall()
        qs = querysets.BaseQuerySet(self.model_class, result)
        return qs

    def exclude(self, **kwargs):
        for key in kwargs:
            if key not in dir(self.model_class):
                raise QuerySetException(f"Field <{key}> does not exists in model {self.model_class.__name__}")
        query_args = ' OR '.join(
            [f"{item}!={kwargs[item]}" if not isinstance(kwargs[item], str) else f"{item}!='{kwargs[item]}'" for item in
             kwargs])
        query = f"SELECT * FROM {self.model_class.table_name} WHERE {query_args};"
        print(query)
        cursor = db.db_connection()
        cursor.execute(query)
        result = cursor.fetchall()
        qs = querysets.BaseQuerySet(self.model_class, result)
        return qs

    def get(self, **kwargs):
        for key in kwargs:
            if key not in dir(self.model_class):
                raise QuerySetException(f"Field <{key}> does not exists in model {self.model_class.__name__}")

        query_args = ' AND '.join(
            [f"{item}={kwargs[item]}" if not isinstance(kwargs[item], str) else f"{item}='{kwargs[item]}'" for item in
             kwargs])
        query = f"SELECT * FROM {self.model_class.table_name} WHERE {query_args};"
        cursor = db.db_connection()
        cursor.execute(query)
        result = cursor.fetchall()
        qs = querysets.BaseQuerySet(self.model_class, result)
        return qs[0] if len(qs) != 0 else None


class MetaModel(type):
    manager_class = BaseManager

    def _get_manager(cls):
        return cls.manager_class(model_class=cls)

    @property
    def objects(cls):
        return cls._get_manager()


class Field:
    # TODO add a pk param
    val = None

    null = False

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


class BaseModel(metaclass=MetaModel):
    table_name = ""
    id = IntegerField(null=False)

    @property
    def pk(self):
        return self.id

    def __init__(self):
        self.id = IntegerField(null=False)
        self.id = 0

    # TODO understand if we need this
    # def __new__(cls, *args, **kwargs):
    #     print("----------")
    #     rv = super().__new__(cls, *args, **kwargs)
    #     for field in rv.__class__.__dict__:
    #         field_value = rv.__class__.__dict__.get(field)
    #         if isinstance(field_value, Field):
    #             swap_class = field_value.__class__()
    #             print(swap_class)
    #             for item in field_value.__dict__:
    #                 print(item)
    #     print("----------")
    #     return rv

    def __getattribute__(self, item):
        if object.__getattribute__(self, item).__class__.__base__ == Field:
            return object.__getattribute__(self, item).get_val()
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        if self.__class__.__dict__.get(key).__class__.__base__ == Field:
            swap_class = self.__class__.__dict__.get(key).__class__()
            for item in self.__class__.__dict__.get(key).__dict__:
                swap_class.__dict__[item] = self.__class__.__dict__.get(key).__dict__[item]
            super(BaseModel, self).__setattr__(key, swap_class)
            swap_class.set_val(value)
            return
        super(BaseModel, self).__setattr__(key, value)


class Employee(BaseModel):
    table_name = "stocks"
    date = CharField(null=True, max_length=255)
    trans = CharField(null=False, max_length=100)
    symbol = CharField(null=False, max_length=100)
    qty = FloatField(null=False)
    price = FloatField(null=False)


# teste = Employee()
# teste1 = Employee()
# teste.late = "e"
# teste1.late = "g"
# print(f"Object: {teste.late}, value: {teste.late}")
# print(f"Object: {teste1.late}, value: {teste1.late}")


# print(teste.late.val)
# teste.late = "1"
# print(teste.late.val)


cur_employee = Employee.objects.exclude(id=0, trans='3')[0]
print(cur_employee.__dict__)
