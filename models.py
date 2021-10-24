import db
import fields
import querysets
from exceptions import QuerySetException
from fields import IntegerField, Field


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
        query = f"SELECT * FROM {self.model_class.table_name} WHERE {query_args};".replace("None", "null")
        cursor = db.db_connection()
        cursor.execute(query)
        result = cursor.fetchone()
        if result is None:
            return None
        model_instance = self.model_class()
        for key in result.keys():
            model_instance.__setattr__(str(key), result[key])
        return model_instance


class MetaModel(type):
    manager_class = BaseManager

    def _get_manager(cls):
        return cls.manager_class(model_class=cls)

    @property
    def objects(cls):
        return cls._get_manager()


class BaseModel(metaclass=MetaModel):
    table_name = None
    id = IntegerField(null=False, pk=True, unique=True, default=1)

    @property
    def pk(self):
        return self.id

    def save(self):
        cursor = db.db_connection()
        db_dict = [item for item in self.__dir__() if isinstance(self.__dict__.get(item), Field) or item == "id"]
        is_new = False if "id" in db_dict else True
        if not is_new:
            if self.__dict__.get("id") is not None:
                cur_instance = self.__class__.objects.get(id=self.__getattribute__("id"))
                if cur_instance is None or cur_instance.id == 0:
                    is_new = True

        if is_new:
            columns = ", ".join(db_dict)
            values_dict = []
            for item in db_dict:
                values_dict.append(self.__dict__.get(item).get_sql_value())
            values = ", ".join(str(item) if item else "null" for item in values_dict)
            query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"

        else:
            cur_instance_id = self.__getattribute__("id")
            values_list = []
            for item in db_dict:
                if item == "id":
                    continue
                values_list.append(f"{item} = {self.__dict__.get(item).get_sql_value()}")
            values = ', '.join(values_list)
            query = f"UPDATE {self.table_name} SET {values} WHERE id={cur_instance_id}"

        cursor.execute(query)
        cursor.connection.commit()

    def remove(self):
        cursor = db.db_connection()
        if self.__dict__.get("id") is not None:
            query = f"DELETE FROM {self.table_name} WHERE id={self.__getattribute__('id')}"
            cursor.execute(query)
            cursor.connection.commit()
        del self

    def __init__(self):
        self.id = None

    def __new__(cls, *args, **kwargs):
        rv = super().__new__(cls, *args, **kwargs)
        for field in rv.__dir__():
            field_value = rv.__class__.__dict__.get(field)
            if isinstance(field_value, Field) or field == "id":
                if field == "id":
                    rv.__setattr__(field, 1)
                    continue
                swap_class = field_value.__class__()
                for item in field_value.__dict__.keys():
                    swap_class.__setattr__(item, field_value.__dict__[item])
                rv.__dict__[field] = swap_class
        return rv

    def __getattribute__(self, item):
        if object.__getattribute__(self, item).__class__.__base__ == Field:
            return object.__getattribute__(self, item).get_val()
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        if self.__class__.__dict__.get(key).__class__.__base__ == Field or key == "id":
            main_class = self.__class__.__dict__.get(key)
            main_class = main_class if main_class else self.__class__.__base__.__dict__.get(key)
            swap_class = self.__class__.__dict__.get(key).__class__()
            swap_class = swap_class if swap_class else self.__class__.__base__.__dict__.get(key).__class__()
            for item in main_class.__dict__:
                swap_class.__dict__[item] = main_class.__dict__[item]
            super(BaseModel, self).__setattr__(key, swap_class)
            swap_class.set_val(value)
            return
        super(BaseModel, self).__setattr__(key, value)


class User(BaseModel):
    table_name = 'user'
    first_name = fields.CharField(null=False)
    last_name = fields.CharField(null=False)
    patronymic = fields.CharField(null=False)
    # age = IntegerField(null=False)



if __name__ == "__main__":
    print(BaseModel.__subclasses__())
