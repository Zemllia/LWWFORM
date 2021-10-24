import exceptions
import fields
from exceptions import QuerySetException
from fields import Field


class BaseQuerySet:
    def __init__(self, model_class, existing_result):
        self.existing_result = []
        db_fields = [item for item in existing_result]
        db_fields = [item for item in db_fields[0].keys()]
        model_fields = [item for item in model_class.__dict__.keys() if isinstance(model_class.__dict__[item], Field)]
        model_fields.append("id")

        for item in model_fields:
            if item not in db_fields:
                raise exceptions.DBException(f"Field <{item}> does not exists in database")

        for row in existing_result:
            model_instance = model_class()
            for key in row.keys():
                model_instance.__setattr__(str(key), row[key])
            self.existing_result.append(model_instance)

    def __getitem__(self, i):
        return self.existing_result[i]

    def __len__(self):
        return len(self.existing_result)

    def __str__(self):
        return str(self.existing_result)

    def all(self):
        return self

    def filter(self, **fields):
        for item in self.existing_result:
            for key in fields:
                if key not in item.__dict__.keys():
                    raise QuerySetException(f"Field <{key}> does not exists in model {item.__class__.__name__}")
                if item.__dict__.get(key) != fields[key]:
                    self.existing_result.remove(item)
                    break
        return self

    def exclude(self, **fields):
        for item in self.existing_result:
            for key in fields:
                if key not in item.__dict__.keys():
                    raise QuerySetException(f"Field <{key}> does not exists in model {item.__class__.__name__}")
                if item.__dict__.get(key) == fields[key]:
                    self.existing_result.remove(item)
                    break
        return self

