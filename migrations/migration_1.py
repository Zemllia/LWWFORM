from fields import CharField, IntegerField
from migrations_manager import MigrationManager
from models import Employee


def run():
    migrations_manager = MigrationManager()
    migrations_manager.create_model(Employee)


def run_migration():
    actions = [
        run
    ]

    for action in actions:
        action()
