from peewee import *
from playhouse.postgres_ext import ArrayField
from datetime import datetime

import config

ketabkala_database = PostgresqlDatabase(
    config.POSTGRES_DATABASE,
    user=config.POSTGRES_USERNAME,
    password=config.POSTGRES_PASSWORD,
    host=config.POSTGRES_HOST,
    port=config.POSTGRES_PORT,
)


class TasksErrors_BestSeller(Model):
    status = IntegerField()
    error_message = TextField()
    url = TextField()
    page = IntegerField()
    created_at = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = "taskserrors_PassiveOrders"
        database = ketabkala_database


class Tasks_Bestseller(Model):
    status = IntegerField()
    message = TextField()
    url = TextField()
    page = IntegerField()
    created_at = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = "tasks_PassiveOrders"
        database = ketabkala_database


TasksErrors_BestSeller.create_table(fail_silently=True)
Tasks_Bestseller.create_table(fail_silently=True)
