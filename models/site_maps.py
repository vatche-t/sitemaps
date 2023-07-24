from peewee import *
from playhouse.postgres_ext import ArrayField

import config

ketabkala_database = PostgresqlDatabase(
    config.POSTGRES_DATABASE,
    user=config.POSTGRES_USERNAME,
    password=config.POSTGRES_PASSWORD,
    host=config.POSTGRES_HOST,
    port=config.POSTGRES_PORT,
)


class SiteMapsDigikala(Model):
    loc = TextField()
    changefreq = TextField()
    priority = FloatField()
    class Meta:
        database = ketabkala_database


# Create the tables in the database
SiteMapsDigikala.create_table(fail_silently=True)
