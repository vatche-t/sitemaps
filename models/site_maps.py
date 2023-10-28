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


class SiteMap(Model):
    sitemap_url = TextField()
    loc = TextField()
    priority = FloatField()

    class Meta:
        database = ketabkala_database


SiteMap.create_table(fail_silently=True)
