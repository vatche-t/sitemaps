import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv


parent_path = Path(__file__).parent.absolute()

load_dotenv(f'{parent_path}/.env')


POSTGRES = os.environ["POSTGRES"]
POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGRES_DATABASE = "sitemaps"

COOKIES = os.environ["COOKIES"]

MONGODB_URI = os.environ["MONGODB_URI"]

MAX_PROCESS_LIFE_TIME = os.environ["MAX_PROCESS_LIFE_TIME"]
PROCESS_RESTART_DELAY_TIME = os.environ["PROCESS_RESTART_DELAY_TIME"]

