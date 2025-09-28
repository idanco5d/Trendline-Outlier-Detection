import os

from dotenv import load_dotenv


load_dotenv()


class Config:
    CELERY_BROKER_URL = os.environ['REDIS_URL']
    CELERY_RESULT_BACKEND = os.environ['REDIS_URL']
    SECRET_KEY = os.environ['APP_SECRET_KEY']
    APP_NAME = os.environ['APP_NAME']