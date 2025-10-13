from celery import Celery
from config import Config

def create_celery_app():
    app = Celery(Config.APP_NAME, include=['algorithm.run'])
    app.conf.update(
        broker_url=Config.CELERY_BROKER_URL,
        result_backend=Config.CELERY_RESULT_BACKEND,
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
    )
    return app

my_celery_app = create_celery_app()