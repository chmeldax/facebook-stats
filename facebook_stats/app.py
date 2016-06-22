from celery import Celery

app = Celery('tasks', broker='redis://localhost:6379/0')
app.config_from_object('facebook_stats.celeryconfig')
