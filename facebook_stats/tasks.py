import redis
import re
from celery.signals import worker_process_init, worker_process_shutdown, task_postrun
from svg.charts import time_series

from facebook_stats.app import app
from facebook_stats.comments import Comments


redis_conn = None
comments = None
dates = None

REDIS_KEY_PATTERN = 'facebook-comments-{key}-' + str(app.conf['UUID'])

@worker_process_init.connect
def init_worker(**kwargs):
    global redis_conn
    global comments
    redis_conn = redis.Redis(decode_responses=True)
    comments = Comments(redis_conn, None, REDIS_KEY_PATTERN)


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global redis_conn
    if redis_conn:
        redis_conn.disconnect()


def before_task():
    global redis_conn
    print("before")
    workers_count_key = REDIS_KEY_PATTERN.format(key='workers-count')
    redis_conn.incr(workers_count_key)


@task_postrun.connect
def after_task(**kwargs):
    global redis_conn
    print('after')
    workers_count_key = REDIS_KEY_PATTERN.format(key='workers-count')
    redis_conn.decr(workers_count_key)
    if int(redis_conn.get(workers_count_key)) == 0:
        summarize()


def summarize():
    global redis_conn
    global dates
    dates = {}
    for key in redis_conn.keys(REDIS_KEY_PATTERN.format(key='date-*')):
        date_key = _parse_date(key)
        dates[date_key] = int(redis_conn.get(key))
        redis_conn.delete(key)
    print(dates)
    _print(dates)


def _print(dates):
    graph = time_series.Plot({})
    graph.timescale_divisions = '1 day'
    graph.stagger_x_labels = True
    graph.min_y_value = 0

    data = list(sum(dates.items(), ()))
    graph.add_data({'data': data, 'title': 'series 1'})
    print(graph.burn())

def _parse_date(date):
    matches = re.search("facebook-comments-date-([\d]+-[\d]+-[\d]+)", date)
    return matches.group(1)


@app.task
def load_batch(url):
    global comments
    next_url = comments.load_batch(url)
    if next_url:
        before_task()
        load_batch.subtask().delay(next_url)
