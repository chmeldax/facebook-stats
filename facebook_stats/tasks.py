import redis
import re
import os
from celery.signals import task_postrun
from svg.charts import time_series

from facebook_stats.app import app
from facebook_stats.comments import Comments


REDIS_KEY_PATTERN = 'facebook-comments-{key}-' + str(app.conf['UUID'])


redis_conn = redis.Redis(decode_responses=True)
comments = Comments(redis_conn, REDIS_KEY_PATTERN)
dates = None


def before_task():
    """
    Used as a hook. Not using native celery's hook
    had issue in tests
    :return: None
    """
    global redis_conn
    print("before")
    workers_count_key = REDIS_KEY_PATTERN.format(key='workers-count')
    redis_conn.incr(workers_count_key)


@task_postrun.connect
def after_task(**kwargs):
    """
    Celery's hook
    :param kwargs:
    :return: None
    """
    global redis_conn
    print('after')
    workers_count_key = REDIS_KEY_PATTERN.format(key='workers-count')
    redis_conn.decr(workers_count_key)
    if int(redis_conn.get(workers_count_key)) == 0:
        summarize()


def summarize():
    """
    Generates SVG with time series
    :return:
    """
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
    _save(graph.burn())


def _save(graph_svg):
    root = os.path.dirname(__file__)
    with open(os.path.join(root, 'output.svg'), 'wb') as f:
        f.write(graph_svg)


def _parse_date(date):
    matches = re.search("facebook-comments-date-([\d]+-[\d]+-[\d]+)", date)
    return matches.group(1)


@app.task
def load_batch(url):
    """
    Celery's task to one batch + all other subsequent ones
    :param url:
    :return:
    """
    global comments
    next_url = comments.load_batch(url)
    if next_url:
        before_task()
        load_batch.subtask().delay(next_url)
