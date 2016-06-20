import json
import urllib
import time
import dateutil.parser
import redis

from celery.signals import worker_process_init, worker_process_shutdown

redis_conn = None
REDIS_KEY_PATTERN = 'facebook-comments-{key}' # Not save if previous task crashed, should add PID or sth like that

@worker_process_init.connect
def init_worker(**kwargs):
    global redis_conn
    redis_conn = redis.Redis()


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global redis_conn
    if redis_conn:
        redis_conn.disconnect()

@app.task
def load_batch(url):
    response = None
    while True: # timeout
        response = _process_request(url)
        extend_sleep = _is_sleep_response(response)
        _process_sleep(extend_sleep)
        if extend_sleep==False:
            break
    _process_batch(response['data'])
    _next_task(response) # when the whole tree will be done?


def _process_request(url):
    raw_response = urllib.request.urlopen(url)
    return json.load(raw_response)


def _is_sleep_response(response):
     return True if response.has_key('sleep') else False # Check API


def _is_data_response(response):
    return True if response.has_key('date') else False


def _process_sleep(extend_sleep):
    global redis_conn
    sleep_key = REDIS_KEY_PATTERN.format(key='sleep')
    if extend_sleep:
        redis_conn.incr(sleep_key, 2)
    time.sleep(redis_conn.get(sleep_key)) # Celery's countdown or sleep?


def _process_batch(response):
    global redis_conn
    for comment in response['data']:
        date = _process_date(comment['created_time'])
        date_key = REDIS_KEY_PATTERN.format(key=date)
        redis_conn.incr(date_key)


def _process_date(date):
    return dateutil.parser.parse(date).strftime("%d-%m-%y")


def _next_task(response):
    try:
        next_url = response['paging']['next']
        load_batch.subtask().delay(next_url)
    except KeyError:
        pass
