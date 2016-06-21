import json
import urllib
import time
import dateutil.parser
import redis

from celery.signals import worker_process_init, worker_process_shutdown, before_task_publish, task_postrun

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


@before_task_publish.connect
def before_task():
    global redis_conn
    workers_count_key = REDIS_KEY_PATTERN.format(key='workers-count')
    redis_conn.incr(workers_count_key)

@task_postrun.connect
def after_task():
    global redis_conn
    workers_count_key = REDIS_KEY_PATTERN.format(key='workers-count')
    redis_conn.decr(workers_count_key)
    if redis_conn.get(workers_count_key) == 0:
        summarize() # do not fire before_task once again?

def summarize():
    pass


@app.task
def load_batch(url):
    response = None
    while True: # timeout
        response = _process_request(url)
        shoud_limit = _check_response(response)
        _process_sleep(shoud_limit)
        if shoud_limit==False:
            break
    _process_batch(response['data'])
    _next_task(response) # when the whole tree will be done?


def _process_request(url):
    raw_response = urllib.request.urlopen(url)
    return json.load(raw_response)


def _check_response(response): # FIXME: Refactor
    if response.has_key('data'):
        return True
    try:
        error_code = response['error']['code']
    except KeyError:
        raise RuntimeError('Unknown response.')
    if error_code == 4:
        return False
    raise RuntimeError('Error with code: {code}'.format(code=error_code))

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
