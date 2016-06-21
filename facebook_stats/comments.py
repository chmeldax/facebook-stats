import requests
import dateutil.parser
import redis
import re

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown, before_task_publish, task_postrun

app = Celery('tasks', broker='redis://localhost:6379/0')
app.config_from_object('celeryconfig')

redis_conn = None
REDIS_KEY_PATTERN = 'facebook-comments-{key}-' + str(app.conf['UUID']) # Not save if previous task crashed, should add PID or sth like that

@worker_process_init.connect
def init_worker(**kwargs):
    global redis_conn
    redis_conn = redis.Redis(decode_responses=True)


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
    dates = {}
    for key in redis_conn.keys(REDIS_KEY_PATTERN.format(key='date-*')):
        date_key = _parse_date(key)
        dates[date_key] = redis_conn.get(key)
    print(dates)

def _parse_date(date):
    matches = re.search("facebook-comments-date-([\d]+-[\d]+-[\d]+)", date)
    return matches.group(1)


@app.task
def load_batch(url):
    response = None
    while True: # timeout
        response = _process_request(url)
        response_type = _get_response_type(response)
        should_limit = True if response_type == 'sleep' else False
        _process_sleep(should_limit)
        if should_limit==False:
            break
    _process_batch(response['data'])
    _next_task(response) # when the whole tree will be done?


def _process_request(url):
    return requests.get(url).json()

def _get_response_type(response): # FIXME: Refactor
    if 'data' in response:
        return 'ok'
    try:
        error_code = response['error']['code']
    except KeyError:
        raise RuntimeError('Unknown response.')
    if error_code == 4:
        return 'sleep'
    raise RuntimeError('Error with code: {code}'.format(code=error_code))

def _process_sleep(extend_sleep):
    global redis_conn
    sleep_key = REDIS_KEY_PATTERN.format(key='sleep')
    if extend_sleep:
        redis_conn.incr(sleep_key, 2)
    sleep = int(redis_conn.get(sleep_key) or 1)
    print("Sleeping for {seconds} seconds.".format(seconds=sleep))
   # time.sleep(sleep) # Celery's countdown or sleep?


def _process_batch(batch):
    global redis_conn
    for comment in batch:
        date = _process_date(comment['created_time'])
        date_string = 'date-{date}'.format(date=date)
        date_key = REDIS_KEY_PATTERN.format(key=date_string)
        redis_conn.incr(date_key)


def _process_date(date):
    return dateutil.parser.parse(date).strftime("%d-%m-%y")


def _next_task(response):
    try:
        next_url = response['paging']['next']
        before_task()
        load_batch.subtask().delay(next_url)
    except KeyError:
        pass
