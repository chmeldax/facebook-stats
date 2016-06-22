import requests
import dateutil.parser
import time


class Comments(object):

    def __init__(self, redis, api_key, redis_key_pattern):
        self._redis = redis
        self._api_key = api_key
        self._redis_key_pattern = redis_key_pattern

    def load_batch(self, url):
        response = None
        while True:  # timeout
            response = self._process_request(url)
            response_type = self._get_response_type(response)
            should_limit = True if response_type == 'sleep' else False
            self._process_sleep(should_limit)
            if not should_limit:
                break
        self._process_batch(response['data'])
        return self._next_task_url(response)  # when the whole tree will be done?

    @staticmethod
    def _process_request(url):
        return requests.get(url).json()

    @staticmethod
    def _get_response_type(response):  # FIXME: Refactor
        if 'data' in response:
            return 'ok'
        try:
            error_code = response['error']['code']
        except KeyError:
            raise RuntimeError('Unknown response.')
        if error_code == 4:
            return 'sleep'
        raise RuntimeError('Error with code: {code}'.format(code=error_code))

    def _process_sleep(self, extend_sleep):
        sleep_key = self._redis_key_pattern.format(key='sleep')
        if extend_sleep:
            self._redis.incr(sleep_key, 2)
        sleep = int(self._redis.get(sleep_key) or 1)
        print("Sleeping for {seconds} seconds.".format(seconds=sleep))
        time.sleep(sleep) # Celery's countdown or sleep?

    def _process_batch(self, batch):
        for comment in batch:
            date = self._process_date(comment['created_time'])
            date_string = 'date-{date}'.format(date=date)
            date_key = self._redis_key_pattern.format(key=date_string)
            self._redis.incr(date_key)

    @staticmethod
    def _process_date(date):
        return dateutil.parser.parse(date).strftime("%d-%m-%y")

    @staticmethod
    def _next_task_url(response):
        try:
            return response['paging']['next']
        except KeyError:
            return None
