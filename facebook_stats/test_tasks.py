import unittest
import redis
from unittest import mock
from httmock import HTTMock, all_requests

import facebook_stats.tasks as tasks
from facebook_stats.comments import Comments


class TestComments(unittest.TestCase):

    def setUp(self):
        tasks.redis_conn = redis.Redis(decode_responses=True)
        tasks.before_task()
        tasks.comments = Comments(
            tasks.redis_conn, tasks.REDIS_KEY_PATTERN)

    @mock.patch('time.sleep', return_value=None)
    def test_load_batch(self, sleep_mock):
        facebook_mock = FacebookMock()
        with mock.patch('facebook_stats.celeryconfig.CELERY_ALWAYS_EAGER', True, create=True):
            with HTTMock(facebook_mock.execute):
                tasks.load_batch.apply(args=(
                    'https://graph.facebook.com/v2.6/51752540096_10151775534413086/comments?fields=created_time&filter=stream&limit=1',)).get()
                self.assertDictEqual(
                    {'10-07-13': 1, '09-07-13': 1}, tasks.dates)
        sleep_mock.assert_has_calls([mock.call(1), mock.call(1)])

    @mock.patch('time.sleep', return_value=None)
    def test_load_batch_with_sleep(self, sleep_mock):
        facebook_sleep_mock = FacebookSleepMock()
        with mock.patch('facebook_stats.celeryconfig.CELERY_ALWAYS_EAGER', True, create=True):
            with HTTMock(facebook_sleep_mock.execute):
                tasks.load_batch.apply(args=(
                    'https://graph.facebook.com/v2.6/51752540096_10151775534413086/comments?fields=created_time&filter=stream&limit=1',)).get()
                self.assertDictEqual(
                    {'11-07-13': 1, '10-07-13': 1}, tasks.dates)
        sleep_mock.assert_has_calls([mock.call(2)])

    def tearDown(self):
        tasks.redis_conn = None
        tasks.comments = None
        tasks.dates = None


class FacebookMock(object):

    @all_requests
    def execute(self, url, request):
        params = self._get_params(url)

        if 'after' not in params:
            return self._first_response()
        elif params['after'] == 'WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD':
            return self._next_response()
        elif params['after'] == 'WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREEyTlRNd09EWTZANamszTnpjME5UZAz0ZD':
            return self._last_response()
        else:
            raise RuntimeError('Unknown url')

    @staticmethod
    def _get_params(url):
        raw_params = url.query.split('&')
        return {name: value for (name, value) in (p.split('=') for p in raw_params)}

    @staticmethod
    def _first_response():
        return """{
          "data": [
            {
              "created_time": "2013-07-09T14:12:33+0000",
              "id": "10151775534413086_29777454"
            }
          ],
          "paging": {
            "cursors": {
              "before": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD",
              "after": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD"
            },
            "next": "https://graph.facebook.com/v2.6/51752540096_10151775534413086/comments?fields=created_time&filter=stream&limit=1&after=WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD"
          }
        }"""

    @staticmethod
    def _next_response():
        return """{
              "data": [
                {
                  "created_time": "2013-07-10T14:12:33+0000",
                  "id": "10151775534413086_29777454"
                }
              ],
              "paging": {
                "cursors": {
                  "before": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD",
                  "after": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREEyTlRNd09EWTZANamszTnpjME5UZAz0ZD"
                },
                "next": "https://graph.facebook.com/v2.6/51752540096_10151775534413086/comments?fields=created_time&filter=stream&limit=2&after=WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREEyTlRNd09EWTZANamszTnpjME5UZAz0ZD"
              }
            }"""

    @staticmethod
    def _last_response():
        return """{
              "data": [
                {
                  "created_time": "2013-07-10T14:12:33+0000",
                  "id": "10151775534413086_29777454"
                }
              ],
              "paging": {
                "cursors": {
                  "before": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD",
                  "after": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREEyTlRNd09EWTZANamszTnpjME5UZAz0ZD"
                },
              }
            }"""


class FacebookSleepMock(object):

    def __init__(self):
        self._sent_error = False

    @all_requests
    def execute(self, url, request):
        if not self._sent_error:
            self._sent_error = True
            return self._sleep_response()
        else:
            return self._last_response()

    @staticmethod
    def _sleep_response():
        return """{
                "error":
                    {
                        "code": 4
                    }
                }"""

    @staticmethod
    def _last_response():
        return """{
      "data": [
        {
          "created_time": "2013-07-10T14:12:33+0000",
          "id": "10151775534413086_29777454"
        },
        {
           "created_time": "2013-07-11T14:12:33+0000",
           "id": "10151775534413086_29777454"
        }
      ],
      "paging": {
        "cursors": {
          "before": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD",
          "after": "WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREEyTlRNd09EWTZANamszTnpjME5UZAz0ZD"
        }
      }
    }"""

if __name__ == '__main__':
    unittest.main()
