import unittest
from unittest import mock
import redis
from httmock import HTTMock, all_requests

import facebook_stats.tasks as tasks
from facebook_stats.comments import Comments


class TestComments(unittest.TestCase):

    def test_load_batch(self):
        tasks.redis_conn = redis.Redis(decode_responses=True)
        tasks.before_task()
        tasks.comments = Comments(tasks.redis_conn, None, tasks.REDIS_KEY_PATTERN)
        with mock.patch('celeryconfig.CELERY_ALWAYS_EAGER', True, create=True):
            with HTTMock(self._facebook_mock):
                tasks.load_batch.apply(args=('https://graph.facebook.com/v2.6/51752540096_10151775534413086/comments?fields=created_time&filter=stream&limit=1',)).get()

    def test_load_batch_with_sleep(self):
        self._sent_error = False
        tasks.redis_conn = redis.Redis(decode_responses=True)
        tasks.before_task()
        tasks.comments = Comments(tasks.redis_conn, None, tasks.REDIS_KEY_PATTERN)
        with mock.patch('celeryconfig.CELERY_ALWAYS_EAGER', True, create=True):
            with HTTMock(self._facebook_sleep_mock):
                tasks.load_batch.apply(args=('https://graph.facebook.com/v2.6/51752540096_10151775534413086/comments?fields=created_time&filter=stream&limit=1',)).get()

    # FIXME: Refactor
    @all_requests
    def _facebook_mock(self, url, request):
        raw_params = url.query.split('&')
        params = {name: value for (name, value) in (p.split('=') for p in raw_params)}

        if 'after' not in params:
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
        elif params['after'] == 'WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREExTmpnd09EWTZANamszTnpjME5UUT0ZD':
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
        elif params['after'] == 'WTI5dGJXVnVkRjlqZAFhKemIzSTZANVEF4TlRFM056VTFOREEyTlRNd09EWTZANamszTnpjME5UZAz0ZD':
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

    # FIXME: Refactor
    @all_requests
    def _facebook_sleep_mock(self, url, request):
        if not self._sent_error:
            self._sent_error = True
            return """{
                "error":
                {
                    "code": 4
                }
                }"""
        else:
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
                }
              }
            }"""
if __name__ == '__main__':
    unittest.main()
