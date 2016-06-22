import argparse
import os

import facebook_stats.tasks
from facebook_stats.app import app


def create_url(object_id, access_token):
    return "https://graph.facebook.com/v2.6/{object_id}/comments?limit=500&fields=created_time&filter=stream&access_token={access_token}".format(object_id=object_id, access_token=access_token)


parser = argparse.ArgumentParser(description='Scrapper for Facebook comments')
parser.add_argument('--access-token', required=True,
                    help='Facebook API access token')
parser.add_argument('--object-id', required=True, help='Facebook Object Id')
args = parser.parse_args()
url = create_url(args.object_id, args.access_token)

pid = os.fork()
if pid == 0:  # child
    argv = [
        'worker',
        '--loglevel=DEBUG',
    ]
    app.worker_main(argv)
elif pid > 0:  # parent
    facebook_stats.tasks.before_task()
    facebook_stats.tasks.load_batch(url).delay()
else:
    raise RuntimeError("Forking failed")
