

import os

from dotenv import load_dotenv, find_dotenv

# Read `.env`.
load_dotenv(find_dotenv())


DATABASE = 'osp.db'

SCRAPER_BUCKET = 'syllascrape'

RESULT_BUCKET = 'osp-pipeline-test'

SCHEDULER_HOST = os.environ.get(
    'SCHEDULER_HOST', 'localhost'
)

SCHEDULER_PORT = 5557
