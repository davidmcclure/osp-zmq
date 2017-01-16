

from distributed import Client
from datetime import datetime as dt

from osp import settings
from osp.services import scraper_bucket
from osp.pipeline import ExtractText
from osp.models import Document


client = Client((
    settings.SCHEDULER_HOST,
    settings.SCHEDULER_PORT,
))

print(dt.now(), 'start')

# List paths.
paths = scraper_bucket.paths('jan-17-world')

# Apply the worker.
futures = client.map(ExtractText(), paths)

# Gather results.
metadata = client.gather(futures, errors='skip')

# Insert metadata rows.
Document.bulk_insert(metadata)

print(dt.now(), 'end')
