

from distributed import Client

from osp import settings
from osp.services import ScraperBucket
from osp.pipeline import extract_text
from osp.models import Document


client = Client((
    settings.SCHEDULER_HOST,
    settings.SCHEDULER_PORT,
))

bucket = ScraperBucket()

# List paths.
paths = bucket.first_n_paths('jan-17-world', 10000)

# Apply the worker.
futures = client.map(extract_text, paths)

# Gather results.
metadata = client.gather(futures, errors='skip')

# Insert metadata rows.
Document.bulk_insert(metadata)
