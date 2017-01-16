

from distributed import Client

from osp import settings
from osp.services import ScraperBucket
from osp.pipeline import extract_text


client = Client((
    settings.SCHEDULER_HOST,
    settings.SCHEDULER_PORT,
))

bucket = ScraperBucket()

paths = bucket.first_n_paths(100)

futures = client.map(extract_text, paths)

metadata = client.gather(futures)

print(metadata)
