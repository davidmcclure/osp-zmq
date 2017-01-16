

from distributed import Client

from osp import settings
from osp.services import ScraperBucket
from osp.pipeline import extract_text


client = Client((
    settings.SCHEDULER_HOST,
    settings.SCHEDULER_PORT,
))

bucket = ScraperBucket()

paths = bucket.first_n_paths('jan-17-world', 1000)

futures = client.map(extract_text, paths)

metadata = client.gather(futures)

print(list(metadata))
