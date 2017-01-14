

from distributed import Client

from osp.services import ScraperBucket
from osp.pipeline import extract_text


client = Client()

bucket = ScraperBucket()

paths = bucket.first_n_paths('jan-17-world', 1000)

text = client.map(extract_text, paths)

results = client.gather(text)

print(list(results))
