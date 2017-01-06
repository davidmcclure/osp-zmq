

from distributed import Client

from osp.pipeline import ScraperBucket, ExtractText


client = Client()

bucket = ScraperBucket.from_env()

paths = bucket.first_n_paths('oct-16', 1000)

extract_text = ExtractText.from_env()

text = client.map(extract_text, paths)
