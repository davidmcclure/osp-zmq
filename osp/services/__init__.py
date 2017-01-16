

from .database import Database
from .buckets import ScraperBucket, ResultBucket


db = Database.from_settings()

scraper_bucket = ScraperBucket.from_settings()

result_bucket = ResultBucket.from_settings()
