

from distributed import Client

from osp.pipeline import ListWARCPaths, ParseWARC


client = Client()

paths = ListWARCPaths.from_env()

parse_warc = ParseWARC.from_env()

docs = client.map(parse_warc, paths)
