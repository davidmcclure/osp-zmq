

from osp.pipeline import Worker, ParseWARC


work = Worker.from_env()

parse_warc = ParseWARC.from_env()

work(parse_warc)
