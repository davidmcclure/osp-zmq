

from osp.pipeline import Sink, write_text_metadata


drain = Sink.from_env()

drain(write_text_metadata)
