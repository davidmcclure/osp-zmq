

from osp.pipeline import Sink, write_doc_metadata


drain = Sink.from_env()

drain(write_doc_metadata)
