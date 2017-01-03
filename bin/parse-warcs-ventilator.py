

from osp.pipeline import Ventilator, ListWARCPaths


ventilate = Ventilator.from_env()

list_paths = ListWARCPaths.from_env()

ventilate(list_paths)
