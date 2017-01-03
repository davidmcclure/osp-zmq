

from osp.pipeline import Ventilator, ListWARCPaths


tasks = ListWARCPaths()

ventilator = Ventilator(tasks)

ventilator()
