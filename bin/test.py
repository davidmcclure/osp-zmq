

import math

from distributed import Client

from osp import settings


client = Client((
    settings.SCHEDULER_HOST,
    settings.SCHEDULER_PORT,
))

result = client.map(math.sqrt, range(1000))

print(sum(client.gather(result)))
