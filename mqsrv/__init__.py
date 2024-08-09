import os
os.environ['GREEN_BACKEND'] = 'gevent'
from greenthread.monkey import monkey_patch; monkey_patch()