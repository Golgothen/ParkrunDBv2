
from mplogger import *
#from parkrundb import *
from proxymanager import *
from message import *
from httpget import *

"""
def geturi(l):
    pm.getURL(l['url'], l['sender'])
    l['result'] = l['receiver'].recv()
"""

loggingQueue = Queue()

listener = LogListener(loggingQueue)
listener.start()

config = sender_config
config['handlers']['queue']['queue'] = loggingQueue
logging.config.dictConfig(config)
logger = logging.getLogger('application')

#p = ParkrunDB(config)
e = Event()

pm = ProxyManager(e, config,)
pm.start()

athleteid = 1831490
eventURL = 'berwicksprings'
for eventNumber in range(1,10):
    p = EventResult(url = f'http://www.parkrun.com.au/{eventURL}/results/weeklyresults/?runSeqNumber={eventNumber}')
    c = Connection(host = 'localhost', port = 3000, config = config)
    
    m = Message('OBJECT', OBJ = p)
    c.send(m)
    x = c.recv()


    