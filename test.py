
from mplogger import *
#from parkrundb import *
from proxymanager import *


def geturi(l):
    pm.getURL(l['url'], l['sender'])
    l['result'] = l['receiver'].recv()


loggingQueue = Queue()

listener = LogListener(loggingQueue)
listener.start()

config = sender_config
config['handlers']['queue']['queue'] = loggingQueue
logging.config.dictConfig(config)
logger = logging.getLogger('application')

#p = ParkrunDB(config)
exitEvent = Event()

pm = ProxyManager(exitEvent, config)
pm.start()


l = []
sender, receiver = Pipe()
l.append({'url':'www.microsoft.com', 'sender':sender, 'receiver':receiver, 'result': None})
sender, receiver = Pipe()
l.append({'url':'www.google.com', 'sender':sender, 'receiver':receiver, 'result': None})
sender, receiver = Pipe()
l.append({'url':'www.github.com', 'sender':sender, 'receiver':receiver, 'result': None})

for i in l:
    i['thread'] = Thread(target = geturi, args = (i,), daemon = True)
    i['thread'].start()

for i in l:
    i['thread'].join()

print(i)
