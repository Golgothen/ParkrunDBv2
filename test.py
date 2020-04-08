
from mplogger import *
from proxymanager import *
from message import *
from httpget import *
from parkrundb import ParkrunDB

loggingQueue = Queue()

listener = LogListener(loggingQueue)
listener.start()

config = sender_config
config['handlers']['queue']['queue'] = loggingQueue
logging.config.dictConfig(config)
logger = logging.getLogger('application')

p = ParkrunDB(config)
e = Event()

pm = ProxyManager(e, config,)
pm.start()


def shutdown():
    print('Stopping ProxyManager')
    e.set()
    print('Waiting for ProxyManager to stop')
    pm.join()
    print('Stopping LogListener')
    listener.stop()
    print('Waiting for LogListener to stop')
    listener.join()
    print('All stop')



data = p.execute ('select top 10 * from getAthleteCheckHistoryList(30)')
for athlete in data:
    logger.debug(f"Checking ID {athlete['AthleteID']}, {athlete['FirstName']} {athlete['LastName']} ({athlete['EventCount']})")
    
    x = AthleteHistory(url = f"http://www.parkrun.com.au/results/athleteeventresultshistory/?athleteNumber={athlete['AthleteID']}&eventNumber=0")
    c = Connection(host = 'localhost', port = 3000, config = config)
    m = Message('OBJECT', OBJ = x)
    c.send(m)
    x = c.recv()
    if athlete['EventCount'] != x['runcount']:
        eventsmissing = x['runcount'] - athlete['EventCount']
        if eventsmissing > 0:
            logger.debug(f"Athlete {athlete['AthleteID']} missing {eventsmissing} events")
            for row in x['history']:
                