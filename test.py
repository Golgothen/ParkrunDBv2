
from mplogger import *
from proxymanager import *
from message import *
from httpget import *
from parkrundb import ParkrunDB
from time import sleep
from timeit import default_timer as timer

def processAthlete(q, config):
    logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    logger.info(f"Logger {__name__} started")
    p = ParkrunDB(config)
    db_athlete=q.get()
    while db_athlete is not None:
        logger.debug(f"Checking ID {db_athlete['AthleteID']}, {db_athlete['FirstName']} {db_athlete['LastName']} ({db_athlete['EventCount']})")
        url = f"http://www.parkrun.com.au/results/athleteeventresultshistory/?athleteNumber={db_athlete['AthleteID']}&eventNumber=0"
        c = Connection(host = 'localhost', port = 3000, config = config)
        c.send(Message('OBJECT', OBJ = AthleteHistory(url = url)))
        ws_athlete = c.recv()
        db_athlete['history'] = p.execute(f"SELECT * FROM getAthleteEventHistory({db_athlete['AthleteID']})")
        eventsmissing = ws_athlete['runcount'] - db_athlete['EventCount']                                                                           # |  |   
        logger.debug(f"Event mismatch was {eventsmissing}")    
        if eventsmissing > 0:                                                                                                                       # |  --> Database is missing an event              
            logger.debug(f"Athlete {db_athlete['AthleteID']} missing {eventsmissing} events")                                                       # |    |
            for w in ws_athlete['history']:                                                                                                         # |    |--> Scan through the web table
                found = False                                                                                                                           # |    |
                for d in db_athlete['history']:                                                                                                     # |    |  |--> Scan through the DB table
                    if w['EventURL'] == d['URL'] and w['EventNumber'] == d['EventNumber']:                                                          # |    |  |  |--> Look for the event in the database
                        found = True                                                                                                                # |    |  |     |--> Found it
                        break                                                                                                                       # |    |  |     |--> stop looking
                if not found:
                    #print(w)                                                                                                                       # |    |  |--> Once a missing event has been located
                    logger.debug(f"Missed event {w['EventNumber']} for parkrun {w['EventURL']}")                                                         # |    |     |
                    eventURL = p.getEventURL(w['EventURL'])                                                                                         # |    |     |--> Attempt to retrieve the URL from the database
                    if eventURL is not None:                                                                                                        # |    |     |--> Check the event exists
                        if p.getParkrunType(w['EventURL']) == 'Special':                                                                            # |    |     |  |--> Check for a special event
                            logger.debug("Special Event detected")                                                                                  # |    |     |  |  |
                            w['EventID'] = p.getEventID(w['EventURL'], w['EventNumber'])
                            w['AthleteID'] = db_athlete['AthleteID']
                            print(w) 
                            p.addParkrunEventPosition(w, False)                                                                                     # |    |     |  |  |--> Add the position record
                        else:                                                                                                                       # |    |     |  |--> Not a special event
                            url = f"{eventURL}{w['EventNumber']}"                                                               # |    |     |     |--> Retrieve the results
                            c = Connection(host = 'localhost', port = 3000, config = config)                                                        # |    |     |     |
                            c.send(Message('OBJECT', OBJ = EventResult(url = url)))                                               # |    |     |     |
                            eventData = c.recv()                                                                                                    # |    |     |     |
                            w['EventID'] = p.replaceParkrunEvent(w)                                                                                 # |    |     |     |
                            for e in eventData:                                                                                                     # |    |     |     |--> Add the results to the database
                                e['EventID'] = w['EventID']                                                                                         # |    |     |        |
                                p.addParkrunEventPosition(e)                                                                                        # |    |     |        |
                            logger.debug(f"Reloaded event {w['EventNumber']}, {w['EventURL']}")                                                     # |    |     |        
                            eventsmissing -= 1                                                                                                      # |    |     |
                    else:                                                                                                                           # |    |     |--> Event does not exist
                        w['RegionID'] = p.execute(f"SELECT dbo.getDefaultRegionID('{w['CountryURL']}')")                                            # |    |     |  |--> Get the default region for this country to add a new event
                        w['Juniors'] = 'juniors' in w['Name']                                                                                       # |    |     |  |--> Detect a junior event
                        p.addParkrun(w)                                                                                                             # |    |     |  |--> Add the parkrun to the database
                        # TODO Add code to import a new event
                continue #w
        elif eventsmissing < 0:
            for d in db_athlete['history']:
                found = False
                for w in ws_athlete['history']:
                    if w['EventURL'] == d['URL'] and w['EventNumber'] == d['EventNumber']:
                        found = True
                        break
                if not found:
                    logger.debug(f"Missed event {w['EventNumber']} for parkrun {w['EventURL']}")                                                         # |    |     |
                    eventURL = p.getEventURL(w['EventURL'])                                                                                         # |    |     |--> Attempt to retrieve the URL from the database
                    if eventURL is not None:                                                                                                        # |    |     |--> Check the event exists
                        if p.getParkrunType(w['EventURL']) == 'Special':                                                                            # |    |     |  |--> Check for a special event
                            logger.debug("Special Event detected")                                                                                  # |    |     |  |  |
                            w['EventID'] = p.getEventID(w['EventURL'], w['EventNumber'])                                                            # |    |     |  |  |--> Get the EventID
                            w['AthleteID'] = ws_athlete['AthleteID']
                            print(w) 
                            p.addParkrunEventPosition(w, False)                                                                                     # |    |     |  |  |--> Add the position record
                        else:                                                                                                                       # |    |     |  |--> Not a special event
                            url = f"{p.getEventURL(w['EventURL'])}{w['EventNumber']}"                                                               # |    |     |     |--> Retrieve the results
                            c = Connection(host = 'localhost', port = 3000, config = config)                                                        # |    |     |     |
                            c.send(Message('OBJECT', OBJ = EventResult(url = url)))                                               # |    |     |     |
                            eventData = c.recv()                                                                                                    # |    |     |     |
                            w['EventID'] = p.replaceParkrunEvent(w)                                                                                 # |    |     |     |
                            for e in eventData:                                                                                                     # |    |     |     |--> Add the results to the database
                                e['EventID'] = w['EventID']                                                                                         # |    |     |        |
                                p.addParkrunEventPosition(e)                                                                                        # |    |     |        |
                            logger.debug(f"Reloaded event {w['EventNumber']}, {w['EventURL']}")                                                     # |    |     |        
                            eventsmissing -= 1                                                                                                      # |    |     |
                    else:                                                                                                                           # |    |     |--> Event does not exist
                        w['RegionID'] = p.execute(f"SELECT dbo.getDefaultRegionID('{w['CountryURL']}')")                                            # |    |     |  |--> Get the default region for this country to add a new event
                        w['Juniors'] = 'juniors' in w['Name']                                                                                       # |    |     |  |--> Detect a junior event
                        p.addParkrun(w)                                                                                                             # |    |     |  |--> Add the parkrun to the database
                        # TODO Add code to import a new event
        else :
            logger.info(f"Athlete {db_athlete['FirstName']} {db_athlete['LastName']}, {db_athlete['AthleteID']} run count OK.")
            p.execute(f"UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = {db_athlete['AthleteID']}")
        db_athlete=q.get()
    logger.info(f"Process exiting")

if __name__ == '__maim__':
def shutdown():
    print('Stopping ProxyManager')
    stopEvent.set()
    print('Waiting for ProxyManager to stop')
    pm.join()
    print('Stopping LogListener')
    listener.stop()
    print('Waiting for LogListener to stop')
    listener.join()
    print('All stop')
    while workQueue.qsize() > 0:
        x = workQueue.get()

loggingQueue = Queue()

listener = LogListener(loggingQueue)
listener.start()

config = sender_config
config['handlers']['queue']['queue'] = loggingQueue
logging.config.dictConfig(config)
logger = logging.getLogger('application')

stopEvent = Event()

procCount = 20

pm = ProxyManager(stopEvent, config, procCount*2)
pm.useproxy = True
pm.start()


pr = ParkrunDB(config)
data = pr.execute ('select  * from getAthleteCheckHistoryList(30) order by eventcount desc')

#data = [x for x in data if x['AthleteID'] == 442]

workQueue = Queue()

for d in data:
    workQueue.put(d)


for i in range(procCount):
    workQueue.put(None)



p = []
while pm.getProxyCount() < (procCount*2) + 5:
    print(f"Proxy count is currently {pm.getProxyCount()}")
    sleep(1)

while len(p) < procCount:
    x = Process(target = processAthlete, args = (workQueue, config), name = f"Worker {len(p)}")
    p.append(x)
    x.start()

lastQueSize = workQueue.qsize()
start = timer()
while workQueue.qsize() > 0: # and pm.getProxyCount() > procCount:
    sleep(5)
    print(f"{(lastQueSize - workQueue.qsize())/((timer() - start) / 60):,.1f} athletes processed per minute. {workQueue.qsize():,.0f} records remain.  {pm.getProxyCount():,.0f} proxies available")
    for i in range(procCount):
        if not p[i].is_alive():
            p[i] = Process(target = processAthlete, args = (workQueue, config), name = f"Worker {i}")
            p[i].start()
            logger.info("1 process restarted")
    
for i in p:
    i.join()

shutdown()
        