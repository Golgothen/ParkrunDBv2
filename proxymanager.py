import requests, logging, logging.config, threading
from multiprocessing import Process, Queue, Pipe, Event
from threading import Thread
from bs4 import BeautifulSoup as soup
import lxml.html
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from mplogger import *

class Crawler():
    
    """
    Crawler is a super class that ProxyManager will interact with.
    Subclasses of this class will have the specific code needed for interpreting each web page table
    """
    
    def __init__(self, name, newProxies, config):
        self.name = name
        self.untestedProxies = newProxies
        self.thread_count = 5
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
        self.config = config

    def getProxies(self):
        # Subclasses will provide this code and put their results into the self.proxyQue
        print('Crawler {} scanning for proxies'.format(self.name))
    


class FreeProxyList(Crawler):
    
    """
    FreeProxyList scrapes proxy information from https://free-proxy-list.net
    """
    
    def __init__(self, untestedProxies, config):
        super().__init__('Free_Proxy_List', untestedProxies, config)
    
    def getProxies(self):
        super().getProxies()
        response = requests.get('https://free-proxy-list.net/', self.headers)
        page_soup = soup(response.text, "html.parser")
        containers = page_soup.find_all("div", {"class": "table-responsive"})[0]
        ip_index = [8*k for k in range(80)]
        for i in ip_index:
            ip = containers.find_all("td")[i].text
            port = containers.find_all("td")[i+1].text
            https = containers.find_all("td")[i+6].text
            #print("ip address : {:<15}   port : {:<5}   https : {:<3} ".format(ip, port, https))
            if https == 'yes':
                self.untestedProxies.put({'proxy':ip + ':' + port, 'source':self.name})
        print('Crawler {} complete. Testing.'.format(self.name))

class SpyOne(Crawler):
    
    """
    SpyOne scrapes proxies from http://spys.one/en/https-ssl-proxy/
    """
    def __init__(self, untestedProxies, config):
        super().__init__('SpyOne', untestedProxies, config)
    
    def getProxies(self):
        super().getProxies()
        response = requests.get('http://spys.one/en/https-ssl-proxy/', headers = self.headers)
        lx = lxml.html.fromstring(response.text)
        # Isolate the table we want
        t = lx[1][2][4][0][0]
        
        for i in range(2,len(t)-1):
            ip = t[i][0][0].text
            port = '8080'
            self.untestedProxies.put({'proxy':ip + ':' + port, 'source':self.name})
        
        print('Crawler {} complete'.format(self.name))



class ProxyScrape(Crawler):
    
    """
    ProxyScrape scrapes proxies from https://api.proxyscrape.com
    """
    
    def __init__(self, untestedProxies, config):
        super().__init__('ProxyScrape', untestedProxies, config)
    
    def getProxies(self):
        super().getProxies()
        response = requests.get('https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=10000&country=all&ssl=all&anonymity=all', headers = self.headers)
        lx = lxml.html.fromstring(response.text)
        l = lx.text.split('\r\n')
        for i in l[:100]:
            self.untestedProxies.put({'proxy':i, 'source':self.name})
        print('Crawler {} complete'.format(self.name))
    
"""
class NonDupeQueue(multiprocessing.queues.Queue):
    
    def __init__(self, *args, **kwargs): 
        ctx = multiprocessing.get_context()
        super(NonDupeQueue, self).__init__(*args, **kwargs, ctx = ctx)
        self.__items = []
    
    def put(self, item): #, *args, **kwargs):
        if item not in self.__items:
            self.__items.append(item)
            super().put(item) #, *args, **kwargs)
    
    def get(self, *args, **kwargs):
        x = super().get(*args, **kwargs)
        self.__items = [i for i in self.__items if i != x]
        return x
"""

           
class ProxyManager(Process):
    
    """
    ProxyManager will be responsible for maintaining a list of available and valid proxies.
    Once a proxy is requested it will be removes from the que of available proxies.
    If the que drops below a pre-defined threshold, all subclasses of crawlers will be invoked to collect more proxies.
    """
    
    def __init__(self, exitEvent, config):
        super(ProxyManager, self).__init__()
        self.crawlers = []
        self.__crawler_threads = []
        self.__proxies = Queue()
        self.__untested_proxies = Queue()
        self.__min_proxy_count = 3
        self.__exitEvent = exitEvent
        self.__proxyList = []
        self.__tester_thread_count = 50
        self.__tester_threads = []
        self.__getter_threads = []
        self.config = config
        self.logger = None
    
    def addCrawler(self, newCrawler):
        # Add the new crawler to the list of crawlers
        self.crawlers.append(newCrawler)
        
    def getProxyCount(self):
        return self.__proxies.qsize()
    
    def testProxy(self): 
        while True:
            if self.__untested_proxies.qsize() == 0:
                print(f'Thread {threading.currentThread().getName()} idle')
            proxy = self.__untested_proxies.get()
            if proxy == None:
                break
            #print('Thread {} Testing {}'.format(threading.currentThread().getName(),proxy))
            try:
                response = requests.get('https://httpbin.org/ip', proxies={"http": proxy['proxy'], "https": proxy['proxy']}, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'},  timeout = 5)
                self.__proxies.put(proxy['proxy'])
                print(f'Thread {threading.currentThread().getName()} added {proxy["proxy"]} from {proxy["source"]}. {self.__untested_proxies.qsize()} proxies remain untested')
            except:
                pass
                #print('Thread {} skipped {} from {}'.format(threading.currentThread().getName(),proxy['proxy'], proxy['source']))
                
    """
    def put(self, newProxy):
        if newProxy not in self.__proxyList:
            self.__proxyList.append(newProxy)
            self.__proxies.put(newProxy)
        else:
            print('Dupe {} skipped.'.format(newProxy))
    """
    
    def getProxy(self):
        # Check if there are sufficient proxies in the 
        if self.__proxies.qsize() <= self.__min_proxy_count:
            # Iterate through the available crawlers
            print('Starting new crawlers')
            print(f'getProxy() : {hex(id(self.crawlers))}')
            print(len(self.crawlers))
            self.__crawler_threads = [x for x in self.__crawler_threads if x.isAlive()]
            for i in self.crawlers:
                running = False
                # see if there is a thread with the same name
                print('Looking for crawler {}'.format(i.name))
                for j in self.__crawler_threads:
                    if j.name == i.name and j.isAlive():
                        # a thread by the same name was found, so don't start it again
                        running = True
                        print('Crawler {} already running'.format(i.name))
                        print(j.isAlive())
                if not running:
                    print('Starting crawler {}'.format(i.name))
                    # start any crawlers that didn't have matching threads (by name)
                    x = Thread(target = i.getProxies, daemon = True, name = i.name)
                    self.__crawler_threads.append(x)
                    x.start()
        
        # If there are no proxies left, execution should block here until one of the crawlers returns a new value into the queue
        x = self.__proxies.get()
        #self.__proxyList = [i for i in self.__proxyList if i != x]
        return x

    def getURL(self, url, returnPipe):
        x = Thread(target = self.__getURL, args = (url, returnPipe,), daemon = True)
        self.__getter_threads.append(x)
        x.start()
    
    def __getURL(self, url, returnPipe):
        completed = False
        proxy = self.getProxy()
        while not completed:
            #self.logger.debug(f"Hitting {url} with proxy {proxy['proxy']}")
            try:
                response = requests.get(url, proxies={"http": proxy['proxy'], "https": proxy['proxy']}, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}, timeout = 10)
            except:
                proxy = self.getProxy()
                continue
            if response.code != 200:
                proxy = self.getProxy()
                continue
            completed = True
            self.__proxies.put(proxy)       # Put the proxy back in the queue if it worked
        temp = response.text#.decode('utf-8', errors='ignore')
        returnPipe.send(lxml.html.fromstring(temp)) 

    
    def run(self):
        logging.config.dictConfig(self.config)
        self.logger = logging.getLogger(__name__)
        self.addCrawler(FreeProxyList(self.__untested_proxies, self.config))
        self.addCrawler(ProxyScrape(self.__untested_proxies, self.config))
        for i in range(self.__tester_thread_count):
            self.__tester_threads.append(Thread(target = self.testProxy, name = 'Tester {}'.format(i), daemon = True)) 
            self.__tester_threads[i].start()
        #self.__proxies = NonDupeQueue()
        print(f'run(): {hex(id(self.crawlers))}')
        for i in self.crawlers:
            x = Thread(target = i.getProxies, daemon = True, name = i.name)
            self.__crawler_threads.append(x)
            x.start()
        self.__exitEvent.wait()
        print('ProxyManager exiting')
        while self.__untested_proxies.qsize() > 0:
            x = self.__untested_proxies.get()
        while self.__proxies.qsize() > 0:
            x = self.__proxies.get()
