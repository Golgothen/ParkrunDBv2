from mplogger import *
import requests, lxml

class BadProxy(Exception):
    pass
class URLFail(Exception):
    pass

class HTTPGet(object):
    
    def __init__(self, **kwargs):
        self.user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
        self.timeout = 10
        self.proxy = None
        for k in kwargs:
            x = k.lower()
            self.__dict__[x] = kwargs[k]
        
        if 'url' not in self.__dict__:
            raise ValueError('Mandatory parameter URL missing.')
        #if 'logger' not in self.__dict__:
        #    raise ValueError('Mandatory parameter LOGGER missing.')
        #else:
            #logging.config.dictConfig(self.config)
            #self.logger = logger #logging.getLogger(__name__)
    
    def parse(self):
        try:
            print(f"Hitting {self.url} via {self.proxy}")
            if self.proxy is not None:
                response = requests.get(self.url, proxies={"http": self.proxy, "https": self.proxy}, headers=self.user_agent, timeout = self.timeout)
            else:
                response = requests.get(self.url, headers=self.user_agent, timeout = self.timeout)
            #self.logger.debug(f'Got response code {response.status_code}')
            if response.status_code != 200:
                print(f'Response code was {response.status_code}')
                if self.proxy is not None:
                    raise BadProxy
                else:
                    raise URLFail
            else:
                return lxml.html.fromstring(response.text)
        except Exception as e:
            print(f'Exception {e}')
            if self.proxy is not None:
                raise BadProxy
            else:
                raise URLFail
        

class BlankPage(HTTPGet):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def parse(self):
        try:
            response = super().parse()
        except:
            raise
        # BlankPage does not process the page any further.  return what was retrieved
        return response

class AthleteHistory(HTTPGet):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def parse(self):
        try:
            html = super().parse()
        except:
            raise
        athlete = {}
        athlete['runcount'] = int(html.xpath('//*[@id="content"]/h2/text()[1]')[0].split(' runs at All Events')[0].split(' ')[-1])
        
        table = html.xpath('//*[@id="results"]')[0]
        rows = table.xpath('//tbody/tr')
        
        athlete['history'] = []
        for row in rows:
            if len(row) < 7:
                continue
            h = {}
            h['EventURL'] = row[0][0].get('href').split('/')[3]
            h['Name'] = row[0][0].text.split(' parkrun')[0]
            h['CountryURL'] = 'http://' + row[0][0].get('href').split('/')[2] + '/'
            h['EventDate'] = datetime.strptime(row[1][0].text,"%d/%m/%Y")
            h['EventNumber'] = int(row[2][0].get('href').split('=')[1])
            h['Pos'] = int(row[3].text)
            t = row[4].text
            if len(t)<6:
                t = '00:' + t 
            h['Time'] = t
            if row[5].text is not None:
                h['AgeGrade'] = row[5].text[:-1]
            else:
                h['AgeGrade'] = None
            athlete['history'].append(h)
        return athlete

class EventResult(HTTPGet):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def parse(self):
        try:
            html = super().parse()
        except:
            raise
        
        table = html.xpath('//*[@id="content"]/div[1]/table')[0]
        headings = ['Pos','parkrunner','Gender','Age Cat','Club','Time']
        
        rows = table.xpath('//tbody/tr')
        
        results = []
        for row in rows:
            d = {}
            for h, v in zip(headings, row):
                if h == 'Pos':
                    d['Pos'] = int(v.text)
                if h == 'parkrunner':
                    if len(v[0])>0:
                        data = v[0][0].text
                        if len(data.split()) == 0:
                            # Unnamed athlete
                            d['FirstName'] = 'Unknown'
                            d['LastName'] = None
                            d['AthleteID'] = 0
                            d['Time'] = None
                            d['Age Cat'] = None
                            d['Age Grade'] = None
                            d['Club'] = None
                            d['Note'] = None
                            break
                        d['FirstName'] = data.split()[0].replace("'","''")
                        lastName = ''
                        l = data.split()
                        l.pop(0)
                        for i in l:
                            lastName += i.capitalize().replace("'","''") + ' '
                        if lastName != '':
                            d['LastName'] = lastName.strip()
                        else:
                            d['LastName'] = ''
                        d['AthleteID'] = int(v[0][0].get('href').split('=')[1])
                    else:
                        # Unknown Athlete
                        d['FirstName'] = 'Unknown'
                        d['LastName'] = None
                        d['AthleteID'] = 0
                        d['Time'] = None
                        d['Age Cat'] = None
                        d['Age Grade'] = None
                        d['Club'] = None
                        d['Note'] = None
                        break
                if h == 'Gender':
                    #30/10/19 - Gender also holds Gender Pos.
                    if len(v[0].text.strip()) > 0:
                        d['Gender'] = v[0].text.strip()[0]
                    else:
                        d['Gender'] = 'M'
                if h == 'Age Cat':
                    if len(v)>0:
                        # 30/10/19 - Age Category and Age Grade are now in the same cell
                        d['Age Cat'] = v[0][0].text
                        if len(v) > 1:
                            d['Age Grade'] = float(v[1].text.split('%')[0])
                        else:
                            d['Age Grade'] = None
                    else:
                        d['Age Cat'] = None
                        d['Age Grade'] = None
                if h == 'Club':
                    if len(v)>0:
                        if v[0][0].text is not None:
                            d[h] = v[0][0].text.replace("'","''")
                        else:
                            d[h] = None
                    else:
                        d[h] = None
                if h == 'Time':
                    data = v[0].text
                    if data is not None:
                        if len(data)<6:
                            data = '0:' + data
                    d['Time'] = data
                    # 30/11/19 - Note is now inside the Time cell
                    d['Note'] = v[1][0].text
            results.append(d)
        if len(results) > 0:
            if 'Pos' not in results[0].keys():
                results = sorted(results, key=lambda k: '0:00:00' if k['Time'] is None else k['Time'])
                for i in range(len(results)):
                    results[i]['Pos'] = i + 1
        return results
        
class EventHistory(HTTPGet):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def parse(self):
        try:
            html = super().parse()
        except:
            raise
        table = html.xpath('//*[@id="results"]')[0]
        headings = ['EventNumber','EventDate','Runners','Volunteers']    
        rows = table.xpath('//tbody/tr')
        
        data = []
        for row in rows:
            d = {}
            for h, v in zip(headings, row.getchildren()):
                if h == 'EventNumber':
                    d[h] = int(v.getchildren()[0].text)
                if h in ['Runners','Volunteers']:
                    if v.text.strip() == 'unknown':
                        d[h] = None
                    else: 
                        d[h] = int(v.text)
                if h == 'EventDate':
                    d[h] = datetime.strptime(v.getchildren()[0].text,"%d/%m/%Y")
            data.insert(0,d)
        return data
        
        
        
        