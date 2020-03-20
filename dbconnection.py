import pyodbc, logging, logging.config


xstr = lambda s: 'NULL' if s is None else str(s)

class Connection():
    def __init__(self, config, server, database, userString, driver):

        #server = 'localhost'
        #database = 'Parkrun'
        #userstring = 'Trusted_Connection=yes'
        #driver = 'SQL Server Native Client 11.0'
        self.connectString = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';'+userString
        pyodbc.pooling = False

        self.conn = pyodbc.connect(self.connectString)
        """
        self.cachedParkrun = None
        self.cachedAgeCat = None
        #self.cachedAthlete = None
        self.cachedClub = None
        self.cachedVolunteer = None
        """
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)
        
    
    def execute(self, sql):
        self.logger.debug(sql)
        c = None
        try:
            if sql[:6].upper() == "SELECT":
                if "FROM" in sql.upper():
                    data = []
                    headings = []
                    c = self.conn.execute(sql)
                    if c.description is not None:
                        for h in c.description:
                            headings.append(h[0])
                        for row in c.fetchall():
                            d = {}
                            for h, v in zip(headings, row):
                                d[h]=v
                            data.append(d)
                        return data
                    else:
                        return None
                else:
                    c = self.conn.execute(sql)
                    return c.fetchall()[0][0]
            if sql[:6].upper() == "INSERT":
                c = self.conn.execute(sql)
                c = self.conn.execute("SELECT SCOPE_IDENTITY()")
                if c.rowcount != 0:
                    t = c.fetchone()[0]
                    if t is not None:
                        data = int(t)
                        self.logger.debug('INSERT returned SCOPE_IDENTITY() {}'.format(int(t)))
                    else:
                        data=None
                        self.logger.debug('INSERT returned no SCOPE_IDENTITY()')
                else:
                    data=None
                c.commit()
                return data
            if sql[:6].upper() in ["DELETE", "UPDATE"]:
                c = self.conn.execute(sql)
                c.commit()
                return None
        except:
            self.logger.error('Error occured executing statement: {}'.format(sql),exc_info = True, stack_info = True)
            if sql[:6] in ['INSERT', 'DELETE', 'UPDATE']:
                if c is not None:
                    self.logger.error('Rolling back previous statement')
                    c.rollback()
            raise

    def close(self):
        self.conn.close()
        del self.conn
