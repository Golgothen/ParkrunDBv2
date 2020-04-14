from dbconnection import *
from mplogger import *
import re

TSQL_str = lambda s: 'NULL' if s is None else str(s)#.replace("'","''")

class ParkrunDB():
    
    def __init__(self, config):
        self.__config = config
        self.__con = Connection(config, 'localhost', 'Parkrun', 'Trusted_Connection=yes', 'SQL Server Native Client 11.0')
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logger {__name__} started")
    
    def execute(self, sql):
        return self.__con.execute(sql)
        

    def getParkrunID(self, parkrunName):
        return self.__con.execute(f"SELECT dbo.getParkrunID('{parkrunName}')")

    def getAgeCatID(self, a):
        if a is None:
            return 1
        startAge = None
        endAge = None
        a = a.replace('+','')
        m = re.findall(r"\d+", a)
        if len(m) > 0:
            startAge = int(m[0])
        if len(m) > 1:
            endAge = int(m[1])
        m = re.search(r"\d", a)
        if m is not None:
            ageGroup = a[:m.start()]
        if startAge is None:
            ageCat = 'S'
            ageGroup = '---'
            gender = a[1]
        else:
            if len(ageGroup) > 1:
                gender = ageGroup[1]
            else:
                gender = ageGroup
            if startAge in [10, 11, 15]:
                ageCat = 'J'
            elif startAge in [18, 20, 25, 30]:
                ageCat = 'S'
            else:
                ageCat = 'V'
        # Add all known representations for Women
        if gender in 'FW':
            gender = 'W'
        # Add all known representations for Men
        if gender in 'MH':
            gender = 'M'
        #Default to Male
        if gender not in 'MW':
            print(f"Gender {gender}")
            gender = 'M'
        if endAge is not None:
            result = f'{ageCat}{gender}{startAge}-{endAge}'
        elif startAge is not None:
            result = f'{ageCat}{gender}{startAge}'
        else:
            result = f'{ageCat}{gender}---'
        self.logger.debug(f'Came up with {result} from {a}')
        return self.__con.execute(f"SELECT dbo.getAgeCategoryID('{result}')")
    
    def addParkrun(self, parkrun):
        return self.__con.execute(f"INSERT INTO Parkruns (RegionID, ParkrunName, URL, LaunchDate, ParkrunTypeID) VALUES ({parkrun['RegionID']}, '{parkrun['Name']}', '{parkrun['EventURL']}', '19000101', {(lambda x: 2 if x else 1)(parkrun['Juniors'])})")
    
    def addParkrunEvent(self, parkrun):
        return self.__con.execute(f"INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES ({self.getParkrunID(parkrun['EventURL'])}, {parkrun['EventNumber']}, CAST('{parkrun['EventDate'].strftime('%Y-%m-%d')}' AS date))")

    def replaceParkrunEvent(self, row):
        EventID = self.__con.execute(f"SELECT dbo.getEventID('{row['EventURL']}', {row['EventNumber']})")
        if EventID is not None:
            self.__con.execute(f"DELETE FROM Events WHERE EventID = {EventID}")
        return self.__con.execute(f"INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES ({self.getParkrunID(row['EventURL'])}, {row['EventNumber']}, CAST('{row['EventDate'].strftime('%Y-%m-%d')}' AS date))")

    def checkParkrunEventRunners(self, row):
        r = self.__con.execute(f"SELECT dbo.getParkrunEventRunners('{row['EventURL']}', {row['EventNumber']})")
        if r is None:
            return False
        else:
            if r != row['Runners']:
                return False
            else:
                return True

    def checkParkrunVolunteers(self, row):
        r = self.__con.execute(f"SELECT dbo.getParkrunEventVolunteers('{row['EventURL']}', {row['EventNumber']})")
        if r != row['Volunteers']:
            return False
        else:
            return True
    
    def getClubID(self, club):
        if club is None: return None
        clubID = self.__con.execute(f"SELECT dbo.getClubID('{TSQL_str(club)}')")
        if clubID == None:
            clubID = self.__con.execute(f"INSERT INTO Clubs (ClubName) VALUES ('{TSQL_str(club)}')")
        return clubID
    
    def getVolunteerID(self, volunteer):
        if volunteer is None: return None
        volunteerID = self.__con.execute(f"SELECT dbo.getVolunteerID('{volunteer}')")
        if volunteerID == None:
            volunteerID = self.__con.execute(f"INSERT INTO VolunteerPositions (VolunteerPosition) VALUES ('{volunteer}')")
        return volunteerID
    
    def getParkrunType(self, parkrun):
        return self.__con.execute(f"SELECT dbo.getParkrunType('{parkrun}')")
    
    def getEventID(self, parkrun, event):
        return self.__con.execute(f"SELECT dbo.getEventID('{parkrun}',{event})")
    
    def getEventURL(self, parkrun):
        return self.__con.execute(f"SELECT dbo.getEventURL('{parkrun}')")
    
    def addAthlete(self, athlete):
        self.logger.debug(athlete)
        if 'AgeCat' not in athlete:
            athlete['AgeCat'] = None
        if 'Club' not in athlete:
            athlete['Club'] = None
        r = self.execute(f"SELECT AthleteID, FirstName, LastName, AgeCategoryID, ClubID FROM Athletes WHERE AthleteID = {athlete['AthleteID']}")
        if len(r) == 0:
            try:
                sql = "INSERT INTO Athletes (AthleteID, FirstName, LastName, AgeCategoryID"
                values = f" VALUES ({athlete['AthleteID']}, '{TSQL_str(athlete['FirstName'][:50])}', '{TSQL_str(athlete['LastName'][:50])}', {TSQL_str(self.getAgeCatID(athlete['AgeCat']))}" 
                if athlete['Club'] is not None:
                    sql += ", ClubID"
                    values += f", {self.getClubID(athlete['Club'])}"
                sql += ")" + values + ")"
                self.execute(sql)
            except pyodbc.Error as e:
                if e.args[0] == 23000:
                    # On rare occasions, an athlete can be entered by another thread/process at the same time, causing a key violation.
                    return
                else:
                    self.logger.error(f'Error adding athlete {athlete}', exec_info = True, stack_info = True)
                    raise
        else:
            r = r[0]
            if athlete['AthleteID'] != 0:
                needUpdate = False
                if athlete['AgeCat'] is not None:
                    if r['AgeCategoryID'] != self.getAgeCatID(athlete['AgeCat']):
                        needUpdate = True
                if r['ClubID'] != self.getClubID(athlete['Club']):
                    needUpdate = True
                if r['FirstName'][:50] != athlete['FirstName'][:50]:
                    needUpdate = True
                if r['LastName'][:50].upper() != xstr(athlete['LastName'][:50].upper()):
                    needUpdate = True
                if needUpdate:
                    if athlete['AgeCat'] is not None:
                        self.__con.execute(f"UPDATE Athletes SET AgeCategoryID = {self.getAgeCatID(athlete['AgeCat'])}, ClubID = {TSQL_str(self.getClubID(athlete['Club']))}, FirstName = '{xstr(athlete['FirstName'][:50])}', LastName = '{xstr(athlete['LastName'][:50])}' WHERE AthleteID = {athlete['AthleteID']}")
                    else:
                        self.__con.execute(f"UPDATE Athletes SET ClubID = {TSQL_str(self.getClubID(athlete['Club']))}, FirstName = '{xstr(athlete['FirstName'][:50])}', LastName = '{xstr(athlete['LastName'][:50])}' WHERE AthleteID = {athlete['AthleteID']}")
                        
        
    def addParkrunEventPosition(self, position, addAthlete = True):
        self.logger.debug(position)
        if addAthlete:
            self.addAthlete(position)
        sql = "INSERT INTO EventPositions (EventID, AthleteID, Position"
        values = f"VALUES ({position['EventID']}, {position['AthleteID']}, {position['Pos']}"

        if position['Time'] is not  None:
            sql += ", GunTime" 
            values += f", CAST('{position['Time']}' as time(0))"
        if 'AgeCat' in position:
            if position['AgeCat'] is not  None:
                sql += ", AgeCategoryID" 
                values += f", {self.getAgeCatID(position['AgeCat'])}"
        if 'AgeGrade' in position:
            if position['AgeGrade'] is not  None:
                sql += ", AgeGrade" 
                values += ", " + xstr(position['AgeGrade'])
        if 'Note' in position:
            if position['Note'] is not  None:
                sql += ", Comment" 
                values +=  ", '" + position['Note'][:30].replace("'","") + "'"
        sql += ") " + values + ")"
        #print(sql)
        self.execute(sql)
        