from dbconnection import *
from mplogger import *

xstr = lambda s: 'NULL' if s is None else str(s)

class ParkrunDB():
    
    def __init__(self, config):
        self.__config = config
        self.__con = Connection(config, 'localhost', 'Parkrun', 'Trusted_Connection=yes', 'SQL Server Native Client 11.0')
        logging.config.dictConfig(config)
        self.logger = logging.getlogger(__name__)
    
    def getParkrunID(self, parkrunName):
        return self.__con.execute("SELECT dbo.getParkrunID('{}')".format(parkrunName))

    def getAgeCatID(self, a):
        if a is None:
            return 1
        startAge = None
        a = a.replace('+','')
        for i in [1,2,3,4,5,6,7,8,9,0]:
            if str(i) in a:
                startAge = int(a[a.find(str(i)):].split('-')[0])
                if '-' in a:
                    endAge = int(a[a.find(str(i)):].split('-')[1])
                else:
                    endAge = None
                ageGroup = a[:a.find(str(i))]
                break
        
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
            
        if gender not in 'MF':
            gender = 'M'
        
        if endAge is not None:
            result = f'{ageCat}{gender}{startAge}-{endAge}'
        else:
            result = f'{ageCat}{gender}{startAge}'
        self.logger.debug(f'Came up with {result} from {a}')
        return self.__con.execute(f"SELECT dbo.getAgeCategoryID('{result}')")
    
    # TODO: Add an update function for athletes and allow getAgeCatID to update the athlete's gender on a mismatch
    
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
        clubID = self.__con.execute(f"SELECT dbo.getClubID('{club}')")
        if clubID == None:
            clubID = self.__con.execute(f"INSERT INTO Clubs (ClubName) VALUES ('{club}')")
        return clubID
    
    def getVolunteerID(self, volunteer):
        if volunteer is None: return None
        volunteerID = self.__con.execute(f"SELECT dbo.getVolunteerID('{volunteer}')")
        if volunteerID == None:
            volunteerID = self.__con.execute(f"INSERT INTO VolunteerPositions (VolunteerPosition) VALUES ('{volunteer}')")
        return volunteerID
    
    def addAthlete(self, athlete):
        r = self.execute(f"SELECT AthleteID, FirstName, LastName, Gender, AgeCategoryID, ClubID FROM Athletes WHERE AthleteID = {athlete['AthleteID']}")
        if len(r) == 0:
            try:
                sql = "INSERT INTO Athletes (AthleteID, FirstName, LastName, AgeCategoryID, Gender"
                values = f" VALUES ({athlete['AthleteID']}, '{xstr(athlete['FirstName'][:50])}', '{xstr(athlete['LastName'][:50])}', {self.getAgeCatID(athlete['Age Cat'])}, '{xstr(athlete['Gender'])}'" 
                if athlete['Club'] is not None:
                    sql += ", ClubID"
                    values += f", {self.getClub(athlete['Club'])}"
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
                if r['AgeCategoryID'] != self.getAgeCatID(athlete['Age Cat']) or \
                   r['ClubID'] != self.getClub(athlete['Club']) or \
                   r['FirstName'][:50] != athlete['FirstName'][:50] or \
                   r['LastName'][:50].upper() != xstr(athlete['LastName'][:50].upper()):
                    self.__con.execute(f"UPDATE Athletes SET AgeCategoryID = {self.getAgeCatID(athlete['Age Cat'])}, ClubID = {self.getClub(athlete['Club'])}, FirstName = '{xstr(athlete['FirstName'][:50])}', LastName = '{xstr(athlete['LastName'][:50])}' WHERE AthleteID = {athlete['AthleteID']}")
        
    def addParkrunEventPosition(self, position, addAthlete = True):
        if addAthlete:
            self.addAthlete(position)
        sql = "INSERT INTO EventPositions (EventID, AthleteID, Position"
        values = f" VALUES ({position['EventID']}, {position['AthleteID']}, {position['Pos']})"

        if position['Time'] is not  None:
            sql += ", GunTime" 
            values += f", CAST('{position['Time']}' as time(0))"
        if position['Age Cat'] is not  None:
            sql += ", AgeCategoryID" 
            values += f", {self.getAgeCatID(position['Age Cat'])}"
        if position['Age Grade'] is not  None:
            sql += ", AgeGrade" 
            values += ", " + xstr(position['Age Grade'])
        if position['Note'] is not  None:
            sql += ", Comment" 
            values +=  ", '" + position['Note'][:30].replace("'","") + "'"
        sql += ")" + values + ")"
        #print(sql)
        self.execute(sql)
    