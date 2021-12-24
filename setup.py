import sqlite3

def createDataTable(db):
    with db:
        db.execute(f"""
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cityId INTEGER NOT NULL,
                    gatherDate TIMESTAMP NOT NULL,
                    date TIMESTAMP,
                    temp REAL,
                    sensedTemp REAL,
                    cover TEXT,
                    windDirection TEXT,
                    windSpeed INTEGER,
                    maxWindSpeed REAL,
                    cloudy REAL,
                    rain REAL,
                    humidity REAL,
                    snow REAL,
                    FOREIGN KEY (cityId) 
                        REFERENCES cityInfo (id)
                        ON UPDATE CASCADE
                        ON DELETE RESTRICT
                );
                """)
        
def createCurrentDataTable(db):
    with db:
        db.execute("""
                CREATE TABLE IF NOT EXISTS current (
                    cityId INTEGER,
                    date TIMESTAMP,
                    temp REAL,
                    windSpeed INT,
                    windDirection REAL,
                    humidity REAL,
                    rainSnowFall REAL,
                    pressure REAL,
                    PRIMARY KEY (cityId, date)
                    FOREIGN KEY (cityId)
                        REFERENCES cityInfo (id)
                        ON UPDATE CASCADE
                        ON DELETE RESTRICT
                );
                """)


def createCityInfoTable(db):
    with db:
        db.execute("""
                CREATE TABLE IF NOT EXISTS cityInfo (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name STRING NOT NULL,
                    pageAddress STRING NOT NULL,
                    nameInCurrent STRING
                );
                """)

def initCityInfoTable(db, data):
    sql = "INSERT OR IGNORE INTO cityInfo (name, pageAddress, nameInCurrent) VALUES (?,?,?);"
    with db:
        db.executemany(sql, data)

def createLogTable(db):
    with db:
        db.execute("""
                   CREATE TABLE IF NOT EXISTS logs (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                       info STRING NOT NULL,
                       times INTEGER
                );
                """)        
        
def createPickleTable(db):
    with db:
        db.execute("""
                   CREATE TABLE IF NOT EXISTS pickles (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                       pickle STRING NOT NULL,
                       pickleType STRING NOT NULL
                   )
                   """)
        
#----------------------------------------------------------------------

def getCityNameLink():
    with open('pageAddresses.txt', 'r') as file:
        lines = file.readlines()
        
    return [line[47:].split(',')[0] for line in lines], [line[:-1] for line in lines]

def getCurrentCityNames():
    with open('helperFileToCityInfoTable.txt', 'r', encoding='utf8') as file:
        lines = [line.replace('\n', '') for line in file.readlines()]
        
    return [line for line in lines if not line.startswith('//') and len(line)]

#----------------------------------------------------------------------

def setup(dbName):
    db = sqlite3.connect(dbName)
    
    cityNames, cityLinks = getCityNameLink()
    currentCityNames = getCurrentCityNames()

    createDataTable(db)
    createCurrentDataTable(db)
    createLogTable(db)
    createPickleTable(db)
    createCityInfoTable(db)

    initCityInfoTable(db, list(zip(cityNames, cityLinks, currentCityNames)))

    db.close()
