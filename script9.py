import re
import os
import sys
import time
import pickle
import sqlite3
import datetime
import requests
import pandas as pd
from tqdm import tqdm
from numpy import ceil, isnan
from setup import setup
from bs4 import BeautifulSoup


databaseFile = './weather.db'
if len(sys.argv) == 2:
    databaseFile = sys.argv[1] if sys.argv[1].endswith('.db') else sys.argv[1] + '.db'
    
if not os.path.exists(databaseFile):
    setup(databaseFile)

class Database:
    def __init__(self, databaseFile):
        self.databaseFile = databaseFile
        self.db = sqlite3.connect(self.databaseFile, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.dbCon = self.db.cursor()
        
    def getColFromTable(self, colName, tableName, where = None):
        if type(colName) is list:
            self.dbCon.execute(f"SELECT {', '.join(colName)} FROM {tableName}{' WHERE ' + where if not where is None else ''};")
            return self.dbCon.fetchall()
        
        self.dbCon.execute(f"SELECT {colName} FROM {tableName}{' WHERE ' + where if not where is None else ''};")
        return [item[0] for item in self.dbCon.fetchall()]
        
    def saveData(self, data, cityId):
        sql = f'INSERT INTO weather VALUES( NULL, {cityId}{",?" * 12});'
        for record in data:
            if not self.sameRecordExist(record, cityId):
                self.dbCon.execute(sql, record)
        self.db.commit()
        
    def sameRecordExist(self, record, cityId):
        date = record[1]
        sql = f"""
              SELECT *
              FROM weather
              WHERE id = (SELECT MAX(id) FROM weather
                            WHERE cityId = {cityId} AND date = '{date}')
              """
        self.dbCon.execute(sql)
        res = self.dbCon.fetchall()
        if not len(res):
            return False
        if self.areDifferent(res[0][4:], record[2:]):
            return False
        return True
    
    def areDifferent(self, recordFromDb, recordFromPandas):
        numericIndexes = [0,1,4,5,6,7,8,9]
        for index, (itemDb, itemPd) in enumerate(zip(recordFromDb, recordFromPandas)):
            if itemDb is None:
                if not isnan(itemPd):
                    return True
                continue
            if index in numericIndexes:
                if float(itemDb) != float(itemPd):
                    return True
            elif itemDb != itemPd:
                return True
        return False
        
    def saveCurrentData(self, data):
        sql = f'INSERT OR IGNORE INTO current VALUES ({",".join("?"*8)});'
        self.dbCon.executemany(sql, data)
        self.db.commit()
        
    #---------------------------------------------------------------------------
    
    def writeLog(self, info, exception=False):
        if exception and self.isInLastLog(info):
            self.incrementNumberInLastLog()
            return
        
        data = [info, "NULL" if not exception else 1]
        
        sql = f"INSERT INTO logs (info, times) VALUES (?,?);"
        self.dbCon.execute(sql, data)
        self.db.commit()
        
    def isInLastLog(self, info):
        sql = 'SELECT info FROM logs WHERE id = (SELECT MAX(id) FROM logs);'
        self.dbCon.execute(sql)
        lastInfo = self.dbCon.fetchall()[0][0]
        return info == lastInfo
    
    def incrementNumberInLastLog(self):
        self.dbCon.execute("""
                           UPDATE logs
                           SET times = times + 1
                           WHERE id = (SELECT MAX(id) FROM logs)
                           """)
        
    def savePickle(self, object):
        sql = 'INSERT INTO pickles (pickle, pickleType) VALUES (?,?);'
        objectPickled = pickle.dumps(object)
        self.dbCon.execute(sql, (objectPickled, str(type(object))))
        self.db.commit()

            
    

def getValues(x):
    if x is None:
        return None
    res = re.search(r'-?\d?\.?\d{1,2}', x)
    if not res is None:
        return float(res.group())
    return None


def getCurrentTime(withSeconds = False):
    currentTime = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) + datetime.timedelta(hours=1)
    return currentTime if withSeconds else currentTime.replace(second=0)

def getGatherDate():
    date = getCurrentTime()
    date = date.replace(minute = 0 if date.minute < 30 else 30, tzinfo=None)
    return date


def formatDate(date, time):
    # date in format yyyy-mm-dd
    # time as a number of hours
    dateSplitted = [int(item) for item in date.split('-')]
    date = datetime.datetime(dateSplitted[0], dateSplitted[1], dateSplitted[2], int(time))
    date = date.replace(minute= 0 if date.minute < 30 else 30)
    return date


def getCurrentData(database):
    nameIdDict = dict(database.getColFromTable(['nameInCurrent', 'id'], 'cityInfo'))
    
    response = requests.get('https://danepubliczne.imgw.pl/api/data/synop/format/csv').text.split('\n')[:-1]
    df = pd.DataFrame([item.split(',') for item in response[1:]], columns=response[0].split(','))
    df['stacja'] = df['stacja'].apply(lambda x: nameIdDict.get(x))
    df = df.dropna()
    df.loc[:, ['stacja', 'predkosc_wiatru', 'kierunek_wiatru']] = df.loc[:, ['stacja', 'predkosc_wiatru', 'kierunek_wiatru']].astype(int)
    df.loc[:,['temperatura', 'wilgotnosc_wzgledna', 'suma_opadu', 'cisnienie']] = df.loc[:,['temperatura', 'wilgotnosc_wzgledna', 'suma_opadu', 'cisnienie']].astype(float)
    df['data'] = df[['data_pomiaru', 'godzina_pomiaru']].apply(lambda x: formatDate(x[0], x[1]),axis=1)
    dataList = df[['stacja', 'data', 'temperatura', 'predkosc_wiatru', 'kierunek_wiatru', 'wilgotnosc_wzgledna', 'suma_opadu', 'cisnienie']].sort_values('stacja').values.tolist()
    dataList = [[item[0], item[1].to_pydatetime(), *item[2:]] for item in dataList]
    return dataList


database = Database(databaseFile)
infoToLogs = ['started downloading', 'ended downloading']
runEvery = 30
colNames = ['hour', 'temp', 'sensedTemp', 'cover', 'windDirection', 'windSpeed', 'maxWindSpeed', 'cloudy%', 'rain', 'humidity%']
monthDict = {'Stycznia':1, 'Lutego':2, 'Marca':3, 'Kwietnia':4, 'Maja':5, 'Czerwca':6, 'Lipca':7, 'Sierpnia':8, 'Września':9, 'Października':10, 'Listopada':11, 'Grudnia':12}



currentTime = getCurrentTime(withSeconds=True)
sleepTime = datetime.timedelta(minutes=1+int(ceil((currentTime.minute+1)/runEvery)*runEvery - currentTime.minute), seconds=-currentTime.second).seconds
database.writeLog(f'starting in: {sleepTime} seconds')
database.writeLog(f'Using database: {databaseFile}')
time.sleep(sleepTime)
    

repIndex = 0

while(True):
    repIndex += 1
    database.writeLog(f'loop nr.{repIndex}\t{infoToLogs[0]}')
    
    cityIdList = database.getColFromTable('id', 'cityInfo')
    pageAddresses = database.getColFromTable('pageAddress', 'cityInfo')
    
    for cityId, pageAddress in tqdm(list(zip(cityIdList, pageAddresses))):
        try:
            basicResponse = requests.get(pageAddress)
            response = BeautifulSoup(basicResponse.text, 'html.parser')
            rawDataList = [re.sub(r'\n+', ',', item.text.replace(',','.')).split(',') for item in response.find_all(class_ = 'weather-entry')]
            df = pd.DataFrame(rawDataList)

            if len(df.columns) == 18:
                indexesToDrop = [0,7,9,11,16,17]
                indexesToGetValues = [2,3,8,10,12,13,14]
                namesForColumns = colNames + ['snow']
                
                indexesToSwitch = []
                for index,item in enumerate([re.search('\d', item) if not item is None else None for item in df[15]]):
                    if not item is None:
                        indexesToSwitch.append(index)

                df.loc[indexesToSwitch, [13,15]] = df.loc[indexesToSwitch, [15,13]].to_numpy()
                df = df.drop(15,axis=1)
                
            else:
                df['snow'] = None
                indexesToDrop = [0,7,9,11,14,15]
                indexesToGetValues = [2,3,8,10,12,13]
                namesForColumns = colNames

            df = df.drop(indexesToDrop, axis=1)


            df[1] = df[1].apply(lambda x: int(int(x)/100))
            df[indexesToGetValues] = df[indexesToGetValues].applymap(getValues)


            df.columns = namesForColumns


            todaysDate = response.find(class_ ="weather-forecast-hbh-day-labelRight").text.split(',')[1][1:].split() 
            # format of todaysDate: ['dd', 'monthNameInPolish']

            date = datetime.datetime(getCurrentTime().year, monthDict[todaysDate[1]], int(todaysDate[0]), df.iat[0,0])
            df['date'] = pd.Series(pd.date_range(date, periods=120, freq='H'))#.apply(lambda x: x.to_pydatetime().replace(tzinfo=None))

            df = df.drop('hour', axis=1)

            df['gatherDate'] = getGatherDate()
            df = df[[df.columns.tolist()[-1], df.columns.tolist()[-2], *df.columns.tolist()[:-2]]]
            
            data = df.values.tolist()
            data = [[item[0].to_pydatetime(), item[1].to_pydatetime(), *item[2:]] for item in data]
            database.saveData(data, cityId)

            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            database.writeLog(f'run into exception due to requests: {e}', exception=True)
            time.sleep(2)
        except Exception as e:
            database.savePickle(basicResponse)
            database.savePickle(df)
            database.writeLog(f'run into exception due to incorrect data: {e}', exception=True)
            time.sleep(2)
        
    try:
        currentData = getCurrentData(database)
        database.saveCurrentData(currentData)
    except Exception as e:
        database.writeLog(f"can't save current data: {e}", exception = True)
    
    currentTime = getCurrentTime(withSeconds=True)
    sleepTime = datetime.timedelta(minutes=1+int(ceil((currentTime.minute+1)/runEvery)*runEvery - currentTime.minute), seconds=-currentTime.second).seconds
    database.writeLog(f'loop nr.{repIndex}\t{infoToLogs[1]}\tsleep for next: {sleepTime}')
    time.sleep(sleepTime)
    
    