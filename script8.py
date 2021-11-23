import os
import re
import time
import datetime
import requests
import pandas as pd
from tqdm import tqdm
from numpy import ceil
from bs4 import BeautifulSoup
import pickle


class Saver:
    def __init__(self, dataDirectory = './data', logFile = './logFile.txt', pickleDirectory = './pickles'):
        self.dataDirectory = dataDirectory
        self.logFile = logFile
        self.pickleDirectory = pickleDirectory
        
    def saveData(self, dataFrame, cityName):
        path = self.dataDirectory + self.getCityPath(cityName) + self.getDataFileName()
        dataFrame.to_csv(path)
        
    def saveCurrentData(self):
        currentWeather = requests.get('https://danepubliczne.imgw.pl/api/data/synop/format/csv').text.split('\n')[:-1]
        pd.DataFrame([item.split(',') for item in currentWeather[1:]], columns=currentWeather[0].split(',')).to_csv(self.dataDirectory + '/current' + self.getDataFileName())
    
        
    def getCityPath(self, cityName):
        retPath = f'/{cityName}'
        if not os.path.isdir(self.dataDirectory + retPath):
            os.mkdir(self.dataDirectory + retPath)
        return retPath
    
    def getDateTime(self, withSeconds=False):
        date = datetime.datetime.now()
        if withSeconds:
            return date.strftime('%y-%m-%d %H:%M:%S')
        return date.strftime('%y-%m-%d_%H.%M')
    
    def getDataFileName(self):
        return '/' + self.getDateTime() + '.csv'
    
    def writeLog(self, info, exception=False):
        if exception and self.isInLastLog(info):
            self.incrementNumberInLastLog()
        else:
            with open(self.logFile, 'a') as file:
                file.write(self.getDateTime(withSeconds=True) + '\t' + info + '\n')
        
    def getPickleFileName(self):
        return  '/' + self.getDateTime() + '.pkl'
            
    def savePickles(self, objects):
        if not type(objects) is list:
            objects = [objects]
            
        for object in objects:
            with open(self.pickleDirectory + self.getPickleFileName(), 'wb') as file:
                pickle.dump(object, file)
            
    def isInLastLog(self, strToFind):
        with open(self.logFile, 'r') as file:
            return strToFind in file.readlines()[-1]
        
    def incrementNumberInLastLog(self):
        with open(self.logFile, 'r+') as file:
            lines = file.readlines()
            splittedLastLine = lines[-1].split()
            lines[-1] = ' '.join(splittedLastLine[:-1]) + ' ' + str(int(splittedLastLine[-1][:-1]) + 1) + 'x'
            file.seek(0)
            file.writelines(lines)
            
    

def getValues(x):
    if x is None:
        return None
    res = re.search(r'-?\d?\.?\d{1,2}', x)
    if not res is None:
        return float(res.group())
    return None


def getCurrentTime(withSeconds = False):
    currentTime = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) + datetime.timedelta(hours=2)
    if withSeconds:
        return currentTime
    return currentTime.replace(second=0)

    


saver = Saver()
infoToLogs = ['started downloading', 'ended downloading']
runEvery = 30
colNames = ['hour', 'temp', 'sensedTemp', 'cover', 'windDirection', 'windSpeed', 'maxWindSpeed', 'cloudy%', 'rain', 'humidity%']
monthDict = {'Stycznia':1, 'Lutego': 2, 'Marca': 3, 'Kwietnia':4, 'Maja':5, 'Czerwca':6, 'Lipca':7, 'Sierpnia':8, 'Września':9, 'Października':10, 'Listopada':11, 'Grudnia':12}


with open('./pageAddresses.txt') as file:
    pageAddresses = file.readlines()
    
    
    
currentTime = getCurrentTime(withSeconds=True)
sleepTime = datetime.timedelta(minutes=int(ceil((currentTime.minute+1)/runEvery)*runEvery - currentTime.minute), seconds=-currentTime.second).seconds
saver.writeLog('starting in: ' + str(sleepTime) + ' seconds')
time.sleep(sleepTime)
    

repIndex = 0

while(True):
    repIndex += 1
    saver.writeLog('loop nr.' + str(repIndex) + '\t' + infoToLogs[0])
    for pageAddress in tqdm(pageAddresses):
        try:
            response = BeautifulSoup(requests.get(pageAddress).text, 'html.parser')


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
            df['date'] = pd.Series(pd.date_range(date, periods=120, freq='H'))
            df = df[[df.columns.tolist()[-1], *df.columns.tolist()[:-1]]]
            df = df.drop('hour', axis=1)

            
            cityToFileName = response.find(class_ = 'weather-currently-city').text.replace(' ', '')
            saver.saveData(df, cityToFileName)

            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            saver.writeLog('run into exception due to requests: ' + str(e), exception=True)
            time.sleep(2)
        except:
            saver.savePickles([response, df])
            saver.writeLog('run into exception due to incorrect data: ' + str(e), exception=True)
            time.sleep(2)
        
    try:
        saver.saveCurrentData()
    except:
        saver.writeLog("can't save current data")
    
    currentTime = getCurrentTime(withSeconds=True)
    sleepTime = datetime.timedelta(minutes=int(ceil((currentTime.minute+1)/runEvery)*runEvery - currentTime.minute), seconds=-currentTime.second).seconds
    saver.writeLog('loop nr.' + str(repIndex) + '\t' + infoToLogs[1] + '\tsleep for next: ' + str(sleepTime))
    time.sleep(sleepTime)
    
    