## This file is compilation of notebook Flight_data_transformation.ipynb to to facilitate the process of data transformation

import random
import pandas as pd
import numpy as np
import datetime
from datetime import date
import requests
from bs4 import BeautifulSoup
def _airport_trainsformation(value):
        '''
        >>> airport_trainsformation('ATL')
        'ATL'
        >>> airport_trainsformation(11097)
        'unk'
        >>> airport_trainsformation(15497)
        'unk'
        '''
        if len(str(value))>3:
            return 'unk'
        else:
            return value
            
def _time_transformation(time):
    """
    >>> time_transformation(0)
    datetime.time(0, 0)
    >>> time_transformation(2400)
    datetime.time(0, 0)
    >>> time_transformation(2359)
    datetime.time(23, 59)
    >>> time_transformation(1200)
    datetime.time(12, 0)
    >>> time_transformation(0734.00)
    datetime.time(7, 34)
    >>> time_transformation(734.00)
    datetime.time(7, 34)
    >>> time_transformation(34.00)
    datetime.time(0, 34)
    >>> time_transformation(4.00)
    datetime.time(0, 4)
    """
    time_string = str(int(time))
    time_string = time_string.split('.')[0]
    time_string = time_string.zfill(4)
    hours = int(time_string[0:2])
    minutes = int(time_string[2:4])
    if hours == 24:
        hours = 0
    hours_minutes = datetime.time(hours,minutes)
    return hours_minutes
def _get_holidays():
    holidays_dates = list()
    URL = 'https://www.officeholidays.com/countries/usa/2015'
    response = requests.get(URL, timeout=20)
    soup = BeautifulSoup(response.text, features="lxml")
    for a in soup.find_all('time')[2:]:
        holidays_dates.append(datetime.datetime.strptime(a.attrs.get("datetime"),'%Y-%m-%d').date())
    return holidays_dates
def flight_data_transformer(data):
    data.drop(data[(data['CANCELLED']>0)].index, inplace=True)
    data = data.drop(['CANCELLED','CANCELLATION_REASON'], axis=1)
    data_without_nulls = data.drop(data[(data['DIVERTED']>0)].index)
    data_without_nulls = data_without_nulls.drop(['DIVERTED'], axis=1)
    Flights_Delays_Combined =  data_without_nulls.drop(['AIR_SYSTEM_DELAY','SECURITY_DELAY','AIRLINE_DELAY',
                        'LATE_AIRCRAFT_DELAY','WEATHER_DELAY'],axis = 1)
                        
    

    Flights_Delays_Combined['ORIGIN_AIRPORT'] = Flights_Delays_Combined['ORIGIN_AIRPORT'].apply(_airport_trainsformation)
    Flights_Delays_Combined['DESTINATION_AIRPORT'] = Flights_Delays_Combined['DESTINATION_AIRPORT'].apply(_airport_trainsformation)
    Flights_Delays_Combined = Flights_Delays_Combined.drop(Flights_Delays_Combined[(Flights_Delays_Combined['DESTINATION_AIRPORT']=='unk')].index)
    Flights_Delays_Combined = Flights_Delays_Combined.drop(Flights_Delays_Combined[(Flights_Delays_Combined['ORIGIN_AIRPORT']=='unk')].index)
    
      
    Flights_Delays_Combined['DEP_TIME'] = Flights_Delays_Combined['DEPARTURE_TIME'].apply(_time_transformation)
    Flights_Delays_Combined['SCH_ARR_TIME'] =Flights_Delays_Combined['SCHEDULED_ARRIVAL'].apply(_time_transformation)
    Flights_Delays_Combined['SCH_DEP_TIME'] =Flights_Delays_Combined['SCHEDULED_DEPARTURE'].apply(_time_transformation)
    Flights_Delays_Combined['ARR_TIME'] =Flights_Delays_Combined['ARRIVAL_TIME'].apply(_time_transformation)
    Flights_Delays_Combined['WHEELS_OFF'] =Flights_Delays_Combined['WHEELS_OFF'].apply(_time_transformation)
    Flights_Delays_Combined['WHEELS_ON'] =Flights_Delays_Combined['WHEELS_ON'].apply(_time_transformation)
    Flights_Delays_Combined['DATE'] = pd.to_datetime(Flights_Delays_Combined[['YEAR','MONTH','DAY']])
    Flights_Delays_Combined['DayOfWeek'] = Flights_Delays_Combined['DATE'].apply(date.weekday)
    Flights_Delays_Combined = Flights_Delays_Combined.drop(['YEAR','MONTH','DAY','DAY_OF_WEEK'], axis=1)

    holidays_dates = _get_holidays()
   

    Flights_Delays_Combined['Holidays'] = np.where(Flights_Delays_Combined['DATE'].isin(holidays_dates), 1, 0)
    
    
    Flights_Delays_Combined = Flights_Delays_Combined.drop(['DEPARTURE_TIME','SCHEDULED_ARRIVAL', 'SCHEDULED_DEPARTURE', 'ARRIVAL_TIME'], axis=1)
    
    return Flights_Delays_Combined
