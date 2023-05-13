from os import environ
from sqlalchemy.exc import OperationalError
import asyncio, json, datetime
from time import time, sleep
from datetime import datetime,timedelta
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table, Column, Integer, String, MetaData, and_, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import pandas as pd
import numpy as np


print('Waiting for the data generator...')
sleep(30)
print('ETL Starting...')



while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')



# Write the solution here
def date_to_timestamp(date_str):
    date_obj =datetime.datetime.strptime(date_str, '%Y-%m-%d')
    timestamp = int(date_obj.timestamp())
    return timestamp
def datetime_to_timestamp(date_str, time_str):
    datetime_str = date_str + ' ' + time_str
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    timestamp = int(datetime_obj.timestamp())
    return timestamp
def datetime_to_timestamp_2(datetime_str):
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    timestamp = int(datetime_obj.timestamp())
    return timestamp
# function to calculate distance between two points
def distance(lat1, lon1, lat2, lon2):
    lat1 = lat1.apply(lambda x: float(x))
    lat2 = lat2.apply(lambda x: float(x))
    lon1 = lon1.apply(lambda x: float(x))
    lon2 = lon2.apply(lambda x: float(x))
    return np.arccos(np.sin(np.radians(lat1)) * np.sin(np.radians(lat2)) + 
                     np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.cos(np.radians(lon2 - lon1))) * 6371
def aggregate_function(data):
    df = pd.DataFrame(data)
    df = df.join(pd.json_normalize(df['location'].map(json.loads).tolist())).drop(['location'], axis=1)
    for i, row in df.iterrows():
        df['datetime'] = pd.to_datetime(df['time'], unit='s')
        df['date']  = pd.to_datetime(df['datetime'], format="%m/%d/%Y").dt.floor('D')
        df['hour'] = df['datetime'].dt.strftime('%H:00:00')
        df['date'] =  df['date'].astype(str)
    groups = df.groupby(['device_id','date','hour']).agg({'temperature': ['max'] , 'device_id': ['count']})
    groups.columns=groups.columns.droplevel(0)
    groups.reset_index(inplace=True)
    # group dataframe by device_id and calculate total distance for each device
    total_distance = df.groupby('device_id').apply(lambda x:np.sum(distance(x['latitude'].shift(), x['longitude'].shift(), x['latitude'], x['longitude']))).reset_index(name='total_distance')
    groups = groups.merge(total_distance,on='device_id')
    #change Column compatible to Table Columns
    groups.columns = [ 'device_id', 'date','hour','max','count_','total_distance']
    groups.to_sql(name='devices_results', con=mysql_engine, if_exists='append', index=False)

    
#object of Create Mysql Devices_Results table 
while True:
    try:
        metadata_obj = MetaData()
        devices_results = Table(
            'devices_results', metadata_obj,
            Column('device_id', String(100)),
            Column('date', String(30)),
            Column('hour', String(20)),
            Column('max', Integer),
            Column('count_', Integer),
            Column('total_distance', String(100))
            
        )
        metadata_obj.create_all(mysql_engine)
        break
    except OperationalError:
        sleep(0.1)
  
 #create declarative base
Base = declarative_base()

# define table
class Devices_Results(Base):
    __tablename__ = 'devices_results'
    id = Column(Integer, primary_key=True)
    device_id = Column(String(255))
    date= Column(String(20))
    hour= Column(String(20))
    max = Column(Integer)
    count_ = Column(Integer)
    total_distance= Column(String(100))    
                
# define table
class Devices(Base):
    __tablename__ = 'devices'
    id = Column(Integer, primary_key=True)
    device_id = Column(String)
    temperature = Column(Integer)
    location = Column(String)
    time = Column(String)
    
while True:
    # Create a session factory
    Session = sessionmaker(bind=psql_engine)
    session_psql = Session()
    max_time = session_psql.query(func.max(Devices.time)).scalar()
    if (max_time):
        dt_max = datetime.fromtimestamp(int(max_time))
        dt_max_minus_1 = dt_max - timedelta(hours=1) 
        max_hour = dt_max_minus_1.hour
        dt_max_hour_Date = dt_max_minus_1.strftime('%Y-%m-%d')
        #   check last records in Mysql DB
        query = 'SELECT MAX(date),MAX(hour) FROM devices_results where date = (select max(date) from devices_results)'
        df = pd.read_sql_query(query, mysql_engine)
        if(df.iloc[0,0]):
            final_time = df.iloc[0,0] + ' ' + df.iloc[0,1]
            currnet_timestamp = datetime_to_timestamp_2(final_time)
            data = session_psql.query(Devices.device_id,Devices.temperature,Devices.location,Devices.time).filter(and_(Devices.time > str(currnet_timestamp)), Devices.time <= str(datetime_to_timestamp(dt_max_hour_Date,str(max_hour)+':00:00'))).all()
            if(data):
                aggregate_function(data)
                continue
        else:
            data = session_psql.query(Devices.device_id,Devices.temperature,Devices.location,Devices.time).filter(and_(Devices.time <= str(datetime_to_timestamp(dt_max_hour_Date,str(max_hour)+':00:00')))).all()
            aggregate_function(data)
            continue
    else :
        sleep(3600)
        continue