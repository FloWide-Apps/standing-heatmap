import streamlit as st
from streamlit_flowide import HeatMap
import requests as rq
import os 
import pandas as pd
import time
import psycopg2
import RobustMotionModel
from shapely.geometry import Polygon

DCM = ''

mapConfig = {}

zones = { }

conn = psycopg2.connect("dbname= host= user= password=")


tags = { t:{} for t in map(
    lambda e:e["primaryId"],
    rq.get(f"{DCM}/generalTags").json()
)}
st.sidebar.header("Tags")
for tagId, tag in tags.items():
    tag['checked'] = st.sidebar.checkbox(f"{tagId}")



def get_data_from_db(cur,tagId,from_epoch,durationSec):    
    query = f"""
        SELECT (EXTRACT(EPOCH FROM ts)*1000.0)::BIGINT,px,py,primaryId,ts FROM (SELECT unnest(position_Ts) As ts,unnest(position_x) AS px,unnest(position_y) AS py, primaryId 
        FROM locations 
        WHERE primaryId='{tagId}' AND (tsrange(timefrom::TIMESTAMP,timeto::TIMESTAMP) && 
        tsrange(to_timestamp({str(from_epoch)})::TIMESTAMP,to_timestamp({str(from_epoch)})::TIMESTAMP+ INTERVAL '{str(durationSec)} sec')) AND 
        position_ts IS NOT NULL) AS t WHERE ts > to_timestamp({str(from_epoch)})
        AND ts < (to_timestamp({str(from_epoch)})::TIMESTAMPTZ + INTERVAL '{str(durationSec)} sec')
        ORDER BY ts
    """
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(),columns=["measurementTime","posx","posy","primaryId","ts"])
    del df["ts"]
    df["measurementTime"] = df["measurementTime"].apply(lambda x:pd.Timestamp(x,unit='ms'))
    df = df.set_index(["measurementTime"])
    return df

def get_stable_positions(df):
    minTimeIntervallUsedToDecideStableState = pd.Timedelta('10s')
    minNumberOfPointsUsedToDecideStableState = 15

    splineKnotDensityMultiplier = 1. / 3.
    rollingMeanOffset = pd.Timedelta('300s')
    thresholdForSplineSplitting = pd.Timedelta(2.0, unit="s")
    stableEstimateStrategy = 'constantValue'
    stableRansacKwargsDict = {'max_trials': 100, 'min_samples': 0.4, 'residual_threshold': 1.0 * 1.0}
    splineRansacKwargsDict = {'max_trials': 1000, 'min_samples': 0.8, 'residual_threshold': 1. * 1.}

    resultDf, motionModelsByTime, motionModels = RobustMotionModel.makeRobustMotionModel(
      df, 
      zones,
      minTimeIntervallUsedToDecideStableState,
      minNumberOfPointsUsedToDecideStableState,
      stableEstimateStrategy='constantValue',
      rollingMeanOffset=rollingMeanOffset,
      splineKnotDensityMultiplier=splineKnotDensityMultiplier,
      thresholdForSplineSplitting=thresholdForSplineSplitting,
      stableRansacKwargsDict=stableRansacKwargsDict,
      splineRansacKwargsDict=splineRansacKwargsDict)

    return resultDf[resultDf["isStable"]]


def create_heatmap_data(df):
    count = len(df)
    data = []
    for row in df.itertuples():
        data.append([
            row.posx,row.posy,3/count
        ])

    return data



dateselect = st.sidebar.date_input('Date')
time_hh = st.sidebar.number_input("Hour",0,23,1)
time_mm = st.sidebar.number_input("Minutes",0,59,1)
duration = st.sidebar.number_input("Duration [min]")
time_object = time.strptime(str(dateselect), "%Y-%m-%d")
from_epoch = int((time.mktime(time_object) + time_hh*3600 + time_mm*60))
durationSec = duration*60

cur = conn.cursor()

for tagId,tag in tags.items():
    if not tag["checked"]:
        continue

    st.header(tagId)
    df = get_data_from_db(cur,tagId,from_epoch,durationSec)

    HeatMap(mapConfig,data=create_heatmap_data(df),key=tagId)


cur.close()
conn.close()
