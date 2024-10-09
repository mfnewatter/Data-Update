import pandas as pd
import datetime
from datetime import datetime as dt
import numpy as np
from regex import D
#import teradatasql

def set_dictionary():
    return r'C:\Users\newatter\OneDrive - McCain Foods Limited\Distributor Sell-Out\Data Dictionaries\\'

def set_time(DICTIONARY):
    return pd.read_excel(DICTIONARY + 'Time Definitions.xlsx')


def add_rolling(df, _list):
    #groupby _list
    df = df.groupby(_list, dropna = False)[['LBS','LBS_LY','LBS_Baseline']].sum().reset_index()
    
    #set index to all but last column in list
    df = df.set_index(_list)
    
    #add new metric SMA_4 (simple moving average - 4 periods)
    #level = all but last 2 items in list
    df['LBS_Lag_1'] = df.groupby(level=_list[0:-1])['LBS'].shift(periods = 1)
    df['LBS_Lag_2'] = df.groupby(level=_list[0:-1])['LBS'].shift(periods = 2)
    df['LBS_Lag_3'] = df.groupby(level=_list[0:-1])['LBS'].shift(periods = 3)
    df['LBS_Lag_4'] = df.groupby(level=_list[0:-1])['LBS'].shift(periods = 4)
    
    df['SMA_4'] = df.groupby(_list[0:-1])['LBS'].transform(lambda x: x.rolling(4, min_periods=1).mean())
    #df['SMA_4'] = df.groupby(level=_list[0:-1])['LBS'].apply(lambda x: x.rolling(4, min_periods=1).mean())
    df['SMA_8'] = df.groupby(_list[0:-1])['LBS'].transform(lambda x: x.rolling(8, min_periods=1).mean())
    df['SMA_12'] = df.groupby(_list[0:-1])['LBS'].transform(lambda x: x.rolling(12, min_periods=1).mean())
    
    df['SMA_4_LY'] = df.groupby(_list[0:-1])['LBS_LY'].transform(lambda x: x.rolling(4, min_periods=1).mean())
    df['SMA_8_LY'] = df.groupby(_list[0:-1])['LBS_LY'].transform(lambda x: x.rolling(8, min_periods=1).mean())
    df['SMA_12_LY'] = df.groupby(_list[0:-1])['LBS_LY'].transform(lambda x: x.rolling(12, min_periods=1).mean())
    
    df['SMA_4_Baseline'] = df.groupby(_list[0:-1])['LBS_Baseline'].transform(lambda x: x.rolling(4, min_periods=1).mean())
    df['SMA_8_Baseline'] = df.groupby(_list[0:-1])['LBS_Baseline'].transform(lambda x: x.rolling(8, min_periods=1).mean())
    df['SMA_12_Baseline'] = df.groupby(_list[0:-1])['LBS_Baseline'].transform(lambda x: x.rolling(12, min_periods=1).mean())
    
    df['LBS_Baseline_Lag_1'] = df.groupby(level=_list[0:-1])['LBS_Baseline'].shift(periods = 1)
    df['LBS_LY_Lag_1'] = df.groupby(level=_list[0:-1])['LBS'].shift(periods = 1)
    
    df['SMA_4_Lag_1'] = df.groupby(level=_list[0:-1])['SMA_4'].shift(periods = 1)
    df['SMA_4_LY_Lag_1'] = df.groupby(level=_list[0:-1])['SMA_4_LY'].shift(periods = 1)
    df['SMA_4_Baseline_Lag_1'] = df.groupby(level=_list[0:-1])['SMA_4_Baseline'].shift(periods = 1)
    
    return df.reset_index()


def add_last_year(df, _list, TIME):

    #list of groupby columns
    #last item in list is Calendar Week Year which is used to pull previous history (Baseline Week = Calendar Week Year) of copied dataframe
    _groupby = _list.copy()
    
    #remove last element (calendar week) in list and replace with YOY Week
    _merge_yoy = _list.copy()[0:-1]
    _merge_yoy.extend(['YOY Week'])

    #remove last element (calendar week) in list and replace with Baseline Week
    _merge_baseline = _list.copy()[0:-1]
    _merge_baseline.extend(['Baseline Week'])

    df = df.groupby(_list, dropna = False)['LBS'].sum().reset_index()

    df_yoy = df.copy().rename(columns={'LBS':'LBS_LY'})
    df_baseline = df.copy().rename(columns={'LBS':'LBS_Baseline'})

    df = df.merge(TIME[['Calendar Week Year','YOY Week','Baseline Week']], how='left', on='Calendar Week Year')

    df = df.merge(df_yoy, how='left', left_on=_merge_yoy, right_on=_groupby).drop(columns={'Calendar Week Year_y'}).rename(columns={'Calendar Week Year_x':'Calendar Week Year'})

    df = df.merge(df_baseline, how='left', left_on=_merge_baseline, right_on=_groupby).drop(columns={'Calendar Week Year_y'}).rename(columns={'Calendar Week Year_x':'Calendar Week Year'})

    del(df_yoy)
    del(df_baseline)

    return df


def add_precovid(df, _list, begin, end):
    #datefield should be last in _list
    datefield = _list[-1]
          
    #remove datefield from list
    _list = _list[0:-1]
    
    #filter data not using last and rename columns
    _df = df[(df[datefield] >= begin) & (df[datefield] <= end)].groupby(_list)['LBS'].sum() / 52
    
    return df.merge(
        _df, how = 'left', left_on = _list, right_on = _list).rename(
        columns = {'LBS_x':'LBS', 'LBS_y':'LBS_PRECOVID'}).fillna(
        value = {'LBS_PRECOVID': 0})


def add_time(df, TIME):

    df = df.merge(TIME[['Calendar Week Year','YOY Week','Baseline Week','Week Starting (Sun)','Week Ending (Sat)', 'COVID Week']],
                   how = 'left', 
                   on = 'Calendar Week Year')
    
    return df


def restaurants(df):
    #restaurants = df.loc[df['COVID Segmentation - (Restaurants)'] == 'Restaurants', :]
    
    if df.columns.isin(['COVID Segmentation - L2']).sum() > 0:
        #Rename rows
        df.loc[df['COVID Segmentation - L2'] == 'Independents (IOs) / Local Eateries / Takeaway', 'COVID Segmentation - L2'] = 'IO'
        df.loc[
            (df['COVID Segmentation - L2'] == 'All Other') | 
            (df['COVID Segmentation - L2'] == 'National Account') | 
            (df['COVID Segmentation - L2'] == 'Region Chains')| 
            (df['COVID Segmentation - L2'] == 'National Accounts'),
            'COVID Segmentation - L2'] = 'Chain'
    
    return df


def full_dataframe(df, _list):
    #7/12/2022 - Was getting NaN and wouldn't join correctly
    if 'City' in df.columns:
        df['City'].fillna('NA', inplace=True)

    weeks = df.groupby(['Calendar Week Year']).size().reset_index().drop(columns={0})
    segments = df.groupby(_list[0:-1]).size().reset_index().drop(columns={0})

    _df = segments.assign(key=1).merge(weeks.assign(key=1), how='outer', on='key').drop(columns = {'key'}) 

    df = _df.merge(df, how = 'left', on = _list) 

    return df


def analyze(df, _list, begin, end):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)

    if 'Calendar Week Year' not in _list:
        _list.extend(['Calendar Week Year'])

    df = full_dataframe(df, _list)

    #add last year lbs
    df = add_last_year(df, _list, TIME)
    
    #add rolling calculation
    df = add_rolling(df, _list)
        
    #add preCOVID baseline
    df = add_precovid(df, _list, begin, end)
    
    df = add_time(df, TIME)

    df = df.round({
        'LBS' : 2,    
        'SMA_4' : 2,
        'SMA_8' : 2,
        'SMA_12' : 2,
        'LBS_LY' : 2,    
        'SMA_4_LY' : 2,
        'SMA_8_LY' : 2,
        'SMA_12_LY' : 2,
        'LBS_Baseline' : 2,    
        'SMA_4_Baseline' : 2,
        'SMA_8_Baseline' : 2,
        'SMA_12_Baseline' : 2,
        'LBS_PRECOVID' : 2,
        'LBS_Lag_1' : 2,
        'LBS_Lag_2' : 2,
        'LBS_Lag_3' : 2,
        'LBS_Lag_4' : 2,
        'LBS_Baseline_Lag_1': 2,
        'LBS_LY_Lag_1': 2,
        'SMA_4_Lag_1' : 2,
        'SMA_4_LY_Lag_1' : 2,
        'SMA_4_Baseline_Lag_1' : 2
        
    }).fillna(value = {
        'LBS' : 0,    
        'SMA_4' : 0,
        'SMA_8' : 0,
        'SMA_12' : 0,
        'LBS_LY' : 0,    
        'SMA_4_LY' : 0,
        'SMA_8_LY' : 0,
        'SMA_12_LY' : 0,
        'LBS_Baseline' : 0,    
        'SMA_4_Baseline' : 0,
        'SMA_8_Baseline' : 0,
        'SMA_12_Baseline' : 0,
        'LBS_PRECOVID' : 0,
        'LBS_Lag_1' : 0,
        'LBS_Lag_2' : 0,
        'LBS_Lag_3' : 0,
        'LBS_Lag_4' : 0,
        'LBS_Baseline_Lag_1': 2,
        'LBS_LY_Lag_1': 2,
        'SMA_4_Lag_1' : 0,
        'SMA_4_LY_Lag_1' : 0,
        'SMA_4_Baseline_Lag_1' : 0
    })

    del(TIME)

    return df


def clean_city(df):
    df['City'] = df['City'].str.strip()
    df['City'] = df['City'].str.upper()
    df['City'].fillna('NA', inplace = True)
    
    #cities = 'TORONTO|MONTREAL|OTTAWA|CALGARY|VANCOUVER|WINNIPEG|MONTREAL|HAMILTON|HALIFAX'
    cities = 'NOT USED CURRENTLY'
    
    #change each city name to the name of the city that matches, cleans up the city names
    for c in cities.split('|'):
        df.loc[df['City'].str.match(c), 'City'] = c
    
    #change all other cities to NA
    df.loc[~df['City'].str.match(cities), 'City'] = 'NA'
    
    return df



def process_list(df, work_list, distributor):

    _process = analyze(df, work_list, 201910, 202009)

    _process = restaurants(_process)
    
    _process['Distributor'] = distributor
    
    #for standardizing output
    work_list.extend(['Distributor','LBS','SMA_4','SMA_8','SMA_12',
                      'YOY Week','LBS_LY','SMA_4_LY','SMA_8_LY','SMA_12_LY',
                      'Baseline Week','LBS_Baseline','SMA_4_Baseline','SMA_8_Baseline','SMA_12_Baseline',
                      'LBS_Lag_1','LBS_Lag_2','LBS_Lag_3','LBS_Lag_4','LBS_Baseline_Lag_1','LBS_LY_Lag_1',
                      'SMA_4_Lag_1', 'SMA_4_LY_Lag_1', 'SMA_4_Baseline_Lag_1',
                      'LBS_PRECOVID','Week Starting (Sun)','Week Ending (Sat)','COVID Week'])
        
    return _process[work_list]