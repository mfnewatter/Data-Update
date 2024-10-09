import pandas as pd
import datetime
from datetime import datetime as dt
import numpy as np
import teradatasql
from sellout_model import set_time, set_dictionary, analyze

DICTIONARY = set_dictionary()
TIME = set_time(DICTIONARY)

def td_to_pandas(query, cur, title=''):
    _data = []
    _start=dt.now()
    print(dt.now().strftime('%m/%d/%Y'))
    print(f'{title} Execution started...', end='', flush=True)
    cur.execute (query)
    print(f'finished. {dt.now() - _start}', flush=True) 
    _start_fetch=dt.now()
    print(f'{title} Fetching data started...', end='', flush=True)
    for row in cur.fetchall():
        _data.append(row) 
    print(f'finished. {dt.now() - _start_fetch}', flush=True) 
    _start=dt.now()
    print(f'{title} Creating DataFrame for started...', end='', flush=True)
    _df = pd.DataFrame(_data)
    _df.columns = [x[0].replace('SAP_', '').lower() for x in cur.description]
    print(f'finished. {dt.now() - _start}', flush=True)
    return _df


def td_dataframe(select_db, query):
    with teradatasql.connect(None, 
                         host='172.29.3.43',
                         user='PNWATTERS',
                         password='teradata123') as con:
        with con.cursor() as cur:
            cur.execute (select_db)
            print('Database selected!', flush=True)            
            dim_df = td_to_pandas(query, cur, 'Query:')
            print('Dim:', dim_df.shape)
    
    return dim_df


def teradata_sales(sellout, distributor, distributor_name):
    #SET QUERY_BAND = 'ApplicationName=MicroStrategy;Version=9.0;ClientUser=NEWATTER;Source=Vantage; Action=BEK Performance;StartTime=20200901T101924;JobID=55096;Importance=666;'  FOR SESSION;
    
    #the current week is pulled from the time dictionary table
    to_week = int(TIME[(TIME['Week Starting (Mon)'] <= dt.now()) & (TIME['Week Ending (Sun)'] >= dt.now())]['Calendar Week Year'].values)
    
    print(f'Starting Teradata connect...', flush = True)
    
    select_db = "DATABASE DL_GBL_TAS_BI"

    query = '''
    select a14.FISCAL_WEEK_NUMBER as FISCAL_WEEK_NUMBER,
        (a14.FISCAL_WEEK_NUMBER_DESCR || ' ' || a14.START_DATE_OF_SAPYW) as FISCAL_WEEK,
        a14.CALENDAR_WEEK_NAME as CALENDAR_WEEK_NUMBER,
        (a14.CALENDAR_WEEK_LONG_DESCRIPTION || ' ' || a14.START_DATE_OF_SAPYW) as CALENDAR_WEEK,
        RIGHT(a16.CUSTOMER_HIER_LVL_1,CAST(10 AS INTEGER)) as CUSTOMER_HIER_LVL_1,
        a16.CUSTOMER_HIER_LVL_1_NAME as CUSTOMER_HIER_LVL_1_NAME,
        a17.DIVISION_NAME as DIVISION_NAME,
        a12.CATEGORY_DESC as CATEGORY_DESC,
        a12.SUB_CATEGORY_DESC as SUB_CATEGORY_DESC,
        a13.PRODUCT_GROUP_FORMAT_DESC as L1_PRODUCT_HIERARCHY,
        a13.PRODUCT_GROUP_SUB_FORMAT_DESC as L2_PRODUCT_HIERARCHY,
        a15.MATERIAL_PRICING_GROUP_ID as MATERIAL_PRICING_GROUP_ID,
        a18.MATERIAL_PRICING_GROUP_DESCRIPTION as MATERIAL_PRICING_GROUP_DESCRIPTION,
        TRIM (LEADING '0' FROM a13.MATERIAL_ID) as MATERIAL_ID,
        a13.MATERIAL_DESCRIPTION as MATERIAL_NAME,
        sum(a11.SALES_VOLUME_WEIGHT_LBS) as ACTUAL_VOLUME_LBS
    from DL_GBL_TAS_BI.FACT_SALES_ACTUAL as a11
    join DL_GBL_TAS_BI.VW_H_PRODUCT_ALL_SALES as a12
        on (a11.MATERIAL_ID = a12.MATERIAL_ID)
    join DL_GBL_TAS_BI.D_MATERIAL_DN_ALL as a13
        on (a11.MATERIAL_ID = a13.MATERIAL_ID)
    join DL_GBL_TAS_BI.D_TIME_FY_V6 as a14
        on (a11.ACCOUNTING_PERIOD_DATE = a14.DAY_CALENDAR_DATE)
    join DL_GBL_TAS_BI.D_MATERIAL_SALES_DATA as a15
        on (a11.DISTRIBUTION_CHANNEL_ID = a15.DISTRIBUTION_CHANNEL_ID and 
        a11.MATERIAL_ID = a15.MATERIAL_ID and 
        a11.SALES_ORGANISATION_ID = a15.SALES_ORGANISATION_ID)
    join DL_GBL_TAS_BI.VW_H_CUSTOMER_ALL_DIVISION00 as a16
        on (a11.CUSTOMER_ID = a16.CUSTOMER and 
        a11.DISTRIBUTION_CHANNEL_ID = a16.DISTRIBUTION_CHANNEL and 
        a11.SALES_ORGANISATION_ID = a16.SALES_ORGANISATION)
    join DL_GBL_TAS_BI.D_DIVISION as a17
        on (a13.DIVISION_ID = a17.DIVISION_ID)
    join DL_GBL_TAS_BI.D_MATERIAL_PRICING_GROUP as a18
        on (a15.MATERIAL_PRICING_GROUP_ID = a18.MATERIAL_PRICING_GROUP_ID)
    where (a14.FISCAL_YEAR_CODE in ('FY2019', 'FY2020', 'FY2021','FY2022')
        and a11.SALES_ORGANISATION_ID in ('US01')
        and a11.DISTRIBUTION_CHANNEL_ID in ('10')
        and RIGHT(a16.CUSTOMER_HIER_LVL_1,CAST(10 AS INTEGER)) in (''' + "'" + str(distributor) + "'" + '''))
        and a14.CALENDAR_WEEK_NAME < ''' + str(to_week) + ''' 
    group by a14.FISCAL_WEEK_NUMBER,
        (a14.FISCAL_WEEK_NUMBER_DESCR || ' ' || a14.START_DATE_OF_SAPYW),
        a14.CALENDAR_WEEK_NAME,
        (a14.CALENDAR_WEEK_LONG_DESCRIPTION || ' ' || a14.START_DATE_OF_SAPYW),
        RIGHT(a16.CUSTOMER_HIER_LVL_1,CAST(10 AS INTEGER)),
        a16.CUSTOMER_HIER_LVL_1_NAME,
        a17.DIVISION_NAME,
        a12.CATEGORY_DESC,
        a12.SUB_CATEGORY_DESC,
        a13.PRODUCT_GROUP_FORMAT_DESC,
        a13.PRODUCT_GROUP_SUB_FORMAT_DESC,
        a15.MATERIAL_PRICING_GROUP_ID,
        a18.MATERIAL_PRICING_GROUP_DESCRIPTION,
        TRIM (LEADING '0' FROM a13.MATERIAL_ID),
        a13.MATERIAL_DESCRIPTION
    ;'''
    
    #create dataframe using both functions td_to_pandas and td_dataframe
    df = td_dataframe(select_db, query)
    
    return teradata_transform(df, sellout, distributor_name)


def teradata_transform(sellin, sellout, distributor_name):
    #consolidates teradata sales with sellout data
    
    #convert from object datatype to float (exports as a number instead of string)
    sellin['actual_volume_lbs'] = sellin['actual_volume_lbs'].astype('float64')
    
    #rename columns for consistancy
    sellin = sellin.rename(columns = {'actual_volume_lbs':'LBS', 
                                      'calendar_week_number':'Calendar Week Year',
                                      'l1_product_hierarchy':'L1 Product Hierarchy',
                                      'l2_product_hierarchy':'L2 Product Hierarchy'})
    
    #transform calendar week year from teradata
    sellin['Calendar Week Year'] = pd.to_numeric(sellin['Calendar Week Year'], errors = 'coerce')

    #transform category so its consolidated
    sellin['Consolidated Category'] = sellin['category_desc']
    sellin.loc[sellin['Consolidated Category'] == 'Sweet Potato' , 'Consolidated Category'] = 'Potato'
    sellin.loc[sellin['Consolidated Category'] != 'Potato' , 'Consolidated Category'] = 'Prepared Foods'
    
    #analyze sellin data
    sellin = analyze(sellin, ['Consolidated Category'], 201910, 202009)
    
    '''
    #   Column                 Non-Null Count  Dtype  
    ---  ------                 --------------  -----  
     0   Consolidated Category  296 non-null    object 
     1   Calendar Week Year     296 non-null    int64  
     2   MCCAIN LBS             296 non-null    float64
     3   MCCAIN SMA_4           296 non-null    float64
     4   MCCAIN SMA_8           296 non-null    float64
     5   MCCAIN SMA_12          296 non-null    float64
     6   LBS_Lag_1              296 non-null    float64
     7   LBS_Lag_2              296 non-null    float64
     8   LBS_Lag_3              296 non-null    float64
     9   LBS_Lag_4              296 non-null    float64
     10  YOY Week               242 non-null    float64
     11  Baseline Week          242 non-null    float64
     12  LBS_LY                 296 non-null    float64
     13  SMA_4_LY               296 non-null    float64
     14  SMA_8_LY               296 non-null    float64
     15  SMA_12_LY              296 non-null    float64
     16  LBS_Baseline           296 non-null    float64
     17  SMA_4_Baseline         296 non-null    float64
     18  SMA_8_Baseline         296 non-null    float64
     19  SMA_12_Baseline        296 non-null    float64
     20  MCCAIN PRECOVID        296 non-null    float64
    '''
    
    #rename columns accordingly
    sellin = sellin.rename(columns = {'LBS':'MCCAIN LBS',
                                      'SMA_4':'MCCAIN SMA_4',
                                      'SMA_8':'MCCAIN SMA_8',
                                      'SMA_12':'MCCAIN SMA_12',
                                      'LBS_PRECOVID':'MCCAIN PRECOVID',
                                      'LBS_Lag_1':'MCCAIN Lag_1',
                                      'LBS_Lag_2':'MCCAIN Lag_2',
                                      'LBS_Lag_3':'MCCAIN Lag_3',
                                      'LBS_Lag_4':'MCCAIN Lag_4',
                                      'LBS_Baseline' : 'MCCAIN LBS_Baseline',
                                      'SMA_4_Baseline' : 'MCCAIN SMA_4_Baseline',
                                      'SMA_8_Baseline' : 'MCCAIN SMA_8_Baseline',
                                      'SMA_12_Baseline' : 'MCCAIN SMA_12_Baseline',
                                      'SMA_4_Lag_1':'MCCAIN SMA_4_Lag_1',
                                      'SMA_4_Baseline_Lag_1' : 'MCCAIN SMA_4_Baseline_Lag_1',
                                      'LBS_Baseline_Lag_1': 'MCCAIN LBS_Baseline_Lag_1'
                                     })
    
    
    #analyze sellout data
    df = analyze(sellout, ['Consolidated Category'], 201910, 202009)
    
    
    df = df.merge(sellin[['Calendar Week Year','Consolidated Category','MCCAIN LBS','MCCAIN SMA_4','MCCAIN SMA_8','MCCAIN SMA_12','MCCAIN PRECOVID',
                          'MCCAIN LBS_Baseline','MCCAIN SMA_4_Baseline','MCCAIN SMA_8_Baseline','MCCAIN SMA_12_Baseline',
                          'MCCAIN Lag_1', 'MCCAIN Lag_2', 'MCCAIN Lag_3', 'MCCAIN Lag_4','MCCAIN LBS_Baseline_Lag_1',
                          'MCCAIN SMA_4_Lag_1', 'MCCAIN SMA_4_Baseline_Lag_1']], how = 'left', 
                  left_on = ['Calendar Week Year','Consolidated Category'], right_on = ['Calendar Week Year','Consolidated Category'])
    
    df = df.fillna({'MCCAIN LBS': 0,
                    'MCCAIN SMA_4': 0,
                    'MCCAIN SMA_8': 0,
                    'MCCAIN SMA_12': 0,
                    'MCCAIN PRECOVID': 0,
                    'MCCAIN Lag_1': 0,
                    'MCCAIN Lag_2': 0,
                    'MCCAIN Lag_3': 0,
                    'MCCAIN Lag_4': 0,
                    'MCCAIN LBS_Baseline': 0,
                    'MCCAIN SMA_4_Baseline': 0,
                    'MCCAIN SMA_8_Baseline': 0,
                    'MCCAIN SMA_12_Baseline': 0,
                    'MCCAIN LBS_Baseline_Lag_1':0,
                    'MCCAIN SMA_4_Lag_1' : 0,
                    'MCCAIN SMA_4_Baseline_Lag_1' : 0
                   })
    
    df['Distributor'] = distributor_name

    df = df[['Consolidated Category','Distributor','Calendar Week Year',
             'LBS','SMA_4','SMA_8','SMA_12','LBS_PRECOVID',
             'LBS_Baseline','SMA_4_Baseline','SMA_8_Baseline','SMA_12_Baseline',
             'LBS_Lag_1', 'LBS_Lag_2', 'LBS_Lag_3', 'LBS_Lag_4', 'LBS_Baseline_Lag_1', 'SMA_4_Lag_1', 'SMA_4_Baseline_Lag_1',
             'MCCAIN LBS','MCCAIN SMA_4','MCCAIN SMA_8','MCCAIN SMA_12','MCCAIN PRECOVID',
             'MCCAIN LBS_Baseline','MCCAIN SMA_4_Baseline','MCCAIN SMA_8_Baseline','MCCAIN SMA_12_Baseline',
             'MCCAIN Lag_1', 'MCCAIN Lag_2', 'MCCAIN Lag_3', 'MCCAIN Lag_4','MCCAIN LBS_Baseline_Lag_1','MCCAIN SMA_4_Lag_1','MCCAIN SMA_4_Baseline_Lag_1',
             'Week Starting (Sun)','Week Ending (Sat)','COVID Week']]
    
    return df
