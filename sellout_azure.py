from sellout_model import process_list, analyze, add_time
from distributor_transformation import set_dictionary, set_time
import pandas as pd
import datetime
from datetime import datetime as dt
import numpy as np
import pyodbc
import warnings
warnings.filterwarnings('ignore')


def azure_sellin(_base, sales_org, distributor, distributor_name):
    _start = dt.now()
    
    server = 'azure-synapse-workspace-01-prod.sql.azuresynapse.net'
    database = 'GDASQLPool01PROD'
    driver= '{ODBC Driver 17 for SQL Server}'
    active = 'ActiveDirectoryIntegrated'

    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';Authentication='+active)

    date = dt.now()
    date = date - datetime.timedelta(days=date.weekday()+1)
    date = date.strftime('%Y-%m-%d')

    print(f'Query ran for {distributor} under sales org {sales_org} for sales on or before {date}')

    sql = '''select a14.[Calendar Week End Date] as calendar_week,
                a12.[Category] as category_desc,
                sum(a11.[Weight Lbs]) as actual_volume_lbs
            from [BI].[Factsellin] as a11
            join [BI].[DimProduct] as a12
                on (a11.[ProductID] = a12.[ProductId])
            join [BI].[DimCalendar] as a14
                on (a11.[Accounting Period Date] = a14.[Calendar date])
            join [BI].[DimCustomerSales] as a15
                on (a11.CustomerID = a15.CustomerID and 
                a11.[Distribution Channel] = a15.[Distribution Channel] and 
                a11.[Sales Org] = a15.[Sales Org])
            where a11.[Sales Org] in (''' + "'" + str(sales_org) + "'" + ''')
                and a11.[Distribution Channel] in ('10')
                and a15.[Customer Hierarchy Level1ID] in (''' + "'" + str(distributor) + "'" + ''')
                and a14.[Calendar date] <= ''' + "'" + str(date) + "'" + ''' 
            group by a14.[Calendar Week End Date],
                a12.[Category]
                '''
    
    data =  pd.read_sql(sql,cnxn)
    
    _end = dt.now()
    
    df = sellin_transform(data)
    df = azure_sellout(df, _base, distributor_name)
    _diff = _end - _start
    
    print(f'All done, took {"{:.1f}".format(_diff.seconds)} seconds...')
    
    return df


def sellin_transform(df):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)

    sellin = df.astype({'calendar_week':'datetime64[ns]'})

    sellin = sellin.merge(TIME[['Week Ending (Sun)', 'Calendar Week Year']],
                        how='left',
                        left_on='calendar_week',
                        right_on='Week Ending (Sun)').drop(columns={'Week Ending (Sun)'})

    sellin = sellin.fillna(0).astype({'Calendar Week Year':'int64'})

    #rename columns for consistancy
    sellin = sellin.rename(columns = {'actual_volume_lbs':'LBS'})

    #transform category so its consolidated
    sellin['Consolidated Category'] = sellin['category_desc']
    sellin.loc[sellin['Consolidated Category'] == 'Sweet Potato' , 'Consolidated Category'] = 'Potato'
    sellin.loc[sellin['Consolidated Category'] != 'Potato' , 'Consolidated Category'] = 'Prepared Foods'

    #analyze sellin data
    sellin = analyze(sellin, ['Consolidated Category'], 201910, 202009)

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

    return sellin


def azure_sellout(sellin, sellout, distributor):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)
    
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


    df['Distributor'] = distributor
    
    df = df[['Consolidated Category','Distributor','Calendar Week Year',
            'LBS','SMA_4','SMA_8','SMA_12','LBS_PRECOVID',
            'LBS_Baseline','SMA_4_Baseline','SMA_8_Baseline','SMA_12_Baseline',
            'LBS_Lag_1', 'LBS_Lag_2', 'LBS_Lag_3', 'LBS_Lag_4', 'LBS_Baseline_Lag_1', 'SMA_4_Lag_1', 'SMA_4_Baseline_Lag_1',
            'MCCAIN LBS','MCCAIN SMA_4','MCCAIN SMA_8','MCCAIN SMA_12','MCCAIN PRECOVID',
            'MCCAIN LBS_Baseline','MCCAIN SMA_4_Baseline','MCCAIN SMA_8_Baseline','MCCAIN SMA_12_Baseline',
            'MCCAIN Lag_1', 'MCCAIN Lag_2', 'MCCAIN Lag_3', 'MCCAIN Lag_4','MCCAIN LBS_Baseline_Lag_1','MCCAIN SMA_4_Lag_1','MCCAIN SMA_4_Baseline_Lag_1',
            'Week Starting (Sun)','Week Ending (Sat)','COVID Week']]

    return df