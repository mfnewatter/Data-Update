import pandas as pd
import numpy as np
import os
from datetime import datetime as dt
import datetime
import pyodbc
import pickle

def import_bek(file_name):

#import file
    if '.csv' in file_name:
        df = pd.read_csv(file_name, thousands = ',', encoding="utf-8", low_memory = False, header = 0,na_values = " ")
        df = df[df['Branch'] != 'Total']
    else:
        df = pd.DataFrame()
        print(file_name)
        _import = pd.read_excel(file_name, sheet_name=None)
        
        for f in _import:
            #print(f)
            if f == 'Sheet1':
                add = pd.DataFrame.from_dict(_import[f])
                
                col = add.columns.to_list()
            else:
                add = _import[f].T.reset_index().T
                add.columns = col
        
            df = pd.concat([df, add])
        
        df = df.rename(columns={
            'Unnamed: 21':'LBS',
            'Unnamed: 22':'LBS'
            })

    return df


def all_df(df, backup, file_name):

    backup_df = pd.read_csv(backup + file_name, low_memory = False, thousands = ',', decimal = '.') 
    
    #, dtype = {
    #            'Calendar Week Year':np.int64,
    #            'LBS':np.float64})

    print(f'Imported shape...{df.shape}', flush = True)

    #create unique list values
    exclude_list = df['Calendar Week Year'].unique().tolist()

    #'SKU ID','Cuisine Type'
    columns = ['City','State','State Name','COVID Segmentation - L1','COVID Segmentation - L2',
            'COVID Segmentation - (Restaurants)','COVID Segmentation - (Restaurants: Sub-Segment)','Restaurant Service Type',
            'Consolidated Category','L1 Product Hierarchy','L2 Product Hierarchy','Calendar Week Year','LBS']

    #import all records from base data minus the new data
    _base = pd.concat([backup_df[~backup_df['Calendar Week Year'].isin(exclude_list)][columns], df[columns]])

    print(f'Final shape...{_base.shape}', flush = True)

    return _base


def build_pfg_frame(file_name):
    date = file_name[118:128]
    
    #df = pd.read_csv(file_name, low_memory=False, thousands=',', dtype={'Qty':'float64','Weight':'float64'})
    df = pd.read_pickle(file_name)

    df['Invoice Week'] = pd.to_datetime(date, format='%Y-%m-%d')
    
    columns_to_return = ['Manufacturer','Segment','Invoice Week','Customer Class','Account Type','MFR SKU',
                         'Qty','Weight','City','State','Brand','Sub-Category','Item Name','Pack','Size','Unit Type','GTIN','Dist SKU']

    return(df[columns_to_return])


def import_pfg():
    # assign directory
    directory = r'C:\Users\newatter\OneDrive - McCain Foods Limited\Historical Sell-Out Sales\PFG Refresh'

    #Loop through all files in directory and create a dataframe

    # iterate over files in
    # that directory
    df = pd.DataFrame()

    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file

        if os.path.isfile(f) and filename.endswith('.pkl'):
            if len(f) > 120:
                df = pd.concat([df, build_pfg_frame(f)])
            
    print(df.shape[0])
    print(df['Invoice Week'].max())

    df.to_csv(r'C:\Users\newatter\OneDrive - McCain Foods Limited\Data Update\files\pfg_us_sellout.csv')

    return df


def import_usfoods(file_name):
    '''
    For importing new data
    '''
    
    if '.csv' in file_name:
        print(f'Importing csv file {dt.now()}', flush = True)
        
        df = pd.read_csv(file_name, low_memory = False, thousands = ',', dtype = {
            'MFG #':'str',
            'PIM #':'str',
            'ASYS #':'str',
            'Vendor #':'str'
        }).rename(columns = {'Year Week':'Calendar Week','LB Current':'LBS'})
        
    else:
        df = pd.read_excel(file_name)
        
        #drop blank column
        df.drop(df.columns[12], axis = 1, inplace = True)
        
        #create list of column names
        col_names = df.columns.tolist()
        
        #transform using first 13 columns (0:12) as row headers
        df = df.melt(id_vars=col_names[0:12], var_name="Calendar Week", value_name="LBS")
        
        #remove blank row
        df = df[df['Calendar Week'] != ' ']

        #convert calendar week to integer
        df['Calendar Week'] = df['Calendar Week'].astype('int64')
    
        df['LBS'] = df['LBS'].replace('-', np.nan)
        df['LBS'] = pd.to_numeric(df['LBS'])
    
    
    print(f'Import file shape: {df.shape}')
    
    return df


def import_sysco_ca():

    _start = dt.now()

    server = 'azure-synapse-workspace-01-prod.sql.azuresynapse.net'
    database = 'GDASQLPool01PROD'
    driver= '{ODBC Driver 18 for SQL Server}'
    active = 'ActiveDirectoryIntegrated'

    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';Authentication='+active)

    date = dt.now()
    date = date - datetime.timedelta(days=date.weekday()+1)
    date = date.strftime('%Y-%m-%d')

    print(f'Query ran for Sysco CA under sales org CA01 for sales on or before {date}')

    sql = '''
        SELECT
        a1.[PeriodStart]
        ,a1.[PeriodEnd]
        ,a2.[Sector__c] as sector
        ,TRIM(a2.[Operator_Segment__c]) as segment
        ,TRIM(a2.[Sub_Segment__c]) as subsegment
        ,TRIM(a3.[Product_Category__c]) as category
        ,SUM(a1.[SalesLbs]) as LBS
        ,SUM(a1.[SalesCases]) as Cases
        FROM [BI].[Factsellout] as a1
        LEFT JOIN [cur].[account] as a2
            on a1.[OperatorID] = a2.[Id]
        LEFT JOIN [cur].[product2] as a3
            on REPLACE(LTRIM(REPLACE(a1.[ProductId], '0', ' ')), ' ', '0') = a3.[ProductCode]
        LEFT JOIN fin.DimSellOutDataProvider as a4
            ON a1.SellOutDataProviderID = a4.SellOutDataProviderID
        WHERE a4.Name = 'Sysco Canada'
        GROUP BY
        a1.[PeriodStart]
        ,a1.[PeriodEnd]
        ,a2.[Sector__c]
        ,a2.[Operator_Segment__c]
        ,a2.[Sub_Segment__c]
        ,a3.[Product_Category__c]
    '''

    data =  pd.read_sql(sql,cnxn)

    _end = dt.now()

    _diff = _end - _start

    print(f'Query took {"{:.1f}".format(_diff.seconds)} seconds')
    
    return data


def import_gfs_ca():

    _start = dt.now()

    server = 'azure-synapse-workspace-01-prod.sql.azuresynapse.net'
    database = 'GDASQLPool01PROD'
    driver= '{ODBC Driver 17 for SQL Server}'
    active = 'ActiveDirectoryIntegrated'

    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';Authentication='+active)

    date = dt.now()
    date = date - datetime.timedelta(days=date.weekday()+1)
    date = date.strftime('%Y-%m-%d')

    print(f'Query ran for GFS CA under sales org CA01 for sales on or before {date}')

    sql = '''
        SELECT
        a1.[PeriodStart]
        ,a1.[PeriodEnd]
        ,a2.[Sector__c] as sector
        ,TRIM(a2.[Operator_Segment__c]) as segment
        ,TRIM(a2.[Sub_Segment__c]) as subsegment
        ,TRIM(a3.[Product_Category__c]) as category
        ,SUM(a1.[SalesLbs]) as LBS
        ,SUM(a1.[SalesCases]) as Cases
        FROM [BI].[Factsellout] as a1
        JOIN [cur].[account] as a2
            on a1.[OperatorID] = a2.[Id]
        LEFT JOIN [cur].[product2] as a3
            on REPLACE(LTRIM(REPLACE(a1.[ProductId], '0', ' ')), ' ', '0') = a3.[ProductCode]
        LEFT JOIN fin.DimSellOutDataProvider as a4
            ON a1.SellOutDataProviderID = a4.SellOutDataProviderID
        WHERE a4.Name = 'GFS Canada'
        GROUP BY
        a1.[PeriodStart]
        ,a1.[PeriodEnd]
        ,a2.[Sector__c]
        ,a2.[Operator_Segment__c]
        ,a2.[Sub_Segment__c]
        ,a3.[Product_Category__c]
    '''

    data =  pd.read_sql(sql,cnxn)

    _end = dt.now()

    _diff = _end - _start

    print(f'Query took {"{:.1f}".format(_diff.seconds)} seconds')
    
    return data


def import_all(search_str):
    # assign directory
    directory = r'C:\Users\newatter\OneDrive - McCain Foods Limited\Data Update\files'

    #Loop through all files in directory and create a dataframe

    # iterate over files in
    # that directory
    all_df = pd.DataFrame()

    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file

        if os.path.isfile(f):
            if search_str in f:
                print(f)
                df = pd.read_csv(f, low_memory=False, thousands=',')
                df.columns = df.columns.str.lower()

                # 11/16/23 - Added fix for category being mispelled
                df['consolidated category'].replace('Perpared Foods', 'Prepared Foods', inplace=True)
                all_df = pd.concat([all_df, df])
            
    #print(df.shape[0])
    #print(df['Invoice Week'].max())

    return all_df

