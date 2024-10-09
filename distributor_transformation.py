import pandas as pd
import numpy as np
from datetime import datetime as dt

def set_dictionary():
    return r'C:\Users\newatter\OneDrive - McCain Foods Limited\Distributor Sell-Out\Data Dictionaries\\'


def set_time(DICTIONARY):
    return pd.read_excel(DICTIONARY + 'Time Definitions.xlsx')


def us_states():
    us_state_abbrev = {
        'Alabama': 'AL',
        'Alaska': 'AK',
        'American Samoa': 'AS',
        'Arizona': 'AZ',
        'Arkansas': 'AR',
        'California': 'CA',
        'Colorado': 'CO',
        'Connecticut': 'CT',
        'Delaware': 'DE',
        'District of Columbia': 'DC',
        'Florida': 'FL',
        'Georgia': 'GA',
        'Guam': 'GU',
        'Hawaii': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL',
        'Indiana': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS',
        'Kentucky': 'KY',
        'Louisiana': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD',
        'Massachusetts': 'MA',
        'Michigan': 'MI',
        'Minnesota': 'MN',
        'Mississippi': 'MS',
        'Missouri': 'MO',
        'Montana': 'MT',
        'Nebraska': 'NE',
        'Nevada': 'NV',
        'New Hampshire': 'NH',
        'New Jersey': 'NJ',
        'New Mexico': 'NM',
        'New York': 'NY',
        'North Carolina': 'NC',
        'North Dakota': 'ND',
        'Northern Mariana Islands':'MP',
        'Ohio': 'OH',
        'Oklahoma': 'OK',
        'Oregon': 'OR',
        'Pennsylvania': 'PA',
        'Puerto Rico': 'PR',
        'Rhode Island': 'RI',
        'South Carolina': 'SC',
        'South Dakota': 'SD',
        'Tennessee': 'TN',
        'Texas': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT',
        'Virgin Islands': 'VI',
        'Virginia': 'VA',
        'Washington': 'WA',
        'West Virginia': 'WV',
        'Wisconsin': 'WI',
        'Wyoming': 'WY'
    }

    # thank you to @kinghelix and @trevormarburger for this idea
    abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))
    
    return pd.DataFrame.from_dict(abbrev_us_state, orient = 'index', columns = ['State Name']).rename_axis('State').reset_index()


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


def transform_bek(df, file_name):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)

    #create dictionary object from Excel file
    #adding sheet_name = None makes it a dictionary type
    _dict = pd.read_excel(DICTIONARY + file_name, sheet_name = None, engine='openpyxl')
    
    #create DataFrame from dictionary object called dict (short for dictionary)
    dict_df = pd.DataFrame.from_dict(_dict['Segment Mapping'])
    
    #create DataFrame from dictionary object called cat (short for category)
    sku_df = pd.DataFrame.from_dict(_dict['SKU Mapping'])
    
    #print shape of df (dimensions)
    print(f'Shape before adding dictionary: {df.shape}', flush = True)

    #Business Unit	SIC Code	SIC Sub
    #Group
    
    #add lower case for merging
    dict_df['Business Unit-lower'] = dict_df['Business Unit'].str.lower()
    dict_df['SIC Code-lower'] = dict_df['SIC Code'].str.lower()
    dict_df['SIC Sub-lower'] = dict_df['SIC Sub'].str.lower()
    
    #COVID Segmentation - L1	COVID Segmentation - L2	COVID Segmentation - (Restaurants)	COVID Segmentation - (Restaurants: Sub-Segment)	Restaurant Service Type	Cuisine Type
    dict_df = dict_df.groupby(['COVID Segmentation - L1','Business Unit-lower','SIC Code-lower','SIC Sub-lower',
                               'COVID Segmentation - L2','COVID Segmentation - (Restaurants)','COVID Segmentation - (Restaurants: Sub-Segment)',
                               'Restaurant Service Type','Cuisine Type'], dropna = False).size().reset_index().drop(columns={0})
    
    #add lower case key columns for merging (removes case mismatch)
    df['Business Unit-lower'] = df['Business Unit'].str.lower()
    df['SIC Code-lower'] = df['SIC Code'].str.lower()
    df['SIC Sub-lower'] = df['SIC Sub'].str.lower()
    
    df = df.rename(columns = {
        'Customer City':'City', 
        'Customer State':'State',
        #'Manufacture Prod.Nbr.':'SKU ID'
        'McCain SKU ID':'SKU ID'
    })
    
    df = df.merge(dict_df, how = 'left', left_on = ['Business Unit-lower','SIC Code-lower','SIC Sub-lower'],
                  right_on = ['Business Unit-lower','SIC Code-lower','SIC Sub-lower']).drop(columns = {'Business Unit-lower','SIC Code-lower','SIC Sub-lower'})
    
    #print shape of df (dimensions)
    print(f'Shape after adding segmentation: {df.shape}', flush = True)
    
    #SKU Mapping
    
    df['SKU ID'] = df['SKU ID'].astype(str)
    sku_df['Mfg ID'] = sku_df['Mfg ID'].astype(str)
    
    df['Mfg ID-lower'] = df['SKU ID'].str.lower()
    sku_df['Mfg ID-lower'] = sku_df['Mfg ID'].str.lower()
    
    sku_df = sku_df.groupby(['Mfg ID-lower','Consolidated Category','L1 Product Hierarchy','L2 Product Hierarchy'], dropna = False).size().reset_index().drop(columns={0})
    
    df = df.merge(sku_df, how = 'left', left_on = ['Mfg ID-lower'],right_on = ['Mfg ID-lower']).drop(columns = {'Mfg ID-lower'})
    
    #print shape of df (dimensions)
    print(f'Shape after adding product segmentation: {df.shape}', flush = True)
    
    df['Week Starting'] = pd.to_datetime(df['Week Beginning Date'])
    
    print(f'Shape before adding time: {df.shape}', flush = True)
    
    df = df.merge(TIME[['Week Starting (Sun)', 'Calendar Week Year']], how = 'left', 
                  left_on = ['Week Starting'], right_on = ['Week Starting (Sun)']).drop(columns = {'Week Starting (Sun)'})
    
    print(f'Shape after adding time: {df.shape}', flush = True)
    
    #exclude certain records
    df = df[~df['Calendar Week Year'].isna()]
    #df = df[df['Branch'] != 'Total']
    df['LBS'] = pd.to_numeric(df['LBS'])
    df['Calendar Week Year'] = df['Calendar Week Year'].astype('int64')
    
    #Merge states
    df = df.merge(us_states(), how = 'left', on = 'State')
       
    df = clean_city(df)

    print(f'Shape after adding dictionary: {df.shape}', flush = True)
    
    is_missing_l1(df, ['Business Unit','SIC Code','SIC Sub'], 'bek')
    is_missing_sku(df, ['SKU ID'], 'bek')

    return df


def transform_pfg(df, file_name):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)
    
    print(f'Starting dataframe shape: {df.shape}', flush = True)
    
    #create dictionary object from Excel file
    #adding sheet_name = None makes it a dictionary type
    _dict = pd.read_excel(DICTIONARY + file_name, sheet_name = None, engine='openpyxl')
    
    #for testing, keys = sheet names
    #print(_dict.keys())
    #'Manufacturer','Segment','Invoice Week','Customer Class','Account Type','MFR SKU','Qty','Weight','State', 'State Name'
    #create DataFrame from dictionary object called dict (short for dictionary)
    dict_df = pd.DataFrame.from_dict(_dict['Segment Mapping'])
    sku_df = pd.DataFrame.from_dict(_dict['SKU Mapping'])
    
    man_df = pd.DataFrame.from_dict(_dict['Manufacturer Mapping'])
    
    manufacturer = man_df[man_df['Mfg. Inclusion Flag'] == 'Include']['Manufacturer'].tolist()
    
    print(f'These manufacturers were included: {manufacturer}', flush = True)
    
    #excluded = df['Manufacturer'].value_counts().reset_index().drop(columns={'Manufacturer'}).rename(columns = {'index':'Manufacturer'})
    #excluded = excluded[~excluded['Manufacturer'].isin(manufacturer)]
    
    excluded = df['Manufacturer'].value_counts()\
                          .reset_index(name='count')\
                          .query('Manufacturer not in @manufacturer')\
                          .rename(columns={'index': 'Manufacturer'})

    #display(df.groupby['Manufacturer'].size().reset_index().drop(columns={0}))
    
    print(f'These manufacturers were not included: {excluded}', flush = True)
    
    df = df[df['Manufacturer'].isin(manufacturer)]
    
    #strip blanks from segment
    df.loc[:, 'Segment'] = df['Segment'].str.strip()
    
    #convert Invoice Week to date
    df.loc[:, 'Invoice Week'] = pd.to_datetime(df['Invoice Week'])
    
    
    #print shape of df (dimensions)
    print(f'Shape before adding dictionary: {df.shape}', flush = True)
    #add lower case for merging
    
    dict_df.loc[:, 'customer_class_lower'] = dict_df['Customer Class'].str.lower()
    dict_df.loc[:, 'segment_lower'] = dict_df['Segment'].str.strip().str.lower()
    dict_df.loc[:, 'account_type_lower'] = dict_df['Account Type'].str.strip().str.lower()
    
    #Type Name	Category Name	COVID Segmentation - L1	COVID Segmentation - L2	COVID Segmentation - (Restaurants)	COVID Segmentation - (Restaurants: Sub-Segment)	Restaurant Service Type
    
    dict_df = dict_df.groupby(['customer_class_lower','segment_lower','account_type_lower','COVID Segmentation - L1','COVID Segmentation - L2',
                               'COVID Segmentation - (Restaurants)','COVID Segmentation - (Restaurants: Sub-Segment)','Restaurant Service Type','Cuisine Type']
                              , dropna = False).size().reset_index().drop(columns={0})
    
    #add lower case key columns for merging (removes case mismatch)
    
    df.loc[:, 'customer_class_lower'] = df['Customer Class'].str.lower()
    df.loc[:, 'segment_lower'] = df['Segment'].str.strip().str.lower()
    df.loc[:, 'account_type_lower'] = df['Account Type'].str.strip().str.lower()
    
    
    df = df.merge(dict_df, how = 'left', left_on = [
        'customer_class_lower','segment_lower','account_type_lower'],
        right_on = ['customer_class_lower','segment_lower','account_type_lower']).drop(columns = {
            'customer_class_lower','segment_lower','account_type_lower'})
    
    #print(f'Shape after 1st merge: {df.shape}', flush = True)
    
    sku_df = sku_df.groupby(['Mfr SKU','Consolidated Category','L1 Product Hierarchy','L2 Product Hierarchy','Case Weight Lbs'], dropna = False).size().reset_index().drop(columns={0})
    
    #both SKU fields need to be strings in order to match
    sku_df.loc[:, 'Mfr SKU'] = sku_df['Mfr SKU'].astype(str)
    df.loc[:, 'MFR SKU'] = df['MFR SKU'].astype(str)
    
    df = df.merge(sku_df[['Mfr SKU','Consolidated Category','L1 Product Hierarchy','L2 Product Hierarchy','Case Weight Lbs']], 
                  how = 'left', 
                  left_on = ['MFR SKU'], 
                  right_on = ['Mfr SKU']).drop(columns = {'Mfr SKU'})
    
    #print(f'Shape after 2nd merge: {df.shape}', flush = True)
    #
    df = df.astype({'Qty':'float64','Weight':'float64','Case Weight Lbs':'float64'})
    
    #calculate case weight if weight = 0 and qty > 0
    df.loc[(df['Weight'] == 0) & (df['Qty'] > 0), 'Weight'] = df['Case Weight Lbs'] * df['Qty']
    
    #add time
    
    df = df.merge(TIME[['Week Starting (Mon)', 'Calendar Week Year']], how = 'left', left_on = ['Invoice Week'], right_on = ['Week Starting (Mon)']).drop(columns={'Week Starting (Mon)'})
    
    #rename metric Weight for consistancy
    df = df.rename(columns={
        'Weight':'LBS',
        'MFR SKU':'SKU ID'})
    
    df = df[~df['Calendar Week Year'].isna()]
    
    #Clean US States
    df.loc[df['State'] == 'tn', 'State'] = 'TN'
    df = df.merge(us_states(), how = 'left', on = 'State')
    df.loc[df['State Name'].isna(), ['State', 'State Name']] = 'None'
    
    print(f'Shape after adding dictionary: {df.shape}', flush = True)
    
    is_missing_l1(df, ['Customer Class','Segment', 'Account Type'], 'pfg')
    is_missing_sku(df, ['Brand','Sub-Category','SKU ID','Item Name','Pack','Size','Unit Type','GTIN','Dist SKU'], 'pfg')

    return df


def transform_pfg2(df, file_name):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)
    
    print(f'Starting dataframe shape: {df.shape}', flush = True)
    
    #create dictionary object from Excel file
    #adding sheet_name = None makes it a dictionary type
    _dict = pd.read_excel(DICTIONARY + file_name, sheet_name = None, engine='openpyxl')
    
    #for testing, keys = sheet names
    #print(_dict.keys())
    
    #create DataFrame from dictionary object called dict (short for dictionary)
    dict_df = pd.DataFrame.from_dict(_dict['Segment Mapping'])
    sku_df = pd.DataFrame.from_dict(_dict['SKU Mapping'])
    
    man_df = pd.DataFrame.from_dict(_dict['Manufacturer Mapping'])
        
    #strip blanks from segment
    df.loc[:, 'Segment'] = df['Segment'].str.strip()
    
    #convert Invoice Week to date
    df.loc[:, 'Invoice Week'] = pd.to_datetime(df['Invoice Week'])
    
    
    #print shape of df (dimensions)
    print(f'Shape before adding dictionary: {df.shape}', flush = True)
    #add lower case for merging
    
    dict_df.loc[:, 'customer_class_lower'] = dict_df['Customer Class'].str.lower()
    dict_df.loc[:, 'segment_lower'] = dict_df['Segment'].str.strip().str.lower()
    dict_df.loc[:, 'account_type_lower'] = dict_df['Account Type'].str.strip().str.lower()
    
    #Type Name	Category Name	COVID Segmentation - L1	COVID Segmentation - L2	COVID Segmentation - (Restaurants)	COVID Segmentation - (Restaurants: Sub-Segment)	Restaurant Service Type
    
    dict_df = dict_df.groupby(['customer_class_lower','segment_lower','account_type_lower','COVID Segmentation - L1','COVID Segmentation - L2',
                               'COVID Segmentation - (Restaurants)','COVID Segmentation - (Restaurants: Sub-Segment)','Restaurant Service Type','Cuisine Type']
                              , dropna = False).size().reset_index().drop(columns={0})
    
    #add lower case key columns for merging (removes case mismatch)
    
    df.loc[:, 'customer_class_lower'] = df['Customer Class'].str.lower()
    df.loc[:, 'segment_lower'] = df['Segment'].str.strip().str.lower()
    df.loc[:, 'account_type_lower'] = df['Account Type'].str.strip().str.lower()
    
    
    df = df.merge(dict_df, how = 'left', left_on = [
        'customer_class_lower','segment_lower','account_type_lower'],
        right_on = ['customer_class_lower','segment_lower','account_type_lower']).drop(columns = {
            'customer_class_lower','segment_lower','account_type_lower'})
    
    
    #both SKU fields need to be strings in order to match
    sku_df.loc[:, 'Mfr SKU'] = sku_df['Mfr SKU'].astype(str)
    df.loc[:, 'MFR SKU'] = df['MFR SKU'].astype(str)
    
    df = df.merge(sku_df[['Mfr SKU','Consolidated Category','L1 Product Hierarchy','L2 Product Hierarchy','Case Weight Lbs']], how = 'left', left_on = ['MFR SKU'], right_on = ['Mfr SKU']).drop(columns = {'Mfr SKU'})
    
    #print(f'Shape after 2nd merge: {df.shape}', flush = True)
    
    df = df.astype({'Qty':'float64','Weight':'float64','Case Weight Lbs':'float64'})
    
    #calculate case weight if weight = 0 and qty > 0
    df.loc[(df['Weight'] == 0) & (df['Qty'] > 0), 'Weight'] = df['Case Weight Lbs'] * df['Qty']
    
    #add time
    
    df = df.merge(TIME[['Week Starting (Mon)', 'Calendar Week Year']], how = 'left', left_on = ['Invoice Week'], right_on = ['Week Starting (Mon)']).drop(columns={'Week Starting (Mon)'})
    
    #rename metric Weight for consistancy
    df = df.rename(columns={
        'Weight':'LBS',
        'MFR SKU':'SKU ID'})
    
    df = df[~df['Calendar Week Year'].isna()]
    
    #Clean US States
    df.loc[df['State'] == 'tn', 'State'] = 'TN'
    df = df.merge(us_states(), how = 'left', on = 'State')
    df.loc[df['State Name'].isna(), ['State', 'State Name']] = 'None'
    
    print(f'Shape after adding dictionary: {df.shape}', flush = True)

    return df


def transform_usfoods(df, file_name):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)

    #create dictionary object from Excel file
    #adding sheet_name = None makes it a dictionary type
    _dict = pd.read_excel(DICTIONARY + file_name, sheet_name = None, engine='openpyxl')

    #create DataFrame from dictionary object called segments
    segments = pd.DataFrame.from_dict(_dict['Segment Mapping v2'])
    
    #create DataFrame from dictionary object called products)
    products = pd.DataFrame.from_dict(_dict['SKU Mapping v3'])
    
    regions = pd.DataFrame.from_dict(_dict['Region Mapping'])
    
    #print shape of df (dimensions)
    print(f'Shape before adding dictionary: {df.shape}', flush = True)
    
    #testing total lbs to see if it matches after merge
    total_lbs = df['LBS'].sum()
    
    print(f'Total before dictionary: {total_lbs}', flush = True)
    
    #Category Segmentation
    #add lower case for merging
    segments['Pyramid Segment-lower'] = segments['Pyramid Segment'].str.lower()
    
    #create unique rows from dictionary
    segments = segments.groupby(['COVID Segmentation - L1','Pyramid Segment-lower',
                               'COVID Segmentation - L2','COVID Segmentation - (Restaurants)','COVID Segmentation - (Restaurants: Sub-Segment)',
                               'Restaurant Service Type'], dropna = False).size().reset_index().drop(columns={0})
    
    df = df.rename(columns = {'Pyr Segment':'Pyramid Segment',
                              'Division':'Market',
                              'MFG #':'Manufacturer Item Number',
                              'Product':'Product Description'})
    
    #df = df.astype({'Manufacturer GTIN':'int64'}).astype({'Manufacturer GTIN':'str'})
    
    #add lower case key columns for merging (removes case mismatch)
    df['Pyramid Segment-lower'] = df['Pyramid Segment'].str.lower()
    #df['PIM Group-lower'] = df['PIM Group'].str.lower()
    
    #remove lower case key columns
    df = df.merge(segments, how = 'left', left_on = ['Pyramid Segment-lower'], right_on = ['Pyramid Segment-lower']).drop(
        columns = {'Pyramid Segment-lower'})
    
    #Material Segementation
    #add lower case for merging
    #products['PIM Group-lower'] = products['PIM Group'].str.lower()
    
    products = products.groupby(['McCain SKU ID','Consolidated Category', 'L1 Product Hierarchy','L2 Product Hierarchy'], 
                                dropna = False).size().reset_index().drop(columns={0})
    
    df = df.merge(products, how = 'left', on = 'McCain SKU ID')
    
    #Time segmentation
    df = df.merge(TIME[['Week Starting (Sun)', 'Calendar Week Year']], how = 'left', 
                  left_on = ['Week Beginning Date'], right_on = ['Week Starting (Sun)']).drop(columns = {'Week Beginning Date'})
    
    #df = df.merge(regions[['Market', 'State']], how = 'left', on = 'Market')
                  
    df = df.merge(us_states(), how = 'left', on = 'State')

    df.loc[df['State Name'].isna(), ['State', 'State Name']] = 'None'
    
    df['City'] = 'NA'
    
    #testing total lbs to see if it matches after merge
    total_lbs = df['LBS'].sum()
    print(f'Total after dictionary: {total_lbs}', flush = True)
    
    #print final shape to see if anything changes (would indicate duplicates in dictionary)
    print(f'Shape after adding dictionary: {df.shape}', flush = True)
    
    is_missing_l1(df, ['Pyramid Segment'], 'usfoods')
    is_missing_sku(df, ['ASYS ID','Manufacturer GTIN','McCain SKU ID'], 'usfoods')

    return df


def transform_sysco_ca(df, file_name):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)
    
    #create dictionary object from Excel file
    #adding sheet_name = None makes it a dictionary type
    _dict = pd.read_excel(DICTIONARY + file_name, sheet_name = None, engine='openpyxl')

    #update category to consolidated category
    df.loc[~df['category'].fillna('').str.contains('POT'), 'Consolidated Category'] = 'Prepared Foods'
    df.loc[df['category'].fillna('').str.contains('POT'), 'Consolidated Category'] = 'Potato'
    
    #create DataFrame from dictionary object called dict (short for dictionary)
    dict_df = pd.DataFrame.from_dict(_dict['Segment Mapping v2'])

    #create DataFrame from dictionary object called cat (short for category)
    cat_df = pd.DataFrame.from_dict(_dict['Province Mapping'])
    cat_df = cat_df[['Province','Cleaned Province Name','Geographic Region']]
    
    #print shape of df (dimensions)
    print(f'Shape before adding dictionary: {df.shape}', flush = True)
    
    #testing total lbs to see if it matches after merge
    total_lbs = df['LBS'].sum()
    print(f'Total before dictionary: {total_lbs}', flush = True)
    
    dict_df = dict_df.groupby([
            'sector','segment','subsegment',
            'COVID Segmentation - L1','COVID Segmentation - L2','COVID Segmentation - (Restaurants)','Restaurant Service Type'
                              ], dropna = False).size().reset_index().drop(columns={0})
    
    #dict_df.to_csv('sysco_test.csv', index=False)

    df['sector'] = df['sector'].str.strip().replace(r'^\s*$', np.nan, regex=True)
    df['segment'] = df['segment'].str.strip().replace(r'^\s*$', np.nan, regex=True)
    df['subsegment'] = df['subsegment'].str.strip().replace(r'^\s*$', np.nan, regex=True)

    dict_df['sector'] = dict_df['sector'].str.strip().replace(r'^\s*$', np.nan, regex=True)
    dict_df['segment'] = dict_df['segment'].str.strip().replace(r'^\s*$', np.nan, regex=True)
    dict_df['subsegment'] = dict_df['subsegment'].str.strip().replace(r'^\s*$', np.nan, regex=True)

    #remove / character
    df['sector'] = df['sector'].str.replace('/', '')

    #remove lower case key columns
    df = df.merge(dict_df, how = 'left', on=['sector','segment','subsegment'])
    
    #add Clean Province Name
    #df = df.merge(cat_df, how = 'left', left_on = ['Province'], right_on = ['Province'])
    
    df['City'] = 'NA'
    df['Region'] = 'NA'
    
    df = clean_city(df)
    
    df['PeriodEnd'] = pd.to_datetime(df['PeriodEnd'])

     #apply calendar week
    df = df.merge(TIME[['Week Ending (Sat)', 'Calendar Week Year']], how = 'left', left_on = ['PeriodEnd'], right_on = ['Week Ending (Sat)']).drop(columns = {'Week Ending (Sat)'})
    
    #df = df.rename(columns = {'category':'Consolidated Category'}).fillna(0).astype({'Calendar Week Year':'int64'})

    #testing total lbs to see if it matches after merge
    total_lbs = df['LBS'].sum()
    print(f'Total after dictionary: {total_lbs}', flush = True)

    #print final shape to see if anything changes (would indicate duplicates in dictionary)
    print(f'Shape after adding dictionary: {df.shape}', flush = True)

    is_missing_l1(df, ['sector','segment','subsegment'], 'sysco_ca')

    return df


def transform_gfs_ca(df, file_name):
    DICTIONARY = set_dictionary()
    TIME = set_time(DICTIONARY)
    
    #create dictionary object from Excel file
    #adding sheet_name = None makes it a dictionary type
    _dict = pd.read_excel(DICTIONARY + file_name, sheet_name = None, engine='openpyxl')

    #update category to consolidated category
    df.loc[~df['category'].fillna('').str.contains('POT'), 'Consolidated Category'] = 'Prepared Foods'
    df.loc[df['category'].fillna('').str.contains('POT'), 'Consolidated Category'] = 'Potato'
    
    #create DataFrame from dictionary object called dict (short for dictionary)
    dict_df = pd.DataFrame.from_dict(_dict['Segment Mapping v2'])

    #create DataFrame from dictionary object called cat (short for category)
    cat_df = pd.DataFrame.from_dict(_dict['Province Mapping'])
    cat_df = cat_df[['Province','Cleaned Province Name','Geographic Region']]
    
    #print shape of df (dimensions)
    print(f'Shape before adding dictionary: {df.shape}', flush = True)
    
    #testing total lbs to see if it matches after merge
    total_lbs = df['LBS'].sum()
    print(f'Total before dictionary: {total_lbs}', flush = True)
    
    dict_df = dict_df.groupby([
            'sector','segment','subsegment',
            'COVID Segmentation - L1','COVID Segmentation - L2','COVID Segmentation - (Restaurants)','Restaurant Service Type'
                              ], dropna = False).size().reset_index().drop(columns={0})
    
    #dict_df.to_csv('sysco_test.csv', index=False)

    df['sector'] = df['sector'].str.strip().replace('', np.nan)
    df['segment'] = df['segment'].str.strip().replace('', np.nan)
    df['subsegment'] = df['subsegment'].str.strip().replace('', np.nan)

    dict_df['sector'] = dict_df['sector'].str.strip().replace('', np.nan)
    dict_df['segment'] = dict_df['segment'].str.strip().replace('', np.nan)
    dict_df['subsegment'] = dict_df['subsegment'].str.strip().replace('', np.nan)

    #remove / character
    df['sector'] = df['sector'].str.replace('/', '')

    #remove lower case key columns
    df = df.merge(dict_df, how = 'left', on=['sector','segment','subsegment'])
    
    #add Clean Province Name
    #df = df.merge(cat_df, how = 'left', left_on = ['Province'], right_on = ['Province'])
    
    df['City'] = 'NA'
    df['Region'] = 'NA'
    
    df = clean_city(df)
    
    df['PeriodEnd'] = pd.to_datetime(df['PeriodEnd'])

     #apply calendar week
    df = df.merge(TIME[['Week Ending (Sat)', 'Calendar Week Year']], how = 'left', left_on = ['PeriodEnd'], right_on = ['Week Ending (Sat)']).drop(columns = {'Week Ending (Sat)'})
    
    #df = df.rename(columns = {'category':'Consolidated Category'}).fillna(0).astype({'Calendar Week Year':'int64'})

    #testing total lbs to see if it matches after merge
    total_lbs = df['LBS'].sum()
    print(f'Total after dictionary: {total_lbs}', flush = True)

    #print final shape to see if anything changes (would indicate duplicates in dictionary)
    print(f'Shape after adding dictionary: {df.shape}', flush = True)

    is_missing_l1(df, ['sector','segment','subsegment'], 'sysco_ca')

    return df


def is_missing_l1(df, _list, distributor):
    DICTIONARY = set_dictionary()

    #check for COVID Segmentation - L1
    missing = df[df['COVID Segmentation - L1'].isna()].groupby(_list, as_index = False, dropna = False)['LBS'].sum()

    if len(missing) > 0:
        print('The following segments are missing:')
        print(missing)
        missing.to_excel(DICTIONARY + 'Segments Missing Dump\\' + dt.now().strftime('%Y%m%d') + '_' + distributor + '_l1_missing.xlsx', index = False)
    else:
        print(f'Nothing missing for COVID Segmentation - L1', flush = True)
    return


def is_missing_sku(df, _list, distributor):
    DICTIONARY = set_dictionary()

    #check for COVID Segmentation - L1
    missing = df[df['Consolidated Category'].isna()].groupby(_list, as_index = False, dropna = False)['LBS'].sum()

    if len(missing) > 0:
        print('The following products are missing:')
        print(missing)
        missing.to_excel(DICTIONARY + 'Segments Missing Dump\\' + dt.now().strftime('%Y%m%d') + '_' + distributor + '_sku_missing.xlsx', index = False)
    else:
        print(f'Nothing missing for products', flush = True)
    return
