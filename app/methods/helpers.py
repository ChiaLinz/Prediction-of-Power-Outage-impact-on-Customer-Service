import pandas as pd

def filter_df_by_location(location, df=pd.read_csv('data/merged_data.csv')):
    '''
    Filters a dataframe containing historical records based on a location tuple

    Keyword Arguements:\n
    location: A two-tuple in the form of (county, state)\n
    df: A dataframe containing historical records. Defaults to 'data/merged_data.csv'
    '''
    county, state = location
    records = None
        
    if county == 'nan':
        records = df.loc[df['State'] == state]
    else:
        records = df.loc[((df['State'] == state) & (df['County'].str.contains(county))) | 
                ((df['State'] == state) & (df['County'] == 'None'))]
    return records

def filter_df_by_date(start_date: str, end_date: str, df=pd.read_csv('data/merged_data.csv')):
    '''
    Filters a dataframe containing historical records based on a location tuple

    Keyword Arguements:\n
    start_date: A str representing a start date to filter by\n
    end_date: A str representing an end date filter by\n
    df: A dataframe containing historical records. Defaults to 'data/merged_data.csv'
    '''
    records = None
    records = df.loc[(df['Start Date'] >= start_date) & (df['Start Date'] <= end_date)]
    return records