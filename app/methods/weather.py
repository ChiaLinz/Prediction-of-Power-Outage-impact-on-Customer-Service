import pandas as pd
import addfips
import aiohttp
import asyncio
import numpy as np
from more_itertools import chunked


class NOAAWeatherDataInterface:
    def __init__(self, token, units='metric'):
        '''
        Keyword arguments:\n
        token -- A token for the NCDC NOAA API\n
        units -- Must be either 'metric' or 'standard'. Defaults to metric
        '''
        self.token = token 
        self.units = units
        self.url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data'

    def __get_fip_code(self, location: tuple[str, str]):
        '''
        Private method to retrieve FIP code for a location. Assumes that State is always 
        included in the location

        Keyword arguments:\n
        location -- A two tuple in the form of (County, State)
        '''
        af = addfips.AddFIPS()
        location = [loc.strip() for loc in location]    # Sanitize location strings

        if location[0] not in [None, "", "Unknown", "None", "nan"]:
            return af.get_county_fips(location[0], state=location[1])
        else:
            return af.get_state_fips(location[1])

    async def __get_record_weather(self, session, location: tuple[str, str], date: str):
        '''
        Private method to interface with API and get weather data for a single location and date record

        Keyword arguments:\n
        location -- A two tuple in the form of (County, State)\n
        date -- A date in the form of 'YYYY-MM-DD'
        '''
        headers = {
            'token': self.token,
        }

        params = {
            'datasetid': 'GHCND', # Daily Summaries Dataset
            'locationid': 'FIPS:%s' % self.__get_fip_code(location),
            'startdate': date,
            'enddate': date,
            'units': self.units,
            'limit': '1000',
            'includemetadata': 'false',
            'datatypeid': ['TMAX', 'TMIN', 'SNOW', 'SNWD', 'PRCP']
        }

        async with session.get(self.url, headers=headers, params=params) as r:
            r_json = await r.json()

            try: 
                df = pd.DataFrame.from_records(r_json['results'])
            except KeyError:
                '''TODO: Logic for failed API request'''
                print(location, r_json)

            df.drop(columns=['attributes', 'station'], inplace=True, errors='ignore')
            df = df.groupby(['datatype'], as_index=False)['value'].mean()
            df = df.pivot(columns='datatype')['value']
            df = pd.DataFrame(np.diagonal(df), df.columns).T
            df = df.rename_axis(None, axis=1)
            df['Date'] = date
            return df.loc[0, :].values.tolist()

    async def __get_multiple_rec_weather_data(self, records):
        '''
        Private helper function to retrieve weather data for multiple records

        Keyword arguments:\n
        records -- A 2D list where each element is of the form (location, date) where
            location -- A three tuple in the form of (City, County, State)\n
            date -- A date in the form of 'YYYY-MM-DD'
        '''
        connector = aiohttp.TCPConnector(limit=1)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for record in records:
                location, date = record
                tasks.append(asyncio.ensure_future(self.__get_record_weather(session, location, date)))

            weather_data = await asyncio.gather(*tasks)
            for rec in weather_data:
                '''TODO: Create a method that interfaces with a database to store data?'''
                print(rec)

            await asyncio.sleep(1)
            return weather_data

        #await asyncio.sleep(1) # Rate limiter to ensure requests/sec isn't overloaded

    def get_multiple_rec_weather_data(self, records):
        '''
        Retrieves weather data for multiple records. Works by sending 
        concurrent requests, but is limited to 5 requests/second

        Keyword arguments:\n
        records -- A 2D list where each element is of the form (location, date)
        where
            location -- A two tuple in the form of (County, State)\n
            date -- A date in the form of 'YYYY-MM-DD'
        '''
        batches = list(chunked(records, 5)) # Create batches of 5 records to be sent every second
        for batch in batches:
            asyncio.run(self.__get_multiple_rec_weather_data(batch))

    async def __get_single_rec_weather_data(self, record):
        '''
        Private helper function to retrieve weather data for a single record

        Keyword arguments:\n
        record -- A list in the form of (location, date) where
            location -- A three tuple in the form of (City, County, State)\n
            date -- A date in the form of 'YYYY-MM-DD'
        '''
        async with aiohttp.ClientSession() as session:
            location, date = record
            weather_data = await asyncio.gather(*[asyncio.ensure_future(self.__get_record_weather(session, location, date))])
            for df in weather_data:
                '''TODO: Create a method that interfaces with a database to store data?'''
                print(df)

    def get_single_rec_weather_data(self, record):
        '''
        Retrieves weather data for a single record

        Keyword arguments:\n
        record -- A list in the form of (location, date) where
            location -- A three tuple in the form of (City, County, State)\n
            date -- A date in the form of 'YYYY-MM-DD'
        '''
        asyncio.run(self.__get_single_rec_weather_data(record))

    '''
    TODO: Create a method that expands datatypes into more useful strings and includes units
    '''

if __name__ == "__main__":
    from tokens import noaa_key
    units = 'metric'

    wd = NOAAWeatherDataInterface(noaa_key, units)

    #location = ('', 'New Jersey')
    #start_date = '2022-02-08'
    #end_date = '2022-02-09'
    #record = [location, start_date, end_date]
    #wd.get_single_rec_weather_data(record)

    name = 'test/states/Alabama.csv'
    df = pd.read_csv(f'{name}')

    weather_records = []
    records = []
    for index, row in df.iterrows():
        if index < 10:
            location = ('', row['State'])
            date = row['Date']
            records.append([location, date])

    wd.get_multiple_rec_weather_data(records)

    #weather_records.append(wd.get_multiple_rec_weather_data(records))
    #print(weather_records)