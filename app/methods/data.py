import requests
from statistics import mean
import datetime as dt
import pandas as pd

base = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
#api_key = str(os.getenv('API_KEY'))
ApiKey='NHBDAL2QBK7KADAS2TY42G6XG'
#UnitGroup sets the units of the output - us or metric
UnitGroup='metric'

def get_weather(location, startDate, endDate):
    # Location for the weather data
    Location = str(location)
    Location = Location.replace(' ', '%20')
    # Optional start and end dates
    # If nothing is specified, the forecast is retrieved.
    # If start date only is specified, a single historical or forecast day will be retrieved
    # If both start and and end date are specified, a date range will be retrieved
    StartDate = startDate
    EndDate = endDate

    #basic query
    apiQuery = base + Location

    if len(startDate):
        apiQuery+='/'+StartDate
        if len(EndDate):
            apiQuery+='/'+EndDate
    apiQuery+='?'

    if (len(UnitGroup)):
        apiQuery += '&unitGroup=' + UnitGroup

    apiQuery += '&key=' + ApiKey
    # JSON format supports daily, hourly, current conditions, weather alerts and events in a single JSON package
    # CSV format requires an 'include' parameter below to indicate which table section is required
    # ContentType = "csv"
    # include sections
    # values include days,hours,current,alerts
    # Include = "days"
    # we can specify the date range of information we are interested in the format yyyy-mm-dd
    response = requests.get(apiQuery)
    data = response.json()
    return data

def get_rel_data(api_data):
    '''
    Extracts relevant data from api call
    Return: DataFrame containing relevenat weather information
    '''
    # Core weather data types
    temp_min = api_data['days'][0]['tempmin']
    temp_max = api_data['days'][0]['tempmax']
    precip = []
    snow = []
    snow_depth = []
    windspeed = []

    month = int(dt.datetime.today().strftime('%m')) - 1
    seasons = ['Winter', 'Winter', 'Spring', 'Spring', 'Spring', 'Summer', 'Summer', 'Summer', 'Fall', 'Fall', 'Fall', 'Winter']
    season = seasons[month]
    season_fall = 1 if season == "Fall" else 0
    season_winter = 1 if season == "Winter" else 0
    season_spring = 1 if season == "Spring" else 0
    season_summer = 1 if season == "Summer" else 0


    for i in range (0,24):
        hours = api_data['days'][0]['hours'][i]
        precip.append(hours['precip'])
        snow.append(hours['snow'])
        snow_depth.append(hours['snowdepth'])
        windspeed.append(hours['windspeed'])

    precip = mean(precip)
    snow = mean(snow)
    snow_depth = mean(snow_depth)
    windspeed = mean(windspeed)

    # Descriptive labels
    description = api_data['description'].lower()
    fog_lbl = thunder_lbl = hail_lbl = dust_lbl = tornado_lbl = wind_lbl = snow_lbl = 0
    if 'fog' in description:
        fog_lbl = 1
    if 'thunder' in description:
        thunder_lbl = 1
    if 'hail' in description:
        hail_lbl = 1
    if 'dust' in description:
        dust_lbl = 1
    if 'tornado' in description:
        tornado_lbl = 1
    if 'wind' in description:
        wind_lbl = 1
    if 'snow' in description:
        snow_lbl = 1
    data = {'TMIN': temp_min, 'TMAX': temp_max, 'PRCP': precip, 
            'SNOW': snow, 'SNWD': snow_depth, 'AWND': windspeed, 'Season_Fall': season_fall, 
            'Season_Spring': season_spring, 'Season_Summer': season_summer, 'Season_Winter': season_winter,
            'Fog': fog_lbl, 'Thunder': thunder_lbl, 'Hail': hail_lbl, 'Dust': dust_lbl, 
            'Tornado': tornado_lbl, 'Wind': wind_lbl, 'Snow': snow_lbl}
    df = pd.DataFrame.from_records([data])
    return df

if __name__ == '__main__':
    import pandas as pd
    current = dt.datetime.today().strftime('%Y-%m-%d')
    enddate = (dt.datetime.today()+dt.timedelta(days=7)).strftime('%Y-%m-%d')

    loc = 'Middlesex County, New Jersey'
    current = get_weather(loc, current, enddate)
    data = get_rel_data(current)
    print(data)