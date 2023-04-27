from xmlrpc.client import Boolean
from flask import (
    Blueprint, 
    render_template, 
    request
)
import app.methods.outage_map as om
import app.methods.helpers as he
import io
import json
import pandas as pd
import plotly
import app.methods.data as weather_data
from datetime import datetime, timedelta
import plotly.graph_objects as go
import datetime as dt
from joblib import load
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder


# Blueprint Configuration
homepage_bp = Blueprint(
    'homepage_bp', __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/homepage/static'
)

@homepage_bp.route('/')
def index():
    return render_template('index.html')

@homepage_bp.route('/outage-map', methods=['GET'])
def outage_map():
    real_time = om.OutageMap()
    fig = real_time.real_time_outages_map()

    buffer = io.StringIO()
    fig.write_html(buffer, full_html=False, include_plotlyjs='cdn')

    nav = True
    nav_flag = request.args.get('navflag', default = 'True')
    if nav_flag == "False":
        nav = False

    return render_template('outage_map.html', map=buffer.getvalue(), nav_flag=nav)

@homepage_bp.route('/search-records', methods=['GET', 'POST'])
def search_records():
    if request.method == 'GET':
        return render_template('search_records.html')
    if request.method == 'POST':
        county = request.form['county'].strip()
        state = request.form['state'].strip()
        start_date = request.form['start_date'].strip()
        end_date = request.form['end_date'].strip()
        if start_date == "":
            start_date = '2017-01-01'
        if end_date == "":
            end_date = '2021-06-28'

        df = he.filter_df_by_location((county, state))
        df = df[['Start Date', 'End Date', 'County', 'State', 'Number of Customers Affected']]
        df.loc[df['County'] == 'None', 'County'] = '---'
        df = he.filter_df_by_date(start_date, end_date, df)

        return render_template('search_records.html', county=county, state=state, start_date=start_date, end_date=end_date, 
            num_records=df.shape[0], df=df.to_html(classes="table table-striped table-sm table-hover table-bordered", index=False, 
            justify='center'))

@homepage_bp.route('/search-records-no-nav', methods=['GET', 'POST'])
def search_records_no_nav():
    if request.method == 'GET':
        return render_template('search_records_nonav.html')
    if request.method == 'POST':
        county = request.form['county'].strip()
        state = request.form['state'].strip()
        start_date = request.form['start_date'].strip()
        end_date = request.form['end_date'].strip()
        if start_date == "":
            start_date = '2017-01-01'
        if end_date == "":
            end_date = '2021-06-28'

        df = he.filter_df_by_location((county, state))
        df = df[['Start Date', 'End Date', 'County', 'State', 'Number of Customers Affected']]
        df.loc[df['County'] == 'None', 'County'] = '---'
        df = he.filter_df_by_date(start_date, end_date, df)

        return render_template('search_records_nonav.html', county=county, state=state, start_date=start_date, end_date=end_date, 
            num_records=df.shape[0], df=df.to_html(classes="table table-striped table-sm table-hover table-bordered", index=False, 
            justify='center'))


@homepage_bp.route('/real-time-weather', methods=['GET', 'POST'])
def real_time_weather():
    current = datetime.today().strftime('%Y-%m-%d')
    enddate = (datetime.today()+timedelta(days=7)).strftime('%Y-%m-%d')

    if request.method == 'GET':
        return render_template('real_time_weather.html')
    if request.method == "POST":
        search = request.form['search']
        # running the method to get the data from the api
        current = weather_data.get_weather(search, current, enddate)
        description = current['description']

        #Day_1
        dateTime = []
        temp = []
        temp_min = []
        temp_max = []
        humidity = []
        precip = []
        precip_type = []
        snow = []
        snow_depth = []
        windgust = []
        windspeed = []

        # general day weather
        day1 = current['days'][0]['datetime']
        temp_min.append(current['days'][0]['tempmin'])
        temp_max.append(current['days'][0]['tempmax'])
        des_day1 = current['days'][0]['description']

        # for all the hours in that day
        for i in range (0,24):
            hours = current['days'][0]['hours'][i]
            dateTime.append(hours['datetime'])
            temp.append(hours['temp'])
            humidity.append(hours['humidity'])
            precip.append(hours['precip'])
            precip_type.append(hours['preciptype'])
            snow.append(hours['snow'])
            snow_depth.append(hours['snowdepth'])
            windgust.append(hours['windgust'])
            windspeed.append(hours['windspeed'])

        dataframe = pd.DataFrame(dateTime, columns=['Time'])
        dataframe['temp'] = temp
        dataframe['humidity'] = humidity
        dataframe['precipitation'] = precip
        dataframe['precipitation type'] = precip_type
        dataframe['snow'] = snow
        dataframe['snow depth'] = snow_depth
        dataframe['windgust'] = windgust
        dataframe['windspeed'] = windspeed

        # creating the graph for the dataframe
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=dateTime, y=temp, name='Temperature', line=dict(color='black')))
        fig1.add_trace(go.Scatter(x=dateTime, y=humidity, name='Humidity', line=dict(color='red')))
        fig1.add_trace(go.Scatter(x=dateTime, y=precip, name='precipitation', line=dict(color='blue')))
        fig1.add_trace(go.Scatter(x=dateTime, y=snow, name='Snow', line=dict(color='green')))
        fig1.add_trace(go.Scatter(x=dateTime, y=snow_depth, name='Snow Depth', line=dict(color='yellow')))
        fig1.add_trace(go.Scatter(x=dateTime, y=windgust, name='Wind Gust', line=dict(color='orange')))
        fig1.add_trace(go.Scatter(x=dateTime, y=windspeed, name='Wind Speed', line=dict(color='violet')))

        fig1.update_layout(title='Temperature Forecast for ' + day1,
                          xaxis_title='Time',
                          yaxis_title='Weather Conditions')
        graphJSON = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

        return render_template('real_time_weather.html', description=description, search=search,
                                dateTime1=day1, description1=des_day1, temp_min1=temp_min, 
                                temp_max1=temp_max, graph1=graphJSON)

@homepage_bp.route('/real-time-weather-no-nav', methods=['GET', 'POST'])
def real_time_weather_no_nav():
    current = datetime.today().strftime('%Y-%m-%d')
    enddate = (datetime.today()+timedelta(days=7)).strftime('%Y-%m-%d')

    if request.method == 'GET':
        return render_template('real_time_weather_nonav.html')
    if request.method == "POST":
        search = request.form['search']
        # running the method to get the data from the api
        current = weather_data.get_weather(search, current, enddate)
        description = current['description']

        #Day_1
        dateTime = []
        temp = []
        temp_min = []
        temp_max = []
        humidity = []
        precip = []
        precip_type = []
        snow = []
        snow_depth = []
        windgust = []
        windspeed = []

        # general day weather
        day1 = current['days'][0]['datetime']
        temp_min.append(current['days'][0]['tempmin'])
        temp_max.append(current['days'][0]['tempmax'])
        des_day1 = current['days'][0]['description']

        # for all the hours in that day
        for i in range (0,24):
            hours = current['days'][0]['hours'][i]
            dateTime.append(hours['datetime'])
            temp.append(hours['temp'])
            humidity.append(hours['humidity'])
            precip.append(hours['precip'])
            precip_type.append(hours['preciptype'])
            snow.append(hours['snow'])
            snow_depth.append(hours['snowdepth'])
            windgust.append(hours['windgust'])
            windspeed.append(hours['windspeed'])

        dataframe = pd.DataFrame(dateTime, columns=['Time'])
        dataframe['temp'] = temp
        dataframe['humidity'] = humidity
        dataframe['precipitation'] = precip
        dataframe['precipitation type'] = precip_type
        dataframe['snow'] = snow
        dataframe['snow depth'] = snow_depth
        dataframe['windgust'] = windgust
        dataframe['windspeed'] = windspeed

        # creating the graph for the dataframe
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=dateTime, y=temp, name='Temperature', line=dict(color='black')))
        fig1.add_trace(go.Scatter(x=dateTime, y=humidity, name='Humidity', line=dict(color='red')))
        fig1.add_trace(go.Scatter(x=dateTime, y=precip, name='precipitation', line=dict(color='blue')))
        fig1.add_trace(go.Scatter(x=dateTime, y=snow, name='Snow', line=dict(color='green')))
        fig1.add_trace(go.Scatter(x=dateTime, y=snow_depth, name='Snow Depth', line=dict(color='yellow')))
        fig1.add_trace(go.Scatter(x=dateTime, y=windgust, name='Wind Gust', line=dict(color='orange')))
        fig1.add_trace(go.Scatter(x=dateTime, y=windspeed, name='Wind Speed', line=dict(color='violet')))

        fig1.update_layout(title='Temperature Forecast for ' + day1,
                          xaxis_title='Time',
                          yaxis_title='Weather Conditions')
        graphJSON = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

        return render_template('real_time_weather_nonav.html', description=description, search=search,
                                dateTime1=day1, description1=des_day1, temp_min1=temp_min, 
                                temp_max1=temp_max, graph1=graphJSON)

@homepage_bp.route('/regression-model', methods=['GET', 'POST'])
def reg_model():
    if request.method == 'GET':
        return render_template('model.html')
    elif request.method == 'POST':
        current = datetime.today().strftime('%Y-%m-%d')
        enddate = (datetime.today()+timedelta(days=7)).strftime('%Y-%m-%d')

        county = request.form['county']
        state = request.form['state']
        start_date = request.form['start_date']

        now = dt.datetime.utcnow()
        start = dt.datetime.strptime(start_date, '%Y-%m-%d %H:%M')
        hours = abs(now-start).total_seconds() / 3600

        loc = county
        if loc != '':
            loc = loc + ', ' + state
        else:
            loc = state
        # Get current weather data for location
        current = weather_data.get_weather(loc, current, enddate)
        df = weather_data.get_rel_data(current)
        df['Outage'] = 1
        df['hours'] = hours
        df['State'] = state

        model_states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California',
                        'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia',
                        'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                        'Louisiana', 'Maine', 'Maryland', 'Michigan', 'Minnesota',
                        'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
                        'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
                        'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
                        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
                        'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
                        'West Virginia', 'Wisconsin', 'Wyoming']
        if state in model_states:
            df['State'] = model_states.index(state)
        else:
            df['State'] = len(model_states)

        df = normalize_data(df.copy())
        xgb_model = load('reg_model.joblib')
        y = xgb_model.predict(df)
        
        pred = f"{y} Customers to be Affected"
        if y[0] <= 2:
            pred = "Outage unlikely to occur based on current weather conditions (Model predicts < 2 people affected)"
        
        return render_template('model.html', pred=pred, county=county, state=state, start_date=start_date)

@homepage_bp.route('/regression-model-no-nav', methods=['GET', 'POST'])
def reg_model_no_nav():
    if request.method == 'GET':
        return render_template('model_nonav.html')
    elif request.method == 'POST':
        current = datetime.today().strftime('%Y-%m-%d')
        enddate = (datetime.today()+timedelta(days=7)).strftime('%Y-%m-%d')

        county = request.form['county']
        state = request.form['state']
        start_date = request.form['start_date']

        now = dt.datetime.utcnow()
        start = dt.datetime.strptime(start_date, '%Y-%m-%d %H:%M')
        hours = abs(now-start).total_seconds() / 3600

        loc = county
        if loc != '':
            loc = loc + ', ' + state
        else:
            loc = state
        # Get current weather data for location
        current = weather_data.get_weather(loc, current, enddate)
        df = weather_data.get_rel_data(current)
        df['Outage'] = 1
        df['hours'] = hours
        df['State'] = state

        model_states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California',
                        'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia',
                        'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                        'Louisiana', 'Maine', 'Maryland', 'Michigan', 'Minnesota',
                        'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
                        'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
                        'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
                        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
                        'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
                        'West Virginia', 'Wisconsin', 'Wyoming']
        if state in model_states:
            df['State'] = model_states.index(state)
        else:
            df['State'] = len(model_states)

        df = normalize_data(df.copy())
        xgb_model = load('reg_model.joblib')
        y = xgb_model.predict(df)

        pred = f"{y} Customers to be Affected"
        if y[0] <= 2:
            pred = "Outage unlikely to occur based on current weather conditions (Model predicts < 2 people affected)"
        
        return render_template('model_nonav.html', pred=pred, county=county, state=state, start_date=start_date)

def normalize_data(df):
    mean_std_dict = {'State': [23.487577957264083, 13.852090218156427],
                    'Outage': [0.01435180451896534, 0.1189370043776488],
                    'AWND': [3.6046979067938993, 1.4829674266885424],
                    'PRCP': [2.6506215026527857, 6.174512701716785],
                    'SNOW': [2.154551489298647, 13.406634232864967],
                    'SNWD': [17.32275710760568, 72.79354447532016],
                    'TMAX': [18.445801819436678, 11.322569065874935],
                    'TMIN': [6.772201132654679, 10.388184118762583],
                    'Fog': [0.6803496574992333, 0.4663440579303284],
                    'Thunder': [0.24240875166138431, 0.4285429917670362],
                    'Hail': [0.010620079746447193, 0.10250574586962279],
                    'Dust': [0.0050991718638176056, 0.07122664631620647],
                    'Tornado': [0.000830692158266026, 0.02880994127495925],
                    'Wind': [0.00011501891422144975, 0.010724138876776201],
                    'Snow': [2.5559758715877723e-05, 0.005055633695301739],
                    'hours': [1.2550957638702203, 13.112778029383072],
                    'Season_Fall': [0.2230983539515387, 0.4163264266446045],
                    'Season_Spring': [0.28217973622329007, 0.4500632414908624],
                    'Season_Summer': [0.23780799509252631, 0.42574366585275464],
                    'Season_Winter': [0.2569139147326449, 0.43693431425849816]}
    for col in df.columns:
        df[col] = df[col].apply(lambda x: (x - mean_std_dict[col][0])/mean_std_dict[col][1])
    return df