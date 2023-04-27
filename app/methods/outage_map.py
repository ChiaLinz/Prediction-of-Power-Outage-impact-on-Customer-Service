import requests
import pandas as pd
import plotly.express as px
import io
import numpy as np
import datetime as dt

class OutageMap:
    def create_map(self, fips, title=""):
        '''
        Method to create an outage map based on a list of FIPS codes

        Keyword arguments:\n
        fips -- A list of FIPS codes to be highlighted

        Optional arguments:\n
        title -- A title for the map. Defaults to "" 
        '''
        df = pd.DataFrame(fips, columns=['CountyFIPS', 'Start Time'])
        df['outage'] = 1
        df['CountyFIPS'] = df.CountyFIPS.astype(int)
        df['Start Time'] = pd.to_datetime(df['Start Time'], utc=True).dt.strftime('%m/%d/%Y %H:%M %Z')
        df['Start Time'] = df['Start Time'].fillna('Unknown')

        fips_master = pd.read_csv('data/fips_table.csv')
        df = df.merge(fips_master, on='CountyFIPS')

        fips_url = 'https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json'
        response = requests.get(fips_url)
        counties = response.json() 

        fig = px.choropleth(df, geojson=counties, locations='CountyFIPS', color='outage',
                                color_continuous_scale='PuBu',
                                range_color=(1,1),
                                title="Real-Time Outage Map",
                                scope="usa",
                                labels={'CountyName':'County', 'StateAbbr':'State'},
                                hover_data={'CountyName':True, 'StateAbbr':True, 'outage':False, 'CountyFIPS':False, 'Start Time':True}
                            )
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, coloraxis_showscale=False)
        fig.update_layout(title = {
                'text': title,
                'y':0.95,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top' 
            })
        fig.layout.xaxis.fixedrange = True
        fig.layout.yaxis.fixedrange = True
        fig.layout.dragmode = False
        return fig

    def real_time_outages_fips(self):
        '''
        Method to get a list of FIPS codes correlating to counties that have 
        real-time power outages
        '''
        outage_url = 'https://odin.ornl.gov/odi'
        params = {
            'format': 'json',
        }

        response = requests.get(outage_url, params=params)
        r_json = response.json()
        outage_counties = []

        for outage in r_json['outage']:
            if 'outageArea' in outage:
                if outage['outageArea']['outageAreaKind'] == "COUNTY" and len(outage['communityDescriptor']) == 5:
                    if  'reportedStartTime' in outage:
                        outage_counties.append([outage['communityDescriptor'], outage['reportedStartTime']])
                    else:
                        outage_counties.append([outage['communityDescriptor'], np.nan])

        return outage_counties

    def real_time_outages_map(self, title="Real-Time Power Outages"):
        '''
        Method to create an outage map for counties with real-time power outages

        Optional arguments:\n
        title -- A title for the map. Defaults to "Real-Time Power Outages" 
        '''
        fips = self.real_time_outages_fips()
        fig = self.create_map(fips, title)
        return fig

if __name__ == "__main__":
    real_time = OutageMap()
    fig = real_time.real_time_outages_map()

    buffer = io.StringIO()

    #fig.write_html('temp/myplot_two.html', full_html=False, include_plotlyjs='cdn')
    fig.show()
