from pathlib import Path
import pandas as pd
import argparse

def split_area(df):
    df_states_only = df[~df['Area'].str.contains('County|Parish', na=False)]
    df_county_only = df[df['Area'].str.contains('County|Parish', na=False)]

    # Splits records into the form of a list that looks like ["State: County", "State: County", ...]
    states_split = df_states_only['Area'].str.split(r': |, ',regex=True)
    counties_split = df_county_only['Area'].str.split(r'; |: ', regex=True) # Special case for mulitiple states in a single row

    # Split function to handle edge cases
    def custom_split(x):
        def f(x, i_max):
            return [x[i] + ': ' + y for i in range(0, i_max, 2) for y in x[i+1].split(', ')]

        def g(x):
            lst = f(x, len(x)-1)
            lst.append(x[-1])
            return lst

        if len(x)%2 == 0:
            if ',' not in x[0]:
                return f(x, len(x))
            else:
                temp = x[0].split(', ')
                temp.append(x[1])
                first = temp.pop(0)
                temp.append(first)
                return g(temp)
        else: 
            return g(x)

    counties_split = counties_split.apply(custom_split)
    df['Area'] = pd.concat([states_split, counties_split])

    # Expands each element of the list into its own row
    df = df.explode('Area', ignore_index=True)

    # Cleans string
    def custom_clean(x):
        if x[-1] in [';', ':']:
            x = x[:-1]
        if x[-1] == ']':
            x = x[:x.index('[')]
        return x
    df['Area'] = df['Area'].apply(custom_clean)

    # Splits Area column further into State and County columns
    df[['State', 'County']] = df['Area'].str.split(pat=': ', expand=True)
    df.drop('Area', axis=1, inplace=True)
    df = df[['Start Date', 'Start Time', 'End Date', 'End Time', 'County', 'State', 'Number of Customers Affected']]
    # Remove 'County' string from entries
    df['County'] = df.County.astype(str)
    df['County'] = df['County'].map(lambda x: x.removesuffix(' County'))
    return df

def clean_data(source_path: Path, dest_path='hist_data/', csv_flag=False):
    '''Cleans annual summary files to save relevant data'''
    if csv_flag:
        df = pd.read_csv(source_path, header=1, skip_blank_lines=True)
    else:
        df = pd.read_excel(source_path, header=1)

    # Isolate outages caused by weather and remove unusable/unecessary data
    df = df[df['Event Type'].str.contains('Weather', na=False)]
    df = df[~df['Date of Restoration'].astype(str).str.contains('Unknown', na=False)]
    df.drop(columns=['Month', 'Alert Criteria', 'Event Type', 'Demand Loss (MW)', 'NERC Region'], inplace=True, errors='ignore')
    df.dropna(axis='columns', how='all', inplace=True)

    # Convert dates and times to consistent format
    df['Date Event Began'] = pd.to_datetime(df['Date Event Began'])
    df['Date Event Began'] = df['Date Event Began'].dt.date
    df['Date of Restoration'] = pd.to_datetime(df['Date of Restoration'])
    df['Date of Restoration'] = df['Date of Restoration'].dt.date
    df['Time Event Began'] = pd.to_datetime(df['Time Event Began'].astype(str))
    df['Time Event Began'] = df['Time Event Began'].dt.time
    df['Time of Restoration'] = pd.to_datetime(df['Time of Restoration'].astype(str))
    df['Time of Restoration'] = df['Time of Restoration'].dt.time

    # Rename cols for clarity
    df = df.rename(columns={"Date Event Began": "Start Date", "Time Event Began": "Start Time", 
        "Date of Restoration": "End Date", "Time of Restoration": "End Time", "Area Affected": "Area"})

    # Create dest filepath if it doesn't exist and save to csv
    filename = source_path.name[:-4]
    if filename[:4] in ["2017", "2018", "2019", "2020", "2021"]:
        df = split_area(df)

    filepath = Path(dest_path + filename + '.csv')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Format excel data files taken from OE417 annual summaries')
    parser.add_argument('-p', '--path', type=str, required=True, help='folder where unclean files are located')
    parser.add_argument('-c','--csv', dest='csv', action='store_true', default=False, help='flag to read csv files instead of excel')
    parser.add_argument('-d','--dest', type=str, default='hist_data/', help='destination to save csv files')
    parser.add_argument('-m','--merge', action='store_true', default=False, help='merge data into a single csv file and delete individual csvs')
    args = parser.parse_args()

    if args.dest[-1] != '/':
        args.dest += '/'

    data_source_path = args.path         # path where unclean data is located
    path_list = Path(data_source_path).glob('*.xls') if not args.csv else Path(data_source_path).glob('*.csv')
    for path in path_list:
        clean_data(path, args.dest, args.csv)

    if args.merge:
        data_list = Path(args.dest).glob('*_Annual_Summary.csv')
        lst = sorted([str(path) for path in data_list])
        df = pd.concat(map(pd.read_csv, lst), ignore_index=True)
        df.to_csv(Path(args.dest + 'merged_data.csv'), index=False)

        # Delete individual files
        for path in lst:
            Path(path).unlink()