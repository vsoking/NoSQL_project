# ------------------------------------------------------------------------
# --- IMPORTS
# ------------------------------------------------------------------------
import time as t
import pandas as pd
import sys

from tqdm import tqdm


# ------------------------------------------------------------------------
# --- GLOBAL VARIABLES
# ------------------------------------------------------------------------
col_events_name = [
    # --- Dates
    'GlobalEventID', 'Day', 'MonthYear', 'Year', 'FractionDate',
    # --- Actors
    'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
    'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code',
    'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
    'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
    'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code',
    'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
    # --- Events
    'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass',
    'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
    # --- Geo
    'Actor1Geo_Type', 'Actor1Geo_Fullname', 'Actor1Geo_CountryCode',
    'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat',
    'Actor1Geo_Long', 'Actor1Geo_FeatureID',
    'Actor2Geo_Type', 'Actor2Geo_Fullname', 'Actor2Geo_CountryCode',
    'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code', 'Actor2Geo_Lat',
    'Actor2Geo_Long', 'Actor2Geo_FeatureID',
    'ActionGeo_Type', 'ActionGeo_Fullname', 'ActionGeo_CountryCode',
    'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code', 'ActionGeo_Lat',
    'ActionGeo_Long', 'ActionGeo_FeatureID',
    # --- General
    'DATEADDED', 'SOURCEURL']


# ------------------------------------------------------------------------
# --- FUNCTIONS
# ------------------------------------------------------------------------
def generate_zip_files(initial_date, final_date):
    '''Generates all url zip-files, for each date between a user defined 
    period of time.    

    Parameters
    ---------
    - initial_date: type: str, format: YYYYMMDDHHMMSS
    - final_date: type: str, format: YYYYMMDDHHMMSS

    Return
    -----
    - dict_zip_files: type: dict
        For each date, it contains 6 url where the data can be imported
        into a dataframe, or dowloaded with a wget cmd.
        . English : eng-export, eng-mentions, eng-gkg
        . Translingual : translingual-export, translingual-mentions, 
        translingual-gkg
    
    '''
    # --- Local variables
    url = 'http://data.gdeltproject.org/gdeltv2/'
    csv = '.export.CSV.zip'

    # --- Dataframe initialization, with datetime index
    datetime_index = pd.date_range(start=initial_date, end=final_date, 
        freq='15min')
    
    df = pd.DataFrame([['', '']], columns=['date-str', 'zip'], 
        index=datetime_index)

    df['date-str'] = df.index.strftime('%Y%m%d%H%M%S')
    df['zip'] = df['date-str'].apply(lambda x: url+x)

    # --- Columns insertion : export, mentions, gkg  
    # --- English files
    df.insert(
        loc=df.shape[1],
        column='eng-'+csv.split('.')[1],
        value=df['zip'].apply(lambda x: x+csv))

    # --- Translingual files
    df.insert(
        loc=df.shape[1],
        column='translingual-'+csv.split('.')[1],
        value=df['zip'].apply(lambda x: x+'.translation'+csv))

    # --- Delete unused columns 
    del df['date-str']
    del df['zip']

    # --- Store all the csv url files in a dictionnary
    dict_zip_files = {}

    for date in df.index:
        dict_zip_files[str(date)] = {
            'eng-export': df.loc[date]['eng-export'],
            'translingual-export': df.loc[date]['translingual-export']
        }

    return dict_zip_files


# ------------------------------------------------------------------------
def read_zip_files(dict_files):
    '''Read zip files downloaded with Pandas and concatenate
    the English and Translingual files.

    Parameters
    ---------
    - dict_files: type: dict
        Contains all the url zip name files for a given date

    Return
    -----
    - dict_df: type: dict
        Dictionnary with all DataFrames inside for each zip file

    '''
    # --------------------------------------------------------------------
    # --- Eng articles
    df_export = pd.read_csv(
        dict_files['eng-export'], 
        sep='\t',
        names=col_events_name,
        header=None)
    
    # --- Other countries articles
    df_export_translingual = pd.read_csv(
        dict_files['translingual-export'], 
        sep='\t',
        names=col_events_name,
        header=None)

    # --------------------------------------------------------------------
    # --- Concatenate DataFrames
    df_export = pd.concat(
        [df_export, df_export_translingual]).reset_index(drop=True)

    dict_df = {'export': df_export}

    return dict_df

# ------------------------------------------------------------------------
def request2(dict_df):
    df_export = dict_df['export']

    # --- Attributs selection for the request
    col_req2_export = ['GlobalEventID', 'ActionGeo_CountryCode',
        'NumMentions']

    df_req2 = df_export[col_req2_export]

    # --- Split day into month and year
    df_day = df_export['Day'].astype(str)

    df_req2.insert(loc=df_req2.shape[1],
        column='Year',
        value=df_day.apply(lambda x: int(x[:4])))

    df_req2.insert(loc=df_req2.shape[1],
        column='MonthYear',
        value=df_day.apply(lambda x: int(x[4:6])))

    df_req2.insert(loc=df_req2.shape[1],
        column='Day',
        value=df_day.apply(lambda x: int(x[6:])))

    return df_req2


# ------------------------------------------------------------------------
# --- MAIN
# ------------------------------------------------------------------------
initial_date = sys.argv[1]  # str
final_date = sys.argv[2]  # str

if __name__=='__main__':
    t_process_init = t.time()

    dict_zip_files = generate_zip_files(initial_date=initial_date, 
        final_date=final_date)

    # --- Lopp over all dates between init and final
    df_req2 = pd.DataFrame([])

    for key_date in tqdm(dict_zip_files.keys()):
        dict_df = read_zip_files(dict_zip_files[key_date])

        df_req2 = pd.concat([
            df_req2, 
            request2(dict_df)]
            ).dropna().reset_index(drop=True)

    df_req2.to_csv('request2.csv', sep=',', index=False)

    print('*'*75)
    print("Number of file treated:", len(dict_zip_files.keys()))
    print("Execution time: {:.2f} s".format(t.time()-t_process_init))