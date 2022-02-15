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

col_mentions_name = ['GlobalEventID', 'EventTimeDate', 'MentionTimeDate',
    'MentionType', 'MentionSourceName', 'MentionIdentifier', 'SentenceID',
    'Actor1CharOffset', 'Actor2CharOffset', 'ActionCharOffset',
    'InRawText', 'Confidence', 'MentionDocLen', 'MentionDocTone',
    'MentionDocTranslationInfo', 'Extras']


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
    file_type = ['.export.CSV.zip', '.mentions.CSV.zip']

    # --- Dataframe initialization, with datetime index
    datetime_index = pd.date_range(start=initial_date, end=final_date, 
        freq='15min')
    
    df = pd.DataFrame([['', '']], columns=['date-str', 'zip'], 
        index=datetime_index)

    df['date-str'] = df.index.strftime('%Y%m%d%H%M%S')
    df['zip'] = df['date-str'].apply(lambda x: url+x)

    # --- Columns insertion : export, mentions, gkg  
    for csv in file_type:
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
            'eng-mentions': df.loc[date]['eng-mentions'],
            'translingual-export': df.loc[date]['translingual-export'],
            'translingual-mentions': df.loc[date]['translingual-mentions']
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

    df_mentions = pd.read_csv(
        dict_files['eng-mentions'], 
        sep='\t',
        names=col_mentions_name,
        header=None)
    
    # --- Other countries articles
    df_export_translingual = pd.read_csv(
        dict_files['translingual-export'], 
        sep='\t',
        names=col_events_name,
        header=None)

    df_mentions_translingual = pd.read_csv(
        dict_files['translingual-mentions'], 
        sep='\t',
        names=col_mentions_name,
        header=None)

    # --------------------------------------------------------------------
    # --- Concatenate DataFrames
    df_export = pd.concat(
        [df_export, df_export_translingual]).reset_index(drop=True)
    
    df_mentions = pd.concat(
        [df_mentions, df_mentions_translingual]).reset_index(drop=True)

    dict_df = {
        'export': df_export,
        'mentions': df_mentions}

    return dict_df


# ------------------------------------------------------------------------
def request1(dict_df):
    '''Processing csv files so it enables us to easily respond to the 
    first CQL request on Cassandra. The purpose of this function is to 
    simplify the data before copying into the Cassandra database.

    IMPORTANT: It's only for ONE 15 minutes period !

    Parameters
    ---------
    - dict_df: type: dict,
        Contains the 3 zip file names and DataFrames: export, mentions & 
        gkg (already concatenate for both English and Translingual 
        articles)

    Return
    -----
    - df_req1: type: DataFrame
        Contains the data ready to be inserted into a table on cassandra

    '''
    df_export = dict_df['export']
    df_mentions = dict_df['mentions']

    # --- Update MentionDocTranslationInfo
    df_mentions['MentionDocTranslationInfo'] = df_mentions[
        'MentionDocTranslationInfo'].apply(lambda x: 
            x.split(';')[0].split(':')[-1] if isinstance(x, str) 
            else 'eng')

    df_req1 = df_mentions.groupby(['GlobalEventID', 'MentionDocTranslationInfo']).agg({
        'EventTimeDate':'count'
        }).reset_index()

    df_req1 = df_req1.merge(
        df_export[['GlobalEventID', 'Day', 'ActionGeo_CountryCode']],
        how='inner',
        on='GlobalEventID'
    ).dropna()

    df_req1.columns = ['GlobalEventID', 'MentionDocTranslationInfo',
        'NbArticles', 'Day', 'ActionGeo_CountryCode']

    return df_req1


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
    df_req1 = pd.DataFrame([])

    for key_date in tqdm(dict_zip_files.keys()):
        dict_df = read_zip_files(dict_zip_files[key_date])

        df_req1 = pd.concat([
            df_req1, 
            request1(dict_df)]
            ).dropna().reset_index(drop=True)

    df_req1.to_csv('request1.csv', sep=',', index=False)

    print('*'*75)
    print("Number of file treated:", len(dict_zip_files.keys()))
    print("Execution time: {:.2f} s".format(t.time()-t_process_init))