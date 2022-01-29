# ------------------------------------------------------------------------
# Created by - Alann Goerke
# Version - 1.0
# Last Update - 28.01.2022


# ------------------------------------------------------------------------
# --- IMPORTS
# ------------------------------------------------------------------------
import pandas as pd
import time as t
import subprocess
import sys


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

col_gkg_name = [''] # --- To be completed


# ------------------------------------------------------------------------
# --- FUNCTIONS
# ------------------------------------------------------------------------
def generate_zip_files(initial_date, final_date):
    '''Generates all zip files to download between a user define period of
    time.

    Parameters
    ---------
    - initial_date: type: str, format: YYYYMMDDHHMMSS
    - final_date: type: str, format: YYYYMMDDHHMMSS

    Return
    -----
    - df: type: DataFrame
        All the zip files for English and Translingual : export, mentions
        and gkg
    
    '''
    # --- Local variables definiton
    url = 'http://data.gdeltproject.org/gdeltv2/'
    file_type = ['.export.CSV.zip', '.mentions.CSV.zip', '.gkg.csv.zip']

    datetime_index = pd.date_range(start=initial_date, end=final_date, freq='15min')

    df = pd.DataFrame([['', '']], columns=['date-str', 'zip'], index=datetime_index)

    df['date-str'] = df.index.strftime('%Y%m%d%H%M%S')
    df['zip'] = df['date-str'].apply(lambda x: url+x)

    for csv in file_type:
        df.insert(loc=df.shape[1],
            column='eng-'+csv.split('.')[1],
            value=df['zip'].apply(lambda x: x+csv))
    
        df.insert(loc=df.shape[1],
            column='translingual-'+csv.split('.')[1],
            value=df['zip'].apply(lambda x: x+'.translation'+csv))

    del df['date-str']
    del df['zip']

    return df


# ------------------------------------------------------------------------
def wget_file(file):
    '''Run the wget bash command to download only ONE user defined file.

    Parameters
    ---------
    - file: type: str

    '''
    cmd = ['wget', file]

    proc = subprocess.Popen(cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    try:
        out, err = proc.communicate(timeout=10)
        code = proc.returncode
        print("OUT: '{}'".format(out))
        print("ERR: '{}'".format(err))
        print("EXIT: {}".format(code))
    except subprocess.TimeoutExpired:
        proc.kill()
        print("TIMEOUT")


# ------------------------------------------------------------------------
def request1(dict_files):
    '''Pre-processing all csv files for only ONE 15 minutes period. The
    prupose of this function is to simplify the data before copying in
    Cassandra database.

    Parameters
    ---------
    - dict_files: type: dict,
        Contain the 6 zip file names (export, mentions & gkg for both 
        English and translingual artciles)

    Return
    -----
    Writes a new .csv file that enable us to easily respond to the first
    CQL request on Cassandra.
    
    '''
    # --------------------------------------------------------------------
    # --- Eng articles 
    df_export = pd.read_csv(dict_files['eng-export'].split('/')[-1], 
        sep='\t',
        names=col_events_name,
        header=None)
    
    df_mentions = pd.read_csv(dict_files['eng-mentions'].split('/')[-1], 
        sep='\t',
        names=col_mentions_name,
        header=None)

    col_req1_export = ['GlobalEventID', 'Day', 'ActionGeo_CountryCode']
    col_req1_mentions = ['GlobalEventID', 'MentionDocTranslationInfo']

    df_req1 = df_export[col_req1_export]
    df_req1 = df_req1.join(df_mentions[col_req1_mentions], rsuffix='_')
    del df_req1['GlobalEventID_']

    # --------------------------------------------------------------------
    # --- Translingual articles
    df_export_translingual = pd.read_csv(
        dict_files['translingual-export'].split('/')[-1], 
        sep='\t',
        names=col_events_name,
        header=None)

    df_mentions_translingual = pd.read_csv(
        dict_files['translingual-mentions'].split('/')[-1], 
        sep='\t',
        names=col_mentions_name,
        header=None)

    col_req1_export_translingual = ['GlobalEventID', 'Day', 
        'ActionGeo_CountryCode']
    col_req1_mentions_translingual = ['GlobalEventID', 
        'MentionDocTranslationInfo']

    df_req1_translingual = df_export_translingual[col_req1_export_translingual]
    df_req1_translingual = df_req1_translingual.join(
        df_mentions_translingual[col_req1_mentions_translingual], 
        rsuffix='_')

    del df_req1_translingual['GlobalEventID_']

    df_req1_translingual['MentionDocTranslationInfo'] = df_req1_translingual[
        'MentionDocTranslationInfo'].apply(
            lambda x: x.split(';')[0].split(':')[-1])

    # --------------------------------------------------------------------
    # --- Join both of Eng and Translingual articles    
    df_req1_final = pd.concat(
        [df_req1, df_req1_translingual]).reset_index(drop=True)
    
    # --- Write df in csv
    df_req1_final.to_csv('data_request1.csv', index=False)


# ------------------------------------------------------------------------
# --- MAIN
# ------------------------------------------------------------------------
initial_date = sys.argv[1]  # str
final_date = sys.argv[2]  # str

if __name__ == '__main__':
    # --------------------------------------------------------------------
    # --- Generation of zip-links between initial and final dates
    print('-'*75)
    print('---', ' Generating zip files, please wait ...\n')
    t_init = t.time()
    zip_files = generate_zip_files(initial_date=initial_date, 
        final_date=final_date)
    print('-'*22, ' ZIP FILES GENERATION SUCCEED ', '-'*21)
    
    # --------------------------------------------------------------------
    # --- Download of all zip files
    print('-'*75)
    print('---', ' Downloading zip files, please wait ...\n')
    for file in zip_files.iloc[0]:
        wget_file(file)
    print('-'*23, ' ZIP FILES DOWNLOAD SUCCEED ', '-'*22)
    
    # --------------------------------------------------------------------
    # --- Pre-processing of zip files for requests
    print('-'*75)
    print('---', ' Pre-processing zip files, please wait ...\n')

    # --- Request 1
    dict_zip_files = dict(zip_files.iloc[0])
    request1(dict_zip_files)
    print('-'*20, ' PRE-PROCESSING REQUEST 1 SUCCEED ', '-'*19, '\n')

    # --- Request 2
    # note : it may be better to use a function read_csv_files, then use 
    #        request1, request2 ... becasue the request 2 is using the same 
    #        tools as the first one.
 
    # --- Request 3

    # --- Request 4

    # --------------------------------------------------------------------
    # --- Delete all zip files


    # --------------------------------------------------------------------
    # --- Docker : copy csv into a containeur 

    # --------------------------------------------------------------------
    # --- Write and Run CQL queries with cassandra-driver if possible

    # --------------------------------------------------------------------
    # --- Delete all zip files

    print('Estimated time : {:.2f}s'.format(t.time()-t_init))


    
