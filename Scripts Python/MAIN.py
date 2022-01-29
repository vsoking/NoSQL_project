# ------------------------------------------------------------------------
# Created by - Alann Goerke
# Version - 1.1
# Last Update - 29.01.2022


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
    '''Generates all zip files to download between a user defined period
    of time.

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
    # --- Local variables
    url = 'http://data.gdeltproject.org/gdeltv2/'
    file_type = ['.export.CSV.zip', '.mentions.CSV.zip', '.gkg.csv.zip']

    # --- Dataframe initialization, with datetime index
    datetime_index = pd.date_range(start=initial_date, end=final_date, 
        freq='15min')
    
    df = pd.DataFrame([['', '']], columns=['date-str', 'zip'], 
        index=datetime_index)

    df['date-str'] = df.index.strftime('%Y%m%d%H%M%S')
    df['zip'] = df['date-str'].apply(lambda x: url+x)

    # --- Columns insertion : export, mentions, gkg  
    for csv in file_type:
        df.insert(loc=df.shape[1],
            column='eng-'+csv.split('.')[1],
            value=df['zip'].apply(lambda x: x+csv))
    
        df.insert(loc=df.shape[1],
            column='translingual-'+csv.split('.')[1],
            value=df['zip'].apply(lambda x: x+'.translation'+csv))

    # --- Delete unused columns 
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

    # --- Run the wget bash command
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
def read_zip_files(dict_files):
    '''Read some of the zip files downloaded with Pandas and concatenate
    the English and Translingual files.

    Parameters
    ---------
    - dict_files: type: dict
        Containing all the zip name files for a given date

    Return
    -----
    - dict_df: type: dict
        Dictionnary with all DataFrames inside for each zip file

    '''
    # --- Eng articles
    df_export = pd.read_csv(dict_files['eng-export'].split('/')[-1], 
        sep='\t',
        names=col_events_name,
        header=None)

    df_mentions = pd.read_csv(dict_files['eng-mentions'].split('/')[-1], 
        sep='\t',
        names=col_mentions_name,
        header=None)
    
    # --- Other countries articles
    df_export_translingual = pd.read_csv(dict_files['translingual-export'].split('/')[-1], 
        sep='\t',
        names=col_events_name,
        header=None)

    df_mentions_translingual = pd.read_csv(dict_files['translingual-mentions'].split('/')[-1], 
        sep='\t',
        names=col_mentions_name,
        header=None)
    
    # --- Concatenate DataFrames
    df_export = pd.concat([df_export, df_export_translingual]).reset_index(drop=True)
    df_mentions = pd.concat([df_mentions, df_mentions_translingual]).reset_index(drop=True)

    dict_df = {'export': df_export, 'mentions': df_mentions}

    return dict_df


# ------------------------------------------------------------------------
def request1(dict_df):
    '''Pre-processing csv files that enable us to easily respond to the 
    first CQL request on Cassandra. The purpose of this function is to 
    simplify the data before copying into the Cassandra database.

    IMPORTANT: It's only for ONE 15 minutes period !

    Parameters
    ---------
    - dict_df: type: dict,
        Contain the 3 zip file names and DataFrames: export, mentions & 
        gkg (already concatenate for both English and Translingual 
        articles)

    Return
    -----
    Writes a '.csv' file containing the data processed.
    
    '''
    df_export = dict_df['export']
    df_mentions = dict_df['mentions']

    # --- Attributs selection
    col_req1_export = ['GlobalEventID', 'Day', 'ActionGeo_CountryCode']
    col_req1_mentions = ['GlobalEventID', 'MentionDocTranslationInfo']

    df_req1 = df_export[col_req1_export]
    df_req1 = df_req1.merge(df_mentions[col_req1_mentions],
        how='inner', on='GlobalEventID')

    # Possible to change NAN into a str like 'UK' if needed
    df_req1['MentionDocTranslationInfo'] = df_req1[
        'MentionDocTranslationInfo'].apply(lambda x: 
            x.split(';')[0].split(':')[-1] if isinstance(x, str) else x)

    # --- Write df in csv
    df_req1.to_csv('data_request1.csv', index=False)


# ------------------------------------------------------------------------
def request2(dict_df):
    '''Pre-processing csv files that enable us to easily respond to the 
    second CQL request on Cassandra. 

    Parameters
    ---------
    - dict_df: type: dict,
        Contain the 3 zip file names and DataFrames: export, mentions & 
        gkg (already concatenate for both English and Translingual 
        articles)

    Return
    -----
    Writes a '.csv' file containing the data processed.
    
    '''
    df_export = dict_df['export']

    col_req2_export = ['GlobalEventID', 'Day', 'MonthYear', 'Year',
        'ActionGeo_CountryCode', 'NumMentions']

    df_req2 = df_export[col_req2_export]

    # --- Write df in csv
    df_req2.to_csv('data_request2.csv', index=False)


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
    dict_zip_files = dict(zip_files.iloc[0])
    dict_df = read_zip_files(dict_files=dict_zip_files)

    # --- Request 1
    request1(dict_df)
    print('-'*20, ' PRE-PROCESSING REQUEST 1 SUCCEED ', '-'*19, '\n')

    # --- Request 2
    request2(dict_df)
    print('-'*20, ' PRE-PROCESSING REQUEST 2 SUCCEED ', '-'*19, '\n')
 
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


    
