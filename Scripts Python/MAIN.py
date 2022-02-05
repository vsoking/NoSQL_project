# ------------------------------------------------------------------------
# Created by - Alann Goerke
# Version - 4.0
# Last Update - 02.05.2022


# ------------------------------------------------------------------------
# --- IMPORTS
# ------------------------------------------------------------------------
import sys
import time as t
import pandas as pd

from cassandra.cluster import Cluster


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
            'eng-gkg': df.loc[date]['eng-gkg'],
            'translingual-export': df.loc[date]['translingual-export'],
            'translingual-mentions': df.loc[date]['translingual-mentions'],
            'translingual-gkg': df.loc[date]['translingual-gkg']
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

    dict_df = {'export': df_export, 'mentions': df_mentions}

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

    # --- Attributs selection for the request
    col_req1_export = ['GlobalEventID', 'Day', 'ActionGeo_CountryCode',
        'NumArticles']
    col_req1_mentions = ['GlobalEventID', 'MentionDocTranslationInfo']

    df_req1 = df_export[col_req1_export]

    df_req1 = df_req1.merge(
        df_mentions[col_req1_mentions],
        how='inner',
        on='GlobalEventID')

    # --- Update MentionDocTranslationInfo
    df_req1['MentionDocTranslationInfo'] = df_req1[
        'MentionDocTranslationInfo'].apply(lambda x: 
            x.split(';')[0].split(':')[-1] if isinstance(x, str) 
            else 'eng')
    
    return df_req1


# ------------------------------------------------------------------------
def cassandra_insert_req1(keyspace, table, df_data):
    '''Insert data, row by row, into a user defined table in Cassandra.

    Parameters
    ---------
    - keyspace: type: str
        Name of the existing keyspace
    - table: type: str
        Name of the existing table for the request 1
    - df_data: type: DataFrame
        Contains all the data, pre-process, to be inserted in a table

    '''
    # --- Cassandra cluster connection
    cluster = Cluster()
    session = cluster.connect(keyspace)

    # --- Data insertion
    insert = "INSERT INTO {} (globaleventid, day, ".format(table) +\
            "actiongeocountrycode, numarticles, mentiondoctranslationinfo)" +\
            " VALUES ("
    
    df_req = df_data.dropna()  # NaN raises pbm if its not deleted

    df_req.insert(
        loc=df_req.shape[1],
        column='insert_query',
        value=insert+df_req['GlobalEventID'].astype(str)+','+
            df_req['Day'].astype(str)+",'"+
            df_req['ActionGeo_CountryCode']+"',"+
            df_req['NumArticles'].astype(str)+",'"+
            df_req['MentionDocTranslationInfo']+"');")
    
    # --- Execution of the insertion query
    df_req['insert_query'].apply(lambda x: session.execute(x))


# ------------------------------------------------------------------------
def format_date(date):
    """Modify the shape of the day from YYYYMMDDHHMMSS 
    to YYYY-MM-DD HH:MM:SS.

    Parameter
    ---------
    - date: type: str, format: YYYYMMDDHHMMSS

    Return
    -----
    - modified_date_shape: type: str, format: YYYY-MM-DD HH:MM:SS

    """
    d = '-'.join([date[:4], date[4:6], date[6:8]])
    h = ':'.join([date[8:10], date[10:12]])

    modified_date_shape = ' '.join([d, h])

    return modified_date_shape


# ------------------------------------------------------------------------
# --- MAIN
# ------------------------------------------------------------------------
initial_date = sys.argv[1]  # str
final_date = sys.argv[2]  # str

req1 = True
req2 = False
req3 = False
req4 = False

if __name__ == '__main__':
    print('*'*75)
    print('GDELT PROJECT - INF728 NOSQL\n')

    t_init = t.time()
    d_init = format_date(initial_date)
    d_final = format_date(final_date)

    # --------------------------------------------------------------------
    # --- Generation of zip-links between initial and final dates
    print('-'*75)
    print('Generating all zip files between {} '.format(d_init) +\
        'and {}, \nplease wait...\n'.format(d_final))
    
    dict_zip_files = generate_zip_files(initial_date=initial_date, 
        final_date=final_date)

    print('Time: {:.2f} s'.format(t.time()-t_init))
    print('-'*22, ' ZIP FILES GENERATION SUCCEED ', '-'*21, '\n')

    # --- Lopp over all dates between init and final
    for key_date in dict_zip_files.keys():
        t_process_init = t.time()

        print('-'*75)
        print('- FILE: {}\n'.format(key_date))

        # ----------------------------------------------------------------
        # --- Processing zip files for requests
        print('Processing zip files, please wait...')

        dict_df = read_zip_files(dict_zip_files[key_date])

        # ----------------------------------------------------------------
        # --- Cassandra : Insert files into a user define table
        # --- Request 1
        if req1:
            print('Request1: Inserting values into Cassandra, ' +\
                'please wait...')
            
            cassandra_insert_req1(KEYSPACE='gdelt_alann',
                TABLE='test1',
                df1=request1(dict_df))

        # --- Request 2
        if req2:
            print('Request2: Inserting values into Cassandra, ' +\
                'please wait...')

        # --- Request 3
        if req3:
            print('Request3: Inserting values into Cassandra, ' +\
                'please wait...')
        
        # --- Request 4
        if req4:
            print('Request4: Inserting values into Cassandra, ' +\
                'please wait...')
        
        print('\nTime: {:.2f} s'.format(t.time()-t_process_init))
        print('-'*22, ' PROCESS & INSERTION SUCCEED ', '-'*22, '\n')

    # --- Display execution
    exec_time = t.time()-t_init
    h = exec_time // 3600
    min = (exec_time - 3600*h) // 60
    sec = (exec_time - 3600*h) % 60

    if exec_time >= 3600:
        print('Total Execution Time: ' +\
            '{} h {} min {:.0f} s'.format(h, min, sec))
    elif exec_time < 60:
        print('Total Execution Time: {:.2f} s'.format(sec))
    else:
        print('Total Execution Time: {} min {:.0f} s'.format(min, sec))

    print('*'*27, ' EXECUTION SUCCEED ', '*'*27, '\n')