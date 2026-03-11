# Note: posting to github to demonstrate REST API from eia.gov
# the pandas_datareader does not seem to be working with
# Python 3.14 so code to download DOW Industrial Average has
# been disabled for now


'''
D214 Data Downloader
Kenneth J. Fletcher

This program downloads model data from two sources
stooq.com - to retrive DJIA Close data
and
eia.gov - to download energy price and usage data (cost data)

Note: This is raw and unsorted (do that later). Requesting eia.gov to 
sort the data slows things down SIGNIFICANTLY
'''
# *********************************
# Flags to control script execution
# *********************************
DEBUG_FLAG = False

# you can get locked out if you make too may requests too quickly
# or more than 1000 in an hour
EIA_THROTTLE = 0.5 # pause between HTTP GET requests in seconds
# eia.gov only supports downloading up to 5000 records at time
EIA_MAX_RECORDS = 5000

# Dow Jones Data
DOWNLOAD_DJI_DATA = False
DJI_START_DATE = '2001-01-01'
DJI_END_DATE = '2025-04-25'

# download datasets (eia records)
DOWNLOAD_DATASETS = True

# EIA.GOV REST API Datasets
# dictionary key is filename, value is URL for REST API
DOWNLOAD_TARGETS = {
'D214_downloadraw_petroleum_price.csv' : 
'''https://api.eia.gov/v2/petroleum/pri/gnd/data/?data[]=value''' +  
'''&frequency=weekly&facets[series][]=EMM_EPM0U_PTE_NUS_DPG&start=2001-01-01'''
,
'D214_downloadraw_petroleum_consumption.csv' : 
'''https://api.eia.gov/v2/petroleum/cons/wpsup/data/?data[]=value''' + 
'''&frequency=weekly&facets[series][]=WGFUPUS2&start=2000-12-29'''
,
'D214_downloadraw_electricity.csv' : 
'''https://api.eia.gov/v2/electricity/retail-sales/data/?data[]=price''' + 
'''&data[]=sales&frequency=monthly&facets[sectorid][]=RES''' + 
'''&facets[stateid][]=US&start=2001-01-01'''
}

# you can get locked out if you make too may requests too quickly
EIA_THROTTLE = 0.5 # pause between HTTP GET requests in seconds

# Import libraries
import os
import sys
import warnings
if not DEBUG_FLAG:
    warnings.filterwarnings('ignore')


# Ken!! this is failing to load with Python 3.14
# import pandas_datareader.data as web 

import urllib3
import time
import json

# output txt to the log file and return txt value so that
# all calls to this function can be used to log the values as part of
# an assignment statement:
# Example: somvar = o( dataframe[col].mean() , lbl='calc mean of col' )
def o( txt , new_line = True  , echo_print=False , lbl=None, \
      new_line_after_lbl = False ):
    global log_file
    if lbl:
        if new_line_after_lbl:
            out = str(lbl) + '\n' + str(txt)
        else:
            out = str(lbl) + ' ' + str(txt)
    else:
        out = str(txt)

    log_file.write( out )
    if (new_line): log_file.write( "\n" )
    if echo_print:
        print( out )
    log_file.flush()
    return(txt)

# log file name is "executing scripts name" + _log.txt
LOG_FILE_NAME = sys.argv[0].replace('.py','') + '_log.txt'
log_file = open(LOG_FILE_NAME , 'w' )

print('Logging output to: ' + LOG_FILE_NAME)

# My API Key to access eia.gov is in a windows enviroment variable for security
API_KEY = 'api_key=' + os.environ.get('EIA_API_KEY', \
    default='No Environmnet Variable Set. This will blow up!')

if API_KEY == 'api_key=No Environmnet Variable Set. This will blow up!':
    print('*********************************')
    print('*********** WARNING *************')
    print('*********************************')
    print('This script uses an API key that must exist in an environment')
    print('variable named EIA_API_KEY ')
    print('A key can freely be obtained from eia.gov')
    sys.exit(0)

 # *********************************
 # Dow Jones Industrial Average Data
 # *********************************
if DOWNLOAD_DJI_DATA:
    data = web.DataReader(name='^DJI',data_source='stooq',start=DJI_START_DATE,\
        end=DJI_END_DATE)
    data.to_csv('D214_downloadraw_DJI_close.csv')

def http_get(url):
    ret = None
    retry = 2 # retry once
    while retry > 0:
        try:
            ret = http.request('GET',url)
            if EIA_THROTTLE > 0.0:
                time.sleep(EIA_THROTTLE)
        except Exception as e:
            o(e,lbl='HTTP GET Exception:',echo_print=True)
            o(url,lbl='URL:',echo_print=True)
        else:
            break
        finally:
            retry -= 1
    return ret

def response_to_dict(response):
    ret = None
    try:
        ret = json.loads(response.data)
    except Exception as e:
        o(e,lbl='Exception:',echo_print=True)
        o(response.data,lbl='Response Data:',echo_print=True)
    return ret

# *******************************
# EIA.GOV Energy Dataset Download
# *******************************
# setup http communication
http = urllib3.PoolManager()

def append_api_key(url):
    if not '?' in url:
        url += '?'
    else:
        url += '&'
    url += API_KEY
    return url

def parse_row(row,isheader):
    data = ''
    num_cols = len(row.keys())
    cur_col = 1
    for key,val in row.items():
        if isheader:
            data += key
        else:
            if val is not None:
                data += val
        if cur_col < num_cols:
            data += ','
        cur_col += 1
    data += '\n'
    return data

def read_chunk(url,current_record,max_record):
    lines = []
    if current_record > 0:
        url += '&offset=' + str(current_record)
    if max_record != -1:
        remaining = max_record - current_record
        chunk_size = remaining if remaining < EIA_MAX_RECORDS else EIA_MAX_RECORDS
        url += '&length=' + str(chunk_size)
    
    response = http_get(url)
    
    if response == None:
        raise ValueError('no response in read_chunk url:' + url)
    response_dict = response_to_dict(response)
    response_data = response_dict['response']['data']
    if max_record == -1:
        max_record = int(response_dict['response']['total'])
        o(max_record,lbl='record count:')
    if len(response_data) == 0:
        raise ValueError('no data read_chunk url: ' + url)
    if current_record == 0: # write column headers
        lines.append(parse_row(response_data[0],True) )
    for row in response_data:
        lines.append(parse_row(row,False))
    return lines , max_record

# download eia.gov data
def download_dataset(url,file_name):
    o(url,lbl='download_dataset for url:')
    o(file_name,lbl='to file:')
        
    current_record = 0
    max_records = -1 # unknown until first read
    
    file_mode = 'w'        
    # this has the disadvantage of opening the file multiple times
    # but it has the advantagoe of never having the entire dataset in memory!
    while current_record < max_records or max_records == -1:
        lines,max_records = read_chunk(url,current_record,max_records)

        # first chunk includes header that is not a record
        current_record += len(lines) - 1 if current_record == 0 else len(lines)

        with open(file_name,file_mode,encoding='utf-8') as file:
            for line in lines:
                file.write(line)  
        file_mode = 'a' # append after first write

if DOWNLOAD_DATASETS:
    for file_name,url in DOWNLOAD_TARGETS.items():
        o('Downloading ' + url + ' data:',echo_print=True)
        download_dataset(append_api_key(url),file_name)
        
