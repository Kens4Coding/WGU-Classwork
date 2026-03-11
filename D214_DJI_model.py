# D214 Dow Model
# Kenneth J. Fletcher

# flags to control program execution
DEBUG_FLAG = True

# Import libraries
import sys
import datetime as dt
import holidays
import math
import pandas as pd
import matplotlib.pyplot as plt
import warnings
if not DEBUG_FLAG:
    warnings.filterwarnings('ignore')
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from statsmodels.regression.linear_model import OLS
from statsmodels.tools.tools import add_constant

RANDOM_STATE = 42
TEST_SPLIT = 0.2

# make friendlier names that those from downloads
FRIENDLY_COLUMN_NAMES = ['DOW Close','Electricity Price',\
    'Electricity Sales','Gasoline Price','Gasoline Consumption','Row Number']

MODEL_TARGET_VAR = 'DOW Close'

MODEL_INPUT_VARS = ['Electricity Price','Electricity Sales',\
                    'Gasoline Price', 'Gasoline Consumption']

MODEL_INPUT_VARS_PRICE_ONLY = ['Electricity Price','Gasoline Price']


# output txt to the log file and return txt value so that
# all calls to this function can be used to log the values part of
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

# *****************************************************************************
# Plotting Predictions
# *****************************************************************************
def plot_predictions(test_y,predicted,file_name,title,suptitle,plot_model_vars):
    plt.close()
    fig,ax = plt.subplots()
    fig.set_size_inches(8,8)
    fig.suptitle(suptitle)
    ax.set_title(title)
    ax.set_xlabel('Daily Close from 1/2/2001 to 1/31/2025')
    ax.set_ylabel('Dow Jones Industrial Average')

    # put the data in a dataframe to sort it    
    data = { 'test' : test_y, 'predicted' : predicted }
    df = pd.DataFrame(data=data,index=test_y.index) 
    df.sort_index(inplace=True) 
    ax.plot(df.index,df['test'],color='blue',label='DJIA Closing Value')
    ax.plot(df.index,df['predicted'],color='green',label='Predicted DJIA Close')

    # option to add scaled input vars to plot
    if plot_model_vars is not None:

        if 'Electricity Price' in plot_model_vars:
            ax.plot(df_electricity.index,df_electricity['price'] * 2000 , \
                    color='red',label='Electricity Price')
            
        if 'Electricity Sales' in plot_model_vars:
            ax.plot(df_electricity.index,df_electricity['sales'] / 3.0 , \
                    color='c', label='Electricity Sales')
            
        if 'Gasoline Price' in plot_model_vars:
            ax.plot(df_petrol_price.index,df_petrol_price['value'] * 2000 ,\
                    color='m',label='Gasoline Price')
        
        if 'Gasoline Consumption' in plot_model_vars:
            ax.plot(df_petrol_consumption.index,df_petrol_consumption['value'],\
                    color='y',label='Gasoline Consumption')
   

    plt.legend()
    fig.tight_layout()
    plt.show()
    plt.savefig(file_name)
    plt.close()


# read csv files where indexes are DateTimeIndex
# basic dataframe checks info(),describe(),head()
def read_csv_dataframe(file_name,usecols,index_col,date_format):
    o('\n_____________________________________________________')
    o('Reading: ' + file_name)
    o('_____________________________________________________')
    df = pd.read_csv(file_name,index_col=index_col,usecols=usecols,\
        date_format=date_format)
    df.index = pd.to_datetime(df.index)
    # energy data downloaded from eia.gov is not sorted
    df.sort_index(inplace=True)
    o(df.isna().sum().sum(),lbl='Nan Count = ')
    o('\ndataframe.Info():')
    o(df.info(buf=log_file))
    o('\ndataframe.describe()')
    o(df.describe())
    o('\ndataframe.head()')
    o(df.head())
    return df

def interpolate_as_daily(df):
    # convert to daily frequency
    df = df.asfreq('D')
    return df.interpolate()

def within_date_range(df,min_date,max_date):
    # copy the masked dataframe, don't want a "View" of the dataframe
    return df[(df.index >= min_date) & (df.index <= max_date)].copy()

def prep_train_test(df,input_vars):

    train_X,test_X,train_y,test_y = \
        train_test_split(df[input_vars],df[MODEL_TARGET_VAR],\
        test_size=TEST_SPLIT,random_state=RANDOM_STATE)
    
    scaler = StandardScaler()
    train_X = scaler.fit_transform(train_X)
    test_X = scaler.transform(test_X)
    
    train_X = add_constant(train_X)
    test_X = add_constant(test_X)
    return train_X,test_X,train_y,test_y

def create_model(train_X,train_y,input_vars):
    model = OLS(train_y,train_X,hasconst=True)
    
    # This puts the field names in the model so they show up in the Summary
    names = input_vars.copy()
    names.insert(0,'Constant')
    model.exog_names[:] = names

    model_fit = model.fit()
    return(model_fit)     

def evaluate_model(title,model,test_X,test_y,file_name,model_vars):
    rmse = []
    sum_residuals = []
    predicted = []
    predicted = model.predict(test_X)
    mse = mean_squared_error(test_y,predicted)
    rmse = round(math.sqrt(mse),2)
    residuals = test_y - predicted
    sum_residuals = round(sum(residuals),2)
    
    # dow data from 1-5-2001 to 1-31-2025 Roughly 24 years (and a month)
    residuals_per_year = round(sum_residuals / 24.0,2)
    o('\n\n_______________________________________________________')
    o('Model Summary: ' + title)
    o('_______________________________________________________')
    o(model.summary())
    o('\nModel Statistics:')
    o(rmse,lbl='Root Mean Squared Error:')
    o(sum_residuals,lbl='Sum of Residuals = ')
    o(residuals_per_year,lbl='Residuals per Year = ')

    accuracy = '\nAccuracy: R-Squared ' + str(round(model.rsquared,3)) + \
        ' RMSE ' + str(rmse)

    plot_predictions(test_y,predicted,file_name,\
        title,'Actual and Predicted DJIA Closing Value' + accuracy,None)  
    
    # Second Plot includes Energy Data Inputs
    file_name = file_name.replace('.png','_with_inputs.png')
    
    plot_predictions(test_y,predicted,file_name,title,\
    'Actual and Predicted DJIA Close with Energy Data Inputs (scaled)' + \
        accuracy, model_vars)  
    
    return (rmse,sum_residuals)


###############################################################################
################ MAIN SCRIPT START ############################################
###############################################################################

# log file name is "executing scripts name" + _log.txt
LOG_FILE_NAME = sys.argv[0].replace('.py','') + '_log.txt'
log_file = open(LOG_FILE_NAME , 'w' )

print('Logging output to: ' + LOG_FILE_NAME)

start_script_time = dt.datetime.now()
o("script start time = " + str(start_script_time),echo_print=True)

# this turns interactive plotting on and prevents plots from freezing the
# script when running in vscode
plt.ion() 

###############################################################################
# Load and examine the raw datasets
###############################################################################

# load raw data
# these CSV files are created by D214_data_downloader.py
# check for nans and get basic info
o('Load CSV Data:')

# the DOW dataframe will be built up to hold final dataset
# it sets the master granularity (daily) but does not have data
# for weekends and holidays
df_dow = read_csv_dataframe('D214_downloadraw_DJI_close.csv',['Date','Close'],'Date',\
    '%m/%d/%y') 

# electicity data period is monthly
# price units: cents per kilowatt-hour 
# sales units: million kilowatt hours
df_electricity = read_csv_dataframe('D214_downloadraw_electricity.csv',\
    ['period','price','sales'],'period','%y-%m') 

# both petroleum data sets have a weekly period 
# value units (price): $/GAL
df_petrol_price = read_csv_dataframe('D214_downloadraw_petroleum_price.csv',\
    ['period','value'],'period','%m/%d/%y')

# value units (U.S. product supplied) : Thousand Barrels per Day
df_petrol_consumption = \
    read_csv_dataframe('D214_downloadraw_petroleum_consumption.csv',\
    ['period','value'],'period','%m/%d/%y')

# Are all DOW values present?
dow_start = df_dow.index.min()
dow_end = df_dow.index.max()
all_days = pd.date_range(dow_start,dow_end)
weekdays = [day for day in all_days if day.weekday() < 5]
# DOW and NYSE use same holdidays
nyse_holidays = holidays.NYSE()
market_day = [day for day in weekdays if day not in nyse_holidays]
market_set = set(market_day)
dow_set = set(df_dow.index)
remainder = market_set - dow_set
if len(remainder) > 0:
    # apparently the holidays package does not include Jan 9 2025 where the
    # stock markets were closed for the national day of mourning for 
    # former president Jimmy Carter
    assert len(remainder) == 1 and remainder.pop() == dt.datetime(2025,1,9)


###############################################################################
# Interpolate Daily Valules for Energy Datasets
###############################################################################
df_electricity = interpolate_as_daily(df_electricity)
df_petrol_price = interpolate_as_daily(df_petrol_price)
df_petrol_consumption = interpolate_as_daily(df_petrol_consumption)

###############################################################################
# Make sure the date ranges of energy data cover the dow data or the assign
# below will blow up
###############################################################################
all_dfs = [df_dow,df_electricity,df_petrol_consumption,df_petrol_price]
largest_min = pd.Timestamp(dt.date.min)
smallest_max = pd.Timestamp(dt.date.max)
for df in all_dfs:
    if df.index.min() > largest_min:
        largest_min = df.index.min()
    if df.index.max() < smallest_max:
        smallest_max = df.index.max()
df_dow = within_date_range(df_dow,largest_min,smallest_max)
df_electricity = \
    within_date_range(df_electricity,largest_min,smallest_max)

df_petrol_price = \
    within_date_range(df_petrol_price,largest_min,smallest_max)
df_petrol_consumption = \
    within_date_range(df_petrol_consumption,largest_min,smallest_max)

o('\n_____________________________________________________')
o('Date Ranges of the Datasets:')
o('_____________________________________________________')
o(min(df_dow.index),lbl='Dow Data Start Date:')
o(max(df_dow.index),lbl='Dow Data End Date:')
o(min(df_electricity.index),lbl='Electricity Data Start Date:')
o(max(df_electricity.index),lbl='Electricity Data End Date:')
o(min(df_petrol_price.index),lbl='Petroleum Price Data Start Date:')
o(max(df_petrol_price.index),lbl='Petroleum Price Data End Date:')
o(min(df_petrol_consumption.index),lbl='Petroleum Consumption Data Start Date:')
o(max(df_petrol_consumption.index),lbl='Petroleum Consumption Data End Date:')


###############################################################################
# Pull energy data into df_dow by date
###############################################################################
df_dow = df_dow.assign( \
    electricity_price = lambda x: df_electricity.loc[x.index,'price'],
    electricity_sales = lambda x: df_electricity.loc[x.index,'sales'],
    petroleum_price = lambda x: df_petrol_price.loc[x.index,'value'],
    petroleum_consumption = lambda x: df_petrol_consumption.loc[x.index,'value']
)

assert df_dow.isna().sum().sum() == 0,"Problem, missing data in Dow frame"

# use row_number for plotting data
df_dow['Row Number'] = [x for x in range(len(df_dow))]

# change the column names to something user friendly
df_dow.columns = FRIENDLY_COLUMN_NAMES

# Save as CSV file
df_dow.to_csv('D214_final_dataset.csv')

pct_of_data = round(100 * TEST_SPLIT)
note = '\nnote: The model is evaluated with the test dataset, ' + \
    str(pct_of_data) + '% of the total data.'

# Create and Evaluate the Full Model using all Exogenous Variables
train_X,test_X,train_y,test_y = prep_train_test(df_dow,MODEL_INPUT_VARS)
model = create_model(train_X,train_y,MODEL_INPUT_VARS)
evaluate_model('Evaluate the Full Model'+note,model,test_X,test_y,\
    'Model Pedictions with All Variables.png',MODEL_INPUT_VARS)

# Create and Evaluate the model with one variable missing
for i in range(len(MODEL_INPUT_VARS)):
    subset = MODEL_INPUT_VARS.copy()
    remove_var = subset[i]
    del subset[i]
    train_X,test_X,train_y,test_y = prep_train_test(df_dow,subset)
    model = create_model(train_X,train_y,subset)
    evaluate_model('Evaluate the Model Without Variable '+ remove_var + note,\
        model,test_X,test_y,'Model Pedictions Without ' + remove_var + '.png',\
        subset)

# Create and Evaluate model with only price data
train_X,test_X,train_y,test_y = prep_train_test(df_dow,\
                                                MODEL_INPUT_VARS_PRICE_ONLY)
model = create_model(train_X,train_y,MODEL_INPUT_VARS_PRICE_ONLY)
evaluate_model('Evaluate the Model with Only Price Data'+note,model,test_X,\
    test_y,'Model Pedictions With Only Price Data.png',\
        MODEL_INPUT_VARS_PRICE_ONLY)

# DONE 
o("\nAssessment complete.",echo_print=True)
end_script_time = dt.datetime.now()
o(end_script_time,echo_print=True)
o("script run time = " + str(end_script_time - start_script_time),\
    echo_print=True)

log_file.close()
