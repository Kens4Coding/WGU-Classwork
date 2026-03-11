Pushing up some example code from my work at WGU   
This is code for my final "Capstone" project to create an OLS regression model to predict the Dow close using Energy Costs of US Average Residiential Electricity and Gasoline. It turned out that cost (price * quantity) was not really needed, just the price. Data was downloaded from eia.gov (Electric/Gas) and DJIA from stooq.com. Note the Download program takes some setting up and the Dow component is having issues with the latest version of Python so I have supplied the .csv files so you can run the model generator without needed to run the download.

Note the **Download eia.gov data with RESTAPI.py** downloader needs your windows machine to have an environment variable named EIA_API_KEY
to define your API_KEY that is free to get from eia.gov  
Also, with my new computer using Python 3.14 the pandas_datareader package is no longer loading so the Dow Close data download is not working and is currently turned off.   




