# -*- coding: utf-8 -*-
"""
Created on Sun Jan  3 18:46:49 2021

Collects and aggregates FDIC Information

"""
from os import path
import requests, zipfile, io
import pandas as pd
import sqlite3 as sql
import datetime as dt

###############################################################################

# Functions

###############################################################################
# Getting in the necessary information 
def get_annual_info(date_marker):
    
    # Getting the necessary URL
    url = 'https://www7.fdic.gov/sod/ShowFileWithStats1.asp?strFileName=ALL_{0}.zip'.format(date_marker)

    # Making the connection and getting the zip object 
    request_response = requests.get(url)
    zip_file = zipfile.ZipFile(io.BytesIO(request_response.content))
    
    # Making a dictionary of the dataframe contents 
    dfs = {text_file.filename: pd.read_csv(zip_file.open(text_file.filename),encoding = "ISO-8859-1")
       for text_file in zip_file.infolist()
       if text_file.filename.endswith('.csv')}
    
    # Returning the dictionary of dataframes 
    return(dfs)
    
# String number converter 
def string_to_numeric(x):
    
    return(pd.to_numeric(x.str.replace(',','')))

# Cleaning the data for load 
def annual_info_cleaner(info_dict):
    
    # Converting this information into a dataframe
    frame = pd.concat(info_dict)
    
    # Converting DEPSUM, DEPDOM, ASSET, DEPSUMBR, SIMS_ESTABLISHED_DATE
    frame.loc[:,['DEPSUM','DEPDOM','ASSET','DEPSUMBR']] = \
        frame.loc[:,['DEPSUM','DEPDOM','ASSET','DEPSUMBR']].apply(string_to_numeric)
        
    # Converting SIMS_ESTABLISHED_DATE to a datetime 
    frame['SIMS_ESTABLISHED_DATE'] =  pd.to_datetime(frame['SIMS_ESTABLISHED_DATE'], format='%m/%d/%Y')
    
    # Returning the necessary information 
    return(frame)
    
# Getting the starting point
def get_start_year():
    
    # Checking if the database has been made yet
    if path.exists('bank_info.db'):
        
        # Querying the database to find the most recently loaded file 
        conn = sql.connect('bank_info.db')
        c    = conn.cursor()
        
        # Finding the most recent year loaded
        c.execute('SELECT MAX(YEAR) + 1 FROM annual_info')
        
        # Getting the output 
        rows = c.fetchall()
        
        # Returning the necessary information 
        return(rows[0][0])
    else:
        
        # Returning the default starting value 
        return(2015)
         
# Getting the year markers 
def get_year_markers():
    
    # Doing it...
    start = get_start_year()    
    end   = dt.datetime.today().year 
    year_markers = [i for i in range(start, end)]
    
    # Returning the necessary output
    return(year_markers)
    
# Loading the annual information 
def load_annual_info():
    
    # Getting the year range for the current data load
    data_range = get_year_markers()
            
    # Making the connection to the database or create if it does not exist
    conn = sql.connect('bank_info.db')

    # Iteratively add information to database 
    for i in data_range:
        
        # Getting the required information 
        req_info = get_annual_info(i)
        
        # Cleaning this information
        clean_info = annual_info_cleaner(req_info)
        
        # Loading this information into the database 
        clean_info.to_sql('annual_info', conn, if_exists='append')
    
    
###############################################################################

# Running the processes
        
###############################################################################   
load_annual_info()
