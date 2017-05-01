# W205 Data Storage and Retrieval
### Spring 2017 Final Project
### Financial Data Acquisition for Predictive Analysis

There are 4 main folders in this repository:
1. historic_load_clean_enrich
2. historic_model_investigate
3. realtime_load_clean_enrich
4. realtime_model_predict

##################################
### historic_load_clean_enrich ###
##################################

Contains files to read in the historic datasets, merge them, and enrich them by adding new columns.

* spark_load_data
    * dataframe_setup.py: meant for Spark, this script defines schema, reads our historic datasets in with the given schema, adds a couple filters, and outputs the data to parquet files. By having the data in parquet files it makes it easier to load them for later use since it is faster and already has schema attached to it.


* spark_usage
  * spark_loading.py: reads data from the parquets we previously created. Transforms data by merging out 5 historical datasets, adding new columns with forward and past returns, and filtering data.

  * fill_nulls.py: defines functions to forward fill and backfill columns of the data. We got this working on a test dataset, but could not get it to run properly on the real dataset.


* clean_enrich.py and load_clean_enrich/ show how this was done in Python

##################################
### historic_model_investigate ###
##################################

Investigations of the historical data, once all the transformations are done. This is where we performed our fitting of the machine learning model.


##################################
### realtime_load_clean_enrich ###
##################################

* stock_scrape folder: contains scripts to scrape the real-time data as well as outputs from the scraping

* realtime_clean_enrich folder: scripts to enrich (add new variables to) the realtime data

##############################
### realtime_model_predict ###
##############################

* realtimepredictions.ipynb: IPython notebook to walk through how the predictions are being made

* realtimepredictions.py: executable script which runs the same code as the IPython notebook

* realtimepredictions.csv: output from the script above

* load_hive_data.sql: creates hive table from the csv above. We can then access the hive table via Tableau
