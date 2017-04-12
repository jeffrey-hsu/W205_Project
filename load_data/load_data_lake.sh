#!/bin/bash

# Save current directopry
CWD=$(pwd)

# Create staging directories
mkdir ~/staging
mkdir ~/staging/final

# Switch to exercise directory
cd ~/staging/final

# Grab data from URL
## NOT WORKING CURRENTLY - NEED TO FIX
# DATA="https://www.dropbox.com/s/knkddwfhbwqzndu/FinalProject_data_csv.zip?dl=1"
# wget "$DATA" -O FinalProject_data_csv
# unzip FinalProject_data_csv

# DATA2="https://www.dropbox.com/s/rspzgqtiy5syxb5/crsp_stock_csv.zip?dl=1"
# wget "$DATA2" -O crsp_stock_csv.zip
# unzip crsp_stock_csv.zip

# Grab data from WRDS cloud
## firm financial ratio data
scp -i ~/.ssh/id_rsa jhsu@wrds-cloud.wharton.upenn.edu:/wrdslin/wrdsapps/sasdata/finratio/firm_ratio.sas7bdat ~/staging/final/financial_suite.sas7bdat
## csrp monthly stock
scp -i ~/.ssh/id_rsa jhsu@wrds-cloud.wharton.upenn.edu:/wrdslin/crsp/textdata/a_stock/msf.txt ~/staging/final/crsp_mth_stock.txt
## stocknames
scp -i ~/.ssh/id_rsa jhsu@wrds-cloud.wharton.upenn.edu:/wrdslin/crsp/textdata/a_stock/stocknames.txt ~/staging/final/stocknames.txt

# create directories in HDFS
hdfs dfs -mkdir /user/w205/rawdata
hdfs dfs -mkdir /user/w205/rawdata/financial_suite
hdfs dfs -mkdir /user/w205/rawdata/crsp_mth_stock
hdfs dfs -mkdir /user/w205/rawdata/stocknames

# rename files, create new sub-directories, and move files to sub-directories
# OLD_FS="FinalProject_data_csv"
# NEW_FS="financial_suite.csv"
# tail -n +2 "$OLD_FS" >$NEW_FS
hdfs dfs -put financial_suite.sas7bdat /user/w205/rawdata/financial_suite


# OLD_FS="crsp_stock_csv"
# NEW_FS="crsp_mth_stock.csv"
# tail -n +2 "$OLD_FS" >$NEW_FS
hdfs dfs -put crsp_mth_stock.txt /user/w205/financial_data/crsp_mth_stock

hdfs dfs -put stocknames.txt /user/w205/rawdata/stocknames

# Change directory back to initial directory
cd $CWD

# Exit
exit
