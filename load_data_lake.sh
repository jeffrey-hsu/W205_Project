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
DATA="https://www.dropbox.com/s/knkddwfhbwqzndu/FinalProject_data_csv.zip?dl=1"
wget "$DATA" -O FinalProject_data_csv
unzip FinalProject_data_csv

DATA2="https://www.dropbox.com/s/rspzgqtiy5syxb5/crsp_stock_csv.zip?dl=1"
wget "$DATA2" -O crsp_stock_csv.zip
unzip crsp_stock_csv.zip

hdfs dfs -mkdir /user/w205/financial_data

#rename files, create new sub-directories, and move files to sub-directories
OLD_FS="FinalProject_data_csv"
NEW_FS="financial_suite.csv"
tail -n +2 "$OLD_FS" >$NEW_FS
hdfs dfs -mkdir /user/w205/financial_data/financial_suite
hdfs dfs -put $NEW_FS /user/w205/financial_data/financial_suite


OLD_FS="crsp_stock_csv"
NEW_FS="crsp_mth_stock.csv"
tail -n +2 "$OLD_FS" >$NEW_FS
hdfs dfs -mkdir /user/w205/financial_data/crsp_mth_stock
hdfs dfs -put crsp_monthly_stock.csv /user/w205/financial_data/crsp_mth_stock

# Change directory back to initial directory
cd $CWD

# Exit
exit
