# save my current working directory
MY_DIR=$(pwd)

# create staging directory
rm ~/staging/final/*
rmdir ~/staging/final
rmdir ~/staging/

# remove files from HDFS
hdfs dfs -rm /user/w205/financial_data/crsp_mth_stock/*
hdfs dfs -rm /user/w205/financial_data/financial_suite/*

# remove HDFS directories
hdfs dfs -rmdir /user/w205/financial_data/crsp_mth_stock
hdfs dfs -rmdir /user/w205/financial_data/financial_suite
hdfs dfs -rmdir /user/w205/financial_data

# Change directory
cd $MY_DIR

exit

