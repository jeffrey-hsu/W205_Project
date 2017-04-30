This folder contains the data transformation and cleaning using Pyspark. The scripts usage is defined below:

* dataframe_setup.py defines the schemas of the primary historical stock information datasets. It then loads the files from HDFS and outputs them to parquet files so that they are easier to access later and already have schema attached.
* spark_loading.py loads the previously created parquet files into Pyspark data frames and applies transformation to them
* fill_nulls.py defines functions for backfilling and forward filling null values in our dataframes. I ran these functions successfully on a test dataset, but was unable to get it to work with our real dataset. It might have been a memory issue.
* OLD folder stores the try out scripts
