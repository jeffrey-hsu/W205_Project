from pyspark import SparkContext
import pandas as pd
import numpy as np
import datetime
import numpy as np
import sklearn
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import pydoop.hdfs as hd
# from statsmodels import robust
# import matplotlib.pyplot as plt
# import matplotlib.cm
# import seaborn as sns
# from IPython.display import display

sc = SparkContext.getOrCreate()

# LOAD DATASETS - MAKE SURE SCHEMAS ARE LOADED ALREADY
    # TO LOAD SCHEMAS, COPY-AND-PASTE FROM SchemaWriter.py
fin_suite = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load("hdfs:///user/w205/financial_data/financial_suite/financial_ratios.csv", schema=schema_fin_suite)
CRSP_comp_merge = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load("hdfs:///user/w205/financial_data/crsp_compustat/crsp_compustat_sec_mth.csv", schema=schema_CRSP_comp)
link_table = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load("hdfs:///user/w205/financial_data/linking_table/linking_table.csv", schema=schema_link_table)
beta_suite = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load("hdfs:///user/w205/financial_data/beta_suite/beta_suite.csv", schema=schema_beta_suite)
recommendations = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load("hdfs:///user/w205/financial_data/recommendations/recommendations.csv", schema=schema_recs)


#MERGING DATA
df = fin_suite.join(link_table, fin_suite.permno == link_table.LPERMNO)


#query data to produce new dataframes
CRSP_comp_merge.createOrReplaceTempView("tempview")
results = spark.sql("SELECT loc FROM tempview limit 50")

# ADD NEW COLUMN (NEED TO CREATE NEW DATAFRAME)
fin_suite_new = fin_suite.withColumn("forward_one_month_prccm", fin_suite.prccm+1)


# taking mean of GVKEY is only an example, obviously we wouldn't do that
# sqlCtx.table("temptable").groupby("LPERMNO").agg("LPERMNO", mean("GVKEY")).collect()


''' SHANES FUNCTIONS BELOW '''
# Percentage of nulls per column
def null_ratio(df):
        null_count = df.isnull().sum()
        null_percent = 100 * df.isnull().sum()/len(df)
        null_table = pd.concat([null_count, null_percent], axis=1)
        null_table = null_table.rename(columns = {0 : 'Null Count', 1 : 'Null Percent'})
        return null_table.sort_values('Null Percent', ascending=0)

def return_all_rows(x):
    pd.set_option('display.max_rows', len(x))
    return x
    pd.reset_option('display.max_rows')

def return_all_columns(x):
    pd.set_option('display.max_columns', len(x))
    return x.head(5)
    pd.reset_option('display.max_columns')

def overview(df):
    print("Number of columns:", len(df.columns))
    print("Number of rows:", len(df.index))
    df.head(5)

def drop_dups(df):
    # list comprehension of the cols that end with '_y'
    y_drop = [x for x in df if x.endswith('_y')]
    df.drop(y_drop, axis=1, inplace=True)

def floatToString(inputValue):
    result = ('%.15f' % inputValue).rstrip('0').rstrip('.')
    return '0' if result == '-0' else result

def mad(arr):
    """
    Get Median Absolute Deviation and multiple by 1.486 to mimic standard deviation
        https://www.ibm.com/support/knowledgecenter/SSWLVY_1.0.0/com.ibm.spss.analyticcatalyst.help/analytic_catalyst/modified_z.html
    Median Absolute Deviation: a "Robust" version of standard deviation.
        Indices variabililty of the sample.
        https://en.wikipedia.org/wiki/Median_absolute_deviation
    """
    arr = np.ma.array(arr).compressed() # should be faster to not use masked arrays.
    med = np.nanmedian(arr)
    mad = np.nanmedian(np.abs(arr - med))
    # Multiply coefficient by 1.486 to mimic Standard Deviation (source: IBM)
    return 1.486 * mad


def meanad(arr):
    """
    Get Mean Absolute Deviation and multiple by 1.253314 to mimic standard deviation
        https://www.ibm.com/support/knowledgecenter/SSWLVY_1.0.0/com.ibm.spss.analyticcatalyst.help/analytic_catalyst/modified_z.html
    Mean Absolute Deviation: a "Robust" version of standard deviation.
        Indices variabililty of the sample.
        https://en.wikipedia.org/wiki/Mean_absolute_deviation
    """
    arr = np.ma.array(arr).compressed() # should be faster to not use masked arrays.
    med = np.nanmedian(arr)
    mad = np.nanmean(np.abs(arr - med))
    # Multiply coefficient by 1.253314 to mimic Standard Deviation (source: IBM)
    return 1.253314 * mad

def modified_z(array):
    try:
        try:
            try:
                median = np.nanmedian(array)
                denominator = mad(array) * 1.486
                array = (array - median) / denominator
                return array
            except:
                median = np.nanmedian(array)
                denominator = meanad(array) * 1.253314
                array = (array - median) / denominator
                return array
        except:
            mean = np.nanmean(array)
            denominator = np.nanstd(array)
            array = (array - mean) / denominator
            return array
    except:
        array = array.fillna(0)


def fill_null(column):
    try:
        median = np.nanmedian(column)
        column = column.fillna(median)
        return column
    except:
        return column

def impute_null(column):
    try:
        imp = Imputer(missing_values='NaN', strategy='median', axis=0)
        imp.fit(column)
        column = imp.transform(column)
        return column
    except:
        return column

def clip_outliers(column):
    # Use try in case all null column
    try:
        floor = column.quantile(0.02)
        ceiling = column.quantile(0.98)
        column = column.clip(floor, ceiling)
        return column
    # If error, return as is
    except:
        return column
