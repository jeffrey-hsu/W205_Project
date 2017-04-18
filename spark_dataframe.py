from pyspark import SparkContext
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm
import seaborn as sns
from IPython.display import display
import datetime
import numpy as np
from statsmodels import robust
import sklearn

sc = SparkContext.getOrCreate()

#one method to create dataframe from txt
my_file = sc.textFile("hdfs:///user/w205/financial_data/financial_suite.txt")
lines = my_file.map(lambda x: x.split("|"))
df = spark.createDataFrame(lines)

#second method to create dataframe from txt
df2 = spark.read.load("hdfs:///user/w205/financial_data/financial_suite.txt", format="text") #WORKS

# easiest to create dataframe from CSVs
CRSP_comp_merge = spark.read.load("hdfs:///user/w205/financial_data/crsp_compustat/crsp_compustat_sec_mth.csv", format="csv", header=True)
link_table = spark.read.load("hdfs:///user/w205/financial_data/linking_table.csv", format="csv", header=True)


#dummy data to play around with dataframes
# test1 = sqlContext.createDataFrame([(1,2,3), (11,12,13), (21,)], ["colName1", "colName2", "colName3"])
# test2 = sqlContext.createDataFrame([(1,2,3), (11,12,13), (21,22,23)], ["colName3", "colName4", "colName5"])

#query data and produce new dataframes
CRSP_comp_merge.createOrReplaceTempView("tempview")
results = spark.sql("SELECT loc FROM tempview limit 50")


''' SHANES TRANSFORMATIONS BELOW '''
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
