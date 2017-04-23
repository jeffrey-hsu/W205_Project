'''
    USE "pyspark --num-executors 5 --driver-memory 2g --executor-memory 2g" TO LAUNCH pyspark
    THIS WILL GIVE YOU MORE MEMORY USAGE

    1. LOAD PARQUET FILES FROM HDFS
    2. TRANSFORMATION OF DATA FRAME
    3. CREATE TEMPVIEW FOR APPLYING QUERYING TO CREATE NEW DATA FRAMES
    4. DEFINE THE FUNCTIONS FOR EXPLORATORY DATA ANALYSIS
'''

from pyspark import SparkContext
import numpy as np
import datetime
import numpy as np
import sklearn
import scipy
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
# import pandas as pd
# import pydoop.hdfs as hd
# from statsmodels import robust
# import matplotlib.pyplot as plt
# import matplotlib.cm
# import seaborn as sns
# from IPython.display import display

sc = SparkContext.getOrCreate()

## NO NEED TO IMPORT SCHEMAS FIRST ANYMORE
## READ DATASETS FROM PARQUET FORMAT
## THIS GREATLY IMPROVES SPEED AND REDUCES MEMORY USAGE
fin_suite = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/fin_suite")
crsp_comp = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/crsp")
link_table = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/link_table")
beta_suite = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/beta")
recommendations = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/recs")


## TRANSFORM COLUMNS WITH DATE VALUE TO DATE TYPE
fix_LINKENDDT = udf(lambda x: '20200101' if x == 'E' else x)
getYear = udf(lambda x: x[0:4] if x is not None else None)
getMonth = udf(lambda x: x[4:6] if x is not None else None)
getYear2= udf(lambda x: x[6:10] if x is not None else None)
getMonth2 = udf(lambda x: x[0:2] if x is not None else None)
ticker_strip = udf(lambda x: x[1:] if x[0] == '@' else None)


def add_lead_lag(df, variable):
    for month in range(1,37):
        w = Window().partitionBy(col("GVKEY")).orderBy(col("GVKEY_year_mth"))
        first_new_col = "forward_"+str(month)+"_month_"+str(variable)
        second_new_col = "past_"+str(month)+"_month_"+str(variable)
        df = df.withColumn(first_new_col, lag(col(variable),-month,None).over(w)) \
        .withColumn(second_new_col, lag(col(variable),month,None).over(w))
    return df

def add_returns(df):
    for month in range(1,37):
        first_new_col = "forward_"+str(month)+"_month_return"
        second_new_col = "past_"+str(month)+"_month_return"
        forward_prccm = "forward_"+str(month)+"_month_prccm"
        past_prccm = "past_"+str(month)+"_month_prccm"
        forward_ajexm = "forward_"+str(month)+"_month_ajexm"
        past_ajexm = "past_"+str(month)+"_month_ajexm"
        forward_trfm= "forward_"+str(month)+"_month_trfm"
        past_trfm =  "past_"+str(month)+"_month_trfm"
        df = df.withColumn(first_new_col, ((((col(forward_prccm)/col(forward_ajexm))*col(forward_trfm) )/((col('prccm')/col('ajexm'))*col('trfm'))) - 1) * 100 ).withColumn(second_new_col, ((((col('prccm')/col('ajexm'))*col('trfm') )/((col(past_prccm)/col(past_ajexm))*col(past_trfm))) - 1) * 100 )
    return df


## MERGING DATA FRAMES
link_table = link_table.withColumn("LINKENDDT2", fix_LINKENDDT(col("LINKENDDT"))).drop("LINKENDDT")
df = fin_suite.join(link_table, fin_suite.gvkey == link_table.GVKEY, 'leftouter').drop(link_table.GVKEY).dropDuplicates()

## ADDING NEW COLUMNS
# Template: new_df = old_df.withColumn("NewColName", calculation_for_new_col)

df = df.withColumn('GVKEY_year_mth', concat(col('gvkey'), lit('-'), getYear(col('public_date')), lit('-'), getMonth(col('public_date')))) \
    .withColumn('CUSIP_year_mth', concat(col('cusip'), lit('-'), getYear(col('public_date')), lit('-'), getMonth(col('public_date')))) \
    .withColumn('TIC_year_mth', concat(col('tic'), lit('-'), getYear(col('public_date')), lit('-'), getMonth(col('public_date')))) \
    .withColumn('PERMNO_year_mth', concat(col('LPERMNO'), lit('-'), getYear(col('public_date')), lit('-'), getMonth(col('public_date'))))

crsp_comp = crsp_comp.withColumn('GVKEY_year_mth',concat(col('gvkey'), lit('-'), getYear(col('datadate')), lit('-'), getMonth(col('datadate'))))


recommendations = recommendations.withColumn("TICKER2", ticker_strip(col("TICKER"))).drop("TICKER") \
.withColumn('TIC_year_mth',concat(col('TICKER2'), lit('-'), getYear2(col('STATPERS')), lit('-'), getMonth2(col('STATPERS')))) \
    .withColumn("recup", col("NUMUP") / col("NUMREC")) \
    .withColumn("recdown", col("NUMDOWN") / col("NUMREC")) \



beta_suite = beta_suite.withColumn('PERMNO_year_mth',concat(col('PERMNO'), lit('-'), getYear(col('DATE')), lit('-'), getMonth(col('DATE'))))


## FILTER DATAFRAME - FILTERS ARE COSTLY STORAGE-WISE
df = df.filter((df.public_date >= df.LINKDT) & (df.public_date <= df.LINKENDDT2)).dropDuplicates()

crsp_comp = add_lead_lag(crsp_comp, "prccm")
crsp_comp = add_lead_lag(crsp_comp, "ajexm")
crsp_comp = add_lead_lag(crsp_comp, "trfm")
crsp_comp = add_returns(crsp_comp)

# JOIN CRSP COMPUSTAT
df = df.join(crsp_comp, df.GVKEY_year_mth == crsp_comp.GVKEY_year_mth, 'leftouter').drop(crsp_comp.GVKEY_year_mth).dropDuplicates()

# JOIN RECOMMENDATIONS
df = df.join(recommendations, df.TIC_year_mth == recommendations.TIC_year_mth, 'leftouter').drop(recommendations.TIC_year_mth).dropDuplicates()

# JOIN BETA SUITE
df = df.join(beta_suite, df.PERMNO_year_mth == beta_suite.PERMNO_year_mth, 'leftouter').drop(beta_suite.PERMNO_year_mth).dropDuplicates()

# Sector - Enrich
# https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard
sector = sqlContext.createDataFrame([(10.0, "Energy"), (15.0, "Materials"), (20.0, "Industrials"), \
(25.0, "Consumer Discretionary"), (30.0, "Consumer Staples"), (35.0, "Health Care"), (40.0, "Financials"), \
(45.0, "Information Technology"), (50.0, "Telecommunication Services"), (55.0, "Utilities"), (60.0, "Real Estate")], \
["GSECTOR", "sector"])

# JOIN sector
df = df.join(sector, df.GSECTOR == sector.GSECTOR, 'leftouter').drop(sector.GSECTOR).dropDuplicates()


######################################################################
### EVERYTHING ABOVE THIS POINT IS PART OF THE EDA TRANSFORMATIONS ###
######################################################################

## QUERY DATA TO PRODUCE NEW DATAFRAMES
crsp_comp.createOrReplaceTempView("tempview")
results = spark.sql("SELECT loc FROM tempview limit 50")


## CONCATENATE COLUMNS TO CREATE NEW UNIQUE KEYS
# df12345 = df.select(concat(col("gvkey"), lit("-"), col("year-month")))

# taking mean of GVKEY is only an example, obviously we wouldn't do that
# sqlCtx.table("temptable").groupby("LPERMNO").agg("LPERMNO", mean("GVKEY")).collect()


''' EDA FUNCTIONS BELOW '''

## PERCENTAGE OF NULLS PER COLUMN
def null_ratio(df):
        null_count = df.isnull().sum()
        null_percent = 100 * df.isnull().sum()/len(df)
        null_table = pd.concat([null_count, null_percent], axis=1)
        null_table = null_table.rename(columns = {0 : 'Null Count', 1 : 'Null Percent'})
        return null_table.sort_values('Null Percent', ascending=0)

null_ratio_udf = udf(null_ratio)

def drop_dups(df):
    # list comprehension of the cols that end with '_y'
    y_drop = [x for x in df if x.endswith('_y')]
    df.drop(y_drop, axis=1, inplace=True)

def floatToString(inputValue):
    result = ('%.15f' % inputValue).rstrip('0').rstrip('.')
    return '0' if result == '-0' else result

floatToString_udf = udf(floatToString)

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
