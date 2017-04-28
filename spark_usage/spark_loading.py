'''
    USE "pyspark --num-executors 6 --driver-memory 3g --executor-memory 6g --executor-cores 6 --conf spark.local.dir=/data/temp/" TO LAUNCH pyspark

    1. LOAD PARQUET FILES FROM HDFS
    2. TRANSFORMATION OF DATA FRAME
    3. CREATE TEMPVIEW FOR APPLYING QUERYING TO CREATE NEW DATA FRAMES
    4. DEFINE THE FUNCTIONS FOR EXPLORATORY DATA ANALYSIS
'''

from pyspark import SparkContext
import numpy as np
import datetime
# import sklearn
# import scipy
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
import sys

sc = SparkContext.getOrCreate()

## NO NEED TO IMPORT SCHEMAS FIRST ANYMORE
## READ DATASETS FROM PARQUET FORMAT
## THIS GREATLY IMPROVES SPEED AND REDUCES MEMORY USAGE
fin_suite = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/fin_suite_filter")
crsp_comp = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/crsp_filter")
link_table = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/link_table")
beta_suite = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/beta_filter")
recommendations = spark.read.parquet("hdfs:///user/w205/financial_data/parquet_files/recs")


## TRANSFORM COLUMNS WITH DATE VALUE TO DATE TYPE
fix_LINKENDDT = udf(lambda x: '20200101' if x == 'E' else x)
getYear = udf(lambda x: x[0:4] if x is not None else None)
getMonth = udf(lambda x: x[4:6] if x is not None else None)
getYear2= udf(lambda x: x[6:10] if x is not None else None)
getMonth2 = udf(lambda x: x[0:2] if x is not None else None)
ticker_strip = udf(lambda x: x[1:] if x[0] == '@' else None)


def add_lead_lag(df, variable):
    for month in range(1,37,3):
        w = Window().partitionBy(col("GVKEY")).orderBy(col("GVKEY_year_mth"))
        first_new_col = "forward_"+str(month)+"_month_"+str(variable)
        second_new_col = "past_"+str(month)+"_month_"+str(variable)
        df = df.withColumn(first_new_col, lag(col(variable),-month,None).over(w)) \
        .withColumn(second_new_col, lag(col(variable),month,None).over(w))
    return df

def add_returns(df):
    for month in range(1,37,3):
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

## FILTER DATAFRAME - FILTERS ARE COSTLY STORAGE-WISE
df = df.filter((df.public_date >= df.LINKDT) & (df.public_date <= df.LINKENDDT2))

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


# Add forward and past values for prccm, ajexm, trfm and add returns
crsp_comp = add_lead_lag(crsp_comp, "prccm")
crsp_comp = add_lead_lag(crsp_comp, "ajexm")
crsp_comp = add_lead_lag(crsp_comp, "trfm")
crsp_comp = add_returns(crsp_comp)

# JOIN CRSP COMPUSTAT - DROP ALL DUPLICATE COLUMNS OTHERWISE YOU CAN'T EXPORT TO PARQUET
df = df.join(crsp_comp, df.GVKEY_year_mth == crsp_comp.GVKEY_year_mth, 'leftouter').drop(crsp_comp.GVKEY_year_mth).drop(df.sic).drop(crsp_comp.GVKEY)

# JOIN RECOMMENDATIONS
df = df.join(recommendations, df.TIC_year_mth == recommendations.TIC_year_mth, 'leftouter').drop(recommendations.TIC_year_mth).drop(recommendations.cusip)

# JOIN BETA SUITE
df = df.join(beta_suite, df.PERMNO_year_mth == beta_suite.PERMNO_year_mth, 'leftouter').drop(beta_suite.PERMNO_year_mth).dropDuplicates()

# Sector - Enrich
# https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard
sector = sqlContext.createDataFrame([(10.0, "Energy"), (15.0, "Materials"), (20.0, "Industrials"), \
(25.0, "Consumer Discretionary"), (30.0, "Consumer Staples"), (35.0, "Health Care"), (40.0, "Financials"), \
(45.0, "Information Technology"), (50.0, "Telecommunication Services"), (55.0, "Utilities"), (60.0, "Real Estate")], \
["GSECTOR", "sector"])

# JOIN sector
df = df.join(sector, df.GSECTOR == sector.GSECTOR, 'leftouter').drop(sector.GSECTOR)

df.write.parquet("hdfs:///user/w205/financial_data/parquet_files/output_file")

######################################################################
### EVERYTHING ABOVE THIS POINT IS PART OF THE EDA TRANSFORMATIONS ###
######################################################################

## QUERY DATA TO PRODUCE NEW DATAFRAMES
crsp_comp.createOrReplaceTempView("tempview")
results = spark.sql("SELECT loc FROM tempview limit 50")


# taking mean of GVKEY is only an example, obviously we wouldn't do that
# sqlCtx.table("temptable").groupby("LPERMNO").agg("LPERMNO", mean("GVKEY")).collect()
