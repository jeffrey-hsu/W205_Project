import pandas as pd
import numpy as np
import datetime
import numpy as np
from sklearn import preprocessing
pd.options.mode.chained_assignment = None  # default='warn'

def drop_dups(df):
    # list comprehension of the cols that end with '_y'
    y_drop = [x for x in df if x.endswith('_y')]
    df.drop(y_drop, axis=1, inplace=True)

def floatToString(inputValue):
    result = ('%.15f' % inputValue).rstrip('0').rstrip('.')
    return '0' if result == '-0' else result

def mad(array):
    # https://en.wikipedia.org/wiki/Median_absolute_deviation
    array = np.ma.array(array).compressed()
    median = np.nanmedian(array)
    mad = np.nanmedian(np.abs(array - median))
    return mad

def meanad(array):
    # https://en.wikipedia.org/wiki/Mean_absolute_deviation
    array = np.ma.array(array).compressed()
    median = np.nanmedian(array)
    mad = np.nanmean(np.abs(array - median))
    return mad

def modified_z(array):
    # Modified Z-Score
    # https://www.ibm.com/support/knowledgecenter/en/SSWLVY_1.0.0/com.ibm.spss.analyticcatalyst.help/analytic_catalyst/modified_z.html
    median = np.nanmedian(array)
    median_absolute_deviation = mad(array)

    if median_absolute_deviation == 0:
        mean_absolute_deviation = meanad(array)
        array = (array - median) / (mean_absolute_deviation * 1.253314)
        return array

    else:
        array = (array - median) / (median_absolute_deviation * 1.486)
        return array

def clip_outliers(column):
    # Use try in case all null column
    try:
        floor = column.quantile(0.05)
        ceiling = column.quantile(0.95)
        column = column.clip(floor, ceiling)
        return column
    # If error, return as is
    except:
        return column

# Load Financial Ratios as foundation
df = pd.read_csv('c:/users/shane/Desktop/W205_Final/sample_data/Financial_Ratios_Firm_Level.csv')
# Load Link table to merge dataframes
link_table = pd.read_csv('c:/users/shane/Desktop/W205_Final/sample_data/CRSP_Compustat_Merged_Linking_Table.csv')

# Replace 'E' (Current) with future date
link_table['LINKENDDT'] = link_table['LINKENDDT'].str.replace('E','20200101').astype(int)

# Convert PERMNO from int to object
link_table['LPERMNO'] = link_table['LPERMNO'].astype(object)

# Link Table - Clean - LINKDT & LINKENDDT
# Get start and end dates for link to determine identification during given time
link_table['LINKDT'] = pd.to_datetime(link_table['LINKDT'], format='%Y%m%d')
link_table['LINKENDDT'] = pd.to_datetime(link_table['LINKENDDT'], format='%Y%m%d')

# Merge - df & Link Table
# Merge financial ratio suite with link table to build more foreign keys
df = pd.merge(df, link_table, on='gvkey', how='left', suffixes=('', '_y'))
# Release Memory
del link_table

# df - Enrich - Time Features
# Convert to time
df['public_date'] = pd.to_datetime(df['public_date'], infer_datetime_format=True)
# Create year
df['year'] = df['public_date'].dt.year
# Create month
df['month'] = df['public_date'].dt.month
# Create year-month
df["year-month"] = df['public_date'].apply(lambda x: x.strftime('%Y-%m'))

# df - Enrich - Time-based Unique Identifiers
# Create Unique Identifier for links from other databases for specific date-time
df["GVKEY-year-month"] = df["gvkey"].map(str) + "-" + df["year-month"]
df["CUSIP-year-month"] = df["cusip"].map(str) + "-" + df["year-month"]
df["TIC-year-month"] = df["tic"].map(str) + "-" + df["year-month"]
df["PERMNO-year-month"] = df["LPERMNO"].map(str) + "-" + df["year-month"]

# df - Subset - Correct Unique ID's per public data
# Filter dataframe to rows of unique identifiers during correct link range
df = df[(df.public_date >= df.LINKDT) & (df.public_date <= df.LINKENDDT)]

# df - Clean - Remove duplicate Time-based Unique Identifiers
df = df.drop_duplicates(['GVKEY-year-month'], keep='last')

# CRSP - Load
# Load CRSP/Compustat Merged Database - Security Monthly from Wharton
CRSP_comp_merge = pd.read_csv('c:/users/shane/desktop/W205_Final/sample_data/CRSP_Compustat_Merged_Security_Monthly.csv')

# CRSP - Enrich - Time Features
# Convert to date-time
CRSP_comp_merge['datadate'] = pd.to_datetime(CRSP_comp_merge['datadate'], infer_datetime_format=True)
# Create year
CRSP_comp_merge['year'] = CRSP_comp_merge['datadate'].dt.year
# Create month
CRSP_comp_merge['month'] = CRSP_comp_merge['datadate'].dt.month
# Create year-month
CRSP_comp_merge["year-month"] = CRSP_comp_merge['datadate'].apply(lambda x: x.strftime('%Y-%m'))

# CRSP - Enrich - Time-based Unique Identifiers
# CRSP - Unique ID in time (GVKEY + time-month)
CRSP_comp_merge["GVKEY-year-month"] = CRSP_comp_merge["GVKEY"].map(str) + "-" + CRSP_comp_merge["year-month"]

# CRSP - Sort - Date and GVKEY
CRSP_comp_merge = CRSP_comp_merge.sort_values(by=['GVKEY','datadate'], ascending=[True,True])

# CRSP - Enrich - Forward (PRCCM -- Price - Close - Monthly) 1 – 36 months
CRSP_comp_merge['forward_one_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-1)
CRSP_comp_merge['forward_two_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-2)
CRSP_comp_merge['forward_three_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-3)
CRSP_comp_merge['forward_four_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-4)
CRSP_comp_merge['forward_five_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-5)
CRSP_comp_merge['forward_six_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-6)
CRSP_comp_merge['forward_seven_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-7)
CRSP_comp_merge['forward_eight_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-8)
CRSP_comp_merge['forward_nine_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-9)
CRSP_comp_merge['forward_ten_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-10)
CRSP_comp_merge['forward_eleven_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-11)
CRSP_comp_merge['forward_twelve_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-12)
CRSP_comp_merge['forward_thirteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-13)
CRSP_comp_merge['forward_fourteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-14)
CRSP_comp_merge['forward_fifteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-15)
CRSP_comp_merge['forward_sixteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-16)
CRSP_comp_merge['forward_seventeen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-17)
CRSP_comp_merge['forward_eighteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-18)
CRSP_comp_merge['forward_nineteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-19)
CRSP_comp_merge['forward_twenty_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-20)
CRSP_comp_merge['forward_twentyone_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-21)
CRSP_comp_merge['forward_twentytwo_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-22)
CRSP_comp_merge['forward_twentythree_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-23)
CRSP_comp_merge['forward_twentyfour_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-24)
CRSP_comp_merge['forward_twentyfive_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-25)
CRSP_comp_merge['forward_twentysix_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-26)
CRSP_comp_merge['forward_twentyseven_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-27)
CRSP_comp_merge['forward_twentyeight_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-28)
CRSP_comp_merge['forward_twentynine_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-29)
CRSP_comp_merge['forward_thirty_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-30)
CRSP_comp_merge['forward_thirtyone_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-31)
CRSP_comp_merge['forward_thirtytwo_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-32)
CRSP_comp_merge['forward_thirtythree_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-33)
CRSP_comp_merge['forward_thirtyfour_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-34)
CRSP_comp_merge['forward_thirtyfive_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-35)
CRSP_comp_merge['forward_thirtysix_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(-36)

# CRSP - Enrich - Forward (AJEXM -- Cumulative Adjustment Factor - Ex Date -Monthly) 1 – 36 months
CRSP_comp_merge['forward_one_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-1)
CRSP_comp_merge['forward_two_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-2)
CRSP_comp_merge['forward_three_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-3)
CRSP_comp_merge['forward_four_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-4)
CRSP_comp_merge['forward_five_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-5)
CRSP_comp_merge['forward_six_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-6)
CRSP_comp_merge['forward_seven_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-7)
CRSP_comp_merge['forward_eight_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-8)
CRSP_comp_merge['forward_nine_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-9)
CRSP_comp_merge['forward_ten_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-10)
CRSP_comp_merge['forward_eleven_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-11)
CRSP_comp_merge['forward_twelve_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-12)
CRSP_comp_merge['forward_thirteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-13)
CRSP_comp_merge['forward_fourteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-14)
CRSP_comp_merge['forward_fifteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-15)
CRSP_comp_merge['forward_sixteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-16)
CRSP_comp_merge['forward_seventeen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-17)
CRSP_comp_merge['forward_eighteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-18)
CRSP_comp_merge['forward_nineteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-19)
CRSP_comp_merge['forward_twenty_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-20)
CRSP_comp_merge['forward_twentyone_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-21)
CRSP_comp_merge['forward_twentytwo_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-22)
CRSP_comp_merge['forward_twentythree_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-23)
CRSP_comp_merge['forward_twentyfour_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-24)
CRSP_comp_merge['forward_twentyfive_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-25)
CRSP_comp_merge['forward_twentysix_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-26)
CRSP_comp_merge['forward_twentyseven_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-27)
CRSP_comp_merge['forward_twentyeight_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-28)
CRSP_comp_merge['forward_twentynine_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-29)
CRSP_comp_merge['forward_thirty_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-30)
CRSP_comp_merge['forward_thirtyone_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-31)
CRSP_comp_merge['forward_thirtytwo_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-32)
CRSP_comp_merge['forward_thirtythree_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-33)
CRSP_comp_merge['forward_thirtyfour_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-34)
CRSP_comp_merge['forward_thirtyfive_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-35)
CRSP_comp_merge['forward_thirtysix_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(-36)

# CRSP - Enrich - Forward (TRFM -- Monthly Total Return Factor) 1 – 36 months
CRSP_comp_merge['forward_one_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-1)
CRSP_comp_merge['forward_two_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-2)
CRSP_comp_merge['forward_three_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-3)
CRSP_comp_merge['forward_four_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-4)
CRSP_comp_merge['forward_five_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-5)
CRSP_comp_merge['forward_six_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-6)
CRSP_comp_merge['forward_seven_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-7)
CRSP_comp_merge['forward_eight_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-8)
CRSP_comp_merge['forward_nine_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-9)
CRSP_comp_merge['forward_ten_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-10)
CRSP_comp_merge['forward_eleven_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-11)
CRSP_comp_merge['forward_twelve_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-12)
CRSP_comp_merge['forward_thirteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-13)
CRSP_comp_merge['forward_fourteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-14)
CRSP_comp_merge['forward_fifteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-15)
CRSP_comp_merge['forward_sixteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-16)
CRSP_comp_merge['forward_seventeen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-17)
CRSP_comp_merge['forward_eighteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-18)
CRSP_comp_merge['forward_nineteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-19)
CRSP_comp_merge['forward_twenty_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-20)
CRSP_comp_merge['forward_twentyone_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-21)
CRSP_comp_merge['forward_twentytwo_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-22)
CRSP_comp_merge['forward_twentythree_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-23)
CRSP_comp_merge['forward_twentyfour_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-24)
CRSP_comp_merge['forward_twentyfive_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-25)
CRSP_comp_merge['forward_twentysix_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-26)
CRSP_comp_merge['forward_twentyseven_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-27)
CRSP_comp_merge['forward_twentyeight_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-28)
CRSP_comp_merge['forward_twentynine_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-29)
CRSP_comp_merge['forward_thirty_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-30)
CRSP_comp_merge['forward_thirtyone_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-31)
CRSP_comp_merge['forward_thirtytwo_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-32)
CRSP_comp_merge['forward_thirtythree_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-33)
CRSP_comp_merge['forward_thirtyfour_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-34)
CRSP_comp_merge['forward_thirtyfive_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-35)
CRSP_comp_merge['forward_thirtysix_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(-36)

# CRSP - Enrich - Forward Return over number of months (1 - 36)
CRSP_comp_merge['forward_one_month_return'] = ((((CRSP_comp_merge['forward_one_month_prccm']/CRSP_comp_merge['forward_one_month_ajexm'])*CRSP_comp_merge['forward_one_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_two_month_return'] = ((((CRSP_comp_merge['forward_two_month_prccm']/CRSP_comp_merge['forward_two_month_ajexm'])*CRSP_comp_merge['forward_two_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_three_month_return'] = ((((CRSP_comp_merge['forward_three_month_prccm']/CRSP_comp_merge['forward_three_month_ajexm'])*CRSP_comp_merge['forward_three_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_four_month_return'] = ((((CRSP_comp_merge['forward_four_month_prccm']/CRSP_comp_merge['forward_four_month_ajexm'])*CRSP_comp_merge['forward_four_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_five_month_return'] = ((((CRSP_comp_merge['forward_five_month_prccm']/CRSP_comp_merge['forward_five_month_ajexm'])*CRSP_comp_merge['forward_five_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_six_month_return'] = ((((CRSP_comp_merge['forward_six_month_prccm']/CRSP_comp_merge['forward_six_month_ajexm'])*CRSP_comp_merge['forward_six_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_seven_month_return'] = ((((CRSP_comp_merge['forward_seven_month_prccm']/CRSP_comp_merge['forward_seven_month_ajexm'])*CRSP_comp_merge['forward_seven_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_eight_month_return'] = ((((CRSP_comp_merge['forward_eight_month_prccm']/CRSP_comp_merge['forward_eight_month_ajexm'])*CRSP_comp_merge['forward_eight_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_nine_month_return'] = ((((CRSP_comp_merge['forward_nine_month_prccm']/CRSP_comp_merge['forward_nine_month_ajexm'])*CRSP_comp_merge['forward_nine_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_ten_month_return'] = ((((CRSP_comp_merge['forward_ten_month_prccm']/CRSP_comp_merge['forward_ten_month_ajexm'])*CRSP_comp_merge['forward_ten_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_eleven_month_return'] = ((((CRSP_comp_merge['forward_eleven_month_prccm']/CRSP_comp_merge['forward_eleven_month_ajexm'])*CRSP_comp_merge['forward_eleven_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twelve_month_return'] = ((((CRSP_comp_merge['forward_twelve_month_prccm']/CRSP_comp_merge['forward_twelve_month_ajexm'])*CRSP_comp_merge['forward_twelve_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirteen_month_return'] = ((((CRSP_comp_merge['forward_thirteen_month_prccm']/CRSP_comp_merge['forward_thirteen_month_ajexm'])*CRSP_comp_merge['forward_thirteen_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_fourteen_month_return'] = ((((CRSP_comp_merge['forward_fourteen_month_prccm']/CRSP_comp_merge['forward_fourteen_month_ajexm'])*CRSP_comp_merge['forward_fourteen_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_fifteen_month_return'] = ((((CRSP_comp_merge['forward_fifteen_month_prccm']/CRSP_comp_merge['forward_fifteen_month_ajexm'])*CRSP_comp_merge['forward_fifteen_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_sixteen_month_return'] = ((((CRSP_comp_merge['forward_sixteen_month_prccm']/CRSP_comp_merge['forward_sixteen_month_ajexm'])*CRSP_comp_merge['forward_sixteen_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_seventeen_month_return'] = ((((CRSP_comp_merge['forward_seventeen_month_prccm']/CRSP_comp_merge['forward_seventeen_month_ajexm'])*CRSP_comp_merge['forward_seventeen_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_eighteen_month_return'] = ((((CRSP_comp_merge['forward_eighteen_month_prccm']/CRSP_comp_merge['forward_eighteen_month_ajexm'])*CRSP_comp_merge['forward_eighteen_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_nineteen_month_return'] = ((((CRSP_comp_merge['forward_nineteen_month_prccm']/CRSP_comp_merge['forward_nineteen_month_ajexm'])*CRSP_comp_merge['forward_nineteen_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twenty_month_return'] = ((((CRSP_comp_merge['forward_twenty_month_prccm']/CRSP_comp_merge['forward_twenty_month_ajexm'])*CRSP_comp_merge['forward_twenty_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentyone_month_return'] = ((((CRSP_comp_merge['forward_twentyone_month_prccm']/CRSP_comp_merge['forward_twentyone_month_ajexm'])*CRSP_comp_merge['forward_twentyone_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentytwo_month_return'] = ((((CRSP_comp_merge['forward_twentytwo_month_prccm']/CRSP_comp_merge['forward_twentytwo_month_ajexm'])*CRSP_comp_merge['forward_twentytwo_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentythree_month_return'] = ((((CRSP_comp_merge['forward_twentythree_month_prccm']/CRSP_comp_merge['forward_twentythree_month_ajexm'])*CRSP_comp_merge['forward_twentythree_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentyfour_month_return'] = ((((CRSP_comp_merge['forward_twentyfour_month_prccm']/CRSP_comp_merge['forward_twentyfour_month_ajexm'])*CRSP_comp_merge['forward_twentyfour_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentyfive_month_return'] = ((((CRSP_comp_merge['forward_twentyfive_month_prccm']/CRSP_comp_merge['forward_twentyfive_month_ajexm'])*CRSP_comp_merge['forward_twentyfive_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentysix_month_return'] = ((((CRSP_comp_merge['forward_twentysix_month_prccm']/CRSP_comp_merge['forward_twentysix_month_ajexm'])*CRSP_comp_merge['forward_twentysix_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentyseven_month_return'] = ((((CRSP_comp_merge['forward_twentyseven_month_prccm']/CRSP_comp_merge['forward_twentyseven_month_ajexm'])*CRSP_comp_merge['forward_twentyseven_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentyeight_month_return'] = ((((CRSP_comp_merge['forward_twentyeight_month_prccm']/CRSP_comp_merge['forward_twentyeight_month_ajexm'])*CRSP_comp_merge['forward_twentyeight_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_twentynine_month_return'] = ((((CRSP_comp_merge['forward_twentynine_month_prccm']/CRSP_comp_merge['forward_twentynine_month_ajexm'])*CRSP_comp_merge['forward_twentynine_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirty_month_return'] = ((((CRSP_comp_merge['forward_thirty_month_prccm']/CRSP_comp_merge['forward_thirty_month_ajexm'])*CRSP_comp_merge['forward_thirty_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirtyone_month_return'] = ((((CRSP_comp_merge['forward_thirtyone_month_prccm']/CRSP_comp_merge['forward_thirtyone_month_ajexm'])*CRSP_comp_merge['forward_thirtyone_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirtytwo_month_return'] = ((((CRSP_comp_merge['forward_thirtytwo_month_prccm']/CRSP_comp_merge['forward_thirtytwo_month_ajexm'])*CRSP_comp_merge['forward_thirtytwo_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirtythree_month_return'] = ((((CRSP_comp_merge['forward_thirtythree_month_prccm']/CRSP_comp_merge['forward_thirtythree_month_ajexm'])*CRSP_comp_merge['forward_thirtythree_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirtyfour_month_return'] = ((((CRSP_comp_merge['forward_thirtyfour_month_prccm']/CRSP_comp_merge['forward_thirtyfour_month_ajexm'])*CRSP_comp_merge['forward_thirtyfour_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirtyfive_month_return'] = ((((CRSP_comp_merge['forward_thirtyfive_month_prccm']/CRSP_comp_merge['forward_thirtyfive_month_ajexm'])*CRSP_comp_merge['forward_thirtyfive_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100
CRSP_comp_merge['forward_thirtysix_month_return'] = ((((CRSP_comp_merge['forward_thirtysix_month_prccm']/CRSP_comp_merge['forward_thirtysix_month_ajexm'])*CRSP_comp_merge['forward_thirtysix_month_trfm'])/((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['trfm']))-1)*100

# CRSP - Enrich - Past (PRCCM -- Price - Close - Monthly) 1 – 36 months
CRSP_comp_merge['past_one_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(1)
CRSP_comp_merge['past_two_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(2)
CRSP_comp_merge['past_three_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(3)
CRSP_comp_merge['past_four_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(4)
CRSP_comp_merge['past_five_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(5)
CRSP_comp_merge['past_six_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(6)
CRSP_comp_merge['past_seven_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(7)
CRSP_comp_merge['past_eight_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(8)
CRSP_comp_merge['past_nine_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(9)
CRSP_comp_merge['past_ten_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(10)
CRSP_comp_merge['past_eleven_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(11)
CRSP_comp_merge['past_twelve_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(12)
CRSP_comp_merge['past_thirteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(13)
CRSP_comp_merge['past_fourteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(14)
CRSP_comp_merge['past_fifteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(15)
CRSP_comp_merge['past_sixteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(16)
CRSP_comp_merge['past_seventeen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(17)
CRSP_comp_merge['past_eighteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(18)
CRSP_comp_merge['past_nineteen_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(19)
CRSP_comp_merge['past_twenty_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(20)
CRSP_comp_merge['past_twentyone_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(21)
CRSP_comp_merge['past_twentytwo_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(22)
CRSP_comp_merge['past_twentythree_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(23)
CRSP_comp_merge['past_twentyfour_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(24)
CRSP_comp_merge['past_twentyfive_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(25)
CRSP_comp_merge['past_twentysix_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(26)
CRSP_comp_merge['past_twentyseven_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(27)
CRSP_comp_merge['past_twentyeight_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(28)
CRSP_comp_merge['past_twentynine_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(29)
CRSP_comp_merge['past_thirty_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(30)
CRSP_comp_merge['past_thirtyone_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(31)
CRSP_comp_merge['past_thirtytwo_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(32)
CRSP_comp_merge['past_thirtythree_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(33)
CRSP_comp_merge['past_thirtyfour_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(34)
CRSP_comp_merge['past_thirtyfive_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(35)
CRSP_comp_merge['past_thirtysix_month_prccm'] = CRSP_comp_merge.groupby('GVKEY')['prccm'].shift(36)

# CRSP - Enrich -  Past (AJEXM -- Cumulative Adjustment Factor - Ex Date -Monthly) 1 – 36 months
CRSP_comp_merge['past_one_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(1)
CRSP_comp_merge['past_two_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(2)
CRSP_comp_merge['past_three_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(3)
CRSP_comp_merge['past_four_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(4)
CRSP_comp_merge['past_five_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(5)
CRSP_comp_merge['past_six_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(6)
CRSP_comp_merge['past_seven_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(7)
CRSP_comp_merge['past_eight_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(8)
CRSP_comp_merge['past_nine_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(9)
CRSP_comp_merge['past_ten_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(10)
CRSP_comp_merge['past_eleven_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(11)
CRSP_comp_merge['past_twelve_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(12)
CRSP_comp_merge['past_thirteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(13)
CRSP_comp_merge['past_fourteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(14)
CRSP_comp_merge['past_fifteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(15)
CRSP_comp_merge['past_sixteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(16)
CRSP_comp_merge['past_seventeen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(17)
CRSP_comp_merge['past_eighteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(18)
CRSP_comp_merge['past_nineteen_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(19)
CRSP_comp_merge['past_twenty_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(20)
CRSP_comp_merge['past_twentyone_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(21)
CRSP_comp_merge['past_twentytwo_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(22)
CRSP_comp_merge['past_twentythree_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(23)
CRSP_comp_merge['past_twentyfour_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(24)
CRSP_comp_merge['past_twentyfive_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(25)
CRSP_comp_merge['past_twentysix_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(26)
CRSP_comp_merge['past_twentyseven_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(27)
CRSP_comp_merge['past_twentyeight_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(28)
CRSP_comp_merge['past_twentynine_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(29)
CRSP_comp_merge['past_thirty_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(30)
CRSP_comp_merge['past_thirtyone_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(31)
CRSP_comp_merge['past_thirtytwo_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(32)
CRSP_comp_merge['past_thirtythree_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(33)
CRSP_comp_merge['past_thirtyfour_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(34)
CRSP_comp_merge['past_thirtyfive_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(35)
CRSP_comp_merge['past_thirtysix_month_ajexm'] = CRSP_comp_merge.groupby('GVKEY')['ajexm'].shift(36)

# CRSP - Enrich -  Past (TRFM -- Monthly Total Return Factor) 1 – 36 months
CRSP_comp_merge['past_one_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(1)
CRSP_comp_merge['past_two_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(2)
CRSP_comp_merge['past_three_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(3)
CRSP_comp_merge['past_four_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(4)
CRSP_comp_merge['past_five_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(5)
CRSP_comp_merge['past_six_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(6)
CRSP_comp_merge['past_seven_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(7)
CRSP_comp_merge['past_eight_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(8)
CRSP_comp_merge['past_nine_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(9)
CRSP_comp_merge['past_ten_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(10)
CRSP_comp_merge['past_eleven_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(11)
CRSP_comp_merge['past_twelve_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(12)
CRSP_comp_merge['past_thirteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(13)
CRSP_comp_merge['past_fourteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(14)
CRSP_comp_merge['past_fifteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(15)
CRSP_comp_merge['past_sixteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(16)
CRSP_comp_merge['past_seventeen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(17)
CRSP_comp_merge['past_eighteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(18)
CRSP_comp_merge['past_nineteen_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(19)
CRSP_comp_merge['past_twenty_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(20)
CRSP_comp_merge['past_twentyone_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(21)
CRSP_comp_merge['past_twentytwo_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(22)
CRSP_comp_merge['past_twentythree_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(23)
CRSP_comp_merge['past_twentyfour_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(24)
CRSP_comp_merge['past_twentyfive_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(25)
CRSP_comp_merge['past_twentysix_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(26)
CRSP_comp_merge['past_twentyseven_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(27)
CRSP_comp_merge['past_twentyeight_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(28)
CRSP_comp_merge['past_twentynine_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(29)
CRSP_comp_merge['past_thirty_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(30)
CRSP_comp_merge['past_thirtyone_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(31)
CRSP_comp_merge['past_thirtytwo_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(32)
CRSP_comp_merge['past_thirtythree_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(33)
CRSP_comp_merge['past_thirtyfour_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(34)
CRSP_comp_merge['past_thirtyfive_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(35)
CRSP_comp_merge['past_thirtysix_month_trfm'] = CRSP_comp_merge.groupby('GVKEY')['trfm'].shift(36)

# CRSP - Enrich -  Past Return over number of months (1 - 36)
CRSP_comp_merge['past_one_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_one_month_trfm'])/((CRSP_comp_merge['past_one_month_prccm']/CRSP_comp_merge['past_one_month_ajexm'])*CRSP_comp_merge['past_one_month_trfm']))-1)*100
CRSP_comp_merge['past_two_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_two_month_trfm'])/((CRSP_comp_merge['past_two_month_prccm']/CRSP_comp_merge['past_two_month_ajexm'])*CRSP_comp_merge['past_two_month_trfm']))-1)*100
CRSP_comp_merge['past_three_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_three_month_trfm'])/((CRSP_comp_merge['past_three_month_prccm']/CRSP_comp_merge['past_three_month_ajexm'])*CRSP_comp_merge['past_three_month_trfm']))-1)*100
CRSP_comp_merge['past_four_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_four_month_trfm'])/((CRSP_comp_merge['past_four_month_prccm']/CRSP_comp_merge['past_four_month_ajexm'])*CRSP_comp_merge['past_four_month_trfm']))-1)*100
CRSP_comp_merge['past_five_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_five_month_trfm'])/((CRSP_comp_merge['past_five_month_prccm']/CRSP_comp_merge['past_five_month_ajexm'])*CRSP_comp_merge['past_five_month_trfm']))-1)*100
CRSP_comp_merge['past_six_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_six_month_trfm'])/((CRSP_comp_merge['past_six_month_prccm']/CRSP_comp_merge['past_six_month_ajexm'])*CRSP_comp_merge['past_six_month_trfm']))-1)*100
CRSP_comp_merge['past_seven_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_seven_month_trfm'])/((CRSP_comp_merge['past_seven_month_prccm']/CRSP_comp_merge['past_seven_month_ajexm'])*CRSP_comp_merge['past_seven_month_trfm']))-1)*100
CRSP_comp_merge['past_eight_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_eight_month_trfm'])/((CRSP_comp_merge['past_eight_month_prccm']/CRSP_comp_merge['past_eight_month_ajexm'])*CRSP_comp_merge['past_eight_month_trfm']))-1)*100
CRSP_comp_merge['past_nine_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_nine_month_trfm'])/((CRSP_comp_merge['past_nine_month_prccm']/CRSP_comp_merge['past_nine_month_ajexm'])*CRSP_comp_merge['past_nine_month_trfm']))-1)*100
CRSP_comp_merge['past_ten_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_ten_month_trfm'])/((CRSP_comp_merge['past_ten_month_prccm']/CRSP_comp_merge['past_ten_month_ajexm'])*CRSP_comp_merge['past_ten_month_trfm']))-1)*100
CRSP_comp_merge['past_eleven_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_eleven_month_trfm'])/((CRSP_comp_merge['past_eleven_month_prccm']/CRSP_comp_merge['past_eleven_month_ajexm'])*CRSP_comp_merge['past_eleven_month_trfm']))-1)*100
CRSP_comp_merge['past_twelve_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twelve_month_trfm'])/((CRSP_comp_merge['past_twelve_month_prccm']/CRSP_comp_merge['past_twelve_month_ajexm'])*CRSP_comp_merge['past_twelve_month_trfm']))-1)*100
CRSP_comp_merge['past_thirteen_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirteen_month_trfm'])/((CRSP_comp_merge['past_thirteen_month_prccm']/CRSP_comp_merge['past_thirteen_month_ajexm'])*CRSP_comp_merge['past_thirteen_month_trfm']))-1)*100
CRSP_comp_merge['past_fourteen_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_fourteen_month_trfm'])/((CRSP_comp_merge['past_fourteen_month_prccm']/CRSP_comp_merge['past_fourteen_month_ajexm'])*CRSP_comp_merge['past_fourteen_month_trfm']))-1)*100
CRSP_comp_merge['past_fifteen_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_fifteen_month_trfm'])/((CRSP_comp_merge['past_fifteen_month_prccm']/CRSP_comp_merge['past_fifteen_month_ajexm'])*CRSP_comp_merge['past_fifteen_month_trfm']))-1)*100
CRSP_comp_merge['past_sixteen_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_sixteen_month_trfm'])/((CRSP_comp_merge['past_sixteen_month_prccm']/CRSP_comp_merge['past_sixteen_month_ajexm'])*CRSP_comp_merge['past_sixteen_month_trfm']))-1)*100
CRSP_comp_merge['past_seventeen_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_seventeen_month_trfm'])/((CRSP_comp_merge['past_seventeen_month_prccm']/CRSP_comp_merge['past_seventeen_month_ajexm'])*CRSP_comp_merge['past_seventeen_month_trfm']))-1)*100
CRSP_comp_merge['past_eighteen_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_eighteen_month_trfm'])/((CRSP_comp_merge['past_eighteen_month_prccm']/CRSP_comp_merge['past_eighteen_month_ajexm'])*CRSP_comp_merge['past_eighteen_month_trfm']))-1)*100
CRSP_comp_merge['past_nineteen_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_nineteen_month_trfm'])/((CRSP_comp_merge['past_nineteen_month_prccm']/CRSP_comp_merge['past_nineteen_month_ajexm'])*CRSP_comp_merge['past_nineteen_month_trfm']))-1)*100
CRSP_comp_merge['past_twenty_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twenty_month_trfm'])/((CRSP_comp_merge['past_twenty_month_prccm']/CRSP_comp_merge['past_twenty_month_ajexm'])*CRSP_comp_merge['past_twenty_month_trfm']))-1)*100
CRSP_comp_merge['past_twentyone_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentyone_month_trfm'])/((CRSP_comp_merge['past_twentyone_month_prccm']/CRSP_comp_merge['past_twentyone_month_ajexm'])*CRSP_comp_merge['past_twentyone_month_trfm']))-1)*100
CRSP_comp_merge['past_twentytwo_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentytwo_month_trfm'])/((CRSP_comp_merge['past_twentytwo_month_prccm']/CRSP_comp_merge['past_twentytwo_month_ajexm'])*CRSP_comp_merge['past_twentytwo_month_trfm']))-1)*100
CRSP_comp_merge['past_twentythree_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentythree_month_trfm'])/((CRSP_comp_merge['past_twentythree_month_prccm']/CRSP_comp_merge['past_twentythree_month_ajexm'])*CRSP_comp_merge['past_twentythree_month_trfm']))-1)*100
CRSP_comp_merge['past_twentyfour_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentyfour_month_trfm'])/((CRSP_comp_merge['past_twentyfour_month_prccm']/CRSP_comp_merge['past_twentyfour_month_ajexm'])*CRSP_comp_merge['past_twentyfour_month_trfm']))-1)*100
CRSP_comp_merge['past_twentyfive_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentyfive_month_trfm'])/((CRSP_comp_merge['past_twentyfive_month_prccm']/CRSP_comp_merge['past_twentyfive_month_ajexm'])*CRSP_comp_merge['past_twentyfive_month_trfm']))-1)*100
CRSP_comp_merge['past_twentysix_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentysix_month_trfm'])/((CRSP_comp_merge['past_twentysix_month_prccm']/CRSP_comp_merge['past_twentysix_month_ajexm'])*CRSP_comp_merge['past_twentysix_month_trfm']))-1)*100
CRSP_comp_merge['past_twentyseven_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentyseven_month_trfm'])/((CRSP_comp_merge['past_twentyseven_month_prccm']/CRSP_comp_merge['past_twentyseven_month_ajexm'])*CRSP_comp_merge['past_twentyseven_month_trfm']))-1)*100
CRSP_comp_merge['past_twentyeight_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentyeight_month_trfm'])/((CRSP_comp_merge['past_twentyeight_month_prccm']/CRSP_comp_merge['past_twentyeight_month_ajexm'])*CRSP_comp_merge['past_twentyeight_month_trfm']))-1)*100
CRSP_comp_merge['past_twentynine_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_twentynine_month_trfm'])/((CRSP_comp_merge['past_twentynine_month_prccm']/CRSP_comp_merge['past_twentynine_month_ajexm'])*CRSP_comp_merge['past_twentynine_month_trfm']))-1)*100
CRSP_comp_merge['past_thirty_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirty_month_trfm'])/((CRSP_comp_merge['past_thirty_month_prccm']/CRSP_comp_merge['past_thirty_month_ajexm'])*CRSP_comp_merge['past_thirty_month_trfm']))-1)*100
CRSP_comp_merge['past_thirtyone_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirtyone_month_trfm'])/((CRSP_comp_merge['past_thirtyone_month_prccm']/CRSP_comp_merge['past_thirtyone_month_ajexm'])*CRSP_comp_merge['past_thirtyone_month_trfm']))-1)*100
CRSP_comp_merge['past_thirtytwo_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirtytwo_month_trfm'])/((CRSP_comp_merge['past_thirtytwo_month_prccm']/CRSP_comp_merge['past_thirtytwo_month_ajexm'])*CRSP_comp_merge['past_thirtytwo_month_trfm']))-1)*100
CRSP_comp_merge['past_thirtythree_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirtythree_month_trfm'])/((CRSP_comp_merge['past_thirtythree_month_prccm']/CRSP_comp_merge['past_thirtythree_month_ajexm'])*CRSP_comp_merge['past_thirtythree_month_trfm']))-1)*100
CRSP_comp_merge['past_thirtyfour_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirtyfour_month_trfm'])/((CRSP_comp_merge['past_thirtyfour_month_prccm']/CRSP_comp_merge['past_thirtyfour_month_ajexm'])*CRSP_comp_merge['past_thirtyfour_month_trfm']))-1)*100
CRSP_comp_merge['past_thirtyfive_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirtyfive_month_trfm'])/((CRSP_comp_merge['past_thirtyfive_month_prccm']/CRSP_comp_merge['past_thirtyfive_month_ajexm'])*CRSP_comp_merge['past_thirtyfive_month_trfm']))-1)*100
CRSP_comp_merge['past_thirtysix_month_return'] = ((((CRSP_comp_merge['prccm']/CRSP_comp_merge['ajexm'])*CRSP_comp_merge['past_thirtysix_month_trfm'])/((CRSP_comp_merge['past_thirtysix_month_prccm']/CRSP_comp_merge['past_thirtysix_month_ajexm'])*CRSP_comp_merge['past_thirtysix_month_trfm']))-1)*100

# CRSP_comp_merge - Clean - Remove duplicate Time-based Unique Identifiers
CRSP_comp_merge = CRSP_comp_merge.drop_duplicates(['GVKEY-year-month'], keep='last')

# Merge - df & CRSP
df = pd.merge(df, CRSP_comp_merge, on='GVKEY-year-month', how='left', suffixes=('', '_y'))
del CRSP_comp_merge

# df - Clean - Remove duplicate features
drop_dups(df)

# Recommendations - Load
# Summary Statistics (Consensus Recommendations) from Wharton
recommendations = pd.read_csv('c:/users/shane/Desktop/W205_Final/sample_data/Recommendations_Summary_Statistics.csv', low_memory=False)

# Recommendations - Enrich - Time Features
recommendations['STATPERS'] = pd.to_datetime(recommendations['STATPERS'], infer_datetime_format=True)
# Create year
recommendations['year'] = recommendations['STATPERS'].dt.year
# Create month
recommendations['month'] = recommendations['STATPERS'].dt.month
# Creat year-month
recommendations["year-month"] = recommendations['STATPERS'].apply(lambda x: x.strftime('%Y-%m'))

# Recommendations - Enrich - Analyst Change
recommendations["recup"] = recommendations["NUMUP"] / recommendations["NUMREC"]
recommendations["recdown"] = recommendations["NUMDOWN"] / recommendations["NUMREC"]

# Recommendations - Clean - Strip @ in Ticker
recommendations['TICKER'] = recommendations['TICKER'].replace('@','',regex=True).astype('object')

# Recommendations - Enrich - Time-based Unique Identifiers
recommendations["TIC-year-month"] = recommendations["TICKER"].map(str) + "-" + recommendations["year-month"]

# recommendations - Clean - Remove duplicate Time-based Unique Identifiers
recommendations = recommendations.drop_duplicates(['TIC-year-month'], keep='last')

# Merge - df & Recommendations
df = pd.merge(df, recommendations, on='TIC-year-month', how='left', suffixes=('', '_y'))
# Release Memory
del recommendations

# df - Clean - Remove duplicate features
drop_dups(df)

# Beta Suite - Load
beta_suite = pd.read_csv('c:/users/shane/Desktop/W205_Final/sample_data/Beta_Suite.csv')

# Beta Suite - Enrich - Time Features
# Convert to data-time
beta_suite['DATE'] = pd.to_datetime(beta_suite['DATE'], infer_datetime_format=True)
# Create year
beta_suite['year'] = beta_suite['DATE'].dt.year
# Create month
beta_suite['month'] = beta_suite['DATE'].dt.month
# Create year-month
beta_suite["year-month"] = beta_suite['DATE'].apply(lambda x: x.strftime('%Y-%m'))

# Beta Suite - Enrich - Time-based Unique Identifiers
beta_suite["PERMNO-year-month"] = beta_suite["PERMNO"].map(str) + "-" + beta_suite["year-month"]

# Merge - df & Beta Suite
df = pd.merge(df, beta_suite, on='PERMNO-year-month', how='left', suffixes=('', '_y'))
# Release Memory
del beta_suite

# df - Clean - Remove duplicate features
drop_dups(df)

sector = pd.DataFrame()

sector['GSECTOR'] = [10.0,
                       15.0,
                       20.0,
                       25.0,
                       30.0,
                       35.0,
                       40.0,
                       45.0,
                       50.0,
                       55.0,
                       60.0]

sector['sector'] = ['Energy',
                        'Materials',
                        'Industrials',
                        'Consumer Discretionary',
                        'Consumer Staples',
                        'Health Care',
                        'Financials',
                        'Information Technology',
                        'Telecommunication Services',
                        'Utilities',
                        'Real Estate']

# Merge - df & Sector
df = pd.merge(df, sector, on='GSECTOR', how='left', suffixes=('', '_y'))
# Release Memory
del sector

# df - Clean - Remove duplicate features
drop_dups(df)

# df - Enrich - Month Features
df['january'] = np.where(df['month'] == 1, int(1), int(0))
df['february'] = np.where(df['month'] == 2, int(1), int(0))
df['march'] = np.where(df['month'] == 3, int(1), int(0))
df['april'] = np.where(df['month'] == 4, int(1), int(0))
df['may'] = np.where(df['month'] == 5, int(1), int(0))
df['june'] = np.where(df['month'] == 6, int(1), int(0))
df['july'] = np.where(df['month'] == 7, int(1), int(0))
df['august'] = np.where(df['month'] == 8, int(1), int(0))
df['september'] = np.where(df['month'] == 9, int(1), int(0))
df['october'] = np.where(df['month'] == 10, int(1), int(0))
df['november'] = np.where(df['month'] == 11, int(1), int(0))
df['december'] = np.where(df['month'] == 12, int(1), int(0))

# df - Subset - Relevant features with adequate data
df = df[[
        'GVKEY',
        'GVKEY-year-month',
        'sector',
        'year-month',
        'forward_one_month_return',
        'forward_two_month_return',
        'forward_three_month_return',
        'forward_four_month_return',
        'forward_five_month_return',
        'forward_six_month_return',
        'forward_seven_month_return',
        'forward_eight_month_return',
        'forward_nine_month_return',
        'forward_ten_month_return',
        'forward_eleven_month_return',
        'forward_twelve_month_return',
        'forward_thirteen_month_return',
        'forward_fourteen_month_return',
        'forward_fifteen_month_return',
        'forward_sixteen_month_return',
        'forward_seventeen_month_return',
        'forward_eighteen_month_return',
        'forward_nineteen_month_return',
        'forward_twenty_month_return',
        'forward_twentyone_month_return',
        'forward_twentytwo_month_return',
        'forward_twentythree_month_return',
        'forward_twentyfour_month_return',
        'forward_twentyfive_month_return',
        'forward_twentysix_month_return',
        'forward_twentyseven_month_return',
        'forward_twentyeight_month_return',
        'forward_twentynine_month_return',
        'forward_thirty_month_return',
        'forward_thirtyone_month_return',
        'forward_thirtytwo_month_return',
        'forward_thirtythree_month_return',
        'forward_thirtyfour_month_return',
        'forward_thirtyfive_month_return',
        'forward_thirtysix_month_return',
        'past_one_month_return',
        'past_two_month_return',
        'past_three_month_return',
        'past_four_month_return',
        'past_five_month_return',
        'past_six_month_return',
        'past_seven_month_return',
        'past_eight_month_return',
        'past_nine_month_return',
        'past_ten_month_return',
        'past_eleven_month_return',
        'past_twelve_month_return',
        'past_thirteen_month_return',
        'past_fourteen_month_return',
        'past_fifteen_month_return',
        'past_sixteen_month_return',
        'past_seventeen_month_return',
        'past_eighteen_month_return',
        'past_nineteen_month_return',
        'past_twenty_month_return',
        'past_twentyone_month_return',
        'past_twentytwo_month_return',
        'past_twentythree_month_return',
        'past_twentyfour_month_return',
        'past_twentyfive_month_return',
        'past_twentysix_month_return',
        'past_twentyseven_month_return',
        'past_twentyeight_month_return',
        'past_twentynine_month_return',
        'past_thirty_month_return',
        'past_thirtyone_month_return',
        'past_thirtytwo_month_return',
        'past_thirtythree_month_return',
        'past_thirtyfour_month_return',
        'past_thirtyfive_month_return',
        'past_thirtysix_month_return',
        'accrual',
        'adv_sale',
        'aftret_eq',
        'aftret_equity',
        'aftret_invcapx',
        'at_turn',
        'bm',
        'CAPEI',
        'capital_ratio',
        'cash_conversion',
        'cash_debt',
        'cash_lt',
        'cash_ratio',
        'cfm',
        'curr_debt',
        'curr_ratio',
        'de_ratio',
        'debt_assets',
        'debt_at',
        'debt_capital',
        'debt_ebitda',
        'debt_invcap',
        'DIVYIELD',
        'dltt_be',
        'dpr',
        'efftax',
        'equity_invcap',
        'evm',
        'fcf_ocf',
        'gpm',
        'GProf',
        'int_debt',
        'int_totdebt',
        'intcov',
        'intcov_ratio',
        'inv_turn',
        'invt_act',
        'lt_debt',
        'lt_ppent',
        'npm',
        'ocf_lct',
        'opmad',
        'opmbd',
        'pay_turn',
        'pcf',
        'pe_exi',
        'pe_inc',
        'pe_op_basic',
        'pe_op_dil',
        'PEG_1yrforward',
        'PEG_ltgforward',
        'PEG_trailing',
        'pretret_earnat',
        'pretret_noa',
        'profit_lct',
        'ps',
        'ptb',
        'ptpm',
        'quick_ratio',
        'rd_sale',
        'rect_act',
        'rect_turn',
        'roa',
        'roce',
        'roe',
        'sale_equity',
        'sale_invcap',
        'sale_nwc',
        'short_debt',
        'staff_sale',
        'totdebt_invcap',
        'dvpspm',
        'dvpsxm',
        'dvrate',
        'spcsrc',
        'alpha',
        'b_hml',
        'b_mkt',
        'b_smb',
        'b_umd',
        'exret',
        'ivol',
        'n',
        'R2',
        'tvol',
        'BUYPCT',
        'HOLDPCT',
        'SELLPCT',
        'MEANREC',
        'MEDREC',
        'recup',
        'recdown',
        'STDEV',
        'january',
        'february',
        'march',
        'april',
        'may',
        'june',
        'july',
        'august',
        'september',
        'october',
        'november',
        'december'
        ]]

# df - Clean - Dividends - DIVYIELD, dvpspm, dvpsxm, & dvrate
# # Companies who do not report dividends are unlikely to have paid dividends, the majority of companies do not pay as well.
df['DIVYIELD'] = df['DIVYIELD'].replace('%','',regex=True).astype('float')/100
df['DIVYIELD'] = pd.to_numeric(df['DIVYIELD'])
df['DIVYIELD'] = df['DIVYIELD'].fillna(0)

df['dvpspm'] = df['dvpspm'].fillna(0)
df['dvpspm'] = pd.to_numeric(df['dvpspm'])
df['dvpsxm'] = df['dvpsxm'].fillna(0)
df['dvpsxm'] = pd.to_numeric(df['dvpsxm'])
df['dvrate'] = df['dvrate'].fillna(0)
df['dvrate'] = pd.to_numeric(df['dvrate'])

# df - Clean - Ratings - SPCSRC
# Per S&P Quality Rankings information;
# A+ - 98-100 percentile (Highest, 2%)
# A - 92-98 percentile (High, 6%)
# A- - 86-92 percentile (Above Average, 6%)
# B+ - 70-86 percentile (Average, 16%)
# B - 51-70 percentile (Below Average, 19%)
# B- - 25-51 percentile (Lower, 26%)
# C - 1-25 percentile (Lowest, 24%)
# D - 0-1 percentile (In Reorganization, 1%)

# Using the average of percentile ranges above
df['spcsrc'] = df['spcsrc'].map({
                                'A+': 99,
                                'A': 95,
                                'A-': 89,
                                'B+': 79.5,
                                'B': 60.5,
                                'B-': 38,
                                'C': 13,
                                'D': 0.5,
                                })

df['spcsrc'] = pd.to_numeric(df['spcsrc'])

# df - Clean - Beta Suite - exret, ivol, R2, tvol
df['exret'] = df['exret'].replace('%','',regex=True).astype('float')/100
df['exret'] = pd.to_numeric(df['exret'])

df['ivol'] = df['ivol'].replace('%','',regex=True).astype('float')/100
df['ivol'] = pd.to_numeric(df['ivol'])

df['R2'] = df['R2'].replace('%','',regex=True).astype('float')/100
df['R2'] = pd.to_numeric(df['R2'])

df['tvol'] = df['tvol'].replace('%','',regex=True).astype('float')/100
df['tvol'] = pd.to_numeric(df['tvol'])

# df - Sort - Date and GVKEY
df = df.sort_values(by=['year-month','GVKEY'], ascending=[True,True])

# df - Clean - All - Fill N/A's
df = df.groupby(['GVKEY'], as_index=False).apply(lambda group: group.ffill())
df = df.groupby(['GVKEY'], as_index=False).apply(lambda group: group.bfill())
df = df.groupby(['sector'], as_index=False).apply(lambda group: group.ffill())
df = df.groupby(['sector'], as_index=False).apply(lambda group: group.bfill())
df = df.fillna(method='ffill')
df = df.fillna(method='bfill')

# df - Clean - Remove duplicate Time-based Unique Identifiers
df["GVKEY-year-month"] = df["GVKEY"].map(str) + "-" + df["year-month"]
df = df.drop_duplicates(['GVKEY-year-month'], keep='last')

# df - Clean - Clip outliers
# Dependent Variable
df['forward_one_month_return'] = df.groupby(['year-month', 'sector'])[['forward_one_month_return']].apply(clip_outliers)
df['forward_two_month_return'] = df.groupby(['year-month', 'sector'])[['forward_two_month_return']].apply(clip_outliers)
df['forward_three_month_return'] = df.groupby(['year-month', 'sector'])[['forward_three_month_return']].apply(clip_outliers)
df['forward_four_month_return'] = df.groupby(['year-month', 'sector'])[['forward_four_month_return']].apply(clip_outliers)
df['forward_five_month_return'] = df.groupby(['year-month', 'sector'])[['forward_five_month_return']].apply(clip_outliers)
df['forward_six_month_return'] = df.groupby(['year-month', 'sector'])[['forward_six_month_return']].apply(clip_outliers)
df['forward_seven_month_return'] = df.groupby(['year-month', 'sector'])[['forward_seven_month_return']].apply(clip_outliers)
df['forward_eight_month_return'] = df.groupby(['year-month', 'sector'])[['forward_eight_month_return']].apply(clip_outliers)
df['forward_nine_month_return'] = df.groupby(['year-month', 'sector'])[['forward_nine_month_return']].apply(clip_outliers)
df['forward_ten_month_return'] = df.groupby(['year-month', 'sector'])[['forward_ten_month_return']].apply(clip_outliers)
df['forward_eleven_month_return'] = df.groupby(['year-month', 'sector'])[['forward_eleven_month_return']].apply(clip_outliers)
df['forward_twelve_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twelve_month_return']].apply(clip_outliers)
df['forward_thirteen_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirteen_month_return']].apply(clip_outliers)
df['forward_fourteen_month_return'] = df.groupby(['year-month', 'sector'])[['forward_fourteen_month_return']].apply(clip_outliers)
df['forward_fifteen_month_return'] = df.groupby(['year-month', 'sector'])[['forward_fifteen_month_return']].apply(clip_outliers)
df['forward_sixteen_month_return'] = df.groupby(['year-month', 'sector'])[['forward_sixteen_month_return']].apply(clip_outliers)
df['forward_seventeen_month_return'] = df.groupby(['year-month', 'sector'])[['forward_seventeen_month_return']].apply(clip_outliers)
df['forward_eighteen_month_return'] = df.groupby(['year-month', 'sector'])[['forward_eighteen_month_return']].apply(clip_outliers)
df['forward_nineteen_month_return'] = df.groupby(['year-month', 'sector'])[['forward_nineteen_month_return']].apply(clip_outliers)
df['forward_twenty_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twenty_month_return']].apply(clip_outliers)
df['forward_twentyone_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentyone_month_return']].apply(clip_outliers)
df['forward_twentytwo_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentytwo_month_return']].apply(clip_outliers)
df['forward_twentythree_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentythree_month_return']].apply(clip_outliers)
df['forward_twentyfour_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentyfour_month_return']].apply(clip_outliers)
df['forward_twentyfive_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentyfive_month_return']].apply(clip_outliers)
df['forward_twentysix_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentysix_month_return']].apply(clip_outliers)
df['forward_twentyseven_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentyseven_month_return']].apply(clip_outliers)
df['forward_twentyeight_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentyeight_month_return']].apply(clip_outliers)
df['forward_twentynine_month_return'] = df.groupby(['year-month', 'sector'])[['forward_twentynine_month_return']].apply(clip_outliers)
df['forward_thirty_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirty_month_return']].apply(clip_outliers)
df['forward_thirtyone_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirtyone_month_return']].apply(clip_outliers)
df['forward_thirtytwo_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirtytwo_month_return']].apply(clip_outliers)
df['forward_thirtythree_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirtythree_month_return']].apply(clip_outliers)
df['forward_thirtyfour_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirtyfour_month_return']].apply(clip_outliers)
df['forward_thirtyfive_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirtyfive_month_return']].apply(clip_outliers)
df['forward_thirtysix_month_return'] = df.groupby(['year-month', 'sector'])[['forward_thirtysix_month_return']].apply(clip_outliers)

# Independent Variable
df['past_one_month_return'] = df.groupby(['year-month', 'sector'])[['past_one_month_return']].apply(clip_outliers)
df['past_two_month_return'] = df.groupby(['year-month', 'sector'])[['past_two_month_return']].apply(clip_outliers)
df['past_three_month_return'] = df.groupby(['year-month', 'sector'])[['past_three_month_return']].apply(clip_outliers)
df['past_four_month_return'] = df.groupby(['year-month', 'sector'])[['past_four_month_return']].apply(clip_outliers)
df['past_five_month_return'] = df.groupby(['year-month', 'sector'])[['past_five_month_return']].apply(clip_outliers)
df['past_six_month_return'] = df.groupby(['year-month', 'sector'])[['past_six_month_return']].apply(clip_outliers)
df['past_seven_month_return'] = df.groupby(['year-month', 'sector'])[['past_seven_month_return']].apply(clip_outliers)
df['past_eight_month_return'] = df.groupby(['year-month', 'sector'])[['past_eight_month_return']].apply(clip_outliers)
df['past_nine_month_return'] = df.groupby(['year-month', 'sector'])[['past_nine_month_return']].apply(clip_outliers)
df['past_ten_month_return'] = df.groupby(['year-month', 'sector'])[['past_ten_month_return']].apply(clip_outliers)
df['past_eleven_month_return'] = df.groupby(['year-month', 'sector'])[['past_eleven_month_return']].apply(clip_outliers)
df['past_twelve_month_return'] = df.groupby(['year-month', 'sector'])[['past_twelve_month_return']].apply(clip_outliers)
df['past_thirteen_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirteen_month_return']].apply(clip_outliers)
df['past_fourteen_month_return'] = df.groupby(['year-month', 'sector'])[['past_fourteen_month_return']].apply(clip_outliers)
df['past_fifteen_month_return'] = df.groupby(['year-month', 'sector'])[['past_fifteen_month_return']].apply(clip_outliers)
df['past_sixteen_month_return'] = df.groupby(['year-month', 'sector'])[['past_sixteen_month_return']].apply(clip_outliers)
df['past_seventeen_month_return'] = df.groupby(['year-month', 'sector'])[['past_seventeen_month_return']].apply(clip_outliers)
df['past_eighteen_month_return'] = df.groupby(['year-month', 'sector'])[['past_eighteen_month_return']].apply(clip_outliers)
df['past_nineteen_month_return'] = df.groupby(['year-month', 'sector'])[['past_nineteen_month_return']].apply(clip_outliers)
df['past_twenty_month_return'] = df.groupby(['year-month', 'sector'])[['past_twenty_month_return']].apply(clip_outliers)
df['past_twentyone_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentyone_month_return']].apply(clip_outliers)
df['past_twentytwo_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentytwo_month_return']].apply(clip_outliers)
df['past_twentythree_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentythree_month_return']].apply(clip_outliers)
df['past_twentyfour_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentyfour_month_return']].apply(clip_outliers)
df['past_twentyfive_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentyfive_month_return']].apply(clip_outliers)
df['past_twentysix_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentysix_month_return']].apply(clip_outliers)
df['past_twentyseven_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentyseven_month_return']].apply(clip_outliers)
df['past_twentyeight_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentyeight_month_return']].apply(clip_outliers)
df['past_twentynine_month_return'] = df.groupby(['year-month', 'sector'])[['past_twentynine_month_return']].apply(clip_outliers)
df['past_thirty_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirty_month_return']].apply(clip_outliers)
df['past_thirtyone_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirtyone_month_return']].apply(clip_outliers)
df['past_thirtytwo_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirtytwo_month_return']].apply(clip_outliers)
df['past_thirtythree_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirtythree_month_return']].apply(clip_outliers)
df['past_thirtyfour_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirtyfour_month_return']].apply(clip_outliers)
df['past_thirtyfive_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirtyfive_month_return']].apply(clip_outliers)
df['past_thirtysix_month_return'] = df.groupby(['year-month', 'sector'])[['past_thirtysix_month_return']].apply(clip_outliers)
df['accrual'] = df.groupby(['year-month', 'sector'])[['accrual']].apply(clip_outliers)
df['adv_sale'] = df.groupby(['year-month', 'sector'])[['adv_sale']].apply(clip_outliers)
df['aftret_eq'] = df.groupby(['year-month', 'sector'])[['aftret_eq']].apply(clip_outliers)
df['aftret_equity'] = df.groupby(['year-month', 'sector'])[['aftret_equity']].apply(clip_outliers)
df['aftret_invcapx'] = df.groupby(['year-month', 'sector'])[['aftret_invcapx']].apply(clip_outliers)
df['at_turn'] = df.groupby(['year-month', 'sector'])[['at_turn']].apply(clip_outliers)
df['bm'] = df.groupby(['year-month', 'sector'])[['bm']].apply(clip_outliers)
df['CAPEI'] = df.groupby(['year-month', 'sector'])[['CAPEI']].apply(clip_outliers)
df['capital_ratio'] = df.groupby(['year-month', 'sector'])[['capital_ratio']].apply(clip_outliers)
df['cash_conversion'] = df.groupby(['year-month', 'sector'])[['cash_conversion']].apply(clip_outliers)
df['cash_debt'] = df.groupby(['year-month', 'sector'])[['cash_debt']].apply(clip_outliers)
df['cash_lt'] = df.groupby(['year-month', 'sector'])[['cash_lt']].apply(clip_outliers)
df['cash_ratio'] = df.groupby(['year-month', 'sector'])[['cash_ratio']].apply(clip_outliers)
df['cfm'] = df.groupby(['year-month', 'sector'])[['cfm']].apply(clip_outliers)
df['curr_debt'] = df.groupby(['year-month', 'sector'])[['curr_debt']].apply(clip_outliers)
df['curr_ratio'] = df.groupby(['year-month', 'sector'])[['curr_ratio']].apply(clip_outliers)
df['de_ratio'] = df.groupby(['year-month', 'sector'])[['de_ratio']].apply(clip_outliers)
df['debt_assets'] = df.groupby(['year-month', 'sector'])[['debt_assets']].apply(clip_outliers)
df['debt_at'] = df.groupby(['year-month', 'sector'])[['debt_at']].apply(clip_outliers)
df['debt_capital'] = df.groupby(['year-month', 'sector'])[['debt_capital']].apply(clip_outliers)
df['debt_ebitda'] = df.groupby(['year-month', 'sector'])[['debt_ebitda']].apply(clip_outliers)
df['debt_invcap'] = df.groupby(['year-month', 'sector'])[['debt_invcap']].apply(clip_outliers)
df['DIVYIELD'] = df.groupby(['year-month', 'sector'])[['DIVYIELD']].apply(clip_outliers)
df['dltt_be'] = df.groupby(['year-month', 'sector'])[['dltt_be']].apply(clip_outliers)
df['dpr'] = df.groupby(['year-month', 'sector'])[['dpr']].apply(clip_outliers)
df['efftax'] = df.groupby(['year-month', 'sector'])[['efftax']].apply(clip_outliers)
df['equity_invcap'] = df.groupby(['year-month', 'sector'])[['equity_invcap']].apply(clip_outliers)
df['evm'] = df.groupby(['year-month', 'sector'])[['evm']].apply(clip_outliers)
df['fcf_ocf'] = df.groupby(['year-month', 'sector'])[['fcf_ocf']].apply(clip_outliers)
df['gpm'] = df.groupby(['year-month', 'sector'])[['gpm']].apply(clip_outliers)
df['GProf'] = df.groupby(['year-month', 'sector'])[['GProf']].apply(clip_outliers)
df['int_debt'] = df.groupby(['year-month', 'sector'])[['int_debt']].apply(clip_outliers)
df['int_totdebt'] = df.groupby(['year-month', 'sector'])[['int_totdebt']].apply(clip_outliers)
df['intcov'] = df.groupby(['year-month', 'sector'])[['intcov']].apply(clip_outliers)
df['intcov_ratio'] = df.groupby(['year-month', 'sector'])[['intcov_ratio']].apply(clip_outliers)
df['inv_turn'] = df.groupby(['year-month', 'sector'])[['inv_turn']].apply(clip_outliers)
df['invt_act'] = df.groupby(['year-month', 'sector'])[['invt_act']].apply(clip_outliers)
df['lt_debt'] = df.groupby(['year-month', 'sector'])[['lt_debt']].apply(clip_outliers)
df['lt_ppent'] = df.groupby(['year-month', 'sector'])[['lt_ppent']].apply(clip_outliers)
df['npm'] = df.groupby(['year-month', 'sector'])[['npm']].apply(clip_outliers)
df['ocf_lct'] = df.groupby(['year-month', 'sector'])[['ocf_lct']].apply(clip_outliers)
df['opmad'] = df.groupby(['year-month', 'sector'])[['opmad']].apply(clip_outliers)
df['opmbd'] = df.groupby(['year-month', 'sector'])[['opmbd']].apply(clip_outliers)
df['pay_turn'] = df.groupby(['year-month', 'sector'])[['pay_turn']].apply(clip_outliers)
df['pcf'] = df.groupby(['year-month', 'sector'])[['pcf']].apply(clip_outliers)
df['pe_exi'] = df.groupby(['year-month', 'sector'])[['pe_exi']].apply(clip_outliers)
df['pe_inc'] = df.groupby(['year-month', 'sector'])[['pe_inc']].apply(clip_outliers)
df['pe_op_basic'] = df.groupby(['year-month', 'sector'])[['pe_op_basic']].apply(clip_outliers)
df['pe_op_dil'] = df.groupby(['year-month', 'sector'])[['pe_op_dil']].apply(clip_outliers)
df['PEG_1yrforward'] = df.groupby(['year-month', 'sector'])[['PEG_1yrforward']].apply(clip_outliers)
df['PEG_ltgforward'] = df.groupby(['year-month', 'sector'])[['PEG_ltgforward']].apply(clip_outliers)
df['PEG_trailing'] = df.groupby(['year-month', 'sector'])[['PEG_trailing']].apply(clip_outliers)
df['pretret_earnat'] = df.groupby(['year-month', 'sector'])[['pretret_earnat']].apply(clip_outliers)
df['pretret_noa'] = df.groupby(['year-month', 'sector'])[['pretret_noa']].apply(clip_outliers)
df['profit_lct'] = df.groupby(['year-month', 'sector'])[['profit_lct']].apply(clip_outliers)
df['ps'] = df.groupby(['year-month', 'sector'])[['ps']].apply(clip_outliers)
df['ptb'] = df.groupby(['year-month', 'sector'])[['ptb']].apply(clip_outliers)
df['ptpm'] = df.groupby(['year-month', 'sector'])[['ptpm']].apply(clip_outliers)
df['quick_ratio'] = df.groupby(['year-month', 'sector'])[['quick_ratio']].apply(clip_outliers)
df['rd_sale'] = df.groupby(['year-month', 'sector'])[['rd_sale']].apply(clip_outliers)
df['rect_act'] = df.groupby(['year-month', 'sector'])[['rect_act']].apply(clip_outliers)
df['rect_turn'] = df.groupby(['year-month', 'sector'])[['rect_turn']].apply(clip_outliers)
df['roa'] = df.groupby(['year-month', 'sector'])[['roa']].apply(clip_outliers)
df['roce'] = df.groupby(['year-month', 'sector'])[['roce']].apply(clip_outliers)
df['roe'] = df.groupby(['year-month', 'sector'])[['roe']].apply(clip_outliers)
df['sale_equity'] = df.groupby(['year-month', 'sector'])[['sale_equity']].apply(clip_outliers)
df['sale_invcap'] = df.groupby(['year-month', 'sector'])[['sale_invcap']].apply(clip_outliers)
df['sale_nwc'] = df.groupby(['year-month', 'sector'])[['sale_nwc']].apply(clip_outliers)
df['short_debt'] = df.groupby(['year-month', 'sector'])[['short_debt']].apply(clip_outliers)
df['staff_sale'] = df.groupby(['year-month', 'sector'])[['staff_sale']].apply(clip_outliers)
df['totdebt_invcap'] = df.groupby(['year-month', 'sector'])[['totdebt_invcap']].apply(clip_outliers)
df['dvpspm'] = df.groupby(['year-month', 'sector'])[['dvpspm']].apply(clip_outliers)
df['dvpsxm'] = df.groupby(['year-month', 'sector'])[['dvpsxm']].apply(clip_outliers)
df['dvrate'] = df.groupby(['year-month', 'sector'])[['dvrate']].apply(clip_outliers)
df['spcsrc'] = df.groupby(['year-month', 'sector'])[['spcsrc']].apply(clip_outliers)
df['alpha'] = df.groupby(['year-month', 'sector'])[['alpha']].apply(clip_outliers)
df['b_hml'] = df.groupby(['year-month', 'sector'])[['b_hml']].apply(clip_outliers)
df['b_mkt'] = df.groupby(['year-month', 'sector'])[['b_mkt']].apply(clip_outliers)
df['b_smb'] = df.groupby(['year-month', 'sector'])[['b_smb']].apply(clip_outliers)
df['b_umd'] = df.groupby(['year-month', 'sector'])[['b_umd']].apply(clip_outliers)
df['exret'] = df.groupby(['year-month', 'sector'])[['exret']].apply(clip_outliers)
df['ivol'] = df.groupby(['year-month', 'sector'])[['ivol']].apply(clip_outliers)
df['n'] = df.groupby(['year-month', 'sector'])[['n']].apply(clip_outliers)
df['R2'] = df.groupby(['year-month', 'sector'])[['R2']].apply(clip_outliers)
df['tvol'] = df.groupby(['year-month', 'sector'])[['tvol']].apply(clip_outliers)
df['BUYPCT'] = df.groupby(['year-month', 'sector'])[['BUYPCT']].apply(clip_outliers)
df['HOLDPCT'] = df.groupby(['year-month', 'sector'])[['HOLDPCT']].apply(clip_outliers)
df['SELLPCT'] = df.groupby(['year-month', 'sector'])[['SELLPCT']].apply(clip_outliers)
df['MEANREC'] = df.groupby(['year-month', 'sector'])[['MEANREC']].apply(clip_outliers)
df['MEDREC'] = df.groupby(['year-month', 'sector'])[['MEDREC']].apply(clip_outliers)
df['recup'] = df.groupby(['year-month', 'sector'])[['recup']].apply(clip_outliers)
df['recdown'] = df.groupby(['year-month', 'sector'])[['recdown']].apply(clip_outliers)
df['STDEV'] = df.groupby(['year-month', 'sector'])[['STDEV']].apply(clip_outliers)

# Clip by month for whole market
# Dependent Variable
df['forward_one_month_return'] = df.groupby(['year-month'])[['forward_one_month_return']].apply(clip_outliers)
df['forward_two_month_return'] = df.groupby(['year-month'])[['forward_two_month_return']].apply(clip_outliers)
df['forward_three_month_return'] = df.groupby(['year-month'])[['forward_three_month_return']].apply(clip_outliers)
df['forward_four_month_return'] = df.groupby(['year-month'])[['forward_four_month_return']].apply(clip_outliers)
df['forward_five_month_return'] = df.groupby(['year-month'])[['forward_five_month_return']].apply(clip_outliers)
df['forward_six_month_return'] = df.groupby(['year-month'])[['forward_six_month_return']].apply(clip_outliers)
df['forward_seven_month_return'] = df.groupby(['year-month'])[['forward_seven_month_return']].apply(clip_outliers)
df['forward_eight_month_return'] = df.groupby(['year-month'])[['forward_eight_month_return']].apply(clip_outliers)
df['forward_nine_month_return'] = df.groupby(['year-month'])[['forward_nine_month_return']].apply(clip_outliers)
df['forward_ten_month_return'] = df.groupby(['year-month'])[['forward_ten_month_return']].apply(clip_outliers)
df['forward_eleven_month_return'] = df.groupby(['year-month'])[['forward_eleven_month_return']].apply(clip_outliers)
df['forward_twelve_month_return'] = df.groupby(['year-month'])[['forward_twelve_month_return']].apply(clip_outliers)
df['forward_thirteen_month_return'] = df.groupby(['year-month'])[['forward_thirteen_month_return']].apply(clip_outliers)
df['forward_fourteen_month_return'] = df.groupby(['year-month'])[['forward_fourteen_month_return']].apply(clip_outliers)
df['forward_fifteen_month_return'] = df.groupby(['year-month'])[['forward_fifteen_month_return']].apply(clip_outliers)
df['forward_sixteen_month_return'] = df.groupby(['year-month'])[['forward_sixteen_month_return']].apply(clip_outliers)
df['forward_seventeen_month_return'] = df.groupby(['year-month'])[['forward_seventeen_month_return']].apply(clip_outliers)
df['forward_eighteen_month_return'] = df.groupby(['year-month'])[['forward_eighteen_month_return']].apply(clip_outliers)
df['forward_nineteen_month_return'] = df.groupby(['year-month'])[['forward_nineteen_month_return']].apply(clip_outliers)
df['forward_twenty_month_return'] = df.groupby(['year-month'])[['forward_twenty_month_return']].apply(clip_outliers)
df['forward_twentyone_month_return'] = df.groupby(['year-month'])[['forward_twentyone_month_return']].apply(clip_outliers)
df['forward_twentytwo_month_return'] = df.groupby(['year-month'])[['forward_twentytwo_month_return']].apply(clip_outliers)
df['forward_twentythree_month_return'] = df.groupby(['year-month'])[['forward_twentythree_month_return']].apply(clip_outliers)
df['forward_twentyfour_month_return'] = df.groupby(['year-month'])[['forward_twentyfour_month_return']].apply(clip_outliers)
df['forward_twentyfive_month_return'] = df.groupby(['year-month'])[['forward_twentyfive_month_return']].apply(clip_outliers)
df['forward_twentysix_month_return'] = df.groupby(['year-month'])[['forward_twentysix_month_return']].apply(clip_outliers)
df['forward_twentyseven_month_return'] = df.groupby(['year-month'])[['forward_twentyseven_month_return']].apply(clip_outliers)
df['forward_twentyeight_month_return'] = df.groupby(['year-month'])[['forward_twentyeight_month_return']].apply(clip_outliers)
df['forward_twentynine_month_return'] = df.groupby(['year-month'])[['forward_twentynine_month_return']].apply(clip_outliers)
df['forward_thirty_month_return'] = df.groupby(['year-month'])[['forward_thirty_month_return']].apply(clip_outliers)
df['forward_thirtyone_month_return'] = df.groupby(['year-month'])[['forward_thirtyone_month_return']].apply(clip_outliers)
df['forward_thirtytwo_month_return'] = df.groupby(['year-month'])[['forward_thirtytwo_month_return']].apply(clip_outliers)
df['forward_thirtythree_month_return'] = df.groupby(['year-month'])[['forward_thirtythree_month_return']].apply(clip_outliers)
df['forward_thirtyfour_month_return'] = df.groupby(['year-month'])[['forward_thirtyfour_month_return']].apply(clip_outliers)
df['forward_thirtyfive_month_return'] = df.groupby(['year-month'])[['forward_thirtyfive_month_return']].apply(clip_outliers)
df['forward_thirtysix_month_return'] = df.groupby(['year-month'])[['forward_thirtysix_month_return']].apply(clip_outliers)

# Independent Variable
df['past_one_month_return'] = df.groupby(['year-month'])[['past_one_month_return']].apply(clip_outliers)
df['past_two_month_return'] = df.groupby(['year-month'])[['past_two_month_return']].apply(clip_outliers)
df['past_three_month_return'] = df.groupby(['year-month'])[['past_three_month_return']].apply(clip_outliers)
df['past_four_month_return'] = df.groupby(['year-month'])[['past_four_month_return']].apply(clip_outliers)
df['past_five_month_return'] = df.groupby(['year-month'])[['past_five_month_return']].apply(clip_outliers)
df['past_six_month_return'] = df.groupby(['year-month'])[['past_six_month_return']].apply(clip_outliers)
df['past_seven_month_return'] = df.groupby(['year-month'])[['past_seven_month_return']].apply(clip_outliers)
df['past_eight_month_return'] = df.groupby(['year-month'])[['past_eight_month_return']].apply(clip_outliers)
df['past_nine_month_return'] = df.groupby(['year-month'])[['past_nine_month_return']].apply(clip_outliers)
df['past_ten_month_return'] = df.groupby(['year-month'])[['past_ten_month_return']].apply(clip_outliers)
df['past_eleven_month_return'] = df.groupby(['year-month'])[['past_eleven_month_return']].apply(clip_outliers)
df['past_twelve_month_return'] = df.groupby(['year-month'])[['past_twelve_month_return']].apply(clip_outliers)
df['past_thirteen_month_return'] = df.groupby(['year-month'])[['past_thirteen_month_return']].apply(clip_outliers)
df['past_fourteen_month_return'] = df.groupby(['year-month'])[['past_fourteen_month_return']].apply(clip_outliers)
df['past_fifteen_month_return'] = df.groupby(['year-month'])[['past_fifteen_month_return']].apply(clip_outliers)
df['past_sixteen_month_return'] = df.groupby(['year-month'])[['past_sixteen_month_return']].apply(clip_outliers)
df['past_seventeen_month_return'] = df.groupby(['year-month'])[['past_seventeen_month_return']].apply(clip_outliers)
df['past_eighteen_month_return'] = df.groupby(['year-month'])[['past_eighteen_month_return']].apply(clip_outliers)
df['past_nineteen_month_return'] = df.groupby(['year-month'])[['past_nineteen_month_return']].apply(clip_outliers)
df['past_twenty_month_return'] = df.groupby(['year-month'])[['past_twenty_month_return']].apply(clip_outliers)
df['past_twentyone_month_return'] = df.groupby(['year-month'])[['past_twentyone_month_return']].apply(clip_outliers)
df['past_twentytwo_month_return'] = df.groupby(['year-month'])[['past_twentytwo_month_return']].apply(clip_outliers)
df['past_twentythree_month_return'] = df.groupby(['year-month'])[['past_twentythree_month_return']].apply(clip_outliers)
df['past_twentyfour_month_return'] = df.groupby(['year-month'])[['past_twentyfour_month_return']].apply(clip_outliers)
df['past_twentyfive_month_return'] = df.groupby(['year-month'])[['past_twentyfive_month_return']].apply(clip_outliers)
df['past_twentysix_month_return'] = df.groupby(['year-month'])[['past_twentysix_month_return']].apply(clip_outliers)
df['past_twentyseven_month_return'] = df.groupby(['year-month'])[['past_twentyseven_month_return']].apply(clip_outliers)
df['past_twentyeight_month_return'] = df.groupby(['year-month'])[['past_twentyeight_month_return']].apply(clip_outliers)
df['past_twentynine_month_return'] = df.groupby(['year-month'])[['past_twentynine_month_return']].apply(clip_outliers)
df['past_thirty_month_return'] = df.groupby(['year-month'])[['past_thirty_month_return']].apply(clip_outliers)
df['past_thirtyone_month_return'] = df.groupby(['year-month'])[['past_thirtyone_month_return']].apply(clip_outliers)
df['past_thirtytwo_month_return'] = df.groupby(['year-month'])[['past_thirtytwo_month_return']].apply(clip_outliers)
df['past_thirtythree_month_return'] = df.groupby(['year-month'])[['past_thirtythree_month_return']].apply(clip_outliers)
df['past_thirtyfour_month_return'] = df.groupby(['year-month'])[['past_thirtyfour_month_return']].apply(clip_outliers)
df['past_thirtyfive_month_return'] = df.groupby(['year-month'])[['past_thirtyfive_month_return']].apply(clip_outliers)
df['past_thirtysix_month_return'] = df.groupby(['year-month'])[['past_thirtysix_month_return']].apply(clip_outliers)
df['accrual'] = df.groupby(['year-month'])[['accrual']].apply(clip_outliers)
df['adv_sale'] = df.groupby(['year-month'])[['adv_sale']].apply(clip_outliers)
df['aftret_eq'] = df.groupby(['year-month'])[['aftret_eq']].apply(clip_outliers)
df['aftret_equity'] = df.groupby(['year-month'])[['aftret_equity']].apply(clip_outliers)
df['aftret_invcapx'] = df.groupby(['year-month'])[['aftret_invcapx']].apply(clip_outliers)
df['at_turn'] = df.groupby(['year-month'])[['at_turn']].apply(clip_outliers)
df['bm'] = df.groupby(['year-month'])[['bm']].apply(clip_outliers)
df['CAPEI'] = df.groupby(['year-month'])[['CAPEI']].apply(clip_outliers)
df['capital_ratio'] = df.groupby(['year-month'])[['capital_ratio']].apply(clip_outliers)
df['cash_conversion'] = df.groupby(['year-month'])[['cash_conversion']].apply(clip_outliers)
df['cash_debt'] = df.groupby(['year-month'])[['cash_debt']].apply(clip_outliers)
df['cash_lt'] = df.groupby(['year-month'])[['cash_lt']].apply(clip_outliers)
df['cash_ratio'] = df.groupby(['year-month'])[['cash_ratio']].apply(clip_outliers)
df['cfm'] = df.groupby(['year-month'])[['cfm']].apply(clip_outliers)
df['curr_debt'] = df.groupby(['year-month'])[['curr_debt']].apply(clip_outliers)
df['curr_ratio'] = df.groupby(['year-month'])[['curr_ratio']].apply(clip_outliers)
df['de_ratio'] = df.groupby(['year-month'])[['de_ratio']].apply(clip_outliers)
df['debt_assets'] = df.groupby(['year-month'])[['debt_assets']].apply(clip_outliers)
df['debt_at'] = df.groupby(['year-month'])[['debt_at']].apply(clip_outliers)
df['debt_capital'] = df.groupby(['year-month'])[['debt_capital']].apply(clip_outliers)
df['debt_ebitda'] = df.groupby(['year-month'])[['debt_ebitda']].apply(clip_outliers)
df['debt_invcap'] = df.groupby(['year-month'])[['debt_invcap']].apply(clip_outliers)
df['DIVYIELD'] = df.groupby(['year-month'])[['DIVYIELD']].apply(clip_outliers)
df['dltt_be'] = df.groupby(['year-month'])[['dltt_be']].apply(clip_outliers)
df['dpr'] = df.groupby(['year-month'])[['dpr']].apply(clip_outliers)
df['efftax'] = df.groupby(['year-month'])[['efftax']].apply(clip_outliers)
df['equity_invcap'] = df.groupby(['year-month'])[['equity_invcap']].apply(clip_outliers)
df['evm'] = df.groupby(['year-month'])[['evm']].apply(clip_outliers)
df['fcf_ocf'] = df.groupby(['year-month'])[['fcf_ocf']].apply(clip_outliers)
df['gpm'] = df.groupby(['year-month'])[['gpm']].apply(clip_outliers)
df['GProf'] = df.groupby(['year-month'])[['GProf']].apply(clip_outliers)
df['int_debt'] = df.groupby(['year-month'])[['int_debt']].apply(clip_outliers)
df['int_totdebt'] = df.groupby(['year-month'])[['int_totdebt']].apply(clip_outliers)
df['intcov'] = df.groupby(['year-month'])[['intcov']].apply(clip_outliers)
df['intcov_ratio'] = df.groupby(['year-month'])[['intcov_ratio']].apply(clip_outliers)
df['inv_turn'] = df.groupby(['year-month'])[['inv_turn']].apply(clip_outliers)
df['invt_act'] = df.groupby(['year-month'])[['invt_act']].apply(clip_outliers)
df['lt_debt'] = df.groupby(['year-month'])[['lt_debt']].apply(clip_outliers)
df['lt_ppent'] = df.groupby(['year-month'])[['lt_ppent']].apply(clip_outliers)
df['npm'] = df.groupby(['year-month'])[['npm']].apply(clip_outliers)
df['ocf_lct'] = df.groupby(['year-month'])[['ocf_lct']].apply(clip_outliers)
df['opmad'] = df.groupby(['year-month'])[['opmad']].apply(clip_outliers)
df['opmbd'] = df.groupby(['year-month'])[['opmbd']].apply(clip_outliers)
df['pay_turn'] = df.groupby(['year-month'])[['pay_turn']].apply(clip_outliers)
df['pcf'] = df.groupby(['year-month'])[['pcf']].apply(clip_outliers)
df['pe_exi'] = df.groupby(['year-month'])[['pe_exi']].apply(clip_outliers)
df['pe_inc'] = df.groupby(['year-month'])[['pe_inc']].apply(clip_outliers)
df['pe_op_basic'] = df.groupby(['year-month'])[['pe_op_basic']].apply(clip_outliers)
df['pe_op_dil'] = df.groupby(['year-month'])[['pe_op_dil']].apply(clip_outliers)
df['PEG_1yrforward'] = df.groupby(['year-month'])[['PEG_1yrforward']].apply(clip_outliers)
df['PEG_ltgforward'] = df.groupby(['year-month'])[['PEG_ltgforward']].apply(clip_outliers)
df['PEG_trailing'] = df.groupby(['year-month'])[['PEG_trailing']].apply(clip_outliers)
df['pretret_earnat'] = df.groupby(['year-month'])[['pretret_earnat']].apply(clip_outliers)
df['pretret_noa'] = df.groupby(['year-month'])[['pretret_noa']].apply(clip_outliers)
df['profit_lct'] = df.groupby(['year-month'])[['profit_lct']].apply(clip_outliers)
df['ps'] = df.groupby(['year-month'])[['ps']].apply(clip_outliers)
df['ptb'] = df.groupby(['year-month'])[['ptb']].apply(clip_outliers)
df['ptpm'] = df.groupby(['year-month'])[['ptpm']].apply(clip_outliers)
df['quick_ratio'] = df.groupby(['year-month'])[['quick_ratio']].apply(clip_outliers)
df['rd_sale'] = df.groupby(['year-month'])[['rd_sale']].apply(clip_outliers)
df['rect_act'] = df.groupby(['year-month'])[['rect_act']].apply(clip_outliers)
df['rect_turn'] = df.groupby(['year-month'])[['rect_turn']].apply(clip_outliers)
df['roa'] = df.groupby(['year-month'])[['roa']].apply(clip_outliers)
df['roce'] = df.groupby(['year-month'])[['roce']].apply(clip_outliers)
df['roe'] = df.groupby(['year-month'])[['roe']].apply(clip_outliers)
df['sale_equity'] = df.groupby(['year-month'])[['sale_equity']].apply(clip_outliers)
df['sale_invcap'] = df.groupby(['year-month'])[['sale_invcap']].apply(clip_outliers)
df['sale_nwc'] = df.groupby(['year-month'])[['sale_nwc']].apply(clip_outliers)
df['short_debt'] = df.groupby(['year-month'])[['short_debt']].apply(clip_outliers)
df['staff_sale'] = df.groupby(['year-month'])[['staff_sale']].apply(clip_outliers)
df['totdebt_invcap'] = df.groupby(['year-month'])[['totdebt_invcap']].apply(clip_outliers)
df['dvpspm'] = df.groupby(['year-month'])[['dvpspm']].apply(clip_outliers)
df['dvpsxm'] = df.groupby(['year-month'])[['dvpsxm']].apply(clip_outliers)
df['dvrate'] = df.groupby(['year-month'])[['dvrate']].apply(clip_outliers)
df['spcsrc'] = df.groupby(['year-month'])[['spcsrc']].apply(clip_outliers)
df['alpha'] = df.groupby(['year-month'])[['alpha']].apply(clip_outliers)
df['b_hml'] = df.groupby(['year-month'])[['b_hml']].apply(clip_outliers)
df['b_mkt'] = df.groupby(['year-month'])[['b_mkt']].apply(clip_outliers)
df['b_smb'] = df.groupby(['year-month'])[['b_smb']].apply(clip_outliers)
df['b_umd'] = df.groupby(['year-month'])[['b_umd']].apply(clip_outliers)
df['exret'] = df.groupby(['year-month'])[['exret']].apply(clip_outliers)
df['ivol'] = df.groupby(['year-month'])[['ivol']].apply(clip_outliers)
df['n'] = df.groupby(['year-month'])[['n']].apply(clip_outliers)
df['R2'] = df.groupby(['year-month'])[['R2']].apply(clip_outliers)
df['tvol'] = df.groupby(['year-month'])[['tvol']].apply(clip_outliers)
df['BUYPCT'] = df.groupby(['year-month'])[['BUYPCT']].apply(clip_outliers)
df['HOLDPCT'] = df.groupby(['year-month'])[['HOLDPCT']].apply(clip_outliers)
df['SELLPCT'] = df.groupby(['year-month'])[['SELLPCT']].apply(clip_outliers)
df['MEANREC'] = df.groupby(['year-month'])[['MEANREC']].apply(clip_outliers)
df['MEDREC'] = df.groupby(['year-month'])[['MEDREC']].apply(clip_outliers)
df['recup'] = df.groupby(['year-month'])[['recup']].apply(clip_outliers)
df['recdown'] = df.groupby(['year-month'])[['recdown']].apply(clip_outliers)
df['STDEV'] = df.groupby(['year-month'])[['STDEV']].apply(clip_outliers)

# df - Enrich - Market Z-Score
df['past_one_month_return_zscore'] = df.groupby(['year-month'])[['past_one_month_return']].apply(modified_z)
df['past_two_month_return_zscore'] = df.groupby(['year-month'])[['past_two_month_return']].apply(modified_z)
df['past_three_month_return_zscore'] = df.groupby(['year-month'])[['past_three_month_return']].apply(modified_z)
df['past_four_month_return_zscore'] = df.groupby(['year-month'])[['past_four_month_return']].apply(modified_z)
df['past_five_month_return_zscore'] = df.groupby(['year-month'])[['past_five_month_return']].apply(modified_z)
df['past_six_month_return_zscore'] = df.groupby(['year-month'])[['past_six_month_return']].apply(modified_z)
df['past_seven_month_return_zscore'] = df.groupby(['year-month'])[['past_seven_month_return']].apply(modified_z)
df['past_eight_month_return_zscore'] = df.groupby(['year-month'])[['past_eight_month_return']].apply(modified_z)
df['past_nine_month_return_zscore'] = df.groupby(['year-month'])[['past_nine_month_return']].apply(modified_z)
df['past_ten_month_return_zscore'] = df.groupby(['year-month'])[['past_ten_month_return']].apply(modified_z)
df['past_eleven_month_return_zscore'] = df.groupby(['year-month'])[['past_eleven_month_return']].apply(modified_z)
df['past_twelve_month_return_zscore'] = df.groupby(['year-month'])[['past_twelve_month_return']].apply(modified_z)
df['past_thirteen_month_return_zscore'] = df.groupby(['year-month'])[['past_thirteen_month_return']].apply(modified_z)
df['past_fourteen_month_return_zscore'] = df.groupby(['year-month'])[['past_fourteen_month_return']].apply(modified_z)
df['past_fifteen_month_return_zscore'] = df.groupby(['year-month'])[['past_fifteen_month_return']].apply(modified_z)
df['past_sixteen_month_return_zscore'] = df.groupby(['year-month'])[['past_sixteen_month_return']].apply(modified_z)
df['past_seventeen_month_return_zscore'] = df.groupby(['year-month'])[['past_seventeen_month_return']].apply(modified_z)
df['past_eighteen_month_return_zscore'] = df.groupby(['year-month'])[['past_eighteen_month_return']].apply(modified_z)
df['past_nineteen_month_return_zscore'] = df.groupby(['year-month'])[['past_nineteen_month_return']].apply(modified_z)
df['past_twenty_month_return_zscore'] = df.groupby(['year-month'])[['past_twenty_month_return']].apply(modified_z)
df['past_twentyone_month_return_zscore'] = df.groupby(['year-month'])[['past_twentyone_month_return']].apply(modified_z)
df['past_twentytwo_month_return_zscore'] = df.groupby(['year-month'])[['past_twentytwo_month_return']].apply(modified_z)
df['past_twentythree_month_return_zscore'] = df.groupby(['year-month'])[['past_twentythree_month_return']].apply(modified_z)
df['past_twentyfour_month_return_zscore'] = df.groupby(['year-month'])[['past_twentyfour_month_return']].apply(modified_z)
df['past_twentyfive_month_return_zscore'] = df.groupby(['year-month'])[['past_twentyfive_month_return']].apply(modified_z)
df['past_twentysix_month_return_zscore'] = df.groupby(['year-month'])[['past_twentysix_month_return']].apply(modified_z)
df['past_twentyseven_month_return_zscore'] = df.groupby(['year-month'])[['past_twentyseven_month_return']].apply(modified_z)
df['past_twentyeight_month_return_zscore'] = df.groupby(['year-month'])[['past_twentyeight_month_return']].apply(modified_z)
df['past_twentynine_month_return_zscore'] = df.groupby(['year-month'])[['past_twentynine_month_return']].apply(modified_z)
df['past_thirty_month_return_zscore'] = df.groupby(['year-month'])[['past_thirty_month_return']].apply(modified_z)
df['past_thirtyone_month_return_zscore'] = df.groupby(['year-month'])[['past_thirtyone_month_return']].apply(modified_z)
df['past_thirtytwo_month_return_zscore'] = df.groupby(['year-month'])[['past_thirtytwo_month_return']].apply(modified_z)
df['past_thirtythree_month_return_zscore'] = df.groupby(['year-month'])[['past_thirtythree_month_return']].apply(modified_z)
df['past_thirtyfour_month_return_zscore'] = df.groupby(['year-month'])[['past_thirtyfour_month_return']].apply(modified_z)
df['past_thirtyfive_month_return_zscore'] = df.groupby(['year-month'])[['past_thirtyfive_month_return']].apply(modified_z)
df['past_thirtysix_month_return_zscore'] = df.groupby(['year-month'])[['past_thirtysix_month_return']].apply(modified_z)
df['accrual_zscore'] = df.groupby(['year-month'])[['accrual']].apply(modified_z)
df['adv_sale_zscore'] = df.groupby(['year-month'])[['adv_sale']].apply(modified_z)
df['aftret_eq_zscore'] = df.groupby(['year-month'])[['aftret_eq']].apply(modified_z)
df['aftret_equity_zscore'] = df.groupby(['year-month'])[['aftret_equity']].apply(modified_z)
df['aftret_invcapx_zscore'] = df.groupby(['year-month'])[['aftret_invcapx']].apply(modified_z)
df['at_turn_zscore'] = df.groupby(['year-month'])[['at_turn']].apply(modified_z)
df['bm_zscore'] = df.groupby(['year-month'])[['bm']].apply(modified_z)
df['CAPEI_zscore'] = df.groupby(['year-month'])[['CAPEI']].apply(modified_z)
df['capital_ratio_zscore'] = df.groupby(['year-month'])[['capital_ratio']].apply(modified_z)
df['cash_conversion_zscore'] = df.groupby(['year-month'])[['cash_conversion']].apply(modified_z)
df['cash_debt_zscore'] = df.groupby(['year-month'])[['cash_debt']].apply(modified_z)
df['cash_lt_zscore'] = df.groupby(['year-month'])[['cash_lt']].apply(modified_z)
df['cash_ratio_zscore'] = df.groupby(['year-month'])[['cash_ratio']].apply(modified_z)
df['cfm_zscore'] = df.groupby(['year-month'])[['cfm']].apply(modified_z)
df['curr_debt_zscore'] = df.groupby(['year-month'])[['curr_debt']].apply(modified_z)
df['curr_ratio_zscore'] = df.groupby(['year-month'])[['curr_ratio']].apply(modified_z)
df['de_ratio_zscore'] = df.groupby(['year-month'])[['de_ratio']].apply(modified_z)
df['debt_assets_zscore'] = df.groupby(['year-month'])[['debt_assets']].apply(modified_z)
df['debt_at_zscore'] = df.groupby(['year-month'])[['debt_at']].apply(modified_z)
df['debt_capital_zscore'] = df.groupby(['year-month'])[['debt_capital']].apply(modified_z)
df['debt_ebitda_zscore'] = df.groupby(['year-month'])[['debt_ebitda']].apply(modified_z)
df['debt_invcap_zscore'] = df.groupby(['year-month'])[['debt_invcap']].apply(modified_z)
df['DIVYIELD_zscore'] = df.groupby(['year-month'])[['DIVYIELD']].apply(modified_z)
df['dltt_be_zscore'] = df.groupby(['year-month'])[['dltt_be']].apply(modified_z)
df['dpr_zscore'] = df.groupby(['year-month'])[['dpr']].apply(modified_z)
df['efftax_zscore'] = df.groupby(['year-month'])[['efftax']].apply(modified_z)
df['equity_invcap_zscore'] = df.groupby(['year-month'])[['equity_invcap']].apply(modified_z)
df['evm_zscore'] = df.groupby(['year-month'])[['evm']].apply(modified_z)
df['fcf_ocf_zscore'] = df.groupby(['year-month'])[['fcf_ocf']].apply(modified_z)
df['gpm_zscore'] = df.groupby(['year-month'])[['gpm']].apply(modified_z)
df['GProf_zscore'] = df.groupby(['year-month'])[['GProf']].apply(modified_z)
df['int_debt_zscore'] = df.groupby(['year-month'])[['int_debt']].apply(modified_z)
df['int_totdebt_zscore'] = df.groupby(['year-month'])[['int_totdebt']].apply(modified_z)
df['intcov_zscore'] = df.groupby(['year-month'])[['intcov']].apply(modified_z)
df['intcov_ratio_zscore'] = df.groupby(['year-month'])[['intcov_ratio']].apply(modified_z)
df['inv_turn_zscore'] = df.groupby(['year-month'])[['inv_turn']].apply(modified_z)
df['invt_act_zscore'] = df.groupby(['year-month'])[['invt_act']].apply(modified_z)
df['lt_debt_zscore'] = df.groupby(['year-month'])[['lt_debt']].apply(modified_z)
df['lt_ppent_zscore'] = df.groupby(['year-month'])[['lt_ppent']].apply(modified_z)
df['npm_zscore'] = df.groupby(['year-month'])[['npm']].apply(modified_z)
df['ocf_lct_zscore'] = df.groupby(['year-month'])[['ocf_lct']].apply(modified_z)
df['opmad_zscore'] = df.groupby(['year-month'])[['opmad']].apply(modified_z)
df['opmbd_zscore'] = df.groupby(['year-month'])[['opmbd']].apply(modified_z)
df['pay_turn_zscore'] = df.groupby(['year-month'])[['pay_turn']].apply(modified_z)
df['pcf_zscore'] = df.groupby(['year-month'])[['pcf']].apply(modified_z)
df['pe_exi_zscore'] = df.groupby(['year-month'])[['pe_exi']].apply(modified_z)
df['pe_inc_zscore'] = df.groupby(['year-month'])[['pe_inc']].apply(modified_z)
df['pe_op_basic_zscore'] = df.groupby(['year-month'])[['pe_op_basic']].apply(modified_z)
df['pe_op_dil_zscore'] = df.groupby(['year-month'])[['pe_op_dil']].apply(modified_z)
df['PEG_1yrforward_zscore'] = df.groupby(['year-month'])[['PEG_1yrforward']].apply(modified_z)
df['PEG_ltgforward_zscore'] = df.groupby(['year-month'])[['PEG_ltgforward']].apply(modified_z)
df['PEG_trailing_zscore'] = df.groupby(['year-month'])[['PEG_trailing']].apply(modified_z)
df['pretret_earnat_zscore'] = df.groupby(['year-month'])[['pretret_earnat']].apply(modified_z)
df['pretret_noa_zscore'] = df.groupby(['year-month'])[['pretret_noa']].apply(modified_z)
df['profit_lct_zscore'] = df.groupby(['year-month'])[['profit_lct']].apply(modified_z)
df['ps_zscore'] = df.groupby(['year-month'])[['ps']].apply(modified_z)
df['ptb_zscore'] = df.groupby(['year-month'])[['ptb']].apply(modified_z)
df['ptpm_zscore'] = df.groupby(['year-month'])[['ptpm']].apply(modified_z)
df['quick_ratio_zscore'] = df.groupby(['year-month'])[['quick_ratio']].apply(modified_z)
df['rd_sale_zscore'] = df.groupby(['year-month'])[['rd_sale']].apply(modified_z)
df['rect_act_zscore'] = df.groupby(['year-month'])[['rect_act']].apply(modified_z)
df['rect_turn_zscore'] = df.groupby(['year-month'])[['rect_turn']].apply(modified_z)
df['roa_zscore'] = df.groupby(['year-month'])[['roa']].apply(modified_z)
df['roce_zscore'] = df.groupby(['year-month'])[['roce']].apply(modified_z)
df['roe_zscore'] = df.groupby(['year-month'])[['roe']].apply(modified_z)
df['sale_equity_zscore'] = df.groupby(['year-month'])[['sale_equity']].apply(modified_z)
df['sale_invcap_zscore'] = df.groupby(['year-month'])[['sale_invcap']].apply(modified_z)
df['sale_nwc_zscore'] = df.groupby(['year-month'])[['sale_nwc']].apply(modified_z)
df['short_debt_zscore'] = df.groupby(['year-month'])[['short_debt']].apply(modified_z)
df['staff_sale_zscore'] = df.groupby(['year-month'])[['staff_sale']].apply(modified_z)
df['totdebt_invcap_zscore'] = df.groupby(['year-month'])[['totdebt_invcap']].apply(modified_z)
df['dvpspm_zscore'] = df.groupby(['year-month'])[['dvpspm']].apply(modified_z)
df['dvpsxm_zscore'] = df.groupby(['year-month'])[['dvpsxm']].apply(modified_z)
df['dvrate_zscore'] = df.groupby(['year-month'])[['dvrate']].apply(modified_z)
df['spcsrc_zscore'] = df.groupby(['year-month'])[['spcsrc']].apply(modified_z)
df['alpha_zscore'] = df.groupby(['year-month'])[['alpha']].apply(modified_z)
df['b_hml_zscore'] = df.groupby(['year-month'])[['b_hml']].apply(modified_z)
df['b_mkt_zscore'] = df.groupby(['year-month'])[['b_mkt']].apply(modified_z)
df['b_smb_zscore'] = df.groupby(['year-month'])[['b_smb']].apply(modified_z)
df['b_umd_zscore'] = df.groupby(['year-month'])[['b_umd']].apply(modified_z)
df['exret_zscore'] = df.groupby(['year-month'])[['exret']].apply(modified_z)
df['ivol_zscore'] = df.groupby(['year-month'])[['ivol']].apply(modified_z)
df['n_zscore'] = df.groupby(['year-month'])[['n']].apply(modified_z)
df['R2_zscore'] = df.groupby(['year-month'])[['R2']].apply(modified_z)
df['tvol_zscore'] = df.groupby(['year-month'])[['tvol']].apply(modified_z)
df['BUYPCT_zscore'] = df.groupby(['year-month'])[['BUYPCT']].apply(modified_z)
df['HOLDPCT_zscore'] = df.groupby(['year-month'])[['HOLDPCT']].apply(modified_z)
df['MEANREC_zscore'] = df.groupby(['year-month'])[['MEANREC']].apply(modified_z)
df['MEDREC_zscore'] = df.groupby(['year-month'])[['MEDREC']].apply(modified_z)
df['recup_zscore'] = df.groupby(['year-month'])[['recup']].apply(modified_z)
df['recdown_zscore'] = df.groupby(['year-month'])[['recdown']].apply(modified_z)
df['SELLPCT_zscore'] = df.groupby(['year-month'])[['SELLPCT']].apply(modified_z)
df['STDEV_zscore'] = df.groupby(['year-month'])[['STDEV']].apply(modified_z)

# df - Enrich - Sector Z-Score
df['past_one_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_one_month_return']].apply(modified_z)
df['past_two_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_two_month_return']].apply(modified_z)
df['past_three_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_three_month_return']].apply(modified_z)
df['past_four_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_four_month_return']].apply(modified_z)
df['past_five_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_five_month_return']].apply(modified_z)
df['past_six_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_six_month_return']].apply(modified_z)
df['past_seven_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_seven_month_return']].apply(modified_z)
df['past_eight_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_eight_month_return']].apply(modified_z)
df['past_nine_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_nine_month_return']].apply(modified_z)
df['past_ten_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_ten_month_return']].apply(modified_z)
df['past_eleven_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_eleven_month_return']].apply(modified_z)
df['past_twelve_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twelve_month_return']].apply(modified_z)
df['past_thirteen_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirteen_month_return']].apply(modified_z)
df['past_fourteen_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_fourteen_month_return']].apply(modified_z)
df['past_fifteen_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_fifteen_month_return']].apply(modified_z)
df['past_sixteen_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_sixteen_month_return']].apply(modified_z)
df['past_seventeen_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_seventeen_month_return']].apply(modified_z)
df['past_eighteen_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_eighteen_month_return']].apply(modified_z)
df['past_nineteen_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_nineteen_month_return']].apply(modified_z)
df['past_twenty_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twenty_month_return']].apply(modified_z)
df['past_twentyone_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentyone_month_return']].apply(modified_z)
df['past_twentytwo_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentytwo_month_return']].apply(modified_z)
df['past_twentythree_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentythree_month_return']].apply(modified_z)
df['past_twentyfour_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentyfour_month_return']].apply(modified_z)
df['past_twentyfive_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentyfive_month_return']].apply(modified_z)
df['past_twentysix_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentysix_month_return']].apply(modified_z)
df['past_twentyseven_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentyseven_month_return']].apply(modified_z)
df['past_twentyeight_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentyeight_month_return']].apply(modified_z)
df['past_twentynine_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_twentynine_month_return']].apply(modified_z)
df['past_thirty_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirty_month_return']].apply(modified_z)
df['past_thirtyone_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirtyone_month_return']].apply(modified_z)
df['past_thirtytwo_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirtytwo_month_return']].apply(modified_z)
df['past_thirtythree_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirtythree_month_return']].apply(modified_z)
df['past_thirtyfour_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirtyfour_month_return']].apply(modified_z)
df['past_thirtyfive_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirtyfive_month_return']].apply(modified_z)
df['past_thirtysix_month_return_sector_zscore'] = df.groupby(['year-month', 'sector'])[['past_thirtysix_month_return']].apply(modified_z)
df['accrual_sector_zscore'] = df.groupby(['year-month', 'sector'])[['accrual']].apply(modified_z)
df['adv_sale_sector_zscore'] = df.groupby(['year-month', 'sector'])[['adv_sale']].apply(modified_z)
df['aftret_eq_sector_zscore'] = df.groupby(['year-month', 'sector'])[['aftret_eq']].apply(modified_z)
df['aftret_equity_sector_zscore'] = df.groupby(['year-month', 'sector'])[['aftret_equity']].apply(modified_z)
df['aftret_invcapx_sector_zscore'] = df.groupby(['year-month', 'sector'])[['aftret_invcapx']].apply(modified_z)
df['at_turn_sector_zscore'] = df.groupby(['year-month', 'sector'])[['at_turn']].apply(modified_z)
df['bm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['bm']].apply(modified_z)
df['CAPEI_sector_zscore'] = df.groupby(['year-month', 'sector'])[['CAPEI']].apply(modified_z)
df['capital_ratio_sector_zscore'] = df.groupby(['year-month', 'sector'])[['capital_ratio']].apply(modified_z)
df['cash_conversion_sector_zscore'] = df.groupby(['year-month', 'sector'])[['cash_conversion']].apply(modified_z)
df['cash_debt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['cash_debt']].apply(modified_z)
df['cash_lt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['cash_lt']].apply(modified_z)
df['cash_ratio_sector_zscore'] = df.groupby(['year-month', 'sector'])[['cash_ratio']].apply(modified_z)
df['cfm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['cfm']].apply(modified_z)
df['curr_debt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['curr_debt']].apply(modified_z)
df['curr_ratio_sector_zscore'] = df.groupby(['year-month', 'sector'])[['curr_ratio']].apply(modified_z)
df['de_ratio_sector_zscore'] = df.groupby(['year-month', 'sector'])[['de_ratio']].apply(modified_z)
df['debt_assets_sector_zscore'] = df.groupby(['year-month', 'sector'])[['debt_assets']].apply(modified_z)
df['debt_at_sector_zscore'] = df.groupby(['year-month', 'sector'])[['debt_at']].apply(modified_z)
df['debt_capital_sector_zscore'] = df.groupby(['year-month', 'sector'])[['debt_capital']].apply(modified_z)
df['debt_ebitda_sector_zscore'] = df.groupby(['year-month', 'sector'])[['debt_ebitda']].apply(modified_z)
df['debt_invcap_sector_zscore'] = df.groupby(['year-month', 'sector'])[['debt_invcap']].apply(modified_z)
df['DIVYIELD_sector_zscore'] = df.groupby(['year-month', 'sector'])[['DIVYIELD']].apply(modified_z)
df['dltt_be_sector_zscore'] = df.groupby(['year-month', 'sector'])[['dltt_be']].apply(modified_z)
df['dpr_sector_zscore'] = df.groupby(['year-month', 'sector'])[['dpr']].apply(modified_z)
df['efftax_sector_zscore'] = df.groupby(['year-month', 'sector'])[['efftax']].apply(modified_z)
df['equity_invcap_sector_zscore'] = df.groupby(['year-month', 'sector'])[['equity_invcap']].apply(modified_z)
df['evm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['evm']].apply(modified_z)
df['fcf_ocf_sector_zscore'] = df.groupby(['year-month', 'sector'])[['fcf_ocf']].apply(modified_z)
df['gpm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['gpm']].apply(modified_z)
df['GProf_sector_zscore'] = df.groupby(['year-month', 'sector'])[['GProf']].apply(modified_z)
df['int_debt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['int_debt']].apply(modified_z)
df['int_totdebt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['int_totdebt']].apply(modified_z)
df['intcov_sector_zscore'] = df.groupby(['year-month', 'sector'])[['intcov']].apply(modified_z)
df['intcov_ratio_sector_zscore'] = df.groupby(['year-month', 'sector'])[['intcov_ratio']].apply(modified_z)
df['inv_turn_sector_zscore'] = df.groupby(['year-month', 'sector'])[['inv_turn']].apply(modified_z)
df['invt_act_sector_zscore'] = df.groupby(['year-month', 'sector'])[['invt_act']].apply(modified_z)
df['lt_debt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['lt_debt']].apply(modified_z)
df['lt_ppent_sector_zscore'] = df.groupby(['year-month', 'sector'])[['lt_ppent']].apply(modified_z)
df['npm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['npm']].apply(modified_z)
df['ocf_lct_sector_zscore'] = df.groupby(['year-month', 'sector'])[['ocf_lct']].apply(modified_z)
df['opmad_sector_zscore'] = df.groupby(['year-month', 'sector'])[['opmad']].apply(modified_z)
df['opmbd_sector_zscore'] = df.groupby(['year-month', 'sector'])[['opmbd']].apply(modified_z)
df['pay_turn_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pay_turn']].apply(modified_z)
df['pcf_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pcf']].apply(modified_z)
df['pe_exi_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pe_exi']].apply(modified_z)
df['pe_inc_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pe_inc']].apply(modified_z)
df['pe_op_basic_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pe_op_basic']].apply(modified_z)
df['pe_op_dil_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pe_op_dil']].apply(modified_z)
df['PEG_1yrforward_sector_zscore'] = df.groupby(['year-month', 'sector'])[['PEG_1yrforward']].apply(modified_z)
df['PEG_ltgforward_sector_zscore'] = df.groupby(['year-month', 'sector'])[['PEG_ltgforward']].apply(modified_z)
df['PEG_trailing_sector_zscore'] = df.groupby(['year-month', 'sector'])[['PEG_trailing']].apply(modified_z)
df['pretret_earnat_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pretret_earnat']].apply(modified_z)
df['pretret_noa_sector_zscore'] = df.groupby(['year-month', 'sector'])[['pretret_noa']].apply(modified_z)
df['profit_lct_sector_zscore'] = df.groupby(['year-month', 'sector'])[['profit_lct']].apply(modified_z)
df['ps_sector_zscore'] = df.groupby(['year-month', 'sector'])[['ps']].apply(modified_z)
df['ptb_sector_zscore'] = df.groupby(['year-month', 'sector'])[['ptb']].apply(modified_z)
df['ptpm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['ptpm']].apply(modified_z)
df['quick_ratio_sector_zscore'] = df.groupby(['year-month', 'sector'])[['quick_ratio']].apply(modified_z)
df['rd_sale_sector_zscore'] = df.groupby(['year-month', 'sector'])[['rd_sale']].apply(modified_z)
df['rect_act_sector_zscore'] = df.groupby(['year-month', 'sector'])[['rect_act']].apply(modified_z)
df['rect_turn_sector_zscore'] = df.groupby(['year-month', 'sector'])[['rect_turn']].apply(modified_z)
df['roa_sector_zscore'] = df.groupby(['year-month', 'sector'])[['roa']].apply(modified_z)
df['roce_sector_zscore'] = df.groupby(['year-month', 'sector'])[['roce']].apply(modified_z)
df['roe_sector_zscore'] = df.groupby(['year-month', 'sector'])[['roe']].apply(modified_z)
df['sale_equity_sector_zscore'] = df.groupby(['year-month', 'sector'])[['sale_equity']].apply(modified_z)
df['sale_invcap_sector_zscore'] = df.groupby(['year-month', 'sector'])[['sale_invcap']].apply(modified_z)
df['sale_nwc_sector_zscore'] = df.groupby(['year-month', 'sector'])[['sale_nwc']].apply(modified_z)
df['short_debt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['short_debt']].apply(modified_z)
df['staff_sale_sector_zscore'] = df.groupby(['year-month', 'sector'])[['staff_sale']].apply(modified_z)
df['totdebt_invcap_sector_zscore'] = df.groupby(['year-month', 'sector'])[['totdebt_invcap']].apply(modified_z)
df['dvpspm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['dvpspm']].apply(modified_z)
df['dvpsxm_sector_zscore'] = df.groupby(['year-month', 'sector'])[['dvpsxm']].apply(modified_z)
df['dvrate_sector_zscore'] = df.groupby(['year-month', 'sector'])[['dvrate']].apply(modified_z)
df['spcsrc_sector_zscore'] = df.groupby(['year-month', 'sector'])[['spcsrc']].apply(modified_z)
df['alpha_sector_zscore'] = df.groupby(['year-month', 'sector'])[['alpha']].apply(modified_z)
df['b_hml_sector_zscore'] = df.groupby(['year-month', 'sector'])[['b_hml']].apply(modified_z)
df['b_mkt_sector_zscore'] = df.groupby(['year-month', 'sector'])[['b_mkt']].apply(modified_z)
df['b_smb_sector_zscore'] = df.groupby(['year-month', 'sector'])[['b_smb']].apply(modified_z)
df['b_umd_sector_zscore'] = df.groupby(['year-month', 'sector'])[['b_umd']].apply(modified_z)
df['exret_sector_zscore'] = df.groupby(['year-month', 'sector'])[['exret']].apply(modified_z)
df['ivol_sector_zscore'] = df.groupby(['year-month', 'sector'])[['ivol']].apply(modified_z)
df['n_sector_zscore'] = df.groupby(['year-month', 'sector'])[['n']].apply(modified_z)
df['R2_sector_zscore'] = df.groupby(['year-month', 'sector'])[['R2']].apply(modified_z)
df['tvol_sector_zscore'] = df.groupby(['year-month', 'sector'])[['tvol']].apply(modified_z)
df['BUYPCT_sector_zscore'] = df.groupby(['year-month', 'sector'])[['BUYPCT']].apply(modified_z)
df['HOLDPCT_sector_zscore'] = df.groupby(['year-month', 'sector'])[['HOLDPCT']].apply(modified_z)
df['MEANREC_sector_zscore'] = df.groupby(['year-month', 'sector'])[['MEANREC']].apply(modified_z)
df['MEDREC_sector_zscore'] = df.groupby(['year-month', 'sector'])[['MEDREC']].apply(modified_z)
df['recup_sector_zscore'] = df.groupby(['year-month', 'sector'])[['recup']].apply(modified_z)
df['recdown_sector_zscore'] = df.groupby(['year-month', 'sector'])[['recdown']].apply(modified_z)
df['SELLPCT_sector_zscore'] = df.groupby(['year-month', 'sector'])[['SELLPCT']].apply(modified_z)
df['STDEV_sector_zscore'] = df.groupby(['year-month', 'sector'])[['STDEV']].apply(modified_z)

# df - Clean - All - Fill N/A's
df = df.groupby(['GVKEY'], as_index=False).apply(lambda group: group.ffill())
df = df.groupby(['GVKEY'], as_index=False).apply(lambda group: group.bfill())
df = df.groupby(['sector'], as_index=False).apply(lambda group: group.ffill())
df = df.groupby(['sector'], as_index=False).apply(lambda group: group.bfill())
df = df.fillna(method='ffill')
df = df.fillna(method='bfill')

# df - Enrich - Add Up/Down binary Dependents
# Dependent
df['forward_one_month_return_up'] = np.where(df['forward_one_month_return'] >= 0, 1, 0)
df['forward_two_month_return_up'] = np.where(df['forward_two_month_return'] >= 0, 1, 0)
df['forward_three_month_return_up'] = np.where(df['forward_three_month_return'] >= 0, 1, 0)
df['forward_four_month_return_up'] = np.where(df['forward_four_month_return'] >= 0, 1, 0)
df['forward_five_month_return_up'] = np.where(df['forward_five_month_return'] >= 0, 1, 0)
df['forward_six_month_return_up'] = np.where(df['forward_six_month_return'] >= 0, 1, 0)
df['forward_seven_month_return_up'] = np.where(df['forward_seven_month_return'] >= 0, 1, 0)
df['forward_eight_month_return_up'] = np.where(df['forward_eight_month_return'] >= 0, 1, 0)
df['forward_nine_month_return_up'] = np.where(df['forward_nine_month_return'] >= 0, 1, 0)
df['forward_ten_month_return_up'] = np.where(df['forward_ten_month_return'] >= 0, 1, 0)
df['forward_eleven_month_return_up'] = np.where(df['forward_eleven_month_return'] >= 0, 1, 0)
df['forward_twelve_month_return_up'] = np.where(df['forward_twelve_month_return'] >= 0, 1, 0)
df['forward_thirteen_month_return_up'] = np.where(df['forward_thirteen_month_return'] >= 0, 1, 0)
df['forward_fourteen_month_return_up'] = np.where(df['forward_fourteen_month_return'] >= 0, 1, 0)
df['forward_fifteen_month_return_up'] = np.where(df['forward_fifteen_month_return'] >= 0, 1, 0)
df['forward_sixteen_month_return_up'] = np.where(df['forward_sixteen_month_return'] >= 0, 1, 0)
df['forward_seventeen_month_return_up'] = np.where(df['forward_seventeen_month_return'] >= 0, 1, 0)
df['forward_eighteen_month_return_up'] = np.where(df['forward_eighteen_month_return'] >= 0, 1, 0)
df['forward_nineteen_month_return_up'] = np.where(df['forward_nineteen_month_return'] >= 0, 1, 0)
df['forward_twenty_month_return_up'] = np.where(df['forward_twenty_month_return'] >= 0, 1, 0)
df['forward_twentyone_month_return_up'] = np.where(df['forward_twentyone_month_return'] >= 0, 1, 0)
df['forward_twentytwo_month_return_up'] = np.where(df['forward_twentytwo_month_return'] >= 0, 1, 0)
df['forward_twentythree_month_return_up'] = np.where(df['forward_twentythree_month_return'] >= 0, 1, 0)
df['forward_twentyfour_month_return_up'] = np.where(df['forward_twentyfour_month_return'] >= 0, 1, 0)
df['forward_twentyfive_month_return_up'] = np.where(df['forward_twentyfive_month_return'] >= 0, 1, 0)
df['forward_twentysix_month_return_up'] = np.where(df['forward_twentysix_month_return'] >= 0, 1, 0)
df['forward_twentyseven_month_return_up'] = np.where(df['forward_twentyseven_month_return'] >= 0, 1, 0)
df['forward_twentyeight_month_return_up'] = np.where(df['forward_twentyeight_month_return'] >= 0, 1, 0)
df['forward_twentynine_month_return_up'] = np.where(df['forward_twentynine_month_return'] >= 0, 1, 0)
df['forward_thirty_month_return_up'] = np.where(df['forward_thirty_month_return'] >= 0, 1, 0)
df['forward_thirtyone_month_return_up'] = np.where(df['forward_thirtyone_month_return'] >= 0, 1, 0)
df['forward_thirtytwo_month_return_up'] = np.where(df['forward_thirtytwo_month_return'] >= 0, 1, 0)
df['forward_thirtythree_month_return_up'] = np.where(df['forward_thirtythree_month_return'] >= 0, 1, 0)
df['forward_thirtyfour_month_return_up'] = np.where(df['forward_thirtyfour_month_return'] >= 0, 1, 0)
df['forward_thirtyfive_month_return_up'] = np.where(df['forward_thirtyfive_month_return'] >= 0, 1, 0)
df['forward_thirtysix_month_return_up'] = np.where(df['forward_thirtysix_month_return'] >= 0, 1, 0)

# Features
df['past_one_month_return_up'] = np.where(df['past_one_month_return'] >= 0, 1, 0)
df['past_two_month_return_up'] = np.where(df['past_two_month_return'] >= 0, 1, 0)
df['past_three_month_return_up'] = np.where(df['past_three_month_return'] >= 0, 1, 0)
df['past_four_month_return_up'] = np.where(df['past_four_month_return'] >= 0, 1, 0)
df['past_five_month_return_up'] = np.where(df['past_five_month_return'] >= 0, 1, 0)
df['past_six_month_return_up'] = np.where(df['past_six_month_return'] >= 0, 1, 0)
df['past_seven_month_return_up'] = np.where(df['past_seven_month_return'] >= 0, 1, 0)
df['past_eight_month_return_up'] = np.where(df['past_eight_month_return'] >= 0, 1, 0)
df['past_nine_month_return_up'] = np.where(df['past_nine_month_return'] >= 0, 1, 0)
df['past_ten_month_return_up'] = np.where(df['past_ten_month_return'] >= 0, 1, 0)
df['past_eleven_month_return_up'] = np.where(df['past_eleven_month_return'] >= 0, 1, 0)
df['past_twelve_month_return_up'] = np.where(df['past_twelve_month_return'] >= 0, 1, 0)
df['past_thirteen_month_return_up'] = np.where(df['past_thirteen_month_return'] >= 0, 1, 0)
df['past_fourteen_month_return_up'] = np.where(df['past_fourteen_month_return'] >= 0, 1, 0)
df['past_fifteen_month_return_up'] = np.where(df['past_fifteen_month_return'] >= 0, 1, 0)
df['past_sixteen_month_return_up'] = np.where(df['past_sixteen_month_return'] >= 0, 1, 0)
df['past_seventeen_month_return_up'] = np.where(df['past_seventeen_month_return'] >= 0, 1, 0)
df['past_eighteen_month_return_up'] = np.where(df['past_eighteen_month_return'] >= 0, 1, 0)
df['past_nineteen_month_return_up'] = np.where(df['past_nineteen_month_return'] >= 0, 1, 0)
df['past_twenty_month_return_up'] = np.where(df['past_twenty_month_return'] >= 0, 1, 0)
df['past_twentyone_month_return_up'] = np.where(df['past_twentyone_month_return'] >= 0, 1, 0)
df['past_twentytwo_month_return_up'] = np.where(df['past_twentytwo_month_return'] >= 0, 1, 0)
df['past_twentythree_month_return_up'] = np.where(df['past_twentythree_month_return'] >= 0, 1, 0)
df['past_twentyfour_month_return_up'] = np.where(df['past_twentyfour_month_return'] >= 0, 1, 0)
df['past_twentyfive_month_return_up'] = np.where(df['past_twentyfive_month_return'] >= 0, 1, 0)
df['past_twentysix_month_return_up'] = np.where(df['past_twentysix_month_return'] >= 0, 1, 0)
df['past_twentyseven_month_return_up'] = np.where(df['past_twentyseven_month_return'] >= 0, 1, 0)
df['past_twentyeight_month_return_up'] = np.where(df['past_twentyeight_month_return'] >= 0, 1, 0)
df['past_twentynine_month_return_up'] = np.where(df['past_twentynine_month_return'] >= 0, 1, 0)
df['past_thirty_month_return_up'] = np.where(df['past_thirty_month_return'] >= 0, 1, 0)
df['past_thirtyone_month_return_up'] = np.where(df['past_thirtyone_month_return'] >= 0, 1, 0)
df['past_thirtytwo_month_return_up'] = np.where(df['past_thirtytwo_month_return'] >= 0, 1, 0)
df['past_thirtythree_month_return_up'] = np.where(df['past_thirtythree_month_return'] >= 0, 1, 0)
df['past_thirtyfour_month_return_up'] = np.where(df['past_thirtyfour_month_return'] >= 0, 1, 0)
df['past_thirtyfive_month_return_up'] = np.where(df['past_thirtyfive_month_return'] >= 0, 1, 0)
df['past_thirtysix_month_return_up'] = np.where(df['past_thirtysix_month_return'] >= 0, 1, 0)

# df - Enrich - Sector Dummy Variablees
sectors =  pd.get_dummies(df.sector, prefix="sector").astype(int)
df  = pd.concat([df, sectors], axis=1)

# Write to CSV
df.to_csv('df.csv')
