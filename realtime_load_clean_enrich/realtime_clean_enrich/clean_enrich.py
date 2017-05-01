import pandas as pd
import numpy as np
import datetime
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'

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

    median = np.nanmedian(array)
    median_absolute_deviation = mad(array)

    if median_absolute_deviation == 0:
        mean_absolute_deviation = meanad(array)
        array = (array - median) / (mean_absolute_deviation * 1.253314)
        return array

    else:
        array = (array - median) / (median_absolute_deviation * 1.486)
        return array


df = pd.read_csv('/data/W205_Final/W205_Project/realtime_load_clean_enrich/stock_scrape/realtime.csv')


# df - Replace "-" with NaN
df = df.replace('-', np.nan)

# df - Clean Dividend Yield, assume unreported as 0.
df['Dividend'] = df['Dividend'].replace('%','',regex=True).astype('float')/100
df['Dividend'] = pd.to_numeric(df['Dividend'])
df['Dividend'] = df['Dividend'].fillna(0)

# df - Clean All Other Variables with % (After String Identifiers)
for column in df.iloc[:,8:]:

    try:
        df[column] = df[column].replace('%','',regex=True).astype('float')
        df[column] = pd.to_numeric(df[column])

    except:
        pass

# df - Enrich - Add Current Date
df['date'] = pd.to_datetime('today')

# df - Enrich - Add Current Month
df['month'] = df['date'].dt.month

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

# df - Enrich - Month Features
df['sector_Financials'] = np.where(df['Sector'] == 'Financial', int(1), int(0))
df['sector_Information Technology'] = np.where(df['Sector'] == 'Technology', int(1), int(0))
df['sector_Consumer Discretionary'] = np.where(df['Sector'] == 'Services', int(1), int(0))
df['sector_Industrials'] = np.where(df['Sector'] == 'Industrial Goods', int(1), int(0))
df['sector_Health Care'] = np.where(df['Sector'] == 'Healthcare', int(1), int(0))
df['sector_Energy'] = np.where(df['Sector'] == 'Basic Materials', int(1), int(0))
df['sector_Materials'] = np.where(df['Sector'] == 'Basic Materials', int(1), int(0))
df['sector_Consumer Staples'] = np.where(df['Sector'] == 'Consumer Goods', int(1), int(0))
df['sector_Utilities'] = np.where(df['Sector'] == 'Utilities', int(1), int(0))
df['sector_Telecommunication Services'] = np.where(df['Sector'] == 'Technology', int(1), int(0))
df['sector_Real Estate'] = np.where(df['Sector'] == 'Services', int(1), int(0))

# df - Clean - All - Fill N/A's
for column in df:

    try:
        df[column] = df[column].fillna(np.nanmedian(df[column]))

    except:
        pass

# df - Enrich - Add Z-Scores for features in both real-time and historic datasets
df['P/E_zscore'] = modified_z(df['P/E'])
df['PEG_zscore'] = modified_z(df['PEG'])
df['P/S_zscore'] = modified_z(df['P/S'])
df['P/B_zscore'] = modified_z(df['P/B'])
df['P/C_zscore'] = modified_z(df['P/C'])
df['Dividend_zscore'] = modified_z(df['Dividend'])
df['ROA_zscore'] = modified_z(df['ROA'])
df['Recom_zscore'] = modified_z(df['Recom'])
df['Curr R_zscore'] = modified_z(df['Curr R'])
df['Quick R_zscore'] = modified_z(df['Quick R'])

# df - Enrich - Add Sector Z-Scores for features in both real-time and historic datasets
df['P/E_sector_zscore'] = df.groupby(['Sector'])[['P/E']].apply(modified_z)
df['PEG_sector_zscore'] = df.groupby(['Sector'])[['PEG']].apply(modified_z)
df['P/S_sector_zscore'] = df.groupby(['Sector'])[['P/S']].apply(modified_z)
df['P/B_sector_zscore'] = df.groupby(['Sector'])[['P/B']].apply(modified_z)
df['P/C_sector_zscore'] = df.groupby(['Sector'])[['P/C']].apply(modified_z)
df['Dividend_sector_zscore'] = df.groupby(['Sector'])[['Dividend']].apply(modified_z)
df['ROA_sector_zscore'] = df.groupby(['Sector'])[['ROA']].apply(modified_z)
df['Recom_sector_zscore'] = df.groupby(['Sector'])[['Recom']].apply(modified_z)
df['Curr R_sector_zscore'] = df.groupby(['Sector'])[['Curr R']].apply(modified_z)
df['Quick R_sector_zscore'] = df.groupby(['Sector'])[['Quick R']].apply(modified_z)

# df - Enrich - Add Up/Down binary past returns
# Time Features
df['Perf Year'] = np.where(df['Perf Year'] >= 0, 1, 0)
df['Perf Half'] = np.where(df['Perf Half'] >= 0, 1, 0)
df['Perf Quart'] = np.where(df['Perf Quart'] >= 0, 1, 0)
df['Perf Month'] = np.where(df['Perf Month'] >= 0, 1, 0)

# df - Write cleaned data to CSV
df.to_csv('/data/W205_Project/realtime_load_clean_enrich/realtime_clean_enrich/realtime.csv')
