import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn import preprocessing

# Load historic training data
train = pd.read_csv('/data/Desktop/W205_Final/W205_Project/historic_load_clean_enrich/df.csv')

# Load real-time data (cleaned)
realtime = pd.read_csv('/data/W205_Final/W205_Project/realtime_load_clean_enrich/realtime_clean_enrich/realtime.csv')

# Load real-time data (raw)
realtime_raw = pd.read_csv('/data/W205_Final/W205_Project/realtime_load_clean_enrich/stock_scrape/realtime.csv')

# Select Features (available in both historic and real-time)
features = [
            'DIVYIELD',
            'ptb',
            'ps',
            'pe_op_basic',
            'sector_Financials',
            'past_twelve_month_return_up',
            'sector_Information Technology',
            'past_six_month_return_up',
            'PEG_trailing',
            'MEANREC',
            'sector_Consumer Discretionary',
            'past_three_month_return_up',
            'pcf',
            'sector_Utilities',
            'sector_Health Care',
            'past_one_month_return_up',
            'roa',
            'DIVYIELD_zscore',
            'curr_ratio',
            'sector_Consumer Staples',
            'ps_zscore',
            'ptb_zscore',
            'sector_Telecommunication Services',
            'sector_Real Estate',
            'quick_ratio',
            'sector_Materials',
            'pe_op_basic_zscore',
            'sector_Energy',
            'sector_Industrials',
            'PEG_trailing_zscore',
            'ps_sector_zscore',
            'january',
            'pcf_zscore',
            'pe_op_basic_sector_zscore',
            'ptb_sector_zscore',
            'DIVYIELD_sector_zscore',
            'february',
            'october',
            'MEANREC_zscore',
            'PEG_trailing_sector_zscore',
            'pcf_sector_zscore',
            'july',
            'roa_zscore',
            'april',
            'june',
            'august',
            'december',
            'march',
            'may',
            'september',
            'MEANREC_sector_zscore',
            'roa_sector_zscore',
            'november',
            'curr_ratio_zscore',
            'curr_ratio_sector_zscore',
            'quick_ratio_sector_zscore'
            ]

# Rename Real-time Features to match historic dataset
realtime = realtime.rename(columns={
            'P/E' : 'pe_op_basic',
            'PEG' : 'PEG_trailing',
            'P/S' : 'ps',
            'P/B' : 'ptb',
            'P/C' : 'pcf',
            'Dividend' : 'DIVYIELD',
            'ROA' : 'roa',
            'Recom' : 'MEANREC',
            'Curr R' : 'curr_ratio',
            'Quick R' : 'quick_ratio',
            'Perf Year' : 'past_twelve_month_return_up',
            'Perf Half' : 'past_six_month_return_up',
            'Perf Quart' : 'past_three_month_return_up',
            'Perf Month' : 'past_one_month_return_up',
            'P/E_zscore' : 'pe_op_basic_zscore',
            'PEG_zscore' : 'PEG_trailing_zscore',
            'P/S_zscore' : 'ps_zscore',
            'P/B_zscore' : 'ptb_zscore',
            'P/C_zscore' : 'pcf_zscore',
            'Dividend_zscore' : 'DIVYIELD_zscore',
            'ROA_zscore' : 'roa_zscore',
            'Recom_zscore' : 'MEANREC_zscore',
            'Curr R_zscore' : 'curr_ratio_zscore',
            'P/E_sector_zscore' : 'pe_op_basic_sector_zscore',
            'P/E_sector_zscore' : 'pe_op_basic_sector_zscore',
            'PEG_sector_zscore' : 'PEG_trailing_sector_zscore',
            'P/S_sector_zscore' : 'ps_sector_zscore',
            'P/B_sector_zscore' : 'ptb_sector_zscore',
            'P/C_sector_zscore' : 'pcf_sector_zscore',
            'Dividend_sector_zscore' : 'DIVYIELD_sector_zscore',
            'ROA_sector_zscore' : 'roa_sector_zscore',
            'Recom_sector_zscore' : 'MEANREC_sector_zscore',
            'Curr R_sector_zscore' : 'curr_ratio_sector_zscore',
            'Quick R_sector_zscore' : 'quick_ratio_sector_zscore'
            })

# Clean - Train - Convert Labels
UpDown = preprocessing.LabelEncoder()
labels = UpDown.fit_transform(train.forward_twelve_month_return_up)

train['labels'] = labels

# Train Machine Learning model using historic data
model = RandomForestClassifier(n_estimators=100, criterion='entropy', max_depth=None, min_samples_split=12,
                             min_samples_leaf=4, max_features='auto', bootstrap=False)

model.fit(train[features], train['labels'])
predicted = np.array(model.predict_proba(realtime[features]))

predictions=pd.DataFrame(predicted, columns=UpDown.classes_)

# Predictions - Change Column Names
predictions = predictions.rename(columns = {
                                0:'Probability of Loss on Investment',
                                1:'Probability of Return on Investment',
                                 })

# Predictions - Add Spread of probability
predictions['Probability Gap'] = predictions['Probability of Return on Investment'] - predictions['Probability of Loss on Investment']

# Predictions/Raw-realtime - Merge Predictions and original scrape to avoid manipulated data in user viewing
realtimepredictions = pd.concat([predictions, realtime_raw], axis=1, join_axes=[predictions.index])

# Real-time predictions - Resort
realtimepredictions = realtimepredictions.sort_values(by=['Probability Gap', 'Probability of Return on Investment', 'Ticker'], ascending=[0, 0, 1])

# Real-time predictions - Write to CSV
realtimepredictions.to_csv('/data/W205_Final/W205_Project/realtime_model_predict/realtimepredictions.csv', index = True, index_label = 'Id'
