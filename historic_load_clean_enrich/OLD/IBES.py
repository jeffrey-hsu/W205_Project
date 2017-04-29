# Load IBES Surprise History from Wharton
surprise_history = pd.read_csv('c:/users/shane/Desktop/W205_Final/sample_data/Sample_Surprise_History.csv', low_memory=False)

# # Add additional time based columns
surprise_history['anndats'] = pd.to_datetime(surprise_history['anndats'])
# # Create year
surprise_history['year'] = surprise_history['anndats'].dt.year
# # Create month
surprise_history['month'] = surprise_history['anndats'].dt.month
surprise_history['year-month'] = surprise_history['anndats'].apply(lambda x: x.strftime('%Y-%m'))
surprise_history["TIC-year-month"] = surprise_history["OFTIC"].map(str) + "-" + surprise_history["year-month"]

surprise_history = surprise_history.drop('year', 1)
surprise_history = surprise_history.drop('month', 1)
surprise_history = surprise_history.drop('year-month', 1)

# # Individual features for each surprise metric and duration
SAL_ANN = surprise_history.loc[(surprise_history.MEASURE == 'SAL') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NET_ANN = surprise_history.loc[(surprise_history.MEASURE == 'NET') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EPS_ANN = surprise_history.loc[(surprise_history.MEASURE == 'EPS') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

PRE_ANN = surprise_history.loc[(surprise_history.MEASURE == 'PRE') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBI_ANN = surprise_history.loc[(surprise_history.MEASURE == 'EBI') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GPS_ANN = surprise_history.loc[(surprise_history.MEASURE == 'GPS') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBT_ANN = surprise_history.loc[(surprise_history.MEASURE == 'EBT') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

DPS_ANN = surprise_history.loc[(surprise_history.MEASURE == 'DPS') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GRM_ANN = surprise_history.loc[(surprise_history.MEASURE == 'GRM') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NAV_ANN = surprise_history.loc[(surprise_history.MEASURE == 'NAV') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

BPS_ANN = surprise_history.loc[(surprise_history.MEASURE == 'BPS') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPX_ANN = surprise_history.loc[(surprise_history.MEASURE == 'CPX') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROE_ANN = surprise_history.loc[(surprise_history.MEASURE == 'ROE') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPS_ANN = surprise_history.loc[(surprise_history.MEASURE == 'CPS') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NDT_ANN = surprise_history.loc[(surprise_history.MEASURE == 'NDT') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROA_ANN = surprise_history.loc[(surprise_history.MEASURE == 'ROA') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ENT_ANN = surprise_history.loc[(surprise_history.MEASURE == 'ENT') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

OPR_ANN = surprise_history.loc[(surprise_history.MEASURE == 'OPR') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBS_ANN = surprise_history.loc[(surprise_history.MEASURE == 'EBS') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CSH_ANN = surprise_history.loc[(surprise_history.MEASURE == 'CSH') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

FFO_ANN = surprise_history.loc[(surprise_history.MEASURE == 'FFO') & (surprise_history.FISCALP == 'ANN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

SAL_QTR = surprise_history.loc[(surprise_history.MEASURE == 'SAL') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NET_QTR = surprise_history.loc[(surprise_history.MEASURE == 'NET') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EPS_QTR = surprise_history.loc[(surprise_history.MEASURE == 'EPS') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

PRE_QTR = surprise_history.loc[(surprise_history.MEASURE == 'PRE') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBI_QTR = surprise_history.loc[(surprise_history.MEASURE == 'EBI') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GPS_QTR = surprise_history.loc[(surprise_history.MEASURE == 'GPS') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBT_QTR = surprise_history.loc[(surprise_history.MEASURE == 'EBT') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

DPS_QTR = surprise_history.loc[(surprise_history.MEASURE == 'DPS') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GRM_QTR = surprise_history.loc[(surprise_history.MEASURE == 'GRM') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NAV_QTR = surprise_history.loc[(surprise_history.MEASURE == 'NAV') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

BPS_QTR = surprise_history.loc[(surprise_history.MEASURE == 'BPS') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPX_QTR = surprise_history.loc[(surprise_history.MEASURE == 'CPX') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROE_QTR = surprise_history.loc[(surprise_history.MEASURE == 'ROE') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPS_QTR = surprise_history.loc[(surprise_history.MEASURE == 'CPS') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NDT_QTR = surprise_history.loc[(surprise_history.MEASURE == 'NDT') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROA_QTR = surprise_history.loc[(surprise_history.MEASURE == 'ROA') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ENT_QTR = surprise_history.loc[(surprise_history.MEASURE == 'ENT') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

OPR_QTR = surprise_history.loc[(surprise_history.MEASURE == 'OPR') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBS_QTR = surprise_history.loc[(surprise_history.MEASURE == 'EBS') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CSH_QTR = surprise_history.loc[(surprise_history.MEASURE == 'CSH') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

FFO_QTR = surprise_history.loc[(surprise_history.MEASURE == 'FFO') & (surprise_history.FISCALP == 'QTR'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

SAL_SAN = surprise_history.loc[(surprise_history.MEASURE == 'SAL') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NET_SAN = surprise_history.loc[(surprise_history.MEASURE == 'NET') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EPS_SAN = surprise_history.loc[(surprise_history.MEASURE == 'EPS') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

PRE_SAN = surprise_history.loc[(surprise_history.MEASURE == 'PRE') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBI_SAN = surprise_history.loc[(surprise_history.MEASURE == 'EBI') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GPS_SAN = surprise_history.loc[(surprise_history.MEASURE == 'GPS') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBT_SAN = surprise_history.loc[(surprise_history.MEASURE == 'EBT') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

DPS_SAN = surprise_history.loc[(surprise_history.MEASURE == 'DPS') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GRM_SAN = surprise_history.loc[(surprise_history.MEASURE == 'GRM') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NAV_SAN = surprise_history.loc[(surprise_history.MEASURE == 'NAV') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

BPS_SAN = surprise_history.loc[(surprise_history.MEASURE == 'BPS') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPX_SAN = surprise_history.loc[(surprise_history.MEASURE == 'CPX') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROE_SAN = surprise_history.loc[(surprise_history.MEASURE == 'ROE') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPS_SAN = surprise_history.loc[(surprise_history.MEASURE == 'CPS') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NDT_SAN = surprise_history.loc[(surprise_history.MEASURE == 'NDT') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROA_SAN = surprise_history.loc[(surprise_history.MEASURE == 'ROA') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ENT_SAN = surprise_history.loc[(surprise_history.MEASURE == 'ENT') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

OPR_SAN = surprise_history.loc[(surprise_history.MEASURE == 'OPR') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBS_SAN = surprise_history.loc[(surprise_history.MEASURE == 'EBS') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CSH_SAN = surprise_history.loc[(surprise_history.MEASURE == 'CSH') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

FFO_SAN = surprise_history.loc[(surprise_history.MEASURE == 'FFO') & (surprise_history.FISCALP == 'SAN'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

SAL_LTG = surprise_history.loc[(surprise_history.MEASURE == 'SAL') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NET_LTG = surprise_history.loc[(surprise_history.MEASURE == 'NET') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EPS_LTG = surprise_history.loc[(surprise_history.MEASURE == 'EPS') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

PRE_LTG = surprise_history.loc[(surprise_history.MEASURE == 'PRE') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBI_LTG = surprise_history.loc[(surprise_history.MEASURE == 'EBI') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GPS_LTG = surprise_history.loc[(surprise_history.MEASURE == 'GPS') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBT_LTG = surprise_history.loc[(surprise_history.MEASURE == 'EBT') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

DPS_LTG = surprise_history.loc[(surprise_history.MEASURE == 'DPS') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

GRM_LTG = surprise_history.loc[(surprise_history.MEASURE == 'GRM') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NAV_LTG = surprise_history.loc[(surprise_history.MEASURE == 'NAV') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

BPS_LTG = surprise_history.loc[(surprise_history.MEASURE == 'BPS') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPX_LTG = surprise_history.loc[(surprise_history.MEASURE == 'CPX') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROE_LTG = surprise_history.loc[(surprise_history.MEASURE == 'ROE') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CPS_LTG = surprise_history.loc[(surprise_history.MEASURE == 'CPS') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

NDT_LTG = surprise_history.loc[(surprise_history.MEASURE == 'NDT') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ROA_LTG = surprise_history.loc[(surprise_history.MEASURE == 'ROA') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

ENT_LTG = surprise_history.loc[(surprise_history.MEASURE == 'ENT') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

OPR_LTG = surprise_history.loc[(surprise_history.MEASURE == 'OPR') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

EBS_LTG = surprise_history.loc[(surprise_history.MEASURE == 'EBS') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

CSH_LTG = surprise_history.loc[(surprise_history.MEASURE == 'CSH') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

FFO_LTG = surprise_history.loc[(surprise_history.MEASURE == 'FFO') & (surprise_history.FISCALP == 'LTG'), ['actual', 'surpmean', 'surpstdev', 'suescore', 'TIC-year-month']]

# # Rename the columns before merging
SAL_ANN = SAL_ANN.rename(columns={'actual': 'actual_SAL_ANN', 'surpmean': 'surpmean_SAL_ANN', 'surpstdev': 'surpstdev_SAL_ANN', 'suescore': 'suescore_SAL_ANN'})

NET_ANN = NET_ANN.rename(columns={'actual': 'actual_NET_ANN', 'surpmean': 'surpmean_NET_ANN', 'surpstdev': 'surpstdev_NET_ANN', 'suescore': 'suescore_NET_ANN'})

EPS_ANN = EPS_ANN.rename(columns={'actual': 'actual_EPS_ANN', 'surpmean': 'surpmean_EPS_ANN', 'surpstdev': 'surpstdev_EPS_ANN', 'suescore': 'suescore_EPS_ANN'})

PRE_ANN = PRE_ANN.rename(columns={'actual': 'actual_PRE_ANN', 'surpmean': 'surpmean_PRE_ANN', 'surpstdev': 'surpstdev_PRE_ANN', 'suescore': 'suescore_PRE_ANN'})

EBI_ANN = EBI_ANN.rename(columns={'actual': 'actual_EBI_ANN', 'surpmean': 'surpmean_EBI_ANN', 'surpstdev': 'surpstdev_EBI_ANN', 'suescore': 'suescore_EBI_ANN'})

GPS_ANN = GPS_ANN.rename(columns={'actual': 'actual_GPS_ANN', 'surpmean': 'surpmean_GPS_ANN', 'surpstdev': 'surpstdev_GPS_ANN', 'suescore': 'suescore_GPS_ANN'})

EBT_ANN = EBT_ANN.rename(columns={'actual': 'actual_EBT_ANN', 'surpmean': 'surpmean_EBT_ANN', 'surpstdev': 'surpstdev_EBT_ANN', 'suescore': 'suescore_EBT_ANN'})

DPS_ANN = DPS_ANN.rename(columns={'actual': 'actual_DPS_ANN', 'surpmean': 'surpmean_DPS_ANN', 'surpstdev': 'surpstdev_DPS_ANN', 'suescore': 'suescore_DPS_ANN'})

GRM_ANN = GRM_ANN.rename(columns={'actual': 'actual_GRM_ANN', 'surpmean': 'surpmean_GRM_ANN', 'surpstdev': 'surpstdev_GRM_ANN', 'suescore': 'suescore_GRM_ANN'})

NAV_ANN = NAV_ANN.rename(columns={'actual': 'actual_NAV_ANN', 'surpmean': 'surpmean_NAV_ANN', 'surpstdev': 'surpstdev_NAV_ANN', 'suescore': 'suescore_NAV_ANN'})

BPS_ANN = BPS_ANN.rename(columns={'actual': 'actual_BPS_ANN', 'surpmean': 'surpmean_BPS_ANN', 'surpstdev': 'surpstdev_BPS_ANN', 'suescore': 'suescore_BPS_ANN'})

CPX_ANN = CPX_ANN.rename(columns={'actual': 'actual_CPX_ANN', 'surpmean': 'surpmean_CPX_ANN', 'surpstdev': 'surpstdev_CPX_ANN', 'suescore': 'suescore_CPX_ANN'})

ROE_ANN = ROE_ANN.rename(columns={'actual': 'actual_ROE_ANN', 'surpmean': 'surpmean_ROE_ANN', 'surpstdev': 'surpstdev_ROE_ANN', 'suescore': 'suescore_ROE_ANN'})

CPS_ANN = CPS_ANN.rename(columns={'actual': 'actual_CPS_ANN', 'surpmean': 'surpmean_CPS_ANN', 'surpstdev': 'surpstdev_CPS_ANN', 'suescore': 'suescore_CPS_ANN'})

NDT_ANN = NDT_ANN.rename(columns={'actual': 'actual_NDT_ANN', 'surpmean': 'surpmean_NDT_ANN', 'surpstdev': 'surpstdev_NDT_ANN', 'suescore': 'suescore_NDT_ANN'})

ROA_ANN = ROA_ANN.rename(columns={'actual': 'actual_ROA_ANN', 'surpmean': 'surpmean_ROA_ANN', 'surpstdev': 'surpstdev_ROA_ANN', 'suescore': 'suescore_ROA_ANN'})

ENT_ANN = ENT_ANN.rename(columns={'actual': 'actual_ENT_ANN', 'surpmean': 'surpmean_ENT_ANN', 'surpstdev': 'surpstdev_ENT_ANN', 'suescore': 'suescore_ENT_ANN'})

OPR_ANN = OPR_ANN.rename(columns={'actual': 'actual_OPR_ANN', 'surpmean': 'surpmean_OPR_ANN', 'surpstdev': 'surpstdev_OPR_ANN', 'suescore': 'suescore_OPR_ANN'})

EBS_ANN = EBS_ANN.rename(columns={'actual': 'actual_EBS_ANN', 'surpmean': 'surpmean_EBS_ANN', 'surpstdev': 'surpstdev_EBS_ANN', 'suescore': 'suescore_EBS_ANN'})

CSH_ANN = CSH_ANN.rename(columns={'actual': 'actual_CSH_ANN', 'surpmean': 'surpmean_CSH_ANN', 'surpstdev': 'surpstdev_CSH_ANN', 'suescore': 'suescore_CSH_ANN'})

FFO_ANN = FFO_ANN.rename(columns={'actual': 'actual_FFO_ANN', 'surpmean': 'surpmean_FFO_ANN', 'surpstdev': 'surpstdev_FFO_ANN', 'suescore': 'suescore_FFO_ANN'})

SAL_QTR = SAL_QTR.rename(columns={'actual': 'actual_SAL_QTR', 'surpmean': 'surpmean_SAL_QTR', 'surpstdev': 'surpstdev_SAL_QTR', 'suescore': 'suescore_SAL_QTR'})

NET_QTR = NET_QTR.rename(columns={'actual': 'actual_NET_QTR', 'surpmean': 'surpmean_NET_QTR', 'surpstdev': 'surpstdev_NET_QTR', 'suescore': 'suescore_NET_QTR'})

EPS_QTR = EPS_QTR.rename(columns={'actual': 'actual_EPS_QTR', 'surpmean': 'surpmean_EPS_QTR', 'surpstdev': 'surpstdev_EPS_QTR', 'suescore': 'suescore_EPS_QTR'})

PRE_QTR = PRE_QTR.rename(columns={'actual': 'actual_PRE_QTR', 'surpmean': 'surpmean_PRE_QTR', 'surpstdev': 'surpstdev_PRE_QTR', 'suescore': 'suescore_PRE_QTR'})

EBI_QTR = EBI_QTR.rename(columns={'actual': 'actual_EBI_QTR', 'surpmean': 'surpmean_EBI_QTR', 'surpstdev': 'surpstdev_EBI_QTR', 'suescore': 'suescore_EBI_QTR'})

GPS_QTR = GPS_QTR.rename(columns={'actual': 'actual_GPS_QTR', 'surpmean': 'surpmean_GPS_QTR', 'surpstdev': 'surpstdev_GPS_QTR', 'suescore': 'suescore_GPS_QTR'})

EBT_QTR = EBT_QTR.rename(columns={'actual': 'actual_EBT_QTR', 'surpmean': 'surpmean_EBT_QTR', 'surpstdev': 'surpstdev_EBT_QTR', 'suescore': 'suescore_EBT_QTR'})

DPS_QTR = DPS_QTR.rename(columns={'actual': 'actual_DPS_QTR', 'surpmean': 'surpmean_DPS_QTR', 'surpstdev': 'surpstdev_DPS_QTR', 'suescore': 'suescore_DPS_QTR'})

GRM_QTR = GRM_QTR.rename(columns={'actual': 'actual_GRM_QTR', 'surpmean': 'surpmean_GRM_QTR', 'surpstdev': 'surpstdev_GRM_QTR', 'suescore': 'suescore_GRM_QTR'})

NAV_QTR = NAV_QTR.rename(columns={'actual': 'actual_NAV_QTR', 'surpmean': 'surpmean_NAV_QTR', 'surpstdev': 'surpstdev_NAV_QTR', 'suescore': 'suescore_NAV_QTR'})

BPS_QTR = BPS_QTR.rename(columns={'actual': 'actual_BPS_QTR', 'surpmean': 'surpmean_BPS_QTR', 'surpstdev': 'surpstdev_BPS_QTR', 'suescore': 'suescore_BPS_QTR'})

CPX_QTR = CPX_QTR.rename(columns={'actual': 'actual_CPX_QTR', 'surpmean': 'surpmean_CPX_QTR', 'surpstdev': 'surpstdev_CPX_QTR', 'suescore': 'suescore_CPX_QTR'})

ROE_QTR = ROE_QTR.rename(columns={'actual': 'actual_ROE_QTR', 'surpmean': 'surpmean_ROE_QTR', 'surpstdev': 'surpstdev_ROE_QTR', 'suescore': 'suescore_ROE_QTR'})

CPS_QTR = CPS_QTR.rename(columns={'actual': 'actual_CPS_QTR', 'surpmean': 'surpmean_CPS_QTR', 'surpstdev': 'surpstdev_CPS_QTR', 'suescore': 'suescore_CPS_QTR'})

NDT_QTR = NDT_QTR.rename(columns={'actual': 'actual_NDT_QTR', 'surpmean': 'surpmean_NDT_QTR', 'surpstdev': 'surpstdev_NDT_QTR', 'suescore': 'suescore_NDT_QTR'})

ROA_QTR = ROA_QTR.rename(columns={'actual': 'actual_ROA_QTR', 'surpmean': 'surpmean_ROA_QTR', 'surpstdev': 'surpstdev_ROA_QTR', 'suescore': 'suescore_ROA_QTR'})

ENT_QTR = ENT_QTR.rename(columns={'actual': 'actual_ENT_QTR', 'surpmean': 'surpmean_ENT_QTR', 'surpstdev': 'surpstdev_ENT_QTR', 'suescore': 'suescore_ENT_QTR'})

OPR_QTR = OPR_QTR.rename(columns={'actual': 'actual_OPR_QTR', 'surpmean': 'surpmean_OPR_QTR', 'surpstdev': 'surpstdev_OPR_QTR', 'suescore': 'suescore_OPR_QTR'})

EBS_QTR = EBS_QTR.rename(columns={'actual': 'actual_EBS_QTR', 'surpmean': 'surpmean_EBS_QTR', 'surpstdev': 'surpstdev_EBS_QTR', 'suescore': 'suescore_EBS_QTR'})

CSH_QTR = CSH_QTR.rename(columns={'actual': 'actual_CSH_QTR', 'surpmean': 'surpmean_CSH_QTR', 'surpstdev': 'surpstdev_CSH_QTR', 'suescore': 'suescore_CSH_QTR'})

FFO_QTR = FFO_QTR.rename(columns={'actual': 'actual_FFO_QTR', 'surpmean': 'surpmean_FFO_QTR', 'surpstdev': 'surpstdev_FFO_QTR', 'suescore': 'suescore_FFO_QTR'})

SAL_SAN = SAL_SAN.rename(columns={'actual': 'actual_SAL_SAN', 'surpmean': 'surpmean_SAL_SAN', 'surpstdev': 'surpstdev_SAL_SAN', 'suescore': 'suescore_SAL_SAN'})

NET_SAN = NET_SAN.rename(columns={'actual': 'actual_NET_SAN', 'surpmean': 'surpmean_NET_SAN', 'surpstdev': 'surpstdev_NET_SAN', 'suescore': 'suescore_NET_SAN'})

EPS_SAN = EPS_SAN.rename(columns={'actual': 'actual_EPS_SAN', 'surpmean': 'surpmean_EPS_SAN', 'surpstdev': 'surpstdev_EPS_SAN', 'suescore': 'suescore_EPS_SAN'})

PRE_SAN = PRE_SAN.rename(columns={'actual': 'actual_PRE_SAN', 'surpmean': 'surpmean_PRE_SAN', 'surpstdev': 'surpstdev_PRE_SAN', 'suescore': 'suescore_PRE_SAN'})

EBI_SAN = EBI_SAN.rename(columns={'actual': 'actual_EBI_SAN', 'surpmean': 'surpmean_EBI_SAN', 'surpstdev': 'surpstdev_EBI_SAN', 'suescore': 'suescore_EBI_SAN'})

GPS_SAN = GPS_SAN.rename(columns={'actual': 'actual_GPS_SAN', 'surpmean': 'surpmean_GPS_SAN', 'surpstdev': 'surpstdev_GPS_SAN', 'suescore': 'suescore_GPS_SAN'})

EBT_SAN = EBT_SAN.rename(columns={'actual': 'actual_EBT_SAN', 'surpmean': 'surpmean_EBT_SAN', 'surpstdev': 'surpstdev_EBT_SAN', 'suescore': 'suescore_EBT_SAN'})

DPS_SAN = DPS_SAN.rename(columns={'actual': 'actual_DPS_SAN', 'surpmean': 'surpmean_DPS_SAN', 'surpstdev': 'surpstdev_DPS_SAN', 'suescore': 'suescore_DPS_SAN'})

GRM_SAN = GRM_SAN.rename(columns={'actual': 'actual_GRM_SAN', 'surpmean': 'surpmean_GRM_SAN', 'surpstdev': 'surpstdev_GRM_SAN', 'suescore': 'suescore_GRM_SAN'})

NAV_SAN = NAV_SAN.rename(columns={'actual': 'actual_NAV_SAN', 'surpmean': 'surpmean_NAV_SAN', 'surpstdev': 'surpstdev_NAV_SAN', 'suescore': 'suescore_NAV_SAN'})

BPS_SAN = BPS_SAN.rename(columns={'actual': 'actual_BPS_SAN', 'surpmean': 'surpmean_BPS_SAN', 'surpstdev': 'surpstdev_BPS_SAN', 'suescore': 'suescore_BPS_SAN'})

CPX_SAN = CPX_SAN.rename(columns={'actual': 'actual_CPX_SAN', 'surpmean': 'surpmean_CPX_SAN', 'surpstdev': 'surpstdev_CPX_SAN', 'suescore': 'suescore_CPX_SAN'})

ROE_SAN = ROE_SAN.rename(columns={'actual': 'actual_ROE_SAN', 'surpmean': 'surpmean_ROE_SAN', 'surpstdev': 'surpstdev_ROE_SAN', 'suescore': 'suescore_ROE_SAN'})

CPS_SAN = CPS_SAN.rename(columns={'actual': 'actual_CPS_SAN', 'surpmean': 'surpmean_CPS_SAN', 'surpstdev': 'surpstdev_CPS_SAN', 'suescore': 'suescore_CPS_SAN'})

NDT_SAN = NDT_SAN.rename(columns={'actual': 'actual_NDT_SAN', 'surpmean': 'surpmean_NDT_SAN', 'surpstdev': 'surpstdev_NDT_SAN', 'suescore': 'suescore_NDT_SAN'})

ROA_SAN = ROA_SAN.rename(columns={'actual': 'actual_ROA_SAN', 'surpmean': 'surpmean_ROA_SAN', 'surpstdev': 'surpstdev_ROA_SAN', 'suescore': 'suescore_ROA_SAN'})

ENT_SAN = ENT_SAN.rename(columns={'actual': 'actual_ENT_SAN', 'surpmean': 'surpmean_ENT_SAN', 'surpstdev': 'surpstdev_ENT_SAN', 'suescore': 'suescore_ENT_SAN'})

OPR_SAN = OPR_SAN.rename(columns={'actual': 'actual_OPR_SAN', 'surpmean': 'surpmean_OPR_SAN', 'surpstdev': 'surpstdev_OPR_SAN', 'suescore': 'suescore_OPR_SAN'})

EBS_SAN = EBS_SAN.rename(columns={'actual': 'actual_EBS_SAN', 'surpmean': 'surpmean_EBS_SAN', 'surpstdev': 'surpstdev_EBS_SAN', 'suescore': 'suescore_EBS_SAN'})

CSH_SAN = CSH_SAN.rename(columns={'actual': 'actual_CSH_SAN', 'surpmean': 'surpmean_CSH_SAN', 'surpstdev': 'surpstdev_CSH_SAN', 'suescore': 'suescore_CSH_SAN'})

FFO_SAN = FFO_SAN.rename(columns={'actual': 'actual_FFO_SAN', 'surpmean': 'surpmean_FFO_SAN', 'surpstdev': 'surpstdev_FFO_SAN', 'suescore': 'suescore_FFO_SAN'})

SAL_LTG = SAL_LTG.rename(columns={'actual': 'actual_SAL_LTG', 'surpmean': 'surpmean_SAL_LTG', 'surpstdev': 'surpstdev_SAL_LTG', 'suescore': 'suescore_SAL_LTG'})

NET_LTG = NET_LTG.rename(columns={'actual': 'actual_NET_LTG', 'surpmean': 'surpmean_NET_LTG', 'surpstdev': 'surpstdev_NET_LTG', 'suescore': 'suescore_NET_LTG'})

EPS_LTG = EPS_LTG.rename(columns={'actual': 'actual_EPS_LTG', 'surpmean': 'surpmean_EPS_LTG', 'surpstdev': 'surpstdev_EPS_LTG', 'suescore': 'suescore_EPS_LTG'})

PRE_LTG = PRE_LTG.rename(columns={'actual': 'actual_PRE_LTG', 'surpmean': 'surpmean_PRE_LTG', 'surpstdev': 'surpstdev_PRE_LTG', 'suescore': 'suescore_PRE_LTG'})

EBI_LTG = EBI_LTG.rename(columns={'actual': 'actual_EBI_LTG', 'surpmean': 'surpmean_EBI_LTG', 'surpstdev': 'surpstdev_EBI_LTG', 'suescore': 'suescore_EBI_LTG'})

GPS_LTG = GPS_LTG.rename(columns={'actual': 'actual_GPS_LTG', 'surpmean': 'surpmean_GPS_LTG', 'surpstdev': 'surpstdev_GPS_LTG', 'suescore': 'suescore_GPS_LTG'})

EBT_LTG = EBT_LTG.rename(columns={'actual': 'actual_EBT_LTG', 'surpmean': 'surpmean_EBT_LTG', 'surpstdev': 'surpstdev_EBT_LTG', 'suescore': 'suescore_EBT_LTG'})

DPS_LTG = DPS_LTG.rename(columns={'actual': 'actual_DPS_LTG', 'surpmean': 'surpmean_DPS_LTG', 'surpstdev': 'surpstdev_DPS_LTG', 'suescore': 'suescore_DPS_LTG'})

GRM_LTG = GRM_LTG.rename(columns={'actual': 'actual_GRM_LTG', 'surpmean': 'surpmean_GRM_LTG', 'surpstdev': 'surpstdev_GRM_LTG', 'suescore': 'suescore_GRM_LTG'})

NAV_LTG = NAV_LTG.rename(columns={'actual': 'actual_NAV_LTG', 'surpmean': 'surpmean_NAV_LTG', 'surpstdev': 'surpstdev_NAV_LTG', 'suescore': 'suescore_NAV_LTG'})

BPS_LTG = BPS_LTG.rename(columns={'actual': 'actual_BPS_LTG', 'surpmean': 'surpmean_BPS_LTG', 'surpstdev': 'surpstdev_BPS_LTG', 'suescore': 'suescore_BPS_LTG'})

CPX_LTG = CPX_LTG.rename(columns={'actual': 'actual_CPX_LTG', 'surpmean': 'surpmean_CPX_LTG', 'surpstdev': 'surpstdev_CPX_LTG', 'suescore': 'suescore_CPX_LTG'})

ROE_LTG = ROE_LTG.rename(columns={'actual': 'actual_ROE_LTG', 'surpmean': 'surpmean_ROE_LTG', 'surpstdev': 'surpstdev_ROE_LTG', 'suescore': 'suescore_ROE_LTG'})

CPS_LTG = CPS_LTG.rename(columns={'actual': 'actual_CPS_LTG', 'surpmean': 'surpmean_CPS_LTG', 'surpstdev': 'surpstdev_CPS_LTG', 'suescore': 'suescore_CPS_LTG'})

NDT_LTG = NDT_LTG.rename(columns={'actual': 'actual_NDT_LTG', 'surpmean': 'surpmean_NDT_LTG', 'surpstdev': 'surpstdev_NDT_LTG', 'suescore': 'suescore_NDT_LTG'})

ROA_LTG = ROA_LTG.rename(columns={'actual': 'actual_ROA_LTG', 'surpmean': 'surpmean_ROA_LTG', 'surpstdev': 'surpstdev_ROA_LTG', 'suescore': 'suescore_ROA_LTG'})

ENT_LTG = ENT_LTG.rename(columns={'actual': 'actual_ENT_LTG', 'surpmean': 'surpmean_ENT_LTG', 'surpstdev': 'surpstdev_ENT_LTG', 'suescore': 'suescore_ENT_LTG'})

OPR_LTG = OPR_LTG.rename(columns={'actual': 'actual_OPR_LTG', 'surpmean': 'surpmean_OPR_LTG', 'surpstdev': 'surpstdev_OPR_LTG', 'suescore': 'suescore_OPR_LTG'})

EBS_LTG = EBS_LTG.rename(columns={'actual': 'actual_EBS_LTG', 'surpmean': 'surpmean_EBS_LTG', 'surpstdev': 'surpstdev_EBS_LTG', 'suescore': 'suescore_EBS_LTG'})

CSH_LTG = CSH_LTG.rename(columns={'actual': 'actual_CSH_LTG', 'surpmean': 'surpmean_CSH_LTG', 'surpstdev': 'surpstdev_CSH_LTG', 'suescore': 'suescore_CSH_LTG'})

FFO_LTG = FFO_LTG.rename(columns={'actual': 'actual_FFO_LTG', 'surpmean': 'surpmean_FFO_LTG', 'surpstdev': 'surpstdev_FFO_LTG', 'suescore': 'suescore_FFO_LTG'})


# # Add features to main dataframe
df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(NET_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(EPS_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(PRE_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(EBI_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(GPS_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(EBT_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(DPS_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(GRM_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(NAV_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(BPS_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(CPX_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(ROE_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(CPS_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(NDT_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(ROA_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(ENT_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(OPR_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(EBS_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(CSH_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(FFO_ANN, on='TIC-year-month', how='left', suffixes=('', '_y'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_SAL_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NET_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EPS_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_PRE_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBI_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_GPS_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBT_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_DPS_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_GRM_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NAV_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_BPS_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CPX_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ROE_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CPS_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NDT_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ROA_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ENT_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_OPR_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBS_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CSH_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_FFO_QRT'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_SAL_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NET_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EPS_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_PRE_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBI_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_GPS_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBT_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_DPS_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_GRM_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NAV_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_BPS_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CPX_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ROE_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CPS_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NDT_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ROA_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ENT_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_OPR_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBS_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CSH_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_FFO_SAN'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_SAL_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NET_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EPS_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_PRE_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBI_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_GPS_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBT_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_DPS_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_GRM_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NAV_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_BPS_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CPX_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ROE_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CPS_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_NDT_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ROA_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_ENT_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_OPR_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_EBS_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_CSH_LTG'))

df = df.merge(SAL_ANN, on='TIC-year-month', how='left', suffixes=('', '_FFO_LTG'))
