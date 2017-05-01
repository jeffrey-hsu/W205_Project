import requests
from bs4 import BeautifulSoup
import csv
import pdb
import datetime
import pandas as pd
import numpy as np

def floatToString(inputValue):
    result = ('%.15f' % inputValue).rstrip('0').rstrip('.')
    return '0' if result == '-0' else result

# Finviz - Set Format Template
url = "http://finviz.com/screener.ashx?v=152&f=ind_stocksonly&ft=4&c=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70"
response = requests.get(url)
html = response.content
soup = BeautifulSoup(html, "lxml")

# Finviz - Grab Stock Count for Page Iteration
firstcount = soup.find_all('option')
lastnum = len(firstcount) - 1
lastpagenum = firstcount[lastnum].attrs['value']
currentpage = int(lastpagenum)

# Finviz - Grab Variable Headers
titlesarray = []

titleslist = soup.find_all('td',{"class" : "table-top"})
titleslisttickerid = soup.find_all('td',{"class" : "table-top-s"})
titleticker = titleslisttickerid[0].text

for title in titleslist:
    titlesarray.append(title.text)

titlesarray.insert(1,titleticker)

# Finviz - Iterate through each page of screener and grab all data
# Create empty lists & counter for while loop
templist = []
alldata = []
i = 0

while(currentpage > 0):
    i += 1
    scraped = int(lastpagenum) - int(currentpage)
    print scraped, "stocks scraped of", int(lastpagenum)
    secondurl = "http://finviz.com/screener.ashx?v=152&f=ind_stocksonly&ft=4&r=" + str(currentpage) + "&c=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70"
    secondresponse = requests.get(secondurl)
    secondhtml = secondresponse.content
    secondsoup = BeautifulSoup(secondhtml, "lxml")
    stockdata = secondsoup.find_all('a', {"class" : "screener-link"})
    stockticker = secondsoup.find_all('a', {"class" : "screener-link-primary"})
    datalength = len(stockdata)
    tickerdatalength = len(stockticker)

    while(datalength > 0):
        templist = [stockdata[datalength - 70].text,
                    stockticker[tickerdatalength-1].text,
                    stockdata[datalength - 69].text,
                    stockdata[datalength - 68].text,
                    stockdata[datalength - 67].text,
                    stockdata[datalength - 66].text,
                    stockdata[datalength - 65].text,
                    stockdata[datalength - 64].text,
                    stockdata[datalength - 63].text,
                    stockdata[datalength - 62].text,
                    stockdata[datalength - 61].text,
                    stockdata[datalength - 60].text,
                    stockdata[datalength - 59].text,
                    stockdata[datalength - 58].text,
                    stockdata[datalength - 57].text,
                    stockdata[datalength - 56].text,
                    stockdata[datalength - 55].text,
                    stockdata[datalength - 54].text,
                    stockdata[datalength - 53].text,
                    stockdata[datalength - 52].text,
                    stockdata[datalength - 51].text,
                    stockdata[datalength - 50].text,
                    stockdata[datalength - 49].text,
                    stockdata[datalength - 48].text,
                    stockdata[datalength - 47].text,
                    stockdata[datalength - 46].text,
                    stockdata[datalength - 45].text,
                    stockdata[datalength - 44].text,
                    stockdata[datalength - 43].text,
                    stockdata[datalength - 42].text,
                    stockdata[datalength - 41].text,
                    stockdata[datalength - 40].text,
                    stockdata[datalength - 39].text,
                    stockdata[datalength - 38].text,
                    stockdata[datalength - 37].text,
                    stockdata[datalength - 36].text,
                    stockdata[datalength - 35].text,
                    stockdata[datalength - 34].text,
                    stockdata[datalength - 33].text,
                    stockdata[datalength - 32].text,
                    stockdata[datalength - 31].text,
                    stockdata[datalength - 30].text,
                    stockdata[datalength - 29].text,
                    stockdata[datalength - 28].text,
                    stockdata[datalength - 27].text,
                    stockdata[datalength - 26].text,
                    stockdata[datalength - 25].text,
                    stockdata[datalength - 24].text,
                    stockdata[datalength - 23].text,
                    stockdata[datalength - 22].text,
                    stockdata[datalength - 21].text,
                    stockdata[datalength - 20].text,
                    stockdata[datalength - 19].text,
                    stockdata[datalength - 18].text,
                    stockdata[datalength - 17].text,
                    stockdata[datalength - 16].text,
                    stockdata[datalength - 15].text,
                    stockdata[datalength - 14].text,
                    stockdata[datalength - 13].text,
                    stockdata[datalength - 12].text,
                    stockdata[datalength - 11].text,
                    stockdata[datalength - 10].text,
                    stockdata[datalength - 9].text,
                    stockdata[datalength - 8].text,
                    stockdata[datalength - 7].text,
                    stockdata[datalength - 6].text,
                    stockdata[datalength - 5].text,
                    stockdata[datalength - 4].text,
                    stockdata[datalength - 3].text,
                    stockdata[datalength - 2].text
                   ]

        alldata.append(templist)
        templist = []
        datalength -= 70
        tickerdatalength -= 1

    currentpage -= 20

print "Finviz scrape complete"

# Finviz - Write to CSV
with open('/data/W205_Final/W205_Project/stock_scrape/finviz.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, lineterminator='\n', fieldnames=titlesarray)

    writer.writeheader()
    for stock in alldata:
        writer.writerow({titlesarray[0] : stock[0],
                        titlesarray[1] : stock[1],
                        titlesarray[2] : stock[2],
                        titlesarray[3] : stock[3],
                        titlesarray[4] : stock[4],
                        titlesarray[5] : stock[5],
                        titlesarray[6] : stock[6],
                        titlesarray[7] : stock[7],
                        titlesarray[8] : stock[8],
                        titlesarray[9] : stock[9],
                        titlesarray[10] : stock[10],
                        titlesarray[11] : stock[11],
                        titlesarray[12] : stock[12],
                        titlesarray[13] : stock[13],
                        titlesarray[14] : stock[14],
                        titlesarray[15] : stock[15],
                        titlesarray[16] : stock[16],
                        titlesarray[17] : stock[17],
                        titlesarray[18] : stock[18],
                        titlesarray[19] : stock[19],
                        titlesarray[20] : stock[20],
                        titlesarray[21] : stock[21],
                        titlesarray[22] : stock[22],
                        titlesarray[23] : stock[23],
                        titlesarray[24] : stock[24],
                        titlesarray[25] : stock[25],
                        titlesarray[26] : stock[26],
                        titlesarray[27] : stock[27],
                        titlesarray[28] : stock[28],
                        titlesarray[29] : stock[29],
                        titlesarray[30] : stock[30],
                        titlesarray[31] : stock[31],
                        titlesarray[32] : stock[32],
                        titlesarray[33] : stock[33],
                        titlesarray[34] : stock[34],
                        titlesarray[35] : stock[35],
                        titlesarray[36] : stock[36],
                        titlesarray[37] : stock[37],
                        titlesarray[38] : stock[38],
                        titlesarray[39] : stock[39],
                        titlesarray[40] : stock[40],
                        titlesarray[41] : stock[41],
                        titlesarray[42] : stock[42],
                        titlesarray[43] : stock[43],
                        titlesarray[44] : stock[44],
                        titlesarray[45] : stock[45],
                        titlesarray[46] : stock[46],
                        titlesarray[47] : stock[47],
                        titlesarray[48] : stock[48],
                        titlesarray[49] : stock[49],
                        titlesarray[50] : stock[50],
                        titlesarray[51] : stock[51],
                        titlesarray[52] : stock[52],
                        titlesarray[53] : stock[53],
                        titlesarray[54] : stock[54],
                        titlesarray[55] : stock[55],
                        titlesarray[56] : stock[56],
                        titlesarray[57] : stock[57],
                        titlesarray[58] : stock[58],
                        titlesarray[59] : stock[59],
                        titlesarray[60] : stock[60],
                        titlesarray[61] : stock[61],
                        titlesarray[62] : stock[62],
                        titlesarray[63] : stock[63],
                        titlesarray[64] : stock[64],
                        titlesarray[65] : stock[65],
                        titlesarray[66] : stock[66],
                        titlesarray[67] : stock[67],
                        titlesarray[68] : stock[68],
                        titlesarray[69] : stock[69]
                        })

# df - Initiate dataframe using Finviz CSV
df = pd.read_csv('/data/W205_Final/W205_Project/stock_scrape/finviz.csv')

# df - Add Key Statistics Data from Yahoo Finance Queries
enterpriseValue = []
enterpriseToEbitda = []
enterpriseToRevenue = []
pegRatio = []
sharesShort = []
profitMargins = []
sharesOutstanding = []
heldPercentInstitutions = []
trailingEps = []
forwardEps = []
bookValue = []
earningsQuarterlyGrowth = []
heldPercentInsiders = []
netIncomeToCommon = []
forwardPE = []
priceToBook = []
yahooBeta = []

for stock in df['Ticker']:
    print 'Importing Key Statistics for ' + stock

    try:
        key_response = requests.get('https://query2.finance.yahoo.com/v10/finance/quoteSummary/' + stock +
                                '?formatted=true&crumb=8ldhetOu7RJ&lang=en-US&region=US&modules=' +
                                'defaultKeyStatistics%2CfinancialData%2CcalendarEvents&corsDomain=finance.yahoo.com')

        key_stats = key_response.json()


        # Enterprise Value
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseValue']['raw'] is not None:
                enterpriseValue.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseValue']['raw'])

        except:
            enterpriseValue.append("NA")

        # EV/EBITDA
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseToEbitda']['raw'] > 0:
                enterpriseToEbitda.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseToEbitda']['raw'])
            else:
                enterpriseToEbitda.append("NA")
        except:
            enterpriseToEbitda.append("NA")

        # EV/Revenue
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseToRevenue']['raw'] > 0:
                enterpriseToRevenue.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseToRevenue']['raw'])
            else:
                enterpriseToRevenue.append("NA")
        except:
            enterpriseToRevenue.append("NA")

        # PEG 5-Year
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['pegRatio']['raw'] is not None:
                pegRatio.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['pegRatio']['raw'])
            else:
                pegRatio.append("NA")
        except:
            pegRatio.append("NA")

        # Short Shares
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesShort']['raw'] is not None:
                sharesShort.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesShort']['raw'])
            else:
                sharesShort.append("NA")
        except:
            sharesShort.append("NA")

        # Profit Margin
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['profitMargins']['raw'] is not None:
                profitMargins.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['profitMargins']['raw'])
            else:
                profitMargins.append("NA")
        except:
            profitMargins.append("NA")

        # Outstanding Shares
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesOutstanding']['raw'] is not None:
                sharesOutstanding.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesOutstanding']['raw'])
            else:
                sharesOutstanding.append("NA")
        except:
            sharesOutstanding.append("NA")

        # Percent of Shares held by Institutions
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['heldPercentInstitutions']['raw'] is not None:
                heldPercentInstitutions.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['heldPercentInstitutions']['raw'])
            else:
                heldPercentInstitutions.append("NA")
        except:
            heldPercentInstitutions.append("NA")

        # Trailing EPS
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['trailingEps']['raw'] is not None:
                trailingEps.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['trailingEps']['raw'])
            else:
                trailingEps.append("NA")
        except:
            trailingEps.append("NA")

        # Forward EPS
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['forwardEps']['raw'] is not None:
                forwardEps.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['forwardEps']['raw'])
            else:
                forwardEps.append("NA")
        except:
            forwardEps.append("NA")

        # Bookings per Shares
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['bookValue']['raw'] is not None:
                bookValue.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['bookValue']['raw'])
            else:
                bookValue.append("NA")
        except:
            bookValue.append("NA")

        # Quarterly Earnings Growth
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['earningsQuarterlyGrowth']['raw'] is not None:
                earningsQuarterlyGrowth.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['earningsQuarterlyGrowth']['raw'])
            else:
                earningsQuarterlyGrowth.append("NA")
        except:
            earningsQuarterlyGrowth.append("NA")

        # Percent of Shares held by Insiders
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['heldPercentInsiders']['raw'] is not None:
                heldPercentInsiders.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['heldPercentInsiders']['raw'])
            else:
                heldPercentInsiders.append("NA")
        except:
            heldPercentInsiders.append("NA")

        # Net Income to Common
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['netIncomeToCommon']['raw'] is not None:
                netIncomeToCommon.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['netIncomeToCommon']['raw'])
            else:
                netIncomeToCommon.append("NA")
        except:
            netIncomeToCommon.append("NA")

        # Forward PE
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['forwardPE']['raw'] > 0:
                forwardPE.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['forwardPE']['raw'])
            else:
                forwardPE.append("NA")
        except:
            forwardPE.append("NA")

        # Price to Bookings
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['priceToBook']['raw'] > 0:
                priceToBook.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['priceToBook']['raw'])
            else:
                priceToBook.append("NA")
        except:
            priceToBook.append("NA")

        # Beta
        try:
            if key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['beta']['raw'] is not None:
                yahooBeta.append(key_stats['quoteSummary']['result'][0]['defaultKeyStatistics']['beta']['raw'])
            else:
                yahooBeta.append("NA")
        except:
            yahooBeta.append("NA")

    except:
        enterpriseValue.append("NA")
        enterpriseToEbitda.append("NA")
        enterpriseToRevenue.append("NA")
        pegRatio.append("NA")
        sharesShort.append("NA")
        profitMargins.append("NA")
        sharesOutstanding.append("NA")
        heldPercentInstitutions.append("NA")
        trailingEps.append("NA")
        forwardEps.append("NA")
        bookValue.append("NA")
        earningsQuarterlyGrowth.append("NA")
        heldPercentInsiders.append("NA")
        netIncomeToCommon.append("NA")
        forwardPE.append("NA")
        priceToBook.append("NA")
        yahooBeta.append("NA")

df['enterpriseValue'] = enterpriseValue
df['enterpriseToEbitda'] = enterpriseToEbitda
df['enterpriseToRevenue'] = enterpriseToRevenue
df['pegRatio'] = pegRatio
df['sharesShort'] = sharesShort
df['profitMargins'] = profitMargins
df['sharesOutstanding'] = sharesOutstanding
df['heldPercentInstitutions'] = heldPercentInstitutions
df['trailingEps'] = trailingEps
df['forwardEps'] = forwardEps
df['bookValue'] = bookValue
df['earningsQuarterlyGrowth'] = earningsQuarterlyGrowth
df['heldPercentInsiders'] = heldPercentInsiders
df['netIncomeToCommon'] = netIncomeToCommon
df['forwardPE'] = forwardPE
df['priceToBook'] = priceToBook
df['yahooBeta'] = yahooBeta

# df - Add Financials Data from Yahoo Finance Queries
totalDebt = []
operatingCashflow = []
operatingMargins = []
currentRatio = []
recommendationKey = []
grossProfits = []
returnOnAssets = []
totalCashPerShare = []
targetMedianPrice = []
targetHighPrice = []
totalRevenue = []
grossMargins = []
targetMeanPrice = []
revenueGrowth = []
earningsGrowth = []
revenuePerShare = []
ebitdaMargins = []
freeCashflow = []
recommendationMean = []
numberOfAnalystOpinions = []
quickRatio = []
totalCash = []
returnOnEquity = []
targetLowPrice = []
debtToEquity = []

for stock in df['Ticker']:
    print 'Importing Financials for ' + stock

    try:
        response = requests.get('https://query2.finance.yahoo.com/v10/finance/quoteSummary/' + stock +
                                '?formatted=true&crumb=8ldhetOu7RJ&lang=en-US&region=US&modules=' +
                                'defaultKeyStatistics%2CfinancialData%2CcalendarEvents&corsDomain=finance.yahoo.com')

        stats = response.json()


        # totalDebt
        try:
            if stats['quoteSummary']['result'][0]['financialData']['totalDebt']['raw'] is not None:
                totalDebt.append(stats['quoteSummary']['result'][0]['financialData']['totalDebt']['raw'])
            else:
                totalDebt.append("NA")

        except:
            totalDebt.append("NA")


        # operatingCashflow
        try:
            if stats['quoteSummary']['result'][0]['financialData']['operatingCashflow']['raw'] is not None:
                operatingCashflow.append(stats['quoteSummary']['result'][0]['financialData']['operatingCashflow']['raw'])
            else:
                operatingCashflow.append("NA")
        except:
            operatingCashflow.append("NA")

        # operatingMargins
        try:
            if stats['quoteSummary']['result'][0]['financialData']['operatingMargins']['raw'] is not None:
                operatingMargins.append(stats['quoteSummary']['result'][0]['financialData']['operatingMargins']['raw'])
            else:
                operatingMargins.append("NA")
        except:
            operatingMargins.append("NA")

        # currentRatio
        try:
            if stats['quoteSummary']['result'][0]['financialData']['currentRatio']['raw'] is not None:
                currentRatio.append(stats['quoteSummary']['result'][0]['financialData']['currentRatio']['raw'])
            else:
                currentRatio.append("NA")
        except:
            currentRatio.append("NA")

        # recommendationKey
        try:
            if stats['quoteSummary']['result'][0]['financialData']['recommendationKey'] is not None:
                recommendationKey.append(stats['quoteSummary']['result'][0]['financialData']['recommendationKey'])
            else:
                recommendationKey.append("NA")
        except:
            recommendationKey.append("NA")

        # grossProfits
        try:
            if stats['quoteSummary']['result'][0]['financialData']['grossProfits']['raw'] is not None:
                grossProfits.append(stats['quoteSummary']['result'][0]['financialData']['grossProfits']['raw'])
            else:
                grossProfits.append("NA")
        except:
            grossProfits.append("NA")

        # returnOnAssets
        try:
            if stats['quoteSummary']['result'][0]['financialData']['returnOnAssets']['raw'] is not None:
                returnOnAssets.append(stats['quoteSummary']['result'][0]['financialData']['returnOnAssets']['raw'])
            else:
                returnOnAssets.append("NA")
        except:
            returnOnAssets.append("NA")

        # totalCashPerShare
        try:
            if stats['quoteSummary']['result'][0]['financialData']['totalCashPerShare']['raw'] is not None:
                totalCashPerShare.append(stats['quoteSummary']['result'][0]['financialData']['totalCashPerShare']['raw'])

        except:
            totalCashPerShare.append("NA")

        # targetMedianPrice
        try:
            if stats['quoteSummary']['result'][0]['financialData']['targetMedianPrice']['raw'] is not None:
                targetMedianPrice.append(stats['quoteSummary']['result'][0]['financialData']['targetMedianPrice']['raw'])
            else:
                targetMedianPrice.append("NA")
        except:
            targetMedianPrice.append("NA")

        # targetHighPrice
        try:
            if stats['quoteSummary']['result'][0]['financialData']['targetHighPrice']['raw'] is not None:
                targetHighPrice.append(stats['quoteSummary']['result'][0]['financialData']['targetHighPrice']['raw'])
            else:
                targetHighPrice.append("NA")
        except:
            targetHighPrice.append("NA")

        # totalRevenue
        try:
            if stats['quoteSummary']['result'][0]['financialData']['totalRevenue']['raw'] is not None:
                totalRevenue.append(stats['quoteSummary']['result'][0]['financialData']['totalRevenue']['raw'])
            else:
                totalRevenue.append("NA")
        except:
            totalRevenue.append("NA")

        # grossMargins
        try:
            if stats['quoteSummary']['result'][0]['financialData']['grossMargins']['raw'] is not None:
                grossMargins.append(stats['quoteSummary']['result'][0]['financialData']['grossMargins']['raw'])
            else:
                grossMargins.append("NA")
        except:
            grossMargins.append("NA")

        # targetMeanPrice
        try:
            if stats['quoteSummary']['result'][0]['financialData']['targetMeanPrice']['raw'] is not None:
                targetMeanPrice.append(stats['quoteSummary']['result'][0]['financialData']['targetMeanPrice']['raw'])
            else:
                targetMeanPrice.append("NA")
        except:
            targetMeanPrice.append("NA")

        # revenueGrowth
        try:
            if stats['quoteSummary']['result'][0]['financialData']['revenueGrowth']['raw'] is not None:
                revenueGrowth.append(stats['quoteSummary']['result'][0]['financialData']['revenueGrowth']['raw'])
            else:
                revenueGrowth.append("NA")
        except:
            revenueGrowth.append("NA")

        # earningsGrowth
        try:
            if stats['quoteSummary']['result'][0]['financialData']['earningsGrowth']['raw'] is not None:
                earningsGrowth.append(stats['quoteSummary']['result'][0]['financialData']['earningsGrowth']['raw'])
            else:
                earningsGrowth.append("NA")
        except:
            earningsGrowth.append("NA")

        # revenuePerShare
        try:
            if stats['quoteSummary']['result'][0]['financialData']['revenuePerShare']['raw'] is not None:
                revenuePerShare.append(stats['quoteSummary']['result'][0]['financialData']['revenuePerShare']['raw'])
            else:
                revenuePerShare.append("NA")
        except:
            revenuePerShare.append("NA")

        # ebitdaMargins
        try:
            if stats['quoteSummary']['result'][0]['financialData']['ebitdaMargins']['raw'] is not None:
                ebitdaMargins.append(stats['quoteSummary']['result'][0]['financialData']['ebitdaMargins']['raw'])
            else:
                ebitdaMargins.append("NA")
        except:
            ebitdaMargins.append("NA")

        # freeCashflow
        try:
            if stats['quoteSummary']['result'][0]['financialData']['freeCashflow']['raw'] is not None:
                freeCashflow.append(stats['quoteSummary']['result'][0]['financialData']['freeCashflow']['raw'])
            else:
                freeCashflow.append("NA")
        except:
            freeCashflow.append("NA")

        # recommendationMean
        try:
            if stats['quoteSummary']['result'][0]['financialData']['recommendationMean']['raw'] is not None:
                recommendationMean.append(stats['quoteSummary']['result'][0]['financialData']['recommendationMean']['raw'])
            else:
                recommendationMean.append("NA")
        except:
            recommendationMean.append("NA")

        # numberOfAnalystOpinions
        try:
            if stats['quoteSummary']['result'][0]['financialData']['numberOfAnalystOpinions']['raw'] is not None:
                numberOfAnalystOpinions.append(stats['quoteSummary']['result'][0]['financialData']['numberOfAnalystOpinions']['raw'])
            else:
                numberOfAnalystOpinions.append("NA")
        except:
            numberOfAnalystOpinions.append("NA")

        # quickRatio
        try:
            if stats['quoteSummary']['result'][0]['financialData']['quickRatio']['raw'] is not None:
                quickRatio.append(stats['quoteSummary']['result'][0]['financialData']['quickRatio']['raw'])
            else:
                quickRatio.append("NA")
        except:
            quickRatio.append("NA")

        # totalCash
        try:
            if stats['quoteSummary']['result'][0]['financialData']['totalCash']['raw'] is not None:
                totalCash.append(stats['quoteSummary']['result'][0]['financialData']['totalCash']['raw'])
            else:
                totalCash.append("NA")
        except:
            totalCash.append("NA")

        # returnOnEquity
        try:
            if stats['quoteSummary']['result'][0]['financialData']['returnOnEquity']['raw'] is not None:
                returnOnEquity.append(stats['quoteSummary']['result'][0]['financialData']['returnOnEquity']['raw'])
            else:
                returnOnEquity.append("NA")
        except:
            returnOnEquity.append("NA")

        # targetLowPrice
        try:
            if stats['quoteSummary']['result'][0]['financialData']['targetLowPrice']['raw'] is not None:
                targetLowPrice.append(stats['quoteSummary']['result'][0]['financialData']['targetLowPrice']['raw'])
            else:
                targetLowPrice.append("NA")
        except:
            targetLowPrice.append("NA")

        # debtToEquity
        try:
            if stats['quoteSummary']['result'][0]['financialData']['debtToEquity']['raw'] is not None:
                debtToEquity.append(stats['quoteSummary']['result'][0]['financialData']['debtToEquity']['raw'])
            else:
                debtToEquity.append("NA")
        except:
            debtToEquity.append("NA")

    except:
        totalDebt.append("NA")
        operatingCashflow.append("NA")
        operatingMargins.append("NA")
        currentRatio.append("NA")
        recommendationKey.append("NA")
        grossProfits.append("NA")
        returnOnAssets.append("NA")
        totalCashPerShare.append("NA")
        targetMedianPrice.append("NA")
        targetHighPrice.append("NA")
        totalRevenue.append("NA")
        grossMargins.append("NA")
        targetMeanPrice.append("NA")
        revenueGrowth.append("NA")
        earningsGrowth.append("NA")
        revenuePerShare.append("NA")
        ebitdaMargins.append("NA")
        freeCashflow.append("NA")
        recommendationMean.append("NA")
        numberOfAnalystOpinions.append("NA")
        quickRatio.append("NA")
        totalCash.append("NA")
        returnOnEquity.append("NA")
        targetLowPrice.append("NA")
        debtToEquity.append("NA")

df['totalDebt'] = totalDebt
df['operatingCashflow'] = operatingCashflow
df['operatingMargins'] = operatingMargins
df['currentRatio'] = currentRatio
df['recommendationKey'] = recommendationKey
df['grossProfits'] = grossProfits
df['returnOnAssets'] = returnOnAssets
df['totalCashPerShare'] = totalCashPerShare
df['targetMedianPrice'] = targetMedianPrice
df['targetHighPrice'] = targetHighPrice
df['totalRevenue'] = totalRevenue
df['grossMargins'] = grossMargins
df['targetMeanPrice'] = targetMeanPrice
df['revenueGrowth'] = revenueGrowth
df['earningsGrowth'] = earningsGrowth
df['revenuePerShare'] = revenuePerShare
df['ebitdaMargins'] = ebitdaMargins
df['freeCashflow'] = freeCashflow
df['recommendationMean'] = recommendationMean
df['numberOfAnalystOpinions'] = numberOfAnalystOpinions
df['quickRatio'] = quickRatio
df['totalCash'] = totalCash
df['returnOnEquity'] = returnOnEquity
df['targetLowPrice'] = targetLowPrice
df['debtToEquity'] = debtToEquity

# df - Write to CSV
df.to_csv('/data/W205_Final/W205_Project/stock_scrape/realtime.csv')
print 'Stock scrape complete'
