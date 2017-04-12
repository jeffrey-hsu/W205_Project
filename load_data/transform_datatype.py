from sas7bdat import SAS7BDAT

''' 
	transforms rawdata file
	of firm financial ratio data 
	into text files 

'''

with SAS7BDAT('/data/staging/final/financial_suite.sas7bdat') as file:
	for row in file:
		# opens file with name of "financial_suite.txt"
		f = open("/data/staging/final/financial_suite.txt","a") 
		f.write(str(row)+"\n")
		f.close()
