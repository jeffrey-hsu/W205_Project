from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sc = SparkContext.getOrCreate()

'''
	1. loads the text datafile to pyspark
	2. apply schema and create DataFrame table
'''

financial_suite = sc.textFile("file:///data/staging/final/financial_suite.txt")
financial_suite_noHeader = financial_suite.zipWithIndex().filter(lambda (row,index): index > 0).keys()
financial_suite_tuple = financial_suite_noHeader.map(lambda x:(x.split(",")[1], x))

schemaString = "gvkey	permno	adate	qdate	public_date\
	CAPEI	BE	bm	evm	pe_op_basic	pe_op_dil	pe_exi\
	pe_inc	ps	pcf	dpr	npm	opmbd	opmad	gpm	ptpm\
	cfm	roa	roe	roce	efftax	aftret_eq	aftret_invcapx\
	aftret_equity	pretret_noa	pretret_earnat	GProf\
	equity_invcap	debt_invcap	totdebt_invcap	capital_ratio\
	int_debt	int_totdebt	cash_lt	invt_act	rect_act\
	debt_at	debt_ebitda	short_debt	curr_debt	lt_debt\
	profit_lct	ocf_lct	cash_debt	fcf_ocf	lt_ppent\
	dltt_be	debt_assets	debt_capital	de_ratio\
	intcov	intcov_ratio	cash_ratio	quick_ratio	curr_ratio\
	cash_conversion	inv_turn	at_turn	rect_turn	pay_turn\
	sale_invcap	sale_equity	sale_nwc	rd_sale	adv_sale\
	staff_sale	accrual	gsector	gicdesc	sp500	ptb	PEG_trailing\
	DIVYIELD	PEG_1yrforward	PEG_ltgforward	FFI5_desc	FFI5\
	FFI10_desc	FFI10	FFI12_desc	FFI12	FFI17_desc	FFI17\
	FFI30_desc	FFI30	FFI38_desc	FFI38	FFI48_desc	FFI48	FFI49_desc	FFI49"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split("\t")]
schema = StructType(fields)
financial_suite_schema = sqlContext.createDataFrame(financial_suite_noHeader, schema)


