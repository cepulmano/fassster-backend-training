# Importing required modules
import pandas as pd

if __name__ == '__main__':
	df = pd.read_parquet('df.linelist.gzip')
	location = "130000000"
	df = df.loc[(df["regionPSGC"] == location)]
	df = df.groupby(['Report_Date']).count()['max_Report_Date']
	print(df)