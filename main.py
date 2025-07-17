import pandas as pd

company_data = pd.read_csv('PRC Data Breach Chronology - 1.13.20.csv', index_col=0)

company_df = pd.DataFrame(company_data)

CIK_data = pd.read_csv('CIK_year.csv', index_col=0)

df2 = pd.DataFrame(CIK_data)

unnamed_cols = company_df.columns[company_df.columns.str.contains('Unnamed:')].to_list()
empty_cols = lambda col: company_df[col].isnull().all()
remove_cols = [col for col in unnamed_cols]
df1 = company_df.drop(columns=remove_cols, axis=1)
df1.info()
df2.info()

#print(company_df.info())
print(df2.head(10))