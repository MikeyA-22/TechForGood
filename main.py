'''
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

'''

import pandas as pd
from thefuzz import process, fuzz
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Load data
breach_file = 'PRC Data Breach.csv'
cik_file = 'CIK Year.csv'
output_file = 'breach_with_CIK_parallel.csv'
unmatched_file = 'unmatched_companies_parallel.csv'

df_breach = pd.read_csv(breach_file)
df_cik = pd.read_csv(cik_file, usecols=['CONM', 'cik'], dtype={'CONM': str, 'cik': str})

# Ensure consistent string types
df_breach['Company'] = df_breach['Company'].astype(str)
df_cik['CONM'] = df_cik['CONM'].astype(str)
cik_names = df_cik['CONM'].dropna().unique().tolist()


# Matching logic using fuzz.token_set_ratio
def match_company(company, strict=90, relaxed=70):
    match = process.extractOne(company, cik_names, scorer=fuzz.token_set_ratio)

    if match is None or match[1] < relaxed:
        return None, None, None, "No match"

    score = match[1]
    matched_name = match[0]
    cik_row = df_cik[df_cik['CONM'] == matched_name]

    cik = cik_row.iloc[0]['cik'] if not cik_row.empty else None
    match_type = "Strict" if score >= strict else "Relaxed"

    return cik, matched_name, score, match_type


# Apply match
results = df_breach['Company'].apply(lambda x: match_company(x))
df_breach['CIK'] = results.apply(lambda x: x[0])
df_breach['Matched Name'] = results.apply(lambda x: x[1])
df_breach['Match Score'] = results.apply(lambda x: x[2])
df_breach['Match Type'] = results.apply(lambda x: x[3])

# Save outputs
df_breach.to_csv('breach_with_CIK.csv', index=False)

# Unmatched companies
unmatched = df_breach[df_breach['CIK'].isna()][['Company']].drop_duplicates()
unmatched.to_csv('unmatched_companies.csv', index=False)

print("Done!")
print("- Saved: breach_with_CIK.csv")
print("- Unmatched: unmatched_companies.csv")