import pandas as pd

excel_file = "example_excel.xlsx"
xl= pd.ExcelFile(excel_file)
last_sheet = xl.sheet_names[-1]
df = pd.read_excel(excel_file, sheet_name=last_sheet)
print(df.head())