import pandas as pd


df = pd.read_csv('transactions_with_sideways_condition.csv', index_col = 0)
print(df.dtypes)
df['datetime'] = df['datetime'].astype(str)
print(df)
df['date'] = df['datetime'].str.slice(0,10)
df['time'] = df['datetime'].str.slice(11, 20)

df.to_csv('transactions_updated.csv')
print(df)
