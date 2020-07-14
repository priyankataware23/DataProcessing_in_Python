import pandas as pd


df=pd.read_csv("/Users/ptaware/PycharmProjects/WS/Files/crypto_prices.csv",usecols=["DateTime","Open","High","Low","Close","Volume","VolumeBTC","Symbol"]
               ,index_col="DateTime")

print(df.head())
df["difference"]=df["Open"] - df["Close"]


df2=df["Symbol"].unique()
print(df2)

df3=df[["Symbol","High","Low"]]
print(df3.head())

#get only rows where symbol=="EOC"
df4=df[df["Symbol"]=="EOC"]
print(df4)




