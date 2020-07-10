#!/usr/bin/env python
# coding: utf-8

# In[44]:


import pandas as pd
import requests
import json
import numpy as np
from datetime import date
from pandas.tseries.offsets import BDay

def b3sa_Options(token):
    r = requests.get(token)
    vJson = json.loads(r.content)
    vJson['token']
    optionList = 'https://arquivos.b3.com.br/api/download/?token={}'.format(vJson['token'])
    r1 = requests.get(optionList)
    data = r1.content.decode("ISO-8859-1").splitlines()
    df = pd.DataFrame(data) 
    df.dropna(inplace = True) 
    df = pd.DataFrame(df.applymap(str))
    df = df[0].str.split(";", n = 100, expand = True)
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.reset_index()
    df.drop("index", axis = 1, inplace=True)
    return df

token = 'https://arquivos.b3.com.br/api/download/requestname?fileName=InstrumentsConsolidated&date={}&recaptchaToken='.format(date.today().strftime("%Y-%m-%d"))
df=b3sa_Options(token)
df2 = df[["TckrSymb", "Asst", "SgmtNm", "SctyCtgyNm", "XprtnDt", "TradgEndDt", "OptnTp", "ExrcPric", "OptnStyle"]]
df2 = df2[df2['SctyCtgyNm']=='OPTION ON EQUITIES']
df2['ExrcPric'] = [x.replace(',', '.') for x in df2['ExrcPric']]
df2['ExrcPric'] = df2['ExrcPric'].astype(float)

lastBD = pd.datetime.today() - BDay(1)
lastBD = lastBD.strftime("%Y-%m-%d")
token = 'https://arquivos.b3.com.br/api/download/requestname?fileName=TradeInformationConsolidated&date={}'.format(lastBD)
df_trade=b3sa_Options(token)

df_trade = df_trade[(df_trade['SgmtNm']=='EQUITY PUT') | (df_trade['SgmtNm']=='EQUITY CALL')]
df_trade=df_trade[(df_trade['TradQty']!='')]
df_trade["Strike"]=0.0
df_trade["Ticker"]=""
df_trade["Validade"]=""
df_trade["Tipo"]=""
df_trade["Price"] = 0.0

for i in df_trade.TckrSymb:
    try:
        df_trade.ix[df_trade.loc[df_trade['TckrSymb']==i].index.values[0],'Strike'] = df2.loc[df2["TckrSymb"]==i].ExrcPric.values[0]
        df_trade.ix[df_trade.loc[df_trade['TckrSymb']==i].index.values[0],'Ticker'] = df2.loc[df2["TckrSymb"]==i].Asst.values[0]
        df_trade.ix[df_trade.loc[df_trade['TckrSymb']==i].index.values[0],'Validade'] = df2.loc[df2["TckrSymb"]==i].XprtnDt.values[0]
        df_trade.ix[df_trade.loc[df_trade['TckrSymb']==i].index.values[0],'Tipo'] = df2.loc[df2["TckrSymb"]==i].OptnStyle.values[0]
    except:
        pass
        
vTopacoes = pd.read_excel(r"C:\Users\kiyo_\Desktop\projects\options\Cotacao.xlsx")

for i in df_trade.Ticker:
    try:
        df_trade.ix[df_trade.loc[df_trade['Ticker']==i].index.values,'Price'] = vTopacoes.loc[vTopacoes[0]==i].Atual.values[0]
    except:
        pass
df_trade = df_trade[df_trade["Price"]!=0.0]
df_trade = df_trade[(df_trade["Price"]/df_trade["Strike"]>0.99) & (df_trade["Price"]/df_trade["Strike"]<1.01)]

new_df1=df_trade[["Ticker","TckrSymb","SgmtNm","Tipo","Validade","LastPric","TradQty","FinInstrmQty","Strike","Price"]]
new_df1=new_df1.reset_index()
new_df1=new_df1.drop(['index'], axis=1)
new_df1["LastPric"]=new_df1["LastPric"].apply(lambda x: x.replace(',','.'))
new_df1["LastPric"]=new_df1["LastPric"].astype(float)
new_df1["Validade"] = pd.to_datetime(new_df1["Validade"])
new_df1["Days"] = new_df1["Validade"].apply(lambda x: x - pd.datetime.today())
new_df1["Days"] = new_df1["Days"].apply(lambda x: x.days)
new_df1["TradQty"]=new_df1["TradQty"].astype(int)
new_df1 = new_df1[new_df1["TradQty"]>5]
new_df1 = new_df1[["Ticker","TckrSymb","SgmtNm","Validade","Tipo","Days","Strike"]]
new_df1["Ticker1"] = new_df1["Ticker"].apply(lambda x: "=@BULLDDE|MOFV!"+x)
new_df1["TckrSymb1"] = new_df1["TckrSymb"].apply(lambda x: "=@BULLDDE|MOFC!"+x)
new_df1.to_csv("Options_V2.csv")


# In[ ]:




