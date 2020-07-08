#!/usr/bin/env python
# coding: utf-8

# In[84]:


import pandas as pd
import requests
import json
import numpy as np
from datetime import date
from pandas.tseries.offsets import BDay

resp = requests.get(url='http://cotacoes.economia.uol.com.br/ws/asset/stock/list')
all_stocks = resp.json()['data']

def get_stock_id(code):
    """ Get the stock id using its code """
    for stock in all_stocks:
        if stock['code'] == '%s.SA' % code:
            return stock['idt']
    raise Exception('invalid code %s' % code)
    
def get_stock_line(code):
    """ Returns the printable version of a stock, showing its current price and daily variation """
    id = get_stock_id(code)
    url = 'http://cotacoes.economia.uol.com.br/ws/asset/%d/intraday' % id
    resp = requests.get(url=url)
    data = resp.json()
    current = data['data'][0]['price']
    return current

token = 'https://arquivos.b3.com.br/api/download/requestname?fileName=InstrumentsConsolidated&date={}&recaptchaToken='.format(date.today().strftime("%Y-%m-%d"))
#token = 'https://arquivos.b3.com.br/api/download/requestname?fileName=InstrumentsConsolidated&date=2020-06-26&recaptchaToken='
r = requests.get(token)
vJson = json.loads(r.content)
vJson['token']
optionList = 'https://arquivos.b3.com.br/api/download/?token={}'.format(vJson['token'])
r1 = requests.get(optionList)

data = r1.content.decode("ISO-8859-1").splitlines()
df = pd.DataFrame(data) 
df1=df.copy()
df1.dropna(inplace = True) 
df1 = df1.applymap(str)
df1 = pd.DataFrame(df1)
df1 = df1[0].str.split(";", n = 100, expand = True)
df1.columns = df1.iloc[0]
df1 = df1[1:]
df1 = df1.reset_index()
df1.drop("index", axis = 1, inplace=True)

df2 = df1[["TckrSymb", "Asst", "SgmtNm", "SctyCtgyNm", "XprtnDt", "TradgEndDt", "OptnTp", "ExrcPric", "OptnStyle"]]
df2 = df2[df2['SctyCtgyNm']=='OPTION ON EQUITIES']
df2['ExrcPric'] = [x.replace(',', '.') for x in df2['ExrcPric']]
df2['ExrcPric'] = df2['ExrcPric'].astype(float)

lastBD = pd.datetime.today() - BDay(1)
lastBD = lastBD.strftime("%Y-%m-%d")
token = 'https://arquivos.b3.com.br/api/download/requestname?fileName=TradeInformationConsolidated&date={}'.format(lastBD)
#token = 'https://arquivos.b3.com.br/api/download/requestname?fileName=InstrumentsConsolidated&date=2020-06-26&recaptchaToken='
r = requests.get(token)
vJson = json.loads(r.content)
vJson['token']
optionList = 'https://arquivos.b3.com.br/api/download/?token={}'.format(vJson['token'])
r1 = requests.get(optionList)

data1 = r1.content.decode("ISO-8859-1").splitlines()
dfRaw2 = pd.DataFrame(data1) 
df_trade=dfRaw2.copy()
df_trade.dropna(inplace = True) 
df_trade = df_trade.applymap(str)
df_trade = pd.DataFrame(df_trade)
df_trade = df_trade[0].str.split(";", n = 100, expand = True)
df_trade.columns = df_trade.iloc[0]
df_trade = df_trade[1:]
df_trade = df_trade.reset_index()
df_trade.drop("index", axis = 1, inplace=True)
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
        
        
df_List = df_trade[['Ticker','Tipo','SgmtNm','TckrSymb','Validade','Strike','TradQty']]
df_List=df_List[df_List["Ticker"]!=""]
#vTopacoes=['ABEV3','B3SA3','BBAS3','BBDC4','BBSE3','BRKM5','BTOW3','COGN3','EGIE3','ENBR3','EVEN3','EZTC3','FLRY3','GGBR4','GNDI3','GOAU4','GRND3','GUAR3','HAPV3','HGTX3','ITSA4','ITUB4','LAME3','LAME4','LCAM3','LOGN3','LREN3','MDIA3','MGLU3','MULT3','NTCO3','ODPV3','PCAR3','PETR4','PRIO3','QUAL3','RADL3','RAIL3','RENT3','SANB11','SAPR11','SBSP3','SMTO3','STBP3','SULA11','SUZB3','TIMP3','TOTS3','VALE3','VIVT4','VVAR3','WEGE3','YDUQ3']
vTopacoes=['PETR4','VALE3','BOVA11','COGN3','BBDC4','VVAR3','ITUB4','CIEL3','IRBR3','BBAS3','ITSA4','ABEV3','USIM5','GGBR4','CSNA3','B3SA3','JBSS3','CMIG4','SUZB3','MRFG3','BRML3','CYRE3']


vTopacoes = pd.DataFrame(vTopacoes)
df_List["Filtro"]=df_List.Ticker.isin(vTopacoes[0]).astype(int)

df_List=df_List[df_List["Filtro"]==1]
df_List.drop("Filtro", axis = 1, inplace=True)

vTopacoes["Atual"] = ""
for i in vTopacoes[0]:
    vTopacoes.ix[vTopacoes.loc[vTopacoes[0]==i].index.values[0],'Atual'] = get_stock_line(i)

for i in df_trade.Ticker:
    try:
        df_trade.ix[df_trade.loc[df_trade['Ticker']==i].index.values,'Price'] = vTopacoes.loc[vTopacoes[0]==i].Atual.values[0]
        #df_trade.ix[df_trade.loc[df_trade['Ticker']==i].index.values[0],'Price'] = vTopacoes.loc[vTopacoes[0]==i].Atual.values[0]
    except:
        pass
    
df_trade.to_csv("opcoes.csv")
df_trade = df_trade[df_trade["Price"]!=0.0]
df_trade = df_trade[(df_trade["Price"]/df_trade["Strike"]>0.99) & (df_trade["Price"]/df_trade["Strike"]<1.01)]


new_df1=df_trade[["Ticker","TckrSymb","SgmtNm","Tipo","Validade","LastPric","TradQty","FinInstrmQty","Strike","Price"]]
new_df1=new_df1.reset_index()
new_df1=new_df1.drop(['index'], axis=1)
new_df1["LastPric"]=new_df1["LastPric"].apply(lambda x: x.replace(',','.'))
new_df1["LastPric"]=new_df1["LastPric"].astype(float)
new_df1["Validade"] = pd.to_datetime(new_df1["Validade"])
new_df1["Gain"]=new_df1["LastPric"]/new_df1["Price"]*100

new_df1["Days"] = new_df1["Validade"].apply(lambda x: x - pd.datetime.today())
new_df1["Days"] = new_df1["Days"].apply(lambda x: x.days)
new_df1["Month%"] = (new_df1["Gain"]/new_df1["Days"])*30
df_Call = new_df1[new_df1['SgmtNm']=='EQUITY CALL']
df_Put = new_df1[new_df1['SgmtNm']=='EQUITY PUT']

new_df1["Days"] = new_df1["Validade"].apply(lambda x: x - pd.datetime.today())
new_df1["Days"] = new_df1["Days"].apply(lambda x: x.days)
new_df1["Month%"] = (new_df1["Gain"]/new_df1["Days"])*30
df_Call = new_df1[new_df1['SgmtNm']=='EQUITY CALL']
df_Put = new_df1[new_df1['SgmtNm']=='EQUITY PUT']


export=new_df1.sort_values(by=['Month%'], ascending=False).head(30)
export=export[["Ticker","TckrSymb","SgmtNm","Validade","Tipo","Days","Strike"]]
export["Ticker1"] = export["Ticker"].apply(lambda x: "=@BULLDDE|MOFV!"+x)
export["TckrSymb1"] = export["TckrSymb"].apply(lambda x: "=@BULLDDE|MOFC!"+x)
export.to_csv("Options.csv")


# In[80]:






# In[81]:





# In[82]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[83]:





# In[ ]:





# In[ ]:




