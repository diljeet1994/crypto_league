import urllib
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json

r = requests.get("https://min-api.cryptocompare.com/data/pricemulti?fsyms=BTC,ETH,XRP,LTC,BCH,NEO,EOS,ETC,TRX,DASH,BNB,XMR,ADA,IOT,HT,QTUM,XRB,OCN,NCASH,XEM,ZEC,ICX,ONT,OMG,ABT,VEN,DGD,ELF,WAVES,LSK,BTG,IOST,SNT,ELA,NBT,CND,BCD,AION,ZIL,GVT,MTL,HSR,MTN,BTM,NEBL,SRN,VIBE,NULS,MCO,LINK,STRAT,XVG,THETA,BCPT,ADX,DOGE,RUFF,GNT,ENJ,NAS,DTA,QSP,MDS,PRO,RCN,STORJ,POWR,GAS,TNP&tsyms=GBP")
data_dict = r.json()
r1 = requests.get( "https://min-api.cryptocompare.com/data/pricemulti?fsyms=MANA,ETHOS,KNC,R,SALT,PAY,OC,CVC,WTC,ITC,SUB,SWFTC,SAN,INS,ZRX,SNC,ENG,BAT,SC,EDO,CMT,APPC,LUN,GNX,WPR,VIB,QASH,AST,GUP,GXS,FCT,ACT,TNT,REQ,QUN,EVX,OST,TRIG,SMT,AIDOC,USDT,BTS,RDD,VVI,DBC,MTX,SOC,STK,ARK,RPX,CTR,EKO,REP,XDN,PPT,BCN,BNT,DGB,AE,BTX,NKC,UTK,YEE,CS,LEND,ETP,DAT,MONA,OMNI,SLS,SKY&tsyms=GBP")
data_dict1 = r1.json()

# all currencies price and name
data_dict.update(data_dict1)

# format data as per requirement
df = pd.DataFrame.from_dict(data_dict)
df = df.T
df['date'] = datetime.now()
df['assetCode'] = df.index
df.rename(columns={'GBP':'assetPrice'}, inplace=True)
print df.head()
data_dict = df.to_dict(orient='records')
print len(data_dict)

# create db connection and dump records
username=urllib.quote_plus('elrondPriceFeedUser')
password = urllib.quote_plus('elrondPriceFeePassword@123')
uri = 'mongodb://'+username+':'+password+'@35.229.83.116:27017/elrondCurrencyPriceFeed?authSource=elrondCurrencyPriceFeed'
client = MongoClient(uri)
db = client['elrondCurrencyPriceFeed']
db.intraDayPriceFeedBO.insert_many(data_dict, ordered=False)


# delete 3 records older than 3 days
dt_three_days_old = datetime.now() - timedelta(3)
dt_three_days_old = dt_three_days_old.replace(minute=59, hour=23, second=59)
db.intraDayPriceFeedBO.delete_many({'date': {'$lte' : dt_three_days_old}})



#
#
#
#
#
# print datetime.now()
# username=urllib.quote_plus('elrondPriceFeedUser')
# password = urllib.quote_plus('elrondPriceFeePassword@123')
# uri = 'mongodb://'+username+':'+password+'@13.127.231.187:27017/elrondCurrencyPriceFeed?authSource=admin'
# client = MongoClient(uri)
# db = client['elrondCurrencyPriceFeed']
# print db
# # collection = db.priceFeedBO.aggregate([
# #     {
# #         '$project': {
# #             'date': 1,
# #             'priceData': 1
# #         },
# #         'redact':{
# #             '$cond':{
# #                 '$if':{
# #                     '$eq': [{'$hour': '$date'}, 11]
# #                 },
# #                 'then': '$$KEEP',
# #                 'else': '$$PRUNE'
# #             }
# #         }
# #         ,
# #         '$project':{
# #             'date': 1,
# #             'priceData':1
# #         }
# #     }
# # ])
#
# collection = db.priceFeedBO.aggregate([
#     {
#         '$redact':{
#             '$cond': {
#                 'if':{
#                      '$and':[
#                          {  '$eq': [{'$hour': '$date'}, 00]},
#                          {  '$gte': [{'$minute': '$date'}, 01]},
#                          {  '$lte': [{'$minute': '$date'}, 10]}
#                     ]
#                 },
#                 'then': '$$KEEP',
#                 'else': '$$PRUNE'
#             }
#         }
#     }
# ])
# dt = datetime.now() - timedelta(3)
# # collection = db.priceFeedBO.find({'date' : {$gte : }})
# df = pd.DataFrame(columns=['Currency', 'GBP', 'USD', 'Date'])
# import webapp2
# import urllib2

# class HourlyCronPage(webapp2.RequestHandler):
#     def get(self):
#         # response = urllib2.urlopen('<url_of_your_cloud_function>')
#         r = requests.get(
#             "https://min-api.cryptocompare.com/data/pricemulti?fsyms=BTC,ETH,XRP,LTC,BCH,NEO,EOS,ETC,TRX,DASH,BNB,XMR,ADA,IOT,HT,QTUM,XRB,OCN,NCASH,XEM,ZEC,ICX,ONT,OMG,ABT,VEN,DGD,ELF,WAVES,LSK,BTG,IOST,SNT,ELA,NBT,CND,BCD,AION,ZIL,GVT,MTL,HSR,MTN,BTM,NEBL,SRN,VIBE,NULS,MCO,LINK,STRAT,XVG,THETA,BCPT,ADX,DOGE,RUFF,GNT,ENJ,NAS,DTA,QSP,MDS,PRO,RCN,STORJ,POWR,GAS,TNP&tsyms=GBP")
#         data_dict = r.json()
#         r1 = requests.get(
#             "https://min-api.cryptocompare.com/data/pricemulti?fsyms=MANA,ETHOS,KNC,R,SALT,PAY,OC,CVC,WTC,ITC,SUB,SWFTC,SAN,INS,ZRX,SNC,ENG,BAT,SC,EDO,CMT,APPC,LUN,GNX,WPR,VIB,QASH,AST,GUP,GXS,FCT,ACT,TNT,REQ,QUN,EVX,OST,TRIG,SMT,AIDOC,USDT,BTS,RDD,VVI,DBC,MTX,SOC,STK,ARK,RPX,CTR,EKO,REP,XDN,PPT,BCN,BNT,DGB,AE,BTX,NKC,UTK,YEE,CS,LEND,ETP,DAT,MONA,OMNI,SLS,SKY&tsyms=GBP")
#         data_dict1 = r1.json()

#         # all currencies price and name
#         data_dict.update(data_dict1)

#         # format data as per requirement
#         df = pd.DataFrame.from_dict(data_dict)
#         df = df.T
#         df['date'] = datetime.now()
#         df['assetCode'] = df.index
#         df.rename(columns={'GBP': 'assetPrice'}, inplace=True)
#         print df.head()
#         data_dict = df.to_dict(orient='records')
#         print len(data_dict)

#         # create db connection and dump records
#         username = urllib.quote_plus('elrondPriceFeedUser')
#         password = urllib.quote_plus('elrondPriceFeePassword@123')
#         uri = 'mongodb://' + username + ':' + password + '@35.229.83.116:27017/elrondCurrencyPriceFeed?authSource=elrondCurrencyPriceFeed'
#         client = MongoClient(uri)
#         db = client['elrondCurrencyPriceFeed']
#         db.intraDayPriceFeedBO.insert_many(data_dict, ordered=False)

#         # delete 3 records older than 3 days
#         dt_three_days_old = datetime.now() - timedelta(3)
#         dt_three_days_old = dt_three_days_old.replace(minute=59, hour=23, second=59)
#         db.intraDayPriceFeedBO.delete_many({'date': {'$lte': dt_three_days_old}})

# # self.response.write(response.read())
# app = webapp2.WSGIApplication([
#     ('/hourly', HourlyCronPage),
# ], debug=True)