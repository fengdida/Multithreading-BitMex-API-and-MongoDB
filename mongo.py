from pymongo import MongoClient
from bitmex_websocket import BitMEXWebsocket
import json
import logging
from time import sleep
import threading
import concurrent.futures

client = MongoClient("mongodb+srv://root:root@bitmex-aw4tz.azure.mongodb.net/test?retryWrites=true&w=majority")
db=client.bitmex

#market={"_id":"market"}
#instrument={"_id":"instrument"}
#trades={"_id":"trades"}
#ticker={"_id":"ticker"}
#db.mkt.insert_many(market,instrument,trades,ticker)

def update_mkt():
    ws_mkt = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    while (ws_mkt.ws.sock.connected):
        market=ws_mkt.market_depth()
        market_order=dict()
        market_order["symbol"]=[]
        market_order["id"]=[]
        market_order["side"]=[]
        market_order["size"]=[]
        market_order["price"]=[]
        for i in market:
            market_order["symbol"].append(i["symbol"])
            market_order["id"].append(i["id"])
            market_order["side"].append(i["side"])
            market_order["size"].append(i["size"])
            market_order["price"].append(i["price"])
        db.mkt.update_one({"_id":"market"},{"$set":market_order})

def update_instrument():
    ws_ins = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    while (ws_ins.ws.sock.connected):
        instrument=ws_ins.get_instrument()
        db.mkt.update_one({"_id":"instrument"},{"$set":instrument})
        logging.info("instrument done")
        sleep(60)
		
def update_ticker():
    ws_ticker = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    while (ws_ticker.ws.sock.connected):
        ws_ticker.get_instrument()
        ticker=ws_ticker.get_ticker()
        db.mkt.update_one({"_id":"ticker"},{"$set":ticker})
        logging.info("ticker done")
        sleep(5)

def update_trades():
    ws_trades = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    while (ws_trades.ws.sock.connected):
        trades=ws_trades.recent_trades()
        db.mkt.update_one({"_id":"trades"},{"$set":{"recent_trades": trades}})
        logging.info("trades done")
        sleep(5)
		
		
def menu(name):
    if name=="trades":
        update_trades()
    elif name=="instrument":
        update_instrument()
    elif name=="market":
        update_mkt()
    elif name=="ticker":
        update_ticker()

def thread_function(name):
    logging.info("Thread %s: starting", name)
    menu(name)    


if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    menus=["instrument","ticker","market","trades"]
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(thread_function, menus)