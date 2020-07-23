import redis
from bitmex_websocket import BitMEXWebsocket
from time import sleep
import json
import logging
import threading
import concurrent.futures

def get_trades():
    ws_trades = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    r=redis.Redis()
    while (ws_trades.ws.sock.connected):
        trades=ws_trades.recent_trades()
        with r.pipeline() as pipe:
            for index,trade in enumerate(trades):
                pipe.hset("trades", index, json.dumps(trade))
            pipe.execute()
        logging.info("trades done")
        sleep(5)

def get_instrument():
    ws_ins = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    r=redis.Redis()
    while (ws_ins.ws.sock.connected):
        instrument=ws_ins.get_instrument()
        with r.pipeline() as pipe:
            for col, value in instrument.items():
                pipe.hset("instrument", col, str(value))
            pipe.execute()
        logging.info("insturment done")
        sleep(60)

def get_mkt():
    ws_mkt = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    r=redis.Redis()
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
        with r.pipeline() as pipe:
            for col, value in market_order.items():
                pipe.hset("market_depth", col, str(value))
            pipe.execute()

def get_ticker():
    ws_ticker = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                         api_key=None, api_secret=None)
    r=redis.Redis()
    while (ws_ticker.ws.sock.connected):
        ws_ticker.get_instrument()
        ticker=ws_ticker.get_ticker()
        with r.pipeline() as pipe:
            for col, value in ticker.items():
                pipe.hset("ticker", col, str(value))
            pipe.execute()
        logging.info("ticker done")
        sleep(5)



def menu(name):
    if name=="trades":
        get_trades()
    elif name=="instrument":
        get_instrument()
    elif name=="market":
        get_mkt()
    elif name=="ticker":
        get_ticker()

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