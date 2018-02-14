# -*- coding:utf-8 -*-

from api.binance_api import *
from api.bithumb_api import *
from comm.logger import logger
from comm.database import conn
import config
import time
import threading
import uuid

hedges = {}
orders = []
binance_orders = []
bithumb_buy_orders = []
bithumb_sell_orders = []
order_check = []
exception = []

threads = {}

def exception_handle():
    for order in exception:
        if order['status'] == 'NEW':


def check_hedge_status():
    while 1:
        for order in order_check:
            if order['exchange'] == 'binance':
                order_info = bnb.get_order_info(order['order_id'])
                if order_info['status'] == 'PARTIALLY_FILLED':
                    exception.append(order)
                    order_check.remove(order)

                hedges[order['hedge_id']]['binance_orders'][order['uuid']] = order_info
                order_check.remove(order)

def start_binance_trade():
    while 1:
        for order in binance_orders:
            if order["status"] == "NEW":
                if order["side"] == "BUY":
                    order_id, order_info = bnb.create_order(side=order['side'], price=order["price"], quantity=order["quantity"])
                else:
                    order_id, order_info = bnb.create_order(order['side'], order["price"], order["quantity"])
                if order_id > 0:
                    hedges[order['hedge_id']]['binance_orders'][order["uuid"]].update(order_info)
                    binance_orders.remove(order)
                    order_check.append(hedges[order['hedge_id']]['binance_orders'][order["uuid"]])
                    print order_info
                else:
                    exception.append(order)
                    logger.error("交易失败：%s" % order_info)

                binance_orders.remove(order)
        time.sleep(0.1)

def start_bithumb_trade():
    while 1:
        for order in bithumb_sell_orders:
            if order["status"] == "NEW":
                order_id, order_info = bhb.order_sell(price=order["price"], currency=order['symbol'], quantity=order["quantity"])
                if order_id > 0:
                    hedges[order['hedge_id']]['bithumb_buys'][order["uuid"]].update(order_info)

                    order_check.append(order)
                    print order_info
                else:
                    exception.append(order)
                    logger.error("交易失败：%s" % order_info)

                bithumb_buy_orders.remove(order)
        time.sleep(0.1)


if __name__ == '__main__':
    order_currency = config.order_currency
    payment_currency = config.payment_currency
    symbol = order_currency + '/' + payment_currency
    bnb = BinanceApi(config.binance_api_key, config.binance_api_secret, order_currency, payment_currency)

    bhb = BithumbApi(config.bithumb_api_key, config.bithumb_api_secret, order_currency, payment_currency)

    binance_trade = threading.Thread(target=start_binance_trade)
    binance_trade.setDaemon(True)
    binance_trade.start()
    threads["binance_trade"] = binance_trade

    bithumb_buy = threading.Thread(target=start_bithumb_buy)
    bithumb_buy.setDaemon(True)
    bithumb_buy.start()
    threads["bithumb_buy"] = bithumb_buy

    bithumb_sell = threading.Thread(target=start_bithumb_sell)
    bithumb_sell.setDaemon(True)
    bithumb_sell.start()
    threads["bithumb_sell"] = bithumb_sell

    time.sleep(5)
    print "@@@@@@@开始对冲，bithumb vs binance @@@@@@@@@@@@"
    while 1:
        bnb_price = bnb.get_current_price()
        bhb_price = bhb.get_current_price()

        if bhb_price != {} and (time.time()*1000 - bhb_price['timestamp']) > 1000:
            logger.info("*******bithumb 价格过期: %f **********" % ((time.time()*1000 - bhb_price['timestamp'])/1000))
            time.sleep(0.5)
            continue
        else:
            diff = bhb_price['price']/bnb_price['price'] - 1
            logger.info("----------当前价差: %f -------------" % diff)

            hedge_id = uuid.uuid1()
            if diff > config.profit_rate:
                bnb_depth = bnb.get_current_depth()
                bhb_depth = bhb.get_current_depth()
                if (time.time()*1000 - bnb_depth['timestamp']) > 1200 or (time.time()*1000 - bhb_depth['timestamp']) > 1200:
                    logger.info("深度数据超过1.2秒")
                    time.sleep(0.5)
                    continue
                '''binance买，bithumb卖'''
                quantity = 0

                if (bhb_depth[order_currency]['bids'][0]['price']/bhb_depth[payment_currency]['asks'][0]['price'])/bnb_depth['asks'][0]['price'] - 1 > config.profit_rate:

                    '''获取币的数量'''
                    bnb_asset = bnb.get_asset_balance()
                    bhb_asset = bhb.get_asset_balance()

                    '''bnb eth可兑换eos数量、bhb eos数量、bnb卖一eos数量、bhb买一eos数量、bhb eth卖一可兑换eos数量，取最小值'''
                    quantity = min(bnb_asset[payment_currency]['free']/bnb_depth['asks'][0]['price'],
                                   bhb_asset[order_currency]['free'],
                                   bnb_depth['asks'][0]['quantity'] * 0.5,
                                   bhb_depth[order_currency]['bids'][0]['quantity'] * 0.5,
                                   (bhb_depth[payment_currency]['asks'][0]['quantity']*bhb_depth[payment_currency]['asks'][0]['price']/bhb_depth[order_currency]['bids'][0]['price'])*0.5)

                '''币安最小交易量'''
                if quantity > 2:
                    binance_order = {
                        "exchange": "binance",
                        "uuid": uuid.uuid1(),
                        "side": "BUY",
                        "symbol": "EOSETH",
                        "price": bnb_depth['asks'][0]['price'],
                        "quantity": quantity,
                        "status": "NEW",
                        "hedge_id": hedge_id
                    }
                    bithumb_orders = {
                        "exchange": "bithumb",
                        "uuid": uuid.uuid1(),
                        "status": "NEW"
                        ,"hedge_id": hedge_id,
                        "bithumb_order_buy": {
                            "side": "bid",
                            "symbol": payment_currency,
                            "price": bhb_depth[payment_currency]['asks'][0]['price'],
                            "quantity": quantity
                        },
                        "bithumb_order_sell": {
                            "side": "ask",
                            "symbol": order_currency,
                            "price": bhb_depth[order_currency]['bids'][0]['price'],
                            "quantity": quantity
                        }
                    }

                    hedge = {
                        'bid_exchange': 'binance',
                        "ask_exchange": 'bithumb',
                        'symbol': symbol,
                        'quantity': quantity,
                        'status': 'create',
                        'time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                        'binance_orders': {
                            binance_order['uuid']: binance_order
                        },
                        'bithumb_buys': {
                            bithumb_order_buy['uuid']: bithumb_order_buy
                        },
                        'bithumb_sells': {
                            bithumb_order_sell['uuid']: bithumb_order_sell
                        }
                    }

                    hedges[hedge_id] = hedge
                    binance_orders.append(binance_order)
                    bithumb_buy_orders.append(bithumb_order_buy)
                    bithumb_sell_orders.append(bithumb_order_sell)
                    logger.info("binance: %s", binance_order)
                    logger.info("bithumb_buy: %s", bithumb_order_buy)
                    logger.info("bithumb_sell: %s", bithumb_order_sell)

            elif diff < 0 and diff < -config.profit_rate:
                bnb_depth = bnb.get_current_depth()
                bhb_depth = bhb.get_current_depth()
                if (time.time()*1000 - bnb_depth['timestamp']) > 1200 or (time.time()*1000 - bhb_depth['timestamp']) > 1200:
                    logger.info("深度数据超过1.2秒")
                    time.sleep(0.5)
                    continue
                '''binance卖，bithumb买'''
                quantity = 0
                if bnb_depth['bids'][0]['price']/(bhb_depth[order_currency]['asks'][0]['price'] / bhb_depth[payment_currency]['bids'][0]['price']) - 1 > config.profit_rate:

                    '''获取币的数量'''
                    bnb_asset = bnb.get_asset_balance()
                    bhb_asset = bhb.get_asset_balance()

                    '''bnb eos可出售数量、bhb eth可买eos数量、bnb买一eos数量、bhb卖一eos数量、bhb eth买一可兑换eos数量，取最小值'''
                    quantity = min(bnb_asset[order_currency]['free'],
                                  bhb_asset[payment_currency]['free']*bhb_depth[payment_currency]['bids'][0]['price']/bhb_depth[order_currency]['asks'][0]['price'],
                                   bnb_depth['bids'][0]['quantity'],
                                   bhb_depth[order_currency]['asks'][0]['quantity'],
                                   bhb_depth[payment_currency]['asks'][0]['quantity']*bhb_depth[payment_currency]['asks'][0]['price']/bhb_depth[order_currency]['bids'][0]['price'])
                if quantity > 2:
                    quantity = 2
                    binance_order = {
                        "exchange": "binance",
                        "uuid": uuid.uuid1(),
                        "side": "SELL",
                        "symbol": "EOSETH",
                        "price": bnb_depth['bids'][0]['price'],
                        "quantity": quantity,
                        "status": "NEW",
                        "hedge_id": hedge_id
                    }
                    bithumb_order_buy = {
                        "exchange": "bithumb",
                        "uuid": uuid.uuid1(),
                        "side": "bid",
                        "symbol": order_currency,
                        "price": bhb_depth[order_currency]['asks'][0]['price'],
                        "quantity": quantity,
                        "status": "NEW",
                        "hedge_id": hedge_id
                    }
                    bithumb_order_sell = {
                        "exchange": "bithumb",
                        "uuid": uuid.uuid1(),
                        "side": "ask",
                        "symbol": payment_currency,
                        "price": bhb_depth[payment_currency]['bids'][0]['price'],
                        "quantity": quantity,
                        "status": "NEW",
                        "hedge_id": hedge_id
                    }

                    hedge = {
                        'bid_exchange': 'binance',
                        "ask_exchange": 'bithumb',
                        'symbol': symbol,
                        'quantity': quantity,
                        'status': 'create',
                        'time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                        'binance_orders': {
                            binance_order['uuid']: binance_order
                        },
                        'bithumb_buys': {
                            bithumb_order_buy['uuid']: bithumb_order_buy
                        },
                        'bithumb_sells': {
                            bithumb_order_sell['uuid']: bithumb_order_sell
                        }
                    }

                    hedges[hedge_id] = hedge
                    binance_orders.append(binance_order)
                    bithumb_buy_orders.append(bithumb_order_buy)
                    bithumb_sell_orders.append(bithumb_order_sell)

                    logger.info("binance: %s", binance_order)
                    logger.info("bithumb_buy: %s", bithumb_order_buy)
                    logger.info("bithumb_sell: %s", bithumb_order_sell)




        time.sleep(1000000)


