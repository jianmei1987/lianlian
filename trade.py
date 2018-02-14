# -*- coding:utf-8 -*-

from api.binance_api import *
from api.bithumb_api import *
from comm.logger import logger
import config
import time
import threading
import uuid

hedges = []
threads = {}
total_profit = 0

def write_hedge_info(hedge):
    try:
        conn.execute('INSERT INTO hedge_info  VALUES (?,?,?,?,?,?,?,?)',
                     [str(hedge['hedge_id']), hedge['bid_exchange'], hedge['ask_exchange'], hedge['symbol'], hedge['quantity'],
                      hedge['profit'], hedge['status'], hedge['date_time']])

        for order_id, order_info in hedge['binance_orders'].items():
            conn.execute('INSERT INTO order_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                         [order_id, 'binance', order_info['symbol'], order_info['side'], order_info['status'],
                          order_info['order_price'], order_info['deal_price'], order_info['order_quantity'],
                          order_info['deal_quantity'], order_info['order_amount'], order_info['deal_amount'], order_info['deal_fee'],
                          order_info['fee_asset'], order_info['date_time'], str(hedge['hedge_id'])])
            for trade in order_info['trades'].values():
                conn.execute('insert into trade_info values (?,?,?,?,?,?,?)',
                             [trade['trade_id'], trade['commission'], trade['commissionAsset'], trade['order_id'],
                              trade['price'], trade['quantity'], trade['date_time']])

            for order_id, order_info in hedge['bithumb_buys'].items():
                conn.execute('INSERT INTO order_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                             [order_id, 'bithumb', order_info['symbol'], order_info['side'],
                              order_info['status'], order_info['order_price'], order_info['deal_price'],
                              order_info['order_quantity'], order_info['deal_quantity'], order_info['order_amount'],
                              order_info['deal_amount'], order_info['deal_fee'], order_info['fee_asset'], order_info['date_time'],
                              str(hedge['hedge_id'])])
                for trade in order_info['trades'].values():
                    conn.execute('insert into trade_info values (?,?,?,?,?,?,?)',
                                 [trade['trade_id'], trade['commission'], trade['commissionAsset'], trade['order_id'],
                                  trade['price'], trade['quantity'], trade['date_time']])

            for order_id, order_info in hedge['bithumb_sells'].items():
                conn.execute('INSERT INTO order_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                             [order_id, 'bithumb', order_info['symbol'], order_info['side'],
                              order_info['status'], order_info['order_price'], order_info['deal_price'],
                              order_info['order_quantity'], order_info['deal_quantity'], order_info['order_amount'],
                              order_info['deal_amount'], order_info['deal_fee'], order_info['fee_asset'], order_info['date_time'],
                              str(hedge['hedge_id'])])
                for trade in order_info['trades'].values():
                    conn.execute('insert into trade_info values (?,?,?,?,?,?,?)',
                                 [trade['trade_id'], trade['commission'], trade['commissionAsset'], trade['order_id'],
                                  trade['price'], trade['quantity'], trade['date_time']])
    except BaseException, e:
        logger.exception("write_hedge_info: %s", e)

    conn.commit()


def check_order_status():
    while 1:
        bnb_order_id = -1
        bhb_order_sell_id = -1
        bhb_order_buy_id = -1
        bnb_order_info = None
        bhb_order_sell_info = None
        bhb_order_buy_info = None
        for hedge in hedges:
            for bnb_order_id, bnb_order_info in hedge['binance_orders'].items():
                if bnb_order_info is not None:
                    if bnb_order_info['status'] == 'CANCLED':
                        continue
                    else:
                        break
                    #bnb_order_info = bnb.get_order_info(bnb_order_id)
                    #hedge['binance_orders'][bnb_order_id] = bnb_order_info

            for bhb_order_sell_id, bhb_order_sell_info in hedge['bithumb_sells'].items():
                if bhb_order_sell_info is not None:
                    if bhb_order_sell_info['status'] == "CANCLED":
                        continue
                    else:
                        break
                    #bhb_order_sell_info = bhb.get_order_status(bhb_order_sell_id)
                    #if bhb_order_sell_info is not None:
                     #   hedge['bithumb_sells'][bhb_order_sell_id] = bhb_order_sell_info

            for bhb_order_buy_id, bhb_order_buy_info in hedge['bithumb_buys'].items():
                if bhb_order_buy_info is not None:
                    if bhb_order_buy_info['status'] == "CANCLED":
                        continue
                    else:
                        break
                    #bhb_order_buy_info = bhb.get_order_status(bhb_order_buy_id)
                    #if bhb_order_buy_info is not None:
                     #   hedge['bithumb_buys'][bhb_order_buy_id] = bhb_order_buy_info

            '''1,1,1:全部成功，直接入库'''
            try:

                if (bnb_order_info is not None and bnb_order_info['status'] == 'FILLED') and (
                        bhb_order_buy_info is not None and bhb_order_buy_info['status'] == 'FILLED') and (
                        bhb_order_sell_info is not None and bhb_order_sell_info['status'] == 'FILLED'):
                    logger.info(bnb_order_info)
                    logger.info(bhb_order_buy_info)
                    logger.info(bhb_order_sell_info)
                    hedge['status'] = 'success'

                    if bnb_order_info['side'] == 'BUY':
                        hedge['profit'] = bhb_order_buy_info['deal_quantity'] - bnb_order_info['deal_amount']
                    else:
                        hedge['profit'] = bnb_order_info['deal_amount'] - bhb_order_sell_info['deal_quantity']

                    logger.info("hedge success: %s", hedge)
                    write_hedge_info(hedge)
                    hedges.remove(hedge)
                else:
                    logger.error("对冲未成功：%s", hedge)
                    bnb_order_info = bnb.get_order_info(bnb_order_id)
                    bhb_order_sell_info = bhb.get_order_status(bhb_order_sell_id)
                    bhb_order_buy_info = bhb.get_order_status(bhb_order_buy_id)
                    if bnb_order_info is not None:
                        hedge['binance_orders'][bnb_order_id] = bnb_order_info
                    if bhb_order_buy_info is not None:
                        hedge['bithumb_buys'][bhb_order_buy_id] = bhb_order_buy_info
                    if bhb_order_sell_info is not None:
                        hedge['bithumb_sells'][bhb_order_sell_id] = bhb_order_sell_info

            except BaseException, e:
                logger.exception("check_order_status: %s", e)

                '''0,0,0:全部不成功，直接取消交易
            elif bnb_order_info['status'] == 'PARTIALLY_FILLED' and bhb_order_buy_info['status'] == 'PARTIALLY_FILLED' and bhb_order_sell_info['status'] == 'PARTIALLY_FILLED':
                bnb.order_cancel(bnb_order_id)
                bhb.order_cancel(bhb_order_buy_id)
                bhb.order_cancel(bhb_order_sell_id)

                0,0,1:binance不成功，bithumb买入不成功，检查目前深度数据是否适合继续交易，如果适合，把剩余的量交易掉
            #elif bnb_order_info['status'] == 'PARTIALLY_FILLED' and bhb_order_buy_info['status'] == 'PARTIALLY_FILLED' and bhb_order_sell_info['status'] == 'FILLED':

                0,1,0:binance不成功，bithumb卖出不成功
            #elif bnb_order_info['status'] == 'PARTIALLY_FILLED' and bhb_order_buy_info['status'] == 'FILLED' and bhb_order_sell_info['status'] == 'PARTIALLY_FILLED':

                0,1,1:binance不成功
            elif bnb_order_info['status'] == 'PARTIALLY_FILLED' and bhb_order_buy_info['status'] == 'FILLED' and bhb_order_sell_info['status'] == 'FILLED':
                bnb_depth = bnb.get_current_depth()
                if bnb_order_info['side'] == 'BUY' and ((bhb_order_sell_info['deal_price'] / bhb_order_buy_info['deal_price']) / bnb_depth['asks'][0]['price'] - 1 > config.profit_rate):
                    tries = 0
                    while tries < 5:
                        bnb_new_order_id, bnb_new_order_info = bnb.create_order("BUY", bnb_depth['asks'][0]['price'], bnb_order_info['order_quantity'] - bnb_order_info['deal_quantity'])
                        if bnb_new_order_id > 0:
                            break
                        tries += 1
                        time.sleep(0.1)
                    if tries == 5:
                        logger.error("bnb未成交重新交易失败，维持原交易")
                    else:
                        bnb.order_cancel(bnb_order_id)
                        hedge['binance_orders'][bnb_new_order_id] = bnb_new_order_info
                elif bnb_order_info['side'] == 'SELL' and (bnb_depth['bids'][0]['price']/ (bhb_order_sell_info['deal_price'] / bhb_order_buy_info['deal_price']) - 1 > config.profit_rate):
                    tries = 0
                    while tries < 5:
                        bnb_new_order_id, bnb_new_order_info = bnb.create_order("SELL", bnb_depth['bids'][0]['price'],
                                                                                bnb_order_info['order_quantity'] -
                                                                                bnb_order_info['deal_quantity'])
                        if bnb_new_order_id > 0:
                            break
                        tries += 1
                        time.sleep(0.1)
                    if tries == 5:
                        logger.error("bnb未成交重新交易失败，维持原交易")
                    else:
                        bnb.order_cancel(bnb_order_id)
                        hedge['binance_orders'][bnb_new_order_id] = bnb_new_order_info

                1,0,0:bithumb买入、卖出都不成功
            #elif bnb_order_info['status'] == 'FILLED' and bhb_order_buy_info['status'] == 'PARTIALLY_FILLED' and bhb_order_sell_info['status'] == 'PARTIALLY_FILLED':

                1,0,1:bithumb买入不成功
            elif bnb_order_info['status'] == 'FILLED' and bhb_order_buy_info['status'] == 'PARTIALLY_FILLED' and bhb_order_sell_info['status'] == 'FILLED':
                bhb_depth = bhb.get_current_depth()
                if bnb_order_info['side'] == 'BUY' and ((bhb_order_sell_info['deal_price']/bhb_depth[payment_currency]['asks'][0]['price']) / bnb_order_info['deal_price'] - 1 > config.profit_rate):
                    tries = 0
                    while tries < 5:
                        bhb_new_order_buy_id, bhb_new_order_buy_info = bhb.order_buy(bhb_depth[payment_currency]['asks'][0]['price'], bhb_order_buy_info['order_quantity'] - bhb_order_buy_info['deal_quantity'], payment_currency)
                        if bhb_new_order_buy_id > 0:
                            break
                        tries += 1
                        time.sleep(0.1)
                    if tries == 5:
                        logger.error("bhb未成交重新交易失败，维持原交易")
                    else:
                        bhb.order_cancel(bhb_order_buy_id)
                        hedge['bithumb_buys'][bhb_new_order_buy_id] = bhb_new_order_buy_info
                elif bnb_order_info['side'] == 'SELL' and (bnb_order_info['deal_price']/(bhb_depth[order_currency]['asks'][0]['price'] / bhb_order_sell_info['deal_price']) - 1 > config.profit_rate):
                    tries = 0
                    while tries < 5:
                        bhb_new_order_buy_id, bhb_new_order_buy_info = bhb.order_buy(
                            bhb_depth[order_currency]['asks'][0]['price'],
                            bhb_order_buy_info['order_quantity'] - bhb_order_buy_info['deal_quantity'],
                            payment_currency)
                        if bhb_new_order_buy_id > 0:
                            break
                        tries += 1
                        time.sleep(0.1)
                    if tries == 5:
                        logger.error("bhb未成交重新交易失败，维持原交易")
                    else:
                        bhb.order_cancel(bhb_order_buy_id)
                        hedge['bithumb_buys'][bhb_new_order_buy_id] = bhb_new_order_buy_info

                1,1,0:bithumb卖出不成功
            #elif bnb_order_info['status'] == 'FILLED' and bhb_order_buy_info['status'] == 'FILLED' and bhb_order_sell_info['status'] == 'PARTIALLY_FILLED':'''


        time.sleep(1)

if __name__ == '__main__':

    order_currency = config.order_currency
    payment_currency = config.payment_currency
    symbol = order_currency + '/' + payment_currency
    bnb = BinanceApi(config.binance_api_key, config.binance_api_secret, order_currency, payment_currency)

    bhb = BithumbApi(config.bithumb_api_key, config.bithumb_api_secret, order_currency, payment_currency)

    check_order_status = threading.Thread(target=check_order_status)
    check_order_status.setDaemon(True)
    check_order_status.start()
    threads["check_order_status"] = check_order_status

    time.sleep(20)
    print "@@@@@@@开始对冲，bithumb vs binance @@@@@@@@@@@@"
    while 1:
        '''当前已经有4个未成功的对冲，等待'''
        if len(hedges) > 3:
            logger.info("当前已经有4个未成功的对冲，等待")
            print ("当前已经有4个未成功的对冲，等待")
            time.sleep(1)
            continue
            '''bnb_price = bnb.get_current_price()
            bhb_price = bhb.get_current_price()
    
            if bhb_price != {} and (time.time()*1000 - bhb_price['timestamp']) > 1000:
                logger.info("*******bithumb 价格过期: %f **********" % ((time.time()*1000 - bhb_price['timestamp'])/1000))
                print ("*******bithumb 价格过期: %f **********" % ((time.time()*1000 - bhb_price['timestamp'])/1000))
                time.sleep(1)
                continue
            else:
                hedge_id = uuid.uuid1()
                binance_order = None
                bithumb_order_sell = None
                bithumb_order_buy = None
                print ("----------当前价差(bhb vs bnb): %f -------------" % (bhb_price['price'] / bnb_price['price'] - 1))
                if bhb_price['price']/bnb_price['price'] - 1 > -0.03:
                    logger.info("----------当前价差(bhb vs bnb): %f -------------" % (bhb_price['price']/bnb_price['price'] - 1))'''

        hedge_id = uuid.uuid1()
        binance_order = None
        bithumb_order_sell = None
        bithumb_order_buy = None

        bnb_depth = bnb.get_current_depth()
        bhb_depth = bhb.get_current_depth()
        if (time.time()*1000 - bnb_depth['timestamp']) > 1200 or (time.time()*1000 - bhb_depth['timestamp']) > 1200:
            logger.info("深度数据超过1.2秒")
            print ("深度数据超过1.2秒")
            time.sleep(1)
            continue

        '''获取币的数量'''
        bnb_asset = bnb.get_asset_balance()
        bhb_asset = bhb.get_asset_balance()

        if bnb_asset == {} or bhb_asset == {}:
            time.sleep(1)
            continue

        logger.info ("***********deep diff(bhb vs bnb):%f*************",
               (bhb_depth[order_currency]['bids'][0]['price'] / bhb_depth[payment_currency]['asks'][0]['price']) /
               bnb_depth['asks'][0]['price'] - 1)

        logger.info ("***********deep diff(bnb vs bhb):%f*************", bnb_depth['bids'][0]['price'] / (
                bhb_depth[order_currency]['asks'][0]['price'] / bhb_depth[payment_currency]['bids'][0]['price']) - 1)

        '''binance买，bithumb卖'''
        quantity = 0
        if (bhb_depth[order_currency]['bids'][0]['price']/bhb_depth[payment_currency]['asks'][0]['price'])/bnb_depth['asks'][0]['price'] - 1 > -0.02:

            '''bnb eth可兑换eos数量、bhb eos数量、bnb卖一eos数量、bhb买一eos数量、bhb eth卖一可兑换eos数量，取最小值'''
            quantity = min(bnb_asset[payment_currency]['free']/bnb_depth['asks'][0]['price'],
                           bhb_asset[order_currency]['free'],
                           bnb_depth['asks'][0]['quantity'] * 0.5,
                           bhb_depth[order_currency]['bids'][0]['quantity'] * 0.5,
                           (bhb_depth[payment_currency]['asks'][0]['quantity']*bhb_depth[payment_currency]['asks'][0]['price']/(bhb_depth[order_currency]['bids'][0]['price']*0.9985))*0.5)

            '''币安最小交易量'''
            if quantity < 2:
                time.sleep(1)
                continue
            elif quantity > 30:
                quantity = 30

            #quantity = 2

            quantity = round(quantity, 4)
            binance_order = {
                "side": "BUY",
                "symbol": order_currency + payment_currency,
                "price": bnb_depth['asks'][0]['price'],
                "quantity": round(quantity,4),
                "date_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            bithumb_order_buy = {
                "side": "bid",
                "symbol": payment_currency,
                "price": bhb_depth[payment_currency]['asks'][0]['price'],
                "quantity": round(quantity*0.9985*bhb_depth[order_currency]['bids'][0]['price']/bhb_depth[payment_currency]['asks'][0]['price'],4),
                "date_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            bithumb_order_sell = {
                "side": "ask",
                "symbol": order_currency,
                "price": bhb_depth[order_currency]['bids'][0]['price'],
                "quantity": round(quantity, 4),
                "date_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }

            '''elif bnb_price['price']/bhb_price['price'] - 1 > config.profit_rate:
                logger.info("----------当前价差(bnb vs bhb): %f -------------" % (bnb_price['price']/bhb_price['price'] - 1))
                print ("----------当前价差(bhb vs bnb): %f -------------" % (bhb_price['price'] / bnb_price['price'] - 1))
            
                bnb_depth = bnb.get_current_depth()
                bhb_depth = bhb.get_current_depth()
                if (time.time()*1000 - bnb_depth['timestamp']) > 1200 or (time.time()*1000 - bhb_depth['timestamp']) > 1200:
                    logger.info("深度数据超过1.2秒")
                    print ("深度数据超过1.2秒")
                    time.sleep(0.5)
                    continue'''
            #quantity = 0
        elif bnb_depth['bids'][0]['price']/(bhb_depth[order_currency]['asks'][0]['price'] / bhb_depth[payment_currency]['bids'][0]['price']) - 1 > 0.04:

            '''bnb eos可出售数量、bhb eth可买eos数量、bnb买一eos数量、bhb卖一eos数量、bhb eth买一可兑换eos数量，取最小值'''
            quantity = min(bnb_asset[order_currency]['free'],
                          bhb_asset[payment_currency]['free']*bhb_depth[payment_currency]['bids'][0]['price']*0.9985/(bhb_depth[order_currency]['asks'][0]['price']*1.0015),
                           bnb_depth['bids'][0]['quantity'],
                           bhb_depth[order_currency]['asks'][0]['quantity'],
                           bhb_depth[payment_currency]['bids'][0]['quantity']*bhb_depth[payment_currency]['bids'][0]['price']*0.9985/bhb_depth[order_currency]['asks'][0]['price'])
            if quantity < 2:
                time.sleep(1)
                continue
            elif quantity > 30:
                quantity = 30

            #quantity = 2

            quantity = round(quantity, 2)
            binance_order = {
                "side": "SELL",
                "symbol": "EOSETH",
                "price": bnb_depth['bids'][0]['price'],
                "quantity": round(quantity,4),
                "date_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            bithumb_order_buy = {
                "side": "bid",
                "symbol": order_currency,
                "price": bhb_depth[order_currency]['asks'][0]['price'],
                "quantity": round(quantity/0.9985,4),
                "date_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            bithumb_order_sell = {
                "side": "ask",
                "symbol": payment_currency,
                "price": bhb_depth[payment_currency]['bids'][0]['price'],
                "quantity": round((bhb_depth[order_currency]['asks'][0]['price']*quantity/0.9985)/(bhb_depth[payment_currency]['bids'][0]['price']*0.9985), 4),
                "date_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }

        if binance_order is None or bithumb_order_sell is None or bithumb_order_buy is None:
            time.sleep(1)
            continue

        logger.info(binance_order)
        logger.info(bithumb_order_sell)
        logger.info(bithumb_order_buy)

        bnb_order_id, bnb_order_info = bnb.create_order(side=binance_order['side'], price=binance_order['price'],
                                                        quantity=binance_order['quantity'])
        #print bnb_order_info
        if bnb_order_id < 0:
            time.sleep(1)
            continue

        while 1:
            bhb_order_sell_id, bhb_order_sell_info = bhb.order_sell(price=bithumb_order_sell['price'],
                                                                    currency=bithumb_order_sell['symbol'],
                                                                    quantity=bithumb_order_sell['quantity'])
            #print bhb_order_sell_info
            if bhb_order_sell_id > 0:
                break
            time.sleep(0.1)
        while 1:
            bhb_order_buy_id, bhb_order_buy_info = bhb.order_buy(
                price=bithumb_order_buy['price'], currency=bithumb_order_buy['symbol'],
                quantity=bithumb_order_buy['quantity'])
            #print bhb_order_buy_info
            if bhb_order_buy_id > 0:
                break
            time.sleep(0.1)

        hedge = {
            'hedge_id': hedge_id,
            'profit': 0.0,
            'symbol': binance_order['symbol'],
            'quantity': binance_order['quantity'],
            'status': 'execute',
            'date_time': binance_order['date_time'],
            'binance_orders': {
                bnb_order_id: bnb_order_info
            },
            'bithumb_buys': {
                bhb_order_buy_id: bhb_order_buy_info
            },
            'bithumb_sells': {
                bhb_order_sell_id: bhb_order_sell_info
            }
        }

        if binance_order['side'] == 'SELL':
            hedge['bid_exchange'] = 'bithumb'
            hedge['ask_exchange'] = 'binance'
        else:
            hedge['bid_exchange'] = 'binance'
            hedge['ask_exchange'] = 'bithumb'

        hedges.append(hedge)

        time.sleep(5)


