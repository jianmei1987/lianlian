# -*- coding:utf-8 -*-

from api.xcoin_api_client import *
import threading
import requests, json
from comm.logger import *
from comm.database import conn
import time
import uuid

class BithumbApi(object):

    def __init__(self, api_key, api_secret, order_currency, payment_currency):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = XCoinAPI(api_key, api_secret);
        self.orders = {}
        self.asset_balance = {}
        self.order_currency = order_currency
        self.payment_currency = payment_currency
        self.symbol = self.order_currency + self.payment_currency
        self.current_price = {}
        self.current_depth = {}
        self.threads = {}
        self._start_check()


    def _get_price(self):
        rgParams = {"order_currency" : self.order_currency, "payment_currency" : self.payment_currency}
        while 1:
            try:
                result = requests.post("https://api.bithumb.com/public/ticker/all", rgParams)
                try:
                    prices = json.loads(result.text)
                except:
                    logger.error("_get_price: %s", result)
            except requests.exceptions.RequestException as e:
                logger.info("_get_price: %s , %s ", prices, e)
                continue
            if int(prices['status']) == 0:
                self.current_price = {"price": float(prices['data'][self.order_currency]['closing_price'])/float(prices['data'][self.payment_currency]['closing_price']),
                                      self.order_currency:  float(prices['data'][self.order_currency]['closing_price']),
                                      self.payment_currency: float(prices['data'][self.payment_currency]['closing_price']),
                                      "timestamp": time.time()*1000}
            else:
                logger.error("_get_price error: %s ", prices['status'])
            #print self.current_price['price'], time.time()*1000
            time.sleep(0.5)

    def _get_depth(self):
        rgParams = {"order_currency": self.order_currency, "payment_currency": self.payment_currency}
        while 1:
            try:
                result = requests.post("https://api.bithumb.com/public/orderbook/all", rgParams)
                #logger.info("_get_depth: %s", result.text)
                try:
                    orderbook = json.loads(result.text)
                except:
                    logger.error("_get_depth: %s",result)
            except requests.exceptions.RequestException as e:
                logger.error("_get_depth: %s",e)
                continue
            if int(orderbook['status']) == 0:
                asks_depth = []
                bids_depth = []
                for ask in orderbook['data'][self.order_currency]['asks']:
                    asks_depth.append({"price": float(ask['price']), "quantity": float(ask['quantity'])})

                for bid in orderbook['data'][self.order_currency]['bids']:
                    bids_depth.append({"price": float(bid['price']), "quantity": float(bid['quantity'])})
                self.current_depth[self.order_currency] = {"asks": asks_depth, "bids": bids_depth}

                asks_depth = []
                bids_depth = []
                for ask in orderbook['data'][self.payment_currency]['asks']:
                    asks_depth.append({"price": float(ask['price']), "quantity": float(ask['quantity'])})

                for bid in orderbook['data'][self.payment_currency]['bids']:
                    bids_depth.append({"price": float(bid['price']), "quantity": float(bid['quantity'])})
                self.current_depth[self.payment_currency] = {"asks": asks_depth, "bids": bids_depth}

                self.current_depth['timestamp'] = time.time()*1000
            else:
                logger.error("_get_depth error: %s ", orderbook['status'])
            time.sleep(0.5)
            #print self.current_depth

    def _get_order_currency_balance(self):
        rgParams = {"currency": self.order_currency}
        while 1:
            if self.asset_balance.has_key(self.order_currency) and self.asset_balance.has_key(self.payment_currency):
                conn.execute('INSERT INTO asset_info  VALUES (?,?,?,?,?,?)', [str(uuid.uuid1()), self.asset_balance[self.order_currency]['free'] + self.asset_balance[self.order_currency]['locked'], self.asset_balance['KRW']['free'] + self.asset_balance['KRW']['locked'], self.asset_balance[self.payment_currency]['free'] + self.asset_balance[self.payment_currency]['locked'], 'bithumb', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())])
                conn.commit()
            try:
                result = self.client.xcoinApiCall("/info/balance/", rgParams)
            except BaseException, e:
                logger.error("_get_order_currency_balance: %s",e)
                time.sleep(1)
                continue
            if int(result['status']) == 0:
                logger.info( "_get_order_currency_balance: %s ", result)
                try:
                    self.asset_balance[self.order_currency] = {
                        "free": float(result['data']['available_' + self.order_currency.lower()]),
                        "locked":float(result['data']['in_use_' + self.order_currency.lower()])}
                except:
                    continue
                self.asset_balance['KRW'] = {"free": float(result['data']['available_krw']), "locked": float(result['data']['in_use_krw'])}
            time.sleep(10)

    def _get_payment_currency_balance(self):
        rgParams = {"currency": self.payment_currency}
        while 1:
            try:
                result = self.client.xcoinApiCall("/info/balance/", rgParams)
            except BaseException, e:
                logger.error("_get_payment_currency_balance: %s",e)
                time.sleep(1)
                continue
            if int(result['status']) == 0:
                logger.info( "_get_payment_currency_balance: %s", result)
                try:
                    self.asset_balance[self.payment_currency] = {
                        "free": float(result['data']['available_' + self.payment_currency.lower()]),
                        "locked":float(result['data']['in_use_' + self.payment_currency.lower()])}
                except:
                    time.sleep(5)
                    continue
            time.sleep(10)


    def _start_check(self):
        #price_thread = threading.Thread(target=self._get_price)
        #price_thread.setDaemon(True)
        #price_thread.start()
        #self.threads["price"] = price_thread
        depth_thread = threading.Thread(target=self._get_depth)
        depth_thread.setDaemon(True)
        depth_thread.start()
        self.threads["depth"] = depth_thread
        order_currency_balance_thread = threading.Thread(target=self._get_order_currency_balance)
        order_currency_balance_thread.setDaemon(True)
        order_currency_balance_thread.start()
        self.threads["order_currency_balance"] = order_currency_balance_thread
        payment_currency_balance_thread = threading.Thread(target=self._get_payment_currency_balance)
        payment_currency_balance_thread.setDaemon(True)
        payment_currency_balance_thread.start()
        self.threads["payment_currency_balance"] = payment_currency_balance_thread

    def get_current_price(self):
        return self.current_price

    def get_current_depth(self):
        return self.current_depth

    def order_buy(self, price, quantity, currency, type='limit'):
        rgParams = {"price": int(price), "units": round(quantity, 4), "order_currency": currency, "type": "bid", "Payment_currency": "KRW" }
        try:
            result = self.client.xcoinApiCall("/trade/place", rgParams)
        except:
            return -2, {}
        try:
            if int(result['status']) == 0:
                order_id = int(result['order_id'])
                self.orders[int(result['order_id'])] = {"side": 'bid', "order_quantity": quantity, "symbol": currency, "order_price": price, "status": "NEW",
                                                       'date_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), 'deal_quantity': 0,
                                                      'order_amount': price * quantity,  'deal_amount': 0, 'deal_fee': 0, 'fee_asset': '', 'deal_price': 0,
                                                      'trades': {}}

                if len(result['data']):
                    for trade_info in result['data']:
                        if trade_info.has_key('contNo') and (
                                not self.orders[order_id]['trades'].has_key(int(trade_info['contNo']))):
                            trade = {
                                'trade_id': int(trade_info['contNo']),
                                'commission': float(trade_info['fee']),
                                'commissionAsset': self.orders[order_id]['symbol'],
                                'price': int(trade_info['price']),
                                'quantity': float(trade_info['units']),
                                'order_id': order_id,
                                'date_time': self.orders[order_id]['date_time']
                            }

                            self.orders[order_id]['trades'][trade['trade_id']] = trade

                            self.orders[order_id]['deal_quantity'] += trade['quantity']
                            self.orders[order_id]['deal_amount'] += trade['quantity'] * trade['price']
                            self.orders[order_id]['deal_fee'] += trade['commission']
                            '''手续费记录方式可能会存在问题，多币种咋办'''
                            if self.orders[order_id]['fee_asset'] != trade['commissionAsset']:
                                self.orders[order_id]['fee_asset'] += trade['commissionAsset'] + ' '
                            self.orders[order_id]['deal_price'] = self.orders[order_id]['deal_amount'] / \
                                                                  self.orders[order_id][
                                                                      'deal_quantity']

                        if abs(self.orders[order_id]['order_quantity'] - self.orders[order_id]['deal_quantity']) < 0.0001:
                            self.orders[order_id]['status'] = 'FILLED'
                        elif self.orders[order_id]['deal_quantity'] == 0:
                            self.orders[order_id]['status'] = 'PARTIALLY_FILLED'
                return int(result['order_id']), self.orders[int(result['order_id'])]
            else:
                logger.error("order_buy: %s, %s", result['status'], result['message'].encode('Euc-kr'))
                return -1, {"code": int(result['status']), "message": result['message']}
        except BaseException,e:
            logger.exception("order_buy: %s",e)
            return -1,None

    def order_sell(self, price, quantity, currency, type='limit'):
        rgParams = {"price": int(price), "units": round(quantity, 4), "order_currency": currency, "type": "ask", "Payment_currency": "KRW" }
        result = None
        try:
            result = self.client.xcoinApiCall("/trade/place", rgParams)
        except BaseException, e:
            logger.exception("order_sell: %s", e)
            return -1, {"code": int(result['status']), "message": result['message']}
        try:
            if int(result['status']) == 0:
                order_id = int(result['order_id'])
                self.orders[order_id] = {"side": 'ask', "order_quantity": quantity, "symbol": currency,
                                         "order_price": price, "status": "NEW",
                                         'date_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                                         'deal_quantity': 0,
                                         'order_amount': price * quantity, 'deal_amount': 0, 'deal_fee': 0,
                                         'fee_asset': '', 'deal_price': 0,
                                         'trades': {}}

                if len(result['data']):
                    for trade_info in result['data']:
                        if trade_info.has_key('cont_id') and (
                        not self.orders[order_id]['trades'].has_key(int(trade_info['cont_id']))):
                            trade = {
                                'trade_id': int(trade_info['cont_id']),
                                'commission': float(trade_info['fee']),
                                'commissionAsset': 'KRW',
                                'price': int(trade_info['price']),
                                'quantity': float(trade_info['units']),
                                'order_id': order_id,
                                'date_time': self.orders[order_id]['date_time']
                            }

                            self.orders[order_id]['trades'][trade['trade_id']] = trade

                            self.orders[order_id]['deal_quantity'] += trade['quantity']
                            self.orders[order_id]['deal_amount'] += trade['quantity'] * trade['price']
                            self.orders[order_id]['deal_fee'] += trade['commission']
                            '''手续费记录方式可能会存在问题，多币种咋办'''
                            if self.orders[order_id]['fee_asset'] != trade['commissionAsset']:
                                self.orders[order_id]['fee_asset'] += trade['commissionAsset'] + ' '
                            self.orders[order_id]['deal_price'] = self.orders[order_id]['deal_amount'] / \
                                                                  self.orders[order_id][
                                                                      'deal_quantity']

                        if abs(self.orders[order_id]['order_quantity'] - self.orders[order_id][
                                'deal_quantity']) < 0.0001:
                            self.orders[order_id]['status'] = 'FILLED'
                        elif self.orders[order_id]['deal_quantity'] == 0:
                            self.orders[order_id]['status'] = 'PARTIALLY_FILLED'

                return order_id, self.orders[int(result['order_id'])]
            else:
                logger.error("order_sell: %s, %s", result['status'], result['message'].encode('Euc-kr'))
                return -1, {"code": int(result['status']), "message": result['message']}
        except BaseException,e:
            logger.exception("order_sell: %s", e)
            return -1, None

    def order_cancel(self, order_id):
        rgParams = {"order_id": str(order_id), "type": self.orders[order_id]["side"],
                    "currency": self.orders[order_id]["currency"]}
        try:
            result = self.client.xcoinApiCall("/trade/cancel", rgParams)
        except:
            return -2
        if int(result.json()['status']) == 0:
            self.orders[order_id]['status'] = "CANCELED"
            return 0, self.orders[order_id]
        return -1, {"code": float(result['status']), "message": result['message']}

    def get_order_status(self, order_id):
        rgParams = {"order_id": str(order_id), "type": self.orders[order_id]["side"], "currency": self.orders[order_id]["symbol"]}
        try:
            result = self.client.xcoinApiCall("/info/order_detail", rgParams)
        except BaseException, e:
            logger.exception('get_order_status return None: %s', e)
            return None
        logger.info("get_order_status: %s", result)
        if int(result['status']) == 0 and len(result['data']):
            try:
                for trade_info in result['data']:
                    if trade_info.has_key('cont_no') and (not self.orders[order_id]['trades'].has_key(int(trade_info['cont_no']))):
                        trade = {
                            'trade_id': int(trade_info['cont_no']),
                            'commission': float(trade_info['fee']),
                            'commissionAsset': trade_info['order_currency'].encode('utf-8'),
                            'price': int(trade_info['price']),
                            'quantity': float(trade_info['units_traded']),
                            'order_id': order_id,
                            'date_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(trade_info['transaction_date'])/1000000))
                        }

                        self.orders[order_id]['trades'][trade['trade_id']] = trade

                        self.orders[order_id]['deal_quantity'] += trade['quantity']
                        self.orders[order_id]['deal_amount'] += trade['quantity'] * trade['price']
                        self.orders[order_id]['deal_fee'] += trade['commission']
                        '''手续费记录方式可能会存在问题，多币种咋办'''
                        if self.orders[order_id]['fee_asset'] != trade['commissionAsset']:
                            self.orders[order_id]['fee_asset'] += trade['commissionAsset'] + ' '
                        self.orders[order_id]['deal_price'] = self.orders[order_id]['deal_amount'] / self.orders[order_id][
                            'deal_quantity']

                    if abs(self.orders[order_id]['order_quantity'] - self.orders[order_id][
                            'deal_quantity']) < 0.0001:
                        self.orders[order_id]['status'] = 'FILLED'
                    elif self.orders[order_id]['deal_quantity'] == 0:
                        self.orders[order_id]['status'] = 'PARTIALLY_FILLED'
            except BaseException, e:
                logger.exception("get_order_status: %s", e)
                return None
        else:
            logger.error("get_order_status: %s, %s", result['status'], result['message'])

        return self.orders[order_id]

    def get_asset_balance(self):
        return self.asset_balance


