# -*- coding:utf-8 -*-

from binance.client import Client
from binance.websockets import BinanceSocketManager
from binance.enums import *
from binance.exceptions import *
from comm.database import conn
import time
import uuid

class BinanceApi(object):

    def __init__(self, api_key, api_secret, order_currency, payment_currency):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = Client(self.api_key, self.api_secret)
        self.bsm = BinanceSocketManager(client=self.client)
        self.orders = {}
        self.asset_balance = {}
        self.order_currency = order_currency
        self.payment_currency = payment_currency
        self.symbol = self.order_currency + self.payment_currency
        self.current_price = {}
        self.current_depth = {}
        self.bsm.start_symbol_ticker_socket(self.symbol, self._process_ticker_message)
        self.bsm.start_depth_socket(self.symbol, self._process_depth_message, BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.bsm.start_user_socket(self._process_user_info_message)
        self.bsm.start()
        self._set_asset_balance()

    def _process_ticker_message(self, msg):
        self.current_price = {"price": float(msg['c']), "timestamp": time.time()*1000}
        #print self.current_price

    def _process_depth_message(self, msg):
        asks_depth = []
        bids_depth = []
        for ask in msg['asks']:
            asks_depth.append({"price": float(ask[0]), "quantity": float(ask[1])})
        for bid in msg['bids']:
            bids_depth.append({"price": float(bid[0]), "quantity": float(bid[1])})
        self.current_depth = {"asks": asks_depth, "bids": bids_depth, "timestamp": time.time()*1000}
        #print self.current_depth

    def _process_user_info_message(self, msg):
        #print msg
        if msg['e'].encode('utf-8') == "outboundAccountInfo":
            '''TODO: 用户数据入库'''
            found = 0
            for balance in msg['B']:
                if balance['a'].encode('utf-8') == self.order_currency:
                    self.asset_balance[self.order_currency] = {"free": float(balance['f']), "locked": float(balance['l'])}
                    found += 1
                elif balance['a'].encode('utf-8') == self.payment_currency:
                    self.asset_balance[self.payment_currency] = {"free": float(balance['f']), "locked": float(balance['l'])}
                    found += 1
                elif found >= 2:
                    break
                else:
                    continue
            conn.execute('INSERT INTO asset_info  VALUES (?,?,?,?,?,?)', [str(uuid.uuid1()),
                self.asset_balance[self.order_currency]['free'] + self.asset_balance[self.order_currency][
                    'locked'], 0.0,
                self.asset_balance[self.payment_currency]['free'] + self.asset_balance[self.payment_currency][
                    'locked'], 'binance', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())])
            conn.commit()
        elif msg['e'].encode('utf-8') == "executionReport":
            if not self.orders.has_key(msg['i']):
                time.sleep(1)
            self.orders[msg['i']]['status'] = msg['X'].encode('utf-8')
            if msg['X'].encode('utf-8') in ["FILLED", "PARTIALLY_FILLED"]:
                trade = {
                    'trade_id': msg['t'],
                    'commission': float(msg['n']),
                    'commissionAsset': msg['N'].encode('utf-8'),
                    'price': float(msg['L']),
                    'quantity': float(msg['l']),
                    'order_id': msg['i'],
                    'date_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg['T']/1000))
                }
                self.orders[msg['i']]['trades'][trade['trade_id']] = trade

                self.orders[msg['i']]['deal_quantity'] += trade['quantity']
                self.orders[msg['i']]['deal_amount'] += trade['quantity'] * trade['price']
                self.orders[msg['i']]['deal_fee'] += trade['commission']
                '''手续费记录方式可能会存在问题，多币种咋办'''
                if self.orders[msg['i']]['fee_asset'] != trade['commissionAsset']:
                    self.orders[msg['i']]['fee_asset'] += trade['commissionAsset'] + ' '
                self.orders[msg['i']]['deal_price'] = self.orders[msg['i']]['deal_amount']/self.orders[msg['i']]['deal_quantity']



    def _set_asset_balance(self):
        tries = 0
        '''sleep?'''
        while tries < 5:
            try:
                asset = self.client.get_asset_balance(self.payment_currency)
            except BinanceAPIException as e:
                print e.code, e.message
                tries += 1
                continue
            if asset is not None:
                self.asset_balance[asset['asset']] = {"free": float(asset['free']), 'locked': float(asset['locked'])}
                break
            time.sleep(0.5)
        if tries == 5:
            print "get asset balance failed! "
            return

        tries = 0
        while tries < 5:
            try:
                asset = self.client.get_asset_balance(self.order_currency)
            except BinanceAPIException as e:
                print e.code, e.message
                tries += 1
                continue
            if asset is not None:
                self.asset_balance[asset['asset']] = {"free": float(asset['free']), 'locked': float(asset['locked'])}
                break
            time.sleep(0.5)
        if tries == 5:
            print "get asset balance failed! "
            return

    def get_current_price(self):
        return self.current_price

    def get_current_depth(self):
        return self.current_depth

    def create_order(self, side, price, quantity ):
        try:
            order_info = self.client.create_order(symbol=self.symbol, side=side, type=ORDER_TYPE_LIMIT, price=str(price), quantity=quantity, timeInForce=TIME_IN_FORCE_GTC )
        except BinanceAPIException as e:
            return -1, {"code": e.code, "message": e.message}

        self.orders[order_info['orderId']] = {"side": side, "order_quantity": float(order_info['origQty']), "symbol": self.symbol,
                                         "order_price": float(order_info['price']), "status": order_info['status'].encode('utf-8'),
                                         'date_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(order_info['transactTime']/1000)),
                                         'deal_quantity': 0, 'order_amount': float(order_info['price'])* float(order_info['origQty']),
                                         'deal_amount': 0, 'deal_fee': 0, 'fee_asset': '', 'deal_price': 0, 'trades': {} }

        return order_info['orderId'], self.orders[order_info['orderId']]


    '''def order_buy(self, price, quantity, order_type=ORDER_TYPE_LIMIT):
        if order_type is ORDER_TYPE_LIMIT:
            try:
                order = self.client.order_limit_buy(symbol=self.symbol, quantity=quantity, price=str(price))
            except BinanceAPIException as e:
                return -1, {"code": e.code, "message": e.message}
        else:
            try:
                order = self.client.order_market_buy(symbol=self.symbol, quantity=quantity, price=str(price))
            except BinanceAPIException as e:
                return -1, {"code": e.code, "message": e.message}

        self.orders[order['orderId']] = {"side": "bid", "executedQty": [], "origQty": float(order['origQty']), "currency": self.symbol,
                                         "price": float(order['price']), "status": order['status'].encode('utf-8')}
        return order['orderId'], self.orders[order['orderId']]

    def order_sell(self, price, quantity, order_type=ORDER_TYPE_LIMIT):
        if order_type is ORDER_TYPE_LIMIT:
            try:
                order = self.client.order_limit_sell(symbol=self.symbol, quantity=quantity, price=str(price))
            except BinanceAPIException as e:
                return -1, {"code": e.code, "message": e.message}
        else:
            try:
                order = self.client.order_market_sell(symbol=self.symbol, quantity=quantity, price=str(price))
            except BinanceAPIException as e:
                return -1, {"code": e.code, "message": e.message}

        #self.orders[order_id] = "NEW"
        self.orders[order['orderId']] = {"side": "ask", "executedQty": [], "origQty": float(order['origQty']),"symbol": self.symbol,
                                         "price": float(order['price']), "status": order['status'].encode('utf-8')}
        return order['orderId'], self.orders[order['orderId']]'''

    def order_cancel(self, order_id):
        try:
            result = self.client.cancel_order(symbol=self.symbol, orderId=order_id)
        except BinanceAPIException as e:
            return -1, {"code": e.code, "message": e.message}
        #self.orders[result['orderId']]["status"] = "CANCELED"
        return result['orderId'], self.orders[result['orderId']]

    def get_order_info(self, order_id):
        if self.orders.has_key(order_id):
            if abs(self.orders[order_id]['order_quantity'] - self.orders[order_id]['deal_quantity']) < 0.0001:
                self.orders[order_id]['status'] = 'FILLED'
            elif self.orders[order_id]['deal_quantity'] == 0:
                self.orders[order_id]['status'] = 'PARTIALLY_FILLED'
            return self.orders[order_id]
        return None

    def check_balance(self, currency):
        return self.asset_balance[currency]

    def get_asset_balance(self):
        return self.asset_balance

    def get_all_trade(self, order_id):
        return self.client.get_my_trades(symbol=self.symbol)

    def get_symbol_info(self, symbol):
        return self.client.get_symbol_info(symbol=symbol)
