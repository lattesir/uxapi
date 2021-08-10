import hashlib
from decimal import Decimal

from ccxt.base.exchange import Exchange
from ccxt.base.errors import (
    ExchangeError,
    AuthenticationError,
    ArgumentsRequired,
    OrderNotFound,
    DDoSProtection,
    BadRequest,
    CancelPending,
    InsufficientFunds,
    PermissionDenied
)


class huobidm(Exchange):
    def describe(self):
        return self.deep_extend(super(huobidm, self).describe(), {
            'id': 'huobidm',
            'name': 'Huobi DM',
            'countries': ['CN'],
            'rateLimit': 50,
            'userAgent': self.userAgents['chrome39'],
            'version': 'v1',
            'hostname': 'api.hbdm.com',
            'has': {
                'cancelAllOrders': True,
                'createMarketOrder': 'emulated',
                'fetchMyTrades': True,
                'fetchOHLCV': True,
                'fetchOrder': True,
                'fetchOrders': True,
                'fetchOpenOrders': True,
                'fetchStatus': True,
            },
            'timeframes': {
                '1m': '1min',
                '5m': '5min',
                '15m': '15min',
                '30m': '30min',
                '1h': '60min',
                '4h': '4hour',
                '1d': '1day',
                '1w': '1week',
                '1M': '1mon',
            },
            'expirations': {
                'this_week': 'CW',
                'next_week': 'NW',
                'quarter': 'CQ',
                'next_quarter': 'NQ',
            },
            'urls': {
                'logo': 'https://user-images.githubusercontent.com/4180036/64329037-1d804100-d001-11e9-987f-43c114fab34e.png',
                'api': 'https://api.hbdm.com',
                'heartbeat': 'https://www.hbdm.com',
                'www': 'https://www.hbdm.com',
                'doc': 'https://huobiapi.github.io/docs/dm/v1/cn/',
                'fees': 'https://support.huobi.so/hc/en-us/articles/360000113122',
            },

            'api': {
                'futures': {
                    'get': [
                        'contract_info',                # 获取合约信息
                        'index',                        # 获取合约指数信息
                        'price_limit',                  # 获取合约最高限价和最低限价
                        'open_interest',                # 获取当前可用合约总持仓量
                        'delivery_price',               # 获取预估交割价
                        'api_state',                    # 查询系统状态
                        'market/depth',                 # 获取行情深度数据
                        'market/history/kline',         # 获取K线数据
                        'market/detail/merged',         # 获取聚合行情
                        'market/detail/batch_merged',   # 批量获取聚合行情
                        'market/trade',                 # 获取市场最近成交记录
                        'market/history/trade',         # 批量获取最近的交易记录
                        'risk_info',                    # 查询合约风险准备金余额和预估分摊比例
                        'insurance_fund',               # 查询合约风险准备金余额历史数据
                        'adjustfactor',                 # 查询平台阶梯调整系数
                        'his_open_interest',            # 平台持仓量的查询
                        'elite_account_ratio',          # 精英账户多空持仓对比-账户数
                        'elite_position_ratio',         # 精英账户多空持仓对比-持仓量
                        'liquidation_orders',           # 获取强平单
                        'index/market/history/index',   # 获取指数K线数据
                        'index/market/history/basis',   # 获取基差数据
                        'api_trading_status',           # 获取用户的API指标禁用信息(private)
                    ],

                    'post': [
                        'account_info',                 # 获取用户账户信息
                        'position_info',                # 获取用户持仓信息
                        'sub_account_list',             # 查询母账户下所有子账户资产信息
                        'sub_account_info',             # 查询单个子账户资产信息
                        'sub_position_info',            # 查询单个子账户持仓信息
                        'financial_record',             # 查询用户财务记录
                        'financial_record_exact',       # 组合查询用户财务记录
                        'user_settlement_records',      # 查询用户结算记录
                        'order_limit',                  # 查询用户当前的下单量限制
                        'fee',                          # 查询用户当前的手续费费率
                        'transfer_limit',               # 查询用户当前的划转限制
                        'position_limit',               # 用户持仓量限制的查询
                        'account_position_info',        # 查询用户账户和持仓信息
                        'master_sub_transfer',          # 母子账户划转
                        'master_sub_transfer_record',   # 获取母账户下的所有母子账户划转记录
                        'available_level_rate',         # 查询用户可用杠杆倍数

                        'order',                        # 合约下单
                        'batchorder',                   # 合约批量下单
                        'cancel',                       # 撤销订单
                        'cancelall',                    # 全部撤单
                        'switch_lever_rate',            # 切换杠杆
                        'order_info',                   # 获取合约订单信息
                        'order_detail',                 # 获取订单明细信息
                        'openorders',                   # 获取合约当前未成交委托
                        'hisorders',                    # 获取合约历史委托
                        'hisorders_exact',              # 组合查询合约历史委托
                        'matchresults',                 # 获取历史成交记录
                        'matchresults_exact',           # 组合查询历史成交记录接口
                        'lightning_close_position',     # 闪电平仓下单
                        'trigger_order',                # 合约计划委托下单
                        'trigger_cancel',               # 合约计划委托撤单
                        'trigger_cancelall',            # 合约计划委托全部撤单
                        'trigger_openorders',           # 获取计划委托当前委托
                        'trigger_hisorders',            # 获取计划委托历史委托
                    ]
                },

                'swap': {
                    'get': [
                        'contract_info',                # 获取合约信息
                        'index',                        # 获取合约指数信息
                        'price_limit',                  # 获取合约最高限价和最低限价
                        'open_interest',                # 获取当前可用合约总持仓量
                        'market/depth',                 # 获取行情深度数据
                        'market/history/kline',         # 获取K线数据
                        'market/detail/merged',         # 获取聚合行情
                        'market/detail/batch_merged',   # 批量获取聚合行情
                        'market/trade',                 # 获取市场最近成交记录
                        'market/history/trade',         # 批量获取最近的交易记录
                        'risk_info',                    # 查询合约风险准备金余额和预估分摊比例
                        'insurance_fund',               # 查询合约风险准备金余额历史数据
                        'adjustfactor',                 # 查询平台阶梯调整系数
                        'his_open_interest',            # 平台持仓量的查询
                        'elite_account_ratio',          # 精英账户多空持仓对比-账户数
                        'elite_position_ratio',         # 精英账户多空持仓对比-持仓量
                        'api_state',                    # 查询系统状态
                        'funding_rate',                 # 获取合约的资金费率
                        'historical_funding_rate',      # 获取合约的历史资金费率
                        'liquidation_orders',           # 获取强平订单
                        'index/market/history/swap_premium_index_kline',    # 获取合约的溢价指数K线
                        'index/market/history/swap_estimated_rate_kline',   # 获取实时预测资金费率的K线数据
                        'index/market/history/swap_basis',                  # 获取基差数据
                        'api_trading_status',           # 获取用户的API指标禁用信息(private)
                    ],

                    'post': [
                        'account_info',                 # 获取用户账户信息
                        'position_info',                # 获取用户持仓信息
                        'account_position_info',        # 查询用户账户和持仓信息
                        'sub_account_list',             # 查询母账户下所有子账户资产信息
                        'sub_account_info',             # 查询单个子账户资产信息
                        'sub_position_info',            # 查询单个子账户持仓信息
                        'financial_record',             # 查询用户财务记录
                        'financial_record_exact',       # 组合查询用户财务记录
                        'user_settlement_records',      # 查询用户结算记录
                        'available_level_rate',         # 查询用户可用杠杆倍数
                        'order_limit',                  # 查询用户当前的下单量限制
                        'fee',                          # 查询用户当前的手续费费率
                        'transfer_limit',               # 查询用户当前的划转限制
                        'position_limit',               # 用户持仓量限制的查询
                        'master_sub_transfer',          # 母子账户划转
                        'master_sub_transfer_record',   # 获取母账户下的所有母子账户划转记录

                        'order',                        # 合约下单
                        'batchorder',                   # 合约批量下单
                        'cancel',                       # 撤销订单
                        'cancelall',                    # 全部撤单
                        'switch_lever_rate',            # 切换杠杆
                        'order_info',                   # 获取合约订单信息
                        'order_detail',                 # 获取订单明细信息
                        'openorders',                   # 获取合约当前未成交委托
                        'hisorders',                    # 获取合约历史委托
                        'hisorders_exact',              # 组合查询合约历史委托
                        'matchresults',                 # 获取历史成交记录
                        'matchresults_exact',           # 组合查询用户历史成交记录
                        'lightning_close_position',     # 闪电平仓下单
                        'trigger_order',                # 合约计划委托下单
                        'trigger_cancel',               # 合约计划委托撤单
                        'trigger_cancelall',            # 合约计划委托全部撤单
                        'trigger_openorders',           # 获取计划委托当前委托
                        'trigger_hisorders',            # 获取计划委托历史委托
                    ],
                },

                'swapusdt': {
                    'get': [
                        'contract_info',                # 获取合约信息
                        'index',                        # 获取合约指数信息
                        'price_limit',                  # 获取合约最高限价和最低限价
                        'open_interest',                # 获取当前可用合约总持仓量
                        'market/depth',                 # 获取行情深度数据
                        'market/history/kline',         # 获取K线数据
                        'market/detail/merged',         # 获取聚合行情
                        'market/detail/batch_merged',   # 批量获取聚合行情
                        'market/trade',                 # 获取市场最近成交记录
                        'market/history/trade',         # 批量获取最近的交易记录
                        'risk_info',                    # 查询合约风险准备金余额和预估分摊比例
                        'insurance_fund',               # 查询合约风险准备金余额历史数据
                        'adjustfactor',                 # 查询平台阶梯调整系数
                        'his_open_interest',            # 平台持仓量的查询
                        'elite_account_ratio',          # 精英账户多空持仓对比-账户数
                        'elite_position_ratio',         # 精英账户多空持仓对比-持仓量
                        'api_state',                    # 查询系统状态
                        'funding_rate',                 # 获取合约的资金费率
                        'batch_funding_rate',           # 批量获取合约的资金费率
                        'historical_funding_rate',      # 获取合约的历史资金费率
                        'liquidation_orders',           # 获取强平订单
                        'index/market/history/linear_swap_premium_index_kline',     # 获取合约的溢价指数K线
                        'index/market/history/linear_swap_estimated_rate_kline',    # 获取实时预测资金费率的K线数据
                        'index/market/history/linear_swap_basis',                   # 获取基差数据
                        'api_trading_status',           # 获取用户的API指标禁用信息(private)
                    ],

                    'post': [
                        'account_info',                 # 获取用户账户信息(逐仓)
                        'cross_account_info',           # 获取用户账户信息(全仓)
                        'position_info',                # 获取用户持仓信息(逐仓)
                        'cross_position_info',          # 获取用户持仓信息(全仓)
                        'account_position_info',        # 查询用户账户和持仓信息(逐仓)
                        'cross_account_position_info',  # 查询用户账户和持仓信息(全仓)
                        'swap_sub_auth',                # 批量设置子账户交易权限
                        'sub_account_list',             # 查询母账户下所有子账户资产信息(逐仓)
                        'cross_sub_account_list',       # 查询母账户下所有子账户资产信息(全仓)
                        'sub_account_info_list',        # 批量获取子账户资产信息(逐仓)
                        'cross_sub_account_info_list',  # 批量获取子账户资产信息(全仓)
                        'sub_account_info',             # 查询单个子账户资产信息(逐仓)
                        'cross_sub_account_info',       # 查询单个子账户资产信息(全仓)
                        'sub_position_info',            # 查询单个子账户持仓信息(逐仓)
                        'cross_sub_position_info',      # 查询单个子账户持仓信息(全仓)
                        'financial_record',             # 查询用户财务记录
                        'financial_record_exact',       # 组合查询用户财务记录
                        'user_settlement_records',        # 查询用户结算记录(逐仓)
                        'cross_user_settlement_records',  # 查询用户结算记录(全仓)
                        'available_level_rate',         # 查询用户可用杠杆倍数(逐仓)
                        'cross_available_level_rate',   # 查询用户可用杠杆倍数(全仓)
                        'order_limit',                  # 查询用户当前的下单量限制
                        'fee',                          # 查询用户当前的手续费费率
                        'transfer_limit',               # 查询用户当前的划转限制(逐仓)
                        'cross_transfer_limit',         # 查询用户当前的划转限制(全仓)
                        'position_limit',               # 用户持仓量限制的查询(逐仓)
                        'cross_position_limit',         # 用户持仓量限制的查询(全仓)
                        'master_sub_transfer',          # 母子账户划转
                        'master_sub_transfer_record',   # 获取母账户下的所有母子账户划转记录
                        'transfer_inner',               # 同账号不同保证金账户的划转

                        'order',                        # 合约下单(逐仓)
                        'cross_order',                  # 合约下单(全仓)
                        'batchorder',                   # 合约批量下单(逐仓)
                        'cross_batchorder',             # 合约批量下单(全仓)
                        'cancel',                       # 撤销订单(逐仓)
                        'cross_cancel',                 # 撤销订单(全仓)
                        'cancelall',                    # 全部撤单(逐仓)
                        'cross_cancelall',              # 全部撤单(全仓)
                        'switch_lever_rate',            # 切换杠杆(逐仓)
                        'cross_switch_lever_rate',      # 切换杠杆(全仓)
                        'order_info',                   # 获取合约订单信息(逐仓)
                        'cross_order_info',             # 获取合约订单信息(全仓)
                        'order_detail',                 # 获取订单明细信息(逐仓)
                        'cross_order_detail',           # 获取订单明细信息(全仓)
                        'openorders',                   # 获取合约当前未成交委托(逐仓)
                        'cross_openorders',             # 获取合约当前未成交委托(全仓)
                        'hisorders',                    # 获取合约历史委托(逐仓)
                        'cross_hisorders',              # 获取合约历史委托(全仓)
                        'hisorders_exact',              # 组合查询合约历史委托(逐仓)
                        'cross_hisorders_exact',        # 组合查询合约历史委托(全仓)
                        'matchresults',                 # 获取历史成交记录(逐仓)
                        'cross_matchresults',           # 获取历史成交记录(全仓)
                        'matchresults_exact',           # 组合查询用户历史成交记录(逐仓)
                        'cross_matchresults_exact',     # 组合查询用户历史成交记录(全仓)
                        'lightning_close_position',         # 闪电平仓下单(逐仓)
                        'cross_lightning_close_position',   # 闪电平仓下单(全仓)

                        'trigger_order',                # 合约计划委托下单(逐仓)
                        'cross_trigger_order',          # 合约计划委托下单(全仓)
                        'trigger_cancel',               # 合约计划委托撤单(逐仓)
                        'cross_trigger_cancel',         # 合约计划委托撤单(全仓)
                        'trigger_cancelall',            # 合约计划委托全部撤单(逐仓)
                        'cross_trigger_cancelall',      # 合约计划委托全部撤单(全仓)
                        'trigger_openorders',           # 获取计划委托当前委托(逐仓)
                        'cross_trigger_openorders',     # 获取计划委托当前委托(全仓)
                        'trigger_hisorders',            # 获取计划委托历史委托(逐仓)
                        'cross_trigger_hisorders',      # 获取计划委托历史委托(全仓)

                        'tpsl_order',                   # 对仓位设置止盈止损订单(逐仓)
                        'cross_tpsl_order',             # 对仓位设置止盈止损订单(全仓)
                        'tpsl_cancel',                  # 止盈止损订单撤单(逐仓)
                        'cross_tpsl_cancel',            # 止盈止损订单撤单(全仓)
                        'tpsl_cancelall',               # 止盈止损订单全部撤单(逐仓)
                        'cross_tpsl_cancelall',         # 止盈止损订单全部撤单(全仓)
                        'tpsl_openorders',              # 查询止盈止损订单当前委托(逐仓)
                        'cross_tpsl_openorders',        # 查询止盈止损订单当前委托(全仓)
                        'tpsl_hisorders',               # 查询止盈止损订单历史委托(逐仓)
                        'cross_tpsl_hisorders',         # 查询止盈止损订单历史委托(全仓)
                        'relation_tpsl_order',          # 查询开仓单关联的止盈止损订单详情(逐仓)
                        'cross_relation_tpsl_order',    # 查询开仓单关联的止盈止损订单详情(全仓)
                    ],
                },

                'general': {
                    'get': [
                        'heartbeat',        # 查询系统是否可用
                    ]
                },
            },

            'fees': {
                'futures': {
                    'taker': 0.0003,
                    'maker': 0.0002,
                },
                'swap': {
                    'taker': 0.0002,
                    'maker': 0.0004,
                },
            },

            'exceptions': {
                'exact': {
                    '403': AuthenticationError,   # 无效身份
                    '1017': OrderNotFound,        # 查询订单不存在
                    '1030': BadRequest,           # 输入错误
                    '1031': BadRequest,           # 非法的报单来源
                    '1032': DDoSProtection,       # 访问次数超出限制
                    '1033': BadRequest,           # 合约周期字段值错误
                    '1034': BadRequest,           # 报单价格类型字段值错误
                    '1035': BadRequest,           # 报单方向字段值错误
                    '1036': BadRequest,           # 报单开平字段值错误
                    '1037': BadRequest,           # 杠杆倍数不符合要求
                    '1038': BadRequest,           # 报单价格不符合最小变动价
                    '1039': BadRequest,           # 报单价格超出限制
                    '1040': BadRequest,           # 报单数量不合法
                    '1041': BadRequest,           # 报单数量超出限制
                    '1042': BadRequest,           # 超出多头持仓限制
                    '1043': BadRequest,           # 超出多头持仓限制
                    '1044': BadRequest,           # 超出平台持仓限制
                    '1045': BadRequest,           # 杠杆倍数与所持有仓位的杠杆不符合
                    '1047': InsufficientFunds,    # 可用保证金不足
                    '1048': BadRequest,           # 持仓量不足
                    '1050': BadRequest,           # 客户报单号重复
                    '1051': OrderNotFound,        # 没有可撤订单
                    '1052': BadRequest,           # 超出批量数目限制
                    '1061': OrderNotFound,        # 订单不存在，无法撤单
                    '1062': CancelPending,        # 撤单中，无法重复撤单
                    '1065': BadRequest,           # 客户报单号不是整数
                    '1066': BadRequest,           # 字段不能为空
                    '1067': BadRequest,           # 字段不合法
                    '1069': BadRequest,           # 报单价格不合法
                    '1071': BadRequest,           # 订单已撤，无法撤单
                    '1100': PermissionDenied,     # 用户没有开仓权限
                    '1101': PermissionDenied,     # 用户没有平仓权限
                    '1102': PermissionDenied,     # 用户没有入金权限
                    '1103': PermissionDenied,     # 用户没有出金权限
                    '1200': AuthenticationError,  # 登录错误
                },
            },
            'options': {
                'leverage': 10,
                'defaultType': 'futures',
                'marginMode': 'cross', # 'cross' or 'isolated'
            }
        })

    def find_method(self, market_type, method_name):
        api = market_type.replace('.', '')
        return getattr(self, f'{api}{method_name}')

    def fetch_markets(self, params={}):
        defaultType = self.safe_string_2(self.options, 'fetchMarkets', 'defaultType', 'futures')
        type = self.safe_string(params, 'type', defaultType)
        method = self.find_method(type, 'GetContractInfo')
        response = method(params)
        markets = self.safe_value(response, 'data')
        if not markets:
            raise ExchangeError(self.id + f' {method} returned empty response')
        return [self.parse_market(market) for market in markets]

    def parse_market(self, market):
        # {
        #     symbol: "BTC",
        #     contract_code: "BTC190906",
        #     contract_type: "this_week",
        #     contract_size: 100,
        #     price_tick: 0.01,
        #     delivery_date: "20190906",
        #     create_date: "20190823",
        #     contract_status: 1
        # }
        if 'delivery_date' in market:
            market_type = 'futures'
            base = baseId = market['symbol']
            quote = quoteId = 'USD'
            expiration = self.expirations[market['contract_type']]
            symbol = f'{base}_{expiration}'
        else:
            symbol = market['contract_code']
            base, quote = symbol.split('-')
            if quote == 'USDT':
                market_type = 'swap.usdt'
                base, quote = quote, base
            else:
                market_type = 'swap'
            baseId = base
            quoteId = quote

        active = (self.safe_integer(market, 'contract_status') == 1)
        price_tick = Decimal(market['price_tick'])
        exp = price_tick.adjusted()
        mantissa = price_tick.scaleb(-exp)
        if round(mantissa) == 10:
            price_precision = -exp - 1
        else:
            price_precision = -exp
        return {
            'id': symbol,
            'symbol': symbol,
            'base': base,
            'quote': quote,
            'baseId': baseId,
            'quoteId': quoteId,
            'type': market_type,
            'active': active,
            'precision': {
                'amount': 0,
                'price': price_precision,
            },
            'limits': {
                'amount': {
                    'min': 1,
                    'max': None,
                },
                'price': {
                    'min': None,
                    'max': None
                },
                'cost': {
                    'min': None,
                    'max': None
                }
            },
            'info': market,
        }

    def fetch_ticker(self, symbol, params=None):
        self.load_markets()
        market = self.market(symbol)
        method = self.find_method(market['type'], 'GetMarketDetailMerged')
        if market['type'] == 'futures':
            request = {'symbol': market['id']}
        else:
            request = {'contract_code': market['id']}
        response = method(self.extend(request, params or {}))
        return self.parse_ticker(response['tick'], market)

    def fetch_tickers(self, symbols=None, params=None):
        self.load_markets()
        type = self.safe_string_2(self.options, 'fetchTickers', 'defaultType', 'futures')
        method = self.find_method(type, 'GetMarketDetailBatchMerged')
        response = method(params or {})
        return self.parse_tickers(response['ticks'], symbols)
        
    def parse_tickers(self, raw_tickers, symbols=None):
        tickers = [self.parse_ticker(ticker) for ticker in raw_tickers]
        return self.filter_by_array(tickers, 'symbol', symbols)

    def parse_ticker(self, ticker, market=None):
        # {
        #   "amount": "260479.503",
        #   "ask": [10253.4, 829],
        #   "bid": [10253.39, 885],
        #   "close": "10253.4",
        #   "count": 602655,
        #   "high": "10564.07",
        #   "id": 1568019891,
        #   "low": "10154.79",
        #   "open": "10522.66",
        #   "ts": 1568019891626,
        #   "vol": "27176782"
        # }
        timestamp = self.safe_integer(ticker, 'ts')
        bid = None
        ask = None
        bidVolume = None
        askVolume = None
        if 'bid' in ticker:
            if isinstance(ticker['bid'], list):
                bid = self.safe_float(ticker['bid'], 0)
                bidVolume = self.safe_float(ticker['bid'], 1)
        if 'ask' in ticker:
            if isinstance(ticker['ask'], list):
                ask = self.safe_float(ticker['ask'], 0)
                askVolume = self.safe_float(ticker['ask'], 1)
        open_ = self.safe_float(ticker, 'open') or None
        close = self.safe_float(ticker, 'close')
        change = None
        percentage = None
        average = None
        if (open_ is not None) and (close is not None):
            change = close - open_
            average = self.sum(open_, close) / 2
            percentage = (change / open_) * 100
        baseVolume = self.safe_float(ticker, 'amount')
        quoteVolume = self.safe_float(ticker, 'vol')
        vwap = None
        if baseVolume is not None and quoteVolume is not None and baseVolume > 0:
            vwap = quoteVolume / baseVolume

        if market:
            symbol = market['symbol']
        else:
            symbol = self.safe_string_2(ticker, 'symbol', 'contract_code')

        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'high': self.safe_float(ticker, 'high'),
            'low': self.safe_float(ticker, 'low'),
            'ask': ask,
            'askVolume': askVolume,
            'bid': bid,
            'bidVolume': bidVolume,
            'vwap': vwap,
            'open': open_,
            'close': close,
            'previousClose': None,
            'change': change,
            'percentage': percentage,
            'average': average,
            'baseVolume': baseVolume,
            'quoteVolume': quoteVolume,
            'info': ticker,
        }

    def fetch_order_book(self, symbol, limit=None, params=None):
        self.load_markets()
        market = self.market(symbol)
        method = self.find_method(market['type'], 'GetMarketDepth')
        if market['type'] == 'futures':
            request = {'symbol': market['id']}
        else:
            request = {'contract_code': market['id']}
        request['type'] = 'step0'
        response = method(self.extend(request, params or {}))
        # {
        #   "asks": [
        #     [10266.11, 6],
        #     [10267.12, 8],
        #     [10267.4, 3],
        #     ...
        #   ],
        #   "bids": [
        #     [10266.1, 2185],
        #     [10265, 13],
        #     [10264.16, 169],
        #     ...
        #   ],
        #   "ch": "market.BTC_CW.depth.step0",
        #   "id": 1568021905,
        #   "mrid": 17747560337,
        #   "ts": 1568021905901,
        #   "version": 1568021905
        # }
        if 'tick' in response:
            if not response['tick']:
                raise ExchangeError(self.id + ' fetchOrderBook() returned empty response: ' + self.json(response))
            orderbook = self.safe_value(response, 'tick')
            result = self.parse_order_book(orderbook, orderbook['ts'])
            result['nonce'] = orderbook['version']
            return result
        raise ExchangeError(self.id + ' fetchOrderBook() returned unrecognized response: ' + self.json(response))

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None,
                    limit=1000, params=None):
        self.load_markets()
        market = self.market(symbol)
        method = self.find_method(market['type'], 'GetMarketHistoryKline')
        if market['type'] == 'futures':
            request = {'symbol': market['id']}
        else:
            request = {'contract_code': market['id']}
        request.update({
            'period': self.timeframes[timeframe],
            'size': limit or 1000
        })
        response = method(self.extend(request, params or {}))
        # {
        #   "ch": "market.BTC_CQ.kline.1min",
        #   "data": [
        #     {
        #       "amount": 223.79326383537403,
        #       "close": 10350.82,
        #       "count": 660,
        #       "high": 10351.34,
        #       "id": 1568022540,
        #       "low": 10344.14,
        #       "open": 10344.15,
        #       "vol": 23158
        #     }
        #   ],
        #   "status": "ok",
        #   "ts": 1568022590047
        # }
        return self.parse_ohlcvs(response['data'], market, timeframe, since, limit)

    def parse_ohlcv(self, ohlcv, market=None, timeframe='1m', since=None, limit=None):
        return [
            self.safe_timestamp(ohlcv, 'id'),
            self.safe_float(ohlcv, 'open'),
            self.safe_float(ohlcv, 'high'),
            self.safe_float(ohlcv, 'low'),
            self.safe_float(ohlcv, 'close'),
            self.safe_float(ohlcv, 'amount'),
        ]

    def fetch_trades(self, symbol, since=None, limit=1000, params=None):
        self.load_markets()
        market = self.market(symbol)
        method = self.find_method(market['type'], 'GetMarketHistoryTrade')
        if market['type'] == 'futures':
            request = {'symbol': market['id']}
        else:
            request = {'contract_code': market['id']}
        request['size'] = limit or 1000
        response = method(self.extend(request, params or {}))
        # {
        #   "ch": "market.BTC_CQ.trade.detail",
        #   "data": [
        #     {
        #       "data": [
        #         {
        #           "amount": 20,
        #           "direction": "sell",
        #           "id": 177510101820000,
        #           "price": 10321.21,
        #           "ts": 1568024107339
        #         }
        #       ],
        #       "id": 17751010182,
        #       "ts": 1568024107339
        #     }
        #   ],
        #   "status": "ok",
        #   "ts": 1568024107692
        # }
        data = self.safe_value(response, 'data')
        result = []
        for i in range(0, len(data)):
            trades = self.safe_value(data[i], 'data', [])
            for j in range(0, len(trades)):
                trade = self.parse_trade(trades[j], market)
                result.append(trade)
        result = self.sort_by(result, 'timestamp')
        return self.filter_by_symbol_since_limit(result, symbol, since, limit)

    def fetch_my_trades(self, symbol=None, since=None, limit=None, params=None):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' fetchMyTrades requires a symbol argument')
        self.load_markets()
        market = self.market(symbol)

        if market['type'] == 'swap.usdt' and self.options['marginMode'] == 'cross':
            margin_mode = 'Cross'
        else:
            margin_mode = ''
        method = self.find_method(market['type'], f'Post{margin_mode}Matchresults')

        if market['type'] == 'futures':
            request = {
                'symbol': market['base'],
                'contract_code': market['info']['contract_code']
            }
        else:
            request = {
                'contract_code': market['id'],
            }
        request.update({
            'trade_type': 0,
            'create_date': 90,
            'page_index': 1,
            'page_size': 50
        })
        response = method(self.extend(request, params or {}))
        # {
        #   "data": {
        #     "current_page": 1,
        #     "total_page": 1,
        #     "total_size": 2,
        #     "trades": [{
        #       "contract_code": "EOS190419",
        #       "contract_type": "this_week",
        #       "create_date": 1555553626736,
        #       "direction": "sell",
        #       "match_id": 3635853382,
        #       "offset": "close",
        #       "offset_profitloss": 0.15646398812252696,
        #       "order_id": 1118,
        #       "symbol": "EOS",
        #       "trade_fee": -0.002897500905469032,
        #       "trade_price": 5.522,
        #       "trade_turnover": 80,
        #       "role": "maker",
        #       "trade_volume": 8
        #     }]
        #   },
        #   "status": "ok",
        #   "ts": 1555654870867
        # }
        return self.parse_trades(response['data']['trades'], market,
                                 since, limit)

    def parse_trade(self, trade, market=None):
        trade_id = self.safe_string_2(trade, 'id', 'match_id')
        order_id = self.safe_string(trade, 'order_id')
        timestamp = self.safe_integer_2(trade, 'ts', 'create_date')
        price = self.safe_float_2(trade, 'price', 'trade_price')
        amount = self.safe_float_2(trade, 'amount', 'trade_volume')
        side = self.safe_string(trade, 'direction')
        role = self.safe_string(trade, 'role')
        fee = None
        fee_cost = self.safe_float(trade, 'trade_fee')
        if fee_cost is not None:
            fee = {
                'cost': -fee_cost,
                'currency': self.safe_string(trade, 'fee_asset'),
            }
        symbol = market['symbol'] if market else None
        return {
            'id': trade_id,
            'info': trade,
            'order': order_id,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'symbol': symbol,
            'type': None,
            'side': side,
            'takerOrMaker': role,
            'price': price,
            'amount': amount,
            'cost': None,
            'fee': fee,
        }

    def fetch_order(self, id, symbol=None, params=None):
        params = params or {}
        ids = [id] if id else None
        orders = self.fetch_orders_by_ids(ids, symbol, params=params)
        if orders:
            return orders[0]
        raise OrderNotFound(f'{self.id} order {id} not found')

    def fetch_orders(self, symbol=None, since=None, limit=None, params=None):
        return self.fetch_orders_by_ids(None, symbol, since, limit, params or {})

    def fetch_orders_by_ids(self, ids, symbol, since=None,
                            limit=None, params=None):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' fetchOpenOrders requires a symbol argument')
        self.load_markets()
        market = self.market(symbol)

        if market['type'] == 'swap.usdt' and self.options['marginMode'] == 'cross':
            margin_mode = 'Cross'
        else:
            margin_mode = ''
        method = self.find_method(market['type'], f'Post{margin_mode}OrderInfo')

        if market['type'] == 'futures':
            request = {'symbol': market['base']}
        else:
            request = {'contract_code': market['id']}
        if ids:
            request['order_id'] = ','.join(ids)
        response = method(self.extend(request, params or {}))
        # {
        #   "status": "ok",
        #   "data": [
        #     {
        #       "symbol": "BTC",
        #       "contract_code": "BTC190927",
        #       "contract_type": "quarter",
        #       "volume": 1,
        #       "price": 10400,
        #       "order_price_type": "limit",
        #       "order_type": 1,
        #       "direction": "buy",
        #       "offset": "open",
        #       "lever_rate": 10,
        #       "order_id": 6145283623,
        #       "client_order_id": null,
        #       "created_at": 1568106770000,
        #       "trade_volume": 1,
        #       "trade_turnover": 100,
        #       "fee": -0.000002414311264596,
        #       "trade_avg_price": 10354.92,
        #       "margin_frozen": 0,
        #       "profit": 0,
        #       "status": 6,
        #       "order_source": "api"
        #     },
        #     ...
        #   ],
        #   "ts": 1568106883922
        # }
        return self.parse_orders(response['data'], market, since, limit)

    def fetch_open_orders(self, symbol=None, since=None,
                          limit=None, params=None):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' fetchOpenOrders requires a symbol argument')
        self.load_markets()
        market = self.market(symbol)

        if market['type'] == 'swap.usdt' and self.options['marginMode'] == 'cross':
            margin_mode = 'Cross'
        else:
            margin_mode = ''
        method = self.find_method(market['type'], f'Post{margin_mode}Openorders')

        if market['type'] == 'futures':
            request = {'symbol': market['base']}
        else:
            request = {'contract_code': market['id']}
        request.update({
            'page_index': 1,
            'page_size': 50,
        })
        response = method(self.extend(request, params or {}))
        # {
        #   "status": "ok",
        #   "data":{
        #     "orders":[
        #       {
        #          "symbol": "BTC",
        #          "contract_type": "this_week",
        #          "contract_code": "BTC180914",
        #          "volume": 111,
        #          "price": 1111,
        #          "order_price_type": "limit",
        #          "direction": "buy",
        #          "offset": "open",
        #          "lever_rate": 10,
        #          "order_id": 106837,
        #          "client_order_id": 10683,
        #          "order_source": "web",
        #          "created_at": 1408076414000,
        #          "trade_volume": 1,
        #          "trade_turnover": 1200,
        #          "fee": 0,
        #          "trade_avg_price": 10,
        #          "margin_frozen": 10,
        #          "status": 1
        #       }
        #     ],
        #     "total_page":15,
        #     "current_page":3,
        #     "total_size":3
        #   },
        #   "ts": 1490759594752
        # }
        orders = self.parse_orders(response['data']['orders'], market, since, None)
        open_orders = [order for order in orders if order.get('status') == 'open']
        if limit:
            open_orders = open_orders[0:limit]
        return open_orders

    def parse_order(self, order, market=None):
        timestamp = self.safe_integer(order, 'created_at')
        status = self.safe_string(order, 'status')
        status = self.parse_order_status(status)
        currency = self.safe_string(order, 'symbol')
        type = market['type'] if market else self.options['defaultType']
        if type == 'futures':
            contract_type = self.safe_string(order, 'contract_type')
            expiration = self.expirations[contract_type]
            symbol = f'{currency}_{expiration}'
        else:
            symbol = self.safe_string(order, 'contract_code')
        side = self.safe_string(order, 'direction')
        price = self.safe_float(order, 'price')
        average = self.safe_float(order, 'trade_avg_price')
        amount = self.safe_float(order, 'volume')
        filled = self.safe_float(order, 'trade_volume')
        remaining = None
        if filled is not None and amount is not None:
            remaining = amount - filled
        fee = None
        fee_cost = self.safe_float(order, 'fee')
        if fee_cost is not None:
            fee = {
                'cost': fee_cost,
                'currency': self.safe_string(order, 'fee_asset'),
            }
        return {
            'id': self.safe_string(order, 'order_id'),
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'lastTradeTimestamp': None,
            'status': status,
            'symbol': symbol,
            'type': self.safe_string(order, 'order_price_type'),
            'side': side,
            'price': price,
            'average': average,
            'amount': amount,
            'filled': filled,
            'remaining': remaining,
            'cost': None,
            'fee': fee,
            'info': order,
        }

    def parse_order_status(self, status):
        statuses = {
            '1':  'open',       # pre commit
            '2':  'open',       # pre commit
            '3':  'open',       # committed
            '4':  'open',       # partial filled
            '5':  'canceled',   # partial canceled
            '6':  'closed',     # completed
            '7':  'canceled',   # canceled
            '11': 'canceled',   # cancelling
        }
        return self.safe_string(statuses, status, status)

    def create_order(self, symbol, type, side, amount, price=None, params=None):
        self.load_markets()
        market = self.market(symbol)
        
        if market['type'] == 'swap.usdt' and self.options['marginMode'] == 'cross':
            margin_mode = 'Cross'
        else:
            margin_mode = ''
        method = self.find_method(market['type'], f'Post{margin_mode}Order')

        params = params or {}
        lever_rate = params.get('lever_rate', self.options['leverage'])
        if type == 'market':
            type = 'optimal_20'
        if market['type'] == 'futures':
            request = {
                'symbol': market['base'],
                'contract_type': market['info']['contract_type'],
                'contract_code': market['info']['contract_code'],
            }
        else:
            request = {
                'contract_code': market['id']
            }
        request.update({
            'volume': self.amount_to_precision(symbol, amount),
            'direction': side,
            'offset': 'open',
            'lever_rate': lever_rate,
            'order_price_type': type
        })
        if type in ['limit', 'post_only', 'fok', 'ioc']:
            request['price'] = self.price_to_precision(symbol, price)
        response = method(self.extend(request, params))
        # {
        #   "status": "ok",
        #   "data": {
        #     "order_id": 6145283619
        #   },
        #   "ts": 1568105905237
        # }
        order_id = self.safe_string(response.get('data', {}), 'order_id')
        timestamp = self.safe_integer(response, 'ts')

        return {
            'info': response,
            'id': order_id,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'lastTradeTimestamp': None,
            'status': None,
            'symbol': symbol,
            'type': type,
            'side': side,
            'price': price,
            'amount': amount,
            'filled': None,
            'remaining': None,
            'cost': None,
            'trades': None,
            'fee': None,
        }

    def cancel_order(self, id, symbol=None, params=None):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' cancelOrder requires a symbol argument')
        ids = [id] if id else None
        return self.cancel_order_by_ids(ids, symbol, params)

    def cancel_order_by_ids(self, ids, symbol=None, params=None):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' cancelOrderByIds requires a symbol argument')
        market = self.market(symbol)

        if market['type'] == 'swap.usdt' and self.options['marginMode'] == 'cross':
            margin_mode = 'Cross'
        else:
            margin_mode = ''
        method = self.find_method(market['type'], f'Post{margin_mode}Cancel')

        if market['type'] == 'futures':
            request = {'symbol': market['base']}
        else:
            request = {'contract_code': market['id']}
        if ids:
            request['order_id'] = ','.join(ids)
        response = method(self.extend(request, params or {}))
        # {
        #   "status": "ok",
        #   "data": {
        #     "errors": [
        #       {
        #         "order_id": "161251",
        #         "err_code": 200417,
        #         "err_msg": "invalid symbol"
        #       },
        #       {
        #         "order_id": 161253,
        #         "err_code": 200415,
        #         "err_msg": "invalid symbol"
        #       }
        #     ],
        #     "successes": [
        #       161256,
        #       1344567
        #     ]
        #   },
        #   "ts": 1490759594752
        # }
        return response

    def cancel_all_orders(self, symbol=None, params=None):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' cancelAllOrders requires a symbol argument')
        market = self.market(symbol)

        if market['type'] == 'swap.usdt' and self.options['marginMode'] == 'cross':
            margin_mode = 'Cross'
        else:
            margin_mode = ''
        method = self.find_method(market['type'], f'Post{margin_mode}Cancelall')

        if market['type'] == 'futures':
            request = {
                'symbol': market['base'],
                'contract_type': market['info']['contract_type']
            }
        else:
            request = {
                'contract_code': market['id']
            }
        return method(self.extend(request, params or {}))

    def fetch_balance(self, params=None):
        self.load_markets()
        defaultType = self.safe_string_2(self.options, 'fetchBalance', 'defaultType')
        type = self.safe_string(params, 'type', defaultType)
        if type is None:
            raise ArgumentsRequired(
                f"{self.id} fetchBalance requires a type parameter"
                f"(one of 'futures', 'swap', 'swap.usdt')"
            )
        
        if type == 'swap.usdt' and self.options['marginMode'] == 'cross':
            margin_mode = 'Cross'
        else:
            margin_mode = ''
        method = self.find_method(type, f'Post{margin_mode}AccountInfo')
        response = method(params or {})
        return self.parse_balance(response)

    def parse_balance(self, response):
        # futures/swap-coin/swap-usdt isolated mode
        # {
        #   "status": "ok",
        #   "data": [
        #     {
        #        "symbol": "BTC",
        #        "margin_balance": 1,
        #        "margin_position": 0,
        #        "margin_frozen": 3.33,
        #        ...
        #     },
        #     ...
        #   ],
        #   "ts":158797866555
        # }

        # swap-usdt cross mode
        # {
        #     "status":"ok",
        #     "data":[
        #         {
        #             "margin_mode":"cross",
        #             "margin_account":"USDT",
        #             "margin_asset":"USDT",
        #             "margin_balance":0.000000549410817836,
        #             ...
        #             "contract_detail":[
        #                 {
        #                     "symbol":"BTC",
        #                     "contract_code":"BTC-USDT",
        #                     ...
        #                 },
        #                 ...
        #             ]
        #         }
        #     ],
        #     "ts":1606906200680
        # }

        balance = {}
        for item in response['data']:
            currency = self.safe_string_2(item, 'symbol', 'margin_account')
            total = self.safe_float(item, 'margin_balance')
            margin_position = self.safe_float(item, 'margin_position', 0.0)
            margin_frozen = self.safe_float(item, 'margin_frozen', 0.0)
            used = margin_position + margin_frozen
            free = total - used
            balance[currency] = {
                'total': total,
                'used': used,
                'free': free,
            }

        totalAll = {currency: balance[currency]['total'] for currency in balance}
        usedAll = {currency: balance[currency]['used'] for currency in balance}
        freeAll = {currency: balance[currency]['free'] for currency in balance}
        balance['total'] = totalAll
        balance['used'] = usedAll
        balance['free'] = freeAll
        return balance
        

    def fetch_status(self, params=None):
        response = self.generalGetHeartbeat(params or {})
        # {
        #    "status":"ok",
        #    "data":{
        #       "heartbeat":1,
        #       "estimated_recovery_time":null,
        #       "swap_heartbeat":1,
        #       "swap_estimated_recovery_time":null
        #    },
        #    "ts":1557714418033
        # }
        defaultType = self.safe_string_2(self.options, 'fetchStatus', 'defaultType')
        type = self.safe_string(params, 'type', defaultType)
        if type.startswith('swap'):
            key = 'swap_heartbeat'
        else:
            key = 'heartbeat'
        heartbeat = self.safe_integer(response['data'], key)
        if heartbeat == 1:
            status = 'ok'
        else:
            status = 'maintenance'
        self.status = {
            'status': status,
            'updated': self.milliseconds(),
        }
        return self.status

    def sign(self, path, api='public', method='GET', params={}, headers=None, body=None):
        if api == 'futures':
            if path.startswith(('market/', 'index/', 'lightning_')):
                prefix = ''
            else:
                prefix = f'api/{self.version}/contract_'

        elif api == 'swap':
            if path.startswith('market/'):
                prefix = 'swap-ex/'
            elif path.startswith('index/'):
                prefix = ''
            else:
                prefix = f'swap-api/{self.version}/swap_'

        elif api == 'swapusdt':
            if path.startswith('market/'):
                prefix = 'linear-swap-ex/'
            elif path.startswith('index/'):
                prefix = ''
            else:
                prefix = f'linear-swap-api/{self.version}/swap_'

        else:
            assert api == 'general' and path == 'heartbeat'
            url = self.urls['heartbeat'] + '/heartbeat'
            return {'url': url, 'method': method, 'body': body, 'headers': headers}

        url = f'/{prefix}{path}'
        url = self.implode_params(url, params)
        query = self.omit(params, self.extract_params(path))
        if method == 'POST' or path == 'api_trading_status':
            self.check_required_credentials()
            timestamp = self.ymdhms(self.milliseconds(), 'T')
            request = self.keysort({
                'SignatureMethod': 'HmacSHA256',
                'SignatureVersion': '2',
                'AccessKeyId': self.apiKey,
                'Timestamp': timestamp,
            })
            auth = self.urlencode(request)
            payload = '\n'.join([method, self.hostname, url, auth])
            signature = self.hmac(self.encode(payload), self.encode(self.secret), hashlib.sha256, 'base64')
            auth += '&' + self.urlencode({'Signature': signature})
            url += '?' + auth
            if method == 'POST':
                body = self.json(query)
                headers = {'Content-Type': 'application/json'}
            else:
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        else:
            if query:
                url += '?' + self.urlencode(query)
        url = self.urls['api'] + url
        return {'url': url, 'method': method, 'body': body, 'headers': headers}

    def handle_errors(self, httpCode, reason, url, method, headers, body,
                      response, requestHeaders, requestBody):
        if response is None:
            return  # fallback to default error handler
        if 'status' in response:
            # {
            #    "status":"error",
            #    "err-code":"order-limitorder-amount-min-error",
            #    "err-msg":"limit order amount error, min: `0.001`",
            #    "data":null
            # }
            status = self.safe_string(response, 'status')
            if status == 'error':
                code = self.safe_string(response, 'err-code')
                feedback = self.id + ' ' + body
                self.throw_exactly_matched_exception(self.exceptions['exact'], code, feedback)
                message = self.safe_string(response, 'err-msg')
                self.throw_exactly_matched_exception(self.exceptions['exact'], message, feedback)
                raise ExchangeError(feedback)
