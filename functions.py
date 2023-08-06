import time
import ccxt
import pandas as pd
import requests
from config import MIXIN_TOKEN, RUN_NAME, Only_Current_Period, Lowest_Profit_Pct, Required_Profit_Pct
from my_logger import get_logger
logger = get_logger("app.func")


def get_spot_symbol_from_symbol_dict(symbol_dict:dict):
    """
    ccxt load_markets的结果是若干个symbol字典，从一个symbol字典中返回对应的现货symbol
    "symbol":"BTC/USD:BTC-230929字典" -- 现货symbol：BTC/USDT
    :param symbol: ccxt load_markets结果中的一个symbol字典：BTC/USD:BTC-230929
    :return: symbol中的base币种字符串：BTC/USDT
    """
    return f'{symbol_dict["base"]}/USDT'


def get_last_price_from_symbol(exchange:ccxt.Exchange, symbol:str, timeframe="1m"):
    """
    通过symbol获取最后一根 已闭合 k线的收盘价，BTC/USDT --> 30000.0
    不包含当前未闭合k线
    :param timeframe: 获取最后一根 已闭合 的收盘价, 默认1m
    :param exchange: ccxt exchange实例, 现货实例就获取现货价格，合约实例就获取合约价格
    :param symbol: spot symbol string, 例如 'BTC/USDT'
    :return: spot price float, 30000.0
    """
    klines = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=5)
    df = pd.DataFrame(klines, columns=['candle_begin_time', 'open', 'high', 'low', 'close', 'volume'])
    df["candle_begin_time"] = pd.to_datetime(df["candle_begin_time"], unit="ms")
    try:
        price = float(df.iloc[-2]["close"])
    except IndexError as err:
        logger.exception(err)
        logger.error(f'symbol: {symbol}')
        logger.error(f'df:\n{df}')
        price = -1
    del klines, df
    return price


def get_cm_fu_from_markets(markets:dict):
    """
    从ccxt load_markets获取 币本位交割合约 的symbols，例如 BNB/USD:BNB-231229
    判定条件：
        1、symbol中包含'-'，说明是 交割合约
        2、symbol中不包含':USDT'，说明是 币本位
        3、symbol中包含'/USD'，说明是 U计价，会有 ADA/BTC:BTC-230929 这样非U计价的合约
        4、基础货币与结算货币相同，有这样的合约 ETH/USD:BTC-230929，基础货币是ETH、结算货币是BTC
        5、到期时间戳 大于 当前时间戳，说明 未下架
        6、只有1个'-'，排除 BTC/USD:BTC-230807-28000-C 这样的期权symbol
    :param markets: ccxt load_markets的返回值 dict
    :return: 所有 币本位交割合约 的dict，例如 { "BNB/USD:BNB-231229": {...}, ...}
    """
    return {
            key: value for key, value in markets.items()
                if "-" in key
                and ":USDT" not in key
                and "/USD" in key
                and value["base"] == value["settle"]
                and value["expiry"] > time.time()*1000
                and key.count("-") == 1
            }


def send_mixin_msg(msg, _type="PLAIN_TEXT"):
    """
    发送mixin消息
    :param msg:
    :param _type: "PLAIN_TEXT"文字信息，"PLAIN_POST" MARKDOWN信息
    :return:
    """
    token = MIXIN_TOKEN
    url = f"https://webhook.exinwork.com/api/send?access_token={token}"

    msg = f"{RUN_NAME}\n" + msg
    value = {
        'category': _type,
        'data': msg,
    }

    try:
        r = requests.post(url, data=value, timeout=2).json()
    except Exception as err:
        logger.error(f"sendMixin Error: {err}")
        logger.exception(err)


def cal_profit_for_exchange(exchange_id):
    """
    计算一个交易所内的期现套利价差
    返回的合约list 示例：
    [
    {"exchange":"binance",
    "contract":"BTC/USD:BTC-230818",
    "price_con":  29341.3   ,
    "price_spot":  29043.92 ,
    "profit": 0.0102,
    },
    ...]
    :param exchange_id: ccxt交易所ID，https://github.com/ccxt/ccxt
    :return: 合约list
    """

    ex = getattr(ccxt, exchange_id)()
    mkts = ex.load_markets()

    symbols_fu_dict = get_cm_fu_from_markets(markets=mkts)

    # 要返回的合约集合, (合约，收益率)
    # {'binance': [('BTC/USD:BTC-230929', 0.0151), ('LINK/USD:LINK-230929', 0.0222)]}
    # [{"exchange":"binance", "contract": "BTC/USD:BTC-230929", "price_con":29999.0, "price_spot":29980.0, "profit":0.0155}, ...]
    # futures = {exchange_id: []}
    futures = []

    for symbol_fu, symbol_dict in symbols_fu_dict.items():
        price_fu = get_last_price_from_symbol(exchange=ex, symbol=symbol_fu)
        symbol_spot = get_spot_symbol_from_symbol_dict(symbol_dict)
        price_spot = get_last_price_from_symbol(exchange=ex, symbol=symbol_spot)

        pct = price_fu / price_spot - 1

        cond_period = (symbol_dict["expiry"] - time.time()*1000 < 1000*3600*24*90) if Only_Current_Period else True
        cond_required = pct > Lowest_Profit_Pct

        if cond_period and cond_required:
            logger.info(f'{exchange_id} {symbol_fu} 期末无风险利润 {pct:.2%}')
            logger.debug(f'{exchange_id} {symbol_fu}({price_fu}) ~ {symbol_spot}({price_spot}) ~ {pct:.2%}')
            # futures[exchange_id].append((symbol_fu, round(pct, 4)))
            futures.append({
                "exchange": exchange_id,
                "contract": symbol_fu,
                "price_con": price_fu,
                "price_spot": price_spot,
                "profit": round(pct, 4),
            })
        time.sleep(0.05)

    return futures


def send_arb_alert(futures:pd.DataFrame, required_pct:float=0.02):
    """
    从合约集合中找出符合 收益率 要求的合约，发送mixin通知
    :param required_pct: 收益率要求，默认2%，发送收益率超过2%的合约信息
    :param futures: 主程序 获取 所有交易所 合约集合的结果
    :return:
    """
    msg = f"### 收益率>{Lowest_Profit_Pct:.2%} 的合约\n"
    msg_higher_than_rqr = ""
    will_send = False

    grouped = futures.groupby('exchange')

    for ex_id, group in grouped:
        msg += f'#### {ex_id}\n'
        for index, row in group.iterrows():
            contract = row['contract']
            profit = row['profit']
            msg += f'- {contract} 期末收益率 {profit:.2%}\n'
            if profit > required_pct:
                msg_higher_than_rqr += f'- {ex_id} {contract} {profit:.2%}\n'
                will_send = True

    if msg_higher_than_rqr:
        msg = f"### 出现 收益率>{required_pct:.2%} 的合约\n" + msg_higher_than_rqr + msg
    else:
        msg = f"### 无 高收益 合约\n" + msg

    if will_send:
        send_mixin_msg(msg, _type="PLAIN_POST")
        logger.debug(f"出现 高收益合约，发送完成")
    else:
        logger.debug(f"没有 高收益合约，不发送，只本地保存")
