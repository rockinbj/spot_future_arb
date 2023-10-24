import time
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed
from functions import *
from config import Exchange_List, Required_Profit_Pct, Required_Within_Days
from my_logger import get_logger
logger = get_logger("app.arb")


def main():
    path_root = Path(__file__).resolve().parent
    path_data = path_root/"data"
    path_data.mkdir(parents=True, exist_ok=True)

    # 多线程 获取 所有exchange 的合约价差list
    _s = time.time() * 1000  # 毫秒时间戳
    returns = Parallel(n_jobs=len(Exchange_List), backend="threading")(delayed(cal_profit_for_exchange)(ex_id) for ex_id in Exchange_List)

    # 将list去空合并成一个list
    futures = [item for sublist in returns if sublist for item in sublist]
    # 将list转换df并存储到本地
    df_fu = pd.DataFrame(futures)
    df_fu["runtime"] = pd.to_datetime(_s, unit="ms")
    df_file = path_data/"records.csv"
    if not df_file.exists():
        df_fu.to_csv(df_file, encoding="gbk", index=False)
    else:
        df_fu.to_csv(df_file, encoding="gbk", index=False, header=False, mode="a")

    logger.debug(f'df_contracts:\n{df_fu}')
    send_arb_alert(df_fu, required_pct=Required_Profit_Pct, required_within_days=Required_Within_Days)


if __name__ == '__main__':
    main()
