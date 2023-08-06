from joblib import Parallel, delayed
from functions import *
from config import Exchange_List, Required_Profit_Pct
from my_logger import get_logger
logger = get_logger("app.arb")


def main():

    futures = Parallel(n_jobs=3, backend="threading")(delayed(cal_profit_for_exchange)(ex_id) for ex_id in Exchange_List)
    futures = {k: v for d in futures for k, v in d.items() if v}

    logger.debug(futures)
    send_arb_alert(futures, required_pct=Required_Profit_Pct)


if __name__ == '__main__':
    main()
