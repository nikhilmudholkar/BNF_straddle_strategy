# https://www.diffchecker.com/diff
from positions import Position
import datetime
import pandas as pd


class BaseStrategy:
    primary_dataframe = None
    primary_code = None

    current_row = None
    dataframes = dict()

    current_loc = None
    current_iloc = None

    positions = dict()

    daily_log = []

    transactions = []

    def __init__(self, amount=None, config=dict()):
        self.amount = amount
        self.original_amount = amount
        self.config = config

    def add_dataframe(self, df, code, is_primary=False):
        if is_primary:
            self.primary_dataframe = df
            self.primary_code = code
        self.dataframes[code] = df

    def execute(self):
        self.setup()
        counter = 0
        for i, row in self.primary_dataframe.iterrows():
            self.current_row = row
            self.current_loc = i
            self.current_iloc = counter
            counter += 1
            self.pre_process_candle()
            self.process_candle()
            self.process_eod_candle()

    def get_position(self, code=None):
        if code is None:
            code = self.primary_code
        position = self.positions.get(code)

        if position is None:
            # print('2>>>', id(self))
            self.positions[code] = Position(code=code, strategy=self)
        return self.positions[code]

    def buy(self, quantity=1, price=None, info=None, code=None):
        self._transact(quantity=quantity, price=price, info=info, code=code)

    def sell(self, quantity=1, price=None, info=None, code=None):
        quantity = -1 * quantity
        # print(code)
        self._transact(quantity=quantity, price=price, info=info, code=code)

    def get_current_row_from_dataframe(self, df):
        current_index = self.current_loc
        # print(f"current index = {current_index}")
        if current_index is not None:
            return df.loc[current_index]
        else:
            return None
        # return df.loc[current_index]

    def _transact(self, quantity=None, price=None, info=None, code=None):
        if code is None:
            code = self.primary_code
        position = self.get_position(code=code)
        position.transact(price=price, quantity=quantity, info=info)

    def pre_process_candle(self):
        pass

    def process_eod_candle(self):
        log = dict()
        log['date'] = self.current_loc
        log['amount'] = self.amount
        log['unrealized'] = 0

        for code, p in self.positions.items():
            if p.quantity != 0:
                log['unrealized'] += p.get_unrealized()

        self.daily_log.append(log)


class BaseIntradayStrategy(BaseStrategy):
    day_high = None
    day_low = None
    day_open = None
    day_close = None

    yesterday_high = None
    yesterday_low = None
    yesterday_open = None
    yesterday_close = None

    current_date = None
    current_time = None

    def execute(self):
        self.setup()
        counter = 0

        for index, row in self.primary_dataframe.iterrows():
            self.spot_current_row = row
            self.spot_current_loc = index

            self.pre_process_daily_candle()
            self.process_daily_candle()

            for i, r in self.dataframes[self.code_ce].iterrows():
                # print(i)
                if i in self.dataframes[self.code_pe].index:

                    # print(i)
                    # print(self.dataframes['BNF_OPTIONS_FILTERED_PE'].loc[i])
                    self.current_row_ce = r
                    self.current_row_pe = self.dataframes[self.code_pe].loc[i]
                    self.current_loc = i
                    self.current_iloc = counter
                    counter += 1
                    # if counter < 500:
                    #     continue

                    try:
                        self.pre_process_candle()
                        self.process_candle()
                        self.process_eod_candle()
                    except Exception as e:
                        # print(e)
                        pass
                else:
                    counter += 1

    def pre_process_candle(self):
        cr_ce = self.current_row_ce
        cr_pe = self.current_row_pe
        cd = self.current_loc.date()

        self.current_time = self.current_loc.time()
        expiry_name = pd.to_datetime(cr_ce['Expiry']).strftime('%d%b%Y')
        self.expiry = expiry_name
        self.close_ce = cr_ce['Close']
        self.close_pe = cr_pe['Close']

    def process_eod_candle(self):
        if self.current_time == datetime.time(hour=15, minute=25):
            log = dict()
            log['date'] = self.current_date
            log['amount'] = self.amount
            log['unrealized'] = 0
            for code, p in self.positions.items():
                # print(f"code: {code},  unrealized profits: {p.get_unrealized(code = code)}")
                if p.quantity != 0:
                    log['unrealized'] += p.get_unrealized(code=code)

            self.daily_log.append(log)

    def pre_process_daily_candle(self):
        cr = self.spot_current_row
        cd = self.spot_current_loc.date()

        # self.current_time = self.current_loc.time()

        if self.current_date is None:
            self.day_open = cr['Open']
            self.day_high = cr['High']
            self.day_low = cr['Low']
            self.current_date = cd
            self.yesterday_date = None
            self.condition_satisfied = False

        if self.current_date < cd:
            self.yesterday_open = self.day_open
            self.yesterday_low = self.day_low
            self.yesterday_high = self.day_high
            self.yesterday_close = self.day_close
            self.yesterday_date = self.current_date

            self.day_open = cr['Open']
            self.day_high = cr['High']
            self.day_low = cr['Low']

            self.current_date = cd

        # if cr['High'] > self.day_high:
        #     self.day_high = cr['High']
        #
        # if cr['Low'] < self.day_low:
        #     self.day_low = cr['Low']
        self.day_high = cr['High']
        self.day_low = cr['Low']
        self.day_close = cr['Close']
