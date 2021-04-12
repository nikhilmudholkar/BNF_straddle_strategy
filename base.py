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

    def __init__(self, amount=None, params=None, config=dict()):
        self.amount = amount
        self.original_amount = amount
        self.config = config
        self.current_loc = None
        self.current_iloc = None
        for key, value in params.items():
            exec(f"self.{key}" + " = value")

    def add_dataframe(self, df, code, is_primary=False):
        if is_primary:
            self.primary_dataframe = df
            self.primary_code = code
        self.dataframes[code] = df

    def execute(self):
        self.setup()
        self.day_ignore = False
        counter = 0
        for i, row in self.primary_dataframe.iterrows():
            self.current_row = row
            self.current_loc = i
            self.current_iloc = counter
            counter += 1
            try:
                self.pre_process_candle()
                self.process_candle()
                self.process_eod_candle()
            except:
                pass


    def get_position(self, code=None):
        if code is None:
            code = self.primary_code
        # print(code)
        position = self.positions.get(code)

        if position is None:
            # print('2>>>', id(self))
            # print(self.current_loc)
            self.positions[code] = Position(code=code, strategy=self)
        return self.positions[code]

    def buy(self, quantity=1, price=None, info=None, code=None):
        self._transact(quantity=quantity, price=price, info=info, code=code)

    def sell(self, quantity=1, price=None, info=None, code=None):
        quantity = -1 * quantity
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
        # print("#######")
        # print(self.current_loc)
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

    def pre_process_candle(self):
        cr = self.current_row
        cd = self.current_loc.date()
        self.current_datetime = self.current_loc
        self.current_time = self.current_loc.time()

        if self.current_date is None:
            self.day_open = cr['Open']
            self.day_high = cr['High']
            self.day_low = cr['Low']
            self.current_date = cd
            self.yesterday_date = None

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

        if cr['High'] > self.day_high:
            self.day_high = cr['High']

        if cr['Low'] < self.day_low:
            self.day_low = cr['Low']

        self.day_close = cr['Close']
        self.curr_open = cr['Open']

    def process_eod_candle(self):
        if self.current_time == datetime.time(hour=15, minute=00):
            log = dict()
            log['date'] = self.current_date
            log['amount'] = self.amount
            log['unrealized'] = 0
            for code, p in self.positions.items():
                if p.quantity != 0:
                    log['unrealized'] += p.get_unrealized(code=code)
            self.daily_log.append(log)
