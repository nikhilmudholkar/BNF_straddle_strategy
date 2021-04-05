import pandas as pd
from base import BaseStrategy, BaseIntradayStrategy
import talib
import math


class StraddleStrategy(BaseIntradayStrategy):
    def setup(self):
        # print(self.dataframes['BNF_OPTIONS_INTRADAY'])
        pass

    def process_daily_candle(self):

        if (self.yesterday_date):
            if (self.day_open >= self.yesterday_low) and (self.day_open <= self.yesterday_high):
                self.condition_satisfied = True
            else:
                self.condition_satisfied = False
        else:
            print("first iteration")
        print(self.current_date, end="\r")
        options_df = self.dataframes['BNF_OPTIONS_INTRADAY']
        filtered_options_df = options_df[self.current_date.strftime('%Y-%m-%d')]
        self.atm_strike = int(math.ceil(self.day_open / 100.0)) * 100
        # print(self.atm_strike)
        filtered_options_df = filtered_options_df[filtered_options_df['Strike Price'] == self.atm_strike]

        filtered_options_df_ce = filtered_options_df[filtered_options_df['CE/PE'] == 'CE']
        filtered_options_df_pe = filtered_options_df[filtered_options_df['CE/PE'] == 'PE']

        if not filtered_options_df.empty and not filtered_options_df_ce.empty and not filtered_options_df_pe.empty:
            # print(filtered_options_df)
            expiry_name = pd.to_datetime(filtered_options_df_ce['Expiry'].unique()[0]).strftime('%d%b%Y')
        else:
            expiry_name = '000000'

        self.code_ce = expiry_name + str(self.atm_strike) + 'CE'
        self.code_pe = expiry_name + str(self.atm_strike) + 'PE'
        self.dataframes['BNF_OPTIONS_FILTERED'] = filtered_options_df

        self.dataframes[self.code_ce] = filtered_options_df_ce
        self.dataframes[self.code_pe] = filtered_options_df_pe
        # print(self.dataframes['BNF_OPTIONS_FILTERED_CE'])

    def process_candle(self):
        position = self.get_position(code=self.code_ce)
        pnl = 0
        # pnl_dict = {}
        for code, p in self.positions.items():
            if p.quantity != 0:
                pnl = pnl + p.get_unrealized(code=code)
                # print(f"code: {code}, quantity: {p.quantity},  unrealized profits: {p.get_unrealized(code = code)}")
                # pnl_dict[code] = p.get_unrealized(code=code)
        # print(pnl_dict)
        if position == 0:
            if self.condition_satisfied and self.current_row_ce['Time'] >= '09:15':
                self.sell(quantity=25, price=self.close_ce, info=f"First trade", code=self.code_ce)
                self.sell_price_ce = self.close_ce
                self.sell(quantity=25, price=self.close_pe, info=f"First trade", code=self.code_pe)
                self.sell_price_pe = self.close_pe

                self.stoploss = -0.1 * (self.sell_price_ce + self.sell_price_pe) * 25
                self.target_step = 0.1 * (self.sell_price_ce + self.sell_price_pe) * 25
                self.target = 0.1 * (self.sell_price_ce + self.sell_price_pe) * 25
        else:
            if pnl <= self.stoploss or pnl <= -3000:
                print(f"pnl = {pnl}")
                print(f"stoploss = {-0.1*(self.sell_price_ce + self.sell_price_pe)*25}")
                print(self.current_date)
                self.buy(quantity=25, price=self.close_ce, info=f"stoploss breached", code=self.code_ce)
                self.buy(quantity=25, price=self.close_pe, info=f"stoploss breached", code=self.code_pe)
                self.condition_satisfied = False
            elif pnl >= self.target:
                self.stoploss = self.stoploss + self.target_step
                self.target = self.target + self.target_step
                print(f"pnl = {pnl}")
                print(f"new stoploss = {self.stoploss}")
                print(f"new target = {self.target}")
                print(self.current_date)
                # self.buy(quantity=25, price=self.close_ce, info="target reached", code=self.code_ce)
                # self.buy(quantity=25, price=self.close_pe, info="target reached", code=self.code_pe)
                # self.condition_satisfied = False
            elif self.current_row_ce['Time'] >= '15:15':
                print(f"pnl = {pnl}")
                print(f"stoploss = {-0.1*(self.sell_price_ce + self.sell_price_pe)*25}")
                print(self.current_date)
                self.buy(quantity=25, price=self.close_ce, info=f"End of Day reached", code=self.code_ce)
                self.buy(quantity=25, price=self.close_pe, info=f"End of day reached", code=self.code_pe)
                self.condition_satisfied = False
            else:
                pass


df_options = pd.read_csv('CleanedData.csv', index_col=7, parse_dates=True)
# df_options = df_options.drop_duplicates(subset=['DateTime', 'CE/PE', 'Strike Price'])
df_bnf_daily = pd.read_csv('BANKNIFTY_daily_data.csv', index_col=0, parse_dates=True)
strategy_obj = StraddleStrategy(amount=1000000)
strategy_obj.add_dataframe(df_bnf_daily, 'BNF_DAILY', is_primary=True)
strategy_obj.add_dataframe(df_options, 'BNF_OPTIONS_INTRADAY')
strategy_obj.execute()

transactions = pd.DataFrame(data=strategy_obj.transactions)
print(transactions)

transactions.to_csv('transactions_with_sideways_condition.csv')

transactions = pd.DataFrame(data=strategy_obj.daily_log)
print(transactions)
transactions.to_csv('daily_log.csv')
