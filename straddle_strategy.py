import pandas as pd
from base import BaseStrategy, BaseIntradayStrategy
import talib
import datetime
import math


class StraddleStrategy(BaseIntradayStrategy):
    def setup(self):
        # print(self.dataframes['BNF_OPTIONS_INTRADAY'])
        pass

    def process_daily_candle(self):

        if self.yesterday_date:
            if self.sideways_condition_check:
                # print("checking sideways condition")
                if (self.day_open >= self.yesterday_low) and (self.day_open <= self.yesterday_high):
                    self.condition_satisfied = True
                else:
                    self.condition_satisfied = False
            else:
                # print("ignoring sideways condition")
                self.condition_satisfied = True
        else:
            print("first iteration")
        print(self.current_date, end="\r")
        options_df = self.dataframes['BNF_OPTIONS_INTRADAY']
        filtered_options_df = options_df[self.current_date.strftime('%Y-%m-%d')]
        self.atm_strike = int(math.ceil(self.day_open / 100.0)) * 100
        filtered_options_df = filtered_options_df[filtered_options_df['Strike Price'] == self.atm_strike]
        filtered_options_df_ce = filtered_options_df[filtered_options_df['CE/PE'] == 'CE']
        filtered_options_df_pe = filtered_options_df[filtered_options_df['CE/PE'] == 'PE']

        if not filtered_options_df.empty and not filtered_options_df_ce.empty and not filtered_options_df_pe.empty:
            expiry_name = pd.to_datetime(filtered_options_df_ce['Expiry'].unique()[0]).strftime('%d%b%Y')
        else:
            expiry_name = '000000'

        self.code_ce = expiry_name + str(self.atm_strike) + 'CE'
        self.code_pe = expiry_name + str(self.atm_strike) + 'PE'
        self.dataframes['BNF_OPTIONS_FILTERED'] = filtered_options_df

        self.dataframes[self.code_ce] = filtered_options_df_ce
        self.dataframes[self.code_pe] = filtered_options_df_pe

    def process_candle(self):
        position = self.get_position(code=self.code_ce)
        start_time = datetime.datetime.strptime(self.start_time, '%H:%M').time()
        end_time = datetime.datetime.strptime(self.end_time, '%H:%M').time()
        current_time = datetime.datetime.strptime(self.current_row_ce['Time'], '%H:%M').time()
        pnl = 0

        for code, p in self.positions.items():
            if p.quantity != 0:
                pnl = pnl + p.get_unrealized(code=code)

        if position == 0:
            # TODO check if current time is being compared in string format. If not, convert to datetime

            if self.condition_satisfied and current_time >= start_time:
                self.sell(quantity=25, price=self.close_ce, info=f"First trade", code=self.code_ce)
                self.sell_price_ce = self.close_ce
                self.sell(quantity=25, price=self.close_pe, info=f"First trade", code=self.code_pe)
                self.sell_price_pe = self.close_pe
                self.stoploss = -self.stoploss_pct * (self.sell_price_ce + self.sell_price_pe) * 25
                self.target_step = self.step_pct * (self.sell_price_ce + self.sell_price_pe) * 25
                self.target = self.target_pct * (self.sell_price_ce + self.sell_price_pe) * 25
        else:
            if pnl <= self.stoploss or pnl <= -3000:
                print(f"pnl = {pnl}")
                print(f"stoploss = {-self.stoploss_pct * (self.sell_price_ce + self.sell_price_pe) * 25}")
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
            elif current_time >= end_time:
                print(f"pnl = {pnl}")
                print(f"stoploss = {-self.stoploss_pct * (self.sell_price_ce + self.sell_price_pe) * 25}")
                print(self.current_date)
                self.buy(quantity=25, price=self.close_ce, info=f"End of Day reached", code=self.code_ce)
                self.buy(quantity=25, price=self.close_pe, info=f"End of day reached", code=self.code_pe)
                self.condition_satisfied = False
            else:
                pass


df_options = pd.read_csv('CleanedData.csv', index_col=7, parse_dates=True)
# df_options = df_options.drop_duplicates(subset=['DateTime', 'CE/PE', 'Strike Price'])
df_bnf_daily = pd.read_csv('BANKNIFTY_daily_data.csv', index_col=0, parse_dates=True)
param_dict = {'start_time': "09:15",
              'end_time': "15:15",
              'sideways_condition_check': True,
              'step_pct': 0.1,
              'stoploss_pct': 0.1,
              'target_pct': 0.1
              }
strategy_obj = StraddleStrategy(amount=1000000, params = param_dict)
strategy_obj.add_dataframe(df_bnf_daily, 'BNF_DAILY', is_primary=True)
strategy_obj.add_dataframe(df_options, 'BNF_OPTIONS_INTRADAY')
strategy_obj.execute()

transactions = pd.DataFrame(data=strategy_obj.transactions)
print(transactions)

transactions.to_csv('transactions_with_sideways_condition.csv')

transactions = pd.DataFrame(data=strategy_obj.daily_log)
print(transactions)
transactions.to_csv('daily_log.csv')
