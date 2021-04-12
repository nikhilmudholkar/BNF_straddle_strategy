import pandas as pd
from base import BaseStrategy, BaseIntradayStrategy
import talib
import sys
import datetime
import math
import gc


class StraddleStrategy(BaseIntradayStrategy):
    def setup(self):
        pass

    def process_candle(self):

        start_time = datetime.datetime.strptime(self.start_time, '%H:%M').time()
        end_time = datetime.datetime.strptime(self.end_time, '%H:%M').time()
        current_time = self.current_time
        pnl = 0

        if current_time == start_time:
            if self.yesterday_date:
                if self.sideways_condition_check:
                    if (self.day_open >= self.yesterday_low) and (self.day_open <= self.yesterday_high):
                        self.condition_satisfied = True
                    else:
                        self.condition_satisfied = False
                else:
                    self.condition_satisfied = True
            else:
                print("first iteration")
                self.condition_satisfied = True
            options_df = self.dataframes['BNF_OPTIONS_INTRADAY']

            self.atm_strike = int(math.ceil(self.curr_open / 100.0)) * 100
            filtered_options_df = options_df[options_df['Strike Price'] == self.atm_strike]
            filtered_options_df_ce = filtered_options_df[filtered_options_df['CE/PE'] == 'CE']
            filtered_options_df_pe = filtered_options_df[filtered_options_df['CE/PE'] == 'PE']
            if not filtered_options_df_ce.empty and not filtered_options_df_pe.empty:
                expiry_name = pd.to_datetime(filtered_options_df_ce['Expiry'].unique()[0]).strftime('%d%b%Y')
            else:
                expiry_name = '000000'
                # self.day_ignore = True
                # return None

            self.code_ce = expiry_name + str(self.atm_strike) + 'CE'
            self.code_pe = expiry_name + str(self.atm_strike) + 'PE'
            self.dataframes[self.code_ce] = filtered_options_df_ce
            self.dataframes[self.code_pe] = filtered_options_df_pe
            self.dataframes['BNF_OPTIONS_FILTERED'] = filtered_options_df
        filtered_options_df = self.dataframes['BNF_OPTIONS_FILTERED']

        current_options_df = filtered_options_df.loc[self.current_datetime]
        current_options_df_ce = current_options_df[current_options_df['CE/PE'] == 'CE']
        current_options_df_pe = current_options_df[current_options_df['CE/PE'] == 'PE']



        self.close_ce = current_options_df_ce['Close'].values[0]
        self.close_pe = current_options_df_pe['Close'].values[0]

        position = self.get_position(code=self.code_ce)

        for code, p in self.positions.items():
            if p.quantity != 0:
                pnl = pnl + p.get_unrealized(code=code)

        if position == 0:
            # TODO check if current time is being compared in string format. If not, convert to datetime
            # print(self.condition_satisfied)
            if self.condition_satisfied == True and current_time >= start_time:
                self.sell(quantity=25, price=self.close_ce, info=f"First trade", code=self.code_ce)
                self.sell_price_ce = self.close_ce

                self.sell(quantity=25, price=self.close_pe, info=f"First trade", code=self.code_pe)
                self.sell_price_pe = self.close_pe
                # print("SELL SUCCESSFUL")
                self.stoploss = -self.stoploss_pct * (self.sell_price_ce + self.sell_price_pe) * 25
                self.target_step = self.step_pct * (self.sell_price_ce + self.sell_price_pe) * 25
                self.target = self.target_pct * (self.sell_price_ce + self.sell_price_pe) * 25
                # self.condition_satisfied = False
        else:
            if pnl <= self.stoploss or pnl <= self.hard_stoploss:
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


parameters = pd.read_csv('parameters.csv', index_col=0)
for index, row in parameters.iterrows():
    param_dict = {'start_time': row['start_time'],
                  'end_time': row['end_time'],
                  'sideways_condition_check': row['sideways_condition_check'],
                  'step_pct': row['step_pct'],
                  'stoploss_pct': row['stoploss_pct'],
                  'hard_stoploss': row['hard_stoploss'],
                  'target_pct': row['target_pct']
                  }
    df_options = pd.read_csv('CleanedData.csv', index_col=7, parse_dates=True)
    df_bnf_intraday = pd.read_csv('BANKNIFTY_2019.csv')
    df_bnf_intraday["DateTime"] = pd.to_datetime(df_bnf_intraday["Date"], format='%Y%m%d').dt.strftime(
        '%Y-%m-%d') + " " + pd.to_datetime(df_bnf_intraday['Time'], format='%H:%M').dt.strftime('%H:%M:%S')
    # print(df_bnf_intraday['DateTime'])
    df_bnf_intraday['DateTime'] = pd.to_datetime(df_bnf_intraday["DateTime"])
    df_bnf_intraday = df_bnf_intraday.set_index('DateTime')
    # print(df_bnf_intraday)
    strategy_obj = StraddleStrategy(amount=1000000, params=param_dict)
    strategy_obj.add_dataframe(df_bnf_intraday, 'BNF_SPOT_INTRADAY', is_primary=True)
    strategy_obj.add_dataframe(df_options, 'BNF_OPTIONS_INTRADAY')
    if index == 1:
        continue
    strategy_obj.execute()

    transactions = pd.DataFrame(data=strategy_obj.transactions)
    print(transactions)

    transactions.to_csv(f'transactions_with_sideways_condition_{index}.csv')

    transactions = pd.DataFrame(data=strategy_obj.daily_log)
    print(transactions)
    transactions.to_csv(f'daily_log_{index}.csv')

