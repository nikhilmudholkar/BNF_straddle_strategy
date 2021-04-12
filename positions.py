class Transaction:
    datetime = None
    price = 0
    quantity = 0

    def get_value(self):
        return self.price * self.quantity

class Position:
    quantity = 0
    average_price = 0

    blocked_margin = 0

    def __init__(self, code=None, strategy=None):
        self.code = code
        self.strategy = strategy
        # print('3>>>', id(strategy))

    def transact(self, price=None, quantity=None, info=None):
        old_quantity = self.quantity
        # print(old_quantity)
        final_quantity = self.quantity + quantity
        df = self.strategy.dataframes.get(self.code)
        # print(df)
        # print("#######")
        # print(self.current_loc)
        current_row = self.strategy.get_current_row_from_dataframe(df)
        # print(current_row)
        if price is None:
            # print(price)
            price = current_row['Close']

        # transaction = Transaction()
        # transaction.datetime = self.strategy.current_loc
        # transaction.price = price
        # self.strategy.transactions.append(transaction)

        transaction = dict()
        transaction['datetime'] = self.strategy.current_loc
        transaction['code'] = self.code
        transaction['price'] = price
        transaction['quantity'] = quantity
        transaction['info'] = info
        transaction['value'] = price * quantity * -1
        # transaction['pnl'] = pnl
        # transaction['brokerage'] = 0.01 * transaction['value']

        current_position = None
        if self.quantity != 0:
            current_position = 'LONG' if self.quantity > 0 else 'SHORT'

        position_increase = False

        if current_position == "LONG":
            if quantity > 0:
                position_increase = True
            else:
                position_increase = False
        elif current_position == "SHORT":
            if quantity > 0:
                position_increase = False
            else:
                position_increase = True
        else:
            position_increase = True

        if position_increase:
            self.increase_position(transaction=transaction)
        else:
            self.decrease_position(transaction=transaction)

        self.quantity = final_quantity
        self.strategy.transactions.append(transaction)
        # print(self.strategy.transactions)

    def increase_position(self, transaction=None):
        value = transaction['value']
        # blocked_value = abs(value)
        blocked_value = value

        old_value = self.quantity * self.average_price
        total_value = value + old_value
        average_price = total_value / (self.quantity + transaction['quantity'])
        if self.strategy.amount < blocked_value:
            raise Exception('Cannot place orders')
        self.average_price = average_price
        self.blocked_margin = blocked_value
        # self.strategy.amount -= blocked_value
        self.strategy.amount += blocked_value

    def decrease_position(self, transaction=None):
        value = transaction['value']
        # blocked_value = abs(value)
        blocked_value = value
        old_value = self.quantity * self.average_price
        total_value = value + old_value
        final_quantity = (self.quantity + transaction['quantity'])
        if final_quantity == 0:
            average_price = 0
        else:
            average_price = total_value / final_quantity
        self.average_price = average_price
        self.blocked_margin = 0
        self.strategy.amount += blocked_value

    def get_unrealized(self, code):
        original_value = self.quantity * self.average_price
        df = self.strategy.dataframes.get(code)
        # print(df)
        current_row = self.strategy.get_current_row_from_dataframe(df)
        price = current_row['Close']
        current_value = self.quantity * price
        # return -(current_value - original_value)
        return original_value + current_value

    def __lt__(self, value):
        return self.quantity < value

    def __le__(self, value):
        return self.quantity <= value

    def __eq__(self, value):
        return self.quantity == value

    def __ne__(self, value):
        return self.quantity != value

    def __gt__(self, value):
        return self.quantity > value

    def __ge__(self, value):
        return self.quantity >= value


class DerivativePosition(Position):

    def increase_position(self, transaction=None):
        # quantity = 1
        # real_quantity = quantity * lot_szie = 20
        # 5000 -> 1
        # 10000 -> -1
        pass

    def decrease_position(self, transaction=None):
        pass
