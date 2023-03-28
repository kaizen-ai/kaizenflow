# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import numpy as np


# %%
class Order:
    
    def __init__(self, action, quantity, coin, limit_price=np.nan):
        self.action = action
        self.coin = coin
        self.quantity = quantity
        self.limit_price = limit_price
        
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        #ret = "action=%s quantity=%s coin=%s limit_price=%s" % (self.action, self.quantity, self.coin, self.limit_price)
        ret = "%s %s %s with limit_price=%s" % (self.action, self.quantity, self.coin, self.limit_price)
        return ret
    
        
class Fill:
    
    def __init__(self, order, quantity):
        assert 0 <= quantity
        assert quantity <= order.quantity
        
        
def get_random_order(seed=None):
    if seed is not None:
        np.random.seed(seed)
    action = "buy" if np.random.random() < 0.5 else "sell"
    quantity = np.random.randint(1, 10)
    coin = "ETH" if np.random.random() < 0.5 else "wBTC"
    limit_price = np.nan
    order = Order(action, quantity, coin, limit_price)
    return order


def total_supply(orders):
    orders_tmp = {}
    for order in orders:
        tag = (order.action, order.coin)
        if tag not in orders_tmp:
            orders_tmp[tag] = 0
        orders_tmp[tag] += order.quantity
    #
    orders_out = []
    for order, val in orders_tmp.items():
        orders_out.append(Order(order[0], order[1], val))
    return orders_out

    
np.random.seed(41)

orders = []
for i in range(10):
    order = get_random_order()
    orders.append(order)

print("\n".join(map(str, orders)))

print("# Sum")
orders2 = total_supply(orders)
print("\n".join(map(str, orders2)))

# %%
o_1 = Order("buy", 1.0, "")
