import pytest
from brownie import DaoCross, accounts, Contract, MockV3Aggregator, MockERC20, reverts


# Deploy the MockV3Aggregator
@pytest.fixture(scope="module")
def price_oracle():
    decimals = 18
    initial_price = 100000000000 # This value should be scaled to the appropriate number of decimals
    mock_v3_aggregator = accounts[0].deploy(MockV3Aggregator, decimals, initial_price)
    yield mock_v3_aggregator


@pytest.fixture
def base_token(accounts):
    return accounts[0].deploy(MockERC20, "BaseToken", "BT")


@pytest.fixture
def daocross(price_oracle, base_token):
    return accounts[0].deploy(
        DaoCross,
        base_token.address,
        300,
        5,
        1,
        price_oracle.address,
    )


def test_buyOrder_baseToken_address(daocross, base_token):
    """
    Checks the `require(_baseToken == address(baseToken));` condition.
    """
    baseToken = "0x1234567890123456789012345678901234567890"
    quantity =  5 * 10**18
    limitPrice = 100000000000
    depositAddress = accounts[1]

    with reverts():
         daocross.buyOrder(baseToken, quantity, limitPrice, depositAddress, {"from": accounts[1], "value": 100000000000*5, "gas_price": "60 gwei"})
    
def test_buyOrder_passedEther(daocross, base_token):
    """
    Checks the `require(msg.value >= fullPrice)` condition.
    """
    quantity =  5 * 10**18
    limitPrice = 100000000000
    depositAddress = accounts[1]

    with reverts():
        daocross.buyOrder(base_token.address, quantity, limitPrice, depositAddress, {"from": accounts[1], "value": 100000000000*4, "gas_price": "60 gwei"})

def test_buyOrder_orderArray(daocross, base_token):
    """
    Checks the length of order array after creating the orders.
    Buy 5 tokens for 100000000000 WEI.
    Buy 2.5 tokens for 100000000000 WEI.
    """
    daocross.buyOrder(base_token.address, 5 * 10**18, 100000000000, accounts[1], {"from": accounts[1], "value": 100000000000*5, "gas_price": "60 gwei"})
    daocross.buyOrder(base_token.address, 2.5 * 10**18, 100000000000, accounts[2], {"from": accounts[2], "value": 100000000000*2.5, "gas_price": "60 gwei"})
    assert daocross.orders(0)[0] == 0
    assert daocross.orders(1)[0] == 1
    
def test_buyOrder_eventEmitted(daocross, base_token):
    """
    Checks wheather the `newBuyOrder` event was emitted or not.
    """
    quantity =  5 * 10**18
    limitPrice = 100000000000
    depositAddress = accounts[1]
    ethAddress = "0x0000000000000000000000000000000000000000"

    tx = daocross.buyOrder(base_token.address, quantity, limitPrice, depositAddress, {"from": accounts[1], "value": 100000000000*5, "gas_price": "60 gwei"})
    assert len(tx.events) == 1
    assert tx.events["newBuyOrder"].values() == [base_token.address, ethAddress, quantity, limitPrice, depositAddress]

def test_sellOrder_baseToken_address(daocross, base_token):
    """
    Checks the `require(_baseToken == address(baseToken));` condition.
    """
    baseToken = "0x1234567890123456789012345678901234567890"
    quantity =  5 * 10**18
    limitPrice = 100000000000
    depositAddress = accounts[1]

    with reverts():
         daocross.sellOrder(baseToken, quantity, limitPrice, depositAddress, {"from": accounts[1], "gas_price": "60 gwei"})

def test_sellOrder_allowance(daocross, base_token):
    """
    Checks `require(allowance >= _quantity);` conditon.
    """
    base_token.transfer(accounts[5], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[5], "gas_price": "60 gwei"})
    
    quantity =  6 * 10**18
    limitPrice = 100000000000
    depositAddress = accounts[5]
    
    with reverts():
        daocross.sellOrder(base_token.address, quantity, limitPrice, depositAddress, {"from": accounts[5], "gas_price": "60 gwei"})

def test_sellOrder_checkBalance(daocross, base_token):
    """
    Checks the balance of this DaoCross contract after the sell order is placed.
    """
    base_token.transfer(accounts[5], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[5], "gas_price": "60 gwei"})
    
    quantity =  5 * 10**18
    limitPrice = 100000000000
    depositAddress = accounts[5]

    daocross.sellOrder(base_token.address, quantity, limitPrice, depositAddress, {"from": accounts[5], "gas_price": "60 gwei"})
    
    assert base_token.balanceOf(daocross.address) == quantity

def test_sellOrder_orderArray(daocross, base_token):
    """
    Checks the length of order array after creating the orders.
    Sell 5 tokens.
    Sell 2.5 tokens.
    """
    base_token.transfer(accounts[5], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[5], "gas_price": "60 gwei"})
    base_token.transfer(accounts[6], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[6], "gas_price": "60 gwei"})
   
    daocross.sellOrder(base_token.address, 5 * 10**18, 100000000000, accounts[5], {"from": accounts[5], "gas_price": "60 gwei"})
    daocross.sellOrder(base_token.address, 2.5 * 10**18, 100000000000, accounts[6], {"from": accounts[6], "gas_price": "60 gwei"})
    assert daocross.orders(0)[0] == 0
    assert daocross.orders(1)[0] == 1
    
def test_sellOrder_eventEmitted(daocross, base_token):
    """
    Checks wheather the `newBuyOrder` event was emitted or not.
    """
    base_token.transfer(accounts[5], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[5], "gas_price": "60 gwei"})

    quantity =  5 * 10**18
    limitPrice = 100000000000
    depositAddress = accounts[5]
    ethAddress = "0x0000000000000000000000000000000000000000"

    tx =  daocross.sellOrder(base_token.address, 5 * 10**18, 100000000000, accounts[5], {"from": accounts[5], "gas_price": "60 gwei"})
    assert tx.events["newSellOrder"].values() == [base_token.address, ethAddress, quantity, limitPrice, depositAddress]


def test_getChainlinkFeedPrice(daocross):
    """
    Checks the functionality of `getChainlinkFeedPrice()` fucntion.
    """
    assert daocross.getChainlinkFeedPrice() == 100000000000

def test_onSwapTime_similarQuantity(daocross, base_token):
    """
    Checks the functionality of token transfers with single order of same quantity.
    Also checks the functionality of eraseOrders() function.
    """
    
    base_token.transfer(accounts[5], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[5], "gas_price": "60 gwei"})

    daocross.buyOrder(base_token.address, 5 * 10**18, 100000000000, accounts[1], {"from": accounts[1], "value": 100000000000*5, "gas_price": "60 gwei"})
    daocross.sellOrder(base_token.address, 5 * 10**18, 100000000000, accounts[5], {"from": accounts[5], "gas_price": "60 gwei"})
    daocross.onSwapTime({"from": accounts[0], "gas_price": "60 gwei"})

    assert base_token.balanceOf(accounts[1]) == 5 * 10**18
    """
    Since orders array is deleted.
    """
    with reverts():
        daocross.orders(0)[0] == 0

def test_onSwapTime_sellOrder_with_greater_than_limitPrice(daocross, base_token):
    """
    Checks the functionality of sell order with Greater than limit Price.
    """
    base_token.transfer(accounts[5], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[5], "gas_price": "60 gwei"})

    daocross.buyOrder(base_token.address, 5 * 10**18, 100000000000, accounts[1], {"from": accounts[1], "value": 100000000000*5, "gas_price": "60 gwei"})
    daocross.sellOrder(base_token.address, 5 * 10**18, 200000000000, accounts[5], {"from": accounts[5], "gas_price": "60 gwei"})
    daocross.onSwapTime({"from": accounts[0], "gas_price": "60 gwei"})

    assert base_token.balanceOf(accounts[5]) == 5 * 10**18

def test_onSwapTime_buyOrder_with_less_than_limitPrice(daocross, base_token):
    """
    Checks the functionality of buy order with less than limit Price.
    """
    initialBalance = accounts[6].balance()

    base_token.transfer(accounts[5], 5* 10**18, {"from": accounts[0], "gas_price": "60 gwei"})
    base_token.approve(daocross, 5 * 10**18, {"from": accounts[5], "gas_price": "60 gwei"})

    daocross.buyOrder(base_token.address, 5 * 10**18, 100000000, accounts[6], {"from": accounts[6], "value": 100000000*5, "gas_price": "60 gwei"})
    daocross.sellOrder(base_token.address, 5 * 10**18, 200000000000, accounts[5], {"from": accounts[5], "gas_price": "60 gwei"})
    daocross.onSwapTime({"from": accounts[0], "gas_price": "60 gwei"})

    assert base_token.balanceOf(accounts[5]) == 5 * 10**18

