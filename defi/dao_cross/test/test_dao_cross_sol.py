import pytest
from brownie import DaoCross, accounts, Contract, MockV3Aggregator, MockERC20


# Deploy the MockV3Aggregator
@pytest.fixture(scope="module")
def price_oracle():
    decimals = 18
    initial_price = 100000000000  # This value should be scaled to the appropriate number of decimals
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


def test_match_orders(daocross, base_token):
    # Submit buy orders.
    # Buy 5 tokens for 100000000000 WEI.
    daocross.buyOrder(base_token.address, 5 * 10**18, 100000000000, accounts[1], {"from": accounts[1], "value": 100000000000*5})
    # Buy 3 tokens for 100000000000 WEI.
    daocross.buyOrder(base_token.address, 3 * 10**18, 100000000000, accounts[2], {"from": accounts[2], "value": 100000000000*3})
    # Add some sell orders.
    # First transfer the token to the users that will participate sell order.
    base_token.transfer(accounts[3], 4 * 10**18, {"from": accounts[0]})
    base_token.transfer(accounts[4], 3* 10**18, {"from": accounts[0]})
    # Approve the tranfers for dao cross.
    base_token.approve(daocross, 4 * 10**18, {"from": accounts[3]})
    base_token.approve(daocross, 3 * 10**18, {"from": accounts[4]})
    # Submit sell orders.
    daocross.sellOrder(base_token.address, 4 * 10**18, 100000000000, accounts[3], {"from": accounts[3]})
    daocross.sellOrder(base_token.address, 3 * 10**18, 100000000000, accounts[4], {"from": accounts[4]})

    # Execute the swap
    tx = daocross.matchOrders(100000000000, {"from": accounts[0]})
    transfers = tx.return_value

    # Validate the transfers
    assert len(transfers) > 0

    for transfer in transfers:
        assert transfer[0] == base_token.address or transfer[0] == "0x0000000000000000000000000000000000000000"
        assert transfer[1] > 0
        assert transfer[2] in [accounts[1], accounts[2], accounts[3], accounts[4]]
        assert transfer[3] in [accounts[1], accounts[2], accounts[3], accounts[4]]
