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


def test_on_swap_time(daocross, base_token):
    # Add some buy orders.
    # Buy 5 tokens for 100000000000 WEI.
    daocross.buyOrder(base_token.address, 5 * 10**18, 100000000000, accounts[1], {"from": accounts[1], "value": 100000000000*5})
    # Buy 3 tokens for 100000000000 WEI.
    daocross.buyOrder(base_token.address, 3 * 10**18, 100000000000, accounts[2], {"from": accounts[2], "value": 100000000000*3})
    # Add some sell orders.
    base_token.transfer(accounts[3], 4 * 10**18, {"from": accounts[0]})
    base_token.transfer(accounts[4], 3* 10**18, {"from": accounts[0]})
    #
    base_token.approve(daocross, 4 * 10**18, {"from": accounts[3]})
    base_token.approve(daocross, 3 * 10**18, {"from": accounts[4]})
    #
    daocross.sellOrder(base_token.address, 4 * 10**18, 100000000000, accounts[3], {"from": accounts[3]})
    daocross.sellOrder(base_token.address, 3 * 10**18, 100000000000, accounts[4], {"from": accounts[4]})

    # Execute the swap
    transfers = daocross.onSwapTime({"from": accounts[0]})

    # Validate the transfers
    assert len(transfers) > 0

    for transfer in transfers:
        assert transfer.token == base_token.address or transfer.token == "0x0000000000000000000000000000000000000000"
        assert transfer.amount > 0
        assert transfer.from_ in [accounts[1], accounts[2], accounts[3], accounts[4]]
        assert transfer.to in [accounts[1], accounts[2], accounts[3], accounts[4]]
