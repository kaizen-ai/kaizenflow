from brownie import MockERC20, TokenTimelock, accounts, reverts, chain
import pytest



@pytest.fixture(scope="module")
def mock_token(MockERC20, accounts):
    return MockERC20.deploy("Mock Token", "MCK", 100,1000, {'from': accounts[0], "gas_price": "60 gwei"})


@pytest.fixture(scope="module")
def time_lock(mock_token, TokenTimelock, accounts):
    return TokenTimelock.deploy(mock_token.address, {"from": accounts[0], "gas_price": "60 gwei"})


@pytest.fixture(autouse=True)
def shared_setup(fn_isolation):
    pass


def test_addParticipant_requireAmount(time_lock, accounts): 
    participant = accounts[1]
    amount = 10000
    release_TimeStamp =  1677064547
    time_lock.addParticipant(participant, amount, release_TimeStamp, {"from": accounts[0], "gas_price": "60 gwei"})
    with reverts():
        time_lock.addParticipant(participant, amount, release_TimeStamp, {"from": accounts[0], "gas_price": "60 gwei"})
    

def test_addParticipant(time_lock, accounts): 
    participant = accounts[1]
    amount = 10000
    release_TimeStamp =  1677064547
    time_lock.addParticipant(participant, amount, release_TimeStamp, {"from": accounts[0], "gas_price": "60 gwei"})
    assert time_lock.getTokensAmountLocked(participant, {"from": participant}) == amount

def test_addAmount_requireAmount(time_lock, accounts):
    with reverts():
        time_lock.addAmount(accounts[1], 1000, {"from": accounts[0], "gas_price": "60 gwei"})   

def test_addAmount(time_lock, accounts):
    participant = accounts[1]
    amount = 10000
    release_TimeStamp =  1677064547
    time_lock.addParticipant(participant, amount, release_TimeStamp, {"from": accounts[0], "gas_price": "60 gwei"})
    new_amount = 1000
    final_amount = amount+new_amount
    time_lock.addAmount(accounts[1], new_amount, {"from": accounts[0], "gas_price": "60 gwei"})
    assert time_lock.getTokensAmountLocked(participant, {"from": participant}) == final_amount

def test_getReleaseTimestamp(time_lock, accounts):
    with reverts():
        time_lock.getReleaseTimestamp(accounts[1], {"from": accounts[1]})

def test_releaseTokens_releaseTimestamp(time_lock, accounts, mock_token):
    participant = accounts[1]
    amount = 3* 10**18
    release_TimeStamp =  1677064547
    time_lock.addParticipant(participant, amount, release_TimeStamp, {"from": accounts[0], "gas_price": "60 gwei"})
    
    with reverts():
        time_lock.releaseTokens({"from": accounts[1], "gas_price": "60 gwei"})


""" def test_releaseTokens(time_lock, accounts, mock_token):
    participant = accounts[1]
    amount = 3* 10**18
    release_TimeStamp =  1677064547
    time_lock.addParticipant(participant, amount, release_TimeStamp, {"from": accounts[0], "gas_price": "60 gwei"})
    chain.sleep(release_TimeStamp - chain.time() + 1)
    #mock_token.approve(accounts[1], amount, {"from": accounts[0], "gas_price": "60 gwei"})
    time_lock.releaseTokens({"from": accounts[1], "gas_price": "60 gwei"}) """
    
