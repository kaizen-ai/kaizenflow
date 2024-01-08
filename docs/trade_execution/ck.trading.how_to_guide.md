<!--ts-->
   * [Trading procedures](#trading-procedures)
      * [Binance](#binance)
         * [Logging into the account](#logging-into-the-account)
            * [API access](#api-access)
            * [UI](#ui)
         * [Re-upping the account balance](#re-upping-the-account-balance)
            * [UI](#ui-1)



<!--te-->
# Trading procedures

## Binance

### Logging into the account

#### API access

- The API keys for different accounts are stored in AWS Secrets.
- All Binance-related secrets have the same name stucture, e.g.
  `binance.preprod.trading.1`.
- Secrets can be accessed via `helpers/hsecrets.py` library.
- To see the example of code required to log into the exchange:
  - See
    [`AbstractCcxtBroker._log_into_exchange()`](https://github.com/cryptokaizen/cmamp/blob/master/oms/broker/ccxt/abstract_ccxt_broker.py#L735)
    method
  - Consult the [CCXT documentation](https://docs.ccxt.com/#/?id=instantiation)

#### UI

- The UI login requires an ID-verified account with 2-factor authentication,
  usually accomplished by Google Authenticator.
- 2 trading accounts are available for the access via UI. Both of these are
  verified using personal government-issued IDs.
- In order to access these accounts, one needs to address the person/people that
  have access to it.

### Re-upping the account balance

#### UI

- Send the required sum in USDT to the cryptowallet connected to one of the
  trading accounts.
  - Ask @jsmerix (Juraj) or @mongolianjesus (Danya) for the crypto wallet
    addresses.
- Log into Binance the account dashboard.
- Check for **Markets** -> **Holdings** section in the middle of the screen. The
  total USDT balance should be increased by the amount transferred.

Initially, the money is deposited in the Spot account, and requires to be
transferred to the Futures account manually.

- Click on the USDT balance.
- In the pop-up window., click **Transfer**.
- Select **From Fiat and Spot** and **To USDS-M Futures**.
- Enter the desired amount to be transferred to the USDS-M Futures account.
- Click **Confirm**.
- To verify the transfer from the UI, go to **Derivatives** -> **USDS-M
  Futures** in the top menu. The available balance is located at the bottom
  right of the trading screen.
