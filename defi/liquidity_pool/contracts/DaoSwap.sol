// SPDX-License-Identifier: MIT
pragma solidity ^0.8.4;

import "../node_modules/@openzeppelin/contracts/access/Ownable.sol";
import "../node_modules/@openzeppelin/contracts/token/ERC20/IERC20.sol";


/// @title Swap contract that allows trading large blocks of coins peer-to-peer.
contract DaoSwap is Ownable {
    string public contractName;
    address public priceOracle;
    // 1 -> TWAP, 2 -> VWAP, 3 -> close.
    uint8 public priceMode;
    // 1 -> proportional, 
    uint8 public swapMode;
    uint16 public swapPeriodInSecs;
    uint16 public swapRandomizationInSecs;
    uint256 public feesAsPct;

    // TODO(gp): @toma consider representing the orders as (token1, quantity, token2). In this way
    // We can represent buying 3 wBTC and paying in ETH as (wBTC, 3, ETH), selling 3 wBTC

    // @notice Represent an order to buy / sell token placed by a user.
    // E.g., a user wants to swap 3 wBTC for ETH, paying at most 1 ETH per wBTC.
    // TODO(gp): @toma we can represent any buy / sell as a swap from to. This might simplify the code.
    struct Order {
        // `true` -> buy, `false` -> sell
        bool direction;
        // `true` if the user was not satisfied with a suggested price.
        bool declined;
        // The address of the token to buy. 0x0 if direction == "sell".
        address tokenToBuy;
        // The address of token to sell. 0x0 if direction == "buy".
        address tokenToSell;
        // Address to send tokens to after the swap.
        address depositAddress;
        address sender;
        // The timestamp of the order creation.
        uint256 timestamp;
        // Amount of tokens to buy / sell.
        uint256 amount;
        // Max price for buy / min price for sell.
        uint256 limitPrice;
    }

    Order[] public orders;

    // Map relevant addresses to True to easily check if an address interacted with this contract.
    mapping(address => bool) public Tokens;

    // Events used to interact with frontend.
    //event newBuyOrder(uint256 timestamp, address token, uint256 amount, uint256 limitPrice, address depositAddress);
    //event newSellOrder(uint256 timestamp, address token, uint256 amount, uint256 limitPrice, address depositAddress);

    /// @param _contractName: the name of the contract, e.g. `DaoSwap ETH`
    /// @param _tokens: the list of tokens to swap (e.g., wBTC, ADA)
    /// @param _swapPeriodInSecs: how often to perform the swap (e.g., 300 to perform a swap every 5 minutes)
    /// @param _swapRandomizationInSecs: how many random seconds to add or subtract to the `swapPeriodsInSecs`.
    ///    E.g., _swapPeriodInSecs=300 and _swapRandomizationInSecs=5, it means that each swap happens every 5 minutes
    ///    with a different number of seconds in [0, 5] before or after the 5 minute mark.
    /// @param _feesAsPct: fees to charge in terms of value exchanged
    /// @param _priceOracle: contract providing the price for the swap
    /// @param _priceMode: how to compute the price for the swap using the oracle price
    ///    E.g.,
    ///    - 1 for `twap` (use the TWAP price since the last swap)
    ///    - 2 for `vwap` (same as TWAP but using a VWAP price)
    ///    - 3 for `close` (use the price at the time of the swap)
    /// @param _swapMode: how to perform the swap between orders
    ///    - 1 for `proportional`: the total buy / sell amount is divided equally among all the orders, regardless of their timestamp
    ///    E.g., if the orders are the following and are all for swapping wBTC and ETH.
    ///         O1 = (buy, 5, wBTC, ETH)
    ///         O2 = (buy, 6, wBTC, ETH)
    ///         O3 = (sell, 7, wBTC, ETH)
    ///         O4 = (sell, 1, wBTC, ETH)
    ///       - The totalBuyAmount to buy is 11 and totalSellAmount to sell is 8
    ///    so the first order gets (5 * 8 / 11) units, the second order gets (6 * 8 / 11), the third order gets 7 units,
    ///    the fourth order gets 1 units
    ///    - 2 for `fifo`: orders are matched based on their timestamp, e.g., (buy, 5), (buy, 6), (sell, 7), (sell, 1)
    ///    the first order is matched with the first 5 units of the third order, the second order is matched with 
    ///    the 2 units from the third and forth order, with an imbalance of 3 which is not crossed
    ///    - 3 for `auction`
    constructor(string memory _contractName, 
           address[] memory _tokens,
           uint8 _swapPeriodInSecs, 
           uint8 _swapRandomizationInSecs, 
           uint8 _feesAsPct,
           uint8 _priceMode, 
           address _priceOracle, 
           uint8 _swapMode) {
        contractName = _contractName;
        swapPeriodInSecs = _swapPeriodInSecs;
        swapRandomizationInSecs = _swapRandomizationInSecs;
        feesAsPct = _feesAsPct;
        priceOracle = _priceOracle;
        priceMode = _priceMode;
        swapMode = _swapMode;
        // Construct the mapping for the tokens availiable for swap.
        for(uint i = 0; i < _tokens.length; i++) {
            Tokens[_tokens[i]] = true;
        }
    }

    /// @notice Create an order to buy the tokens for ETH.
    /// @param _tokenToBuy: the address of token 
    /// @param _amount: amount of token to buy
    /// @param _limitPrice: the max price for one token in WEI
    /// @param _depositAddress: the address to send the tokens after the order is completed
    function buyOrder(address _tokenToBuy, 
                uint256 _amount, 
                uint256 _limitPrice,
                address _depositAddress) external payable {
        require(msg.value > 0, "Send ETH to get tokens");
        require(Tokens[_tokenToBuy] == true, "The token is not availiable for swap");
        Order memory order = Order(
            true,
            false,
            _tokenToBuy,
            address(0),
            _depositAddress,
            msg.sender,
            block.timestamp,
            _amount,
            _limitPrice
        );
        // TODO(Toma): refactor this.
        // No token type separation for now, all orders go to one array.
        orders.push(order);
    }

    /// Ok but how will we stop user from sending orders while we executing our current swap?
    // It probaby will take some seconds.

    /// @notice Create an order to sell the tokens for ETH.
    /// @param _tokenToSell: the address of token 
    /// @param _amount: amount of token to sell
    /// @param _limitPrice: the upper price for one token in WEI
    /// @param _depositAddress: the address to send the ETH after the order is completed
    function sellOrder(address _tokenToSell, 
                uint256 _amount, 
                uint256 _limitPrice,
                address _depositAddress) external payable {
        require(Tokens[_tokenToSell] == true, "The token is not availiable for swap");
        // NOTE: User needs to approve the smart contract to spend their tokens.
        require(IERC20(_tokenToSell).balanceOf(msg.sender) >= _amount, "Not enough tokens on user balance");
        // Receive tokens.
        // do we need to process the output of transferFrom?
        // It's better to recieve tokens straightaway as users can move tokens to different address
        // during the swap period and break calculated swap proportion.
        IERC20(_tokenToSell).transferFrom(msg.sender, address(this), _amount);
        Order memory order = Order(
            false,
            false,
            address(0),
            _tokenToSell,
            _depositAddress,
            msg.sender,
            block.timestamp,
            _amount,
            _limitPrice
        );
        // No token type separation for now, all orders go to one array.
        orders.push(order);
    }

    /// @notice Execute the swap.
    /// @param _token the token to swap
    function onSwapTime(address _token) public onlyOwner {
        uint256 price;
        if (priceMode == 1) {
            price = getTwapPrice(_token);
        } else if (priceMode == 2)  {
            price = getVwapPrice(_token);
        } else if (priceMode == 3) {
            price = getOraclePrice(_token);
        }
        ///
        uint256 totBuyAmount;
        uint256 totSellAmount;
        // Iterate over the orders to count total buy and total sell amount.
        for (uint i=0; i<orders.length; i++) {
            if (orders[i].direction == true) {
                // Process buy order.
                if (orders[i].limitPrice <= price) {
                    // Limit price is the max price the user can pay for one token,
                    // so it should be less or equal to the actual price.
                    totBuyAmount += orders[i].amount;
                } else {
                    Order storage declineOrder = orders[i];
                    declineOrder.declined = true;
                    // Return ETH to sender address.
                    (bool sent, bytes memory data) = declineOrder.sender.call{value: declineOrder.amount}("");
                    require(sent, "Failed to send Ether");
                }
            } else {
                // Process sell order.
                if (orders[i].limitPrice >= price) {
                    // Limit price is the min price the user want to recieve for one token,
                    // so it should be more or equal to the actual price.
                    totSellAmount += orders[i].amount;
                } else {
                    Order storage declineOrder = orders[i];
                    declineOrder.declined = true;
                    // Return tokens to sender address.
                    IERC20(declineOrder.tokenToBuy).transfer(declineOrder.sender, declineOrder.amount);
                }
            }
        }
        //
        if (swapMode == 1) {
            /// The decimals to multilpy numerator on, the bigger the number the bigger the precision.
            uint8 decimals = 3;
            // We use only `proportional` mode for now.
            (uint256 pctSellAmount, uint256 pctBuyAmount) = countProportional(totBuyAmount, totSellAmount, decimals);
            // And here againg go through each order and count the amount of tokens/wei for the users 
            // with countBuyOrder or countSellOrder
            for (uint i=0; i<orders.length; i++) {
                if (orders[i].declined == false) {
                    if (orders[i].direction == true) {
                        // Process buy orders.
                        // Get amount of tokens to send to user (tokens are stored with 18 decimals)
                        uint256 tokenAmount = countBuyOrder(pctBuyAmount, decimals, orders[i].amount);
                        // Send tokens to user.
                        IERC20(orders[i].tokenToBuy).transfer(orders[i].depositAddress, tokenAmount);
                    } else {
                        // Process sell orders.
                        uint256 weiAmount = countSellOrder(pctSellAmount, decimals, orders[i].amount, price);
                        // Send WEI to user.
                        (bool sent, bytes memory data) = orders[i].depositAddress.call{value: weiAmount}("");
                        require(sent, "Failed to send Ether");
                    }
                }
            }
        }
    }

    /// @notice Calculate the proportion of buy and sell amount.
    /// E.g. users want to (buy, 4), (buy, 1) and (sell, 3), (sell, 5) -> totBuy = 5, totSell = 8.
    /// Proportion 5/8 = 0.625. 
    /// (buy, 4) gets 4*1 tokens, (buy, 1) gets 1*1.
    /// and since we have only 5 tokens in this swap,
    /// (sell, 3) gets ETH for 3*0.625 = 1.875 tokens and (sell, 5) gets ETH for 5*0.625 = 3.125
    /// 1.875 + 3.125 = 5 = exactly the amount of tokens user wanted to buy.
    /// BUT since we had 8 tokens, sold 5 tokens, 3 left on the contract. What we are going to do with them?
    ///
    /// @param _totBuyAmount: the amount of tokens to buy (number with 18 decimals)
    /// @param _totSellAmount: the amount of tokens to sell (number with 18 decimals)
    /// @param _decimals: the decimals to multilpy numerator on, the bigger the number the bigger the precision.
    function countProportional(uint256 _totBuyAmount, uint256 _totSellAmount, uint8 _decimals) public pure returns (uint256, uint256) {
        uint256 pctBuyAmount;
        uint256 pctSellAmount;
        // We can't operate with floats in solidity, so to get the proportion,
        // the numerator needs to be multiplied by 10, 100 or 1000 etc... to be bigger than denominator.
        if (_totBuyAmount < _totSellAmount) {
            pctSellAmount = (_totBuyAmount * 10**_decimals) / _totSellAmount;
            pctBuyAmount = 1 * (10**_decimals);
        } else if (_totBuyAmount > _totSellAmount) {
            pctBuyAmount = (_totSellAmount * 10**_decimals) / _totBuyAmount;
            pctSellAmount = 1 * (10**_decimals);
        } else {
            pctSellAmount = 1 * (10**_decimals);
            pctBuyAmount = 1 * (10**_decimals);
        }
        return (pctSellAmount, pctBuyAmount);
    }

    /// @notice Count the amount of tokens user can get according to proportion.
    /// @param _pctBuyAmount: the proportion of desired tokens user can get (multiplied by 10**_decimals),
    /// e.g. if proportion 0.625 and _decimals 3, _pctBuyAmount = 625
    /// @param _decimals: the used number of decimals
    /// @param _orderAmount: the amount of tokens to buy (with 18 decimals), e.g. 1550000000000000000 for 1.55 token
    function countBuyOrder(uint256 _pctBuyAmount, uint8 _decimals, uint256 _orderAmount) public pure returns (uint256) {
        // Count the amount of tokens to send to the user. We store tokens amount as uint with 18 decimals.
        // E.g. user buys 17 tokens: ( 17*10^18 * 726 ) / 10^3 == 17*10^18 * 0.726
        uint256 result = (_orderAmount * _pctBuyAmount) / 10**_decimals;
        return result;
    }

    /// @notice Count the amount of WEI user will get.
    /// @param _pctSellAmount: the proportion of desired WEI user can get (multiplied by 10**_decimals)
    /// @param _decimals: the used number of decimals
    /// @param _orderAmount: the amount of tokens to sell (with 18 decimals), e.g. 1550000000000000000 for 1.55 token
    /// @param _tokenPrice: the price for 1 token in WEI, e.g. 53330000000000000 WEI ( = 0.05333 ETH)
    function countSellOrder(uint256 _pctSellAmount, uint8 _decimals, uint256 _orderAmount, uint256 _tokenPrice) public pure returns (uint256) {
        // E.g. user wants to sell 17 tokens, but buy orders cover only 72,6%.
        // ( 17*10^18 * 726 ) / 10^3 == 17*10^18 * 0.726
        uint256 tokensAmount = (_orderAmount * _pctSellAmount) / 10**_decimals;
        // Count amount of WEI user will get for tokens.
        uint256 result = (tokensAmount * _tokenPrice) / 10**18;
        return result;
    }

    /// @notice Get token price from the Oracle.
    /// @param _token address of token to ask price for
    function getOraclePrice(address _token) internal pure returns (uint256) {
        // harcoded for now, one token costs 53330000000000000 WEI which is 0.05333 ETH
        return 53330000000000000;
    }


    /// @notice Get token price with TWAP algorithm.
    /// @param _token address of token to count price for
    function getTwapPrice(address _token) internal pure returns (uint256)  {
        //TWAP = (TP1+ TP2… + TPn) / n
        // harcoded for now, one token costs 53330000000000000 WEI which is 0.05333 ETH
        return 53330000000000000;
    }

    /// @notice Get token price with VWAP algorithm.
    /// @param _token address of token to count price for
    function getVwapPrice(address _token) internal pure returns (uint256)  {
        //VWAP = (V1 x P1 + V2 x P2… + Vn x Pn) / TotalVolume
        // harcoded for now, one token costs 53330000000000000 WEI which is 0.05333 ETH
        return 53330000000000000;
    }


}

