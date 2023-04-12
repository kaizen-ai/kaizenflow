// SPDX-License-Identifier: MIT
pragma solidity ^0.8.4;

import "./PriceOracle.sol";
import "../node_modules/@openzeppelin/contracts/access/Ownable.sol";
import "../node_modules/@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "../node_modules/@openzeppelin/contracts/security/PullPayment.sol";
import "./OrderMinHeap.sol";
import "./OrderMatching.sol";


/// @title Swap contract that allows trading large blocks of coins peer-to-peer.
contract DaoCross is Ownable, PullPayment {
    //using OrderMinHeap for OrderMinHeap.Order;
    //using OrderMinHeap for OrderMinHeap.Heap;

    // Declare the heaps as storage variables.
    OrderMinHeap.Heap private buyHeap;
    OrderMinHeap.Heap private sellHeap;

    // From https://docs.chain.link/data-feeds/price-feeds/addresses.
    PriceOracle public priceOracle;
    uint16 public swapPeriodInSecs;
    uint8 public swapRandomizationInSecs;
    uint256 public feesAsPct;
    // The ERC20 token to buy / sell relatively to ETH.
    IERC20 baseToken;

    OrderMinHeap.Order[] public orders;

    // Events used to interact with frontend.
    event newBuyOrder(address baseToken, address quoteToken, uint256 amount, uint256 limitPrice, address depositAddress);
    event newSellOrder(address baseToken, address quoteToken, uint256 amount, uint256 limitPrice, address depositAddress);

    /// @param _baseToken: a token to swap (e.g., wBTC, ADA)
    /// @param _swapPeriodInSecs: how often to perform the swap (e.g., 300 to perform a swap every 5 minutes)
    /// @param _swapRandomizationInSecs: how many random seconds to add or subtract to the `swapPeriodsInSecs`.
    ///    E.g., _swapPeriodInSecs=300 and _swapRandomizationInSecs=5, it means that each swap happens every 5 minutes
    ///    with a different number of seconds in [0, 5] before or after the 5 minute mark.
    /// @param _feesAsPct: fees to charge in terms of value exchanged
    /// @param _priceOracle: contract providing the price for the swap
    constructor(address _baseToken,
           uint16 _swapPeriodInSecs, 
           uint8 _swapRandomizationInSecs, 
           uint8 _feesAsPct,
           address _priceOracle) {
        swapPeriodInSecs = _swapPeriodInSecs;
        swapRandomizationInSecs = _swapRandomizationInSecs;
        feesAsPct = _feesAsPct;
        priceOracle = new PriceOracle(_priceOracle);
        baseToken = IERC20(_baseToken);
    }

    /// @notice Create an order to buy the tokens for ETH
    /// @param _baseToken: the base ERC20 token
    /// @param _quantity: amount of token to buy
    /// @param _limitPrice: the max price for one token in WEI
    /// @param _depositAddress: the address to send the tokens after the order is completed
    function buyOrder(address _baseToken,
                uint256 _quantity, 
                uint256 _limitPrice,
                address _depositAddress) external payable {
        require(_baseToken == address(baseToken));
        //require(msg.value > 0, "Send ETH to get tokens");
        OrderMinHeap.Order memory order = OrderMinHeap.Order(
            msg.sender,
            _baseToken,
            address(0x0), // define ETH as zero addr
            _quantity,
            _limitPrice,
            true,
            _depositAddress,
            block.timestamp

        );
        orders.push(order);
        emit newBuyOrder(_baseToken, address(0x0), _quantity, _limitPrice, _depositAddress);
    }

    /// @notice Create an order to sell the tokens for ETH.
    /// @param _baseToken: amount of token to sell
    /// @param _quantity: amount of token to sell
    /// @param _limitPrice: the upper price for one token in WEI
    /// @param _depositAddress: the address to send the ETH after the order is completed
    function sellOrder(address _baseToken,
                uint256 _quantity, 
                uint256 _limitPrice,
                address _depositAddress) external payable {
        require(_baseToken == address(baseToken));
        // NOTE: User needs to approve the smart contract to spend their tokens.

        //uint256 allowance = baseToken.allowance(msg.sender, address(this));
        //require(allowance >= _quantity, "Check the token allowance");

        // Receive tokens.
        // It's better to recieve tokens straightaway as users can move tokens to different address
        // during the swap period and break calculated swap proportion.

        // baseToken.transferFrom(msg.sender, address(this), _quantity);

        //
        OrderMinHeap.Order memory order = OrderMinHeap.Order(
            msg.sender,
            _baseToken,
            address(0x0), // define ETH as zero addr
            _quantity,
            _limitPrice,
            false,
            _depositAddress,
            block.timestamp

        );
        orders.push(order);
        emit newSellOrder(_baseToken, address(0x0), _quantity, _limitPrice, _depositAddress);
    }

    /// @notice Execute the swap.
    function onSwapTime() public onlyOwner returns (OrderMatching.Transfer[] memory) {
        uint256 clearingPrice = getChainlinkFeedPrice();
        // Initialize the heaps.
        uint32 size = uint32(orders.length);
        buyHeap = OrderMinHeap.createHeap(size);
        sellHeap = OrderMinHeap.createHeap(size);
        OrderMatching.Transfer[] memory transfers = OrderMatching.matchOrders(orders, buyHeap, sellHeap, clearingPrice);
        eraseOrders();
        // TODO(Toma): implement payments!
        return transfers;
    }

    /// @notice Implement pull-payment strategy for ERC20 tokens. 
    function withdrawTokens() public { 
        uint256 allowance = baseToken.allowance(address(this), msg.sender);
        require(allowance > 0, "No tokens are allowed to transfer.");
        baseToken.transferFrom(address(this), msg.sender, allowance);        
    }

    /// @notice Get token price from the Chainlink price feed.
    function getChainlinkFeedPrice() public view returns (uint256) {
        //int256 price = priceOracle.getLatestPrice();
        //require(price > 0, "Price should be more than zero.");
        //uint256 uintPrice = uint(price);
        uint256 uintPrice = 100000000000;
        return uintPrice;
    }

    function eraseOrders() public onlyOwner {
        // Remove orders from the previous.
        delete orders;
        // Create new orders array.
        OrderMinHeap.Order[] storage orders;
    }


}