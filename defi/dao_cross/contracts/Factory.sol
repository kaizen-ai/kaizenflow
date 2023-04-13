// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "./DaoCross.sol";
import "../node_modules/@openzeppelin/contracts/access/Ownable.sol";


contract SwapFactory is Ownable {

    // Registry for swap in token address => swap address format.
    address[] public coveredTokens;
    mapping(address => address) tokenToSwap;

    event PairCreated(address indexed token, address swapAddress, address creator, uint256 timestamp);

    constructor() {}

    // Create new eth-token swap pair.
    function createNewPair(
           address _token,
           uint16 _swapPeriodInSecs,
           uint8 _swapRandomizationInSecs, 
           uint8 _feesAsPct,
           address _priceOracle) public {
        require(_token != address(0));
        require(tokenToSwap[_token] == address(0), "Swap contract for this token already exists");
        address swapAddress = address(new DaoCross(
                                _token, 
                                _swapPeriodInSecs,
                                _swapRandomizationInSecs,
                                _feesAsPct,
                                _priceOracle));
        tokenToSwap[_token] = swapAddress;
        coveredTokens.push(_token);
        emit PairCreated(_token, swapAddress, msg.sender, block.timestamp);
    }

    function getSwapByToken(address _token) external view returns (address) {
        address swapAddr = tokenToSwap[_token];
        require(swapAddr != address(0), "There is no swap contract for this token yet.");
        return swapAddr;
    }

    function getCoveredTokens() external view returns (address[] memory) {
        return coveredTokens;
    }

}