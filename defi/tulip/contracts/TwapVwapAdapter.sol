// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "../node_modules/@chainlink/contracts/src/v0.7/ChainlinkClient.sol";

contract TwapVwapAdapter is ChainlinkClient {
    uint256 public price;
    uint256 private fee;
    address private oracle;
    bytes32 private jobIdTwap;
    bytes32 private jobIdVwap;

    
    constructor(address _oracle, string memory _jobIdTwap, string memory _jobIdVwap, uint256 _fee) public {
        setPublicChainlinkToken();
        oracle = _oracle;
        jobIdTwap = stringToBytes32(_jobIdTwap);
        jobIdVwap = stringToBytes32(_jobIdVwap);
        fee = _fee;
    }
    
    function requestTwap(string _symbol, uint128 _startTimestamp, uint128 _endTimestamp) public returns (bytes32 requestId) {
        Chainlink.Request memory request = buildChainlinkRequest(jobIdTwap, address(this), this.fulfill.selector);
        // Set the parameters.
        request.add("symbol", _symbol);
        request.add("start_time", _startTimestamp);
        request.add("end_time", _endTimestamp);
        return sendChainlinkRequestTo(oracle, request, fee);
    }

    function requestVwap(string _symbol, uint128 _startTimestamp, uint128 _endTimestamp) public returns (bytes32 requestId) {
        Chainlink.Request memory request = buildChainlinkRequest(jobIdVwap, address(this), this.fulfill.selector);
        // Set the parameters.
        request.add("symbol", _symbol);
        request.add("start_time", _startTimestamp);
        request.add("end_time", _endTimestamp);
        return sendChainlinkRequestTo(oracle, request, fee);
    }
    
    function fulfill(bytes32 _requestId, uint256 _price) public recordChainlinkFulfillment(_requestId) {
        price = _price;
    }
    
    function stringToBytes32(string memory source) private pure returns (bytes32 result) {
        bytes memory tempEmptyStringTest = bytes(source);
        if (tempEmptyStringTest.length == 0) {
            return 0x0;
        }
        assembly { // solhint-disable-line no-inline-assembly
            result := mload(add(source, 32))
        }
    }
}