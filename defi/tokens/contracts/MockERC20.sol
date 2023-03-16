// SPDX-License-Identifier: MIT
pragma solidity ^0.8.4;

import "../node_modules/@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "../node_modules/@openzeppelin/contracts/access/Ownable.sol";

contract MockERC20 is ERC20, Ownable {
    uint256 public MAXIMUM_SUPPLY;

    constructor(string memory name, string memory symbol, uint256 initialSupply, uint256 maxSupply) ERC20(name, symbol) {
        MAXIMUM_SUPPLY = maxSupply * 10**decimals();
        initialSupply = initialSupply * 10**decimals();
        require(initialSupply <= MAXIMUM_SUPPLY, "Premint amount is more than maximum supply");
        _mint(msg.sender, initialSupply);
    }
}
