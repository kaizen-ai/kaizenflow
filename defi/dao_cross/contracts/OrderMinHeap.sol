pragma solidity ^0.8.0;

library OrderMinHeap {
    struct Order {
        uint256 index;
        address walletAddress;
        address baseToken;
        address quoteToken;
        uint256 quantity;
        uint256 limitPrice;
        bool isBuy;
        address depositAddress;
        uint256 timestamp;
    }

    struct Heap {
        uint32 size;
        Order[] data;
    }

    function createHeap(Heap storage self) internal {
        self.size = 0;
    }

    function insert(Heap storage self, Order storage order) internal {
        require(self.size < self.data.length, "Heap overflow");
        uint32 _index = self.size++;
        while (_index > 0 && self.data[parent(_index)].limitPrice < order.limitPrice) {
            self.data[_index] = self.data[parent(_index)];
            _index = parent(_index);
        }
        self.data[_index] = order;
    }

    function removeTop(Heap storage self) internal {
        require(self.size > 0, "Heap underflow");
        self.data[0] = self.data[--self.size];
        sink(self, 0);
    }

    function topIndex(Heap storage self) internal view returns (uint256) {
        require(self.size > 0, "Heap underflow");
        return self.data[0].index;
    }

    function parent(uint32 i) private pure returns (uint32) {
        return (i - 1) / 2;
    }

    function left(uint32 i) private pure returns (uint32) {
        return 2 * i + 1;
    }

    function right(uint32 i) private pure returns (uint32) {
        return 2 * i + 2;
    }

    function sink(Heap storage self, uint32 i) private {
        uint32 min = i;
        if (left(i) < self.size && self.data[left(i)].limitPrice > self.data[min].limitPrice) {
            min = left(i);
        }
        if (right(i) < self.size && self.data[right(i)].limitPrice > self.data[min].limitPrice) {
            min = right(i);
        }
        if (min != i) {
            (self.data[i], self.data[min]) = (self.data[min], self.data[i]);
            sink(self, min);
        }
    }
}