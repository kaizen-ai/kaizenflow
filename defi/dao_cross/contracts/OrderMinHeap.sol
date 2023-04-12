pragma solidity ^0.8.0;

library OrderMinHeap {
    struct Heap {
        Order[] data;
        uint32 size;
    }

    struct Order {
        //uint256 id;
        address walletAddress;
        address baseToken;
        address quoteToken;
        uint256 quantity;
        uint256 limitPrice;
        bool isBuy; // true for buy, false for sell
        address depositAddress;
        // The timestamp of the order creation.
        uint256 timestamp;
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

    function isLessThan(Order memory a, Order memory b) internal pure returns (bool) {
        if (a.isBuy) {
            return a.limitPrice > b.limitPrice || (a.limitPrice == b.limitPrice && a.timestamp < b.timestamp);
        } else {
            return a.limitPrice < b.limitPrice || (a.limitPrice == b.limitPrice && a.timestamp < b.timestamp);
        }
    }

    function top(Heap storage heap) internal view returns (Order memory) {
        require(heap.data.length > 0, "Heap is empty");
        return heap.data[0];
    }

    function insert(Heap storage heap, Order memory order) internal {
        // Push the new order to the end of the data array.
        heap.data.push(order);
        // Increment the size of the heap.
        heap.size++;
        // Bubble up the newly added order to maintain the min-heap property.
        bubbleUp(heap, heap.size - 1);
    }

    function createHeap(uint32 _initialCapacity) internal pure returns (Heap memory) {
        Order[] memory data = new Order[](_initialCapacity);
        uint32 size = 0;
        return Heap(data, size);
    }

    function removeTop(Heap storage heap) internal {
        require(heap.data.length > 0, "Heap is empty");
        if (heap.data.length == 1) {
            heap.data.pop();
        } else {
            heap.data[0] = heap.data[heap.data.length - 1];
            heap.data.pop();
            heapify(heap, 0);
        }
    }

    /**
    * @dev Restores the min-heap property by recursively moving the element at
    *      position `i` downwards in the heap until it's in the correct position.
    *
    * @param _heap The OrderMinHeap instance to perform heapify on.
    * @param _index The index of the element in the heap's data array to be heapified.
    */
    function heapify(Heap storage _heap, uint32 _index) internal {
        uint32 smallest = _index;
        uint32 l = left(_index);
        uint32 r = right(_index);
        // Find the smallest element among the current element, its left child, and its right child.
        if (l < _heap.data.length && isLessThan(_heap.data[l], _heap.data[smallest])) {
            smallest = l;
        }
        if (r < _heap.data.length && isLessThan(_heap.data[r], _heap.data[smallest])) {
            smallest = r;
        }
        // If the smallest element is not the current element, swap them and continue heapifying downwards.
        if (smallest != _index) {
            _heap.data[smallest] = _heap.data[_index];
            _heap.data[_index] = _heap.data[smallest];
            heapify(_heap, smallest);
        }
    }

    /*
    * @dev Bubble up the element at the given index in the heap.
    *
    * This function is called after inserting a new element into the heap.
    * It restores the heap property by repeatedly swapping the element at the given
    * index with its parent if the parent is greater than the element (for a min-heap),
    * until the element reaches its correct position in the heap or becomes the root.
    *
    * @param heap The heap data structure containing the element to bubble up.
    * @param index The index of the element in the heap to bubble up.
    */
    function bubbleUp(Heap storage heap, uint32 _index) internal {
        uint32 parentIndex = parent(_index);
        while (_index > 0 && heap.data[_index].limitPrice < heap.data[parentIndex].limitPrice) {
            // Swap the elements at index i and parentIndex
            heap.data[_index] = heap.data[parentIndex];
            heap.data[parentIndex] = heap.data[_index];
            // Move up the heap
            _index = parentIndex;
            parentIndex = parent(_index);
        }
}
}