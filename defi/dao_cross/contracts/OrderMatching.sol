pragma solidity ^0.8.0;

import "./OrderMinHeap.sol";

library OrderMatching {
    using OrderMinHeap for OrderMinHeap.Heap;
    using OrderMinHeap for OrderMinHeap.Order;

    struct Transfer {
        address token;
        uint256 amount;
        address from;
        address to;
    }

    function matchOrders(OrderMinHeap.Order[] memory orders,
                 OrderMinHeap.Heap storage buyHeap,
                 OrderMinHeap.Heap storage sellHeap,
                 uint256 clearingPrice)
        public
        returns (Transfer[] memory transfers)
    {

        // Push orders to the heaps based on the action type and filtered by limit price.
        for (uint256 i = 0; i < orders.length; i++) {
            OrderMinHeap.Order memory order = orders[i];
            if (order.isBuy == true && order.limitPrice >= clearingPrice) {
                OrderMinHeap.insert(buyHeap, order);
            } else if (order.isBuy == false && order.limitPrice <= clearingPrice) {
                OrderMinHeap.insert(sellHeap, order);
            }
        }

        // Initialize transfers array.
        // The length of the transfers array is initially set to twice the length of the orders array 
        // to ensure that it has enough space to accommodate all possible transfers without resizing 
        // the array during the matching process. This is a trade-off made for simplicity and ease 
        // of implementation.
        transfers = new Transfer[](orders.length * 2);
        uint256 transferIndex = 0;

        // Successively compare buyHeap top with sellHeap top, matching quantity until zero or queues empty.
        while (buyHeap.size > 0 && sellHeap.size > 0) {
            OrderMinHeap.Order memory buyOrder = OrderMinHeap.top(buyHeap);
            OrderMinHeap.removeTop(buyHeap);
            OrderMinHeap.Order memory sellOrder = OrderMinHeap.top(sellHeap);
            OrderMinHeap.removeTop(sellHeap);

            // Transfer quantity is equal to the min quantity among the matching buy and sell orders.
            uint256 quantity = buyOrder.quantity < sellOrder.quantity ? buyOrder.quantity : sellOrder.quantity;

            // Get base token transfer dict and add it to the transfers list.
            Transfer memory baseTransfer = Transfer(
                buyOrder.baseToken,
                quantity,
                sellOrder.walletAddress,
                buyOrder.depositAddress
            );
            transfers[transferIndex++] = baseTransfer;

            // Get quote token transfer dict and add it to the transfers list.
            Transfer memory quoteTransfer = Transfer(
                buyOrder.quoteToken,
                quantity * clearingPrice,
                buyOrder.walletAddress,
                sellOrder.depositAddress
            );
            transfers[transferIndex++] = quoteTransfer;

            // Change orders quantities with respect to the implemented transfers.
            buyOrder.quantity -= quantity;
            sellOrder.quantity -= quantity;

            // Reinsert orders with updated quantities if they are still active.
            if (buyOrder.quantity > 0) {
                OrderMinHeap.insert(buyHeap, buyOrder);
            }
            if (sellOrder.quantity > 0) {
                OrderMinHeap.insert(sellHeap, sellOrder);
            }
        }
        // Resize the transfers array to match the actual number of transfers.
        Transfer[] memory resizedTransfers = new Transfer[](transferIndex);
        for (uint256 i = 0; i < transferIndex; i++) {
            resizedTransfers[i] = transfers[i];
        }
        // Return the transfers array.
        return resizedTransfers;
    }


}