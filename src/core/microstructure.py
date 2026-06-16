import logging
from typing import Dict, List, Any, Tuple

logger = logging.getLogger("QUANT_CORE.MICROSTRUCTURE")

class OrderBookProcessor:
    def __init__(self, depth_levels: int = 10):
        self.depth_levels = depth_levels

    def calculate_imbalance(self, bids: List[List[str]], asks: List[List[str]]) -> Tuple[float, float]:
        """
        Parses raw order book structures and isolates the directional liquidity imbalance delta.
        Returns a tuple containing: (order_book_imbalance_value, mid_market_price)
        """
        try:
            # Take the slice of target execution depth layers
            active_bids = bids[:self.depth_levels]
            active_asks = asks[:self.depth_levels]
            
            if not active_bids or not active_asks:
                return 0.0, 0.0

            # Compute mid-market pricing index anchor
            best_bid = float(active_bids[0][0])
            best_ask = float(active_asks[0][0])
            mid_price = (best_bid + best_ask) / 2.0

            # Aggregate volume weight distribution metrics across structural depths
            total_bid_volume = sum(float(level[1]) for level in active_bids)
            total_ask_volume = sum(float(level[1]) for level in active_asks)
            
            volume_sum = total_bid_volume + total_ask_volume
            if volume_sum == 0:
                return 0.0, mid_price

            # Scale values tightly between -1.0 (Extreme Ask Wall) and +1.0 (Extreme Bid Wall)
            obi = (total_bid_volume - total_ask_volume) / volume_sum
            return round(obi, 4), round(mid_price, 2)

        except Exception as e:
            logger.error(f"Failed to process streaming order book slice matrix: {e}")
            return 0.0, 0.0