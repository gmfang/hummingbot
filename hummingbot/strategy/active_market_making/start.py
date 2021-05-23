from typing import (
    List,
    Tuple,
)

from hummingbot import data_path
import os.path
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.active_market_making import (
    ActiveMarketMakingStrategy,
)
from hummingbot.strategy.active_market_making.active_market_making_config_map import active_market_making_config_map as c_map
from decimal import Decimal
import pandas as pd


def start(self):
    try:
        order_amount = c_map.get("order_amount").value
        order_optimization_enabled = c_map.get("order_optimization_enabled").value
        order_refresh_time = c_map.get("order_refresh_time").value
        exchange = c_map.get("exchange").value.lower()
        raw_trading_pair = c_map.get("market").value
        inventory_target_base_pct = 0 if c_map.get("inventory_target_base_pct").value is None else \
            c_map.get("inventory_target_base_pct").value / Decimal('100')
        filled_order_delay = c_map.get("filled_order_delay").value
        order_refresh_tolerance_pct = c_map.get("order_refresh_tolerance_pct").value / Decimal('100')
        add_transaction_costs_to_orders = c_map.get("add_transaction_costs").value
        ping_pong_enabled = c_map.get("ping_pong_enabled").value

        trading_pair: str = raw_trading_pair
        maker_assets: Tuple[str, str] = self._initialize_market_assets(exchange, [trading_pair])[0]
        market_names: List[Tuple[str, List[str]]] = [(exchange, [trading_pair])]
        self._initialize_wallet(token_trading_pairs=list(set(maker_assets)))
        self._initialize_markets(market_names)
        self.assets = set(maker_assets)
        maker_data = [self.markets[exchange], trading_pair] + list(maker_assets)
        self.market_trading_pair_tuples = [MarketTradingPairTuple(*maker_data)]

        strategy_logging_options = ActiveMarketMakingStrategy.OPTION_LOG_ALL
        closing_time = c_map.get("closing_time").value * Decimal(3600 * 24 * 1e3)
        min_profit_percent = c_map.get("min_profit_percent").value / Decimal(100)
        volatility_buffer_size = c_map.get("volatility_buffer_size").value
        debug_csv_path = os.path.join(data_path(),
                                      HummingbotApplication.main_application().strategy_file_name.rsplit('.', 1)[0] +
                                      f"_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")

        self.strategy = ActiveMarketMakingStrategy(
            market_info=MarketTradingPairTuple(*maker_data),
            order_amount=order_amount,
            order_optimization_enabled=order_optimization_enabled,
            inventory_target_base_pct=inventory_target_base_pct,
            order_refresh_time=order_refresh_time,
            order_refresh_tolerance_pct=order_refresh_tolerance_pct,
            filled_order_delay=filled_order_delay,
            add_transaction_costs_to_orders=add_transaction_costs_to_orders,
            logging_options=strategy_logging_options,
            hb_app_notification=True,
            closing_time=closing_time,
            debug_csv_path=debug_csv_path,
            volatility_buffer_size=volatility_buffer_size,
            is_debug=False,
            ping_pong_enabled=ping_pong_enabled,
            min_profit_percent=min_profit_percent,
        )
    except Exception as e:
        self._notify(str(e))
        self.logger().error("Unknown error during initialization.", exc_info=True)
