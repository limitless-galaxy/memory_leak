import random
import time
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Callable, Optional, Union

from nautilus_trader.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy

from nautilus_trader.core.data import Data
from nautilus_trader.model.data.bar import Bar, BarType
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.enums import BookType, OrderSide
from nautilus_trader.model.events.order import OrderEvent
from nautilus_trader.model.events.position import PositionChanged, PositionEvent
from nautilus_trader.model.identifiers import ClientId, InstrumentId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.instruments.crypto_perpetual import CryptoPerpetual
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.position import Position



class MyStrategyConfig(StrategyConfig):
    instruments_id: list[str]
    market: str



class MyStrategy(Strategy):
    instruments: list
    orders_submitted: int = 0
    should_submit_orders: bool = True
    trade_volume = 5  # usd

    def __init__(self, config: MyStrategyConfig) -> None:
        super().__init__(config)

        self.market_client = ClientId("BINANCE")
        self.trade_volume = Decimal(config.trade_volume)
        self.instruments_id = [InstrumentId.from_str(instrument) for instrument in config.instruments_id]

    def on_start(self) -> None:
        super().on_start()
        self.instruments = [self.cache.instrument(iid) for iid in self.instruments_id]
        for instrument in self.instruments:
            self.subscribe_quote_ticks(instrument.id)
            self.cancel_all_orders(instrument.id)

    def on_event(self, event: OrderEvent) -> None:
        return super().on_event(event)

    def on_quote_tick(self, tick: QuoteTick):
        super().on_quote_tick(tick)

        random_triggered = random.randint(1, 300) % 3 == 0
        if random_triggered and self.should_submit_orders:
            print("random triggered!")
            time.sleep(2)
            instrument = self.cache.instrument(tick.instrument_id)
            order = self.order_factory.market(
                instrument_id=instrument.id,
                order_side=OrderSide.BUY,
                quantity=instrument.make_qty(self.trade_volume / tick.ask),
            )
            print(f"{datetime.now()} submitting order")
            self.submit_order(order)
            print(f"{datetime.now()} order submitted")
            self.increment_orders_submitted()
            # time.sleep(2)

    def increment_orders_submitted(self, max_orders=3):
        self.orders_submitted += 1
        if self.orders_submitted > max_orders:
            self.should_submit_orders = False

    def on_synthetic_bar(self, bar_data: Bar) -> None:
        """Handle when Bar data received"""

    def on_historical_data(self, data: Data) -> None:
        """Handle historical data"""

    def handle_with_signal(self, instrument_id: InstrumentId, current_time: datetime) -> None:
        pass


    # def on_stop(self) -> None:
    #     """Close position on stop strategy"""
    #     self.log.info("Stop hook triggered!")
    #     self.exit_all_positions()
