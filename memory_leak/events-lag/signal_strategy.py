# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2022 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
from datetime import datetime
import random
from _decimal import Decimal
from collections import deque
from typing import Optional, Union

import pandas as pd
from galaxy_nautilus_core.gatherings.gathering_simple_limit_order import SimpleLimitGatheringConfig, \
    SimpleLimitExecutionGathering
from galaxy_nautilus_core.gatherings.gathering_simple_market_order import SimpleMarketGatheringConfig, \
    SimpleMarketExecutionGathering
from galaxy_nautilus_core.indicators.happy_hours import HappyHours
from galaxy_nautilus_core.strategy.base_strategy import BaseStrategy, BaseStrategyConfig, BaseStrategyTarget
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.message import Event
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.data.tick import TradeTick
from nautilus_trader.model.events.order import OrderAccepted, OrderCanceled
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.enums import BookType, OrderSide
from nautilus_trader.common.logging import LogColor


# *** THIS IS A TEST STRATEGY ***


class SignalStrategyConfig(BaseStrategyConfig):
    """
    Configuration for ``SignalStrategy`` instances.
    """

    instruments_id: str

class ExtBaseStrategyTarget(BaseStrategyTarget):
    hh: HappyHours

class SignalStrategy(BaseStrategy):
    """
    A strategy that simply emits a signal counter (FOR TESTING PURPOSES ONLY).

    Parameters
    ----------
    config : OrderbookImbalanceConfig
        The configuration for the instance.
    """

    def __init__(self, config: SignalStrategyConfig):
        super().__init__(config)
        self.instrument_id = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
        self.instrument_ids = []
        for instrument in self.config.instruments_id:
            self.instrument_ids.append(InstrumentId.from_str(instrument))
        self.instrument: Optional[Instrument] = None
        self.counter = 0
        self.events = pd.DataFrame(columns=["client_order_id", "event_type", "ts_event", "event"])
        self.filtered_events = None
        self.filtered_events_ts = None
        self.file_name = "order_test_results_" + str(int(self.clock.timestamp()))
        self.targets = {}

    def on_start(self):
        super().on_start()
        """Actions to be performed on strategy start."""
        self.instrument = self.cache.instrument(self.instrument_id)

        for instrument_id in self.instrument_ids:
            self.subscribe_trade_ticks(instrument_id=instrument_id)
            self.subscribe_quote_ticks(instrument_id=instrument_id)
            self.targets[instrument_id] = ExtBaseStrategyTarget()
            self.targets[instrument_id].orders_history = deque([])
            self.cancel_all_orders(instrument_id)
            self.targets[instrument_id].hh = HappyHours(7,7)

    def on_quote_tick(self, tick: QuoteTick):
        """Actions to be performed when the strategy is running and receives a quote tick."""
        instrument_id = tick.instrument_id
        current_time = self.ts_to_datetime_rounded(tick.ts_event)
        if self.counter % 20 == 0:
            self.targets[instrument_id].hh.update_raw(tick.bid, tick.ask, current_time)
        if instrument_id.symbol.value != "BTCUSDT-PERP":
            return None
        super().on_quote_tick(tick)
        ask = tick.ask
        bid = tick.bid
        ts_event = tick.ts_event
        self.counter += 1
        if self.counter % 10 == 0:
            self.log.info(f"{self.counter}")
        if self.counter % 250 == 0:
            self.log.info(f"Save events...", LogColor.YELLOW)
            self.events.to_csv(f"{self.file_name}.csv")
            self.filtered_events.to_csv(f"{self.file_name}_filtered.csv")
            self.filtered_events_ts.to_csv(f"{self.file_name}_filtered_ts.csv")
        if self.counter % 20 == 0:
            for i in range(10):
                # df = pd.DataFrame(columns=["first","second","third"])
                # for j in range(1500):
                #     df.loc[j] = [555, 666, 777]
                #     maxx = df.max()
                #     minn = df.min()
                if random.choices([0,1,2,3,4]) == [0]:
                    self.publish_signal(name="counter", value=self.counter, ts_event=tick.ts_event)
                    side = OrderSide.SELL if self.counter % 40 == 0 else OrderSide.BUY
                    # gathering_config = SimpleLimitGatheringConfig(
                    #     instrument_id_value=instrument_id.value,
                    #     target_volume=Decimal(40),
                    #     price=self.instrument.make_price(bid/2) if side == OrderSide.BUY else self.instrument.make_price(ask*2),
                    #     order_side=side,
                    # )
                    # self.targets[instrument_id].gathering = SimpleLimitExecutionGathering(
                    #     config=gathering_config,
                    #     execution_config=self.gathering_execution_config,
                    # )
                    # self.log.info("Send gathering", LogColor.GREEN)
                    order = self.order_factory.limit(
                        instrument_id = instrument_id,
                        order_side = side,
                        quantity = self.instrument.make_qty(0.0025) if side == OrderSide.BUY else self.instrument.make_qty(0.001),
                        price = self.instrument.make_price(bid/2) if side == OrderSide.BUY else self.instrument.make_price(ask*2)
                    )
                    self.submit_order(order)
                    self.log.info(f"Send order {order}", LogColor.GREEN)

    def on_trade_tkick(self, tick: TradeTick):
        """Actions to be performed when the strategy is running and receives a trade tick."""
        # self.counter += 1
        self.publish_signal(name="counter", value=self.counter, ts_event=tick.ts_event)

    def on_event(self, event: Event) -> None:
        self.log.info(f"Event: {event}", LogColor.BLUE)
        self.events.loc[self.clock.utc_now()] = [event.client_order_id.value, event.__class__.__name__,
                                                 pd.to_datetime(event.ts_event), event]
        if isinstance(event, OrderAccepted):
            self.cancel_order(self.cache.orders_open(self.instrument.id.venue)[0])
        if isinstance(event, OrderCanceled):
            if self.targets[event.instrument_id].gathering:
                self.targets[event.instrument_id].gathering.data_subscriptions = []
                self.targets[event.instrument_id].gathering = None
            self.filtered_events = self.events.reset_index().assign(time_lag=self.events.
                                                                    reset_index(names=["index"]).
                                                                    groupby('client_order_id', group_keys=False).
                                                                    index.apply(lambda x: x - x.iloc[0])
                                                                    ).set_index("index")
            self.filtered_events_ts = self.events.assign(time_lag=self.events.
                                                                    groupby('client_order_id', group_keys=False).
                                                                    ts_event.apply(lambda x: x - x.iloc[0])
                                                                    ).set_index("ts_event")

    def on_stop(self):
        pass

    def ts_to_datetime_rounded(self, ts: Union[int, str]):
        """Convert ts_init/ts_event to datetime format (rounded by seconds)"""
        return datetime.utcfromtimestamp(int(str(ts)[:10]))