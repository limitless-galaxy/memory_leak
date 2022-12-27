import gc
import random
from typing import Optional

import msgspec
import requests
from nautilus_trader.adapters.binance.futures.parsing.data import (
    parse_perpetual_instrument_http,
)
from nautilus_trader.adapters.binance.futures.schemas.market import (
    BinanceFuturesExchangeInfo,
)
from nautilus_trader.backtest.engine import BacktestEngine, BacktestResult
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.common.logging import LogColor
from nautilus_trader.config import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestVenueConfig,
)
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId, Venue
from datetime import datetime, timedelta


def get_perp_market():
    """
    Get info about instrument for parsing
    Return:
          exchange_info (BinanceFuturesExchangeInfo): results with data about instruments
    """
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    exchange_data = requests.get(
        url,
    )
    exchange_info = msgspec.json.Decoder(BinanceFuturesExchangeInfo).decode(exchange_data.content)
    return exchange_info


class BatchesBacktestNode(BacktestNode):

    def _run(
            self,
            run_config_id: str,
            engine_config: BacktestEngineConfig,
            venue_configs: list[BacktestVenueConfig],
            data_configs: list[BacktestDataConfig],
            batch_size_bytes: Optional[int] = None,
    ) -> BacktestResult:

        engine: BacktestEngine = self._create_engine(
            run_config_id=run_config_id,
            config=engine_config,
            venue_configs=venue_configs,
            data_configs=data_configs,
        )

        # Run backtest
        self._run_db_streaming(
            run_config_id=run_config_id,
            engine=engine,
            data_configs=data_configs,
        )

        # Release data objects
        engine.dispose()
        return engine.get_result()

    def _run_db_streaming(self, run_config_id: str, engine: BacktestEngine, data_configs: list[BacktestDataConfig]):
        exchange_info = get_perp_market()
        for config in data_configs:
            instrument_id_value = config.instrument_id
            look_for_instrument = instrument_id_value.split("-")[0]
            symbol = [v for v in exchange_info.symbols if v.symbol == look_for_instrument][0]
            instrument = parse_perpetual_instrument_http(
                symbol_info=symbol,
                ts_init=dt_to_unix_nanos(datetime.now()),
                ts_event=dt_to_unix_nanos(datetime.now()),
            )
            engine.add_instrument(instrument)

        timestep = timedelta(days=1)

        for data_quote in self.get_data(data_configs, timestep):
            data = {
                "data": data_quote,
                "type": QuoteTick,
            }
            self._load_engine_data(engine=engine, data=data)
            engine.run_streaming(run_config_id=run_config_id)
            for account in engine.cache.accounts():
                for balance in account.last_event.balances:
                    engine._log.info(str(balance), LogColor.GREEN)
            # engine.cache.flush_db()
            # data_quote.clear()
            # data.clear()
            engine.clear_data()
            gc.collect()

        print("Account report:")
        print(engine.trader.generate_account_report(Venue("BINANCE")))
        print("Fee report:")
        print(engine.trader.generate_order_fills_report())
        print("Order report:")
        print(engine.trader.generate_orders_report())
        engine.end_streaming()
        engine.dispose()

    def get_data(self, data_configs: list[BacktestDataConfig], timestep) -> dict:

        start_times = {config.instrument_id: config.start_time for config in data_configs}
        end_times = {config.instrument_id: config.end_time for config in data_configs}
        start_time = min(start_times.values())
        current_time = start_time
        end_time = max(end_times.values())

        aggregate_list_quoteticks = []
        fake_data = self.get_fake_ticks(date_start=current_time, date_stop=current_time+timestep)
        aggregate_list_quoteticks.extend(self.parse_fake_data_to_quotetick(fake_data, data_configs[0].instrument_id))

        while current_time < end_time:
            till_time = current_time + timestep
            if till_time > end_time:
                till_time = end_time
            yield aggregate_list_quoteticks
            current_time = till_time

    def get_fake_ticks(self, date_start: datetime, date_stop: datetime) -> list:
        i = 0
        data_list = []
        step = 5 * 10e5
        date_time = date_start.timestamp() * 10e8
        date_time_end = date_stop.timestamp() * 10e8
        while date_time + i < date_time_end:
            data_list.append([date_time + i, 11, 10, 111, 100])
            i += step
        print(f"Number of entries: {len(data_list)}")
        return data_list

    def parse_fake_data_to_quotetick(self, data: list, instrument_id_value: str) -> QuoteTick:
        for row in data:
            time = row[0]
            ask, bid = str(row[1]), str(row[2])
            askvol, bidvol = str(row[3]), str(row[4])
            data_qt = {
                "instrument_id": instrument_id_value,
                "bid": bid,
                "ask": ask,
                "bid_size": bidvol,
                "ask_size": askvol,
                "ts_event": time,
                "ts_init": time,
            }
            yield QuoteTick.from_dict(data_qt)
            data_qt.clear()
            del row


