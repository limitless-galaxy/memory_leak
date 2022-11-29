import datetime
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

DATEDIFF = 946684800000000000
# 946695600000000000


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


# TODO: move to nautilus_core after testing!
class KDBBatchesBacktestNode(BacktestNode):
    """Node for backtest using kdb database of perpetual futures on binance"""

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
            instrument_id_value = config.instrument_id  # todo
            look_for_instrument = instrument_id_value.split("-")[0]
            symbol = [v for v in exchange_info.symbols if v.symbol == look_for_instrument][0]
            instrument = parse_perpetual_instrument_http(
                symbol_info=symbol,
                ts_init=dt_to_unix_nanos(datetime.datetime.now()),
                ts_event=dt_to_unix_nanos(datetime.datetime.now()),
            )
            engine.add_instrument(instrument)

        timestep = datetime.timedelta(days=1)

        for data_quote in self.get_data_from_kdb(data_configs, timestep):
            data = {
                "data": data_quote,
                "type": QuoteTick,
            }
            self._load_engine_data(engine=engine, data=data)
            engine.run_streaming(run_config_id=run_config_id)
            for account in engine.cache.accounts():
                for balance in account.last_event.balances:
                    engine._log.info(str(balance), LogColor.GREEN)
            engine.cache.flush_db()
            while len(data["data"]) > 0:
                item = data["data"].pop()
                del item
            del data
            while len(data_quote) > 0:
                item = data_quote.pop()
                del item
            del data_quote
            # data_quote.clear()
            # data.clear()
            engine.clear_data()  # todo ?can change it?
            gc.collect()

        print("Account report:")
        print(engine.trader.generate_account_report(Venue("BINANCE")))
        print("Fee report:")
        print(engine.trader.generate_order_fills_report())
        # print(engine.trader.generate_positions_report())
        print("Order report:")
        print(engine.trader.generate_orders_report())
        engine.end_streaming()
        engine.dispose()

    def get_data_from_kdb(self, data_configs: list[BacktestDataConfig], timestep) -> dict:
        """
        Get QuoteTicks from kdb database using batches
        """
        start_times = {config.instrument_id: config.start_time for config in data_configs}
        end_times = {config.instrument_id: config.end_time for config in data_configs}
        start_time = min(start_times.values())
        current_time = start_time
        end_time = max(end_times.values())
        aggregate_list_quoteticks = []
        while current_time < end_time:
            till_time = current_time + timestep
            if till_time > end_time:
                till_time = end_time
            while len(aggregate_list_quoteticks) > 0:
                item = aggregate_list_quoteticks.pop()
                del item
            # aggregate_list_quoteticks.clear()
            aggregate_list_quoteticks = []
            for config in data_configs:
                instrument_id = config.instrument_id.split("-")[0]
                # for item in kdb_query(token=instrument_id, date_start=current_time, date_stop=till_time):
                fake_data = self.kdb_query_fake(date_start=current_time, date_stop=till_time)
                aggregate_list_quoteticks.extend(self.parser_kdb_data_to_quotetick(fake_data, config.instrument_id, 0))
                del fake_data
            yield aggregate_list_quoteticks
            current_time = till_time

        # TODO: transform to Nautilus data format depending on data_config.type

    def kdb_query_fake(self, date_start: datetime, date_stop: datetime):
        i = 0
        data_list = []
        step = 3*10e6
        date_time = date_start.timestamp() * 10e8
        date_time_end = date_stop.timestamp() * 10e8
        while date_time + i < date_time_end:
            data_list.append([date_time+i, 11, 10, 111, 100])
            i += step
        print(f"Number of entries: {len(data_list)}")
        return data_list

    def parser_kdb_data_to_quotetick(self, data: list, instrument_id_value: str, drop_ratio: int = 0) -> QuoteTick:
        """Parser from KDB to QuoteTick"""
        for row in data:
            time = row[0]
            if random.randint(0, 99) < drop_ratio:
                continue
            ask, bid = self.check_precisions(str(row[1]), str(row[2]))
            askvol, bidvol = self.check_precisions(str(row[3]), str(row[4]))
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

    def check_precisions(self, number1: str, number2: str):
        """Set the same precisions to both numbers and back it as string"""
        if "." in number1:
            len1 = len(number1.split(".")[1])
        else:
            len1 = 0
        if "." in number2:
            len2 = len(number2.split(".")[1])
        else:
            len2 = 0
        precision = max(len1, len2)
        zeros = f".{precision}f"
        return format(float(number1), zeros), format(float(number2), zeros)
