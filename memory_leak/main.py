from datetime import datetime

import msgspec
import requests
from nautilus_trader.adapters.binance.futures.parsing.data import (
    parse_perpetual_instrument_http,
)
from nautilus_trader.adapters.binance.futures.schemas.market import (
    BinanceFuturesExchangeInfo,
)
from nautilus_trader.config import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestRunConfig,
    BacktestVenueConfig,
    ImportableStrategyConfig,
)
from nautilus_trader.model.data.tick import QuoteTick
from node import BatchesBacktestNode

START = datetime(2021, 10, 17)  # 2021.10.17
END = datetime(2022, 11, 15)
instruments_list = [
    "BTCUSDT-PERP.BINANCE",
]


def get_market():
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


instruments = {}
# making dict of instruments
exchange_info = get_market()
for symbol_info in exchange_info.symbols:
    if symbol_info.symbol + "-PERP.BINANCE" in instruments_list:
        parsed = parse_perpetual_instrument_http(
            symbol_info=symbol_info,
            ts_init=100000,
            ts_event=100000,
        )
        instruments[symbol_info.symbol] = parsed

###########################################################################################
# Configure backtest engine

venues_config = [
    BacktestVenueConfig(
        name="BINANCE",
        oms_type="NETTING",
        account_type="MARGIN",
        base_currency="USD",
        starting_balances=["1000000 USDT"],
    )
]

data_config = [
    BacktestDataConfig(
        catalog_path=str(""),
        data_cls=QuoteTick,
        instrument_id='BTCUSDT-PERP.BINANCE',
        start_time=START,
        end_time=END,
    )
]

strategies = [
    ImportableStrategyConfig(
        strategy_path="memory_leak.signal_strategy:SignalStrategy",
        config_path="memory_leak.signal_strategy:SignalStrategyConfig",
        config=dict(
            instrument_id="BTCUSDT-PERP.BINANCE",
        ),
    )
]

config = BacktestRunConfig(
    engine=BacktestEngineConfig(strategies=strategies, log_level="INFO"),
    data=data_config,
    venues=venues_config,
)

# Run the backtest!
###########################################################################################
node = BatchesBacktestNode(configs=[config])

results = node.run()
