#!/usr/bin/env python3
import json
import os
import pathlib
from typing import Optional

import click
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.config import (
    BinanceDataClientConfig,
    BinanceExecClientConfig,
)
from nautilus_trader.adapters.binance.factories import (
    BinanceLiveDataClientFactory,
    BinanceLiveExecClientFactory,
)
from nautilus_trader.config import (
    CacheDatabaseConfig,
    ImportableStrategyConfig,
    InstrumentProviderConfig,
    LiveExecEngineConfig,
    StrategyFactory,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import ClientId
from dotenv import find_dotenv, load_dotenv

# from strategy_seasonality.binance_data import ExtBinanceLiveDataClientFactory


def get_trading_node(trader_id: Optional[str], client: ClientId):
    binance_api_key = os.getenv("BINANCE_API_KEY")
    # binance_api_key = "c3013974cdaa2b9c51530a887af83a4f09a2e10689f5e639ced2cbfa2c4e752d"  # testnet API
    binance_api_secret = os.getenv("BINANCE_API_SECRET")
    # binance_api_secret = "9c3cfae372b425e2ad7097809c1208f052554d0f59ae7bc3e89c6b4decd2e965"  # testnet API

    if trader_id is None:
        trader_id = "TRADER-001"

    config_node = TradingNodeConfig(
        trader_id=trader_id,
        log_level="DEBUG",
        # exec_engine=LiveExecEngineConfig(reconciliation_lookback_mins=1440, reconciliation=False),
        exec_engine=LiveExecEngineConfig(reconciliation=False),
        cache_database=CacheDatabaseConfig(type="in-memory"),
        data_clients={
            client.value: BinanceDataClientConfig(
                api_key=binance_api_key,
                api_secret=binance_api_secret,
                account_type=BinanceAccountType.FUTURES_USDT,
                # base_url_http=None,  # Override with custom endpoint
                # base_url_ws=None,  # Override with custom endpoint
                us=False,  # If client is for Binance US
                # testnet=True,  # If client uses the testnet
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
        },
        exec_clients={
            client.value: BinanceExecClientConfig(
                api_key=binance_api_key,
                api_secret=binance_api_secret,
                account_type=BinanceAccountType.FUTURES_USDT,
                # base_url_http=None,  # Override with custom endpoint
                # base_url_ws=None,  # Override with custom endpoint
                us=False,  # If client is for Binance US
                # testnet=True,  # If client uses the testnet
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
        },
        timeout_connection=20.0,
        timeout_reconciliation=15.0,
        timeout_portfolio=15.0,
        timeout_disconnection=15.0,
        timeout_post_stop=2.0,
    )

    return TradingNode(config=config_node)


def get_strategy():
    # strategy_path = os.getenv("STRATEGY_PATH")
    # config_path = os.getenv("CONFIG_PATH")
    strategy_path = "signal_strategy:SignalStrategy"
    config_path = "signal_strategy:SignalStrategyConfig"
    # white_list = json.loads(os.getenv("WHITE_LIST").replace("'", '"'))
    white_list = [
        "BTCUSDT-PERP.BINANCE",
    ]
    config = ImportableStrategyConfig(
        strategy_path=strategy_path,
        config_path=config_path,
        config=dict(
            oms_type="NETTING",
            instruments_id=white_list,
            # trade_volume=float(50),
            market="BINANCE",
        ),
    )
    return StrategyFactory.create(config)


@click.command()
@click.option("--trader-id", "-i", default=None, required=False, help="ID of the trader for the node.")
def start(trader_id: Optional[str]):
    project_dir = pathlib.Path(__file__).parent.resolve()
    binance_client = ClientId("BINANCE")

    node = get_trading_node(trader_id, binance_client)
    strategy = get_strategy()

    node.trader.add_strategy(strategy)

    node.add_data_client_factory(binance_client.value, BinanceLiveDataClientFactory)
    node.add_exec_client_factory(binance_client.value, BinanceLiveExecClientFactory)
    node.build()

    try:
        node.start()
    finally:
        node.dispose()


if __name__ == "__main__":
    load_dotenv(".env")
    start()
