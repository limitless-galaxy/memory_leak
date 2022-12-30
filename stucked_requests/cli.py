#!/usr/bin/env python3
import json
import os
import pathlib
from typing import Optional

import click
from dotenv import load_dotenv, find_dotenv

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.config import (
    BinanceDataClientConfig,
    BinanceExecClientConfig,
)
from nautilus_trader.adapters.binance.factories import BinanceLiveExecClientFactory, BinanceLiveDataClientFactory
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



def get_trading_node(trader_id: Optional[str], client: ClientId):
    binance_api_key = os.getenv("BINANCE_API_KEY")
    binance_api_secret = os.getenv("BINANCE_API_SECRET")

    if trader_id is None:
        trader_id = "TRADER-001"

    config_node = TradingNodeConfig(
        trader_id=trader_id,
        log_level="INFO",
        # exec_engine={
        #     "reconciliation_lookback_mins": 131440,
        #     "reconciliation": False
        # },
        # exec_engine=LiveExecEngineConfig(reconciliation_lookback_mins=1440, reconciliation=True),
        exec_engine=LiveExecEngineConfig(reconciliation=False),
        cache_database=CacheDatabaseConfig(type="in-memory"),
        data_clients={
            client.value: BinanceDataClientConfig(
                api_key=binance_api_key,
                api_secret=binance_api_secret,
                account_type=BinanceAccountType.FUTURES_USDT,
                base_url_http=None,  # Override with custom endpoint
                base_url_ws=None,  # Override with custom endpoint
                us=False,  # If client is for Binance US
                testnet=False,  # If client uses the testnet
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
        },
        exec_clients={
            client.value: BinanceExecClientConfig(
                api_key=binance_api_key,
                api_secret=binance_api_secret,
                account_type=BinanceAccountType.FUTURES_USDT,
                # base_url_http=None,  # Override with custom endpoint
                # base_ws_http=None,  # Override with custom endpoint
                us=False,  # If client is for Binance US
                testnet=False,  # If client uses the testnet
                # load_all_instruments=True,  # If load all instruments on start
                # load_instrument_ids=[],  # Optionally pass a list of instrument IDs
                instrument_provider=InstrumentProviderConfig(load_all=True),
                clock_sync_interval_secs=10 * 60,
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
    strategy_path = os.getenv("STRATEGY_PATH")
    config_path = os.getenv("CONFIG_PATH")
    white_list = json.loads(os.getenv("WHITE_LIST").replace("'", '"'))
    config = ImportableStrategyConfig(
        strategy_path=strategy_path,
        config_path=config_path,
        config=dict(
            instruments_id=white_list,
            market=os.getenv("MARKET"),
        ),
    )
    return StrategyFactory.create(config)


@click.command()
@click.option("--trader-id", "-i", default=None, required=False, help="ID of the trader for the node.")
def start(trader_id: Optional[str]):
    load_dotenv(find_dotenv())
    binance_client = ClientId(os.getenv("MARKET"))

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
    start()
