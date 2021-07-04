#!/usr/bin/env python

import asyncio
import logging
from typing import (
    Optional,
    List
)
from hummingbot.core.data_type.kline_stream_tracker_data_source import \
    KlineStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.kline_stream_tracker import KlineStreamTracker
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from .binance_api_kline_stream_data_source import \
    BinanceAPIKlineStreamDataSource


class BinanceKlineStreamTracker(KlineStreamTracker):
    _bust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bust_logger is None:
            cls._bust_logger = logging.getLogger(__name__)
        return cls._bust_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None,
                 domain: str = "com"):
        super().__init__()
        self._trading_pairs = trading_pairs
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[KlineStreamTrackerDataSource] = None
        self._kline_stream_tracking_task: Optional[asyncio.Task] = None
        self._domain = domain

    @property
    def data_source(self) -> KlineStreamTrackerDataSource:
        if not self._data_source:
            self._data_source = BinanceAPIKlineStreamDataSource(
                trading_pairs=self._trading_pairs, domain=self._domain)
        return self._data_source

    @property
    def exchange_name(self) -> str:
        if self._domain == "com":
            return "binance"
        else:
            return f"binance_{self._domain}"

    async def start(self):
        self._kline_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_kline_stream(self._ev_loop,
                                                     self._kline_stream)
        )
        await safe_gather(self._kline_stream_tracking_task)
