#!/usr/bin/env python

import asyncio
import logging
import time
from typing import (
    AsyncIterable,
    Dict,
    List,
    Optional
)
import ujson
import websockets
from hummingbot.core.data_type.kline_stream_tracker_data_source import \
    KlineStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.binance.binance_utils import \
    convert_to_exchange_trading_pair

BINANCE_KLINE_STREAM_ENDPOINT = "@kline_1m"
BINANCE_WSS_KLINE_STREAM = "wss://stream.binance.{}:9443/ws/"


class BinanceAPIKlineStreamDataSource(KlineStreamTrackerDataSource):
    MESSAGE_TIMEOUT = 10.0
    PING_TIMEOUT = 5.0

    _bausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None,
                 domain: str = "com"):
        self._trading_pairs = trading_pairs
        self._last_recv_time: float = 0
        self._domain = domain
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> \
            AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(),
                                                      timeout=self.MESSAGE_TIMEOUT)
                    self._last_recv_time = time.time()
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter,
                                               timeout=self.PING_TIMEOUT)
                        self._last_recv_time = time.time()
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning(
                "WebSocket ping timed out for Kline stream. Going to reconnect...")
            return
        except websockets.exceptions.ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_kline_stream(self, ev_loop: asyncio.BaseEventLoop,
                                      output: asyncio.Queue):
        while True:
            try:
                url = BINANCE_WSS_KLINE_STREAM.format(self._domain)
                trading_pair = self._trading_pairs[0]
                stream_url: str = f"{url}{convert_to_exchange_trading_pair(trading_pair).lower()}{BINANCE_KLINE_STREAM_ENDPOINT}"

                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    async for raw_msg in self._inner_messages(ws):
                        msg: Dict[str, any] = ujson.loads(raw_msg)
                        # Enqueue the kline message to the queue.
                        output.put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection for Kline stream. Retrying after 30 seconds...",
                    exc_info=True)
                await asyncio.sleep(30.0)
