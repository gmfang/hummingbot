#!/usr/bin/env python

import asyncio
from abc import abstractmethod, ABC
from enum import Enum
import logging
from typing import (
    Optional,
    List,
    Deque
)
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.kline_stream_tracker_data_source import \
    KlineStreamTrackerDataSource
from hummingbot.core.data_type.kline import Kline
import numpy as np
import talib
from collections import deque


class KlineStreamTrackerDataSourceType(Enum):
    # LOCAL_CLUSTER = 1 deprecated
    REMOTE_API = 2
    EXCHANGE_API = 3


class KlineStreamTracker(ABC):
    _ust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._ust_logger is None:
            cls._ust_logger = logging.getLogger(__name__)
        return cls._ust_logger

    def __init__(self):
        self._kline_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._klines: Deque[Kline] = deque([], maxlen=200)
        self._ema_short = float("Nan")
        self._ema_long = float("Nan")
        self._macd_histograms: List[float] = []

    @property
    @abstractmethod
    def data_source(self) -> KlineStreamTrackerDataSource:
        raise NotImplementedError

    @property
    def last_recv_time(self) -> float:
        return self.data_source.last_recv_time

    @abstractmethod
    async def start(self):
        raise NotImplementedError

    @property
    def kline_stream(self) -> asyncio.Queue:
        return self._kline_stream

    @property
    def ema_short(self) -> float:
        return self._ema_short

    @property
    def ema_long(self) -> float:
        return self._ema_long

    @property
    def macd_histograms(self) -> List[float]:
        return self._macd_histograms

    @property
    def klines(self) -> List[Kline]:
        return self._klines

    def add_kline(self, kline: Kline):
        self._klines.append(kline)

    def calc_tech_indicators(self):
        array = [float(kline.close_price) for kline in self._klines]
        # self.logger().info(f"HAHA array is {array}")
        np_closes = np.array(array)

        ema_short = talib.EMA(np_closes, timeperiod=7)
        ema_long = talib.EMA(np_closes, timeperiod=20)
        macd = talib.MACD(np_closes, fastperiod=7, slowperiod=20,
                          signalperiod=9)

        self._ema_short = ema_short[-1]
        self._ema_long = ema_long[-1]
        # MACD output 3 lists. We only need last list(histogram). We only
        # copy the last 10 histograms.
        self._macd_histograms = macd[-1][-10:]

        self.logger().info(
            f"(Classic) EMA_7 is {self._ema_short}, EMA_20 is {self._ema_long}, MACD(7, 20, 9) Histogram is {macd[-1][-1]} Histogram list is {self._macd_histograms}")
