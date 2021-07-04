# distutils: language=c++
from hummingbot.logger import HummingbotLogger
import logging
from hummingbot.core.pubsub cimport PubSub

ob_logger = None

cdef class Kline(PubSub):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ob_logger
        if ob_logger is None:
            ob_logger = logging.getLogger(__name__)
        return ob_logger

    def __init__(self, price_open: double, price_high: double, price_low: double, price_close: double):
        super().__init__()
        self._open = price_open
        self._high = price_high
        self._low = price_low
        self._close = price_close

    @property
    def close_price(self) -> double:
        return self._close

    def __repr__(self):
        return f"open: {self._open}, high: {self._high}, low: {self._low}, close: {self._close} "
