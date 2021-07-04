# distutils: language=c++

from libc.stdint cimport int64_t
from libcpp.set cimport set
from libcpp.list cimport list
from libcpp.vector cimport vector
from hummingbot.core.pubsub cimport PubSub


cdef class Kline(PubSub):
    cdef double _open
    cdef double _high
    cdef double _low
    cdef double _close
