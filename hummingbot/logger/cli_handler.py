#!/usr/bin/env python

from logging import StreamHandler
from typing import Optional
from datetime import datetime
import pytz


class CLIHandler(StreamHandler):
    def formatException(self, _) -> Optional[str]:
        return None

    def format(self, record) -> str:
        exc_info = record.exc_info
        if record.exc_info is not None:
            record.exc_info = None
        retval = f'{datetime.fromtimestamp(record.created, pytz.timezone("America/New_York")).strftime("%H:%M:%S")} - {record.name.split(".")[-1]} - ' \
                 f'{record.msg}'
        if exc_info:
            retval += " (See log file for stack trace dump)"
        record.exc_info = exc_info
        return retval
