#!/usr/bin/env python

import asyncio
from hummingbot.core.utils.async_utils import safe_ensure_future

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication
# import yappi


class ExitCommand:
    def exit(self,  # type: HummingbotApplication
             force: bool = False):
        safe_ensure_future(self.exit_loop(force), loop=self.ev_loop)

    async def exit_loop(self,  # type: HummingbotApplication
                        force: bool = False):
        if self.strategy_task is not None and not self.strategy_task.cancelled():
            self.strategy_task.cancel()
        if force is False and self._trading_required:
            success = await self._cancel_outstanding_orders()
            if not success:
                self._notify('Wind down process terminated: Failed to cancel all outstanding orders. '
                             '\nYou may need to manually cancel remaining orders by logging into your chosen exchanges'
                             '\n\nTo force exit the app, enter "exit -f"')
                return
            # Freeze screen 1 second for better UI
            await asyncio.sleep(1)

        self._notify("Winding down notifiers...")
        for notifier in self.notifiers:
            notifier.stop()
        # f = open("yappi_result.log", "a")
        # stats = yappi.get_func_stats()
        # sorted_stats = stats.sort(sort_type="tavg", sort_order="desc")
        # sorted_stats.print_all(out=f, columns={
        #     0: ("name", 108),  # More chars for function name
        #     1: ("ncall", 8),
        #     2: ("tsub", 8),
        #     3: ("ttot", 8),
        #     4: ("tavg", 8)
        # })
        self.app.exit()
