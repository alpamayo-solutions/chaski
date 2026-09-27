"""Compatibility names for the shared generation-based Doorbell."""

import asyncio

from .doorbell import Doorbell


class Wakeup(Doorbell):
    @property
    def version(self):
        return self.generation

    def notify(self):
        self.ring()

    def wait(self, version, timeout=None):
        return self.wait_after(version, timeout)

    async def wait_async(self, version, timeout=None, *, stop=None):
        waiting = asyncio.create_task(self.after(version))
        stopping = asyncio.create_task(stop.wait()) if stop is not None else None
        try:
            await asyncio.wait(
                {waiting, stopping} if stopping else {waiting}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            waiting.cancel()
            if stopping is not None:
                stopping.cancel()
            await asyncio.gather(waiting, *([stopping] if stopping else []), return_exceptions=True)
