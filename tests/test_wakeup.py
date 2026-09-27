import asyncio
import contextlib
import threading

from chaski._wakeup import Wakeup


def test_notification_between_check_and_wait_is_not_lost():
    wake = Wakeup()
    version = wake.version
    wake.notify()
    returned = threading.Event()
    thread = threading.Thread(target=lambda: (wake.wait(version), returned.set()), daemon=True)
    thread.start()
    assert returned.wait(0.2)
    thread.join()

    async def run():
        await asyncio.wait_for(wake.wait_async(version), 0.2)

    asyncio.run(run())


def test_async_wait_cancellation_does_not_leave_a_closed_loop_subscriber():
    wake = Wakeup()

    async def run():
        task = asyncio.create_task(wake.wait_async(wake.version))
        await asyncio.sleep(0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())
    wake.notify()  # would fail if the closed loop were still subscribed


def test_batch_retry_is_not_shortened_by_continuous_hints():
    from types import SimpleNamespace

    from chaski.stream_changes import BatchWait

    calls = []
    wake = Wakeup()
    before = wake.version
    wake.notify()
    batch = BatchWait(wake, interval=0, stop=SimpleNamespace(wait=calls.append))
    batch.wait(before, retry=90)
    assert calls[0] == 90
