import pytest
import time
from concurrent.futures import TimeoutError
from umaapy.util.event_processor import *


class DummyCommand(Command):
    def __init__(self):
        self.ran = False

    def execute(self):
        self.ran = True
        return "done"


class TestEventProcessor:
    ep = EventProcessor()

    def test_one_off_execution(self):
        assert self.ep.running()
        fut = self.ep.submit(lambda: 42, priority=MEDIUM)
        result = fut.result(timeout=1)
        assert result == 42

    def test_command_execution(self):
        assert self.ep.running()
        cmd = DummyCommand()
        fut = self.ep.submit(cmd, priority=LOW)
        res = fut.result(timeout=1)
        assert cmd.ran is True
        assert res is "done"

    def test_recurring_task(self):
        assert self.ep.running()
        calls = []

        def recur():
            calls.append(time.time())

        tid = self.ep.submit_recurring(recur, interval_ms=50)
        time.sleep(0.18)
        self.ep.cancel(tid)
        # Expect at least 3 calls (at 0ms, ~50ms, ~100ms, ~150ms)
        assert len(calls) >= 3
