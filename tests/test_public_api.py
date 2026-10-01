"""Names customer services use are importable from the public surface, and
are the same objects the internal modules define."""

from __future__ import annotations

import chaski
import chaski.connector
import chaski.dataops
import chaski.dataops.base
import chaski.executor
import chaski.retry


def test_chaski_exports_the_retry_command_and_connector_helpers():
    from chaski import Backoff, Command, CommandRejected, CommandResult, run_connector

    assert Backoff is chaski.retry.Backoff
    assert Command is chaski.executor.Command
    assert CommandRejected is chaski.executor.CommandRejected
    assert CommandResult is chaski.executor.CommandResult
    assert run_connector is chaski.connector.run
    assert {"Backoff", "Command", "CommandRejected", "CommandResult", "run_connector"} <= set(chaski.__all__)


def test_chaski_dataops_exports_command_result_and_save_checkpoint():
    from chaski.dataops import CommandResult, save_checkpoint

    assert CommandResult is chaski.executor.CommandResult
    assert save_checkpoint is chaski.dataops.base.save_checkpoint
    assert {"CommandResult", "save_checkpoint"} <= set(chaski.dataops.__all__)


def test_every_name_in_all_is_importable():
    for module in (chaski, chaski.dataops):
        for name in module.__all__:
            assert getattr(module, name) is not None, f"{module.__name__}.{name}"
