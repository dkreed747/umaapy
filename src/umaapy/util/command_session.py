"""
.. module:: command_session
   :synopsis: Tracks the lifecycle of a single UMAA command exchange.
"""

from __future__ import annotations

from typing import Optional, Callable, Any, Dict, Tuple, List, Union, TYPE_CHECKING
import logging
from threading import Condition

import rti.connextdds as dds

from umaapy import get_event_processor
from umaapy.util.umaa_utils import HashableNumericGUID


from umaapy.util.event_processor import Command, LOW
from umaapy.util.uuid_factory import generate_guid, guid_pretty_print
from umaapy.util.timestamp import Timestamp

from umaapy.umaa_types import (
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CommandStatusEnumType,
    UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType as CommandStatusReasonEnumType,
)

if TYPE_CHECKING:
    from umaapy.core.command_consumer import CommandConsumer


class CommandSession:
    """
    Manages a single command’s acknowledgements, status, execution updates, and cancellation.

    :param command_consumer: Parent CommandConsumer instance
    :type command_consumer: CommandConsumer
    :param command: A DDS command instance, pre-populated with source & destination
    :type command: Any
    """

    def __init__(self, command_consumer: CommandConsumer, command: Any):
        """
        Initialize a new CommandSession.

        :param command_consumer: The owning CommandConsumer
        :type command_consumer: CommandConsumer
        :param command: Pre-populated command instance
        :type command: Any
        """
        self._consumer: CommandConsumer = command_consumer
        self._command: Any = command
        self._acknowledged: bool = False
        self._state: Optional[CommandStatusEnumType] = None
        self._reason: Optional[CommandStatusReasonEnumType] = None
        self._msg: Optional[str] = None
        self._session_id: HashableNumericGUID = HashableNumericGUID(generate_guid())
        self._command.sessionID = self._session_id.to_umaa()
        self._session_started = False
        self._logger: logging.Logger = self._consumer._logger.getChild(guid_pretty_print(self._session_id))
        self._lock = Condition()

        self._ack_callbacks: List[Union[Callable[[Any], None], Command]] = []
        self._status_callbacks: Dict[CommandStatusEnumType, List[Union[Callable[[Any], None], Command]]] = {
            status: [] for status in CommandStatusEnumType
        }
        self._execution_status_callbacks: Optional[List[Union[Callable[[Any], None], Command]]] = (
            [] if self._consumer._execution_status_reader else None
        )

        self._terminal_states = frozenset(
            (CommandStatusEnumType.CANCELED, CommandStatusEnumType.COMPLETED, CommandStatusEnumType.FAILED)
        )

        self._logger.info(f"Command session initialized.")

    def __eq__(self, other: CommandSession) -> bool:
        if not isinstance(other, CommandSession):
            return False

        return self._session_id == other._session_id

    def __hash__(self):
        return hash(self._session_id)

    def is_started(self) -> bool:
        with self._lock:
            return self._session_started

    def state(self) -> Optional[CommandStatusEnumType]:
        with self._lock:
            return self._state

    def reason(self) -> Optional[CommandStatusReasonEnumType]:
        with self._lock:
            return self._reason

    def message(self) -> Optional[str]:
        with self._lock:
            return self._msg

    def is_terminal(self) -> bool:
        with self._lock:
            if self._state is None:
                return False

            return self._state in self._terminal_states

    def execute(
        self, timeout: Optional[float] = 10.0, wait_for_terminal: bool = True
    ) -> Tuple[bool, Optional[CommandStatusEnumType], Optional[CommandStatusReasonEnumType], Optional[str]]:
        """
        Send the command and optionally block until it completes or fails.

        :param timeout: Seconds to wait before timing out
        :type timeout: Optional[float]
        :param wait_for_terminal: Block until terminal state if True, else return when executing
        :type wait_for_terminal: bool
        :returns: (ok, state, reason, message)
        :rtype: tuple
        :raises RuntimeError: if execute is called more than once
        """
        with self._lock:
            if self.is_started():
                self._logger.warning("Execute already called for this session")
                raise RuntimeError("Execute already called for this session")

            self._session_started = True
            self._command.timeStamp = Timestamp.now().to_umaa()

            self._logger.info("Sending command...")
            with self._consumer._consumer_lock:
                self._consumer._writer.write(self._command)

            predicate = (
                (lambda: self.is_terminal())
                if wait_for_terminal
                else (lambda: self._state is not None and self._state == CommandStatusEnumType.EXECUTING)
            )

            ok: bool = self._lock.wait_for(predicate, timeout)

            if not ok:
                self._logger.warning(
                    f"Command timed out before reaching desired state {'TERMINAL' if wait_for_terminal else 'EXECUTING'}"
                )
                self.cancel(block=False)
                return (ok, self._state, self._reason, self._msg)

            if self.is_terminal():
                if self._state == CommandStatusEnumType.COMPLETED:
                    self._logger.info("Command completed.")
                elif self._state == CommandStatusEnumType.FAILED:
                    self._logger.warning(f"Command failed (reason={self._reason}, msg={self._msg})")
                self.cancel(block=False)
                return (ok, self._state, self._reason, self._msg)

            self._logger.info("Command is executing...")
            return (ok, self._state, self._reason, self._msg)

    def execute_async(self) -> None:
        """
        Send the command without blocking; user must track callbacks or poll state.

        :raises RuntimeWarning: if called after a session has already started
        """
        with self._lock:
            if self.is_started():
                self._logger.warning("Execute already called for this session.")
                raise RuntimeWarning("Execute already called for this session.")

            self._session_started = True
            self._command.timeStamp = Timestamp.now().to_umaa()

            self._logger.info("Sending command...")
            with self._consumer._consumer_lock:
                self._consumer._writer.write(self._command)

    def update(self, command: Any, block: bool = False, timeout: Optional[float] = None) -> None:
        """
        Send an updated version of a previously-sent command.

        :param command: New command instance with updated fields
        :type command: Any
        :param block: If True, block until the update is acknowledged
        :type block: bool
        :param timeout: Seconds to wait when blocking
        :type timeout: Optional[float]
        :raises RuntimeWarning: if the session has not been started or is already terminal
        """
        with self._lock:
            if not self.is_started():
                self._logger.warning("Cannot update a session that hasn't been executed yet.")
                raise RuntimeWarning("Cannot update a session that hasn't been executed yet.")

            if self.is_terminal():
                self._logger.warning("Cannot update a session that is terminal")
                raise RuntimeWarning("Cannot update a session that is terminal")

            command.source = self._command.source
            command.destination = self._command.destination
            command.sessionID = self._command.sessionID
            command.timeStamp = Timestamp.now().to_umaa()
            self._command = command

            self._logger.info("Sending command update...")

            if not block:
                return

            predicate = lambda: self._reason == CommandStatusReasonEnumType.UPDATED

            ok: bool = self._lock.wait_for(predicate, timeout)

        if not ok:
            self._logger.warning("Command update timed out before reaching desired state ")
            return

        self._logger.info("Command updated.")

    def cancel(self, block: bool = False, timeout: Optional[float] = None) -> None:
        """
        Dispose of the command instance to request cancellation.

        :param block: If True, block until the cancellation is acknowledged
        :type block: bool
        :param timeout: Seconds to wait when blocking
        :type timeout: Optional[float]
        """
        with self._lock:
            if not self.is_started():
                self._logger.warning("Only commands that have been started and are not terminal can be canceled.")
                return

            with self._consumer._consumer_lock:
                ih = self._consumer._writer.lookup_instance(self._command)
                if ih != dds.InstanceHandle.nil:
                    self._logger.info(f"Disposing command...")
                    self._consumer._writer.dispose_instance(ih)
                else:
                    self._logger.debug("No instance to dispose - doing nothing.")

            if not block:
                self._state = None
                self._reason = None
                self._msg = None
                self._session_started = False
                return

            predicate = (
                lambda: (
                    self._state == CommandStatusEnumType.CANCELED
                    and self._reason == CommandStatusReasonEnumType.CANCELED
                )
                or self.is_terminal()
            )

            ok: bool = self._lock.wait_for(predicate, timeout)
            self._state = None
            self._reason = None
            self._msg = None
            self._session_started = False

        if not ok:
            self._logger.warning("Command cancellation timed out before reaching desired state ")
            return

        self._logger.info("Command cancelled.")

    def close(self) -> None:
        """
        Clean up the session: cancel any in-flight command and free resources with the consumer.
        """
        with self._lock:
            if self.is_started():
                self._logger.debug("Cleaning up resources on session close...")
                self.cancel()

            self._consumer.free_command_session(self)
            self._logger.debug("Command session closed.")

    def add_ack_callback(self, cb: Union[Callable[[Any], None], Command]) -> None:
        self._logger.debug("Adding acknowledgement callback...")
        self._ack_callbacks.append(cb)

    def remove_ack_callback(self, cb: Union[Callable[[Any], None], Command]) -> None:
        self._logger.debug("Removing acknowledgement callback...")
        self._ack_callbacks.remove(cb)

    def add_status_callback(self, status: CommandStatusEnumType, cb: Union[Callable[[Any], None], Command]) -> None:
        self._logger.debug("Adding status callback...")
        self._status_callbacks[status].append(cb)

    def remove_status_callback(self, status: CommandStatusEnumType, cb: Union[Callable[[Any], None], Command]) -> None:
        self._logger.debug("Removing status callback...")
        self._status_callbacks[status].remove(cb)

    def add_execution_status_callback(self, cb: Union[Callable[[Any], None], Command]) -> None:
        self._logger.debug("Adding execution status callback...")
        self._execution_status_callbacks.append(cb)

    def remove_execution_status_callback(self, cb: Union[Callable[[Any], None], Command]) -> None:
        self._logger.debug("Removing execution status callback...")
        self._execution_status_callbacks.remove(cb)

    def _handle_ack(self, ack: Any) -> None:
        with self._lock:
            if not self.is_started():
                self._logger.debug("Ack received while session is not started.")
                return
            self._acknowledged = True
            self._lock.notify_all()
        for ack_cb in self._ack_callbacks:
            get_event_processor().submit(ack_cb, ack)

    def _handle_status(self, status: Any) -> None:
        with self._lock:
            if not self.is_started():
                self._logger.debug(f"Status received while session is not started: {status.commandStatus}.")
                return
            self._state = status.commandStatus
            self._reason = status.commandStatusReason
            self._msg = status.logMessage
            if self._state == CommandStatusEnumType.COMPLETED or self._state == CommandStatusEnumType.FAILED:
                get_event_processor().submit(lambda: self.cancel(block=False), priority=LOW)
            self._lock.notify_all()
        for status_cb in self._status_callbacks[status.commandStatus]:
            get_event_processor().submit(status_cb, status)

    def _handle_execution_status(self, execution_status: Any) -> None:
        if not self.is_started():
            self._logger.debug(f"Execution status received while session is not started.")
            return
        for execution_status_cb in self._execution_status_callbacks:
            get_event_processor().submit(execution_status_cb, execution_status)
