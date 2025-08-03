from typing import Any, Type, Optional, Callable, Tuple, override
import threading
import time
import logging

import rti.connextdds as dds

from umaapy.util.umaa_utils import validate_umaa_type, UMAAConcept
from umaapy.util.dds_configurator import UmaaQosProfileCategory
from umaapy import get_configurator
from umaapy.util.timestamp import Timestamp
from umaapy.util.event_processor import Command
from umaapy.util.uuid_factory import guid_pretty_print

from umaapy.umaa_types import (
    UMAA_Common_IdentifierType,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CmdStatus,
    UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType as CmdReason,
)


class UmaaCommandException(Exception):
    """
    Exception raised for UMAA command-processing errors.

    :param reason: UMAA command status reason enum indicating failure cause.
    :type reason: CmdReason
    :param message: Human-readable error message.
    :type message: str
    """

    def __init__(self, reason: CmdReason, message: str = ""):
        super().__init__(message)
        self.reason: CmdReason = reason
        self.message: str = message


class UmaaCommand(Command):
    """
    Wraps a UMAA command sample with lifecycle, acknowledgement,
    status updates, and execution hooks.

    :param source: Identifier of the command issuer for ACK/status topics.
    :type source: UMAA_Common_IdentifierType
    :param command: DDS command data sample to execute.
    :type command: Any
    :param logger: Logger for debug and warning output.
    :type logger: logging.Logger
    :param ack_writer: DataWriter for command acknowledgement messages.
    :type ack_writer: dds.DataWriter
    :param status_writer: DataWriter for command status updates.
    :type status_writer: dds.DataWriter
    :param execution_status_writer: Optional DataWriter for detailed execution status.
    :type execution_status_writer: Optional[dds.DataWriter]
    :raises RuntimeError: If any provided DDS type is not a valid UMAA sample.
    """

    def __init__(
        self,
        source: UMAA_Common_IdentifierType,
        command: Any,
        logger: logging.Logger,
        ack_writer: dds.DataWriter,
        status_writer: dds.DataWriter,
        execution_status_writer: Optional[dds.DataWriter] = None,
    ):
        # Initialize logger and source identifier
        self._source_id: UMAA_Common_IdentifierType = source

        # Validate the command sample
        if not validate_umaa_type(command, UMAAConcept.COMMAND):
            raise RuntimeError(f"'{type(command).__name__.split('_')[-1]}' is not a valid UMAA command.")
        self.command: Any = command
        self._logger: logging.Logger = logger.getChild(guid_pretty_print(command.sessionID))

        # Validate acknowledgement writer type
        ack_type = ack_writer.topic.type()
        if not validate_umaa_type(ack_type, UMAAConcept.ACKNOWLEDGEMENT):
            raise RuntimeError(
                f"'{ack_type.__class__.__name__.split('_')[-1]}' is not a valid UMAA command acknowledgement."
            )
        self._ack_writer: dds.DataWriter = ack_writer

        # Validate status writer type
        status_type = status_writer.topic.type()
        if not validate_umaa_type(status_type, UMAAConcept.STATUS):
            raise RuntimeError(f"'{status_type.__class__.__name__.split('_')[-1]}' is not a valid UMAA status.")
        self._status_writer: dds.DataWriter = status_writer

        # Validate optional execution-status writer
        if execution_status_writer:
            exec_type = execution_status_writer.topic.type()
            if not validate_umaa_type(exec_type, UMAAConcept.EXECUTION_STATUS):
                raise RuntimeError(
                    f"'{exec_type.__class__.__name__.split('_')[-1]}' is not a valid UMAA execution status."
                )
        self._execution_status_writer: Optional[dds.DataWriter] = execution_status_writer

        # Internal synchronization and flags
        self._cancelled: bool = False
        self._updated: bool = False
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def update(self, new_command: Any) -> None:
        """
        Atomically update the command data and notify waiting threads.

        :param new_command: Updated command sample.
        :type new_command: Any
        """
        with self._condition:
            self.command = new_command
            self._updated = True
            self._condition.notify_all()

    def cancel(self) -> None:
        """
        Request cancellation of command execution.
        """
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()

    def wait_for(
        self,
        predicate: Callable[[], bool],
        timeout: Optional[float] = None,
    ) -> Tuple[bool, bool]:
        """
        Block until a condition is met, update arrives, or cancellation.

        :param predicate: A no-arg function returning True when ready.
        :type predicate: Callable[[], bool]
        :param timeout: Maximum seconds to wait; None to wait indefinitely.
        :type timeout: Optional[float]
        :return: (predicate_satisfied, was_updated)
        :rtype: Tuple[bool, bool]
        """
        end_time = time.monotonic() + timeout if timeout else None
        with self._condition:
            while True:
                if self._cancelled:
                    return False, False
                if self._updated:
                    return False, True
                if predicate():  # Desired condition reached
                    return True, False
                # Compute remaining time
                if end_time is not None:
                    remaining = end_time - time.monotonic()
                    if remaining <= 0:
                        return False, False
                    self._condition.wait(remaining)
                else:
                    self._condition.wait()

    @override
    def execute(self, *args: Any, **kwargs: Any) -> None:
        """
        Orchestrate command lifecycle: send ack, status updates,
        call lifecycle hooks, and handle exceptions.
        """
        try:
            self._send_ack()

            while True:
                # Stage: Issued
                with self._condition:
                    status_reason = CmdReason.UPDATED if self._updated else CmdReason.SUCCEEDED
                    self._send_status(CmdStatus.ISSUED, status_reason, "Command issued.")
                    self._updated = False

                # Stage: Commanded
                self._send_status(CmdStatus.COMMANDED, CmdReason.SUCCEEDED, "Command is being processed.")
                self.on_commanded()
                if self._cancelled:
                    self._send_status(CmdStatus.CANCELED, CmdReason.CANCELED, "Command canceled.")
                    return
                if self._updated:
                    continue

                # Stage: Executing
                self._send_status(CmdStatus.EXECUTING, CmdReason.SUCCEEDED, "Command execution started.")
                self.on_executing()
                if self._cancelled:
                    self._send_status(CmdStatus.CANCELED, CmdReason.CANCELED, "Command canceled.")
                    return
                if self._updated:
                    continue

                # Stage: Complete
                self.on_complete()
                self._send_status(CmdStatus.COMPLETED, CmdReason.SUCCEEDED, "Command completed.")
                break

        except UmaaCommandException as uce:
            # UMAA-defined failure
            self._send_status(CmdStatus.FAILED, uce.reason, uce.message)
            self.on_failed(uce)
        except Exception as e:
            # Unexpected exception
            self._send_status(
                CmdStatus.FAILED,
                CmdReason.SERVICE_FAILED,
                f"{type(e).__name__} during execute: {e}",
            )
            self.on_error(e)
        finally:
            # Always call terminal hook
            self._logger.debug("Command execution terminal stage.")
            self.on_terminal()

            ack = self._ack_writer.topic.type()
            ack.source = self._source_id
            ack.sessionID = self.command.sessionID
            try:
                ih = self._ack_writer.lookup_instance(ack)
                self._ack_writer.dispose_instance(ih)
            except Exception as e:
                pass

            status = self._status_writer.topic.type()
            status.source = self._source_id
            status.sessionID = self.command.sessionID
            try:
                ih = self._status_writer.lookup_instance(ack)
                self._status_writer.dispose_instance(ih)
            except Exception as e:
                pass

            if self._execution_status_writer:
                execution_status = self._execution_status_writer.topic.type()
                execution_status.source = self._source_id
                execution_status.sessionID = self.command.sessionID
                try:
                    ih = self._execution_status_writer.lookup_instance(ack)
                    self._execution_status_writer.dispose_instance(ih)
                except Exception as e:
                    pass

    def on_commanded(self) -> None:
        """
        Hook called after command is in COMMANDED state. Override in subclass.
        """
        pass

    def on_executing(self) -> None:
        """
        Hook called when transitioning to EXECUTING state. Must be overridden.

        :raises UmaaCommandException: If execution fails.
        """
        raise UmaaCommandException(CmdReason.SERVICE_FAILED, "on_executing not implemented")

    def on_complete(self) -> None:
        """
        Hook called when command completes successfully. Override in subclass.
        """
        pass

    def on_failed(self, command_exception: UmaaCommandException) -> None:
        """
        Hook called after a UMAA CommandException failure. Override to clean up.

        :param command_exception: The exception raised during execute.
        :type command_exception: UmaaCommandException
        """
        pass

    def on_error(self, exception: Exception) -> None:
        """
        Hook called after an unexpected exception. Override to handle errors.

        :param exception: The exception caught during execute.
        :type exception: Exception
        """
        pass

    def on_terminal(self) -> None:
        """
        Hook called in the finally block of execute, always runs once per command.
        """
        pass

    def _send_ack(self) -> None:
        """
        Publish a command acknowledgement message with timestamp and session ID.
        """
        sample = self._ack_writer.topic.type()
        sample.timeStamp = Timestamp.now().to_umaa()
        sample.source = self._source_id
        sample.sessionID = self.command.sessionID
        sample.command = self.command
        self._ack_writer.write(sample)

    def _send_status(
        self,
        status: CmdStatus,
        reason: CmdReason,
        message: str,
    ) -> None:
        """
        Publish a status update message for the command lifecycle.

        :param status: Current command status enum.
        :type status: CmdStatus
        :param reason: Reason enum for status transition.
        :type reason: CmdReason
        :param message: Descriptive log message.
        :type message: str
        """
        # Log at appropriate level
        if status == CmdStatus.FAILED:
            self._logger.warning(message)
        else:
            self._logger.debug(message)

        status_sample = self._status_writer.topic.type()
        status_sample.timeStamp = Timestamp.now().to_umaa()
        status_sample.source = self._source_id
        status_sample.sessionID = self.command.sessionID
        status_sample.commandStatus = status
        status_sample.commandStatusReason = reason
        status_sample.logMessage = message
        self._status_writer.write(status_sample)


class UmaaCommandFactory:
    """
    Factory to construct UmaaCommand instances with pre-configured DDS writers.

    :param ack_type: DDS type for acknowledgements.
    :param status_type: DDS type for status updates.
    :param execution_status_type: Optional DDS type for execution status.
    :raises RuntimeError: If any provided type is invalid UMAA sample.
    """

    def __init__(
        self,
        ack_type: Type,
        status_type: Type,
        execution_status_type: Optional[Type] = None,
    ):
        # Placeholder for instance context
        self.source_id: Optional[UMAA_Common_IdentifierType] = None
        self.logger: Optional[logging.Logger] = None

        # Validate provided types
        if not validate_umaa_type(ack_type(), UMAAConcept.ACKNOWLEDGEMENT):
            raise RuntimeError(f"'{ack_type.__name__.split('_')[-1]}' is not a valid UMAA acknowledgement.")
        if not validate_umaa_type(status_type(), UMAAConcept.STATUS):
            raise RuntimeError(f"'{status_type.__name__.split('_')[-1]}' is not a valid UMAA status.")
        if execution_status_type and not validate_umaa_type(execution_status_type(), UMAAConcept.EXECUTION_STATUS):
            raise RuntimeError(f"'{execution_status_type.__name__.split('_')[-1]}' is not a valid UMAA exec status.")

        # Create DDS DataWriters for each sample type
        cfg = get_configurator()
        self._ack_writer: dds.DataWriter = cfg.get_writer(ack_type, profile_category=UmaaQosProfileCategory.COMMAND)
        self._status_writer: dds.DataWriter = cfg.get_writer(
            status_type, profile_category=UmaaQosProfileCategory.COMMAND
        )
        self._execution_status_writer: Optional[dds.DataWriter] = (
            cfg.get_writer(execution_status_type, profile_category=UmaaQosProfileCategory.COMMAND)
            if execution_status_type
            else None
        )

    def build(self, command: Any) -> UmaaCommand:
        """
        Build a new UmaaCommand instance bound to this factory's writers.

        :param command: DDS command data sample.
        :type command: Any
        :return: Configured UmaaCommand ready for execution.
        :rtype: UmaaCommand
        """
        self.logger.debug("Building UmaaCommand instance.")
        return UmaaCommand(
            self.source_id,
            command,
            self.logger,
            self._ack_writer,
            self._status_writer,
            self._execution_status_writer,
        )
