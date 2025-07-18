from typing import Any, Type, Optional, override, Callable, Tuple
import threading, time, logging

import rti.connextdds as dds

from umaapy.util.umaa_utils import validate_command, validate_ack, validate_status, validate_execution_status
from umaapy.util.dds_configurator import UmaaQosProfileCategory
from umaapy import configurator
from umaapy.util.timestamp import Timestamp
from umaapy.util.event_processor import Command

from umaapy.umaa_types import (
    UMAA_Common_IdentifierType,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CmdStatus,
    UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType as CmdReason,
)


class UmaaCommandException(Exception):
    def __init__(self, reason: CmdReason, message: str = ""):
        super().__init__(message)
        self.reason = reason
        self.message = message


class UmaaCommand(Command):
    def __init__(
        self,
        source: UMAA_Common_IdentifierType,
        command: Any,
        logger: logging.Logger,
        ack_writer: dds.DataWriter,
        status_writer: dds.DataWriter,
        execution_status_writer: Optional[dds.DataWriter] = None,
    ):
        self._logger: logging.Logger = logger
        self._source_id = source
        if not validate_command(command):
            raise RuntimeError(f"'{type(command).__name__.split("_")[-1]}' is not a valid UMAA command.")
        self.command: Any = command
        if not validate_ack(ack_writer.topic.type()):
            raise RuntimeError(
                f"'{ack_writer.topic.type.__name__.split("_")[-1]}' is not a valid UMAA command acknowledgement."
            )
        self._ack_writer: dds.DataWriter = ack_writer
        if not validate_status(status_writer.topic.type()):
            raise RuntimeError(f"'{status_writer.topic.type.__name__.split("_")[-1]}' is not a valid UMAA status.")
        self._status_writer: dds.DataWriter = status_writer
        if execution_status_writer and not validate_execution_status(execution_status_writer.topic.type()):
            raise RuntimeError(
                f"'{execution_status_writer.topic.type.__name__.split("_")[-1]}' is not a valid UMAA execution status."
            )
        self.execution_status_writer: Optional[dds.DataWriter] = execution_status_writer

        self._cancelled = False
        self._updated = False
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def update(self, new_command: Any):
        self._logger.debug("Command update received.")
        with self._condition:
            self.command = new_command
            self._updated = True
            self._condition.notify_all()

    def cancel(self):
        self._logger.debug("Command cancellation request received.")
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()

    def wait_for(self, predicate: Callable[[], bool], timeout: Optional[float] = None) -> Tuple[bool, bool]:
        end = time.monotonic() + timeout if timeout is not None else None
        with self._condition:
            while True:
                if self._cancelled:
                    return False, False
                if self._updated:
                    return False, True
                if predicate():
                    return True, False
                if end is not None:
                    time_left = end - time.monotonic()
                    if time_left < 0:
                        return False, False
                    self._condition.wait(time_left)
                else:
                    self._condition.wait()

    @override
    def execute(self, *args, **kwargs):
        try:
            self._send_ack()
            while True:
                with self._condition:
                    if self._updated:
                        self._send_status(CmdStatus.ISSUED, CmdReason.UPDATED, "Command updated.")
                    else:
                        self._send_status(CmdStatus.ISSUED, CmdReason.SUCCEEDED, "Command received.")
                    self._updated = False
                self._send_status(CmdStatus.COMMANDED, CmdReason.SUCCEEDED, "Command is being processed.")
                self.on_commanded()
                if self._cancelled:
                    self._send_status(CmdStatus.CANCELED, CmdReason.CANCELED, "Command has been canceled.")
                    break
                if self._updated:
                    continue
                self._send_status(CmdStatus.EXECUTING, CmdReason.SUCCEEDED, "Command is being executed.")
                self.on_executing()
                if self._cancelled:
                    self._send_status(CmdStatus.CANCELED, CmdReason.CANCELED, "Command has been canceled.")
                    break
                if self._updated:
                    continue
                self._send_status(CmdStatus.EXECUTING, CmdReason.SUCCEEDED, "Command has been completed.")
                self.on_complete()
                break
        except UmaaCommandException as uce:
            self._send_status(CmdStatus.FAILED, uce.reason, uce.message)
            self.on_failed(uce)
        except Exception as e:
            self._send_status(
                CmdStatus.FAILED,
                CmdReason.SERVICE_FAILED,
                f"{type(e).__name__.split("_")[-1]} provider caught while executing command - {str(e)}",
            )
            self.on_error(e)
        finally:
            self._logger.debug("Cleaning up command.")
            self.on_terminal()

    def on_commanded(self):
        pass

    def on_executing(self):
        raise UmaaCommandException(CmdReason.SERVICE_FAILED, "Required function on_executing not implemented")

    def on_complete(self):
        pass

    def on_failed(self, command_exception: UmaaCommandException):
        pass

    def on_error(self, exception: Exception):
        pass

    def on_terminal(self):
        pass

    def _send_ack(self):
        ack_type = self._ack_writer.topic.type()
        ack_type.timeStamp = Timestamp.now().to_umaa()
        ack_type.source = self._source_id
        ack_type.sessionID = self.command.sessionID
        ack_type.command = self.command
        self._ack_writer.write(ack_type)

    def _send_status(
        self,
        status: CmdStatus,
        reason: CmdReason,
        message: str,
    ):
        if status == CmdStatus.FAILED:
            self._logger.warning(message)
        else:
            self._logger.debug(message)

        status_type = self._status_writer.topic.type()
        status_type.timeStamp = Timestamp.now().to_umaa()
        status_type.source = self._source_id
        status_type.sessionID = self.command.sessionID
        status_type.commandStatus = status
        status_type.commandStatusReason = reason
        status_type.logMessage = message

        self._status_writer.write(status_type)


class UmaaCommandFactory:
    def __init__(
        self,
        ack_type: Type,
        ack_type_topic,
        status_type: Type,
        status_type_topic: str,
        execution_status_type: Optional[Type] = None,
        execution_status_type_topic: Optional[str] = None,
    ):
        self.source_id = None
        self.logger = None
        if not validate_ack(ack_type()):
            raise RuntimeError(f"'{ack_type.__name__.split("_")[-1]}' is not a valid UMAA command acknowledgement.")
        if not validate_status(status_type()):
            raise RuntimeError(f"'{status_type.__name__.split("_")[-1]}' is not a valid UMAA status.")
        if execution_status_type and not validate_execution_status(execution_status_type()):
            raise RuntimeError(
                f"'{execution_status_type.__name__.split("_")[-1]}' is not a valid UMAA execution status."
            )

        self._ack_writer: dds.DataWriter = configurator.get_writer(
            ack_type, ack_type_topic, UmaaQosProfileCategory.COMMAND
        )
        self._status_writer: dds.DataWriter = configurator.get_writer(
            status_type, status_type_topic, UmaaQosProfileCategory.COMMAND
        )
        self._execution_status_writer: dds.DataWriter = (
            configurator.get_writer(execution_status_type, execution_status_type_topic, UmaaQosProfileCategory.COMMAND)
            if execution_status_type is not None
            else None
        )

    def build(self, command: Any) -> UmaaCommand:
        self._logger.debug("Default UmaaCommandFactory.build method called - returning default UmaaCommand instance.")
        return UmaaCommand(
            self.source_id, self.logger, command, self._ack_writer, self._status_writer, self._execution_status_writer
        )
