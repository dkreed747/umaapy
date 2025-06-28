from typing import Any, Type, Callable, Union, Dict, Tuple, override, Optional
from uuid import UUID
import logging, threading, time
import rti.connextdds as dds

from umaapy.util.event_processor import EventProcessor, Command
from umaapy.util.dds_configurator import UmaaQosProfileCategory, WriterListenerEventType
from umaapy import event_processor, configurator
from umaapy.util.timestamp import Timestamp

from umaapy.types import (
    UMAA_Common_IdentifierType,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CmdStatus,
    UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType as CmdReason,
)


class UMAACommandException(Exception):
    def __init__(self, reason: CmdReason, message: str = ""):
        super().__init__(message)
        self.reason = reason
        self.message = message


class UMAACommand(Command):
    def __init__(
        self,
        logger: logging.Logger,
        source: UMAA_Common_IdentifierType,
        command: Any,
        ack_writer: dds.DataWriter,
        status_writer: dds.DataWriter,
        execution_status_writer: Optional[dds.DataWriter] = None,
    ):
        self._logger: logging.Logger = logger
        self._source_id = source
        self.command: Any = command
        self._ack_writer: dds.DataWriter = ack_writer
        self._status_writer: dds.DataWriter = status_writer
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
                    self._updated = False
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
    def execute(self):
        self._send_ack()
        try:
            while True:
                with self._condition:
                    if self._updated:
                        self._send_status(CmdStatus.ISSUED, CmdReason.SUCCEEDED, "Command update received.")
                    else:
                        self._send_status(CmdStatus.ISSUED, CmdReason.SUCCEEDED, "Command received.")
                    self._updated = False
                self._send_status(CmdStatus.COMMANDED, CmdReason.SUCCEEDED, "Command is being processed.")
                self.on_commanded()
                if self._cancelled:
                    break
                if self._updated:
                    continue
                self._send_status(CmdStatus.EXECUTING, CmdReason.SUCCEEDED, "Command is being executed.")
                self.on_executing()
                if self._cancelled:
                    break
                if self._updated:
                    continue
                self._send_status(CmdStatus.EXECUTING, CmdReason.SUCCEEDED, "Command has been completed.")
                self.on_complete()
                break
        except UMAACommandException as uce:
            self._send_status(CmdStatus.FAILED, uce.reason, uce.message)
            self.on_failed(uce)
        except Exception as e:
            self._send_status(
                CmdStatus.FAILED,
                CmdReason.SERVICE_FAILED,
                f"{type(e).__name__} caught while executing command - {str(e)}",
            )
            self.on_error(e)
        finally:
            self._logger.debug("Cleaning up command.")
            self.on_terminal()

    def on_commanded(self):
        pass

    def on_executing(self):
        raise UMAACommandException(CmdReason.SERVICE_FAILED, "Required function on_executing not implemented")

    def on_complete(self):
        pass

    def on_failed(self, command_exception: UMAACommandException):
        pass

    def on_error(self, exception: Exception):
        pass

    def on_terminal(self):
        pass

    def _send_ack(self):
        ack_type = self._ack_writer.topic.type()
        if not hasattr(ack_type, "timeStamp"):
            self._logger.error(f"Ack type '{type(ack_type).__name__}' does not have expected field 'timeStamp'")
            return

        ack_type.timeStamp = Timestamp.now().to_umaa()
        if not hasattr(ack_type, "source"):
            self._logger.error(f"Ack type '{type(ack_type).__name__}' does not have expected field 'source'")
            return

        ack_type.source = self._source_id
        if not hasattr(ack_type, "sessionID"):
            self._logger.error(f"Ack type '{type(ack_type).__name__}' does not have expected field 'sessionID'")
            return

        ack_type.sessionID = self.command.sessionID
        if not hasattr(ack_type, "command"):
            self._logger.error(f"Ack type '{type(ack_type).__name__}' does not have expected field 'command'")
            return

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
        if not hasattr(status_type, "timeStamp"):
            self._logger.error(f"Status type '{type(status_type).__name__}' does not have expected field 'timeStamp'")
            return

        status_type.timeStamp = Timestamp.now().to_umaa()
        if not hasattr(status_type, "source"):
            self._logger.error(f"Status type '{type(status_type).__name__}' does not have expected field 'source'")
            return

        status_type.source = self._source_id
        if not hasattr(status_type, "sessionID"):
            self._logger.error(f"Status type '{type(status_type).__name__}' does not have expected field 'sessionID'")
            return

        status_type.sessionID = self.command.sessionID
        if not hasattr(status_type, "commandStatus"):
            self._logger.error(
                f"Status type '{type(status_type).__name__}' does not have expected field 'commandStatus'"
            )
            return

        status_type.commandStatus = status
        if not hasattr(status_type, "commandStatusReason"):
            self._logger.error(
                f"Status type '{type(status_type).__name__}' does not have expected field 'commandStatusReason'"
            )
            return

        status_type.commandStatusReason = reason
        if not hasattr(status_type, "logMessage"):
            self._logger.error(f"Status type '{type(status_type).__name__}' does not have expected field 'logMessage'")
            return
        status_type.logMessage = message
        self._status_writer.write(status_type)


class CommandProvider(dds.DataReaderListener):
    def __init__(
        self,
        source: UMAA_Common_IdentifierType,
        cmd_type: Any,
        cmd_type_topic: str,
        ack_type: Any,
        ack_type_topic,
        status_type: Any,
        status_type_topic: str,
        execution_status_type: Optional[Any] = None,
        execution_status_type_topic: Optional[str] = None,
    ):
        super().__init__()
        self._source_id: UMAA_Common_IdentifierType = source
        self._cmd_type: Any = cmd_type
        self._ack_type: Any = ack_type
        self._status_type: Any = status_type
        self._execution_status_type: Any = execution_status_type

        self._pool: EventProcessor = event_processor
        self._cmd_reader: dds.DataReader = configurator.get_reader(
            cmd_type, cmd_type_topic, UmaaQosProfileCategory.COMMAND
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

        self.name = self._cmd_type.__name__.split("CommandType")[0].split("_")[-1] + self.__class__.__name__
        self._logger = logging.getLogger(f"{self.name}")
        self._logger.info(f"Initialized {self.name}...")
        self._cmd_reader.set_listener(self, dds.StatusMask.ALL)

    def on_data_available(self, reader: dds.DataReader):
        # Content filtering if needed for destination
        # Build UMAA Command type - need to pass in a factory?
        # Hold onto command type and update/cancel as needed
        pass

    def on_liveliness_changed(self, reader: dds.DataReader, status: dds.LivelinessChangedStatus):
        self._logger.trace("On liveliness changed triggered")
        pass

    def on_requested_deadline_missed(self, reader: dds.DataReader, status: dds.RequestedDeadlineMissedStatus):
        self._logger.trace("On requested deadline missed triggered")
        pass

    def on_requested_incompatible_qos(self, reader: dds.DataReader, status: dds.RequestedIncompatibleQosStatus):
        self._logger.trace("On requested incompatible qos triggered")
        pass

    def on_sample_lost(self, reader: dds.DataReader, status: dds.SampleLostStatus):
        self._logger.trace("On sample lost triggered")
        pass

    def on_sample_rejected(self, reader: dds.DataReader, status: dds.SampleRejectedStatus):
        self._logger.trace("On sample rejected triggered")
        pass

    def on_subscription_matched(self, reader: dds.DataReader, status: dds.SubscriptionMatchedStatus):
        self._logger.trace("On subscription matched triggered")
        pass
