from typing import Any, Type
import logging

_logger = logging.getLogger(f"{__file__.split("/")[-1]}")


def validate_command(command: Any) -> bool:
    if not hasattr(command, "timeStamp"):
        _logger.error(f"'{type(command).__name__}' does not have expected field 'timeStamp'")
        return False

    if not hasattr(command, "source"):
        _logger.error(f"'{type(command).__name__}' does not have expected field 'source'")
        return False

    if not hasattr(command, "destination"):
        _logger.error(f"'{type(command).__name__}' does not have expected field 'destination'")
        return False

    if not hasattr(command, "sessionID"):
        _logger.error(f"'{type(command).__name__}' does not have expected field 'sessionID'")
        return False

    _logger.debug(f"'{type(command).__name__}' is a valid UMAA command type")
    return True


def validate_ack(ack: Any) -> bool:
    if not hasattr(ack, "timeStamp"):
        _logger.error(f"'{type(ack).__name__}' does not have expected field 'timeStamp'")
        return False

    if not hasattr(ack, "source"):
        _logger.error(f"'{type(ack).__name__}' does not have expected field 'source'")
        return False

    if not hasattr(ack, "sessionID"):
        _logger.error(f"'{type(ack).__name__}' does not have expected field 'sessionID'")
        return False

    if not hasattr(ack, "command"):
        _logger.error(f"'{type(ack).__name__}' does not have expected field 'command'")
        return False

    _logger.debug(f"'{type(ack).__name__}' is a valid UMAA command ack type")
    return True


def validate_status(status: Any) -> bool:
    if not hasattr(status, "timeStamp"):
        _logger.error(f"'{type(status).__name__}' does not have expected field 'timeStamp'")
        return False

    if not hasattr(status, "source"):
        _logger.error(f"'{type(status).__name__}' does not have expected field 'source'")
        return False

    if not hasattr(status, "sessionID"):
        _logger.error(f"'{type(status).__name__}' does not have expected field 'sessionID'")
        return False

    if not hasattr(status, "commandStatus"):
        _logger.error(f"'{type(status).__name__}' does not have expected field 'commandStatus'")
        return False

    if not hasattr(status, "commandStatusReason"):
        _logger.error(f"'{type(status).__name__}' does not have expected field 'commandStatusReason'")
        return False

    if not hasattr(status, "logMessage"):
        _logger.error(f"'{type(status).__name__}' does not have expected field 'logMessage'")
        return False

    _logger.debug(f"'{type(status).__name__}' is a valid UMAA status type")
    return True


def validate_execution_status(execution_status: Any) -> bool:
    if not hasattr(execution_status, "timeStamp"):
        _logger.error(f"'{type(execution_status).__name__}' does not have expected field 'timeStamp'")
        return False

    if not hasattr(execution_status, "source"):
        _logger.error(f"'{type(execution_status).__name__}' does not have expected field 'source'")
        return False

    if not hasattr(execution_status, "sessionID"):
        _logger.error(f"'{type(execution_status).__name__}' does not have expected field 'sessionID'")
        return False

    _logger.debug(f"'{type(execution_status).__name__}' is a valid UMAA execution status type")
    return True


def validate_report(report: Any) -> bool:
    if not hasattr(report, "timeStamp"):
        _logger.error(f"'{type(report).__name__}' does not have expected field 'timeStamp'")
        return False

    if not hasattr(report, "source"):
        _logger.error(f"'{type(report).__name__}' does not have expected field 'source'")
        return False

    _logger.debug(f"'{type(report).__name__}' is a valid UMAA report type")
    return True


def topic_from_type(umaa_type: Type) -> str:
    return umaa_type.__name__.replace("_", "::")
