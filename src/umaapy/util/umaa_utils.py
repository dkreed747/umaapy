from typing import Any, Type
import logging

_logger = logging.getLogger(__name__)


# Utility functions to validate UMAA DDS topic sample types and derive topic names.


def validate_command(command: Any) -> bool:
    """
    Validate that the given object has the required fields for a UMAA command sample.

    Checks for:
      - timeStamp
      - source
      - destination
      - sessionID

    :param command: An instance of a DDS command data type.
    :type command: Any
    :return: True if the object has all required fields, False otherwise.
    :rtype: bool
    """
    if not hasattr(command, "timeStamp"):
        _logger.error(f"'{type(command).__name__}' missing required 'timeStamp' field")
        return False

    if not hasattr(command, "source"):
        _logger.error(f"'{type(command).__name__}' missing required 'source' field")
        return False

    if not hasattr(command, "destination"):
        _logger.error(f"'{type(command).__name__}' missing required 'destination' field")
        return False

    if not hasattr(command, "sessionID"):
        _logger.error(f"'{type(command).__name__}' missing required 'sessionID' field")
        return False

    _logger.debug(f"'{type(command).__name__}' is a valid UMAA command type")
    return True


def validate_ack(ack: Any) -> bool:
    """
    Validate that the given object has the required fields for a UMAA command acknowledgement.

    Checks for:
      - timeStamp
      - source
      - sessionID
      - command

    :param ack: An instance of a DDS acknowledgement data type.
    :type ack: Any
    :return: True if the object has all required fields, False otherwise.
    :rtype: bool
    """
    if not hasattr(ack, "timeStamp"):
        _logger.error(f"'{type(ack).__name__}' missing required 'timeStamp' field")
        return False

    if not hasattr(ack, "source"):
        _logger.error(f"'{type(ack).__name__}' missing required 'source' field")
        return False

    if not hasattr(ack, "sessionID"):
        _logger.error(f"'{type(ack).__name__}' missing required 'sessionID' field")
        return False

    if not hasattr(ack, "command"):
        _logger.error(f"'{type(ack).__name__}' missing required 'command' field")
        return False

    _logger.debug(f"'{type(ack).__name__}' is a valid UMAA command acknowledgement type")
    return True


def validate_status(status: Any) -> bool:
    """
    Validate that the given object has the required fields for a UMAA command status update.

    Checks for:
      - timeStamp
      - source
      - sessionID
      - commandStatus
      - commandStatusReason
      - logMessage

    :param status: An instance of a DDS status update data type.
    :type status: Any
    :return: True if the object has all required fields, False otherwise.
    :rtype: bool
    """
    if not hasattr(status, "timeStamp"):
        _logger.error(f"'{type(status).__name__}' missing required 'timeStamp' field")
        return False

    if not hasattr(status, "source"):
        _logger.error(f"'{type(status).__name__}' missing required 'source' field")
        return False

    if not hasattr(status, "sessionID"):
        _logger.error(f"'{type(status).__name__}' missing required 'sessionID' field")
        return False

    if not hasattr(status, "commandStatus"):
        _logger.error(f"'{type(status).__name__}' missing required 'commandStatus' field")
        return False

    if not hasattr(status, "commandStatusReason"):
        _logger.error(f"'{type(status).__name__}' missing required 'commandStatusReason' field")
        return False

    if not hasattr(status, "logMessage"):
        _logger.error(f"'{type(status).__name__}' missing required 'logMessage' field")
        return False

    _logger.debug(f"'{type(status).__name__}' is a valid UMAA status type")
    return True


def validate_execution_status(execution_status: Any) -> bool:
    """
    Validate that the given object has the required fields for a UMAA execution status update.

    Checks for:
      - timeStamp
      - source
      - sessionID

    :param execution_status: An instance of a DDS execution status data type.
    :type execution_status: Any
    :return: True if the object has all required fields, False otherwise.
    :rtype: bool
    """
    if not hasattr(execution_status, "timeStamp"):
        _logger.error(f"'{type(execution_status).__name__}' missing required 'timeStamp' field")
        return False

    if not hasattr(execution_status, "source"):
        _logger.error(f"'{type(execution_status).__name__}' missing required 'source' field")
        return False

    if not hasattr(execution_status, "sessionID"):
        _logger.error(f"'{type(execution_status).__name__}' missing required 'sessionID' field")
        return False

    _logger.debug(f"'{type(execution_status).__name__}' is a valid UMAA execution status type")
    return True


def validate_report(report: Any) -> bool:
    """
    Validate that the given object has the required fields for a UMAA report sample.

    Checks for:
      - timeStamp
      - source

    :param report: An instance of a DDS report data type.
    :type report: Any
    :return: True if the object has all required fields, False otherwise.
    :rtype: bool
    """
    if not hasattr(report, "timeStamp"):
        _logger.error(f"'{type(report).__name__}' missing required 'timeStamp' field")
        return False

    if not hasattr(report, "source"):
        _logger.error(f"'{type(report).__name__}' missing required 'source' field")
        return False

    _logger.debug(f"'{type(report).__name__}' is a valid UMAA report type")
    return True


def topic_from_type(umaa_type: Type) -> str:
    """
    Derive a DDS topic name from a UMAA type class by replacing underscores with '::'.

    :param umaa_type: The UMAA DDS type class.
    :type umaa_type: Type
    :return: Topic name string used in DDS filters.
    :rtype: str
    """
    # Convert C++-style nested names to :: separators
    return umaa_type.__name__.replace("_", "::")
