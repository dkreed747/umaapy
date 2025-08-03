from __future__ import annotations

from typing import Optional, Any, Type, Dict, Tuple, List, override, TYPE_CHECKING
import logging
from threading import Condition
import re
from uuid import UUID

import rti.connextdds as dds

from umaapy.util.provider import Provider
from umaapy.util.command_session import CommandSession
from umaapy.util.reader_listener import ReaderListener
from umaapy import get_configurator, get_event_processor
from umaapy.util.umaa_utils import (
    UMAAConcept,
    validate_umaa_type,
    HashableNumericGUID,
    HashableIdentifierType,
)
from umaapy.util.dds_configurator import UmaaQosProfileCategory
from umaapy.util.event_processor import LOW
from umaapy.util.uuid_factory import guid_from_uuid, guid_to_hex


from umaapy.umaa_types import (
    UMAA_Common_IdentifierType as IdentifierType,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CommandStatusEnumType,
    UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType as CommandStatusReasonEnumType,
)

if TYPE_CHECKING:
    from umaapy.util.provider import Provider

_LIT_RE = re.compile(r"(?P<field>\w+(?:\.\w+)*)\s*=\s*&hex\(\s*" r"([0-9A-Fa-f]{2}(?:\s+[0-9A-Fa-f]{2}){15})\s*\)")
_PH_RE = re.compile(r"(?P<field>\w+(?:\.\w+)*)\s*=\s*&hex\(\s*%(\d+)\s*\)")
_ARR_RE = re.compile(r"(?P<field>\w+(?:\.\w+)*)\[(\d+)\]\s*=\s*%(\d+)")

"""
.. module:: command_consumer
   :synopsis: UMAA command consumer; sends commands and listens for ack/status/execution_status.

This module implements the CommandConsumer, which manages DDS writers and
filtered readers to send UMAA commands to providers and track their lifecycle.
"""


class CommandConsumer(dds.DataWriterListener):
    """
    Listens for publication matches and manages command sessions.

    :param source_id: Identifier of this consumer
    :type source_id: IdentifierType
    :param command_type: DDS data type for commands
    :type command_type: Type
    :param ack_type: DDS data type for acknowledgements
    :type ack_type: Type
    :param status_type: DDS data type for status messages
    :type status_type: Type
    :param execution_status_type: DDS data type for execution status messages, optional
    :type execution_status_type: Optional[Type]
    :raises RuntimeError: if any of the provided types fail UMAA validation
    """

    def __init__(
        self,
        source_id: IdentifierType,
        command_type: Type,
        ack_type: Type,
        status_type: Type,
        execution_status_type: Optional[Type] = None,
    ):
        """
        Initialize the CommandConsumer by creating a writer and three filtered readers.

        :param source_id: Identifier of this consumer
        :type source_id: IdentifierType
        :param command_type: DDS data type for commands
        :type command_type: Type
        :param ack_type: DDS data type for acknowledgements
        :type ack_type: Type
        :param status_type: DDS data type for status messages
        :type status_type: Type
        :param execution_status_type: DDS type for execution status, optional
        :type execution_status_type: Optional[Type]
        :raises RuntimeError: if any of the provided types fail UMAA validation
        """
        super().__init__()
        self.source_id: IdentifierType = source_id

        if not validate_umaa_type(command_type(), UMAAConcept.COMMAND):
            raise RuntimeError(f"'{command_type.__name__.split('_')[-1]}' is not a valid UMAA command.")
        if not validate_umaa_type(ack_type(), UMAAConcept.ACKNOWLEDGEMENT):
            raise RuntimeError(f"'{ack_type.__name__.split('_')[-1]}' is not a valid UMAA acknowledgement.")
        if not validate_umaa_type(status_type(), UMAAConcept.STATUS):
            raise RuntimeError(f"'{status_type.__name__.split('_')[-1]}' is not a valid UMAA status.")
        if execution_status_type and not validate_umaa_type(execution_status_type(), UMAAConcept.EXECUTION_STATUS):
            raise RuntimeError(f"'{execution_status_type.__name__.split('_')[-1]}' is not a valid UMAA exec status.")

        self.name = command_type.__name__.split("CommandType")[0].split("_")[-1] + self.__class__.__name__
        self._logger = logging.getLogger(f"{self.name}")

        self._writer = get_configurator().get_writer(command_type, profile_category=UmaaQosProfileCategory.COMMAND)
        (self._ack_reader, self._ack_cft) = get_configurator().get_filtered_reader(
            ack_type, "1 = 0", profile_category=UmaaQosProfileCategory.COMMAND
        )
        (self._status_reader, self._status_cft) = get_configurator().get_filtered_reader(
            status_type, "1 = 0", profile_category=UmaaQosProfileCategory.COMMAND
        )
        (self._execution_status_reader, self._execution_status_cft) = (
            get_configurator().get_filtered_reader(
                execution_status_type, "1 = 0", profile_category=UmaaQosProfileCategory.COMMAND
            )
            if execution_status_type
            else (None, None)
        )

        self._reader_filter: dds.Filter = dds.Filter("1 = 0")
        self._consumer_lock = Condition()

        self._providers_by_source: Dict[HashableIdentifierType, Provider] = {}
        self._providers_by_handle: Dict[dds.InstanceHandle, Provider] = {}
        self._providers_by_session: Dict[HashableNumericGUID, Provider] = {}

        self._writer.set_listener(self, dds.StatusMask.PUBLICATION_MATCHED)

        self._ack_reader.set_listener(ReaderListener(self._on_ack), dds.StatusMask.DATA_AVAILABLE)
        self._status_reader.set_listener(ReaderListener(self._on_status), dds.StatusMask.DATA_AVAILABLE)
        if self._execution_status_reader:
            self._execution_status_reader.set_listener(
                ReaderListener(self._on_execution_status), dds.StatusMask.DATA_AVAILABLE
            )
        self._logger.info(f"Initialized {self.name}")

    def add_provider(self, source_id: IdentifierType, name: Optional[str] = None) -> Provider:
        """
        Register or replace a provider by its source ID.

        :param source_id: The provider's IdentifierType
        :type source_id: IdentifierType
        :param name: Optional human-readable name
        :type name: Optional[str]
        :return: The newly registered Provider
        :rtype: Provider
        """
        pid = HashableIdentifierType(source_id)
        provider = Provider(source=pid, name=name)
        self._providers_by_source[pid] = provider
        return provider

    def remove_provider(self, source_id: IdentifierType) -> None:
        """
        Unregister a provider, cancel any non-terminal sessions, and clean up.

        :param source_id: The provider's IdentifierType to remove
        :type source_id: IdentifierType
        """
        provider = self._providers_by_source.pop(HashableIdentifierType(source_id), None)
        if not provider:
            return

        if provider.reader_handle in self._providers_by_handle:
            del self._providers_by_handle[provider.reader_handle]

        for session_id, session in provider.sessions.items():
            session.cancel()
        self._providers_by_session.pop(session_id, None)

    def get_providers(self) -> List[Provider]:
        return list(self._providers_by_source.values())

    def create_command_session(self, command: Any, provider_id: IdentifierType) -> Optional[CommandSession]:
        """
        Create a new CommandSession for the given provider.

        :param command: A pre-populated command instance
        :type command: Any
        :param provider_id: Identifier of the target provider
        :type provider_id: IdentifierType
        :return: A new CommandSession, or None if provider is unknown
        :rtype: Optional[CommandSession]
        """
        provider: Optional[Provider] = self._providers_by_source.get(HashableIdentifierType(provider_id), None)
        if provider is None:
            self._logger.warning(
                "Unable to make a command session for unknown provider: {provider_id} - did you forget to add a provider?"
            )
            return None

        command.source = self.source_id
        command.destination = provider_id
        session = CommandSession(self, command)
        provider.sessions[session._session_id] = session
        self._providers_by_session[session._session_id] = provider
        self._update_content_filters()
        self._logger.debug(f"New command session created.")
        return session

    def free_command_session(self, session: CommandSession) -> None:
        """
        Free up resources for a finished CommandSession.

        :param session: Session to free
        :type session: CommandSession
        """
        provider = self._providers_by_session.pop(session._session_id, None)
        if provider is not None:
            provider.sessions.pop(session._session_id)
        self._update_content_filters()

    def execute(
        self, command: Any, provider_id: IdentifierType, timeout: float = 10.0, wait_for_terminal: bool = True
    ) -> Tuple[bool, Optional[CommandStatusEnumType], Optional[CommandStatusReasonEnumType], Optional[str]]:
        """
        Send a single‐shot command and wait for its outcome.

        :param command: Command instance to send
        :type command: Any
        :param provider_id: Target provider’s IdentifierType
        :type provider_id: IdentifierType
        :param timeout: Seconds to wait before giving up
        :type timeout: float
        :param wait_for_terminal: If True, wait until session is in a terminal state
        :type wait_for_terminal: bool
        :returns: Tuple of (ok, status, reason, message)
        :rtype: Tuple[bool, Optional[CommandStatusEnumType], Optional[CommandStatusReasonEnumType], Optional[str]]
        """
        session = self.create_command_session(command, provider_id)
        self._logger.debug(f"Executing single-shot command on {self._providers_by_session[session._session_id]}")
        result = session.execute(timeout, wait_for_terminal)
        session.cancel()
        return result

    @override
    def on_publication_matched(self, writer: dds.DataWriter, status: dds.PublicationMatchedStatus):
        """
        Handle DDS PUBLICATION_MATCHED events to discover new providers or detect departures.

        :param writer: The DataWriter whose subscriptions changed
        :type writer: dds.DataWriter
        :param status: Status object containing the new subscription handles
        :type status: dds.PublicationMatchedStatus
        """
        try:
            current_handles = set(writer.matched_subscriptions)

            for handle, provider in self._providers_by_handle.items():
                if handle not in current_handles:
                    self._logger.info(f"{provider} has left the domain - scheduling provider to be removed")
                    get_event_processor().submit(lambda: self.remove_provider(provider.source.to_umaa()), priority=LOW)

            for handle in current_handles:
                if handle not in self._providers_by_handle:
                    sub_data = writer.matched_subscription_data(handle)
                    cfp = getattr(sub_data, "content_filter_property", None)

                    if cfp is None:
                        self._logger.debug(f"{handle} is not using a content filter - unable to deduce source ID")
                        continue

                    filter_expression = getattr(cfp, "filter_expression", None)
                    filter_parameters = getattr(cfp, "filter_parameters", [])

                    if filter_expression is None:
                        self._logger.debug(
                            f"Content filtered topic is missing expression property - unable to deduce source ID"
                        )
                        continue

                    provider_source_id = self._parse_source_from_filter(
                        filter_expression,
                        filter_parameters,
                    )

                    if provider_source_id is None:
                        self._logger.warning(
                            f"Unable to deduce provider source ID for {handle} from (filter_expression={sub_data.ContentFilteredTopic.filter_expression}, filter_parameters{sub_data.ContentFilteredTopic.filter_parameters})"
                        )
                        continue

                    provider: Optional[Provider] = self._providers_by_source.get(
                        HashableIdentifierType(provider_source_id), None
                    )

                    if provider:
                        provider.reader_handle = handle
                        self._providers_by_handle[handle] = provider
                        self._logger.info("Discovered new subscription that matches user registered provider")
                    else:
                        provider = self.add_provider(provider_source_id, f"Provider-{handle}")
                        provider.reader_handle = handle
                        self._providers_by_handle[handle] = provider
                        self._logger.info(
                            f"Discovered new subscription that does not match any current providers - implicitly creating"
                        )

        except Exception as e:
            self._logger.error(f"Error in on_publication_matched - {e}")

    def _on_ack(self, reader: dds.DataReader):
        for ack in reader.take_data():
            provider: Optional[Provider] = self._providers_by_source.get(HashableIdentifierType(ack.source), None)
            if provider is None:
                self._logger.debug(f"Ack received from unknown provider with ID: {ack.source}")
                continue

            session: Optional[CommandSession] = provider.sessions.get(HashableNumericGUID(ack.sessionID), None)

            if session is None:
                self._logger.debug(f"Ack received references an unknown session on {provider}")

            session._handle_ack(ack)

    def _on_status(self, reader: dds.DataReader):
        for status in reader.take_data():
            provider: Optional[Provider] = self._providers_by_source.get(HashableIdentifierType(status.source), None)
            if provider is None:
                self._logger.debug(f"Status received from unknown provider with ID: {status.source}")
                continue

            session: Optional[CommandSession] = provider.sessions.get(HashableNumericGUID(status.sessionID), None)

            if session is None:
                self._logger.debug(f"Status received references an unknown session on {provider}")

            session._handle_status(status)

    def _on_execution_status(self, reader: dds.DataReader):
        for execution_status in reader.take_data():
            provider: Optional[Provider] = self._providers_by_source.get(
                HashableIdentifierType(execution_status.source), None
            )
            if provider is None:
                self._logger.debug(
                    f"Execution status received from unknown provider with ID: {execution_status.source}"
                )
                continue

            session: Optional[CommandSession] = provider.sessions.get(
                HashableNumericGUID(execution_status.sessionID), None
            )

            if session is None:
                self._logger.debug(f"Execution status received references an unknown session on {provider}")

            session._handle_status(execution_status)

    def _update_content_filters(self):
        filter_components = []
        for pid, provider in self._providers_by_source.items():
            prefix = f"source.parentID = &hex({guid_to_hex(pid.parentID)}) AND source.id = &hex({guid_to_hex(pid.id)})"
            for sid in provider.sessions:
                filter_components.append(f"{prefix} AND sessionID = &hex({guid_to_hex(sid.to_umaa())})")

        self._logger.debug(
            f"Updating content filter expression for {len(filter_components)} sessions across {len(self._providers_by_source)} providers."
        )
        reader_filter: dds.Filter = dds.Filter(" OR ".join(filter_components) if len(filter_components) else "1 = 0")
        self._ack_cft.set_filter(reader_filter)
        self._status_cft.set_filter(reader_filter)
        if self._execution_status_cft is not None:
            self._execution_status_cft.set_filter(reader_filter)

    def _parse_source_from_filter(
        self, filter_expression: str, filter_parameters: List[str]
    ) -> Optional[IdentifierType]:
        """
        Extract the provider's source ID from a content filter expression.

        :param expr: The filter expression (e.g. "source = &hex(...)")
        :type expr: str
        :param params: The filter parameters
        :type params: List[str]
        :returns: Parsed IdentifierType or None
        :rtype: Optional[IdentifierType]
        """
        # 1 literal hex in the expr
        guids: Dict[str, UUID] = {}
        for m in _LIT_RE.finditer(filter_expression):
            fld = m.group("field").split(".")[-1]
            if fld in ("parentID", "id"):
                hexstr = m.group(2).replace(" ", "")
                try:
                    guids[fld] = UUID(hex=hexstr)
                except ValueError:
                    return None
        if "parentID" in guids and "id" in guids:
            return IdentifierType(parentID=guid_from_uuid(guids["parentID"]), id=guid_from_uuid(guids["id"]))

        # 2 &hex(%N) placeholders
        guids.clear()
        for m in _PH_RE.finditer(filter_expression):
            fld = m.group("field").split(".")[-1]
            if fld in ("parentID", "id"):
                idx = int(m.group(2))
                if idx < 0 or idx >= len(filter_parameters):
                    return None
                hexstr = filter_parameters[idx].replace(" ", "")
                try:
                    guids[fld] = UUID(hex=hexstr)
                except ValueError:
                    return None
        if "parentID" in guids and "id" in guids:
            return IdentifierType(parentID=guid_from_uuid(guids["parentID"]), id=guid_from_uuid(guids["id"]))

        # 3 array-indexed placeholders: field[i] = %N
        temp: dict[str, dict[int, int]] = {}
        for m in _ARR_RE.finditer(filter_expression):
            fld = m.group("field").split(".")[-1]
            if fld in ("parentID", "id"):
                byte_i = int(m.group(2))
                param_i = int(m.group(3))
                temp.setdefault(fld, {})[byte_i] = param_i

        # assemble GUIDs from 16 bytes each
        guids.clear()
        for fld, mapping in temp.items():
            if set(mapping.keys()) != set(range(16)):
                continue
            try:
                hex_bytes = [f"{int(filter_parameters[mapping[i]]):02x}" for i in range(16)]
            except (IndexError, ValueError):
                return None
            hexstr = "".join(hex_bytes)
            try:
                guids[fld] = UUID(hex=hexstr)
            except ValueError:
                return None

        if "parentID" in guids and "id" in guids:
            return IdentifierType(parentID=guid_from_uuid(guids["parentID"]), id=guid_from_uuid(guids["id"]))

        return None
