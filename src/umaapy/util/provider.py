"""
.. module:: provider
   :synopsis: Tracks remote UMAA providers and their active command sessions.
"""

from __future__ import annotations

from typing import Optional, Dict, TYPE_CHECKING
from dataclasses import dataclass, field

from umaapy.dds_backend import dds

from umaapy.util.umaa_utils import HashableNumericGUID, HashableIdentifierType

if TYPE_CHECKING:
    from umaapy.util.command_session import CommandSession


@dataclass
class Provider:
    """
    Represents the consumer view of a UMAA service provider with its DDS instance handle and sessions.

    :param source: Unique identifier for the provider
    :type source: HashableIdentifierType
    :param name: Optional display name
    :type name: Optional[str]
    :param reader_handle: DDS instance handle for filtering
    :type reader_handle: Optional[dds.InstanceHandle]
    :param sessions: Active CommandSession objects keyed by session ID
    :type sessions: Dict[HashableNumericGUID, CommandSession]
    """

    source: HashableIdentifierType
    name: Optional[str] = None
    reader_handle: Optional[dds.InstanceHandle] = None
    sessions: Dict[HashableNumericGUID, CommandSession] = field(default_factory=dict)

    def __repr__(self) -> str:
        """
        Return a human-readable representation.

        :returns: e.g. "Provider(source=..., handle=..., active_sessions=3)"
        :rtype: str
        """
        name_str = self.name if self.name is not None else "Provider"
        source_str = str(self.source) if self.source is not None else "UNKNOWN"
        handle_str = str(self.reader_handle) if self.reader_handle is not None else "UNKNOWN"
        return f"{name_str}(source={source_str}, reader_handle={handle_str}, active_sessions={len(self.sessions)})"
