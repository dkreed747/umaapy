from typing import Any, Type, Callable, Union, Dict, List
from uuid import UUID
import logging
import rti.connextdds as dds

from umaapy.util.event_processor import Command, MEDIUM
from umaapy.util.dds_configurator import UmaaQosProfileCategory, WriterListenerEventType
from umaapy import get_event_processor, get_configurator
from umaapy.util.timestamp import Timestamp
from umaapy.util.umaa_utils import UMAAConcept, validate_umaa_type

from umaapy.umaa_types import UMAA_Common_IdentifierType


class ReportProvider(dds.DataWriterListener):
    """
    DDS DataWriterListener for publishing UMAA reports and handling writer-status callbacks.

    This class wraps a DDS DataWriter configured for UMAA reports, stamps each sample
    with a source identifier and timestamp, and provides hooks for writer-related events.

    :param source: UMAA identifier to assign to outgoing reports for filtering.
    :type source: UMAA_Common_IdentifierType
    :param data_type: The DDS report type class (e.g., SomeReportType).
    :type data_type: Type
    :param report_priority: Priority for dispatching callbacks in the UMAA EventProcessor.
    :type report_priority: int
    :raises RuntimeError: If the provided data_type is not a valid UMAA report.
    """

    def __init__(
        self,
        source: UMAA_Common_IdentifierType,
        data_type: Type,
        report_priority: int = MEDIUM,
    ):
        super().__init__()
        # Validate that the data type adheres to UMAA report specifications
        if not validate_umaa_type(data_type(), UMAAConcept.REPORT):
            raise RuntimeError(f"'{data_type.__name__.split('_')[-1]}' is not a valid UMAA report.")
        # Store configuration
        self._source_id: UMAA_Common_IdentifierType = source
        self._data_type: Type = data_type
        self._report_priority: int = report_priority

        # Obtain a DDS DataWriter for UMAA reports with the proper QoS profile
        self._writer: dds.DataWriter = get_configurator().get_writer(
            self._data_type,
            profile_category=UmaaQosProfileCategory.REPORT,
        )
        # Prepare storage for writer event callbacks
        self._callbacks: Dict[WriterListenerEventType, List[Union[Callable[..., None], Command]]] = {
            evt: [] for evt in WriterListenerEventType
        }

        # Setup a descriptive logger name
        self.name = self._data_type.__name__.split("ReportType")[0].split("_")[-1] + self.__class__.__name__
        self._logger: logging.Logger = logging.getLogger(self.name)
        self._logger.info(f"Initialized {self.name}...")

        # Attach this listener to the DDS writer for all status mask events
        self._writer.set_listener(self, dds.StatusMask.ALL)

    def publish(self, sample: Any) -> None:
        """
        Publish a UMAA report by stamping it with source ID and timestamp, then writing.

        :param sample: The report instance to publish over DDS.
        :type sample: Any
        """
        # Assign the source ID so consumers can filter by this provider
        sample.source = self._source_id
        # Assign the current timestamp in UMAA format
        sample.timeStamp = Timestamp.now().to_umaa()
        self._logger.debug("Writing sample")
        # Write the sample to the DDS network
        self._writer.write(sample)

    def dispose(self) -> None:
        """
        Dispose of the current DDS instance to signal end-of-life of this report.

        Looks up the instance by key and issues a dispose if it exists.
        """
        # Create a key-only sample with the same source to lookup
        key_holder = self._data_type()
        key_holder.source = self._source_id
        # Retrieve the DDS instance handle
        ih = self._writer.lookup_instance(key_holder)
        if ih != dds.InstanceHandle.nil:
            self._logger.debug(f"Disposing {self._data_type.__name__} on shutdown...")
            # Issue a dispose to remove the instance from DDS
            self._writer.dispose_instance(ih)
        else:
            self._logger.debug("No instance to dispose - doing nothing.")

    def add_event_callback(
        self,
        event: WriterListenerEventType,
        callback: Union[Callable[..., None], Command],
    ) -> None:
        """
        Register a callback or UMAA Command for a writer-status event.

        :param event: The WriterListenerEventType to listen for.
        :type event: WriterListenerEventType
        :param callback: A callable or UMAA Command to invoke when the event occurs.
        :type callback: Union[Callable[..., None], Command]
        """
        self._callbacks[event].append(callback)

    def remove_event_callback(
        self,
        event: WriterListenerEventType,
        callback: Union[Callable[..., None], Command],
    ) -> None:
        """
        Unregister a previously added writer-status event callback.

        :param event: The WriterListenerEventType to stop listening for.
        :type event: WriterListenerEventType
        :param callback: The callback or UMAA Command to remove.
        :type callback: Union[Callable[..., None], Command]
        """
        self._callbacks[event].remove(callback)

    def on_application_acknowledgment(
        self,
        writer: dds.DataWriter,
        ack_info: dds.AcknowledgmentInfo,
    ):
        """
        DDS callback when an application acknowledgment is received for a written sample.
        """
        self._logger.debug("On application acknowledgement triggered")
        self._dispatch(
            WriterListenerEventType.ON_APPLICATION_ACKNOWLEDGMENT,
            writer=writer,
            ack_info=ack_info,
        )

    def on_instance_replaced(
        self,
        writer: dds.DataWriter,
        instance: dds.InstanceHandle,
    ):
        """
        DDS callback when a writer instance is replaced.
        """
        self._logger.debug("On instance replaced triggered")
        self._dispatch(
            WriterListenerEventType.ON_INSTANCE_REPLACED,
            writer=writer,
            instance=instance,
        )

    def on_liveliness_lost(
        self,
        writer: dds.DataWriter,
        status: dds.LivelinessLostStatus,
    ):
        """
        DDS callback when liveliness is lost for a writer.
        """
        self._logger.debug("On liveliness lost triggered")
        self._dispatch(
            WriterListenerEventType.ON_LIVELINESS_LOST,
            writer=writer,
            status=status,
        )

    def on_offered_deadline_missed(
        self,
        writer: dds.DataWriter,
        status: dds.OfferedDeadlineMissedStatus,
    ):
        """
        DDS callback when the writer misses an offered deadline.
        """
        self._logger.debug("On offered deadline missed triggered")
        self._dispatch(
            WriterListenerEventType.ON_OFFERED_DEADLINE_MISSED,
            writer=writer,
            status=status,
        )

    def on_offered_incompatible_qos(
        self,
        writer: dds.DataWriter,
        status: dds.OfferedIncompatibleQosStatus,
    ):
        """
        DDS callback when offered QoS is incompatible.
        """
        self._logger.debug("On offered incompatible qos triggered")
        self._dispatch(
            WriterListenerEventType.ON_OFFERED_INCOMPATIBLE_QOS,
            writer=writer,
            status=status,
        )

    def on_publication_matched(
        self,
        writer: dds.DataWriter,
        status: dds.PublicationMatchedStatus,
    ):
        """
        DDS callback when a publication matches or unmatches a subscription.
        """
        self._logger.debug("On publication matched triggered")
        self._dispatch(
            WriterListenerEventType.ON_PUBLICATION_MATCHED,
            writer=writer,
            status=status,
        )

    def on_reliable_reader_activity_changed(
        self,
        writer: dds.DataWriter,
        status: dds.ReliableReaderActivityChangedStatus,
    ):
        """
        DDS callback when reliable reader activity changes.
        """
        self._logger.debug("On reliable reader activity changed triggered")
        self._dispatch(
            WriterListenerEventType.ON_RELIABLE_READER_ACTIVITY_CHANGED,
            writer=writer,
            status=status,
        )

    def on_reliable_writer_cache_changed(
        self,
        writer: dds.DataWriter,
        status: dds.ReliableWriterCacheChangedStatus,
    ):
        """
        DDS callback when the reliable writer cache changes.
        """
        self._logger.debug("On reliable writer cache changed triggered")
        self._dispatch(
            WriterListenerEventType.ON_RELIABLE_WRITER_CACHE_CHANGED,
            writer=writer,
            status=status,
        )

    def on_service_request_accepted(
        self,
        writer: dds.DataWriter,
        status: dds.ServiceRequestAcceptedStatus,
    ):
        """
        DDS callback when a service request is accepted by the middleware.
        """
        self._logger.debug("On service request accepted triggered")
        self._dispatch(
            WriterListenerEventType.ON_SERVICE_REQUEST_ACCEPTED,
            writer=writer,
            status=status,
        )

    def _dispatch(
        self,
        event: WriterListenerEventType,
        *args,
        **kwargs,
    ) -> None:
        """
        Dispatch a writer-status event to all registered callbacks via the UMAA EventProcessor.

        :param event: The WriterListenerEventType that occurred.
        :type event: WriterListenerEventType
        :param args: Positional arguments to pass to each callback.
        :param kwargs: Keyword arguments to pass to each callback.
        """
        for cb in self._callbacks[event]:
            # Submit each callback to the UMAA event processor with configured priority
            get_event_processor().submit(cb, *args, priority=self._report_priority, **kwargs)
