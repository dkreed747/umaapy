from typing import Any, Type, Callable, Union, Dict, List, Optional, override
import logging
import rti.connextdds as dds

from umaapy.util.event_processor import EventProcessor, Command, MEDIUM
from umaapy.util.dds_configurator import ReaderListenerEventType
from umaapy import get_event_processor, get_configurator
from umaapy.util.umaa_utils import UMAAConcept, validate_umaa_type
from umaapy.util.uuid_factory import guid_to_hex

from umaapy.umaa_types import UMAA_Common_IdentifierType as IdentifierType


class ReportConsumer(dds.DataReaderListener):
    """
    A DDS DataReaderListener that listens for UMAA report topics, stores the latest report,
    and dispatches report and event callbacks via the UMAA EventProcessor.

    :param sources: List of UMAA identifier sources to filter incoming reports.
    :type sources: List[IdentifierType]
    :param report_type: The DDS report type class (e.g., SomeReportType).
    :type report_type: Type
    :param report_priority: Priority for dispatching report callbacks to the event processor.
    :type report_priority: int
    :raises RuntimeError: If the provided report_type is not a valid UMAA report.
    """

    def __init__(
        self,
        sources: List[IdentifierType],
        report_type: Type,
        report_priority: int = MEDIUM,
    ):
        super().__init__()
        # Ensure the report type adheres to UMAA specifications
        if not validate_umaa_type(report_type(), UMAAConcept.REPORT):
            raise RuntimeError(f"'{report_type.__name__.split('_')[-1]}' is not a valid UMAA report.")

        # Store configuration
        self._source_ids: List[IdentifierType] = sources
        self._report_type: Type = report_type
        self._report_priority: int = report_priority

        # Build a DDS filter to match any of the provided sources
        filter_expression = " OR ".join(
            [
                f"source.parentID = &hex({guid_to_hex(source.parentID)})"
                f" AND source.id = &hex({guid_to_hex(source.id)})"
                for source in sources
            ]
        )
        # Create a filtered DDS DataReader for the report type
        self._reader, _ = get_configurator().get_filtered_reader(report_type, filter_expression)

        # Store the most recent valid report sample
        self._latest_report: Optional[Any] = None

        # Registered callbacks for new reports
        self._report_callbacks: List[Union[Callable[[Optional[Any]], None], Command]] = []
        # Registered callbacks for other DDS listener events
        self._callbacks: Dict[ReaderListenerEventType, List[Union[Callable[..., None], Command]]] = {
            evt: [] for evt in ReaderListenerEventType
        }

        # Setup logger with a descriptive name
        self.name = self._report_type.__name__.split("ReportType")[0].split("_")[-1] + self.__class__.__name__
        self._logger: logging.Logger = logging.getLogger(self.name)

        # Attach this listener to the DDS reader for all status changes
        self._reader.set_listener(self, dds.StatusMask.ALL)
        self._logger.debug(f"Reader filter expression: {filter_expression}")
        self._logger.info(f"Initialized {self.name}...")

    def add_report_callback(self, callback: Union[Callable[[Optional[Any]], None], Command]) -> None:
        """
        Register a callback to be invoked when a new report arrives.

        :param callback: A callable taking the report (or None) or a UMAA Command.
        :type callback: Union[Callable[[Optional[Any]], None], Command]
        """
        self._report_callbacks.append(callback)

    def remove_report_callback(self, callback: Union[Callable[[Optional[Any]], None], Command]) -> None:
        """
        Unregister a previously added report callback.

        :param callback: The callback to remove.
        :type callback: Union[Callable[[Optional[Any]], None], Command]
        """
        self._report_callbacks.remove(callback)

    def add_event_callback(self, event: ReaderListenerEventType, callback: Union[Callable[..., None], Command]) -> None:
        """
        Register a callback for a specific DDS listener event.

        :param event: The ReaderListenerEventType to listen for.
        :type event: ReaderListenerEventType
        :param callback: A callable or UMAA Command to invoke.
        :type callback: Union[Callable[..., None], Command]
        """
        self._callbacks[event].append(callback)

    def remove_event_callback(
        self, event: ReaderListenerEventType, callback: Union[Callable[..., None], Command]
    ) -> None:
        """
        Unregister a previously added event callback for a DDS listener event.

        :param event: The ReaderListenerEventType to remove the callback from.
        :type event: ReaderListenerEventType
        :param callback: The callback to remove.
        :type callback: Union[Callable[..., None], Command]
        """
        self._callbacks[event].remove(callback)

    def get_latest_report(self) -> Optional[Any]:
        """
        Retrieve the most recently received valid report sample.

        :return: The latest report object or None if no valid report has been received.
        :rtype: Optional[Any]
        """
        return self._latest_report

    @override
    def on_data_available(self, reader: dds.DataReader):
        """
        DDS callback when new data arrives. Dispatches the ON_DATA_AVAILABLE event,
        then processes each sample, invoking report callbacks or signalling invalid samples.

        :param reader: The DDS DataReader triggering this callback.
        :type reader: dds.DataReader
        """
        self._logger.debug("On data available triggered")
        # Notify any registered event callbacks
        self._dispatch_event(ReaderListenerEventType.ON_DATA_AVAILABLE, reader)
        # Process incoming samples
        for data, info in reader.take():
            if info.valid:
                # Valid report sample: dispatch to callbacks
                self._dispatch_report(data)
            else:
                # Invalid or disposed sample: notify callbacks with None
                self._dispatch_report(None)

    @override
    def on_liveliness_changed(self, reader: dds.DataReader, status: dds.LivelinessChangedStatus):
        """
        DDS callback for liveliness changes. Dispatches the ON_LIVELINESS_CHANGED event.
        """
        self._logger.debug("On liveliness changed triggered")
        self._dispatch_event(ReaderListenerEventType.ON_LIVELINESS_CHANGED, reader, status)

    @override
    def on_requested_deadline_missed(self, reader: dds.DataReader, status: dds.RequestedDeadlineMissedStatus):
        """
        DDS callback for missed deadline events. Dispatches the ON_REQUESTED_DEADLINE_MISSED event.
        """
        self._logger.debug("On requested deadline missed triggered")
        self._dispatch_event(ReaderListenerEventType.ON_REQUESTED_DEADLINE_MISSED, reader, status)

    @override
    def on_requested_incompatible_qos(self, reader: dds.DataReader, status: dds.RequestedIncompatibleQosStatus):
        """
        DDS callback for incompatible QoS events. Dispatches the ON_REQUESTED_INCOMPATIBLE_QOS event.
        """
        self._logger.debug("On requested incompatible qos triggered")
        self._dispatch_event(ReaderListenerEventType.ON_REQUESTED_INCOMPATIBLE_QOS, reader, status)

    @override
    def on_sample_lost(self, reader: dds.DataReader, status: dds.SampleLostStatus):  # noqa
        """
        DDS callback for sample lost events. Dispatches the ON_SAMPLE_LOST event.
        """
        self._logger.debug("On sample lost triggered")
        self._dispatch_event(ReaderListenerEventType.ON_SAMPLE_LOST, reader, status)

    @override
    def on_sample_rejected(self, reader: dds.DataReader, status: dds.SampleRejectedStatus):
        """
        DDS callback for sample rejected events. Dispatches the ON_SAMPLE_REJECTED event.
        """
        self._logger.debug("On sample rejected triggered")
        self._dispatch_event(ReaderListenerEventType.ON_SAMPLE_REJECTED, reader, status)

    @override
    def on_subscription_matched(self, reader: dds.DataReader, status: dds.SubscriptionMatchedStatus):
        """
        DDS callback for subscription matched events. Dispatches the ON_SUBSCRIPTION_MATCHED event.
        """
        self._logger.debug("On subscription matched triggered")
        self._dispatch_event(ReaderListenerEventType.ON_SUBSCRIPTION_MATCHED, reader, status)

    def _dispatch_report(self, report: Optional[Any]) -> None:
        """
        Submit the provided report (or None) to all registered report callbacks via the event processor.

        :param report: The report data or None for invalid samples.
        :type report: Optional[Any]
        """
        # Update stored latest report if valid
        if report is not None:
            self._latest_report = report
        # Dispatch to each callback
        for cb in self._report_callbacks:
            get_event_processor().submit(cb, report, priority=self._report_priority)

    def _dispatch_event(self, event: ReaderListenerEventType, *args, **kwargs) -> None:
        """
        Submit listener events to all registered callbacks for the given event type.

        :param event: The ReaderListenerEventType to dispatch.
        :type event: ReaderListenerEventType
        :param args: Positional args to pass to callbacks.
        :param kwargs: Keyword args to pass to callbacks.
        """
        for cb in self._callbacks[event]:
            get_event_processor().submit(cb, *args, priority=self._report_priority, **kwargs)
