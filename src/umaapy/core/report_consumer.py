from typing import Any, Type, Callable, Union, Dict, List, override, Optional
import logging
import rti.connextdds as dds

from umaapy.util.event_processor import EventProcessor, Command, MEDIUM
from umaapy.util.dds_configurator import ReaderListenerEventType
from umaapy import get_event_processor, get_configurator
from umaapy.util.umaa_utils import validate_report
from umaapy.util.uuid_factory import guid_to_hex

from umaapy.umaa_types import UMAA_Common_IdentifierType as IdentifierType


class ReportConsumer(dds.DataReaderListener):
    def __init__(self, sources: List[IdentifierType], report_type: Type, report_priority: int = MEDIUM):
        super().__init__()
        if not validate_report(report_type()):
            raise RuntimeError(f"'{report_type.__name__.split("_")[-1]}' is not a valid UMAA report.")
        self._source_ids: List[IdentifierType] = sources
        self._report_type: Type = report_type
        self._report_priority: int = report_priority
        filter_expression = " OR ".join(
            [
                f"source.parentID = &hex({guid_to_hex(source.parentID)}) AND source.id = &hex({guid_to_hex(source.id)})"
                for source in sources
            ]
        )
        self._reader: dds.DataReader = get_configurator().get_filtered_reader(report_type, filter_expression)
        self._latest_report: Optional[Any] = None

        self._report_callbacks: List[Union[Callable[[Optional[Any]], None], Command]] = []

        self._callbacks: Dict[ReaderListenerEventType, List[Union[Callable[..., None], Command]]] = {
            evt: [] for evt in ReaderListenerEventType
        }

        self.name = self._report_type.__name__.split("ReportType")[0].split("_")[-1] + self.__class__.__name__
        self._logger: logging.Logger = logging.getLogger(self.name)

        self._reader.set_listener(self, dds.StatusMask.ALL)
        self._logger.debug(f"Reader filter expression: {filter_expression}")
        self._logger.info(f"Initialized {self.name}...")

    def add_report_callback(self, callback: Union[Callable[[Optional[Any]], None], Command]) -> None:
        self._report_callbacks.append(callback)

    def remove_report_callback(self, callback: Union[Callable[[Optional[Any]], None], Command]) -> None:
        self._report_callbacks.remove(callback)

    def add_event_callback(self, event: ReaderListenerEventType, callback: Union[Callable[..., None], Command]) -> None:
        self._callbacks[event].append(callback)

    def remove_event_callback(
        self, event: ReaderListenerEventType, callback: Union[Callable[..., None], Command]
    ) -> None:
        self._callbacks[event].remove(callback)

    def get_latest_report(self) -> Optional[Any]:
        return self._latest_report

    @override
    def on_data_available(self, reader: dds.DataReader):
        self._logger.debug("On data available triggered")
        self._dispatch_event(ReaderListenerEventType.ON_DATA_AVAILABLE, reader)
        for data, info in reader.take():
            if info.valid:
                self._dispatch_report(data)
            else:
                self._dispatch_report(None)

    @override
    def on_liveliness_changed(self, reader: dds.DataReader, status: dds.LivelinessChangedStatus):
        self._logger.debug("On liveliness changed triggered")
        self._dispatch_event(ReaderListenerEventType.ON_LIVELINESS_CHANGED, reader, status)

    @override
    def on_requested_deadline_missed(self, reader: dds.DataReader, status: dds.RequestedDeadlineMissedStatus):
        self._logger.debug("On requested deadline missed triggered")
        self._dispatch_event(ReaderListenerEventType.ON_REQUESTED_DEADLINE_MISSED, reader, status)

    @override
    def on_requested_incompatible_qos(self, reader: dds.DataReader, status: dds.RequestedIncompatibleQosStatus):
        self._logger.debug("On requested incompatible qos triggered")
        self._dispatch_event(ReaderListenerEventType.ON_REQUESTED_INCOMPATIBLE_QOS, reader, status)

    @override
    def on_sample_lost(self, reader: dds.DataReader, status: dds.SampleLostStatus):
        self._logger.debug("On sample lost triggered")
        self._dispatch_event(ReaderListenerEventType.ON_SAMPLE_LOST, reader, status)

    @override
    def on_sample_rejected(self, reader: dds.DataReader, status: dds.SampleRejectedStatus):
        self._logger.debug("On sample rejected triggered")
        self._dispatch_event(ReaderListenerEventType.ON_SAMPLE_REJECTED, reader, status)

    @override
    def on_subscription_matched(self, reader: dds.DataReader, status: dds.SubscriptionMatchedStatus):
        self._logger.debug("On subscription matched triggered")
        self._dispatch_event(ReaderListenerEventType.ON_SUBSCRIPTION_MATCHED, reader, status)

    def _dispatch_report(self, report: Optional[Any]) -> None:
        for cb in self._report_callbacks:
            get_event_processor().submit(cb, report, priority=self._report_priority)

    def _dispatch_event(self, event: ReaderListenerEventType, *args, **kwargs) -> None:
        for cb in self._callbacks[event]:
            get_event_processor().submit(cb, *args, priority=self._report_priority, **kwargs)
