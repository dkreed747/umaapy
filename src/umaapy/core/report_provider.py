from typing import Any, Type, Callable, Union, Dict, List
from uuid import UUID
import logging
import rti.connextdds as dds

from umaapy.util.event_processor import EventProcessor, Command, MEDIUM
from umaapy.util.dds_configurator import UmaaQosProfileCategory, WriterListenerEventType
from umaapy import event_processor, configurator
from umaapy.util.timestamp import Timestamp
from umaapy.util.umaa_utils import validate_report

from umaapy.umaa_types import UMAA_Common_IdentifierType


class ReportProvider(dds.DataWriterListener):
    def __init__(self, source: UMAA_Common_IdentifierType, data_type: Type, report_priority: int = MEDIUM):
        super().__init__()
        if not validate_report(data_type()):
            raise RuntimeError(f"'{data_type.__name__.split("_")[-1]}' is not a valid UMAA report.")
        self._source_id: UMAA_Common_IdentifierType = source
        self._data_type: Type = data_type
        self._report_priority = report_priority
        self._pool: EventProcessor = event_processor
        self._writer: dds.DataWriter = configurator.get_writer(
            self._data_type, profile_category=UmaaQosProfileCategory.REPORT
        )
        self._callbacks: Dict[WriterListenerEventType, List[Union[Callable[..., None], Command]]] = {
            evt: [] for evt in WriterListenerEventType
        }

        self.name = self._data_type.__name__.split("ReportType")[0].split("_")[-1] + self.__class__.__name__
        self._logger = logging.getLogger(f"{self.name}")
        self._logger.info(f"Initialized {self.name}...")
        self._writer.set_listener(self, dds.StatusMask.ALL)

    def publish(self, sample: Any) -> None:
        sample.source = self._source_id
        sample.timeStamp = Timestamp.now().to_umaa()
        self._logger.debug("Writing sample")
        self._writer.write(sample)

    def dispose(self) -> None:
        key_holder = self._data_type()
        key_holder.source = self._source_id
        ih = self._writer.lookup_instance(key_holder)
        if ih != dds.InstanceHandle.nil:
            self._logger.debug(f"Disposing {self._data_type.__name__} on shutdown...")
            self._writer.dispose_instance(ih)
        else:
            self._logger.debug("No instance to dispose - doing nothing.")

    def add_event_callback(self, event: WriterListenerEventType, callback: Union[Callable[..., None], Command]) -> None:
        self._callbacks[event].append(callback)

    def remove_event_callback(
        self, event: WriterListenerEventType, callback: Union[Callable[..., None], Command]
    ) -> None:
        self._callbacks[event].remove(callback)

    def on_application_acknowledgment(self, writer: dds.DataWriter, ack_info: dds.AcknowledgmentInfo):
        self._logger.debug("On application acknowledgement triggered")
        self._dispatch(WriterListenerEventType.ON_APPLICATION_ACKNOWLEDGMENT, writer=writer, ack_info=ack_info)

    def on_instance_replaced(self, writer: dds.DataWriter, instance: dds.InstanceHandle):
        self._logger.debug("On instance replaced triggered")
        self._dispatch(WriterListenerEventType.ON_INSTANCE_REPLACED, writer=writer, instance=instance)

    def on_liveliness_lost(self, writer: dds.DataWriter, status: dds.LivelinessLostStatus):
        self._logger.debug("On liveliness lost triggered")
        self._dispatch(WriterListenerEventType.ON_LIVELINESS_LOST, writer=writer, status=status)

    def on_offered_deadline_missed(self, writer: dds.DataWriter, status: dds.OfferedDeadlineMissedStatus):
        self._logger.debug("On offered deadline missed triggered")
        self._dispatch(WriterListenerEventType.ON_OFFERED_DEADLINE_MISSED, writer=writer, status=status)

    def on_offered_incompatible_qos(self, writer: dds.DataWriter, status: dds.OfferedIncompatibleQosStatus):
        self._logger.debug("On offered incompatible qos triggered")
        self._dispatch(WriterListenerEventType.ON_OFFERED_INCOMPATIBLE_QOS, writer=writer, status=status)

    def on_publication_matched(self, writer: dds.DataWriter, status: dds.PublicationMatchedStatus):
        self._logger.debug("On publication matched triggered")
        self._dispatch(WriterListenerEventType.ON_PUBLICATION_MATCHED, writer=writer, status=status)

    def on_reliable_reader_activity_changed(
        self, writer: dds.DataWriter, status: dds.ReliableReaderActivityChangedStatus
    ):
        self._logger.debug("On reliable reader activity changed triggered")
        self._dispatch(WriterListenerEventType.ON_RELIABLE_READER_ACTIVITY_CHANGED, writer=writer, status=status)

    def on_reliable_writer_cache_changed(self, writer: dds.DataWriter, status: dds.ReliableWriterCacheChangedStatus):
        self._logger.debug("On reliable writer cache changed triggered")
        self._dispatch(WriterListenerEventType.ON_RELIABLE_WRITER_CACHE_CHANGED, writer=writer, status=status)

    def on_service_request_accepted(self, writer: dds.DataWriter, status: dds.ServiceRequestAcceptedStatus):
        self._logger.debug("On service request accepted triggered")
        self._dispatch(WriterListenerEventType.ON_SERVICE_REQUEST_ACCEPTED, writer=writer, status=status)

    def _dispatch(self, event: WriterListenerEventType, *args, **kwargs) -> None:
        for cb in self._callbacks[event]:
            self._pool.submit(cb, *args, priority=self._report_priority, **kwargs)
