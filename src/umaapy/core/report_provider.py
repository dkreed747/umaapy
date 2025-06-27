from typing import Any, Type, Callable, Union, Dict, List
from uuid import UUID
import logging
import rti.connextdds as dds

from umaapy.util.event_processor import EventProcessor, Command
from umaapy.util.dds_configurator import UmaaQosProfileCategory, WriterListenerEventType
from umaapy import event_processor, configurator
from umaapy.util.timestamp import Timestamp

from umaapy.types import UMAA_Common_IdentifierType


class ReportProvider(dds.DataWriterListener):
    def __init__(self, source: UMAA_Common_IdentifierType, data_type: Any, topic: str):
        super().__init__()
        self._data_type = data_type
        self._source_id: UMAA_Common_IdentifierType = source
        self._topic: str = topic
        self._pool: EventProcessor = event_processor
        self._writer: dds.DataWriter = configurator.get_writer(
            self._topic, self._data_type, UmaaQosProfileCategory.REPORT
        )
        self._callbacks: Dict[WriterListenerEventType, List[Union[Callable[..., None], Command]]] = {
            evt: [] for evt in WriterListenerEventType
        }

        self.name = self._data_type.__name__.split("_")[-1] + self.__class__.__name__
        self._logger = logging.getLogger(f"{self.name}")
        self._logger.info(f"Initialized {self.name}...")
        self._writer.set_listener(self, dds.StatusMask.ALL)

    def publish(self, sample: Any) -> None:
        if not hasattr(sample, "source") or not hasattr(sample, "timeStamp"):
            self._logger.warning(
                f"{type(sample).__name__} does not have required report fields 'source' or 'timeStamp'."
            )
            return

        sample.source = self._source_id
        sample.timeStamp = Timestamp.now().to_umaa()
        self._logger.debug("Writing sample")
        self._writer.write(sample)

    def dispose(self) -> None:
        key_holder = self._data_type()
        if not hasattr(key_holder, "source"):
            self._logger.warning(f'{self._data_type.__name__} does not have required key field "source"')
            return

        key_holder.source = self._source_id
        ih = self._writer.lookup_instance(key_holder)
        if ih != dds.InstanceHandle.nil:
            self._logger.info(f"Disposing {self._data_type.__name__} on shutdown...")
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
        self._logger.info("On application acknowledgement triggered")

    def on_instance_replaced(self, writer: dds.DataWriter, instance: dds.InstanceHandle):
        self._logger.info("On instance replaced triggered")

    def on_liveliness_lost(self, writer: dds.DataWriter, status: dds.LivelinessLostStatus):
        self._logger.info("On liveliness lost triggered")

    def on_offered_deadline_missed(self, writer: dds.DataWriter, status: dds.OfferedDeadlineMissedStatus):
        self._logger.info("On offered deadline missed triggered")

    def on_offered_incompatible_qos(self, writer: dds.DataWriter, status: dds.OfferedIncompatibleQosStatus):
        self._logger.info("On offered incompatible qos triggered")

    def on_publication_matched(self, writer: dds.DataWriter, status: dds.PublicationMatchedStatus):
        self._logger.info("On publication matched triggered")
        self._logger.info(f"Publication subscriber count: {status.current_count}")
        self._dispatch(WriterListenerEventType.ON_PUBLICATION_MATCHED, writer=writer, status=status)

    def on_reliable_reader_activity_changed(
        self, writer: dds.DataWriter, status: dds.ReliableReaderActivityChangedStatus
    ):
        self._logger.info("On reliable reader activity changed triggered")

    def on_reliable_writer_cache_changed(self, writer: dds.DataWriter, status: dds.ReliableWriterCacheChangedStatus):
        self._logger.info("On reliable writer cache changed triggered")

    def on_service_request_accepted(self, writer: dds.DataWriter, status: dds.ServiceRequestAcceptedStatus):
        self._logger.info("On service request accepted triggered")

    def _dispatch(self, event: WriterListenerEventType, *args, **kwargs) -> None:
        for cb in self._callbacks[event]:
            if issubclass(cb, Command):
                self._logger.info("Dispatching subclass of Command")
                cmd = cb(*args, **kwargs)
                self._pool.submit(cmd)
            elif isinstance(cb, Command):
                self._logger.info("Dispatching instance of Command")
                self._pool.submit(cb)
            else:
                self._logger.info("Dispatching callable")
                self._pool.submit(cb)
