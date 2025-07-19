from typing import Type, Optional, List
from enum import Enum
import threading
import rti.connextdds as dds

from umaapy.util.umaa_utils import topic_from_type

# dds.Logger.instance.verbosity_by_category(dds.LogCategory.all_categories, dds.Verbosity.STATUS_ALL)


class UmaaQosProfileCategory(Enum):
    """UMAA QoS profile type enum"""

    COMMAND = 0
    CONFIG = 1
    REPORT = 2


class WriterListenerEventType(Enum):
    """Internal enum matching the DDS writer listener callbacks"""

    ON_OFFERED_DEADLINE_MISSED = 0
    ON_OFFERED_INCOMPATIBLE_QOS = 1
    ON_PUBLICATION_MATCHED = 2
    ON_INSTANCE_REPLACED = 3
    ON_APPLICATION_ACKNOWLEDGMENT = 4
    ON_RELIABLE_READER_ACTIVITY_CHANGED = 5
    ON_RELIABLE_WRITER_CACHE_CHANGED = 6
    ON_SERVICE_REQUEST_ACCEPTED = 7
    ON_LIVELINESS_LOST = 8


class ReaderListenerEventType(Enum):
    """Internal enum matching the DDS reader listener callbacks"""

    ON_DATA_AVAILABLE = 0
    ON_LIVELINESS_CHANGED = 1
    ON_REQUESTED_DEADLINE_MISSED = 2
    ON_REQUESTED_INCOMPATIBLE_QOS = 3
    ON_SAMPLE_LOST = 4
    ON_SAMPLE_REJECTED = 5
    ON_SUBSCRIPTION_MATCHED = 6


class DDSConfigurator:
    """DDS Utility Class that handles QoS, reader, writer, and topic management"""

    PROFILE_DICT = {
        UmaaQosProfileCategory.COMMAND: "UMAAPyQosLib::Command",
        UmaaQosProfileCategory.CONFIG: "UMAAPyQosLib::Config",
        UmaaQosProfileCategory.REPORT: "UMAAPyQosLib::Report",
    }

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, domain_id: int, qos_file: str = ""):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._domain_id = domain_id
        self._qos_file = qos_file
        self.qos_provider = dds.QosProvider(qos_file)
        self.participant = dds.DomainParticipant(
            domain_id, qos=self.qos_provider.participant_qos_from_profile("UMAAPyQosLib::ParticipantProfile")
        )

        self.publisher = dds.Publisher(self.participant)
        self.subscriber = dds.Subscriber(self.participant)

    def get_topic(self, data_type: Type, name: str = None):
        name = topic_from_type(data_type) if name is None else name
        topic = dds.Topic.find(self.participant, name)
        if topic is None:
            topic = dds.Topic(self.participant, name, data_type)
        return topic

    def get_writer(
        self,
        data_type: Type,
        topic_name: str = None,
        profile_category: UmaaQosProfileCategory = UmaaQosProfileCategory.REPORT,
    ) -> dds.DataWriter:
        profile = self.PROFILE_DICT[profile_category]
        topic = self.get_topic(data_type, topic_name)
        writer_qos: dds.DataWriterQos = self.qos_provider.datawriter_qos_from_profile(profile)
        return dds.DataWriter(self.publisher, topic, qos=writer_qos)

    def get_reader(
        self,
        data_type: Type,
        topic_name: str = None,
        profile_category: UmaaQosProfileCategory = UmaaQosProfileCategory.REPORT,
    ) -> dds.DataReader:
        profile = self.PROFILE_DICT[profile_category]
        topic = self.get_topic(data_type, topic_name)
        reader_qos: dds.DataReaderQos = self.qos_provider.datareader_qos_from_profile(profile)
        return dds.DataReader(self.subscriber, topic, qos=reader_qos)

    def get_filtered_reader(
        self,
        data_type: Type,
        filter_expression: str,
        filter_parameters: Optional[List[str]] = None,
        topic_name: str = None,
        profile_category: UmaaQosProfileCategory = UmaaQosProfileCategory.REPORT,
    ) -> dds.DataReader:
        profile = self.PROFILE_DICT[profile_category]
        topic = self.get_topic(data_type, topic_name)
        reader_qos: dds.DataReaderQos = self.qos_provider.datareader_qos_from_profile(profile)
        cft = dds.ContentFilteredTopic.find(self.participant, f"{topic.name}Filtered")
        if cft is None:
            cft = dds.ContentFilteredTopic(
                topic, f"{topic.name}Filtered", dds.Filter(filter_expression, parameters=filter_parameters or [])
            )
        return dds.DataReader(self.subscriber, cft, qos=reader_qos)

    @classmethod
    def reset(cls):
        with cls._instance_lock:
            inst = cls._instance
            if not inst:
                return

            inst.participant.close_contained_entities()
            inst.participant.close()
            if hasattr(inst, "_initialized"):
                delattr(inst, "_initialized")
            inst.__init__(inst._domain_id, inst._qos_file)
