from enum import Enum
import threading
import rti.connextdds as dds


class UmaaQosProfileCategory(Enum):
    COMMAND = 0
    CONFIG = 1
    REPORT = 2


class DDSConfigurator:
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

        self.qos_provider = dds.QosProvider(qos_file)
        self.participant = dds.DomainParticipant(
            domain_id, qos=self.qos_provider.participant_qos_from_profile("UMAAPyQosLib::ParticipantProfile")
        )

        self.publisher = dds.Publisher(self.participant)
        self.subscriber = dds.Subscriber(self.participant)
        self.topics = {}

    def get_topic(self, name: str, data_type):
        if name not in self.topics:
            self.topics[name] = dds.Topic(self.participant, name, data_type)
        return self.topics[name]

    def get_writer(
        self, topic_name: str, data_type, profile_category: UmaaQosProfileCategory = UmaaQosProfileCategory.REPORT
    ) -> dds.DataWriter:
        profile = self.PROFILE_DICT[profile_category]
        topic = self.get_topic(topic_name, data_type)
        writer_qos = self.qos_provider.datawriter_qos_from_profile(profile)
        return dds.DataWriter(self.publisher, topic, qos=writer_qos)

    def get_reader(
        self, topic_name: str, data_type, profile_category: UmaaQosProfileCategory = UmaaQosProfileCategory.REPORT
    ) -> dds.DataReader:
        profile = self.PROFILE_DICT[profile_category]
        topic = self.get_topic(topic_name, data_type)
        reader_qos = self.qos_provider.datareader_qos_from_profile(profile)
        return dds.DataReader(self.subscriber, topic, qos=reader_qos)
