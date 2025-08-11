from typing import Type, Optional, List, Tuple, Any, Dict, Iterable
from enum import Enum
import threading
import rti.connextdds as dds
import importlib
import inspect

from umaapy.util.umaa_utils import (
    topic_from_type,
    classify_obj_by_umaa,
    UMAAConcept,
    classify_obj_by_umaa,
    get_specializations_from_generalization,
)

from umaapy.util.multi_topic_support import (
    CombinedSample,
    UmaaReaderAdapter,
    UmaaFilteredReaderAdapter,
    UmaaWriterAdapter,
)
from umaapy.util.multi_topic_reader import ReaderNode
from umaapy.util.multi_topic_writer import WriterNode, TopLevelWriter
from umaapy.util.multi_topic_reader_decorators import (
    GenSpecReader,
    LargeSetReader,
    LargeListReader,
)
from umaapy.util.multi_topic_writer_decorators import (
    GenSpecWriter,
    LargeSetWriter,
    LargeListWriter,
)


class UmaaQosProfileCategory(Enum):
    """
    Enum of UMAA QoS profile categories used to select the appropriate
    QoS settings for DDS writers and readers.

    :cvar COMMAND: QoS profile for command topics.
    :cvar CONFIG:  QoS profile for configuration topics.
    :cvar REPORT:  QoS profile for report topics.
    """

    COMMAND = 0
    CONFIG = 1
    REPORT = 2


class WriterListenerEventType(Enum):
    """
    Internal enum representing DataWriter listener callback event types.
    Matches each DDS DataWriterListener method for event dispatching.

    :cvar ON_OFFERED_DEADLINE_MISSED:         Writer missed its offered deadline.
    :cvar ON_OFFERED_INCOMPATIBLE_QOS:       Offered QoS is incompatible.
    :cvar ON_PUBLICATION_MATCHED:            Publication matched subscription.
    :cvar ON_INSTANCE_REPLACED:              Writer instance replaced.
    :cvar ON_APPLICATION_ACKNOWLEDGMENT:     Application-level ack received.
    :cvar ON_RELIABLE_READER_ACTIVITY_CHANGED: Reliable reader activity changed.
    :cvar ON_RELIABLE_WRITER_CACHE_CHANGED:  Reliable writer cache changed.
    :cvar ON_SERVICE_REQUEST_ACCEPTED:        Service request accepted by middleware.
    :cvar ON_LIVELINESS_LOST:                Writer liveliness lost.
    """

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
    """
    Internal enum representing DataReader listener callback event types.
    Matches each DDS DataReaderListener method for event dispatching.

    :cvar ON_DATA_AVAILABLE:                New data is available to read.
    :cvar ON_LIVELINESS_CHANGED:           Liveliness of data reader changed.
    :cvar ON_REQUESTED_DEADLINE_MISSED:    Reader missed a requested deadline.
    :cvar ON_REQUESTED_INCOMPATIBLE_QOS:   Incompatible requested QoS.
    :cvar ON_SAMPLE_LOST:                  A sample was lost.
    :cvar ON_SAMPLE_REJECTED:              A sample was rejected.
    :cvar ON_SUBSCRIPTION_MATCHED:         Subscription matched a publication.
    """

    ON_DATA_AVAILABLE = 0
    ON_LIVELINESS_CHANGED = 1
    ON_REQUESTED_DEADLINE_MISSED = 2
    ON_REQUESTED_INCOMPATIBLE_QOS = 3
    ON_SAMPLE_LOST = 4
    ON_SAMPLE_REJECTED = 5
    ON_SUBSCRIPTION_MATCHED = 6


class DDSConfigurator:
    """
    Singleton utility class for DDS DomainParticipant and QoS management.

    Provides methods to obtain topics, DataWriters, DataReaders, and
    content-filtered readers using UMAA-defined QoS profiles.

    This configurator ensures a single DomainParticipant per process,
    guarding creation with a threading lock.

    :ivar PROFILE_DICT: Mapping from UmaaQosProfileCategory to QoS library profile names.
    :ivar participant: The global DomainParticipant instance.
    :ivar publisher:   Publisher attached to the participant.
    :ivar subscriber:  Subscriber attached to the participant.
    """

    # Map UMAA profile categories to the corresponding QosProvider profiles
    PROFILE_DICT = {
        UmaaQosProfileCategory.COMMAND: "UMAAPyQosLib::Command",
        UmaaQosProfileCategory.CONFIG: "UMAAPyQosLib::Config",
        UmaaQosProfileCategory.REPORT: "UMAAPyQosLib::Report",
    }

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        Ensure only one DDSConfigurator instance exists (singleton pattern).
        Thread-safe via a class-level lock.
        """
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, domain_id: int, qos_file: str = ""):
        """
        Initialize the DDS DomainParticipant, Publisher, Subscriber, and QosProvider.

        Subsequent calls will no-op if already initialized.

        :param domain_id: DDS domain ID for the DomainParticipant.
        :param qos_file:  Path to the XML QoS profile file (optional).
        """
        # Prevent reinitialization on multiple __init__ calls
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        # Store configuration parameters
        self._domain_id = domain_id
        self._qos_file = qos_file

        # Load QoS definitions from the specified file
        self.qos_provider = dds.QosProvider(qos_file)
        # Create the DomainParticipant using the default participant profile
        self.participant = dds.DomainParticipant(
            domain_id, qos=self.qos_provider.participant_qos_from_profile("UMAAPyQosLib::ParticipantProfile")
        )

        # Create a Publisher and Subscriber on the same participant
        self.publisher = dds.Publisher(self.participant)
        self.subscriber = dds.Subscriber(self.participant)

    def get_topic(self, data_type: Type, name: str = None) -> dds.Topic:
        """
        Retrieve (or create) a DDS Topic for the given data type.

        :param data_type: The DDS type class to publish/subscribe.
        :param name:      Optional topic name override. If None, a default
                          name is derived via umaapy.util.umaa_utils.topic_from_type.
        :return:          A Topic instance registered with the participant.
        """
        # Derive topic name if not provided
        topic_name = topic_from_type(data_type) if name is None else name
        # Try to find an existing topic by name
        topic = dds.Topic.find(self.participant, topic_name)
        if topic is None:
            # Create new topic if not found
            topic = dds.Topic(self.participant, topic_name, data_type)
        return topic

    def get_writer(
        self,
        data_type: Type,
        topic_name: str = None,
        profile_category: UmaaQosProfileCategory = UmaaQosProfileCategory.REPORT,
    ) -> dds.DataWriter:
        """
        Create a DataWriter for the specified data type and QoS profile.

        :param data_type:      The DDS type class for the writer.
        :param topic_name:     Optional override of the topic name.
        :param profile_category: QoS profile enum to select writer settings.
        :return:               A configured DataWriter instance.
        """
        # Look up the string profile name
        profile = self.PROFILE_DICT[profile_category]
        # Get or create the topic
        topic = self.get_topic(data_type, topic_name)
        # Load the DataWriter QoS from profile
        writer_qos: dds.DataWriterQos = self.qos_provider.datawriter_qos_from_profile(profile)
        # Instantiate and return the writer
        return dds.DataWriter(self.publisher, topic, qos=writer_qos)

    def get_reader(
        self,
        data_type: Type,
        topic_name: str = None,
        profile_category: UmaaQosProfileCategory = UmaaQosProfileCategory.REPORT,
    ) -> dds.DataReader:
        """
        Create a DataReader for the specified data type and QoS profile.

        :param data_type:      The DDS type class for the reader.
        :param topic_name:     Optional override of the topic name.
        :param profile_category: QoS profile enum to select reader settings.
        :return:               A configured DataReader instance.
        """
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
    ) -> Tuple[dds.DataReader, dds.ContentFilteredTopic]:
        """
        Create a content-filtered DataReader using the given filter expression.

        :param data_type:         The DDS type class for the reader.
        :param filter_expression: A valid DDS filter expression string.
        :param filter_parameters: Optional list of filter parameters.
        :param topic_name:        Optional override of the topic name.
        :param profile_category:  QoS profile enum to select reader settings.
        :return:                  A tuple of the configured DataReader and its associated content-filtered topic.
        """
        profile = self.PROFILE_DICT[profile_category]
        topic = self.get_topic(data_type, topic_name)
        reader_qos: dds.DataReaderQos = self.qos_provider.datareader_qos_from_profile(profile)
        # Attempt to find or create the ContentFilteredTopic
        filter_name = f"{topic.name}Filtered"
        cft = dds.ContentFilteredTopic.find(self.participant, filter_name)
        if cft is None:
            cft = dds.ContentFilteredTopic(
                topic, filter_name, dds.Filter(filter_expression, parameters=filter_parameters or [])
            )
        # Return a DataReader bound to the filtered topic
        return dds.DataReader(self.subscriber, cft, qos=reader_qos), cft

    def get_umaa_reader(self, data_type: Type) -> dds.DataReader:
        """
        Build a UMAA-aware reader graph for `data_type` and return an adapter that
        mimics a DataReader (set_listener(listener, status_mask), read/take, read_data/take_data).
        """

        root_reader = self.get_reader(data_type)
        root_node = ReaderNode(root_reader, key_fn=self._mt_new_key, new_base_fn=self._mt_wrap_base)
        self._mt_augment_reader_node(root_node, data_type)
        return UmaaReaderAdapter(root_node, root_reader)

    def get_filtered_umaa_reader(
        self, data_type: Type, filter_expression: str, filter_parameters: Optional[List[str]] = None
    ) -> dds.DataReader:
        """
        Same as get_umaa_reader, but the ROOT is bound to a ContentFilteredTopic.
        Child element/spec readers are left unfiltered (typical UMAA pattern).
        """

        root_reader, cft = self.get_filtered_reader(
            data_type, filter_expression=filter_expression, filter_parameters=filter_parameters
        )
        root_node = ReaderNode(root_reader, key_fn=self._mt_new_key, new_base_fn=self._mt_wrap_base)
        self._mt_augment_reader_node(root_node, data_type)
        return UmaaFilteredReaderAdapter(root_node, root_reader, cft)

    def get_umaa_writer(self, data_type: Type) -> dds.DataWriter:
        """
        Build a UMAA-aware writer graph and return a writer adapter that feels like a DataWriter.
        Exposes .new() and .write(builder) plus set_listener(listener, status_mask).
        """

        root_writer = self.get_writer(data_type)
        root_node = WriterNode(root_writer)
        self._mt_augment_writer_node(root_node, data_type)

        top = TopLevelWriter(root_node, base_factory=data_type)
        return UmaaWriterAdapter(root_node, top, root_writer)

    @classmethod
    def reset(cls) -> None:
        """
        Reset the singleton DDSConfigurator by closing and reinitializing
        the underlying DomainParticipant and contained entities.

        Useful in tests or when changing domain or QoS file on the fly.
        """
        with cls._instance_lock:
            inst = cls._instance
            if not inst:
                return
            # Clean up DDS entities
            inst.participant.close_contained_entities()
            inst.participant.close()
            # Remove initialized flag and recreate instance
            if hasattr(inst, "_initialized"):
                delattr(inst, "_initialized")
            inst.__init__(inst._domain_id, inst._qos_file)

    def _mt_new_key(self, sample: object) -> int:
        # Per-sample unique key; decorators map IDs -> parent key internally.
        return id(sample)

    def _mt_wrap_base(self, sample):
        return CombinedSample(base=sample)

    def _mt_classify_type(self, umaa_type: Type):
        """Return classify_obj_by_ummaa map for a default-constructed instance."""
        return classify_obj_by_umaa(umaa_type())

    def _mt_obj_at_path(self, root_obj: object, path: Tuple[str, ...]) -> object:
        cur = root_obj
        for seg in path:
            cur = getattr(cur, seg)
        return cur

    def _mt_parent_cls_for_path(self, umaa_type: Type, path: Tuple[str, ...]) -> Type:
        """Type of the object that directly owns the last path segment (metadata field)."""
        if not path:
            return umaa_type
        parent_obj = self._mt_obj_at_path(umaa_type(), path[:-1])
        return type(parent_obj)

    def _mt_attr_base_from_metadata_field(self, field_name: str) -> Optional[str]:
        for suffix in ("SetMetadata", "ListMetadata"):
            if field_name.endswith(suffix):
                return field_name[: -len(suffix)]
        return None

    def _mt_resolve_collection_element_type(self, parent_cls: Type, attr_base: str, kind: str) -> Type:
        """
        Resolve element type by UMAA naming rule:
        <ParentMessageName><CapitalizedAttributeName><Set|List>Element
        """

        mod = importlib.import_module("umaapy.umaa_types")
        parent_name = parent_cls.__name__
        cap_attr = attr_base[:1].upper() + attr_base[1:]
        suffix = "SetElement" if kind == "set" else "ListElement"
        candidate = f"{parent_name}{cap_attr}{suffix}"
        try:
            return getattr(mod, candidate)
        except AttributeError as e:
            raise RuntimeError(f"Could not resolve {kind} element type '{candidate}' in umaapy.umaa_types") from e

    def _mt_iter_generalizations(self, umaa_type: Type) -> List[Tuple[Tuple[str, ...], Type, Dict[str, Type]]]:
        """[(path, generalization_type, {specTopicShort: specType, ...}), ...]"""
        cmap = self._mt_classify_type(umaa_type)
        out = []
        for path, finfo in cmap.items():
            if UMAAConcept.GENERALIZATION in finfo.classifications:
                gen_t = finfo.python_type
                specs = get_specializations_from_generalization(gen_t)  # {'RouteObjectiveType': <class ...>, ...}
                out.append((path, gen_t, specs))
        return out

    def _mt_iter_large_sets(self, umaa_type: Type) -> List[Tuple[str, Type]]:
        """[(attr_base_name, element_type), ...]"""
        cmap = self._mt_classify_type(umaa_type)
        root_inst = umaa_type()
        out: List[Tuple[str, Type]] = []
        for path, finfo in cmap.items():
            if UMAAConcept.LARGE_SET in finfo.classifications and path:
                field = path[-1]
                base = self._mt_attr_base_from_metadata_field(field)
                if not base:
                    continue
                parent_cls = self._mt_parent_cls_for_path(umaa_type, path)
                elem_t = self._mt_resolve_collection_element_type(parent_cls, base, "set")
                out.append((base, elem_t))
        return out

    def _mt_iter_large_lists(self, umaa_type: Type) -> List[Tuple[str, Type]]:
        """[(attr_base_name, element_type), ...]"""
        cmap = self._mt_classify_type(umaa_type)
        root_inst = umaa_type()
        out: List[Tuple[str, Type]] = []
        for path, finfo in cmap.items():
            if UMAAConcept.LARGE_LIST in finfo.classifications and path:
                field = path[-1]
                base = self._mt_attr_base_from_metadata_field(field)
                if not base:
                    continue
                parent_cls = self._mt_parent_cls_for_path(umaa_type, path)
                elem_t = self._mt_resolve_collection_element_type(parent_cls, base, "list")
                out.append((base, elem_t))
        return out

    def _mt_make_genspec_reader(self, path: Tuple[str, ...]):
        """
        Prefer a path-aware GenSpecReader if available; otherwise fall back to the plain one.
        This lets you keep Gen/Spec fields nested (e.g., ('objective',)) without changing the
        decorator import sites if you add path support later.
        """
        try:
            if "attr_path" in inspect.signature(GenSpecReader.__init__).parameters:
                return GenSpecReader(attr_path=path)
        except Exception:
            pass
        return GenSpecReader()

    def _mt_make_genspec_writer(self, path: Tuple[str, ...]):
        try:
            if "attr_path" in inspect.signature(GenSpecWriter.__init__).parameters:
                return GenSpecWriter(attr_path=path, resolve_child_by_topic_name=lambda t: t)
        except Exception:
            pass
        return GenSpecWriter(resolve_child_by_topic_name=lambda t: t)

    def _mt_augment_reader_node(self, node, umaa_type: Type):
        """Attach UMAA reader decorators + recursively wire children based on classification."""
        # Large Sets
        for set_name, elem_t in self._mt_iter_large_sets(umaa_type):
            node.register_decorator(set_name, LargeSetReader(set_name), required=True)
            elem_reader = self.get_reader(elem_t)
            child = ReaderNode(elem_reader, key_fn=self._mt_new_key, new_base_fn=self._mt_wrap_base)
            node.attach_child(set_name, topic_from_type(elem_t), child)
            self._mt_augment_reader_node(child, elem_t)

        # Large Lists
        for list_name, elem_t in self._mt_iter_large_lists(umaa_type):
            node.register_decorator(list_name, LargeListReader(list_name), required=True)
            elem_reader = self.get_reader(elem_t)
            child = ReaderNode(elem_reader, key_fn=self._mt_new_key, new_base_fn=self._mt_wrap_base)
            node.attach_child(list_name, topic_from_type(elem_t), child)
            self._mt_augment_reader_node(child, elem_t)

        for path, gen_t, specs in self._mt_iter_generalizations(umaa_type):
            node.register_decorator("gen_spec", self._mt_make_genspec_reader(path), required=True)
            for topic_short, spec_t in specs.items():
                spec_reader = self.get_reader(spec_t)
                child = ReaderNode(spec_reader, key_fn=self._mt_new_key, new_base_fn=self._mt_wrap_base)
                node.attach_child("gen_spec", topic_short or topic_from_type(spec_t), child)
                self._mt_augment_reader_node(child, spec_t)

    def _mt_augment_writer_node(self, node, umaa_type: Type):
        """Attach UMAA writer decorators + recursively wire children based on classification."""
        # Large Sets
        for set_name, elem_t in self._mt_iter_large_sets(umaa_type):
            node.register_decorator(set_name, LargeSetWriter(set_name))
            elem_writer = self.get_writer(elem_t)
            child = WriterNode(elem_writer)
            node.attach_child(set_name, topic_from_type(elem_t), child)
            self._mt_augment_writer_node(child, elem_t)

        # Large Lists
        for list_name, elem_t in self._mt_iter_large_lists(umaa_type):
            node.register_decorator(list_name, LargeListWriter(list_name))
            elem_writer = self.get_writer(elem_t)
            child = WriterNode(elem_writer)
            node.attach_child(list_name, topic_from_type(elem_t), child)
            self._mt_augment_writer_node(child, elem_t)

        # Generalization / Specializations
        for path, gen_t, specs in self._mt_iter_generalizations(umaa_type):
            node.register_decorator("gen_spec", self._mt_make_genspec_writer(path))
            for topic_short, spec_t in specs.items():
                spec_writer = self.get_writer(spec_t)
                child = WriterNode(spec_writer)
                node.attach_child("gen_spec", topic_short or topic_from_type(spec_t), child)
                self._mt_augment_writer_node(child, spec_t)
