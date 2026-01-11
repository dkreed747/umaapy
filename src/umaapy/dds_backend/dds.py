from __future__ import annotations

from dataclasses import dataclass
from enum import IntFlag
from typing import Any, Iterable, List, Optional, Sequence, Tuple
import re

from cyclonedds import core, domain, pub, qos, sub, topic, util
from cyclonedds.internal import InvalidSample


class InstanceHandle:
    nil = 0


class InstanceState:
    ALIVE = core.InstanceState.Alive
    NOT_ALIVE_DISPOSED = core.InstanceState.NotAliveDisposed
    NOT_ALIVE_NO_WRITERS = core.InstanceState.NotAliveNoWriters
    ANY = core.InstanceState.Any


class StatusMask(IntFlag):
    NONE = 0
    INCONSISTENT_TOPIC = core.DDSStatus.InconsistentTopic
    OFFERED_DEADLINE_MISSED = core.DDSStatus.OfferedDeadlineMissed
    REQUESTED_DEADLINE_MISSED = core.DDSStatus.RequestedDeadlineMissed
    OFFERED_INCOMPATIBLE_QOS = core.DDSStatus.OfferedIncompatibleQos
    REQUESTED_INCOMPATIBLE_QOS = core.DDSStatus.RequestedIncompatibleQos
    SAMPLE_LOST = core.DDSStatus.SampleLost
    SAMPLE_REJECTED = core.DDSStatus.SampleRejected
    DATA_ON_READERS = core.DDSStatus.DataOnReaders
    DATA_AVAILABLE = core.DDSStatus.DataAvailable
    LIVELINESS_LOST = core.DDSStatus.LivelinessLost
    LIVELINESS_CHANGED = core.DDSStatus.LivelinessChanged
    PUBLICATION_MATCHED = core.DDSStatus.PublicationMatched
    SUBSCRIPTION_MATCHED = core.DDSStatus.SubscriptionMatched

    # Not supported by Cyclone DDS Python, keep as no-ops
    INSTANCE_REPLACED = 0
    APPLICATION_ACKNOWLEDGMENT = 0
    RELIABLE_READER_ACTIVITY_CHANGED = 0
    RELIABLE_WRITER_CACHE_CHANGED = 0
    RELIABLE_READER_CACHE_CHANGED = 0
    SERVICE_REQUEST_ACCEPTED = 0

    ALL = core.DDSStatus.All


class DataReaderListener:
    def __init__(self) -> None:
        pass


class DataWriterListener:
    def __init__(self) -> None:
        pass


class NoOpDataReaderListener(DataReaderListener):
    def on_data_available(self, reader) -> None:
        pass

    def on_liveliness_changed(self, reader, status) -> None:
        pass

    def on_requested_deadline_missed(self, reader, status) -> None:
        pass

    def on_requested_incompatible_qos(self, reader, status) -> None:
        pass

    def on_sample_lost(self, reader, status) -> None:
        pass

    def on_sample_rejected(self, reader, status) -> None:
        pass

    def on_subscription_matched(self, reader, status) -> None:
        pass


class NoOpDataWriterListener(DataWriterListener):
    def on_offered_deadline_missed(self, writer, status) -> None:
        pass

    def on_offered_incompatible_qos(self, writer, status) -> None:
        pass

    def on_liveliness_lost(self, writer, status) -> None:
        pass

    def on_publication_matched(self, writer, status) -> None:
        pass

    def on_reliable_writer_cache_changed(self, writer, status) -> None:
        pass

    def on_reliable_reader_activity_changed(self, writer, status) -> None:
        pass

    def on_instance_replaced(self, writer, handle) -> None:
        pass


class PublicationMatchedStatus:
    pass


class SubscriptionMatchedStatus:
    pass


class LivelinessChangedStatus:
    pass


class LivelinessLostStatus:
    pass


class RequestedDeadlineMissedStatus:
    pass


class RequestedIncompatibleQosStatus:
    pass


class OfferedDeadlineMissedStatus:
    pass


class OfferedIncompatibleQosStatus:
    pass


class SampleLostStatus:
    pass


class SampleRejectedStatus:
    pass


class ReliableReaderActivityChangedStatus:
    pass


class ReliableWriterCacheChangedStatus:
    pass


class ServiceRequestAcceptedStatus:
    pass


class AcknowledgmentInfo:
    pass


class Filter:
    def __init__(self, expression: str, parameters: Optional[Sequence[str]] = None) -> None:
        self.expression = expression or ""
        self.parameters = [str(p) for p in (parameters or [])]

    def match(self, sample: Any) -> bool:
        expr = (self.expression or "").strip()
        if not expr:
            return True
        expr = expr.strip()
        if re.fullmatch(r"1\s*=\s*0", expr):
            return False
        if re.fullmatch(r"1\s*=\s*1", expr):
            return True

        parts = re.split(r"\s+OR\s+", expr, flags=re.IGNORECASE)
        for part in parts:
            ands = re.split(r"\s+AND\s+", part, flags=re.IGNORECASE)
            if all(self._match_clause(clause.strip(), sample) for clause in ands if clause.strip()):
                return True
        return False

    def _match_clause(self, clause: str, sample: Any) -> bool:
        if not clause:
            return True

        arr_match = re.match(r"^(?P<field>[\\w\\.]+)\\[(?P<idx>\\d+)\\]\\s*=\\s*(?P<rhs>.+)$", clause)
        if arr_match:
            field = arr_match.group("field")
            idx = int(arr_match.group("idx"))
            rhs = arr_match.group("rhs").strip()
            value = _get_field_value(sample, field)
            if value is None:
                return False
            if hasattr(value, "value"):
                value = value.value
            try:
                return int(value[idx]) == _parse_numeric(rhs, self.parameters)
            except Exception:
                return False

        hex_match = re.match(r"^(?P<field>[\\w\\.]+)\\s*=\\s*&hex\\(\\s*(?P<hex>[^\\)]+)\\s*\\)\\s*$", clause)
        if hex_match:
            field = hex_match.group("field")
            hex_val = hex_match.group("hex").strip()
            value = _get_field_value(sample, field)
            if value is None:
                return False
            if hasattr(value, "value"):
                value = value.value
            expected = _parse_hex(hex_val, self.parameters)
            return expected is not None and list(value) == expected

        basic = re.match(r"^(?P<field>[\\w\\.]+)\\s*=\\s*(?P<rhs>.+)$", clause)
        if not basic:
            return False
        field = basic.group("field")
        rhs = basic.group("rhs").strip()
        value = _get_field_value(sample, field)
        if value is None:
            return False
        if hasattr(value, "value"):
            value = value.value

        if isinstance(value, (int, float)):
            try:
                return value == _parse_numeric(rhs, self.parameters)
            except Exception:
                return False
        return str(value) == _parse_string(rhs, self.parameters)


class ContentFilteredTopic:
    _registry: dict[int, dict[str, "ContentFilteredTopic"]] = {}

    def __init__(self, base_topic: "Topic", name: str, filter: Filter) -> None:
        self.topic = base_topic
        self.name = name
        self.filter = filter
        reg = self._registry.setdefault(id(base_topic._participant), {})
        reg[name] = self

    @classmethod
    def find(cls, participant: "DomainParticipant", name: str) -> Optional["ContentFilteredTopic"]:
        reg = cls._registry.get(id(participant), {})
        return reg.get(name)

    def set_filter(self, filter: Filter) -> None:
        self.filter = filter

    @property
    def filter_expression(self) -> str:
        return self.filter.expression

    @property
    def filter_parameters(self) -> List[str]:
        return list(self.filter.parameters)


class Topic:
    def __init__(self, participant: "DomainParticipant", name: str, data_type: type) -> None:
        self._participant = participant
        self._topic = topic.Topic(participant._participant, name, data_type)
        self.name = name
        self.data_type = data_type
        participant._topics[name] = self

    @classmethod
    def find(cls, participant: "DomainParticipant", name: str) -> Optional["Topic"]:
        return participant._topics.get(name)

    def type(self) -> type:
        return self.data_type


class Publisher:
    def __init__(self, participant: "DomainParticipant") -> None:
        self._publisher = pub.Publisher(participant._participant)

    def close(self) -> None:
        _delete_entity(self._publisher)


class Subscriber:
    def __init__(self, participant: "DomainParticipant") -> None:
        self._subscriber = sub.Subscriber(participant._participant)

    def close(self) -> None:
        _delete_entity(self._subscriber)


class DomainParticipant:
    def __init__(self, domain_id: int = 0, qos: Optional[qos.Qos] = None) -> None:
        self._participant = domain.DomainParticipant(domain_id, qos=qos)
        self._topics: dict[str, Topic] = {}

    def close_contained_entities(self) -> None:
        try:
            children = list(self._participant.get_children())
        except Exception:
            children = []
        for child in children:
            _delete_recursive(child)

    def close(self) -> None:
        _delete_entity(self._participant)


@dataclass(frozen=True)
class _SampleState:
    instance_state: int


class SampleInfo:
    def __init__(self, *, valid: bool, instance_state: int, instance_handle: int, source_timestamp: int = 0) -> None:
        self.valid = valid
        self.state = _SampleState(instance_state=instance_state)
        self.instance_state = instance_state
        self.instance_handle = instance_handle
        self.source_timestamp = source_timestamp


class DataReader:
    def __init__(
        self,
        subscriber_or_participant: Any,
        topic_or_cft: Any,
        qos: Optional[qos.Qos] = None,
        listener: Optional[core.Listener] = None,
    ) -> None:
        if isinstance(subscriber_or_participant, Subscriber):
            sub_entity = subscriber_or_participant._subscriber
        else:
            sub_entity = subscriber_or_participant._participant

        if isinstance(topic_or_cft, ContentFilteredTopic):
            self._cft = topic_or_cft
            topic_entity = topic_or_cft.topic._topic
            self.topic = topic_or_cft.topic
        else:
            self._cft = None
            topic_entity = topic_or_cft._topic
            self.topic = topic_or_cft

        self._reader = sub.DataReader(sub_entity, topic_entity, qos=qos, listener=listener)

    def set_listener(self, listener: Optional[object], status_mask: StatusMask = StatusMask.NONE) -> None:
        if listener is None:
            self._reader.set_listener(None)
            return
        self._reader.set_listener(_build_reader_listener(self, listener, status_mask))

    def read(self, max_samples: int = 64) -> List[Tuple[Any, SampleInfo]]:
        return self._collect_samples(self._reader.read, max_samples, apply_filter=True)

    def take(self, max_samples: int = 64) -> List[Tuple[Any, SampleInfo]]:
        return self._collect_samples(self._reader.take, max_samples, apply_filter=True)

    def read_data(self, max_samples: int = 64) -> List[Any]:
        return [sample for sample, info in self.read(max_samples) if info.valid and sample is not None]

    def take_data(self, max_samples: int = 64) -> List[Any]:
        return [sample for sample, info in self.take(max_samples) if info.valid and sample is not None]

    def key_value(self, handle: int) -> Optional[Any]:
        if handle is None:
            return None
        samples = self._reader.read(1, instance_handle=handle)
        for sample in samples:
            if isinstance(sample, InvalidSample):
                continue
            return sample
        return None

    def close(self) -> None:
        _delete_entity(self._reader)

    def _collect_samples(self, fn, max_samples: int, apply_filter: bool) -> List[Tuple[Any, SampleInfo]]:
        raw_samples = fn(max_samples)
        out: List[Tuple[Any, SampleInfo]] = []
        for sample in raw_samples:
            if isinstance(sample, InvalidSample):
                info = sample.sample_info
                wrapped = SampleInfo(
                    valid=False,
                    instance_state=info.instance_state,
                    instance_handle=info.instance_handle,
                    source_timestamp=info.source_timestamp,
                )
                out.append((None, wrapped))
                continue

            info = getattr(sample, "sample_info", None)
            if info is None:
                wrapped = SampleInfo(valid=True, instance_state=InstanceState.ALIVE, instance_handle=0)
            else:
                wrapped = SampleInfo(
                    valid=getattr(info, "valid_data", True),
                    instance_state=info.instance_state,
                    instance_handle=info.instance_handle,
                    source_timestamp=info.source_timestamp,
                )
            if apply_filter and self._cft is not None and wrapped.valid:
                if not self._cft.filter.match(sample):
                    continue
            out.append((sample, wrapped))
        return out


class DataWriter:
    def __init__(
        self,
        publisher_or_participant: Any,
        topic_obj: Topic,
        qos: Optional[qos.Qos] = None,
        listener: Optional[core.Listener] = None,
    ) -> None:
        if isinstance(publisher_or_participant, Publisher):
            pub_entity = publisher_or_participant._publisher
        else:
            pub_entity = publisher_or_participant._participant
        self.topic = topic_obj
        self._writer = pub.DataWriter(pub_entity, topic_obj._topic, qos=qos, listener=listener)

    def write(self, sample: Any) -> None:
        self._writer.write(sample)

    def lookup_instance(self, sample: Any) -> Optional[int]:
        return self._writer.lookup_instance(sample)

    def dispose_instance(self, handle: Optional[int]) -> None:
        if handle is None or handle == InstanceHandle.nil:
            return
        self._writer.dispose_instance_handle(handle)

    def set_listener(self, listener: Optional[object], status_mask: StatusMask = StatusMask.NONE) -> None:
        if listener is None:
            self._writer.set_listener(None)
            return
        self._writer.set_listener(_build_writer_listener(self, listener, status_mask))

    def close(self) -> None:
        _delete_entity(self._writer)

    @property
    def matched_subscriptions(self) -> Iterable[int]:
        raise NotImplementedError("matched_subscriptions is not available in Cyclone DDS Python")

    def matched_subscription_data(self, _handle: int) -> Any:
        raise NotImplementedError("matched_subscription_data is not available in Cyclone DDS Python")


class QosProvider:
    def __init__(self, _xml_path: str = "") -> None:
        self._profiles = {
            "Command": _build_command_qos(),
            "Config": _build_config_qos(),
            "Report": _build_report_qos(),
        }

    def participant_qos_from_profile(self, _profile: str) -> qos.Qos:
        return qos.Qos()

    def datawriter_qos_from_profile(self, profile: str) -> qos.Qos:
        return self._profiles[profile.split("::")[-1]]

    def datareader_qos_from_profile(self, profile: str) -> qos.Qos:
        return self._profiles[profile.split("::")[-1]]


def Uint8Seq(values: Iterable[int]) -> List[int]:
    if isinstance(values, (bytes, bytearray)):
        return [b for b in values]
    return list(values)


DataReaderQos = qos.Qos
DataWriterQos = qos.Qos
DomainParticipantQos = qos.Qos
Policy = qos.Policy
Qos = qos.Qos


def _build_reader_listener(reader_wrapper: DataReader, listener: object, status_mask: StatusMask) -> core.Listener:
    def wrap(name, *args):
        cb = getattr(listener, name, None)
        if callable(cb):
            try:
                cb(*args)
            except Exception:
                pass

    kwargs = {}
    if status_mask & StatusMask.DATA_AVAILABLE:
        kwargs["on_data_available"] = lambda _r: wrap("on_data_available", reader_wrapper)
    if status_mask & StatusMask.LIVELINESS_CHANGED:
        kwargs["on_liveliness_changed"] = lambda _r, s: wrap("on_liveliness_changed", reader_wrapper, s)
    if status_mask & StatusMask.REQUESTED_DEADLINE_MISSED:
        kwargs["on_requested_deadline_missed"] = lambda _r, s: wrap("on_requested_deadline_missed", reader_wrapper, s)
    if status_mask & StatusMask.REQUESTED_INCOMPATIBLE_QOS:
        kwargs["on_requested_incompatible_qos"] = lambda _r, s: wrap("on_requested_incompatible_qos", reader_wrapper, s)
    if status_mask & StatusMask.SAMPLE_LOST:
        kwargs["on_sample_lost"] = lambda _r, s: wrap("on_sample_lost", reader_wrapper, s)
    if status_mask & StatusMask.SAMPLE_REJECTED:
        kwargs["on_sample_rejected"] = lambda _r, s: wrap("on_sample_rejected", reader_wrapper, s)
    if status_mask & StatusMask.SUBSCRIPTION_MATCHED:
        kwargs["on_subscription_matched"] = lambda _r, s: wrap("on_subscription_matched", reader_wrapper, s)
    return core.Listener(**kwargs)


def _build_writer_listener(writer_wrapper: DataWriter, listener: object, status_mask: StatusMask) -> core.Listener:
    def wrap(name, *args):
        cb = getattr(listener, name, None)
        if callable(cb):
            try:
                cb(*args)
            except Exception:
                pass

    kwargs = {}
    if status_mask & StatusMask.OFFERED_DEADLINE_MISSED:
        kwargs["on_offered_deadline_missed"] = lambda _w, s: wrap("on_offered_deadline_missed", writer_wrapper, s)
    if status_mask & StatusMask.OFFERED_INCOMPATIBLE_QOS:
        kwargs["on_offered_incompatible_qos"] = lambda _w, s: wrap("on_offered_incompatible_qos", writer_wrapper, s)
    if status_mask & StatusMask.LIVELINESS_LOST:
        kwargs["on_liveliness_lost"] = lambda _w, s: wrap("on_liveliness_lost", writer_wrapper, s)
    if status_mask & StatusMask.PUBLICATION_MATCHED:
        kwargs["on_publication_matched"] = lambda _w, s: wrap("on_publication_matched", writer_wrapper, s)
    return core.Listener(**kwargs)


def _build_command_qos() -> qos.Qos:
    return qos.Qos(
        qos.Policy.Reliability.Reliable(max_blocking_time=util.duration(seconds=1)),
        qos.Policy.History.KeepLast(depth=5),
        qos.Policy.Liveliness.Automatic(lease_duration=util.duration(seconds=5)),
        qos.Policy.ReaderDataLifecycle(
            autopurge_nowriter_samples_delay=util.duration(seconds=0),
            autopurge_disposed_samples_delay=util.duration(seconds=10),
        ),
    )


def _build_config_qos() -> qos.Qos:
    return qos.Qos(
        qos.Policy.Reliability.Reliable(max_blocking_time=util.duration(seconds=1)),
        qos.Policy.Durability.TransientLocal,
        qos.Policy.ReaderDataLifecycle(
            autopurge_nowriter_samples_delay=util.duration(seconds=0),
            autopurge_disposed_samples_delay=util.duration(seconds=10),
        ),
    )


def _build_report_qos() -> qos.Qos:
    return qos.Qos(
        qos.Policy.Reliability.Reliable(max_blocking_time=util.duration(seconds=1)),
        qos.Policy.History.KeepLast(depth=1),
        qos.Policy.ReaderDataLifecycle(
            autopurge_nowriter_samples_delay=util.duration(seconds=0),
            autopurge_disposed_samples_delay=util.duration(seconds=10),
        ),
    )


def _parse_hex(value: str, params: Sequence[str]) -> Optional[List[int]]:
    value = value.strip()
    if value.startswith("%"):
        idx = int(value[1:])
        if idx < 0 or idx >= len(params):
            return None
        value = params[idx]
    value = value.replace(" ", "")
    if len(value) % 2 != 0:
        return None
    try:
        return [int(value[i : i + 2], 16) for i in range(0, len(value), 2)]
    except ValueError:
        return None


def _parse_numeric(value: str, params: Sequence[str]) -> float:
    value = value.strip()
    if value.startswith("%"):
        idx = int(value[1:])
        value = params[idx]
    if "." in value:
        return float(value)
    return int(value)


def _parse_string(value: str, params: Sequence[str]) -> str:
    value = value.strip()
    if value.startswith("%"):
        idx = int(value[1:])
        return params[idx]
    return value.strip("\"'")


def _get_field_value(sample: Any, field: str) -> Any:
    cur = sample
    for part in field.split("."):
        if cur is None:
            return None
        cur = getattr(cur, part, None)
    return cur


def _delete_entity(entity: Any) -> None:
    if entity is None or not hasattr(entity, "_ref"):
        return
    try:
        if entity._ref in core.Entity._entities:
            del core.Entity._entities[entity._ref]
    except Exception:
        pass
    try:
        entity._delete(entity._ref)
    except Exception:
        pass


def _delete_recursive(entity: Any) -> None:
    try:
        children = list(entity.get_children())
    except Exception:
        children = []
    for child in children:
        _delete_recursive(child)
    _delete_entity(entity)
