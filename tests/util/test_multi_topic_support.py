import pytest

from umaapy.util.multi_topic_support import (
    SetCollection,
    ListCollection,
    CombinedSample,
    CombinedBuilder,
    OverlayView,
    BuilderEditView,
    CombinedEditHandle,
    ElementHandle,
    SetEditor,
    ListEditor,
)

from umaapy.util.umaa_utils import (
    NumericGUID,
    HashableNumericGUID,
    path_for_set_element,
    path_for_list_element,
    guid_key,
    guid_equal,
)


def mk_guid(n: int) -> NumericGUID:
    return NumericGUID(value=tuple([n] * 16))


def test_guid_helpers_hash_and_equal():
    g1 = mk_guid(1)
    g2 = mk_guid(1)
    hk1 = guid_key(g1)
    hk2 = guid_key(g2)
    assert isinstance(hk1, HashableNumericGUID)
    assert hk1 == hk2
    assert guid_equal(g1, g2)


def test_set_and_list_collections_basic():
    s = SetCollection()
    l = ListCollection()

    class Elem:
        pass

    e1 = Elem()
    e1.elementID = mk_guid(10)
    e2 = Elem()
    e2.elementID = mk_guid(11)

    s.add(e1)
    s.add(e2)
    assert len(s.to_runtime()) == 2
    l.append(e1)
    l.append(e2)
    assert [tuple(x.elementID.value) for x in l.to_runtime()] == [tuple(e1.elementID.value), tuple(e2.elementID.value)]


def test_combinedsample_overlay_and_collections():
    class Base:
        pass

    base = Base()
    cs = CombinedSample(base=base)
    cs2 = cs.clone_with_collections({"waypoints": [1, 2]})
    assert cs2.collections["waypoints"] == [1, 2]

    # nested overlay at path
    class Spec:
        pass

    spec = Spec()
    cs3 = cs2.add_overlay_at(("objective",), spec)
    v = cs3.view
    # accessing objective should yield an overlay view
    assert isinstance(getattr(v, "objective"), OverlayView)


def test_combinedbuilder_per_node_bags_and_spawn_child():
    class Base:
        pass

    b = CombinedBuilder(Base())
    coll = b.ensure_collection_at((), "missionPlan", "set")
    assert coll is b.collections["missionPlan"]

    # Spawn a child for an element path
    eid = mk_guid(99)
    child = b.spawn_child(path_for_set_element("missionPlan", eid), base_obj=object())
    # child inherits collections registered under that path (none yet)
    assert child.collections == {}


def test_edit_handle_write_through_attributes_and_collections():
    class Objective:
        __slots__ = ("speed",)

        def __init__(self):
            self.speed = 0.0

    class Cmd:
        __slots__ = ("objective",)

        def __init__(self):
            self.objective = Objective()

    b = CombinedBuilder(Cmd())
    h = CombinedEditHandle(b)
    h.objective.speed = 3.5  # write-through
    assert b.base.objective.speed == 3.5

    # collections bag at path ('objective',)
    bag = h.objective.collections
    assert bag == b.collections_by_path.setdefault(("objective",), {})


def test_element_editors_return_element_handles():
    class Waypoint:
        pass

    class Command:
        pass

    b = CombinedBuilder(Command())
    le = ListEditor(b, (), "waypoints", Waypoint)
    eh = le.append_new()
    assert isinstance(eh, ElementHandle)
    assert "waypoints" in b.collections
