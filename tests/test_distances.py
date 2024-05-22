from hypothesis import given, strategies as st

from kithairon.picklists import (
    _dest_motion_distance_by_wells,
    _transducer_motion_distance_by_wells,
    well_to_tuple,
)

well384 = st.tuples(
    st.text(alphabet="ABCDEFGHIJKLMNOP", min_size=1, max_size=1),
    st.integers(min_value=1, max_value=24),
).map(lambda x: x[0] + str(x[1]))
well96 = st.tuples(
    st.text(alphabet="ABCDEFGH", min_size=1, max_size=1),
    st.integers(min_value=1, max_value=12),
).map(lambda x: x[0] + str(x[1]))


def tuple_to_well(t: tuple[int, int]) -> str:
    if t[1] < 0 or t[1] >= 24:  # max 24 for 384-well plate  # noqa: PLR2004
        raise IndexError
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[t[0]] + str(t[1] + 1)


@given(
    s1=well384,
    d1=well384,
    mrow=st.integers(min_value=-16, max_value=16),
    mcol=st.integers(min_value=-24, max_value=24),
)
def test_zero_dest_motion_steps_384_384(s1: str, d1: str, mrow: int, mcol: int):
    s1t = well_to_tuple(s1)
    d1t = well_to_tuple(d1)

    s2t = (s1t[0] + mrow, s1t[1] + mcol)
    d2t = (d1t[0] + mrow, d1t[1] - mcol)
    if (
        (d2t[0] < 0)
        or (d2t[0] >= 16)
        or (d2t[1] < 0)
        or (d2t[1] >= 24)
        or (s2t[0] < 0)
        or (s2t[0] >= 16)
        or (s2t[1] < 0)
        or (s2t[1] >= 24)
    ):
        return
    s2 = tuple_to_well(s2t)
    d2 = tuple_to_well(d2t)

    assert _dest_motion_distance_by_wells(s1, d1, s2, d2) == 0


@given(
    s1=well96,
    d1=well96,
    mrow=st.integers(min_value=-8, max_value=8),
    mcol=st.integers(min_value=-12, max_value=12),
)
def test_zero_dest_motion_steps_96_96(s1: str, d1: str, mrow: int, mcol: int):
    s1t = well_to_tuple(s1)
    d1t = well_to_tuple(d1)

    s2t = (s1t[0] + mrow, s1t[1] + mcol)
    d2t = (d1t[0] + mrow, d1t[1] - mcol)
    if (
        (d2t[0] < 0)
        or (d2t[0] >= 8)
        or (d2t[1] < 0)
        or (d2t[1] >= 12)
        or (s2t[0] < 0)
        or (s2t[0] >= 8)
        or (s2t[1] < 0)
        or (s2t[1] >= 12)
    ):
        return
    s2 = tuple_to_well(s2t)
    d2 = tuple_to_well(d2t)

    assert _dest_motion_distance_by_wells(s1, d1, s2, d2, 9.0, 9.0, 9.0, 9.0) == 0


@given(
    s1=well384,
    d1=well96,
    mrow=st.integers(min_value=-16, max_value=16),
    mcol=st.integers(min_value=-24, max_value=24),
)
def test_zero_dest_motion_steps_384_96(s1: str, d1: str, mrow: int, mcol: int):
    s1t = well_to_tuple(s1)
    d1t = well_to_tuple(d1)

    s2t = (s1t[0] + 2 * mrow, s1t[1] + 2 * mcol)
    d2t = (d1t[0] + mrow, d1t[1] - mcol)
    if (
        (d2t[0] < 0)
        or (d2t[0] >= 8)
        or (d2t[1] < 0)
        or (d2t[1] >= 12)
        or (s2t[0] < 0)
        or (s2t[0] >= 16)
        or (s2t[1] < 0)
        or (s2t[1] >= 24)
    ):
        return
    s2 = tuple_to_well(s2t)
    d2 = tuple_to_well(d2t)

    assert _dest_motion_distance_by_wells(s1, d1, s2, d2, 4.5, 4.5, 9.0, 9.0) == 0


def test_basic_zero_dest_motion_steps():
    assert _dest_motion_distance_by_wells("A1", "A1", "A1", "A1") == 0
    assert _dest_motion_distance_by_wells("A1", "A24", "A24", "A1") == 0
    assert _dest_motion_distance_by_wells("A2", "P23", "A23", "P2") == 0


@given(
    s1=well384,
    di=st.integers(min_value=-16, max_value=16),
    dj=st.integers(min_value=-23, max_value=23),
    d1=well384,
    d2=well384,
    sx=st.floats(min_value=0.1, max_value=10.0),
    sy=st.floats(min_value=0.1, max_value=10.0),
    dx=st.floats(min_value=0.1, max_value=10.0),
    dy=st.floats(min_value=0.1, max_value=10.0),
)
def test_transducer_motion_distance(s1, di, dj, d1, d2, sx, sy, dx, dy):
    s1t = well_to_tuple(s1)
    s2t = (s1t[0] + di, s1t[1] + dj)
    if (s2t[0] < 0) or (s2t[0] >= 16) or (s2t[1] < 0) or (s2t[1] >= 24):
        return
    s2 = tuple_to_well(s2t)

    assert _transducer_motion_distance_by_wells(
        s1, d1, s2, d2, sx, sy, dx, dy
    ) == sy * abs(di) + sx * abs(dj)
