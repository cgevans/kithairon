from typing import cast

import numpy as np
import pytest
from numpy.testing import assert_almost_equal, assert_array_almost_equal

from kithairon import Labware, Survey
from kithairon.surveys import PlateSurveyXML


@pytest.fixture(scope="module")
def surveyreport():
    return Survey.from_file("tests/test_data/surveyreport-cp.xml")


@pytest.fixture(scope="module")
def platesurvey():
    return Survey.from_file("tests/test_data/platesurvey.xml")


@pytest.fixture(scope="module")
def labware_elwx() -> Labware:
    return Labware.from_file("tests/test_data/Labware.elwx")


def test_volumes(platesurvey: Survey):
    assert_almost_equal(platesurvey.volumes_array()[1, 4], 25.955)


def test_no_barcode(platesurvey: Survey):
    assert platesurvey.plate_barcode is None


def test_raw_status(platesurvey: Survey):
    assert isinstance(platesurvey.raw, PlateSurveyXML)
    assert (
        platesurvey.raw.wells[10].status
        == "Data missing for well (1th row, 11th column), defaulting to 0.0 value of AQ"
    )


def test_volumes_surveyreport(surveyreport: Survey):
    assert_array_almost_equal(
        surveyreport.volumes_array(), np.array([[49.034, 49.983, 49.963, 51.841]])
    )


def test_find_full_plate(surveyreport: Survey, labware_elwx: Labware):
    arr = surveyreport.volumes_array(full_plate=True, labware=labware_elwx)
    arr2 = surveyreport.volumes_array(labware=labware_elwx)

    assert_array_almost_equal(arr, arr2)

    assert arr.shape == (16, 24)


def test_plot_plate(surveyreport: Survey, labware_elwx: Labware):
    ax = surveyreport.plot_volumes()

    import matplotlib.collections

    qm = ax.get_children()[0]
    assert isinstance(qm, matplotlib.collections.QuadMesh)
    plot_data = cast(np.ndarray, qm.get_array()).data

    assert_array_almost_equal(plot_data, surveyreport.volumes_array())
