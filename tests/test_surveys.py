from typing import cast

import numpy as np
import polars as pl
import pytest
from numpy.testing import assert_almost_equal, assert_array_almost_equal

from kithairon import Labware, SurveyData
from kithairon.surveys import EchoPlateSurveyXML


@pytest.fixture(scope="module")
def surveyreport():
    return SurveyData.read_xml("tests/test_data/surveyreport-cp.xml")


@pytest.fixture(scope="module")
def platesurvey():
    return SurveyData.read_xml("tests/test_data/platesurvey.xml")


@pytest.fixture(scope="module")
def labware_elwx() -> Labware:
    return Labware.from_file("tests/test_data/Labware.elwx")


def test_volumes(platesurvey: SurveyData):
    assert_almost_equal(platesurvey._value_array_of_survey()[1, 4], 25.955)


def test_no_barcode(platesurvey: SurveyData):
    print(pl.col("plate_barcode"))
    assert platesurvey.data.select(pl.col("plate_barcode").is_null().all().alias("v"))[
        "v"
    ][0]


def test_raw_status(platesurvey: SurveyData):
    assert (
        platesurvey.data.get_column("status")[10]
        == "Data missing for well (1th row, 11th column), defaulting to 0.0 value of AQ"
    )


def test_volumes_surveyreport(surveyreport: SurveyData):
    assert_array_almost_equal(
        surveyreport.volumes_array(), np.array([[49.034, 49.983, 49.963, 51.841]])
    )


def test_find_full_plate(surveyreport: SurveyData, labware_elwx: Labware):
    arr = surveyreport.volumes_array(full_plate=True)
    arr2 = surveyreport.volumes_array(full_plate=False)

    assert_array_almost_equal(arr[2:3, 4:8], arr2)


def test_plot_plate(platesurvey: SurveyData, labware_elwx: Labware):
    ax = platesurvey.heatmap()

    import matplotlib.collections

    qm = ax[0].get_children()[0]
    assert isinstance(qm, matplotlib.collections.QuadMesh)
    plot_data = cast(np.ndarray, qm.get_array()).data

    assert_array_almost_equal(plot_data, platesurvey._value_array_of_survey())
