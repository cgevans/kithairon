"""Test-suite-wide setup.

Some tests exercise APIs that fall back to ``kithairon.labware.DEFAULT_LABWARE``
(e.g. ``SurveyData.plate_shape``). In real use the default labware is loaded
from the user's XDG data dir and is intentionally instrument-specific, so the
library does *not* ship a built-in default. For tests we install the bundled
``tests/test_data/Labware.elwx`` as the default so the suite runs anywhere
without requiring a per-machine config.
"""

from pathlib import Path

import pytest

from kithairon import Labware
from kithairon import labware as _labware_module

TEST_LABWARE = Path(__file__).parent / "test_data" / "Labware.elwx"


@pytest.fixture(autouse=True, scope="session")
def _install_default_labware():
    previous = _labware_module.DEFAULT_LABWARE
    _labware_module.DEFAULT_LABWARE = Labware.from_file(TEST_LABWARE)
    try:
        yield
    finally:
        _labware_module.DEFAULT_LABWARE = previous
